# Deflection Module: Refactor & Database Writeback Plan

Status: **All phases COMPLETE** — writer + `save_results` implemented; unit tests
and the live DB round-trip are green.
Last updated: 2026-09-28 (Phase 2 implemented + live round-trip verified).

## Goal

1. Add idempotent writeback of deflection results to `SMD.FWD`, `SMD.LWD`, `SMD.BB`.
2. Split the single `Deflection` class into a base class + per-type subclasses
   (FWD / LWD / BB), preserving verified behavior.
3. Fix the bugs surfaced by the verification work (see below).

## Verification baseline (DONE)

`tests/deflection/test_deflection_db_verification.py` — characterization tests
that run the current implementation against production data (credentials from
`~/.dbt/profiles.yml`, profile `events_analysis`; module skips without it).
All 4 tests pass in ~3s and are the safety net for the refactor:

| Test | Verifies | Result |
|---|---|---|
| `test_fwd_matches_smd_fwd` | `FWD_2_2020` → `SMD.FWD` 2020: drop selection, NORM, D0_D200, CORR (tol 1e-6) | max diff ~1e-7 |
| `test_bb_matches_smd_bb` | `BB_2_2020` → `SMD.BB` 2020: BB_D0/BB_D0_D200 exact; CORR + FWD-conversion (tol 2e-4) | 277/277 rows |
| `test_lwd_normalization_matches_smd_lwd` | round-trip on `SMD.LWD` 2021: NORM_LWD_D0/D1, D0_D200 | max diff ~6e-9 |
| `test_lwd_temp_correction_matches_smd_lwd` | round-trip: CORR_LWD_D0/D1, CORR_D0_D200 | max diff ~6e-9 |

## Findings from DB verification

### Confirmed facts (design inputs)

- **Reference load is 40 for both FWD and LWD** (`FORCE` in kN, `LOAD_KG` in kg —
  implied ref reverse-engineered as exactly 40.0 across all rows).
- **Target tables are keyed by `YEAR`**, unique per `(LINKID, STA)`; FWD rows are
  1:1 with `(LINKID, STA, SURVEY_DIREC)` as well.
- **Lookup tables exist in SMD**: `D0_TEMP_CORRECTION`, `D200_TEMP_CORRECTION`,
  `BB_D0_TEMP_CORRECTION`, `BB_D0_D200_TEMP_CORRECTION`, `BB_D0_FWD_CONVERSION`,
  `BB_D0_D200_FWD_CONVERSION` (plus `FWD_LWD_D0_TEMP` / `FWD_LWD_D0_D200_TEMP`
  which hold identical values with `f`-prefixed column names).
- Staging tables (`FWD_2_YYYY`, `LWD_2_YYYY`, `BB_2_YYYY`) are the pipeline inputs;
  `SMD.FWD/LWD/BB` are the calculated results.
- **Target schemas contain columns the class does not produce** (`YEAR`, `SEMESTER`,
  `SEGMENT_LENGTH`, `BALAI_ID`, `SATKER_PPK_ID`, `BM_PROV_ID`, `OBJECTID`, …) →
  the writer needs an explicit per-type column mapping, not a blind `to_sql`.

### Additional finding

- **`force_col` is never used in the BB path** — BB has no sorting and no
  normalization, so the force value plays no role in its calculation. Today's
  callers must pass a dead argument (the BB verification test passes
  `force_col="LOAD_TON"` purely to satisfy the constructor). The per-type
  column config (see Phase 1) makes this explicit: `BbColumns` has no `force`
  field.

### Data issues (not code bugs)

- `SMD.LWD` was **not** loaded from `LWD_2_2021` (stored raw deflections differ
  entirely) → staging-to-target verification is impossible for LWD; round-trip
  on stored rows is used instead.
- `SMD.FWD` 2025 rows with `UPDATE_DATE = 2026-01-05` disagree with the current
  implementation's CORR output (~85% of that batch), while the `2026-01-06`
  batch matches 100% → that batch was loaded with different lookup-table values.
- Staging LWD/BB tables contain duplicate measurements per station; dedup happens
  **outside** the class (BB dedups by full measurement signature).

### Bugs in the current implementation (fix in Phase 1)

1. **Crash on empty input / all-null `FORCE`**: `_sorting()` returns `None`, then
   line 100 (`self.sorted[asp_temp]`) raises `TypeError`. The legacy
   `tests/deflection/test_deflection.py::test_fwd` currently fails for exactly
   this reason (0 unprocessed linkids remain in `FWD_2_2025`).
2. **Unconditional drop of `['OBJECTID', 'SURVEY_DATE', 'UPDATE_DATE']`**:
   `SMD.LWD` has no `SURVEY_DATE` → `KeyError` on LWD round-trips. Must use
   `errors="ignore"` or a per-type drop list.
3. **`ampt_tlap` is computed unconditionally** (line 100), even when
   `sort_only=True` or `sorted` is `None`.
4. **Float-key fragility** in the temp-correction join: input keys use
   `int(np.round(x,1)*10)` while lookup keys use `.round(1).astype(int)` —
   currently self-consistent but brittle. Make both sides explicit integer
   tenths (`(round(x,1)*10).__round__()`) in the refactor.

## Phase 1 — Class split (behavior-preserving refactor + bug fixes)

New structure:

```
src/deflection/
├── base.py        # DeflectionBase — route filtering, _sorting helper,
│                  #   _read_lookup_table, temp-correction engine, save_results
├── columns.py     # FwdLwdColumns / BbColumns — frozen dataclasses (see below)
├── fwd.py         # FwdDeflection   — sorting + survey_direc required
├── lwd.py         # LwdDeflection   — thin subclass (no sorting)
├── bb.py          # BbDeflection    — own formulas (D3−D1, D2−D1) + BB→FWD conversion
├── factory.py     # create_deflection(data_type=...) → correct subclass
├── deflection.py  # backward-compat shim (keeps `from src.deflection.deflection import Deflection` working)
└── writer.py      # Phase 2
```

### Column config (locked)

Column names move into **frozen dataclasses holding only input-schema column
names**; run-behavior params (`df`, `force_ref`, `routes`, `sort_only`, `conn`)
stay on the constructor. One flat dataclass per type family — no nesting, no
god-object mixing connection/run options with schema.

```python
@dataclass(frozen=True)
class FwdLwdColumns:
    # location / shared
    routeid: str = "LINKID"
    sta: str | None = "STA"
    from_m: str | None = None
    to_m: str | None = None
    survey_direc: str | None = "SURVEY_DIREC"
    asphalt_temp: str = "ASPHALT_TEMP"
    surf_thickness: str = "SURF_THICKNESS"
    # measurement — no defaults, per-type
    force: str
    d0: str
    d200: str

@dataclass(frozen=True)
class BbColumns:
    # location / shared — same fields as above
    ...
    # measurement — BB schema, note: NO force field (unused in BB path)
    bb_d1: str = "BB_D1"
    bb_d2: str = "BB_D2"
    bb_d3: str = "BB_D3"
```

Per-type instances live as class attributes, so the common case is two args:

```python
class FwdDeflection(NormalizedDeflectionBase):
    COLUMNS = FwdLwdColumns(force="FORCE", d0="FWD_D1", d200="FWD_D2")

class LwdDeflection(NormalizedDeflectionBase):
    COLUMNS = FwdLwdColumns(force="LOAD_KG", d0="LWD_D0", d200="LWD_D1")

FwdDeflection(df, conn=engine)            # all defaults
FwdDeflection(df, conn=engine,
              columns=FwdLwdColumns(force="FORCE", d0="FWD_D1", d200="FWD_D2",
                                    from_m="FROM_STA", to_m="TO_STA", sta=None))
```

Each subclass also declares its other class constants instead of runtime branching:

```python
class BbDeflection(DeflectionBase):
    TARGET_TABLE = "SMD.BB"
    REQUIRES_SORTING = False
    COLUMNS = BbColumns()
    def _calculate(self): ...
```

Base `__init__` becomes a template: `_filter_routes()` → `_sort_if_needed()` →
`_calculate()`. The `Deflection` factory shim maps legacy kwargs
(`force_col=…`, `d0_col=…`, …) into the config so existing call sites keep
working unchanged.

Bug fixes folded into this phase (each gets a unit test):

- Empty input / all-null force → `self.sorted` stays an empty DataFrame, no crash.
- Safe metadata drop (`errors="ignore"`).
- `ampt_tlap` computed only inside the calculation step.
- Explicit integer-tenths keys on both sides of the lookup join.
- Typo `coversion_factor` → `conversion_factor`; drop `(object)` inheritance;
  `isinstance(routes, list)` instead of `type(routes) == list`.

**Acceptance**: `tests/deflection/test_deflection_db_verification.py` passes
unchanged (all 4 tests).

### Phase 1 completion (DONE)

Files created under `src/deflection/`:
`columns.py` (`FwdLwdColumns`, `BbColumns` frozen dataclasses), `base.py`
(`DeflectionBase`, `NormalizedDeflectionBase`), `fwd.py`, `lwd.py`, `bb.py`,
`factory.py` (`create_deflection`), `deflection.py` (legacy `Deflection(...)`
shim mapping flat kwargs to the config), `__init__.py` (re-exports).
New tests: `tests/deflection/test_deflection_unit.py` (10 DB-free tests).

Results:
- `tests/deflection/test_deflection_db_verification.py` — **4/4 pass unchanged**
  (numeric behavior preserved; acceptance gate met).
- `tests/deflection/test_deflection_unit.py` — **10/10 pass**.
- Bug fixes verified by the new tests: empty / all-null-force input no longer
  crashes (`sorted` stays an empty DataFrame); LWD input without `SURVEY_DATE`
  no longer raises `KeyError`; `ampt_tlap` is lazy; lookup join keys are now
  symmetric via `_tenths_key`.
- Legacy `tests/deflection/test_deflection.py::test_fwd` was **repointed**
  (decision: keep as an integration check). Its old "`LINKID NOT IN FWD`" query
  returned 0 rows because all staging rows are now processed; it now samples
  staging rows for linkids already present in `SMD.FWD` 2025 (ROWNUM cap 500)
  and passes. Full deflection suite: **15/15 green**
  (4 DB characterization + 10 unit + 1 repointed integration).

Implementation note: `NormalizedDeflectionBase` needs its derived column names
(`NORM_*`/`CORR_*`) available while `_calculate()` runs from the base `__init__`
(before the subclass `__init__` body continues). Handled with a
`_set_derived_names()` helper called at the top of `_calculate()` and after
`super().__init__()`.

## Phase 2 — Database writeback

New `src/deflection/writer.py`, following the idempotent pattern of
`src/traffic/vcr_aadt_adt/writer.py`:

```
write_deflection_results(connection, df, data_type, year, routes) -> int
```

- Table mapping per subclass `TARGET_TABLE` (`SMD.FWD` / `SMD.LWD` / `SMD.BB`).
- **Idempotent**: `DELETE FROM <table> WHERE YEAR = :1 AND LINKID IN (...)`
  (route-narrowed; skip narrowing when routes == 'ALL'), then `executemany`
  insert, commit on success / rollback on error.
- **Year comes from a parameter** (staging tables have `SURVEY_YEAR`, not `YEAR`).
- **Explicit per-type column mapping** into the target schema (targets have
  extra columns: `YEAR`, `SEMESTER`, `SEGMENT_LENGTH`, …); `SEMESTER` also a
  parameter, defaulting to `None`.
- Extract `_to_python` (numpy→Python conversion) into a shared helper reused by
  both this writer and `traffic/vcr_aadt_adt/writer.py`.

### Phase 2 — DB inspection findings (resolved)

- **`OBJECTID` is the only NOT NULL column** in `SMD.FWD` / `SMD.LWD` / `SMD.BB`
  (everything else is nullable), and it has **no identity, default, or trigger**.
  A plain `INSERT` without it fails with `ORA-01400`.
- OBJECTID values are **geodatabase-managed** and must be allocated with
  `sde.gdb_util.next_rowid(owner, table)`. Verified against this schema:
  `SELECT sde.gdb_util.next_rowid('SMD', '<TABLE>') FROM DUAL` → `int`
  (TWO-argument signature; `EXECUTE` granted to PUBLIC). The legacy loader used
  the same function (see `rabbitmq-dbt-consumer-review.md` finding 6).
- **Route-attribute columns are enriched elsewhere**: `BALAI_ID`,
  `SATKER_PPK_ID`, `BM_PROV_ID`, `SEGMENT_LENGTH`, and `FROM_STA`/`TO_STA` are
  populated in `SMD.LWD`/`SMD.BB` (and partly empty in `SMD.FWD`) but are NOT
  produced by the `Deflection` classes and are absent from some staging tables.
  The writer therefore writes **only the columns the calculation actually
  produces** (whitelist intersection with the target schema); absent target
  columns are left NULL. Route-attribute enrichment is out of scope.
- In `SMD.FWD`, `SURVEY_DATE`, `COPIED`, `FROM_STA`, `TO_STA`, `SEGMENT_LENGTH`
  are 0/185 populated — consistent with a subset write.

Public API on `DeflectionBase`:

```python
def save_results(self, year: int, semester: int | None = None) -> int:
    """Persist self.sorted to the subclass TARGET_TABLE; returns rows written."""
```

- Explicit call only — never auto-save in `__init__`.
- Raises clear errors when `conn` is None or `sorted` is empty.
- Tracks processed routes during `_filter_routes()` for the DELETE narrowing.

**Acceptance**: writer unit tests (mocked cursor: DELETE SQL + params, INSERT
shape + OBJECTID allocation, column whitelist, commit/rollback) + a DB
round-trip test against a scratch table proving `save_results()` output
re-reads identically. To avoid creating/dropping objects in the production
`SMD` schema, the DB round-trip test is **opt-in** (skipped unless
`DEFLECTION_DB_WRITE_TEST=1`) and uses an injected sequential OBJECTID provider
(a scratch table is not registered in the geodatabase, so `gdb_util.next_rowid`
cannot serve it).

### Phase 2 completion (DONE)

Files created/changed:
- `src/deflection/writer.py` — `write_deflection_results`, `next_object_id`,
  `_allocate_object_ids`, `_delete_existing`, `_insert_results`, plus
  `TARGET_TABLES` / `TARGET_COLUMNS` whitelists.
- `src/deflection/base.py` — `DeflectionBase.save_results(year, semester=None)`
  (guards `conn`/empty `sorted`; borrows a raw connection and closes it).
- `src/worker/db.py` — shared `to_python`; `traffic/vcr_aadt_adt/writer.py`
  now imports it (local `_to_python` removed).
- `src/deflection/__init__.py` — exports `write_deflection_results`,
  `next_object_id`.
- `tests/deflection/test_writer.py` — 16 mocked-cursor tests.
- `tests/deflection/test_writer_db.py` — opt-in scratch-table round-trip
  (creates/drops `SMD.DEFL_WRITER_TEST` only; never touches production tables).

Design specifics locked during implementation:
- `routes` normalization: `None`/`"ALL"` → no narrowing; list/tuple/set →
  `LINKID IN (...)`; single string → `LINKID = :2`; empty collection → no narrowing.
- Insert columns = `OBJECTID` + whitelisted df columns (target order) + `YEAR`
  + `SEMESTER`; unknown df columns ignored; target columns absent from df left NULL.
- OBJECTID allocated one at a time via `sde.gdb_util.next_rowid('SMD', <table>)`
  (injectable `object_id_provider` for tests; per-row loop can be batched later).

Results:
- `tests/deflection/` — **31 passed, 1 skipped** (the opt-in DB test).
- `tests/traffic/vcr_aadt_adt/test_aadt_writer.py` — **7 passed** (shared-helper
  refactor is behavior-preserving).

Live DB round-trip: **PASSED**
(`DEFLECTION_DB_WRITE_TEST=1 uv run pytest tests/deflection/test_writer_db.py -v`)
— scratch table `SMD.DEFL_WRITER_TEST` created and dropped, round-trip values
and idempotency verified. Production tables unchanged
(`SMD.FWD` 59,433 / `SMD.LWD` 46,474 / `SMD.BB` 554 rows, matching pre-run counts).

## Phase 3 — Test consolidation

- `tests/deflection/test_deflection_db_verification.py` — DONE (characterization).
- `tests/deflection/test_deflection_unit.py` — DONE (10 DB-free tests: empty
  input, all-null force, LWD without `SURVEY_DATE`, factory routing, sort_only,
  normalization/BB formulas, config contracts, legacy shim).
- `tests/deflection/test_writer.py` — DONE (16 mocked-cursor tests).
- `tests/deflection/test_writer_db.py` — DONE (opt-in live round-trip, skipped by
  default; run with `DEFLECTION_DB_WRITE_TEST=1`).
- Legacy `tests/deflection/test_deflection.py::test_fwd` — DONE: repointed to a
  stable query (staging rows whose linkids already exist in `SMD.FWD` 2025,
  capped at 500 rows). Kept as an extra integration check.

## Decisions locked (previously open questions)

| # | Question | Decision |
|---|---|---|
| 1 | Delete key | `YEAR` + `LINKID` (route-narrowed), per AADT writer pattern |
| 2 | YEAR source | `save_results(year=…)` parameter |
| 3 | Column handling | explicit per-type mapping into target schema |
| 4 | Commit semantics | auto-commit inside writer, rollback on error |
| 5 | `_to_python` helper | extract to shared module, reuse in traffic writer |
| 6 | Constructor arg bloat | frozen per-type column dataclasses (`FwdLwdColumns` / `BbColumns`) as class attributes; behavior params stay on constructor; legacy kwargs mapped by factory shim |
| 7 | BB force argument | `BbColumns` has no `force` field — force is unused in the BB path (finding from verification) |
| 8 | OBJECTID generation | `sde.gdb_util.next_rowid('SMD', <table>)` per row (geodatabase-managed); injectable `object_id_provider` for tests |
| 9 | DB write test safety | opt-in via `DEFLECTION_DB_WRITE_TEST=1`, scratch table only, injected ids — default test run performs no production writes |
| 10 | Non-produced target columns | left NULL; route-attribute enrichment (`BALAI_ID`, `SEGMENT_LENGTH`, …) is out of scope |
