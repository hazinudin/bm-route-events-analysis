# Conditional STA-unit join for `rni_iri_join`

The IRI mart joins silently lose ~98.5% of segments for all periods before 2025 because
`ROUGHNESS_<sem>_<year>` tables store `FROM_STA`/`TO_STA` in **km** while
`RNI_<sem>_<year>` / `stg_rni_combined` store them in **decameters (dam)**.

---

## 1. Problem (verified)

`events_analysis/macros/rni_iri_join.sql:11-14` joins:

```sql
LEFT JOIN smd.roughness_{{semester}}_{{year}} a
    ON a.LINKID = b.LINKID
    AND a.FROM_STA = b.FROM_STA          -- unit mismatch for <= 2024
    AND a.LANE_CODE = b.LANE_CODE
```

Followed by `WHERE IRI is not NULL` (line 15), which turns the LEFT JOIN into an inner
join and drops every unmatched row.

Measured units (all periods, `MIN(TO_STA - FROM_STA)` and `MAX(FROM_STA)`):

| Table set | Periods | FROM_STA range | min delta | Unit |
|---|---|---|---|---|
| `RNI_*` (all) | all | 0 – 15,390 | 0.5 – 1 | dam |
| `ROUGHNESS_*` | 2019 – 2024 | 0 – 153.9 | 0.0009 – 0.01 | km |
| `ROUGHNESS_*` | 2025 – 2026 | 0 – 15,390 | 1 – 10 | dam |

Impact on `KEMANTAPAN_MEAN_IRI` (all 3,306 links present in both, so the row count hides it):

| Period | `SUM(TOTAL_LENGTH)` | Joined stg rows |
|---|---|---|
| 2023 / 2 | 688.71 m (~0.69 km) | 16,124 / 1,045,790 (1.5%) |
| 2025 / 2 | 47,558.27 m (~47.56 km) | 1,047,627 / 1,047,627 (100%) |

With a corrected join (`a.FROM_STA * 100 = b.FROM_STA`) 2023/2 recomputes to
47,537.84 m — within 0.04% of 2025/2, confirming the data itself is fine.

---

## 2. Key decisions

| Decision | Choice | Why |
|---|---|---|
| Fix location | **`rni_iri_join` macro only** | One macro feeds all 8 IRI marts (`mean/max/lkm`, `iri`/`iri_pok`, ±`_sk`). No model SQL changes needed. |
| Detection time | **Macro execution time (`run_query`)** | Same pattern already used by `rni_source` (`adapter.get_relation` + `run_query`). Scale factor is baked into the compiled SQL — zero runtime overhead vs. per-row `CASE` in the join predicate. |
| Detection heuristic | `MIN(TO_STA - FROM_STA) WHERE TO_STA > FROM_STA`; **`< 1` → km, else dam** | User proposed 10 = dam / 0.1 = km, but observed minima are 1 (dam) and 0.0009–0.01 (km). `< 1` separates both families with 2+ orders of magnitude margin on every table. (`MAX(FROM_STA) < 1000` would work equally well; min-delta kept per request.) |
| Scale factor | Multiply the roughness side: `a.FROM_STA * 100 = b.FROM_STA` | stg side stays untouched; other predicates unchanged. Only FROM_STA participates in the join — TO_STA/SEGMENT_LENGTH in the output all come from `b` (stg), so no other column needs rescaling. |
| Unit source of truth | The **roughness** table, not the RNI table | RNI tables are uniformly dam across all periods (verified above). |
| PCI macro | **Not changed now** | `rni_pci_join` has the same latent issue (`PCI_*` tables show the same km→dam split at 2025, and `PCI_2_2025` looks mixed: max FROM_STA 17,050 but min delta 0.02). Flagged as follow-up, out of scope. |

---

## 3. Changes

### 3.1 New helper macro — `events_analysis/macros/rni_sta_scale.sql`

Returns the integer scale factor (100 or 1) to multiply the roughness-side `FROM_STA` by.

```sql
{% macro rni_sta_scale(semester, year) %}
    {%- if not execute -%}
        {{ return(1) }}
    {%- endif -%}

    {%- set table_name = 'ROUGHNESS_' ~ semester ~ '_' ~ year -%}
    {%- set rel = adapter.get_relation(target.database, 'SMD', table_name) -%}
    {%- if rel is none -%}
        {{ exceptions.raise_compiler_error(
            "Source table SMD." ~ table_name ~ " does not exist. Aborting build.") }}
    {%- endif -%}

    {%- set min_delta = run_query(
        "SELECT MIN(TO_STA - FROM_STA) FROM " ~ rel ~ " WHERE TO_STA > FROM_STA"
      ).columns[0][0] -%}

    {%- if min_delta is none -%}
        {{ exceptions.raise_compiler_error(
            "Cannot determine FROM_STA units for SMD." ~ table_name
            ~ " (no rows with TO_STA > FROM_STA). Aborting build.") }}
    {%- elif min_delta < 1 -%}
        {{ return(100) }}
    {%- else -%}
        {{ return(1) }}
    {%- endif -%}
{% endmacro %}
```

### 3.2 Edit — `events_analysis/macros/rni_iri_join.sql`

```sql
{% macro rni_iri_join(semester, year, route_selection)%}
{% set sta_scale = rni_sta_scale(semester, year) %}
SELECT
    b.LINKID,
    b.FROM_STA,
    b.TO_STA,
    b.SEGMENT_LENGTH,
    a.IRI,
    a.IRI_POK,
    b.SURF_TYPE
FROM (select * from {{ref("stg_rni_combined")}} where year = {{year}} and semester = {{semester}}) b
LEFT JOIN  smd.roughness_{{semester}}_{{year}} a
    ON a.LINKID = b.LINKID
    AND a.FROM_STA * {{sta_scale}} = b.FROM_STA
    AND a.LANE_CODE = b.LANE_CODE
WHERE IRI is not NULL {% if route_selection is not none %}AND b.LINKID in ({{"'" + route_selection | join("', '") + "'"}}){% endif %}
{% endmacro %}
```

No changes to any model file, to `stg_rni_combined`, or to dbt_project.yml.

---

## 4. Edge cases (observed in the schema)

| Case | Behavior |
|---|---|
| `ROUGHNESS_2_2018` — table exists but has **no** FROM_STA/TO_STA columns | Detection query raises `ORA-00904` → surfaces as a dbt compile error. Acceptable: that table was unusable by the join anyway. If 2018 ever needs to run, handle separately. |
| `ROUGHNESS_1_2019` — FROM_STA all NULL | Detection returns NULL → explicit `raise_compiler_error` with a clear message instead of silently emitting scale 1. |
| Roughness table missing for requested period | `adapter.get_relation` check raises a clear error (mirrors `rni_source`). Today the failure would surface later at SQL execution. |
| Parse-time (`dbt compile`/docs, `not execute`) | Returns scale 1 — parse stays side-effect-free, matching the `rni_source` pattern. |
| Cost of detection | One `MIN()` scan (~1M rows) per model invocation; 8 IRI models → 8 small queries per full run. Negligible vs. the mart queries themselves. |

---

## 5. Verification

1. `dbt compile` with `--vars '{"year": 2023, "semester": 2}'` and inspect the compiled
   `kemantapan_mean_iri.sql` → join predicate must contain `a.FROM_STA * 100 = b.FROM_STA`.
2. Same with `--vars '{"year": 2025, "semester": 2}'` → predicate must contain
   `a.FROM_STA * 1 = b.FROM_STA`.
3. Run the compiled SQL read-only for both periods and compare `SUM(TOTAL_LENGTH)`:
   expect ~47.5k m for both (previously 688.71 m vs 47,558.27 m).
4. (Optional, after fix is accepted) Re-run the affected marts for 2019–2024 periods to
   backfill correct values — the current mart rows for those periods are understated.

## 6. Follow-ups (out of scope)

- Apply the same conditional-scale join to `rni_pci_join` (`PCI_*` tables show the same
  km→dam split at 2025; `PCI_2_2025` appears internally mixed and needs a closer look).
- Investigate why `KEMANTAPAN_MEAN_IRI` rows for 2023/2 were written with 1.5% coverage
  without anyone noticing — consider a dbt test asserting joined-row coverage
  (e.g. `SUM(TOTAL_LENGTH)` within a tolerance band vs. previous period).
