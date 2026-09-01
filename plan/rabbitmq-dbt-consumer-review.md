# Review: RabbitMQ → dbt Consumer Plan

Review of [`rabbitmq-dbt-consumer.md`](./rabbitmq-dbt-consumer.md), verified against the
actual codebase (dbt 1.10.16 / dbt-oracle 1.10.0, thin mode) — including empirically
running `dbt ls` / `dbt compile` to test the plan's selector claims.

**Verdict:** the architecture and dbt DAG analysis are solid, but three findings are
build-blockers (one verified to silently drop all IRI/PCI events), and several sections
describe AMQP operations or model behavior that don't exist as written.

---

## Verified correct (tested, not just read)

- PCI DAG fix (§3.1) is real and complete: `dbt ls --select stg_rni_combined+` returns
  exactly **20 models**, matching §3.2's breakdown (1 staging + 7 rekap + 8 IRI + 4 PCI).
  No other `smd.rni_*` hardcodes exist anywhere in the project.
- All `unique_key` / `delete+insert` claims in §9 match the actual model configs.
- The `_sk` kemantapan marts filter `routes` directly; the join macros pass `routes`
  through (`rni_pci_join`, `rni_iri_join`).
- The §2 rationale (Python, sync pika, `dbtRunner`, prefetch=1, single replica) is sound —
  dbt is blocking and Oracle-bound; async buys nothing.
- Test-plan structure (three tiers, dlq.py scope boundary, `--empty`/`is_incremental()`
  caveat) is unusually well thought out.

---

## Critical issues (build blockers)

### 1. §5 routing selectors are invalid dbt syntax — consumer will ACK while doing nothing

`kemantapan_*_iri*` and `kemantapan_*_pci*` assume glob support. **dbt `--select` has no
glob wildcards in node names.** Verified:

```text
$ dbt ls --select "kemantapan_*_iri*"
The selection criterion 'kemantapan_*_iri*' does not match any enabled nodes
No nodes selected!          # exit code 0

$ dbt compile --select "kemantapan_*_iri*"
Nothing to do.              # exit code 0
```

Empty selection → exit 0 → `dbtRunnerResult.success=True` → consumer `basic_ack`s a
message for which **zero models ran**. All IRI/PCI events silently dropped. Unit tests
U2/U3 won't catch it — they mock `dbtRunner` and assert the glob string was passed, not
that it matches anything.

**Fixes (pick one):**

- Add `+tags: ["iri"]` / `+tags: ["pci"]` in `dbt_project.yml`, select `tag:iri` /
  `tag:pci` (cleanest; self-maintaining as models are added).
- Explicit model lists (the 8 IRI + 4 PCI names are a stable, known set).
- `stg_rni_combined+` is fine as-is (graph operators are real syntax).

**Guard regardless of fix:** assert the selection is non-empty — check
`len(result.result) > 0` in the consumer, and/or add a CI step running
`dbt ls --select <each ROUTING_TO_SELECT value>` that fails on empty output.
(`--warn-error` would also catch this, but every dbt invocation currently warns about the
stale `models.events_analysis.example` config block in `dbt_project.yml` — clean that up
first if going that route.)

### 2. §7/§8 retry design violates AMQP semantics — §17 encodes a duplication bug

**Headers cannot be modified on requeue.** `basic_nack(requeue=True)` returns the original
message with original headers; there is no way to increment `x-retry-count` that way. The
plan says "requeue with `x-retry-count` header" (§7 step 7, §8) but the tests say
"republished with `x-retry-count: 1`" (U11) — different mechanisms. Worse, U11 as written
does **both** (`basic_nack(requeue=True)` *and* republish), duplicating the message on
every retry.

**Correct pattern:** `basic_publish` a new copy with incremented `x-retry-count`, **then**
`basic_ack` (or `basic_nack(requeue=False)`) the original. There is a publish-then-ack
crash window (crash between → duplicate), which §9 idempotency makes tolerable — state it.

Separately, **§8's "exponential backoff" has no mechanism**: plain requeue/republish is
immediate — a downed Oracle gets a hot retry loop. Backoff requires a TTL retry queue with
DLX back to the main queue, or the `rabbitmq-delayed-message-exchange` plugin. Either spec
the topology or drop the claim.

### 3. DLQ mechanism is doubly-specified and self-contradictory

Three sections imply three different designs:

- §4 declares a DLQ with "bindings = same 3 routing keys" but **never declares the
  dead-letter exchange** those bindings attach to. If the DLQ binds to `validation.events`
  directly with `verified.*` keys, every *new* event also lands in the DLQ immediately.
- §7/§8 say "nack(requeue=False) → DLQ with `x-error: ...`". Broker dead-lettering cannot
  attach custom headers — it only adds `x-death` / `x-first-death-*`. Custom `x-error`
  requires the consumer to manually publish to the DLQ (which is what §11's `dlq.py` does).
- Doing **both** (DLX on the queue *and* manual publish) double-delivers every failure.

**Pick one.** Manual publish (dlq.py + `basic_ack` of the original) matches U14 and gives
the `x-error` headers; then the queue must *not* have `x-dead-letter-exchange` set, and §4
needs a separate DLX exchange (e.g. `validation.events.dlx`) for the DLQ bindings to mean
anything. If you prefer broker dead-lettering (simpler, survives consumer crashes between
publish and ack), drop the custom `x-error` header and read `x-death` instead — and drop
dlq.py.

---

## Data-model inconsistencies (plan vs. actual SQL)

### 4. §9's idempotency rationale is factually wrong about `stg_rni_combined`

§9: *"The `stg_rni_combined` incremental path detects changed routes via `UPDATE_DATE` and
only touches those rows."* In the actual model, lines 24–29 compute `max_update_date` via
`run_query` and then **never use it** — the `WHERE` clause is `1=1` + optional
`LINKID IN (...)`. Dead code. Actual behavior: **full year+semester partition rescan**
(optionally route-filtered), made safe by `delete+insert`.

Still idempotent — but:

- §6's schema comment "omit `routes` = all changed routes" is misleading; it's *all*
  routes in the partition. Set Oracle-load expectations accordingly.
- Either fix the model to actually filter `UPDATE_DATE > max_update_date`, or fix the
  plan's description.

### 5. `full_refresh: true` interacts dangerously with the year-scoped models

Two undocumented surprises:

- The marts (`rekap_*`, `kemantapan_*`) bake `var('year')`/`var('semester')` into the SQL
  with **no `is_incremental()` branch**. `--full-refresh` drops the target table and reruns
  the model SQL → the recreated table contains **only the message's year**; all other
  years' rows are destroyed. "Full refresh" here means "single-year partition replacement."
- `stg_rni_combined`'s full-refresh path does the opposite: it **ignores**
  `year`/`semester`/`routes` entirely and unions a hardcoded list — `[2022, 2023, 2024,
  2025] × [semester 2]`. A `full_refresh` message for `year=2026, semester=1` rebuilds stg
  with 2022–2025 sem-2 data only, while downstream marts compute `year=2026, semester=1`
  → empty results → and their 2022–2025 rows are wiped by the drop/recreate.

As specified, the §6 escape hatch can cause silent cross-year data loss. Options: gate
`full_refresh` behind a separate runbook action with documented semantics; fix the models
to make full-refresh year-aware (and add semester 1 / newer years to the stg union list);
or drop the field from the v1 schema.

### 6. §3.3's "transitive routes filtering" claim is wrong (harmless, but misdescribed)

The plan says the `_sk` rekap marts "inherit route filtering when the parent was just
rebuilt with the same routes filter in the same run." They don't: `rekap_lebar_rni_sk`
queries the parent's *materialized table* (`select * from ref("rekap_lebar_rni") where
year = ...`), which still contains all routes — `delete+insert` on the parent only
replaced the filtered LINKIDs. The `_sk` rekap mart **always recomputes the full year
partition**, every time, regardless of what the parent just did. The caveat below the
claim ("will re-process all routes for that year") is the correct statement; the
"unless..." clause contradicts it and should be deleted. Functionally fine (idempotent),
just more Oracle work than implied — relevant when sizing `stop_grace_period`.

---

## Failure-handling gaps

### 7. §8's transient/permanent split misroutes deterministic errors

`result.exception is not None` lumps Oracle connection blips (transient — retry is right)
with compile/reference errors (deterministic — retry is pointless). Example: a message for
`year=2026, semester=2` → `source('binamarga', 'RNI_2_2026')` **doesn't exist in
sources.yml** (only `RNI_1_2026` is declared) → compilation exception → retried 3× with
backoff → DLQ minutes later. Same for Jinja errors or a missing `smd.pci_2_2026` table.

**Fix:** classify by exception type — retry only database/connection errors; send
compilation/reference errors straight to DLQ (`x-error: "dbt_compile_error"`). Better:
validate `(year, semester)` against the declared source-table list in the consumer and
reject as `invalid_payload` before invoking dbt at all.

### 8. The crash path is an unbounded poison-message loop

§8: "Process crash mid-run → RabbitMQ redelivers." But `x-retry-count` only increments on
the *handled* exception path. A message that reliably crashes the consumer (e.g. OOM during
dbt parse) redelivers → crashes → redelivers forever.

**Fix:** declare `dbt.events.worker` as a **quorum queue with `x-delivery-limit: 3`** —
the broker counts redeliveries and dead-letters automatically after the limit. One queue
argument covers the entire crash-loop class. (Bonus: consider `x-single-active-consumer`
to broker-enforce §15.5's "exactly one replica" rule during deployments.)

### 9. The `verified.rni` cascade has an undocumented hard dependency

`stg_rni_combined+` rebuilds the IRI **and PCI** marts, which read hardcoded
`smd.roughness_<sem>_<yr>` / `smd.pci_<sem>_<yr>` tables. If the RNI event for a period
arrives before the PCI raw table exists → ORA-00942 → PCI nodes fail → `success=False` →
the whole message DLQs as `dbt_run_failed`, even though RNI + rekap succeeded. §2's
"independent" assumption covers *ordering*, not *existence of sibling raw tables at
cascade time*. At minimum document this coupling and the re-drive procedure; alternatively,
inspect per-node statuses and ACK-with-warning if only same-class downstream nodes failed
(policy decision — make it consciously).

---

## Smaller inconsistencies

| # | Section | Issue |
|---|---|---|
| 10 | §10 | The "v2 Rust engine via FFI / `DbtRunnerError` / not the original exception" notes describe **dbt Fusion / dbt-core v2**, but §13 pins `dbt-core>=1.7,<2.0` (env has 1.10.16). On 1.x, `result.exception` *is* the original Python exception. Align the notes with the pinned version — the retry classifier (finding 7) depends on it. |
| 11 | §7 step 5 | `cli_args` never includes `--project-dir` / `--profiles-dir` / `--target`. It works only because dbt honors `DBT_PROJECT_DIR`/`DBT_PROFILES_DIR` env vars — say so, and note the profile's default target is literally named `dev` (profiles.yml), confusing for a prod consumer. Consider explicit `DBT_TARGET` + `--target`. |
| 12 | §7 step 6 | `process_data_events(time_limit=0)` looping until the future completes is a 100% CPU busy-spin. Use `time_limit=1` (blocks, wakes on events) or add a sleep. Also state explicitly: **all pika channel operations (ack/nack/publish) happen on the main thread** — pika's BlockingConnection is not thread-safe; the worker thread only returns the future. |
| 13 | §13 vs §17.3a | `--empty` and dbt unit tests require dbt **1.8+**, but the floor is `>=1.7`. Bump to `>=1.8` (or match the lockfile at 1.10). §17.4 uses `pytest`, `pytest-mock`, `testcontainers` but §13/`pyproject.toml` has no dev dependency group; `uv sync --no-dev` in the Dockerfile presumes one exists. |
| 14 | §15.3 | `read_only: true` + tmpfs only for `/tmp`: dbt writes `logs/dbt.log` under the project dir by default → **read-only failure inside the container**. Add `DBT_LOG_PATH=/tmp/...` (or a volume). The `dbt-target` volume alone isn't enough. |
| 15 | §17 tiers | I6 asserts "no duplicate rows" but sits in the integration tier, which by its own definition has no real Oracle — that assertion is only possible in 17.3b. Move it. |
| 16 | §12/§5 | `RABBITMQ_ROUTING_KEYS` (env) and `ROUTING_TO_SELECT` (code) can drift apart — an extra env binding becomes a DLQ magnet (`unknown_routing_key`). Derive bindings from the dict keys, or validate `env ⊆ dict` at startup. |
| 17 | §14 | `heartbeat=0` as the "first iteration" fallback: a *hung* (not dead) consumer then holds the unacked message indefinitely — pair it with a supervisor liveness probe, not just restart-on-exit. |

---

## Things missing entirely

1. **DLQ ownership / replay runbook.** Nothing says who drains `dbt.events.worker.dlq` or
   how a fixed message gets re-driven (rabbitmqadmin? shovel? a `replay` subcommand?).
   §8 generates DLQ traffic on four paths; include the re-drive procedure, especially
   given finding 9.
2. **Duplicate-event coalescing.** N queued `verified.iri` events for the same
   `(year, semester)` → N identical full-partition rebuilds, sequentially. Since each run
   recomputes the whole partition from source tables, runs 2..N are pure waste. A
   "peek pending same-key messages and ack them with the first" coalescing step is a
   cheap, high-value optimization — at least an Open Items entry. (Related property worth
   stating: because runs are partition rebuilds, event *ordering* within a key doesn't
   affect correctness — only finding 9's table-existence coupling does.)
3. **`emitted_at` is collected but never used.** Cheap win: log/trace
   `now() - emitted_at` as processing lag (OTel span attribute), optionally warn on very
   stale events.
4. **Ordering-violation detection.** §2 assumes upstream emits `verified.rni` before
   `verified.iri`/`verified.pci`. If violated, IRI/PCI marts build from stale stg data and
   ACK — silently. No need to block it, but tracing `stg_rni_combined`'s
   `MAX(UPDATE_DATE)` per partition on each run would make out-of-order builds visible in
   Jaeger.
5. **Startup validation of selector liveness** (cheap version of finding 1's guard): at
   consumer start, run `dbt ls --select` for each `ROUTING_TO_SELECT` value and refuse to
   start on empty. Catches selector typos *and* renamed models at deploy time instead of
   first-event time.
6. **OBJECTID churn note.** Every rebuild regenerates `OBJECTID` via
   `sde.gdb_util.next_rowid(...)`, so redeliveries/retries produce new OBJECTIDs for the
   same logical rows. No duplicate rows (§9 holds), but if any downstream
   ArcGIS/feature-service consumer keys on OBJECTID stability, that's worth one sentence
   in §9.

---

## Bottom line

Do not build from the plan until findings **1–3** are fixed — the selector bug alone makes
the consumer a silent message-eater, and the retry/DLQ sections describe AMQP operations
that don't exist as written. Findings **4–5** mean operational behavior (partition-wide
rebuilds, `full_refresh` blast radius) differs materially from what the plan documents.
The rest are correctness-of-documentation and hardening items that are cheap to fix now
and expensive to discover in production.
