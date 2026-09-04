# RabbitMQ → dbt Consumer Plan

A Python consumer that listens to a RabbitMQ topic exchange and triggers dbt model
rebuilds for the `events_analysis` project (RNI / IRI / PCI road-condition marts on Oracle).

---

## 1. Goal

When an upstream system verifies RNI, IRI, or PCI data, it publishes a message to the
`validation.events` topic exchange. The consumer receives the message and runs the
appropriate dbt selection against the `events_analysis` project, then acks/nacks based
on the dbt run result.

---

## 2. Key Decisions (and rationale)

| Decision | Choice | Why |
|---|---|---|
| Language | **Python** | The project is already Python (dbt-core + dbt-oracle, uv, Python 3.11+). dbt is a Python tool with a Python API (`dbtRunner`). Go/Rust would have to shell out to the `dbt` CLI and replicate the painful `dbt-oracle`/`oracledb` install in a second toolchain. |
| Consumer style | **Sync (`pika`), not async (`aio-pika`)** | dbt has **no async API** — `dbtRunner.invoke()` is a blocking call. v2 serializes invocations through thread-level locks; parallel execution in one process is explicitly unsupported. With `prefetch_count=1` and one queue, there is no I/O concurrency to exploit during a dbt run. Async would just wrap a blocking call in `run_in_executor` — pure ceremony. |
| Topology | **Single queue, 3 bindings, sequential** | One consumer process, `prefetch_count=1`, manual ack. Simplest ops. The bottleneck is Oracle, not message throughput. |
| dbt invocation | **`dbtRunner` (Python API)**, not subprocess | In-process, structured `dbtRunnerResult` with per-node statuses. Useful for downstream status reporting (publishing run results back to a queue). |
| Routing-key dependency | **Independent** — each key rebuilds only its own selection | Assume upstream emits `verified.rni` before `verified.iri`/`verified.pci` when ordering matters. No defensive `stg_rni_combined` refresh on IRI/PCI events. |
| PCI DAG fix | **Applied** — `rni_pci_join` macro now uses `ref("stg_rni_combined")` | Previously the macro hardcoded `smd.rni_<sem>_<yr>`, creating a DAG gap: `stg_rni_combined+` cascaded to rekap + IRI marts but silently skipped all PCI marts. Now fixed (see §3). |

---

## 3. dbt DAG Context (verified)

### 3.1 The PCI macro fix

`events_analysis/macros/rni_pci_join.sql` previously joined `smd.rni_<sem>_<yr>` directly
(hardcoded schema.table), so dbt could not see that `kemantapan_*_pci*` models depend on
`stg_rni_combined`. The macro now mirrors `rni_iri_join`:

```sql
-- Before
INNER JOIN smd.rni_{{semester}}_{{year}} b

-- After
INNER JOIN (select * from {{ref("stg_rni_combined")}} where year = {{year}} and semester = {{semester}}) b
```

Verified: `dbt ls --select stg_rni_combined+` now returns **20 models**, including all 4 PCI
marts (`kemantapan_max_pci`, `kemantapan_max_pci_sk`, `kemantapan_mean_pci`,
`kemantapan_mean_pci_sk`) that were previously missing. The PCI macro was the **only**
place with a hardcoded `smd.rni_*` reference — no other DAG gaps exist.

### 3.2 What `stg_rni_combined+` reaches (20 models)

- `stg_rni_combined` (staging, incremental, `unique_key=['LINKID','YEAR','SEMESTER']`, `delete+insert`)
- 7 rekap marts: `rekap_lebar_rni`, `rekap_lebar_rni_sk`, `rekap_tipe_jalan`,
  `rekap_tipe_jalan_sk`, `rekap_tipe_perkerasan`, `rekap_tipe_perkerasan_sk`,
  `rekap_tipe_perkerasan_lkm`
- 8 IRI kemantapan marts: `kemantapan_lkm_iri`, `kemantapan_lkm_iri_pok`,
  `kemantapan_mean_iri`, `kemantapan_mean_iri_pok`, `kemantapan_mean_iri_sk`,
  `kemantapan_mean_iri_pok_sk`, `kemantapan_max_iri`, `kemantapan_max_iri_sk`
- 4 PCI kemantapan marts: `kemantapan_mean_pci`, `kemantapan_mean_pci_sk`,
  `kemantapan_max_pci`, `kemantapan_max_pci_sk`

> **Note:** The `_sk` models also `ref("active_lrs")`, which is NOT downstream of
> `stg_rni_combined` (it reads the `lrs` source). So `stg_rni_combined+` will not rebuild
> it. `active_lrs` is a static network table built once; it must be bootstrapped before
> the consumer starts. Worth a pre-flight existence check.

### 3.3 `routes` var support

All models support `routes` either directly or transitively:

- **Directly:** `stg_rni_combined`; all 4 non-`_sk` rekap marts; all non-`_sk` kemantapan
  marts (pass `routes` to the join macro); all `_sk` kemantapan marts (filter directly).
- **Transitively (via parent `ref`):** the 3 `_sk` rekap marts
  (`rekap_lebar_rni_sk`, `rekap_tipe_jalan_sk`, `rekap_tipe_perkerasan_sk`) — they query
  their non-`_sk` parent filtered only by `year`, so they inherit route filtering when
  the parent was rebuilt with the same `routes` filter in the same run.

> **Caveat:** the `_sk` rekap marts will re-process **all routes for that year/semester**
> (parent filters by `year`, not `routes`), unless the parent was just rebuilt with the
> same `routes` filter. In the `verified.rni` cascade this is fine (parent runs before
> child). For `verified.iri`/`verified.pci` (which don't touch rekap) it's irrelevant.

**Conclusion:** always safe to pass `routes` in `--vars` — no model will break.

---

## 4. RabbitMQ Topology

All failure routing is broker-managed via Dead Letter Exchanges (DLX) — the canonical
RabbitMQ worker/retry/dead pattern. The consumer **never** manually publishes to the DLQ and
**never** uses `basic_nack(requeue=True)` (that redelivers immediately, bypasses the DLX, and
spins forever). The consumer only acks, nacks with `requeue=False`, or republishes a retry
copy to the retry exchange. Because the broker owns the terminal hop, there is no
publish-then-ack crash window on the DLQ path; only the transient-retry republish has one
(tolerable — see §9).

Three exchanges, three queues:

```
Exchanges:
  validation.events        type=topic durable=true      # publisher entry point
  validation.events.dlx    type=topic durable=true      # dead-letter exchange (terminal)
  validation.events.retry  type=topic durable=true      # retry entry point (the work queue
                                                        #   does NOT bind this, so retry copies
                                                        #   wait, not re-run)

Queues:
  dbt.events.worker        durable=true  (quorum recommended)
    args:  x-dead-letter-exchange     = validation.events.dlx
           x-dead-letter-routing-key  = dead           # nack(requeue=False) -> DLQ
    bind on validation.events:  verified.rni / verified.iri / verified.pci
    optional hardening:  x-delivery-limit = 3  (quorum; caps crash-loop redeliveries,
                          dead-letters with reason delivery_limit -> DLQ; see §8)

  dbt.events.worker.retry  durable=true  (classic; TTL hold, NO consumer)
    args:  x-dead-letter-exchange     = validation.events      # expiry -> back to work queue
           (no x-dead-letter-routing-key -> keeps the original verified.* routing key)
    bind on validation.events.retry:  #   (catches all retry republishes)

  dbt.events.worker.dlq    durable=true                # terminal dead-letter queue
    bind on validation.events.dlx:  dead              # (or '#' to also catch expiry traffic)
```

Message lifecycle:

- **Happy path:** publisher → `validation.events` → `dbt.events.worker` → consumer acks.
- **Transient failure:** consumer republishes a copy to `validation.events.retry` with the
  original routing key, per-message `expiration` (exponential backoff) and incremented
  `x-retry-count`, then acks the original. The copy sits in the retry queue; when its TTL
  expires the broker dead-letters it back to `validation.events` (with its original
  `verified.*` routing key) → back onto the work queue.
- **Permanent failure / retry-saturated:** consumer `basic_nack(requeue=False)` → broker
  dead-letters to `validation.events.dlx` (routing key `dead`) → `dbt.events.worker.dlq`.

The broker appends an `x-death` header (reason `rejected`/`expired`/`delivery_limit`, source
queue, time, count, original routing keys) to every dead-lettered message — that is the DLQ
audit metadata. There are **no application-set `x-error` headers**: the consumer cannot mutate
the stored message at nack time, so granular failure classification (`invalid_payload`,
`unknown_routing_key`, `dbt_run_failed`, `retry_saturated`) is emitted to logs / OpenTelemetry
traces keyed by `job_id`, which the DLQ message retains in its body for correlation.

Single consumer process reads `method.routing_key` from each delivery to decide the dbt
selection. `basic_qos(prefetch_count=1)` — only one unacked message at a time. Manual ack.
All topology (exchanges, queues, bindings) is declared idempotently by the consumer at
startup (`topology.py`, see §11); a mismatch between declared and live queue arguments means
the topology was changed out-of-band — refuse to start.

---

## 5. Routing-Key → dbt Selection Map

dbt's `--select` does **not** support glob wildcards inside node names — `kemantapan_*_iri*`
matches zero nodes and exits 0 (verified on dbt 1.10.16). An empty selection returns
`success=True`, so the consumer would ack IRI/PCI messages while running **no** models. IRI
and PCI are therefore selected by **dbt tags**.

**One-time setup — tag the models** (in each model's `config()` block): the 8 IRI marts get
`tags=['iri']`, the 4 PCI marts get `tags=['pci']`:

| Model | tag |
|---|---|
| `kemantapan_lkm_iri`, `kemantapan_lkm_iri_pok`, `kemantapan_mean_iri`, `kemantapan_mean_iri_pok`, `kemantapan_mean_iri_sk`, `kemantapan_mean_iri_pok_sk`, `kemantapan_max_iri`, `kemantapan_max_iri_sk` | `iri` |
| `kemantapan_mean_pci`, `kemantapan_mean_pci_sk`, `kemantapan_max_pci`, `kemantapan_max_pci_sk` | `pci` |

e.g. `kemantapan_mean_iri.sql`:
```sql
{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR', 'SEMESTER'],
        incremental_strategy='delete+insert',
        tags=['iri']
    )
}}
```

Tags self-maintain as models are added; `dbt ls --select tag:iri` returns the 8 IRI models,
`tag:pci` the 4 PCI models. Then select by tag:

```python
ROUTING_TO_SELECT = {
    "verified.rni": "stg_rni_combined+",   # graph operator — cascades to all 19 downstream
    "verified.iri": "tag:iri",             # 8 IRI models
    "verified.pci": "tag:pci",             # 4 PCI models
}
```

`stg_rni_combined+` uses a real dbt graph operator and is unaffected. All three require
`year` + `semester` vars; `routes` is optional.

**Non-empty-selection guard (mandatory, regardless of selector mechanism):** an empty dbt
selection returns `success=True` with exit 0. After `dbtRunner.invoke`, assert the selection
actually produced nodes before acking:

```python
result = runner.invoke(cli_args)
node_results = result.result or []
if result.success and not node_results:
    # zero models ran for a "successful" invocation -> hard failure, do NOT ack
    basic_nack(delivery_tag, requeue=False)   # broker DLX -> DLQ (reason logged: empty_selection)
```

**CI selector gate (deploy-time guard):** run `dbt ls --select <each ROUTING_TO_SELECT
value>` in CI (and optionally at consumer startup) and fail/refuse-to-start on empty output.
This catches a tag typo or a renamed model at deploy time instead of first-event time.

---

## 6. Message Schema

Pydantic model for the JSON body:

```python
class TriggerMessage(BaseModel):
    job_id: str                         # UUID for traceability
    routing_key: str                    # echoed from envelope (for logging)
    year: int                           # e.g. 2025
    semester: int                       # 1 or 2
    routes: list[str] | None = None      # optional; omit = all changed routes
    full_refresh: bool = False          # optional; forces --full-refresh
    emitted_at: datetime                # ISO 8601 timestamp from publisher
```

### 6.1 AMQP envelope

The publisher sends the message to the `validation.events` topic exchange. The AMQP envelope
must contain:

| Field | Required value | Purpose |
|---|---|---|
| Exchange | `validation.events` | Topic exchange receiving verification events |
| Exchange type | `topic` | Routes messages by verification type |
| Routing key | `verified.rni`, `verified.iri`, or `verified.pci` | Selects the dbt invocation |
| `content_type` | `application/json` | Identifies the body format |
| `content_encoding` | `utf-8` | Identifies the body encoding |
| `delivery_mode` | `2` / persistent | Allows the message to survive broker restart |
| `message_id` | Same value as `job_id` | Broker-level message correlation |
| `timestamp` | UTC publish time | Broker-level publish timestamp |

The consumer treats the envelope routing key as authoritative. The JSON `routing_key` field is
an audit copy and must match it; a mismatch is invalid and must be sent to the DLQ. The publisher
must publish the message as one atomic delivery, not one message per route.

### 6.2 JSON body

Example for an RNI verification event:

```json
{
  "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
  "routing_key": "verified.rni",
  "year": 2025,
  "semester": 2,
  "routes": ["01001", "01002"],
  "full_refresh": false,
  "emitted_at": "2026-08-21T09:30:00Z"
}
```

Message variants use the same body structure. Only the envelope routing key and matching body
field change:

```text
verified.rni -> rebuild stg_rni_combined+
verified.iri -> rebuild tag:iri
verified.pci -> rebuild tag:pci
```

Field rules:

- `job_id` is a unique UUID generated by the publisher and is used for tracing logs, retries,
  and DLQ messages.
- `year` is the four-digit target year.
- `semester` must be `1` or `2`.
- `routes` is optional. Omit it or set it to `null` to process all changed routes. When present,
  it must be a list of route/LINKID strings.
- `full_refresh` is optional and defaults to `false`. Set it to `true` only when a full dbt
  refresh is explicitly required.
- `emitted_at` is a UTC ISO 8601 timestamp from the publisher.

The publisher must use UTF-8 JSON and must not include database credentials or other secrets in
the body. An invalid JSON body, missing required field, invalid semester, or routing-key mismatch
is rejected with `basic_nack(requeue=False)`; the broker dead-letters it to the DLQ via the DLX
(§4) and the consumer logs the reason `invalid_payload` keyed by `job_id`.

Invalid payloads → `basic_nack(requeue=False)` → broker DLX → DLQ (reason `invalid_payload`,
recorded in logs/traces, not in message headers).

---

## 7. Consumer Flow (per message)

1. **`basic_qos(prefetch_count=1)`** — one unacked message at a time.
2. **Receive** delivery; read `method.routing_key`.
3. **Validate** body with pydantic. Invalid → nack(requeue=False) → DLQ.
4. **Resolve selection** from `ROUTING_TO_SELECT[routing_key]`. Unknown key →
   `basic_nack(requeue=False)` (broker DLX → DLQ); log reason `unknown_routing_key`.
5. **Build CLI args:**
   ```python
   cli_args = ["run", "--select", selection, "--vars",
               json.dumps({
                   "year": msg.year,
                   "semester": msg.semester,
                   **({"routes": msg.routes} if msg.routes else {}),
               })]
   if msg.full_refresh:
       cli_args.append("--full-refresh")
   ```
6. **Invoke** `dbtRunner.invoke(cli_args)` in a single worker thread. The main thread
   pumps `connection.process_data_events(time_limit=0)` in a loop until the future
   completes, keeping RabbitMQ heartbeats alive during long dbt runs.
7. **Inspect** `dbtRunnerResult` and act (see §8 for the full failure matrix):
   - `success=True` **and** `len(result.result) > 0` → `basic_ack`.
   - `success=True` but `len(result.result) == 0` → empty-selection hard failure →
     `basic_nack(requeue=False)` (broker DLX → DLQ); log reason `empty_selection` (§5 guard).
   - `success=False`, `exception is None` (dbt-level model/test failures) →
     `basic_nack(requeue=False)` (broker DLX → DLQ); log reason `dbt_run_failed` + per-node
     statuses from `result.result`. Not retried — re-running the same bad data won't fix it.
   - `exception is not None` (transient — Oracle connection blip) → bounded retry: read the
     incoming `x-retry-count` header (absent = 0). If `< RETRY_MAX`, `basic_publish` a copy to
     `validation.events.retry` with the original routing key, per-message `expiration` =
     `2**n` seconds (exponential backoff), and `x-retry-count = n + 1`; then `basic_ack` the
     original. If `>= RETRY_MAX`, `basic_nack(requeue=False)` (broker DLX → DLQ); log reason
     `retry_saturated`.
8. **Log** `job_id`, routing key, selection, duration, node-level statuses to
   `logs/consumer.log`.

---

## 8. Failure Handling & Retry

All terminal failures use `basic_nack(requeue=False)` and rely on the broker DLX (§4) to move
the message to the DLQ — the consumer never manually publishes to the DLQ, so there is no
publish-then-ack crash window on the terminal path. Only the transient-retry republish has one
(tolerable — see §9). `basic_nack(requeue=True)` is never used: it redelivers immediately,
bypasses the DLX, and spins a deterministic failure into an infinite hot loop.

| Failure type | Detection | Action | DLQ/trace reason |
|---|---|---|---|
| Invalid payload | pydantic `ValidationError` | nack(requeue=False) → broker DLX → DLQ | `invalid_payload` |
| Unknown routing key | not in `ROUTING_TO_SELECT` | nack(requeue=False) → broker DLX → DLQ | `unknown_routing_key` |
| Empty selection | `success=True` but `len(result.result)==0` | nack(requeue=False) → broker DLX → DLQ | `empty_selection` (see §5) |
| dbt model/test failures | `success=False`, `exception is None` | nack(requeue=False) → broker DLX → DLQ. Not retried — re-running the same bad data won't fix it. | `dbt_run_failed` (+ per-node statuses) |
| Transient (Oracle down, FFI error) | `exception is not None` | Republish to `validation.events.retry` with `expiration=2**n`s and `x-retry-count=n+1`, then ack. After `RETRY_MAX` → nack(requeue=False) → DLQ. | `retry_saturated` |
| Process crash mid-run | unacked message | RabbitMQ redelivers on reconnect (crash-safe — see §9). Optional quorum `x-delivery-limit=3` caps the crash-loop class and dead-letters with reason `delivery_limit` → DLQ. | (delivery_limit) |

Exponential backoff is real, not just claimed: the per-message `expiration` on the republished
copy makes the message sit in `dbt.events.worker.retry` for `2**n` seconds before the broker
dead-letters it back to the work queue. No `rabbitmq-delayed-message-exchange` plugin required.

**Transient-retry crash window:** the consumer republishes the retry copy *then* acks the
original. If the process dies between those two steps, the original is redelivered *and* the
retry copy exists → a duplicate retry. §9 idempotency makes this tolerable (delete+insert on
stable unique keys). There is no equivalent window on the terminal/DLQ path, which is pure
broker dead-lettering.

---

## 9. Idempotency & Crash Safety

All target models are `incremental` with `delete+insert` on stable `unique_key`s:

- `stg_rni_combined`: `['LINKID', 'YEAR', 'SEMESTER']`
- rekap marts: `['LINKID', 'YEAR']`
- IRI kemantapan marts: `['LINKID', 'YEAR', 'SEMESTER']`
- PCI kemantapan marts: `['LINKID', 'YEAR']`

A redelivered message (process crash mid-run) re-applies cleanly — no dedup layer needed. The
same holds for the §8 transient-retry crash window: a duplicate retry copy and the original
redelivery both run `delete+insert` over the same `(year, semester)` partition, producing no
duplicate rows.
The `stg_rni_combined` incremental path detects changed routes via `UPDATE_DATE` and only
touches those rows.

---

## 10. dbtRunner Integration Notes

- Construct a **single** `dbtRunner` instance on the main thread. Reuse it for every
  message — v2's thread-level lock makes per-message construction wasteful and you lose
  any cached state.
- Call `.invoke()` from **one thread at a time** (the worker thread). The main thread
  must not call `.invoke()` while the worker is running.
- v2's engine is Rust accessed via FFI; `result.exception` is a `DbtRunnerError`
  (engine failure) or an unwrapped Python exception (FFI boundary failure), not the
  original Python exception object.
- `result.result` varies by command — for `run`, it's a list of per-node run results
  with `.unique_id` and `.status` (useful for the DLQ status report).

---

## 11. Project Structure

```
src/dbt_events_consumer/
  __init__.py
  __main__.py        # entrypoint: python -m dbt_events_consumer
  config.py          # env-driven: RABBITMQ_URL, DBT_PROJECT_DIR, DBT_PROFILES_DIR, RETRY_MAX
  schema.py          # pydantic TriggerMessage model
  runner.py          # dbtRunner wrapper: thread + heartbeat pump, returns dbtRunnerResult
  consumer.py        # pika BlockingConnection + ROUTING_TO_SELECT map; basic_consume; ack/nack/retry decisions
  topology.py        # idempotent declare of all 3 exchanges, 3 queues, and bindings (§4); called at startup
  observability.py   # structured file logs to logs/ (dev) + OpenTelemetry traces to the existing collector → Jaeger (prod), correlated by job_id
```

> **Topology + failure-routing ownership (build-phase reminder):** `topology.py` is a pure,
> idempotent declarer — signature `(connection) -> None`, it declares the three exchanges,
> three queues, and all bindings from §4 and asserts the live queue arguments match (a mismatch
> means the topology was changed out-of-band; refuse to start). The per-message failure decision
> (read `x-retry-count`, compare against `RETRY_MAX`, choose ack / nack(requeue=False) /
> republish-to-retry-with-backoff) is **stateful** and reads the inbound delivery, so it lives in
> `consumer.py`'s handler. There is **no `dlq.py`** and no manual DLQ publish: terminal failures
> are `nack(requeue=False)` and the broker owns the DLQ hop.

Run with:

```bash
python -m dbt_events_consumer
```

---

## 12. Configuration (env-driven)

| Var | Default | Description |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/%2F` | AMQP connection URL |
| `RABBITMQ_EXCHANGE` | `validation.events` | Topic exchange name |
| `RABBITMQ_QUEUE` | `dbt.events.worker` | Consumer queue name |
| `RABBITMQ_DLQ` | `dbt.events.worker.dlq` | Dead-letter queue name |
| `RABBITMQ_ROUTING_KEYS` | `verified.rni,verified.iri,verified.pci` | Comma-separated bindings |
| `DBT_PROJECT_DIR` | `./events_analysis` | Path to dbt project root |
| `DBT_PROFILES_DIR` | `~/.dbt` | Path to dbt profiles |
| `RETRY_MAX` | `3` | Max transient-retry attempts before the DLQ |
| `LOG_DIR` | `./logs` | Where to write consumer logs |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP endpoint of the existing OpenTelemetry collector (gRPC). Set empty/unset to disable tracing (dev). |
| `OTEL_SERVICE_NAME` | `dbt-events-consumer` | Service name reported to the collector/Jaeger |
| `OTEL_RESOURCE_ATTRIBUTES` | _(empty)_ | Extra OTel resource attrs (e.g. `deployment.environment=prod`) |

The dead-letter exchange, retry exchange, and retry queue names are **derived** from the base
names above by `topology.py` (`<EXCHANGE>.dlx`, `<EXCHANGE>.retry`, `<QUEUE>.retry`) — no
separate env vars, so the topology cannot drift out of sync (see §4).

---

## 13. Dependencies (add to `pyproject.toml`)

```toml
dependencies = [
    "dbt-core>=1.7,<2.0",
    "dbt-oracle>=1.7,<2.0",
    "dotenv>=0.9.9",
    "pandas>=2.3.3",
    "pyarrow>=25.0.0",
    "sqlalchemy>=2.0.45",
    "pika>=1.3",          # NEW: sync RabbitMQ client
    "pydantic>=2.0",      # NEW: payload validation
    "opentelemetry-sdk>=1.27",               # NEW: tracing SDK
    "opentelemetry-exporter-otlp>=1.27",      # NEW: OTLP exporter → existing collector
]
```

No `tenacity` — retry logic is RabbitMQ-level (republish to the retry exchange with
`x-retry-count` + per-message `expiration`; the broker dead-letters it back after the TTL), not
in-process.
The OpenTelemetry packages ship traces to the **existing** collector/Jaeger instance already
provisioned for production; no new tracing backend to stand up.

---

## 14. Deployment / Runtime Notes

- Run as a supervised process (systemd unit or k8s Deployment with `restartPolicy=Always`).
  On crash, the supervisor restarts the consumer; RabbitMQ redelivers the last unacked
  message (idempotent — see §9).
- Set `prefetch_count=1` — never raise it; dbt's global-state rule means two concurrent
  runs in one process are unsafe (v2 locks them anyway, so you'd just serialize with
  extra overhead).
- Heartbeat handling (the known pika gotcha): the worker-thread + `process_data_events`
  pump pattern (§7 step 6) keeps heartbeats alive during long dbt runs. Alternative for
  a first iteration: `pika.BlockingConnection` with `heartbeat=0` + supervised restart.
- Oracle sessions: each dbt run opens its own Oracle session via `dbt-oracle` thin mode.
  Ensure the Oracle connection pool / profile `threads` setting is compatible with one
  run at a time.

---

## 15. Docker Deployment Plan

### 15.1 Image options evaluated

| Option | Assessment |
|---|---|
| **Project-owned image from `python:3.11-slim`** | **Recommended.** Install the versions from `uv.lock`, including `dbt-core`, `dbt-oracle`, `pika`, and `pydantic`. Copy the consumer and dbt project into the image. This keeps the runtime reproducible and matches the existing thin-mode Oracle plan. |
| `ghcr.io/dbt-labs/dbt-core:<version>` | Not sufficient by itself. It provides dbt Core, but the Oracle adapter and this RabbitMQ consumer still need to be installed. Use only as a base if its Python and dependency versions are verified against the lockfile. |
| `ghcr.io/it-at-m/dbt-oracle:<version>` | Possible alternative. This community image includes `dbt-oracle` and Oracle Instant Client for thick mode. It is not the project-owned or Oracle-maintained adapter image, so pin an exact tag and digest, scan it, and validate its architecture and dependency versions before production use. |

The Oracle-maintained `oracle/dbt-oracle` repository distributes the adapter package, but it is
not a complete image for this consumer. The default should therefore be a custom image owned by
this project. Use the prebuilt `it-at-m` image only when Oracle thick mode is required, for
example for native network encryption.

### 15.2 Image contents

Add these deployment files:

```text
Dockerfile
.dockerignore
deploy/
  compose.yaml
  profiles.yml.example
```

The image should:

- Use a pinned `python:3.11-slim-bookworm` base digest.
- Install dependencies with `uv sync --frozen --no-dev --no-install-project` so the image uses
  the committed `uv.lock` rather than resolving dependencies at runtime.
- Include `src/dbt_events_consumer` and `events_analysis` in `/app`.
- Set `DBT_PROJECT_DIR=/app/events_analysis` and `PYTHONPATH=/app/src`.
- Set `DBT_PROFILES_DIR=/run/secrets/dbt` and mount `profiles.yml` at runtime.
- Set `PYTHONUNBUFFERED=1` so consumer logs are visible immediately in Docker logs.
- Start with `python -m dbt_events_consumer`.

Illustrative Dockerfile shape:

```dockerfile
FROM python:3.11-slim-bookworm

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \\
    PYTHONPATH="/app/src" \\
    PYTHONUNBUFFERED="1" \\
    DBT_PROJECT_DIR="/app/events_analysis" \\
    DBT_PROFILES_DIR="/run/secrets/dbt"

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv \\
    && uv sync --frozen --no-dev --no-install-project

COPY src/dbt_events_consumer ./src/dbt_events_consumer
COPY events_analysis ./events_analysis

ENTRYPOINT ["python", "-m", "dbt_events_consumer"]
```

The final Dockerfile must be adjusted if the consumer package is made installable. Do not copy
`.venv`, `.git`, dbt logs, or `events_analysis/target` from the build context.

### 15.3 Runtime configuration

RabbitMQ and Oracle are external services. The container only runs the consumer and dbt. A
Compose deployment should use the following settings:

```yaml
services:
  dbt-events-consumer:
    image: registry.example.com/bm-route-events-analysis:${IMAGE_TAG}
    restart: unless-stopped
    init: true
    stop_grace_period: 30m
    env_file:
      - .env
    environment:
      DBT_PROJECT_DIR: /app/events_analysis
      DBT_PROFILES_DIR: /run/secrets/dbt
      RABBITMQ_QUEUE: dbt.events.worker
      RABBITMQ_DLQ: dbt.events.worker.dlq
      RABBITMQ_ROUTING_KEYS: verified.rni,verified.iri,verified.pci
      LOG_DIR: /var/log/dbt-events
    volumes:
      - ./secrets/profiles.yml:/run/secrets/dbt/profiles.yml:ro
      - dbt-target:/app/events_analysis/target
      - consumer-logs:/var/log/dbt-events
    read_only: true
    tmpfs:
      - /tmp

volumes:
  dbt-target:
  consumer-logs:
```

Do not put Oracle passwords or RabbitMQ credentials in the image, Dockerfile, or committed
Compose file. Supply them through Docker secrets, an environment-injection mechanism, or the
deployment platform's secret manager. Prefer `amqps://` and the Oracle TLS configuration when
the services are not on a trusted private network.

### 15.4 Startup and shutdown behavior

The container entrypoint should perform these checks before starting the RabbitMQ consumer:

1. Verify that `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` exist.
2. Run `dbt debug --project-dir /app/events_analysis --profiles-dir /run/secrets/dbt`.
3. Perform the `active_lrs` pre-flight check from §16 item 1 once that check is implemented.
4. Exit non-zero if a check fails so Docker restarts the container.

On `SIGTERM`, stop accepting new deliveries, allow the current dbt invocation to finish, and
ack only after a successful run. Set `stop_grace_period` longer than the maximum expected dbt
run. If the grace period expires, the process may be killed and RabbitMQ will redeliver the
unacked message after restart.

### 15.5 Deployment sequence

1. CI runs unit tests, `dbt parse`, and a connectivity validation against a non-production
   target where available.
2. CI builds the image, scans it, and publishes an immutable tag based on the Git commit SHA.
3. Deploy the image to a host that can reach both RabbitMQ and Oracle; verify DNS, firewall rules,
   TLS certificates, and Oracle client requirements.
4. Run the startup checks and confirm the consumer declares `validation.events`,
   `dbt.events.worker`, and `dbt.events.worker.dlq`.
5. Publish one test event for each routing key and verify the expected dbt selection, ack, and
   logs. Test invalid payloads and transient failures against the DLQ/retry behavior.
6. Run exactly one consumer replica for v1. Do not perform an overlapping deployment that leaves
   two consumer processes executing dbt concurrently; stop the old replica before starting the
   new one, or add an explicit distributed lock.

### 15.6 Operations

- Configure Docker log rotation or ship the `consumer-logs` volume to centralized logging.
- Alert on container restarts, RabbitMQ connection loss, queue depth, DLQ depth, and dbt run
  duration.
- Pin both the base image digest and dependency lockfile. Never deploy `latest`.
- Rebuild regularly for OS and Python security updates, then rerun the dbt and Oracle smoke tests.
- Keep the image stateless apart from temporary dbt artifacts and logs. The database remains the
  source of truth, and RabbitMQ remains responsible for redelivery of unacked messages.

### 15.7 Docker acceptance criteria

- A clean host can start the consumer with only Docker, the image, runtime secrets, and network
  access to RabbitMQ and Oracle.
- `dbt debug` succeeds inside the image using the mounted profile.
- RNI, IRI, and PCI messages are consumed sequentially from `dbt.events.worker`.
- Container termination during a dbt run results in RabbitMQ redelivery and an idempotent rerun.
- No database credentials are present in image layers, Git, or normal container logs.

### 15.8 Research references

- [dbt Core container package](https://github.com/dbt-labs/dbt-core/pkgs/container/dbt-core)
- [Oracle-maintained dbt-oracle adapter](https://github.com/oracle/dbt-oracle)
- [Oracle setup for dbt Core](https://docs.getdbt.com/docs/local/connect-data-platform/oracle-setup)
- [Community dbt-oracle container](https://github.com/it-at-m/dbt-oracle)

---

## 16. Open Items / Future Considerations

1. **Pre-flight `active_lrs` check** — the `_sk` models depend on `active_lrs`, which is
   not in the `stg_rni_combined+` cascade. Add a startup or per-message existence probe,
   or document that `active_lrs` must be bootstrapped before the consumer starts.
2. **Status reporting queue** — optionally publish run summaries (job_id, node statuses,
   duration) to a `dbt.run.results` queue for downstream monitoring/alerting. The
   `dbtRunner` API makes this cheap since `result.result` is structured.
3. **Backpressure / queue depth alerts** — if the publisher outpaces dbt runs, the queue
   grows unbounded. Add a depth threshold alert (separate from the consumer).
4. **Per-semester partitioning** — if RNI/IRI/PCI events for different semesters ever need
   to run concurrently, partition into separate queues (one consumer process per
   semester) to avoid Oracle table contention. Not needed for v1.
5. **Observability** — tracing is provided by the **existing OpenTelemetry collector +
   Jaeger instance** already provisioned for production. The consumer emits OTLP traces
   (one span per message, attributes: `job_id`, routing key, selection, duration, per-node
   dbt statuses, retry count, DLQ reason). `job_id` is the cross-system correlation key, so
   a message's journey publisher → consumer → dbt run → DLQ is visible as one Jaeger trace.
   File logs to `LOG_DIR` remain for local/dev where the collector isn't running. Remaining
   work: decide whether dbt run durations also go to Prometheus as metrics, or whether Jaeger
   span durations are sufficient for SLO/alerting.

---

## 17. Test Plan

Every failure branch in §7/§8 maps to at least one test scenario below. Tests are split into
three tiers by what they require to run.

### 17.1 Unit tests — pure logic, no broker, no dbt, no Oracle

Mock the pika channel and `dbtRunner.invoke`. These run in CI on every commit, fast.

| # | Scenario | Assert |
|---|---|---|
| U1 | Valid RNI payload, `dbtRunner` returns `success=True` | `basic_ack` called once; selection = `stg_rni_combined+`; `--vars` contains `year`/`semester`; no `--full-refresh` |
| U2 | Valid IRI payload, `routes=None` | `--vars` JSON has no `routes` key; selection = `tag:iri` |
| U3 | Valid PCI payload, `routes=["01001"]` | `--vars` JSON has `"routes": ["01001"]`; selection = `tag:pci` |
| U4 | `full_refresh=True` | CLI args contain `--full-refresh` |
| U5 | Malformed JSON body | `basic_nack(requeue=False)` called; reason `invalid_payload` logged (no manual DLQ publish — that is broker-side, asserted in I3) |
| U6 | Valid JSON, missing required field (e.g. no `semester`) | `basic_nack(requeue=False)`; reason `invalid_payload` |
| U7 | `semester=3` (out of range) | `basic_nack(requeue=False)`; reason `invalid_payload` |
| U8 | Envelope routing key not in `ROUTING_TO_SELECT` | `basic_nack(requeue=False)`; reason `unknown_routing_key` |
| U9 | Envelope routing key ≠ body `routing_key` field | `basic_nack(requeue=False)`; reason `invalid_payload` (mismatch) |
| U10 | `dbtRunner` returns `success=False`, `exception=None` | `basic_nack(requeue=False)` called once; reason `dbt_run_failed` + per-node statuses from `result.result` logged (no manual DLQ publish) |
| U11 | `dbtRunner` returns `exception is not None`, incoming `x-retry-count` absent | `basic_publish` to `validation.events.retry` with original routing key, `expiration=2s`, `x-retry-count: 1`; then `basic_ack` of original. **No `basic_nack(requeue=True)`.** |
| U12 | Same as U11, incoming `x-retry-count: 2` (`RETRY_MAX=3`) | republish with `expiration=8s`, `x-retry-count: 3`; then `basic_ack` |
| U13 | Same as U11, incoming `x-retry-count: 3` (== `RETRY_MAX`) | `basic_nack(requeue=False)` (broker DLX → DLQ); reason `retry_saturated` |
| U14 | Empty selection: `success=True` but `len(result.result)==0` | `basic_nack(requeue=False)` (broker DLX → DLQ); reason `empty_selection` (the §5 guard). Asserts the consumer never acks a zero-model run. |
| U15 | Config load: missing `RABBITMQ_URL` | startup raises before connecting |
| U16 | Config load: non-integer `RETRY_MAX` | startup raises |

> Retry-count bookkeeping (U11–U13) lives in `consumer.py`'s handler — there is no `dlq.py`.
> U14 now asserts the §5 empty-selection guard (no manual DLQ publish); the real DLQ-via-DLX
> arrival is covered in the integration tier (I3).

### 17.2 Integration tests — real RabbitMQ, mocked-or-real dbt

Require a RabbitMQ broker (testcontainers or CI service container). dbt runs against
`dbt debug` / a parse-only target — **not** a real Oracle mart build, to keep CI hermetic.

| # | Scenario | Assert |
|---|---|---|
| I1 | Consumer declares `validation.events`, `validation.events.dlx`, `validation.events.retry`, `dbt.events.worker`, `dbt.events.worker.retry`, `dbt.events.worker.dlq` with correct bindings on startup | topology visible via management API |
| I2 | Publish valid `verified.rni` event; `dbtRunner` stubbed to succeed | message acked; queue empty; one Jaeger span emitted with `job_id` attr (if collector reachable) |
| I3 | Publish invalid payload | message lands in `dbt.events.worker.dlq` via broker DLX (with `x-death` header, reason `rejected`); original queue empty; consumer log/trace carries reason `invalid_payload` keyed by `job_id` |
| I4 | Publish `verified.xyz` (unknown key) | DLQ via broker DLX; reason `unknown_routing_key` |
| I5 | Transient failure → retry loop until `RETRY_MAX` then DLQ | retry queue holds the copy for `2**n`s each; `x-retry-count` 1→2→3; copy returns to work queue after each TTL; then DLQ via DLX with reason `rejected` (consumer logs `retry_saturated`) |
| I6 | Kill consumer mid-run (SIGKILL) with one unacked message | on restart, RabbitMQ redelivers; second run acks; no duplicate rows (idempotency per §9) |
| I7 | Long-running dbt stub (> heartbeat interval) | connection does not drop; heartbeats kept alive by the §7 step-6 pump |
| I8 | SIGTERM during a run | consumer stops accepting new deliveries, lets current run finish, acks on success, exits 0 |

### 17.3 End-to-end / Docker acceptance — real broker; dbt validated via dry run (no non-prod Oracle)

> **Constraint: there is currently no non-prod Oracle target.** dbt has no true "dry run"
> that validates SQL against a database schema *without* a DB connection. The options form a
> spectrum, and the plan's `dbt-core>=1.7,<2.0` pin puts the 1.8 features (`--empty`,
> unit tests) in range:

| dbt method | DB needed? | What it validates |
|---|---|---|
| `dbt parse` | no | manifest, refs resolve, YAML |
| `dbt compile` | no | Jinja → SQL renders, ref resolution, template syntax (NOT column existence) |
| `dbt run --empty` (1.8+) | **yes** | SQL executes against the real schema (cols/types/deps) with 0 rows from refs/sources — ~zero data cost. Closest thing to a true dry run |
| dbt unit tests (1.8+) | no (mocked rows) | transform logic: given input rows → expected output rows. Good fit for the `rni_pci_join` / `rni_iri_join` macros |
| `dbt debug` | yes | connection only |

**`--empty` caveat specific to this project:** the target marts are `incremental` with
`delete+insert`. Under `--empty`, `is_incremental()` returns false if the relation does not
exist in the target schema, so dbt runs the model as **full-refresh → creates/drops tables**.
Pointed at the production mart schema that is destructive. Therefore `--empty` must target an
**isolated scratch schema** (a dedicated `--target` profile whose schema is `DBT_TEST` or
similar), never the real mart schema. The scratch schema is the dry-run environment; it is
created on the prod Oracle instance but is not read by any consumer/report.

Given the constraint, the E2E tier is split into two sub-tiers:

**17.3a CI dry-run gate (no real data; scratch schema on the Oracle instance)**

| # | Scenario | Assert |
|---|---|---|
| E1 | Clean host starts consumer with Docker + secrets + network | container reaches running state |
| E2 | `dbt debug --target scratch` inside image with mounted profile | exits 0 (connection + auth OK) |
| E3 | `dbt compile --select stg_rni_combined+ tag:iri tag:pci` | exits 0; all selectors resolve (20 models total); compiled SQL in `target/compiled/` (no DB needed — run in per-commit CI, not just here) |
| E4 | `dbt run --empty --target scratch --select <each routing key's selection>` with `--vars year/semester` | exits 0 for each selection; SQL executes against the scratch schema; no rows inserted; validates cols/types/refs against the real Oracle dictionary |
| E5 | dbt unit tests for `rni_pci_join` and `rni_iri_join` macros (1.8+ YAML, mocked rows) | pass; guards the §3.1 PCI macro fix against regression |
| E6 | Grep image layers + logs | no Oracle password / AMQP credentials present |

**17.3b True data E2E — deferred until a non-prod Oracle target exists**

| # | Scenario | Assert |
|---|---|---|
| ED1 | Publish one event per routing key (RNI/IRI/PCI) against a non-prod Oracle target | each acks; expected dbt selection runs; target tables update; idempotent rerun produces no duplicate rows |
| ED2 | Container killed during a dbt run | RabbitMQ redelivers; idempotent rerun acks; no duplicate rows |

Until ED1/ED2 are unblocked, 17.3a is the promotion gate. Running ED1 against the production
mart schema as a one-off (small semester, `full_refresh=False`) is possible but must be a
controlled, sign-off event — not an automated CI step.

### 17.4 Tooling notes

- Unit tier: `pytest` + `pytest-mock`; no broker, no dbt. Target: seconds.
- Integration tier: `pytest` + `testcontainers-python` for RabbitMQ. The RabbitMQ container
  (and its data volume) is spun up in a session/module fixture and **always torn down after
  the test run, even on failure** — use a `yield` fixture so the container's `stop()` and
  `del` run in the teardown regardless of test outcome, and disable any volume mount
  (`RABBITMQ_ENABLED_PLUGINS_FILE` / mnesia dir) so nothing persists on the host. No CI
  service container alternative — testcontainers is the only broker source in the suite.
  `dbtRunner` is monkeypatched to a stub returning canned `dbtRunnerResult`s unless the
  scenario explicitly exercises the heartbeat pump (I7).
- CI dbt tier (per-commit, no DB): `dbt parse` + `dbt compile` + dbt 1.8 unit tests for the
  join macros. These need no Oracle connection and catch Jinja/ref/regression-logic failures
  early. `dbt compile` does NOT validate column existence — only 17.3a E4 (`--empty`) does.
- CI selector gate (the §5 guard): run `dbt ls --select stg_rni_combined+`, `dbt ls --select
  tag:iri`, `dbt ls --select tag:pci` and fail the build if any is empty — catches a tag typo
  or renamed model at deploy time instead of first-event time.
- Dry-run gate (17.3a E4): requires Oracle network reachability + a scratch-schema profile
  (`DBT_TEST` or similar). Run pre-promotion, not per-commit. `--empty` still opens Oracle
  sessions — keep `threads: 1` and run selections sequentially.
- True E2E (17.3b): deferred until non-prod Oracle exists; blocked, not skipped.
- Jaeger assertions (I2/I5): query the collector/Jaeger API for a span with the expected
  `job_id` attribute; skip the assertion if `OTEL_EXPORTER_OTLP_ENDPOINT` is unset (dev).
- No `tenacity` — retry tests (U11–U13, I5) assert the broker-level retry path: republish to
  the retry exchange with `x-retry-count` + `expiration`, TTL-expiry back to the work queue,
  then DLQ via DLX after `RETRY_MAX`. Not in-process retry.
