# Plan: Shared Worker Framework + Traffic Consumer

## Goal

Extract the generic RabbitMQ consumer machinery from `src/dbt_events_consumer/` into a reusable `src/worker/` framework, then build a new `src/traffic/consumer/` worker on top of it. Both workers share the same message topology, retry semantics, observability, and graceful-shutdown behavior while keeping their domain logic separate.

## Decisions

| Item | Decision |
|---|---|
| Shared package | `src/worker/` |
| Traffic worker location | `src/traffic/consumer/` |
| Message source | Same `validation.events` topic exchange; traffic binds only to `verified.rtc` |
| Post-calculation action | Write `AADTPipeline` result DataFrame to an Oracle target table |
| Packaging | Plain `src/` package (no separate workspace member) |
| Env vars | Shared names (`RABBITMQ_URL`, `RETRY_MAX`, etc.); each worker reads from its own process env |

## Current state analysis

`src/dbt_events_consumer/` mixes two concerns:

| Module | Concern | Reusable? |
|---|---|---|
| `topology.py` | RabbitMQ topology declaration (DLX/retry pattern) | 100% |
| `schema.py` | `TriggerMessage` pydantic model | 100% |
| `observability.py` | Structured logging + OpenTelemetry tracing setup | ~95% (parameterize logger name) |
| `consumer.py` | Ack/nack/retry/backoff, parse/validate, transient handling | ~70% (extract routing/result specifics via handler protocol) |
| `__main__.py` | Connect, declare, signal handling, consume loop | ~85% (startup checks are worker-specific) |
| `runner.py` | dbt-specific runner | dbt-specific, but the heartbeat-pump pattern is generic |
| `config.py` | Mixed: RabbitMQ fields shared; dbt dirs worker-specific | split |

The traffic side (`src/traffic/vcr_aadt_adt/`) is a synchronous, long-running Oracle job — the same shape as a dbt run and therefore needs the same heartbeat-pump pattern.

## Proposed structure

```text
src/
  worker/                              # generic RabbitMQ worker framework
    __init__.py
    settings.py                        # WorkerSettings + load_worker_settings()
    schema.py                          # TriggerMessage
    topology.py                        # declare_topology()
    observability.py                   # setup_observability(settings)
    db.py                              # SQLAlchemy engine factory (Oracle-focused but generic)
    outcomes.py                        # Success / PermanentFailure / TransientFailure
    handler.py                         # MessageHandler Protocol
    consumer.py                        # EventConsumer: parse → validate → handler → ack/nack/retry
    heartbeat.py                       # run_with_heartbeat(fn, connection)
    app.py                             # run_worker(settings, handler_factory, startup_checks)

  dbt_events_consumer/                 # refactored to a thin domain worker
    __init__.py
    __main__.py                        # entrypoint
    config.py                          # DbtWorkerSettings(WorkerSettings) + dbt dirs
    handler.py                         # routing → dbt selector → dbtRunner → Outcome
    runner.py                          # DbtRunnerWrapper using worker.heartbeat

  traffic/
    vcr_aadt_adt/                      # existing AADT/VCR pipeline + NEW result writer
      writer.py                        # DataFrame → Oracle upsert/insert helper
    consumer/                          # NEW traffic worker
      __init__.py
      __main__.py
      config.py                        # TrafficWorkerSettings + target table
      handler.py                       # AADTPipeline + result writer → Outcome
```

## Core abstraction: `MessageHandler → Outcome`

The framework owns all transport concerns. Workers only implement domain execution.

```python
class MessageHandler(Protocol):
    def handle(
        self,
        msg: TriggerMessage,
        routing_key: str,
        connection: Any,
    ) -> HandlerOutcome: ...
```

`EventConsumer` performs the following for each delivery:

1. Parse JSON and validate with pydantic. Failure → `basic_nack(requeue=False)` with reason `invalid_payload`.
2. Check that `method.routing_key` is in `settings.rabbitmq_routing_keys`. Failure → reason `unknown_routing_key`.
3. Check that body `routing_key` matches the envelope. Failure → reason `invalid_payload`.
4. Call `handler.handle(msg, routing_key, connection)`.
5. Map the returned outcome:
   - `Success(metrics)` → `basic_ack`
   - `PermanentFailure(reason, detail, metrics)` → `basic_nack(requeue=False)` (broker DLX → DLQ)
   - `TransientFailure(exc)` → republish to the retry exchange with `x-retry-count + 1` and per-message TTL `2**n` seconds, then `basic_ack` the original; if saturated → `basic_nack(requeue=False)` with reason `retry_saturated`

## Worker-specific handlers

### `DbtMessageHandler`

- Maintains the internal `ROUTING_TO_SELECT` map:
  - `verified.rni` → `stg_rni_combined+`
  - `verified.iri` → `tag:iri`
  - `verified.pci` → `tag:pci`
- Builds CLI args with `--vars` and optional `--full-refresh`.
- Runs `dbtRunner.invoke()` inside `worker.heartbeat.run_with_heartbeat` so RabbitMQ heartbeats survive long dbt runs.
- Interprets `dbtRunnerResult` including the empty-selection guard (`success=True` but zero nodes is a permanent failure).

### `TrafficMessageHandler`

- Accepts only `verified.rtc` (enforced by its `RABBITMQ_ROUTING_KEYS` configuration).
- Calls `AADTPipeline(connection=worker_oracle_conn).calculate(routes=msg.routes, year=msg.year, semester=msg.semester)`.
- Writes the returned DataFrame to the configured Oracle target table via `traffic/vcr_aadt_adt/writer.py`.
- Oracle connection errors / heartbeat failures → `TransientFailure`.
- Data/schema errors → `PermanentFailure`.

## Oracle connection for the traffic worker

`vcr_aadt_adt/db_conn.py` opens the Oracle connection at **module import time** by reading `SMD_Package/.env`. This is acceptable for the existing calculator but unsuitable for a supervised worker, because:

- The worker cannot connect to RabbitMQ and fail cleanly if Oracle config is missing.
- It forces a dependency on `SMD_Package/.env` in the container.

Therefore:

- Keep `vcr_aadt_adt` backward-compatible for existing callers.
- Do **not** import `vcr_aadt_adt.db_conn` from the worker.
- `traffic/consumer/config.py` loads Oracle credentials from environment variables or from `~/.dbt/profiles.yml` (the same pattern used in `scripts/export_*.py`).
- `TrafficMessageHandler` creates its own `oracledb.connect(...)` and passes it explicitly to `AADTPipeline(connection=...)`.

## Shared database connection factory (`worker/db.py`)

Move the Oracle connection creation logic from `traffic/vcr_aadt_adt/db_conn.py` into a reusable `worker/db.py` module built on SQLAlchemy `create_engine()`.

```python
# worker/db.py
from sqlalchemy import create_engine

def create_oracle_engine(
    *,
    host: str,
    port: str | int,
    service: str,
    user: str,
    password: str,
    client_dir: str | None = None,
) -> Engine:
    ...
```

URL format: `oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service}`.

If thick mode is needed, `worker/db.py` calls `oracledb.init_oracle_client(client_dir)` before creating the engine (mirroring the fallback logic in the current `db_conn.py`).

### Compatibility with `vcr_aadt_adt`

`vcr_aadt_adt/db_conn.py` becomes a thin backward-compatible wrapper:

```python
from worker.db import create_oracle_engine

_engine = None

def get_smd_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_oracle_engine(...)
    return _engine

def get_smd_connection():
    return get_smd_engine().raw_connection()

smd_connection = get_smd_connection()
```

`engine.raw_connection()` returns a DB-API connection that supports `.cursor()` and `.commit()`, so `vcr_aadt_adt/db_context.py` and `pd.read_sql(..., con=self.connection)` continue to work unchanged.

Future modules can import `create_oracle_engine` directly from `worker.db` instead of relying on the global `smd_connection`.

## Result writer

The result writer lives in `traffic/vcr_aadt_adt/writer.py` because persistence is a domain concern, not a messaging concern. Keeping it in the pipeline package makes `vcr_aadt_adt` a complete library (compute + persist) and keeps `traffic/consumer/` focused only on transport.

```python
def write_aadt_results(
    connection,
    df: pd.DataFrame,
    year: int,
    semester: int,
    routes: list[str] | None,
    target_table: str,
) -> int:
    ...
```

Default target table via env `TRAFFIC_TARGET_TABLE` (suggested: `SMD.AADT`). Strategy:

1. Delete existing rows for `(year, semester)`, narrowed by `routes` when provided.
2. Insert the new DataFrame rows.
3. Commit and return row count.

This keeps the same idempotency guarantee as the dbt worker: redeliveries and retry copies are safe.

## Configuration

`WorkerSettings` holds only shared fields:

- `rabbitmq_url`
- `rabbitmq_exchange`
- `rabbitmq_queue`
- `rabbitmq_dlq`
- `rabbitmq_routing_keys`
- `retry_max`
- `log_dir`
- `otel_endpoint`
- `otel_service_name`
- `otel_resource_attributes`

Worker-specific settings extend the base:

```python
@dataclass(frozen=True)
class DbtWorkerSettings(WorkerSettings):
    dbt_project_dir: Path
    dbt_profiles_dir: Path

@dataclass(frozen=True)
class TrafficWorkerSettings(WorkerSettings):
    traffic_target_table: str
    # Oracle credentials loaded from env / profiles at startup
```

Both workers read the same RabbitMQ/retry/log/OTel env var names, so each runs with its own `.env` file.

## Tests

1. **Framework tests** (`tests/worker/`):
   - Consumer outcome mapping with a fake handler (retry/backoff/DLQ branches)
   - Topology declaration
   - Observability setup
   - Schema validation
   - App signal handling and graceful shutdown
   - `create_oracle_engine()` URL building and thick-mode fallback

2. **dbt worker tests** (`tests/dbt_events_consumer/`):
   - Selection map and CLI arg building
   - Empty-selection guard
   - Runner heartbeat pump

3. **Traffic domain tests** (`tests/traffic/vcr_aadt_adt/`):
   - Writer delete/insert logic with a mocked cursor
   - Writer idempotency for `(year, semester, routes)`

4. **Traffic worker tests** (`tests/traffic/consumer/`):
   - Outcome mapping with a mocked `AADTPipeline` and writer
   - Oracle connection error → transient
   - Config loading

## Docker and deployment

### Docker images

Use separate Dockerfiles per worker. Both share the same base image and dependency sync step, but copy different source trees and set worker-specific environment defaults.

```text
Dockerfile                # dbt worker image
Dockerfile.traffic        # traffic worker image
```

- `Dockerfile` copies `src/worker/`, `src/dbt_events_consumer/`, and `events_analysis/`.
- `Dockerfile.traffic` copies `src/worker/`, `src/traffic/vcr_aadt_adt/`, and `src/traffic/consumer/`.
- Both use `python:3.11-slim-bookworm` and `uv sync --frozen --no-dev --no-install-project` from the shared `pyproject.toml`/`uv.lock`.

### Compose deployment

Update `deploy/compose.yaml` to add a `traffic-events-consumer` service alongside the existing `dbt-events-consumer`:

```yaml
services:
  dbt-events-consumer:
    image: hazinuddin/dbt-route-events-worker:${IMAGE_TAG}
    # ... existing service unchanged ...

  traffic-events-consumer:
    image: hazinuddin/traffic-route-events-worker:${IMAGE_TAG}
    restart: unless-stopped
    init: true
    stop_grace_period: 30m
    env_file:
      - .env
    environment:
      RABBITMQ_QUEUE: traffic.events.worker
      RABBITMQ_DLQ: traffic.events.worker.dlq
      RABBITMQ_ROUTING_KEYS: verified.rtc
      LOG_DIR: /var/log/traffic-events
      TRAFFIC_TARGET_TABLE: SMD.AADT
    networks:
      - backend
    volumes:
      - traffic-logs:/var/log/traffic-events
    read_only: true
    tmpfs:
      - /tmp

volumes:
  dbt-logs:
  dbt-target:
  consumer-logs:
  traffic-logs:
```

Oracle credentials for the traffic worker are supplied through the same `.env` file or Docker secrets mechanism used by the dbt worker; they are not committed to the image or Compose file.

## Implementation order

1. Create `src/worker/` and extract the generic modules from `dbt_events_consumer` with no behavior change.
2. Add `worker/db.py` with the SQLAlchemy `create_oracle_engine()` factory.
3. Refactor `dbt_events_consumer` to use `worker`; verify all existing tests pass.
4. Refactor `traffic/vcr_aadt_adt/db_conn.py` to wrap `worker.db` while preserving the existing `smd_connection` API.
5. Add `traffic/vcr_aadt_adt/writer.py` for DataFrame → Oracle persistence.
6. Build `src/traffic/consumer/` with config and handler.
7. Add traffic domain and worker tests.
8. Add `Dockerfile.traffic` and update `deploy/compose.yaml`.
9. Update `README.md` and any deployment docs.

## Open item

Confirm the exact Oracle target table and primary-key columns for the AADT/VCR result. `AADTPipeline` outputs `LINKID, VCR, VOLUME, CAPACITY, NUM_VEH1..NUM_VEH7C, CESA, AADT`. The proposed default is `SMD.AADT` with delete/insert keyed by `(YEAR, SEMESTER, LINKID)`.
