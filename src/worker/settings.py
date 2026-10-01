from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class WorkerSettings:
    rabbitmq_url: str
    rabbitmq_exchange: str
    rabbitmq_queue: str
    rabbitmq_dlq: str
    rabbitmq_routing_keys: tuple[str, ...]
    retry_max: int
    log_dir: Path
    otel_endpoint: str | None
    otel_service_name: str
    otel_resource_attributes: str

    @property
    def dlx_exchange(self) -> str:
        # Per-worker DLX. The main exchange is shared with the publisher, but
        # the dead-letter/retry exchanges must be scoped to this worker's queue
        # so two workers on the same broker don't cross-route each other's
        # dead-letters and retries.
        return f"{self.rabbitmq_queue}.dlx"

    @property
    def retry_exchange(self) -> str:
        return f"{self.rabbitmq_queue}.retry.exchange"

    @property
    def retry_queue(self) -> str:
        return f"{self.rabbitmq_queue}.retry"


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required but not set")
    return value


def _int_field(name: str, default: str) -> int:
    raw = os.getenv(name, default)
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _routing_keys(name: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(k.strip() for k in raw.split(",") if k.strip())


def load_worker_settings(
    *,
    default_queue: str = "events.worker",
    default_dlq: str = "events.worker.dlq",
    default_routing_keys: tuple[str, ...] = (),
    default_service_name: str = "events-consumer",
) -> WorkerSettings:
    load_dotenv()

    rabbitmq_url = _required("RABBITMQ_URL")
    retry_max = _int_field("RETRY_MAX", "3")

    routing_keys_default = ",".join(default_routing_keys)
    routing_keys = _routing_keys("RABBITMQ_ROUTING_KEYS", routing_keys_default)

    otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or None

    return WorkerSettings(
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=os.getenv("RABBITMQ_EXCHANGE", "validation.events"),
        rabbitmq_queue=os.getenv("RABBITMQ_QUEUE", default_queue),
        rabbitmq_dlq=os.getenv("RABBITMQ_DLQ", default_dlq),
        rabbitmq_routing_keys=routing_keys,
        retry_max=retry_max,
        log_dir=Path(os.getenv("LOG_DIR", "./logs")),
        otel_endpoint=otel_endpoint,
        otel_service_name=os.getenv("OTEL_SERVICE_NAME", default_service_name),
        otel_resource_attributes=os.getenv("OTEL_RESOURCE_ATTRIBUTES", ""),
    )
