from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    rabbitmq_url: str
    rabbitmq_exchange: str
    rabbitmq_queue: str
    rabbitmq_dlq: str
    rabbitmq_routing_keys: tuple[str, ...]
    dbt_project_dir: Path
    dbt_profiles_dir: Path
    retry_max: int
    log_dir: Path
    otel_endpoint: str | None
    otel_service_name: str
    otel_resource_attributes: str

    @property
    def dlx_exchange(self) -> str:
        return f"{self.rabbitmq_exchange}.dlx"

    @property
    def retry_exchange(self) -> str:
        return f"{self.rabbitmq_exchange}.retry"

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


def load_settings() -> Settings:
    load_dotenv()

    rabbitmq_url = _required("RABBITMQ_URL")
    retry_max = _int_field("RETRY_MAX", "3")

    routing_keys_raw = os.getenv(
        "RABBITMQ_ROUTING_KEYS", "verified.rni,verified.iri,verified.pci"
    )
    routing_keys = tuple(k.strip() for k in routing_keys_raw.split(",") if k.strip())

    otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or None

    return Settings(
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=os.getenv("RABBITMQ_EXCHANGE", "validation.events"),
        rabbitmq_queue=os.getenv("RABBITMQ_QUEUE", "dbt.events.worker"),
        rabbitmq_dlq=os.getenv("RABBITMQ_DLQ", "dbt.events.worker.dlq"),
        rabbitmq_routing_keys=routing_keys,
        dbt_project_dir=Path(os.getenv("DBT_PROJECT_DIR", "./events_analysis")),
        dbt_profiles_dir=Path(os.getenv("DBT_PROFILES_DIR", "~/.dbt")).expanduser(),
        retry_max=retry_max,
        log_dir=Path(os.getenv("LOG_DIR", "./logs")),
        otel_endpoint=otel_endpoint,
        otel_service_name=os.getenv("OTEL_SERVICE_NAME", "dbt-events-consumer"),
        otel_resource_attributes=os.getenv("OTEL_RESOURCE_ATTRIBUTES", ""),
    )
