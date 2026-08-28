import logging
from pathlib import Path

import pytest

from dbt_events_consumer.config import Settings

CONFIG_ENV_VARS = [
    "RABBITMQ_URL",
    "RABBITMQ_EXCHANGE",
    "RABBITMQ_QUEUE",
    "RABBITMQ_DLQ",
    "RABBITMQ_ROUTING_KEYS",
    "DBT_PROJECT_DIR",
    "DBT_PROFILES_DIR",
    "RETRY_MAX",
    "LOG_DIR",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
]


@pytest.fixture
def clean_env(monkeypatch):
    for var in CONFIG_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def make_settings(tmp_path):
    def _make(**overrides):
        defaults = dict(
            rabbitmq_url="amqp://guest:guest@localhost:5672/%2F",
            rabbitmq_exchange="validation.events",
            rabbitmq_queue="dbt.events.worker",
            rabbitmq_dlq="dbt.events.worker.dlq",
            rabbitmq_routing_keys=(
                "verified.rni",
                "verified.iri",
                "verified.pci",
            ),
            dbt_project_dir=Path("./events_analysis"),
            dbt_profiles_dir=Path("~/.dbt").expanduser(),
            retry_max=3,
            log_dir=tmp_path,
            otel_endpoint=None,
            otel_service_name="dbt-events-consumer",
            otel_resource_attributes="",
        )
        defaults.update(overrides)
        return Settings(**defaults)

    return _make


@pytest.fixture(autouse=True)
def clean_logger():
    yield
    logger = logging.getLogger("dbt_events_consumer")
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
