import logging
from pathlib import Path

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from worker import observability as observability_module
from worker.settings import WorkerSettings

WORKER_ENV_VARS = [
    "RABBITMQ_URL",
    "RABBITMQ_EXCHANGE",
    "RABBITMQ_QUEUE",
    "RABBITMQ_DLQ",
    "RABBITMQ_ROUTING_KEYS",
    "RETRY_MAX",
    "LOG_DIR",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
]


@pytest.fixture
def clean_env(monkeypatch):
    for var in WORKER_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def make_worker_settings(tmp_path):
    def _make(**overrides):
        defaults = dict(
            rabbitmq_url="amqp://guest:guest@localhost:5672/%2F",
            rabbitmq_exchange="validation.events",
            rabbitmq_queue="events.worker",
            rabbitmq_dlq="events.worker.dlq",
            rabbitmq_routing_keys=("verified.rni",),
            retry_max=3,
            log_dir=tmp_path,
            otel_endpoint=None,
            otel_service_name="events-consumer",
            otel_resource_attributes="",
        )
        defaults.update(overrides)
        return WorkerSettings(**defaults)

    return _make


@pytest.fixture(autouse=True)
def clean_worker_logger():
    yield
    logger = logging.getLogger("events_consumer")
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def reset_tracing_state():
    # OpenTelemetry disallows replacing the global TracerProvider by default;
    # reset the private guard so each test starts with a fresh provider.
    trace._TRACER_PROVIDER_SET_ONCE._done = False  # type: ignore[attr-defined]
    observability_module._tracing_initialized = False
    yield
    trace._TRACER_PROVIDER_SET_ONCE._done = False  # type: ignore[attr-defined]
    observability_module._tracing_initialized = False
