import pytest

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
