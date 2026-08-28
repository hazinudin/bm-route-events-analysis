import pytest

from dbt_events_consumer.config import load_settings


def test_missing_rabbitmq_url_raises(clean_env):
    with pytest.raises(ValueError, match="RABBITMQ_URL"):
        load_settings()


def test_non_integer_retry_max_raises(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    clean_env.setenv("RETRY_MAX", "notanint")
    with pytest.raises(ValueError, match="RETRY_MAX"):
        load_settings()


def test_defaults(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    settings = load_settings()
    assert settings.rabbitmq_url == "amqp://guest:guest@localhost:5672/%2F"
    assert settings.rabbitmq_exchange == "validation.events"
    assert settings.rabbitmq_queue == "dbt.events.worker"
    assert settings.rabbitmq_dlq == "dbt.events.worker.dlq"
    assert settings.rabbitmq_routing_keys == (
        "verified.rni",
        "verified.iri",
        "verified.pci",
    )
    assert settings.retry_max == 3
    assert settings.otel_service_name == "dbt-events-consumer"


def test_derived_topology_names(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    settings = load_settings()
    assert settings.dlx_exchange == "validation.events.dlx"
    assert settings.retry_exchange == "validation.events.retry"
    assert settings.retry_queue == "dbt.events.worker.retry"


def test_otel_endpoint_disabled_when_empty(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    settings = load_settings()
    assert settings.otel_endpoint is None


def test_otel_endpoint_disabled_when_unset(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    settings = load_settings()
    assert settings.otel_endpoint is None


def test_otel_endpoint_set(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    settings = load_settings()
    assert settings.otel_endpoint == "http://collector:4317"


def test_routing_keys_parsed_from_csv(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_env.setenv("RABBITMQ_ROUTING_KEYS", "verified.rni, verified.iri , verified.pci")
    settings = load_settings()
    assert settings.rabbitmq_routing_keys == (
        "verified.rni",
        "verified.iri",
        "verified.pci",
    )
