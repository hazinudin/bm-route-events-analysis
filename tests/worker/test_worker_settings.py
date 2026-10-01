import pytest

from worker.settings import WorkerSettings, load_worker_settings


def test_missing_rabbitmq_url_raises(clean_env):
    with pytest.raises(ValueError, match="RABBITMQ_URL"):
        load_worker_settings()


def test_non_integer_retry_max_raises(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    clean_env.setenv("RETRY_MAX", "notanint")
    with pytest.raises(ValueError, match="RETRY_MAX"):
        load_worker_settings()


def test_defaults(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    settings = load_worker_settings()
    assert settings.rabbitmq_url == "amqp://guest:guest@localhost:5672/%2F"
    assert settings.rabbitmq_exchange == "validation.events"
    assert settings.rabbitmq_queue == "events.worker"
    assert settings.rabbitmq_dlq == "events.worker.dlq"
    assert settings.rabbitmq_routing_keys == ()
    assert settings.retry_max == 3
    assert settings.otel_service_name == "events-consumer"


def test_default_routing_keys(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    settings = load_worker_settings(default_routing_keys=("a", "b"))
    assert settings.rabbitmq_routing_keys == ("a", "b")


def test_routing_keys_override_defaults(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_env.setenv("RABBITMQ_ROUTING_KEYS", "x,y")
    settings = load_worker_settings(default_routing_keys=("a", "b"))
    assert settings.rabbitmq_routing_keys == ("x", "y")


def test_derived_topology_names(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    settings = load_worker_settings()
    assert settings.dlx_exchange == "events.worker.dlx"
    assert settings.retry_exchange == "events.worker.retry.exchange"
    assert settings.retry_queue == "events.worker.retry"


def test_otel_endpoint_disabled_when_empty(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    settings = load_worker_settings()
    assert settings.otel_endpoint is None


def test_otel_endpoint_set(clean_env):
    clean_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    settings = load_worker_settings()
    assert settings.otel_endpoint == "http://collector:4317"


def test_worker_settings_is_frozen():
    settings = WorkerSettings(
        rabbitmq_url="amqp://x",
        rabbitmq_exchange="ex",
        rabbitmq_queue="q",
        rabbitmq_dlq="q.dlq",
        rabbitmq_routing_keys=(),
        retry_max=1,
        log_dir="/tmp",
        otel_endpoint=None,
        otel_service_name="svc",
        otel_resource_attributes="",
    )
    with pytest.raises(AttributeError):
        settings.retry_max = 5
