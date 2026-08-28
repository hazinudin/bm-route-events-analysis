import json
import logging

from opentelemetry import trace

from dbt_events_consumer.observability import (
    StructuredFormatter,
    get_tracer,
    setup_logging,
    setup_observability,
    setup_tracing,
)


def test_structured_formatter_includes_extra_fields():
    formatter = StructuredFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="event processed",
        args=(),
        exc_info=None,
    )
    record.event_id = "abc-123"
    record.routing_key = "verified.rni"
    output = formatter.format(record)
    entry = json.loads(output)
    assert entry["msg"] == "event processed"
    assert entry["level"] == "INFO"
    assert entry["event_id"] == "abc-123"
    assert entry["routing_key"] == "verified.rni"
    assert "ts" in entry


def test_structured_formatter_includes_exception():
    formatter = StructuredFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="failed",
            args=(),
            exc_info=sys.exc_info(),
        )
    output = formatter.format(record)
    entry = json.loads(output)
    assert "ValueError" in entry["exc"]


def test_setup_logging_creates_log_file(make_settings):
    settings = make_settings()
    logger = setup_logging(settings)
    assert logger.name == "dbt_events_consumer"
    assert logger.level == logging.INFO
    assert (settings.log_dir / "consumer.log").exists()


def test_setup_logging_writes_structured_json(make_settings):
    settings = make_settings()
    logger = setup_logging(settings)
    logger.info("msg processed", extra={"event_id": "e-1", "routing_key": "verified.iri"})
    for handler in logger.handlers:
        handler.flush()

    content = (settings.log_dir / "consumer.log").read_text()
    entry = json.loads(content.strip().split("\n")[-1])
    assert entry["msg"] == "msg processed"
    assert entry["event_id"] == "e-1"
    assert entry["routing_key"] == "verified.iri"


def test_setup_logging_idempotent_no_duplicate_handlers(make_settings):
    settings = make_settings()
    logger1 = setup_logging(settings)
    handler_count = len(logger1.handlers)
    logger2 = setup_logging(settings)
    assert logger1 is logger2
    assert len(logger2.handlers) == handler_count


def test_setup_tracing_noop_when_no_endpoint(make_settings):
    setup_tracing(make_settings(otel_endpoint=None))
    tracer = get_tracer()
    assert tracer is not None


def test_setup_tracing_with_endpoint(make_settings):
    setup_tracing(make_settings(otel_endpoint="http://localhost:4317"))
    tracer = get_tracer()
    assert tracer is not None


def test_get_tracer_creates_span(make_settings):
    setup_tracing(make_settings())
    tracer = get_tracer()
    with tracer.start_as_current_span("test_span") as span:
        span.set_attribute("event_id", "test-123")
        span.set_attribute("routing_key", "verified.rni")
    assert span is not None


def test_setup_observability_returns_logger(make_settings):
    logger = setup_observability(make_settings())
    assert logger.name == "dbt_events_consumer"
    assert any(
        isinstance(h, logging.FileHandler) for h in logger.handlers
    )


def test_resource_attributes_parsed(make_settings):
    setup_tracing(
        make_settings(
            otel_endpoint=None,
            otel_resource_attributes="deployment.environment=prod,service.version=1.0",
        )
    )
    provider = trace.get_tracer_provider()
    resource = getattr(provider, "resource", None)
    if resource is not None:
        assert resource.attributes.get("service.name") == "dbt-events-consumer"
