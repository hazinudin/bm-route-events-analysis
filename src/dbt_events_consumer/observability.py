from worker.observability import (
    StructuredFormatter,
    get_tracer as _get_tracer,
    setup_logging as _setup_logging,
    setup_observability as _setup_observability,
    setup_tracing,
)

_LOGGER_NAME = "dbt_events_consumer"


def setup_logging(settings):
    return _setup_logging(settings, _LOGGER_NAME)


def setup_observability(settings):
    return _setup_observability(settings, _LOGGER_NAME)


def get_tracer():
    return _get_tracer(_LOGGER_NAME)


__all__ = [
    "StructuredFormatter",
    "get_tracer",
    "setup_logging",
    "setup_observability",
    "setup_tracing",
]
