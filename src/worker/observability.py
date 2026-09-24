from __future__ import annotations

import json
import logging
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from worker.settings import WorkerSettings

_tracing_initialized = False

_STANDARD_LOGRECORD_KEYS = frozenset(
    {
        "name",
        "msg",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "args",
        "taskName",
        "message",
    }
)


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOGRECORD_KEYS:
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging(settings: WorkerSettings, logger_name: str) -> logging.Logger:
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = StructuredFormatter()

    file_handler = logging.FileHandler(settings.log_dir / "consumer.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def setup_tracing(settings: WorkerSettings) -> None:
    global _tracing_initialized
    if _tracing_initialized:
        return
    _tracing_initialized = True

    resource_attrs: dict[str, str] = {
        "service.name": settings.otel_service_name,
    }
    if settings.otel_resource_attributes:
        for pair in settings.otel_resource_attributes.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                resource_attrs[k.strip()] = v.strip()

    resource = Resource.create(resource_attrs)
    provider = TracerProvider(resource=resource)

    if settings.otel_endpoint:
        exporter = OTLPSpanExporter(endpoint=settings.otel_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)


def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


def setup_observability(settings: WorkerSettings, logger_name: str) -> logging.Logger:
    setup_tracing(settings)
    return setup_logging(settings, logger_name)
