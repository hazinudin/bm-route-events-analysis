from __future__ import annotations

import json
import logging
import time
from typing import Any, Protocol

import pika
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

from worker.handler import MessageHandler
from worker.observability import get_tracer
from worker.outcomes import (
    HandlerOutcome,
    PermanentFailure,
    Success,
    TransientFailure,
)
from worker.schema import TriggerMessage
from worker.settings import WorkerSettings

RETRY_HEADER = "x-retry-count"


class RabbitChannel(Protocol):
    """Subset of the Pika channel API used by the event consumer."""

    connection: Any

    def basic_ack(self, *, delivery_tag: int) -> None: ...

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None: ...

    def basic_publish(
        self,
        *,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties: pika.spec.BasicProperties,
    ) -> Any: ...


class EventConsumer:
    def __init__(
        self,
        settings: WorkerSettings,
        handler: MessageHandler,
        logger: logging.Logger,
        tracer_name: str = "events_consumer",
    ) -> None:
        self._settings = settings
        self._handler = handler
        self._logger = logger
        self._tracer = get_tracer(tracer_name)

    def on_message(
        self,
        channel: RabbitChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        delivery_tag = method.delivery_tag
        routing_key = method.routing_key

        with self._tracer.start_as_current_span("consume_message") as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination", routing_key)

            start = time.monotonic()
            msg = self._parse_or_reject(
                channel, delivery_tag, routing_key, body, span,
            )
            if msg is None:
                return

            job_id = msg.job_id
            span.set_attribute("job_id", job_id)

            if not self._validate_routing(
                channel, delivery_tag, routing_key, msg, span,
            ):
                return

            self._handle_message(
                channel, delivery_tag, properties, body, routing_key,
                msg, span, start,
            )

    def _parse_or_reject(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        routing_key: str,
        body: bytes,
        span: Any,
    ) -> TriggerMessage | None:
        try:
            return self._parse_body(body, routing_key)
        except (ValidationError, json.JSONDecodeError, TypeError) as exc:
            job_id = self._extract_job_id(body)
            self._reject_message(
                channel, delivery_tag, span, job_id, routing_key,
                "invalid_payload", str(exc),
                exception=exc,
            )
            return None

    def _validate_routing(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        routing_key: str,
        msg: TriggerMessage,
        span: Any,
    ) -> bool:
        if routing_key not in self._settings.rabbitmq_routing_keys:
            self._reject_message(
                channel, delivery_tag, span, msg.job_id, routing_key,
                "unknown_routing_key",
                f"routing key {routing_key!r} not in "
                f"{list(self._settings.rabbitmq_routing_keys)}",
            )
            return False

        if msg.routing_key != routing_key:
            self._reject_message(
                channel, delivery_tag, span, msg.job_id, routing_key,
                "invalid_payload",
                f"envelope routing key {routing_key!r} != body "
                f"routing_key {msg.routing_key!r}",
            )
            return False

        return True

    def _handle_message(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        properties: pika.spec.BasicProperties,
        body: bytes,
        routing_key: str,
        msg: TriggerMessage,
        span: Any,
        start: float,
    ) -> None:
        try:
            outcome = self._handler.handle(
                msg, routing_key, channel.connection,
            )
        except Exception as exc:
            self._handle_transient(
                channel, delivery_tag, properties, body,
                routing_key, msg.job_id, exc, span,
            )
            return

        duration = time.monotonic() - start
        self._handle_outcome(
            channel, delivery_tag, properties, body, routing_key,
            msg, outcome, span, duration,
        )

    def _handle_outcome(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        properties: pika.spec.BasicProperties,
        body: bytes,
        routing_key: str,
        msg: TriggerMessage,
        outcome: HandlerOutcome,
        span: Any,
        duration: float,
    ) -> None:
        if isinstance(outcome, Success):
            self._logger.info(
                "message handled",
                extra={
                    "job_id": msg.job_id,
                    "routing_key": routing_key,
                    "duration_s": round(duration, 3),
                    **(outcome.metrics or {}),
                },
            )
            span.set_attribute("duration_s", round(duration, 3))
            for key, value in (outcome.metrics or {}).items():
                span.set_attribute(key, value)
            channel.basic_ack(delivery_tag=delivery_tag)
            return

        if isinstance(outcome, PermanentFailure):
            self._reject_message(
                channel, delivery_tag, span, msg.job_id, routing_key,
                outcome.reason, outcome.detail,
                duration=duration,
                metrics=outcome.metrics,
            )
            return

        if isinstance(outcome, TransientFailure):
            self._handle_transient(
                channel, delivery_tag, properties, body, routing_key,
                msg.job_id, outcome.exc, span,
            )
            return

        # Defensive fallback: unknown outcome type is treated as transient so
        # the message is not silently lost.
        self._handle_transient(
            channel, delivery_tag, properties, body, routing_key,
            msg.job_id,
            RuntimeError(f"unknown handler outcome: {outcome!r}"),
            span,
        )

    def _reject_message(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        span: Any,
        job_id: str | None,
        routing_key: str,
        reason: str,
        detail: str,
        duration: float | None = None,
        metrics: dict[str, Any] | None = None,
        exception: BaseException | None = None,
    ) -> None:
        self._log_failure(
            job_id, routing_key, reason, detail, duration=duration,
            metrics=metrics,
        )
        span.set_status(Status(StatusCode.ERROR, reason))
        if exception is not None:
            span.record_exception(exception)
        span.set_attribute("dlq_reason", reason)
        if metrics:
            for key, value in metrics.items():
                span.set_attribute(key, value)
        channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

    def _parse_body(self, body: bytes, routing_key: str) -> TriggerMessage:
        payload = json.loads(body)
        return TriggerMessage(**payload)

    def _extract_job_id(self, body: bytes) -> str | None:
        try:
            return json.loads(body).get("job_id")
        except (json.JSONDecodeError, TypeError):
            return None

    def _handle_transient(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        properties: pika.spec.BasicProperties,
        body: bytes,
        routing_key: str,
        job_id: str | None,
        exc: Exception,
        span: Any,
    ) -> None:
        retry_count = self._read_retry_count(properties)
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc)))

        if retry_count >= self._settings.retry_max:
            self._log_failure(
                job_id, routing_key, "retry_saturated",
                f"transient failures exhausted after {retry_count} retries: {exc}",
            )
            span.set_attribute("dlq_reason", "retry_saturated")
            span.set_attribute("retry_count", retry_count)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
            return

        next_retry = retry_count + 1
        expiration_ms = str((2 ** next_retry) * 1000)

        headers = dict(properties.headers or {})
        headers[RETRY_HEADER] = next_retry

        channel.basic_publish(
            exchange=self._settings.retry_exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type=properties.content_type,
                content_encoding=properties.content_encoding,
                delivery_mode=2,
                message_id=properties.message_id,
                timestamp=properties.timestamp,
                expiration=expiration_ms,
                headers=headers,
            ),
        )

        self._logger.warning(
            "transient failure, republishing for retry",
            extra={
                "job_id": job_id,
                "routing_key": routing_key,
                "retry_count": next_retry,
                "backoff_ms": expiration_ms,
                "error": str(exc),
            },
        )
        span.set_attribute("retry_count", next_retry)
        span.add_event("republish_for_retry", {"backoff_ms": expiration_ms})

        channel.basic_ack(delivery_tag=delivery_tag)

    def _read_retry_count(self, properties: pika.spec.BasicProperties) -> int:
        if properties.headers and RETRY_HEADER in properties.headers:
            return int(properties.headers[RETRY_HEADER])
        return 0

    def _log_failure(
        self,
        job_id: str | None,
        routing_key: str,
        reason: str,
        detail: str,
        duration: float | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        extra: dict[str, Any] = {
            "job_id": job_id,
            "routing_key": routing_key,
            "dlq_reason": reason,
            "detail": detail,
        }
        if duration is not None:
            extra["duration_s"] = round(duration, 3)
        if metrics:
            extra.update(metrics)
        self._logger.error("message rejected", extra=extra)
