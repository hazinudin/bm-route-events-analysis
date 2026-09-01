from __future__ import annotations

import json
import logging
import time
from typing import Any, Protocol

import pika
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

from dbt_events_consumer.config import Settings
from dbt_events_consumer.observability import get_tracer
from dbt_events_consumer.runner import DbtRunnerWrapper
from dbt_events_consumer.schema import TriggerMessage

ROUTING_TO_SELECT: dict[str, str] = {
    "verified.rni": "stg_rni_combined+",
    "verified.iri": "tag:iri",
    "verified.pci": "tag:pci",
}

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
        settings: Settings,
        runner: DbtRunnerWrapper,
        logger: logging.Logger,
    ) -> None:
        self._settings = settings
        self._runner = runner
        self._logger = logger
        self._tracer = get_tracer()

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

            event_id = msg.event_id
            span.set_attribute("event_id", event_id)

            if not self._validate_routing(
                channel, delivery_tag, routing_key, msg, span,
            ):
                return

            self._run_message(
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
            event_id = self._extract_event_id(body)
            self._reject_message(
                channel, delivery_tag, span, event_id, routing_key,
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
        if routing_key not in ROUTING_TO_SELECT:
            self._reject_message(
                channel, delivery_tag, span, msg.event_id, routing_key,
                "unknown_routing_key",
                f"no dbt selection for routing key {routing_key!r}",
            )
            return False

        if msg.routing_key != routing_key:
            self._reject_message(
                channel, delivery_tag, span, msg.event_id, routing_key,
                "invalid_payload",
                f"envelope routing key {routing_key!r} != body "
                f"routing_key {msg.routing_key!r}",
            )
            return False

        return True

    def _run_message(
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
        selection = ROUTING_TO_SELECT[routing_key]
        span.set_attribute("dbt.selection", selection)
        span.set_attribute("dbt.year", msg.year)
        span.set_attribute("dbt.semester", msg.semester)

        try:
            result = self._runner.run(
                self._build_cli_args(selection, msg),
                connection=channel.connection,
            )
        except Exception as exc:
            self._handle_transient(
                channel, delivery_tag, properties, body,
                routing_key, msg.event_id, exc, span,
            )
            return

        duration = time.monotonic() - start
        span.set_attribute("dbt.duration_s", round(duration, 3))
        self._handle_result(
            channel, delivery_tag, properties, body, routing_key,
            selection, msg.event_id, result, span, duration,
        )

    def _handle_result(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        properties: pika.spec.BasicProperties,
        body: bytes,
        routing_key: str,
        selection: str,
        event_id: str,
        result: Any,
        span: Any,
        duration: float,
    ) -> None:
        node_results = result.result or []
        node_count = len(node_results)

        if result.success and node_count == 0:
            self._reject_message(
                channel, delivery_tag, span, event_id, routing_key,
                "empty_selection",
                f"selection {selection!r} produced 0 models",
                duration=duration,
            )
            return

        if not result.success and result.exception is None:
            statuses = self._collect_node_statuses(node_results)
            self._reject_message(
                channel, delivery_tag, span, event_id, routing_key,
                "dbt_run_failed",
                f"dbt run failed; node statuses: {statuses}",
                duration=duration,
                node_count=node_count,
            )
            return

        if result.exception is not None:
            self._handle_transient(
                channel, delivery_tag, properties, body, routing_key,
                event_id, result.exception, span,
            )
            return

        self._logger.info(
            "dbt run succeeded",
            extra={
                "event_id": event_id,
                "routing_key": routing_key,
                "selection": selection,
                "duration_s": round(duration, 3),
                "node_count": node_count,
            },
        )
        span.set_attribute("dbt.node_count", node_count)
        channel.basic_ack(delivery_tag=delivery_tag)

    def _reject_message(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        span: Any,
        event_id: str | None,
        routing_key: str,
        reason: str,
        detail: str,
        duration: float | None = None,
        node_count: int | None = None,
        exception: BaseException | None = None,
    ) -> None:
        self._log_failure(
            event_id, routing_key, reason, detail, duration=duration,
        )
        span.set_status(Status(StatusCode.ERROR, reason))
        if exception is not None:
            span.record_exception(exception)
        span.set_attribute("dlq_reason", reason)
        if node_count is not None:
            span.set_attribute("dbt.node_count", node_count)
        channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

    def _parse_body(self, body: bytes, routing_key: str) -> TriggerMessage:
        payload = json.loads(body)
        return TriggerMessage(**payload)

    def _extract_event_id(self, body: bytes) -> str | None:
        try:
            return json.loads(body).get("event_id")
        except (json.JSONDecodeError, TypeError):
            return None

    def _build_cli_args(self, selection: str, msg: TriggerMessage) -> list[str]:
        vars_dict: dict[str, Any] = {
            "year": msg.year,
            "semester": msg.semester,
        }
        if msg.routes:
            vars_dict["routes"] = msg.routes

        cli_args = [
            "run",
            "--select",
            selection,
            "--vars",
            json.dumps(vars_dict),
        ]
        if msg.full_refresh:
            cli_args.append("--full-refresh")
        return cli_args

    def _handle_transient(
        self,
        channel: RabbitChannel,
        delivery_tag: int,
        properties: pika.spec.BasicProperties,
        body: bytes,
        routing_key: str,
        event_id: str | None,
        exc: Exception,
        span: Any,
    ) -> None:
        retry_count = self._read_retry_count(properties)
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc)))

        if retry_count >= self._settings.retry_max:
            self._log_failure(
                event_id, routing_key, "retry_saturated",
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
                "event_id": event_id,
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

    def _collect_node_statuses(self, node_results: list[Any]) -> dict[str, str]:
        statuses: dict[str, str] = {}
        for node in node_results:
            uid = getattr(node, "unique_id", str(node))
            status = getattr(node, "status", "unknown")
            statuses[uid] = str(status)
        return statuses

    def _log_failure(
        self,
        event_id: str | None,
        routing_key: str,
        reason: str,
        detail: str,
        duration: float | None = None,
    ) -> None:
        extra: dict[str, Any] = {
            "event_id": event_id,
            "routing_key": routing_key,
            "dlq_reason": reason,
            "detail": detail,
        }
        if duration is not None:
            extra["duration_s"] = round(duration, 3)
        self._logger.error("message rejected", extra=extra)
