from __future__ import annotations

import json
import logging
import time
from typing import Any

import pika
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
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        delivery_tag = method.delivery_tag
        routing_key = method.routing_key
        event_id: str | None = None

        with self._tracer.start_as_current_span("consume_message") as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute("messaging.destination", routing_key)

            start = time.monotonic()
            try:
                msg = self._parse_body(body, routing_key)
            except (ValidationError, json.JSONDecodeError, TypeError) as exc:
                event_id = self._extract_event_id(body)
                self._log_failure(event_id, routing_key, "invalid_payload", str(exc))
                span.set_attribute("dlq_reason", "invalid_payload")
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                return

            event_id = msg.event_id
            span.set_attribute("event_id", event_id)

            if routing_key not in ROUTING_TO_SELECT:
                self._log_failure(
                    event_id, routing_key, "unknown_routing_key",
                    f"no dbt selection for routing key {routing_key!r}",
                )
                span.set_attribute("dlq_reason", "unknown_routing_key")
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                return

            if msg.routing_key != routing_key:
                self._log_failure(
                    event_id, routing_key, "invalid_payload",
                    f"envelope routing key {routing_key!r} != body "
                    f"routing_key {msg.routing_key!r}",
                )
                span.set_attribute("dlq_reason", "invalid_payload")
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                return

            selection = ROUTING_TO_SELECT[routing_key]
            span.set_attribute("dbt.selection", selection)
            span.set_attribute("dbt.year", msg.year)
            span.set_attribute("dbt.semester", msg.semester)

            cli_args = self._build_cli_args(selection, msg)

            try:
                result = self._runner.run(cli_args, connection=channel.connection)
            except Exception as exc:
                self._handle_transient(
                    channel, delivery_tag, properties, body,
                    routing_key, event_id, exc, span,
                )
                return

            duration = time.monotonic() - start
            span.set_attribute("dbt.duration_s", round(duration, 3))

            node_results = result.result or []
            node_count = len(node_results)

            if result.success and node_count == 0:
                self._log_failure(
                    event_id, routing_key, "empty_selection",
                    f"selection {selection!r} produced 0 models",
                    duration=duration,
                )
                span.set_attribute("dlq_reason", "empty_selection")
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                return

            if not result.success and result.exception is None:
                statuses = self._collect_node_statuses(node_results)
                self._log_failure(
                    event_id, routing_key, "dbt_run_failed",
                    f"dbt run failed; node statuses: {statuses}",
                    duration=duration,
                )
                span.set_attribute("dlq_reason", "dbt_run_failed")
                span.set_attribute("dbt.node_count", node_count)
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                return

            if result.exception is not None:
                self._handle_transient(
                    channel, delivery_tag, properties, body,
                    routing_key, event_id, result.exception, span,
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
        channel: pika.adapters.blocking_connection.BlockingChannel,
        delivery_tag: int,
        properties: pika.spec.BasicProperties,
        body: bytes,
        routing_key: str,
        event_id: str | None,
        exc: Exception,
        span: Any,
    ) -> None:
        retry_count = self._read_retry_count(properties)

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


def start_consuming(
    connection: pika.BlockingConnection,
    settings: Settings,
    runner: DbtRunnerWrapper,
    logger: logging.Logger,
) -> None:
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    consumer = EventConsumer(settings, runner, logger)
    channel.basic_consume(
        queue=settings.rabbitmq_queue,
        on_message_callback=consumer.on_message,
        auto_ack=False,
    )
    logger.info(
        "consumer started",
        extra={
            "queue": settings.rabbitmq_queue,
            "routing_keys": list(settings.rabbitmq_routing_keys),
        },
    )
    channel.start_consuming()
