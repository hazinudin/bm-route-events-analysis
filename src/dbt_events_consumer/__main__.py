from __future__ import annotations

import signal
import sys
from typing import Any

import pika

from dbt_events_consumer.config import Settings, load_settings
from dbt_events_consumer.consumer import EventConsumer
from dbt_events_consumer.observability import setup_observability
from dbt_events_consumer.runner import DbtRunnerWrapper
from dbt_events_consumer.topology import TopologyError, declare_topology


def _check_dirs(settings: Settings, logger: Any) -> None:
    if not settings.dbt_project_dir.is_dir():
        logger.error(
            "startup check failed",
            extra={"check": "dbt_project_dir", "path": str(settings.dbt_project_dir)},
        )
        sys.exit(1)
    if not settings.dbt_profiles_dir.is_dir():
        logger.error(
            "startup check failed",
            extra={"check": "dbt_profiles_dir", "path": str(settings.dbt_profiles_dir)},
        )
        sys.exit(1)


def _mask_url(url: str) -> str:
    if "@" in url:
        scheme_part, rest = url.split("://", 1)
        creds, host = rest.split("@", 1)
        if ":" in creds:
            user = creds.split(":")[0]
        else:
            user = creds
        return f"{scheme_part}://{user}@{host}"
    return url


def main() -> None:
    settings = load_settings()
    logger = setup_observability(settings)

    _check_dirs(settings, logger)

    logger.info(
        "starting consumer",
        extra={
            "rabbitmq_url": _mask_url(settings.rabbitmq_url),
            "queue": settings.rabbitmq_queue,
            "exchange": settings.rabbitmq_exchange,
            "routing_keys": list(settings.rabbitmq_routing_keys),
            "retry_max": settings.retry_max,
            "otel_enabled": settings.otel_endpoint is not None,
        },
    )

    try:
        params = pika.URLParameters(settings.rabbitmq_url)
        connection = pika.BlockingConnection(params)
    except Exception as exc:
        logger.error(
            "failed to connect to RabbitMQ",
            extra={"detail": str(exc), "rabbitmq_url": _mask_url(settings.rabbitmq_url)},
        )
        sys.exit(1)

    try:
        declare_topology(connection, settings)
    except TopologyError as exc:
        logger.error("topology declaration failed", extra={"detail": str(exc)})
        connection.close()
        sys.exit(1)

    runner = DbtRunnerWrapper(settings)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    consumer = EventConsumer(settings, runner, logger)
    channel.basic_consume(
        queue=settings.rabbitmq_queue,
        on_message_callback=consumer.on_message,
        auto_ack=False,
    )

    stopping = False

    def on_signal(signum: int, frame: Any) -> None:
        nonlocal stopping
        if not stopping:
            stopping = True
            logger.info(
                "received signal, draining current message",
                extra={"signal": signal.Signals(signum).name},
            )
            channel.stop_consuming()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    logger.info("consumer ready, waiting for messages")

    try:
        channel.start_consuming()
    except Exception as exc:
        logger.error("consumer error", extra={"detail": str(exc)})
        raise
    finally:
        try:
            channel.close()
        except Exception:
            pass
        connection.close()
        logger.info("consumer stopped")


if __name__ == "__main__":
    main()
