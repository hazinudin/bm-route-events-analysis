from __future__ import annotations

from typing import Any

from pika.exceptions import AMQPError

from worker.settings import WorkerSettings


class TopologyError(Exception):
    pass


def declare_topology(connection: Any, settings: WorkerSettings) -> None:
    """
    Trigger all exchanges and queues declaration in RabbitMQ.
    """
    channel = connection.channel()
    try:
        _declare_exchanges(channel, settings)
        _declare_queues(channel, settings)
        _bind_queues(channel, settings)
    except AMQPError as exc:
        raise TopologyError(
            f"Failed to declare RabbitMQ topology (possible out-of-band "
            f"arg mismatch on an existing queue): {exc}"
        ) from exc
    finally:
        channel.close()


def _declare_exchanges(channel: Any, settings: WorkerSettings) -> None:
    """
    Declares all exchanges.
    """
    channel.exchange_declare(
        exchange=settings.rabbitmq_exchange,
        exchange_type="topic",
        durable=True,
    )
    channel.exchange_declare(
        exchange=settings.dlx_exchange,
        exchange_type="topic",
        durable=True,
    )
    channel.exchange_declare(
        exchange=settings.retry_exchange,
        exchange_type="topic",
        durable=True,
    )


def _declare_queues(channel: Any, settings: WorkerSettings) -> None:
    """
    Declares all queues along with its argument.
    """
    channel.queue_declare(
        queue=settings.rabbitmq_queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": settings.dlx_exchange,
            "x-dead-letter-routing-key": "dead",
        },
    )

    channel.queue_declare(
        queue=settings.retry_queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": settings.rabbitmq_exchange,
        },
    )

    channel.queue_declare(
        queue=settings.rabbitmq_dlq,
        durable=True,
        arguments={},
    )


def _bind_queues(channel: Any, settings: WorkerSettings) -> None:
    """
    Bind queues to the exchange.
    """
    for routing_key in settings.rabbitmq_routing_keys:
        channel.queue_bind(
            exchange=settings.rabbitmq_exchange,
            queue=settings.rabbitmq_queue,
            routing_key=routing_key,
        )

    channel.queue_bind(
        exchange=settings.retry_exchange,
        queue=settings.retry_queue,
        routing_key="#",  # Matches any routing key
    )

    channel.queue_bind(
        exchange=settings.dlx_exchange,
        queue=settings.rabbitmq_dlq,
        routing_key="dead",
    )
