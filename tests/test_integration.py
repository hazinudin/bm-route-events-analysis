import json
import logging
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import docker
import pika
import pytest
from testcontainers.community.rabbitmq import RabbitMqContainer

from dbt_events_consumer.config import Settings
from dbt_events_consumer.consumer import EventConsumer
from dbt_events_consumer.topology import declare_topology

RABBIT_IMAGE = "rabbitmq:3.13-management"


def _docker_available() -> bool:
    try:
        return docker.from_env().ping()
    except Exception:
        return False


@pytest.fixture(scope="module")
def rabbitmq():
    if not _docker_available():
        pytest.skip("Docker daemon is not available")
    container = RabbitMqContainer(RABBIT_IMAGE)
    container.start()
    yield container
    container.stop()
    del container


@pytest.fixture(scope="module")
def conn_params(rabbitmq):
    return rabbitmq.get_connection_params()


@pytest.fixture
def settings(make_settings, rabbitmq):
    host = rabbitmq.get_container_host_ip()
    port = rabbitmq.get_exposed_port(5672)
    url = f"amqp://guest:guest@{host}:{port}/%2F"
    return make_settings(rabbitmq_url=url, retry_max=1)


@pytest.fixture
def test_conn(conn_params):
    connection = pika.BlockingConnection(conn_params)
    yield connection
    try:
        connection.close()
    except Exception:
        pass


def _purge_all(test_conn, settings):
    """Purge all queues to ensure a clean state between tests."""
    ch = test_conn.channel()
    for queue in [
        settings.rabbitmq_queue,
        settings.retry_queue,
        settings.rabbitmq_dlq,
    ]:
        try:
            ch.queue_purge(queue)
        except Exception:
            pass
    ch.close()


def _setup_clean_state(test_conn, settings):
    """Declare topology then purge all queues for a clean test state."""
    declare_topology(test_conn, settings)
    _purge_all(test_conn, settings)


def _queue_depth(test_conn, queue_name: str) -> int:
    ch = test_conn.channel()
    method = ch.queue_declare(queue=queue_name, passive=True)
    count = method.method.message_count
    ch.close()
    return count


def _wait_for(
    test_conn: pika.BlockingConnection,
    queue_name: str,
    expected: int,
    timeout: float = 10,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _queue_depth(test_conn, queue_name) == expected:
            return True
        time.sleep(0.1)
    return False


def _publish(
    test_conn: pika.BlockingConnection,
    settings: Settings,
    routing_key: str,
    body: bytes,
    exchange: str | None = None,
    headers: dict[str, Any] | None = None,
) -> None:
    ch = test_conn.channel()
    ch.basic_publish(
        exchange=exchange or settings.rabbitmq_exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            message_id="test-msg-id",
            timestamp=int(time.time()),
            headers=headers,
        ),
    )
    ch.close()


class StubRunner:
    def __init__(self, result: Any = None, exception: Exception | None = None):
        self._result = result
        self._exception = exception
        self.call_count = 0
        self.last_args: list[str] | None = None

    def run(self, cli_args: list[str], connection: Any | None = None) -> Any:
        self.call_count += 1
        self.last_args = cli_args
        if self._exception:
            raise self._exception
        return self._result


class ConsumerThread:
    def __init__(self, conn_params, settings, runner, logger):
        self._conn_params = conn_params
        self._settings = settings
        self._runner = runner
        self._logger = logger
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self._thread: threading.Thread | None = None

    def start(self):
        self._connection = pika.BlockingConnection(self._conn_params)
        declare_topology(self._connection, self._settings)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        consumer = EventConsumer(self._settings, self._runner, self._logger)
        self._channel.basic_consume(
            queue=self._settings.rabbitmq_queue,
            on_message_callback=consumer.on_message,
            auto_ack=False,
        )
        self._thread = threading.Thread(target=self._channel.start_consuming, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10):
        if self._connection and self._connection.is_open and self._channel:
            self._connection.add_callback_threadsafe(
                self._channel.stop_consuming
            )
        if self._thread:
            self._thread.join(timeout=timeout)
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception:
                pass


def _make_result(success=True, node_count=1, exception=None):
    result = MagicMock()
    result.success = success
    result.exception = exception
    nodes = []
    for i in range(node_count):
        node = MagicMock()
        node.unique_id = f"model.test.model_{i}"
        node.status = "success"
        nodes.append(node)
    result.result = nodes
    return result


def _valid_body(routing_key="verified.rni", **overrides):
    body = {
        "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
        "routing_key": routing_key,
        "year": 2025,
        "semester": 2,
        "emitted_at": "2026-08-21T09:30:00Z",
    }
    body.update(overrides)
    return json.dumps(body).encode()


class TestI1TopologyDeclaration:
    def test_i1_topology_declared_on_startup(self, test_conn, settings, conn_params):
        connection = pika.BlockingConnection(conn_params)
        declare_topology(connection, settings)
        connection.close()

        ch = test_conn.channel()
        for exchange in [
            settings.rabbitmq_exchange,
            settings.dlx_exchange,
            settings.retry_exchange,
        ]:
            ch.exchange_declare(exchange=exchange, exchange_type="topic", passive=True)

        for queue in [
            settings.rabbitmq_queue,
            settings.retry_queue,
            settings.rabbitmq_dlq,
        ]:
            ch.queue_declare(queue=queue, passive=True)

        ch.close()


class TestI2ValidMessageAcked:
    def test_i2_valid_rni_acked(self, test_conn, settings, conn_params):
        _setup_clean_state(test_conn, settings)
        runner = StubRunner(result=_make_result(success=True, node_count=3))
        logger = logging.getLogger("test_i2")
        consumer_thread = ConsumerThread(conn_params, settings, runner, logger)
        consumer_thread.start()
        try:
            _publish(test_conn, settings, "verified.rni", _valid_body())
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=10)
            assert runner.call_count == 1
            assert "stg_rni_combined+" in runner.last_args
        finally:
            consumer_thread.stop()


class TestI3InvalidPayloadToDLQ:
    def test_i3_invalid_json_lands_in_dlq(self, test_conn, settings, conn_params):
        _setup_clean_state(test_conn, settings)
        runner = StubRunner(result=_make_result())
        logger = logging.getLogger("test_i3")
        consumer_thread = ConsumerThread(conn_params, settings, runner, logger)
        consumer_thread.start()
        try:
            _publish(test_conn, settings, "verified.rni", b"not valid json")
            assert _wait_for(test_conn, settings.rabbitmq_dlq, 1, timeout=10)
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=5)
            assert runner.call_count == 0
        finally:
            consumer_thread.stop()


class TestI4UnknownRoutingKeyToDLQ:
    def test_i4_unknown_key_lands_in_dlq(self, test_conn, settings, conn_params):
        _setup_clean_state(test_conn, settings)

        ch = test_conn.channel()
        ch.queue_bind(
            exchange=settings.rabbitmq_exchange,
            queue=settings.rabbitmq_queue,
            routing_key="verified.xyz",
        )
        ch.close()

        runner = StubRunner(result=_make_result())
        logger = logging.getLogger("test_i4")
        consumer_thread = ConsumerThread(conn_params, settings, runner, logger)
        consumer_thread.start()
        try:
            body = _valid_body(routing_key="verified.xyz")
            _publish(test_conn, settings, "verified.xyz", body)
            assert _wait_for(test_conn, settings.rabbitmq_dlq, 1, timeout=10)
            assert runner.call_count == 0
        finally:
            consumer_thread.stop()
            ch = test_conn.channel()
            ch.queue_unbind(
                exchange=settings.rabbitmq_exchange,
                queue=settings.rabbitmq_queue,
                routing_key="verified.xyz",
            )
            ch.close()


class TestI5TransientRetryThenDLQ:
    def test_i5_retry_then_dlq_after_max(self, test_conn, settings, conn_params):
        _setup_clean_state(test_conn, settings)
        runner = StubRunner(exception=RuntimeError("Oracle down"))
        logger = logging.getLogger("test_i5")
        consumer_thread = ConsumerThread(conn_params, settings, runner, logger)
        consumer_thread.start()
        try:
            _publish(test_conn, settings, "verified.rni", _valid_body())

            # First failure: republish to retry queue (backoff = 2^1 = 2s)
            assert _wait_for(test_conn, settings.retry_queue, 1, timeout=5)

            # Wait for retry TTL to expire (~2s + margin) — message returns to work queue
            assert _wait_for(test_conn, settings.retry_queue, 0, timeout=10)

            # Second failure: retry_count=1 >= RETRY_MAX=1 → DLQ
            assert _wait_for(test_conn, settings.rabbitmq_dlq, 1, timeout=10)
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=5)
            assert runner.call_count == 2
        finally:
            consumer_thread.stop(timeout=15)


class TestI7HeartbeatDuringLongRun:
    def test_i7_connection_survives_long_dbt_run(
        self, test_conn, settings, conn_params
    ):
        _setup_clean_state(test_conn, settings)

        class SlowRunner:
            def __init__(self):
                self.call_count = 0

            def run(self, cli_args, connection=None):
                self.call_count += 1
                time.sleep(3)
                return _make_result(success=True, node_count=1)

        runner = SlowRunner()
        logger = logging.getLogger("test_i7")

        conn_params_with_heartbeat = pika.ConnectionParameters(
            host=conn_params.host,
            port=conn_params.port,
            virtual_host=conn_params.virtual_host,
            credentials=conn_params.credentials,
            heartbeat=5,
        )

        consumer_thread = ConsumerThread(
            conn_params_with_heartbeat, settings, runner, logger
        )
        consumer_thread.start()
        try:
            _publish(test_conn, settings, "verified.rni", _valid_body())
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=15)
            assert runner.call_count == 1

            # Connection should still be alive after the 3s "dbt run"
            assert consumer_thread._connection is not None
            assert consumer_thread._connection.is_open
        finally:
            consumer_thread.stop()
