import json
import logging
import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import docker
import pika
import pytest
from testcontainers.community.rabbitmq import RabbitMqContainer

from traffic.consumer.config import TrafficWorkerSettings, load_settings
from traffic.consumer.handler import TrafficMessageHandler
from worker.app import run_worker
from worker.topology import declare_topology

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
def settings(rabbitmq, tmp_path):
    host = rabbitmq.get_container_host_ip()
    port = rabbitmq.get_exposed_port(5672)
    return TrafficWorkerSettings(
        rabbitmq_url=f"amqp://guest:guest@{host}:{port}/%2F",
        rabbitmq_exchange="validation.events",
        rabbitmq_queue="traffic.events.worker",
        rabbitmq_dlq="traffic.events.worker.dlq",
        rabbitmq_routing_keys=("verified.rtc",),
        retry_max=1,
        log_dir=tmp_path,
        otel_endpoint=None,
        otel_service_name="traffic-events-consumer",
        otel_resource_attributes="",
        traffic_target_table="SMD.AADT",
        oracle_host="db.example.com",
        oracle_port=1521,
        oracle_service="SVC",
        oracle_user="u",
        oracle_password="p",
        oracle_client_dir=None,
    )


@pytest.fixture
def test_conn(conn_params):
    connection = pika.BlockingConnection(conn_params)
    yield connection
    try:
        connection.close()
    except Exception:
        pass


def _purge_all(test_conn, settings):
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
    settings: TrafficWorkerSettings,
    body: bytes,
    headers: dict[str, Any] | None = None,
) -> None:
    ch = test_conn.channel()
    ch.basic_publish(
        exchange=settings.rabbitmq_exchange,
        routing_key="verified.rtc",
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


def _valid_body(**overrides):
    body = {
        "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
        "routing_key": "verified.rtc",
        "year": 2024,
        "semester": 2,
        "routes": ["01001", "01002"],
        "emitted_at": "2026-08-21T09:30:00Z",
    }
    body.update(overrides)
    return json.dumps(body).encode()


@pytest.fixture
def patched_oracle():
    """Patch Oracle connectivity so the test needs only RabbitMQ."""
    with patch("traffic.consumer.handler.oracledb.connect") as mock_connect, patch(
        "traffic.consumer.handler.AADTPipeline"
    ) as mock_pipeline_cls, patch(
        "traffic.consumer.handler.write_aadt_results"
    ) as mock_writer:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_pipeline = MagicMock()
        mock_pipeline.calculate.return_value = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_writer.return_value = 2
        yield {
            "connect": mock_connect,
            "pipeline": mock_pipeline,
            "writer": mock_writer,
        }


class TrafficConsumerThread:
    def __init__(self, conn_params, settings, logger):
        self._conn_params = conn_params
        self._settings = settings
        self._logger = logger
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self._thread: threading.Thread | None = None

    def start(self):
        self._connection = pika.BlockingConnection(self._conn_params)
        declare_topology(self._connection, self._settings)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        handler = TrafficMessageHandler(self._settings)
        from worker.consumer import EventConsumer

        consumer = EventConsumer(
            self._settings,
            handler,
            self._logger,
            tracer_name="traffic_events_consumer",
        )
        self._channel.basic_consume(
            queue=self._settings.rabbitmq_queue,
            on_message_callback=consumer.on_message,
            auto_ack=False,
        )
        self._thread = threading.Thread(
            target=self._channel.start_consuming, daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 10):
        if self._connection and self._connection.is_open and self._channel:
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)
        if self._thread:
            self._thread.join(timeout=timeout)
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception:
                pass


class TestT1TrafficTopologyDeclaration:
    def test_t1_topology_declared_on_startup(self, test_conn, settings, conn_params):
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


class TestT2ValidTrafficMessageAcked:
    def test_t2_valid_rtc_acked(
        self, test_conn, settings, conn_params, patched_oracle
    ):
        _setup_clean_state(test_conn, settings)
        logger = logging.getLogger("test_t2")
        consumer_thread = TrafficConsumerThread(conn_params, settings, logger)
        consumer_thread.start()
        try:
            _publish(test_conn, settings, _valid_body())
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=10)
            patched_oracle["pipeline"].calculate.assert_called_once_with(
                routes=["01001", "01002"], year=2024, semester=2
            )
            patched_oracle["writer"].assert_called_once()
        finally:
            consumer_thread.stop()


class TestT3InvalidPayloadToDLQ:
    def test_t3_invalid_json_lands_in_dlq(
        self, test_conn, settings, conn_params, patched_oracle
    ):
        _setup_clean_state(test_conn, settings)
        logger = logging.getLogger("test_t3")
        consumer_thread = TrafficConsumerThread(conn_params, settings, logger)
        consumer_thread.start()
        try:
            _publish(test_conn, settings, b"not valid json")
            assert _wait_for(test_conn, settings.rabbitmq_dlq, 1, timeout=10)
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=5)
            patched_oracle["pipeline"].calculate.assert_not_called()
        finally:
            consumer_thread.stop()


class TestT4TransientRetryThenDLQ:
    def test_t4_retry_then_dlq_after_max(
        self, test_conn, settings, conn_params, patched_oracle
    ):
        import oracledb

        _setup_clean_state(test_conn, settings)
        patched_oracle["pipeline"].calculate.side_effect = oracledb.DatabaseError(
            "Oracle down"
        )
        logger = logging.getLogger("test_t4")
        consumer_thread = TrafficConsumerThread(conn_params, settings, logger)
        consumer_thread.start()
        try:
            _publish(test_conn, settings, _valid_body())

            # First failure: republish to retry queue (backoff = 2^1 = 2s)
            assert _wait_for(test_conn, settings.retry_queue, 1, timeout=5)

            # Wait for retry TTL to expire
            assert _wait_for(test_conn, settings.retry_queue, 0, timeout=10)

            # Second failure: retry_count=1 >= RETRY_MAX=1 -> DLQ
            assert _wait_for(test_conn, settings.rabbitmq_dlq, 1, timeout=10)
            assert _wait_for(test_conn, settings.rabbitmq_queue, 0, timeout=5)
            assert patched_oracle["pipeline"].calculate.call_count == 2
        finally:
            consumer_thread.stop(timeout=15)
