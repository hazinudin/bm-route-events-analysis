from pathlib import Path

import pytest

from traffic.consumer.config import TrafficWorkerSettings

TRAFFIC_ENV_VARS = [
    "RABBITMQ_URL",
    "RABBITMQ_EXCHANGE",
    "RABBITMQ_QUEUE",
    "RABBITMQ_DLQ",
    "RABBITMQ_ROUTING_KEYS",
    "RETRY_MAX",
    "LOG_DIR",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
    "TRAFFIC_TARGET_TABLE",
    "ORACLE_HOST",
    "ORACLE_PORT",
    "ORACLE_SERVICE",
    "ORACLE_USER",
    "ORACLE_PASSWORD",
    "ORACLE_CLIENT_DIR",
    "DBT_PROFILES_DIR",
]


@pytest.fixture
def clean_traffic_env(monkeypatch):
    for var in TRAFFIC_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def make_traffic_settings(tmp_path):
    def _make(**overrides):
        defaults = dict(
            rabbitmq_url="amqp://guest:guest@localhost:5672/%2F",
            rabbitmq_exchange="validation.events",
            rabbitmq_queue="traffic.events.worker",
            rabbitmq_dlq="traffic.events.worker.dlq",
            rabbitmq_routing_keys=("verified.rtc",),
            retry_max=3,
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
        defaults.update(overrides)
        return TrafficWorkerSettings(**defaults)

    return _make
