from unittest.mock import MagicMock

import pytest
from pika.exceptions import ChannelClosedByBroker

from dbt_events_consumer.topology import TopologyError, declare_topology


@pytest.fixture
def mock_channel():
    connection = MagicMock()
    channel = MagicMock()
    connection.channel.return_value = channel
    return connection, channel


def _extract_queue_args(channel):
    return {
        call.kwargs["queue"]: call.kwargs
        for call in channel.queue_declare.call_args_list
    }


def test_declares_three_topic_exchanges(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    names = [c.kwargs["exchange"] for c in ch.exchange_declare.call_args_list]
    assert "validation.events" in names
    assert "dbt.events.worker.dlx" in names
    assert "dbt.events.worker.retry.exchange" in names
    for c in ch.exchange_declare.call_args_list:
        assert c.kwargs["exchange_type"] == "topic"
        assert c.kwargs["durable"] is True


def test_work_queue_has_dlx_and_dead_routing_key(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    q = _extract_queue_args(ch)["dbt.events.worker"]
    assert q["durable"] is True
    assert q["arguments"]["x-dead-letter-exchange"] == "dbt.events.worker.dlx"
    assert q["arguments"]["x-dead-letter-routing-key"] == "dead"


def test_retry_queue_dlx_points_to_main_exchange(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    q = _extract_queue_args(ch)["dbt.events.worker.retry"]
    assert q["durable"] is True
    assert q["arguments"]["x-dead-letter-exchange"] == "validation.events"
    assert "x-dead-letter-routing-key" not in q["arguments"]


def test_dlq_is_plain_durable_queue(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    q = _extract_queue_args(ch)["dbt.events.worker.dlq"]
    assert q["durable"] is True
    assert q["arguments"] == {}


def test_work_queue_bound_to_main_exchange_with_all_routing_keys(
    mock_channel, make_settings
):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    work_binds = [
        c
        for c in ch.queue_bind.call_args_list
        if c.kwargs["queue"] == "dbt.events.worker"
    ]
    assert len(work_binds) == 3
    rks = {c.kwargs["routing_key"] for c in work_binds}
    assert rks == {"verified.rni", "verified.iri", "verified.pci"}
    for c in work_binds:
        assert c.kwargs["exchange"] == "validation.events"


def test_retry_queue_bound_with_wildcard(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    retry_binds = [
        c
        for c in ch.queue_bind.call_args_list
        if c.kwargs["queue"] == "dbt.events.worker.retry"
    ]
    assert len(retry_binds) == 1
    assert retry_binds[0].kwargs["exchange"] == "dbt.events.worker.retry.exchange"
    assert retry_binds[0].kwargs["routing_key"] == "#"


def test_dlq_bound_to_dlx_with_dead_key(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())

    dlq_binds = [
        c
        for c in ch.queue_bind.call_args_list
        if c.kwargs["queue"] == "dbt.events.worker.dlq"
    ]
    assert len(dlq_binds) == 1
    assert dlq_binds[0].kwargs["exchange"] == "dbt.events.worker.dlx"
    assert dlq_binds[0].kwargs["routing_key"] == "dead"


def test_channel_closed_after_declare(mock_channel, make_settings):
    conn, ch = mock_channel
    declare_topology(conn, make_settings())
    ch.close.assert_called_once()


def test_broker_error_wrapped_as_topology_error(mock_channel, make_settings):
    conn, ch = mock_channel
    ch.queue_declare.side_effect = ChannelClosedByBroker(
        406, "PRECONDITION_FAILED"
    )

    with pytest.raises(TopologyError, match="PRECONDITION_FAILED"):
        declare_topology(conn, make_settings())


def test_custom_names_propagate_to_topology(mock_channel, make_settings):
    conn, ch = mock_channel
    settings = make_settings(
        rabbitmq_exchange="custom.events",
        rabbitmq_queue="custom.worker",
        rabbitmq_dlq="custom.dlq",
    )
    declare_topology(conn, settings)

    queue_names = {c.kwargs["queue"] for c in ch.queue_declare.call_args_list}
    assert queue_names == {"custom.worker", "custom.worker.retry", "custom.dlq"}

    q = _extract_queue_args(ch)["custom.worker"]
    assert q["arguments"]["x-dead-letter-exchange"] == "custom.worker.dlx"
