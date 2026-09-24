import signal
from unittest.mock import MagicMock, patch

import pytest

from worker.app import _mask_url, run_worker
from worker.topology import TopologyError


def test_mask_url_hides_password():
    assert (
        _mask_url("amqp://guest:secret@localhost:5672/%2F")
        == "amqp://guest@localhost:5672/%2F"
    )


def test_mask_url_no_credentials():
    assert _mask_url("amqp://localhost:5672") == "amqp://localhost:5672"


@patch("worker.app.pika")
@patch("worker.app.declare_topology")
@patch("worker.app.setup_observability")
def test_run_worker_exits_on_connection_failure(
    mock_setup_obs, mock_declare, mock_pika, make_worker_settings
):
    settings = make_worker_settings()
    mock_setup_obs.return_value = MagicMock()
    mock_pika.URLParameters.return_value = MagicMock()
    mock_pika.BlockingConnection.side_effect = ConnectionError("broker down")

    with pytest.raises(SystemExit) as exc_info:
        run_worker(settings, MagicMock, logger_name="test")
    assert exc_info.value.code == 1


@patch("worker.app.pika")
@patch("worker.app.declare_topology")
@patch("worker.app.setup_observability")
def test_run_worker_exits_on_topology_error(
    mock_setup_obs, mock_declare, mock_pika, make_worker_settings
):
    settings = make_worker_settings()
    mock_setup_obs.return_value = MagicMock()

    mock_connection = MagicMock()
    mock_pika.BlockingConnection.return_value = mock_connection
    mock_pika.URLParameters.return_value = MagicMock()
    mock_declare.side_effect = TopologyError("arg mismatch")

    with pytest.raises(SystemExit) as exc_info:
        run_worker(settings, MagicMock, logger_name="test")
    assert exc_info.value.code == 1
    mock_connection.close.assert_called_once()


@patch("worker.app.signal")
@patch("worker.app.pika")
@patch("worker.app.declare_topology")
@patch("worker.app.setup_observability")
def test_run_worker_starts_and_stops_consuming(
    mock_setup_obs, mock_declare, mock_pika, mock_signal, make_worker_settings
):
    settings = make_worker_settings()
    mock_logger = MagicMock()
    mock_setup_obs.return_value = mock_logger

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_pika.BlockingConnection.return_value = mock_connection
    mock_pika.URLParameters.return_value = MagicMock()

    run_worker(settings, MagicMock, logger_name="test")

    mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
    mock_channel.basic_consume.assert_called_once()
    assert mock_channel.basic_consume.call_args.kwargs["queue"] == "events.worker"
    assert mock_channel.basic_consume.call_args.kwargs["auto_ack"] is False
    mock_channel.start_consuming.assert_called_once()
    mock_channel.close.assert_called_once()
    mock_connection.close.assert_called_once()
    assert mock_signal.signal.call_count == 2
    sigs = {c.args[0] for c in mock_signal.signal.call_args_list}
    assert sigs == {mock_signal.SIGTERM, mock_signal.SIGINT}
