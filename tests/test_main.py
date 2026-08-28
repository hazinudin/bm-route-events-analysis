import signal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dbt_events_consumer.__main__ import _check_dirs, _mask_url, main


def test_mask_url_hides_password():
    assert (
        _mask_url("amqp://guest:secret@localhost:5672/%2F")
        == "amqp://guest@localhost:5672/%2F"
    )


def test_mask_url_no_credentials():
    assert _mask_url("amqp://localhost:5672") == "amqp://localhost:5672"


def test_mask_url_user_only():
    assert (
        _mask_url("amqp://guest@localhost:5672")
        == "amqp://guest@localhost:5672"
    )


def test_check_dirs_passes(make_settings, tmp_path):
    import logging

    settings = make_settings(
        dbt_project_dir=tmp_path,
        dbt_profiles_dir=tmp_path,
    )
    logger = logging.getLogger("test_check_dirs")
    _check_dirs(settings, logger)


def test_check_dirs_fails_on_missing_project_dir(make_settings, tmp_path):
    settings = make_settings(
        dbt_project_dir=Path("/nonexistent/path/xyz"),
        dbt_profiles_dir=tmp_path,
    )
    with pytest.raises(SystemExit) as exc_info:
        _check_dirs(settings, MagicMock())
    assert exc_info.value.code == 1


def test_check_dirs_fails_on_missing_profiles_dir(make_settings, tmp_path):
    settings = make_settings(
        dbt_project_dir=tmp_path,
        dbt_profiles_dir=Path("/nonexistent/path/xyz"),
    )
    with pytest.raises(SystemExit) as exc_info:
        _check_dirs(settings, MagicMock())
    assert exc_info.value.code == 1


@patch("dbt_events_consumer.__main__.pika")
@patch("dbt_events_consumer.__main__.declare_topology")
@patch("dbt_events_consumer.__main__.DbtRunnerWrapper")
@patch("dbt_events_consumer.__main__.setup_observability")
@patch("dbt_events_consumer.__main__.load_settings")
def test_main_exits_on_topology_error(
    mock_load, mock_setup_obs, mock_runner_cls, mock_declare, mock_pika,
    make_settings, tmp_path,
):
    from dbt_events_consumer.topology import TopologyError

    settings = make_settings(
        dbt_project_dir=tmp_path,
        dbt_profiles_dir=tmp_path,
    )
    mock_load.return_value = settings
    mock_setup_obs.return_value = MagicMock()

    mock_connection = MagicMock()
    mock_pika.BlockingConnection.return_value = mock_connection
    mock_pika.URLParameters.return_value = MagicMock()
    mock_declare.side_effect = TopologyError("arg mismatch")

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1
    mock_connection.close.assert_called_once()


@patch("dbt_events_consumer.__main__.pika")
@patch("dbt_events_consumer.__main__.declare_topology")
@patch("dbt_events_consumer.__main__.DbtRunnerWrapper")
@patch("dbt_events_consumer.__main__.setup_observability")
@patch("dbt_events_consumer.__main__.load_settings")
def test_main_exits_on_connection_failure(
    mock_load, mock_setup_obs, mock_runner_cls, mock_declare, mock_pika,
    make_settings, tmp_path,
):
    settings = make_settings(
        dbt_project_dir=tmp_path,
        dbt_profiles_dir=tmp_path,
    )
    mock_load.return_value = settings
    mock_setup_obs.return_value = MagicMock()

    mock_pika.URLParameters.return_value = MagicMock()
    mock_pika.BlockingConnection.side_effect = ConnectionError("broker down")

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1


@patch("dbt_events_consumer.__main__.pika")
@patch("dbt_events_consumer.__main__.declare_topology")
@patch("dbt_events_consumer.__main__.DbtRunnerWrapper")
@patch("dbt_events_consumer.__main__.setup_observability")
@patch("dbt_events_consumer.__main__.load_settings")
def test_main_starts_and_stops_consuming(
    mock_load, mock_setup_obs, mock_runner_cls, mock_declare, mock_pika,
    make_settings, tmp_path,
):
    settings = make_settings(
        dbt_project_dir=tmp_path,
        dbt_profiles_dir=tmp_path,
    )
    mock_load.return_value = settings
    mock_logger = MagicMock()
    mock_setup_obs.return_value = mock_logger

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_pika.BlockingConnection.return_value = mock_connection
    mock_pika.URLParameters.return_value = MagicMock()

    main()

    mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
    mock_channel.basic_consume.assert_called_once()
    assert mock_channel.basic_consume.call_args.kwargs["queue"] == "dbt.events.worker"
    assert mock_channel.basic_consume.call_args.kwargs["auto_ack"] is False
    mock_channel.start_consuming.assert_called_once()
    mock_channel.close.assert_called_once()
    mock_connection.close.assert_called_once()
