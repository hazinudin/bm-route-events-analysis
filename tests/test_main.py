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


@patch("dbt_events_consumer.__main__.run_worker")
@patch("dbt_events_consumer.__main__.load_settings")
def test_main_delegates_to_run_worker(
    mock_load, mock_run_worker, make_settings, tmp_path,
):
    settings = make_settings(
        dbt_project_dir=tmp_path,
        dbt_profiles_dir=tmp_path,
    )
    mock_load.return_value = settings

    main()

    mock_run_worker.assert_called_once()
    args, kwargs = mock_run_worker.call_args
    assert args[0] is settings
    assert callable(args[1])
    assert kwargs["logger_name"] == "dbt_events_consumer"
    assert kwargs["startup_checks"] is not None
