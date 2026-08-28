import time
from unittest.mock import MagicMock, patch

import pytest

from dbt_events_consumer.runner import DbtRunnerWrapper


def test_run_adds_project_and_profiles_dir(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)

    with patch.object(wrapper._runner, "invoke") as mock_invoke:
        mock_invoke.return_value = MagicMock(success=True)
        wrapper.run(["run", "--select", "stg_rni_combined+"])

    args = mock_invoke.call_args.args[0]
    assert "--project-dir" in args
    assert str(settings.dbt_project_dir) in args
    assert "--profiles-dir" in args
    assert str(settings.dbt_profiles_dir) in args
    assert "run" in args
    assert "--select" in args
    assert "stg_rni_combined+" in args


def test_run_returns_invoke_result(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)

    expected = MagicMock(success=True)
    with patch.object(wrapper._runner, "invoke", return_value=expected):
        result = wrapper.run(["run", "--select", "tag:iri"])

    assert result is expected


def test_run_without_connection_invokes_directly(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)

    with patch.object(wrapper._runner, "invoke") as mock_invoke:
        mock_invoke.return_value = MagicMock(success=True)
        wrapper.run(["run", "--select", "tag:iri"], connection=None)

    mock_invoke.assert_called_once()


def test_run_pumps_heartbeats_with_connection(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)
    connection = MagicMock()

    def slow_invoke(args):
        time.sleep(0.3)
        return MagicMock(success=True)

    with patch.object(wrapper._runner, "invoke", side_effect=slow_invoke):
        wrapper.run(["run", "--select", "tag:iri"], connection=connection)

    assert connection.process_data_events.call_count >= 1


def test_run_propagates_exception_from_worker(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)
    connection = MagicMock()

    with patch.object(wrapper._runner, "invoke", side_effect=RuntimeError("FFI crash")):
        with pytest.raises(RuntimeError, match="FFI crash"):
            wrapper.run(["run", "--select", "tag:iri"], connection=connection)


def test_run_does_not_mutate_input_args(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)

    original = ["run", "--select", "tag:pci"]
    with patch.object(wrapper._runner, "invoke") as mock_invoke:
        mock_invoke.return_value = MagicMock(success=True)
        wrapper.run(original)

    assert original == ["run", "--select", "tag:pci"]


def test_run_with_full_refresh_flag(make_settings):
    settings = make_settings()
    wrapper = DbtRunnerWrapper(settings)

    with patch.object(wrapper._runner, "invoke") as mock_invoke:
        mock_invoke.return_value = MagicMock(success=True)
        wrapper.run(["run", "--select", "stg_rni_combined+", "--full-refresh"])

    args = mock_invoke.call_args.args[0]
    assert "--full-refresh" in args
