from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from traffic.consumer.handler import TrafficMessageHandler
from worker.outcomes import PermanentFailure, Success, TransientFailure
from worker.schema import TriggerMessage


def _make_message(**overrides):
    defaults = {
        "job_id": "job-1",
        "routing_key": "verified.rtc",
        "year": 2024,
        "semester": 2,
        "routes": ["01001", "01002"],
        "emitted_at": "2026-08-21T09:30:00Z",
    }
    defaults.update(overrides)
    return TriggerMessage(**defaults)


class TestTrafficMessageHandler:
    @pytest.fixture
    def handler(self, make_traffic_settings):
        return TrafficMessageHandler(make_traffic_settings())

    def test_success_writes_results(self, handler, make_traffic_settings):
        df = pd.DataFrame({"LINKID": ["01001"], "AADT": [100]})

        with patch("traffic.consumer.handler.oracledb.connect") as mock_connect, patch(
            "traffic.consumer.handler.AADTPipeline"
        ) as mock_pipeline_cls, patch(
            "traffic.consumer.handler.write_aadt_results"
        ) as mock_writer:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            mock_pipeline = MagicMock()
            mock_pipeline.calculate.return_value = df
            mock_pipeline_cls.return_value = mock_pipeline
            mock_writer.return_value = 1

            outcome = handler.handle(_make_message(), "verified.rtc", MagicMock())

            assert isinstance(outcome, Success)
            assert outcome.metrics["row_count"] == 1
            mock_connect.assert_called_once()
            mock_pipeline.calculate.assert_called_once_with(
                routes=["01001", "01002"], year=2024, semester=2
            )
            mock_writer.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_oracle_connect_error_is_transient(self, handler, make_traffic_settings):
        with patch("traffic.consumer.handler.oracledb.connect") as mock_connect:
            mock_connect.side_effect = RuntimeError("cannot connect")

            outcome = handler.handle(_make_message(), "verified.rtc", MagicMock())

            assert isinstance(outcome, TransientFailure)

    def test_oracle_database_error_during_calc_is_transient(
        self, handler, make_traffic_settings
    ):
        import oracledb

        with patch("traffic.consumer.handler.oracledb.connect") as mock_connect, patch(
            "traffic.consumer.handler.AADTPipeline"
        ) as mock_pipeline_cls:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            mock_pipeline = MagicMock()
            mock_pipeline.calculate.side_effect = oracledb.DatabaseError("timeout")
            mock_pipeline_cls.return_value = mock_pipeline

            outcome = handler.handle(_make_message(), "verified.rtc", MagicMock())

            assert isinstance(outcome, TransientFailure)
            mock_conn.close.assert_called_once()

    def test_data_error_is_permanent(self, handler, make_traffic_settings):
        with patch("traffic.consumer.handler.oracledb.connect") as mock_connect, patch(
            "traffic.consumer.handler.AADTPipeline"
        ) as mock_pipeline_cls:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            mock_pipeline = MagicMock()
            mock_pipeline.calculate.side_effect = ValueError("bad route")
            mock_pipeline_cls.return_value = mock_pipeline

            outcome = handler.handle(_make_message(), "verified.rtc", MagicMock())

            assert isinstance(outcome, PermanentFailure)
            assert outcome.reason == "traffic_calculation_failed"
            mock_conn.close.assert_called_once()

    def test_passes_connection_to_heartbeat(self, handler, make_traffic_settings):
        df = pd.DataFrame({"LINKID": ["01001"], "AADT": [100]})
        rabbit_conn = MagicMock()

        with patch("traffic.consumer.handler.oracledb.connect") as mock_connect, patch(
            "traffic.consumer.handler.AADTPipeline"
        ) as mock_pipeline_cls, patch(
            "traffic.consumer.handler.run_with_heartbeat"
        ) as mock_heartbeat, patch(
            "traffic.consumer.handler.write_aadt_results"
        ) as mock_writer:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            mock_pipeline = MagicMock()
            mock_pipeline.calculate.return_value = df
            mock_pipeline_cls.return_value = mock_pipeline
            mock_heartbeat.return_value = df
            mock_writer.return_value = 1

            handler.handle(_make_message(), "verified.rtc", rabbit_conn)

            mock_heartbeat.assert_called_once()
            assert mock_heartbeat.call_args.args[1] is rabbit_conn
