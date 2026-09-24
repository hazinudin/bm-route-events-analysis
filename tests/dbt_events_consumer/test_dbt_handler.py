import json
from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from dbt_events_consumer.handler import ROUTING_TO_SELECT, DbtMessageHandler
from worker.schema import TriggerMessage


def _make_message(**overrides):
    defaults = {
        "job_id": "job-1",
        "routing_key": "verified.rni",
        "year": 2025,
        "semester": 2,
        "emitted_at": "2026-08-21T09:30:00Z",
    }
    defaults.update(overrides)
    return TriggerMessage(**defaults)


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


class TestRoutingToSelectMap:
    def test_map_contains_all_three_keys(self):
        assert ROUTING_TO_SELECT["verified.rni"] == "stg_rni_combined+"
        assert ROUTING_TO_SELECT["verified.iri"] == "stg_rni_combined tag:iri"
        assert ROUTING_TO_SELECT["verified.pci"] == "stg_rni_combined tag:pci"


class TestCliArgs:
    def test_rni_selection_and_vars(self):
        runner = MagicMock()
        runner.run.return_value = _make_result(node_count=3)
        handler = DbtMessageHandler(MagicMock(), runner)

        msg = _make_message()
        outcome = handler.handle(msg, "verified.rni", None)

        assert outcome.metrics["node_count"] == 3
        args = runner.run.call_args.args[0]
        assert "stg_rni_combined+" in args
        vars_json = args[args.index("--vars") + 1]
        vars_dict = json.loads(vars_json)
        assert vars_dict["year"] == 2025
        assert vars_dict["semester"] == 2
        assert "routes" not in vars_dict

    def test_pci_with_routes(self):
        runner = MagicMock()
        runner.run.return_value = _make_result(node_count=2)
        handler = DbtMessageHandler(MagicMock(), runner)

        msg = _make_message(routing_key="verified.pci", routes=["01001"])
        handler.handle(msg, "verified.pci", None)

        args = runner.run.call_args.args[0]
        assert "stg_rni_combined tag:pci" in args
        vars_json = args[args.index("--vars") + 1]
        assert json.loads(vars_json)["routes"] == ["01001"]

    def test_full_refresh_flag(self):
        runner = MagicMock()
        runner.run.return_value = _make_result()
        handler = DbtMessageHandler(MagicMock(), runner)

        msg = _make_message(full_refresh=True)
        handler.handle(msg, "verified.rni", None)

        args = runner.run.call_args.args[0]
        assert "--full-refresh" in args


class TestOutcomeMapping:
    def test_empty_selection_is_permanent_failure(self):
        runner = MagicMock()
        runner.run.return_value = _make_result(success=True, node_count=0)
        handler = DbtMessageHandler(MagicMock(), runner)

        outcome = handler.handle(_make_message(), "verified.rni", None)

        assert outcome.reason == "empty_selection"

    def test_dbt_run_failure_is_permanent(self):
        runner = MagicMock()
        runner.run.return_value = _make_result(success=False, node_count=2)
        handler = DbtMessageHandler(MagicMock(), runner)

        outcome = handler.handle(_make_message(), "verified.rni", None)

        assert outcome.reason == "dbt_run_failed"

    def test_runner_exception_is_transient(self):
        runner = MagicMock()
        runner.run.side_effect = RuntimeError("Oracle connection lost")
        handler = DbtMessageHandler(MagicMock(), runner)

        outcome = handler.handle(_make_message(), "verified.rni", None)

        assert isinstance(outcome.exc, RuntimeError)

    def test_result_exception_is_transient(self):
        runner = MagicMock()
        runner.run.return_value = _make_result(
            success=False, exception=RuntimeError("dbt crashed")
        )
        handler = DbtMessageHandler(MagicMock(), runner)

        outcome = handler.handle(_make_message(), "verified.rni", None)

        assert isinstance(outcome.exc, RuntimeError)


class TestTracing:
    def test_success_creates_dbt_run_child_span(self):
        runner = MagicMock()
        runner.run.return_value = _make_result(node_count=2)
        handler = DbtMessageHandler(MagicMock(), runner)

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        handler._tracer = provider.get_tracer("test")

        handler.handle(_make_message(), "verified.rni", None)

        spans = exporter.get_finished_spans()
        assert [s.name for s in spans] == ["dbt_run"]
        assert spans[0].attributes["dbt.selection"] == "stg_rni_combined+"
        assert spans[0].attributes["dbt.year"] == 2025
        assert spans[0].attributes["dbt.semester"] == 2
        assert spans[0].status.status_code == StatusCode.UNSET

    def test_exception_recorded_on_child_span(self):
        runner = MagicMock()
        runner.run.side_effect = RuntimeError("Oracle connection lost")
        handler = DbtMessageHandler(MagicMock(), runner)

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        handler._tracer = provider.get_tracer("test")

        handler.handle(_make_message(), "verified.rni", None)

        spans = exporter.get_finished_spans()
        assert spans[0].name == "dbt_run"
        assert spans[0].status.status_code == StatusCode.ERROR
        exception_events = [e for e in spans[0].events if e.name == "exception"]
        assert len(exception_events) == 1
        assert exception_events[0].attributes["exception.type"] == "RuntimeError"
