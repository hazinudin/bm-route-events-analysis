import json
import logging
from unittest.mock import MagicMock, call

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode
from pydantic import ValidationError

from dbt_events_consumer.consumer import (
    ROUTING_TO_SELECT,
    EventConsumer,
    RETRY_HEADER,
)


@pytest.fixture
def mock_channel():
    channel = MagicMock()
    channel.connection = MagicMock()
    return channel


@pytest.fixture
def consumer(make_settings, mock_channel):
    settings = make_settings()
    runner = MagicMock()
    logger = logging.getLogger("dbt_events_consumer.test")
    return EventConsumer(settings, runner, logger), settings, runner


def _make_properties(**overrides):
    props = MagicMock()
    props.content_type = "application/json"
    props.content_encoding = "utf-8"
    props.delivery_mode = 2
    props.message_id = "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12"
    props.timestamp = None
    props.headers = None
    for k, v in overrides.items():
        setattr(props, k, v)
    return props


def _make_method(routing_key="verified.rni"):
    method = MagicMock()
    method.routing_key = routing_key
    method.delivery_tag = 1
    return method


def _valid_body(**overrides):
    body = {
        "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
        "routing_key": "verified.rni",
        "year": 2025,
        "semester": 2,
        "emitted_at": "2026-08-21T09:30:00Z",
    }
    body.update(overrides)
    return json.dumps(body).encode()


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


class TestU1ToU4ValidPayloads:
    def test_u1_valid_rni_success_ack(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.return_value = _make_result(success=True, node_count=3)

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), _valid_body(),
        )

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_channel.basic_nack.assert_not_called()

        args = runner.run.call_args.args[0]
        assert "stg_rni_combined+" in args
        vars_json = args[args.index("--vars") + 1]
        vars_dict = json.loads(vars_json)
        assert vars_dict["year"] == 2025
        assert vars_dict["semester"] == 2
        assert "routes" not in vars_dict
        assert "--full-refresh" not in args

    def test_u2_valid_iri_routes_none(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.return_value = _make_result(success=True, node_count=8)

        body = _valid_body(
            routing_key="verified.iri",
            semester=1,
        )
        body_dict = json.loads(body)
        body_dict["routing_key"] = "verified.iri"
        body = json.dumps(body_dict).encode()

        cons.on_message(
            mock_channel, _make_method("verified.iri"),
            _make_properties(), body,
        )

        mock_channel.basic_ack.assert_called_once()
        args = runner.run.call_args.args[0]
        assert "tag:iri" in args
        vars_json = args[args.index("--vars") + 1]
        assert "routes" not in json.loads(vars_json)

    def test_u3_valid_pci_with_routes(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.return_value = _make_result(success=True, node_count=4)

        body = json.dumps({
            "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
            "routing_key": "verified.pci",
            "year": 2025,
            "semester": 2,
            "routes": ["01001"],
            "emitted_at": "2026-08-21T09:30:00Z",
        }).encode()

        cons.on_message(
            mock_channel, _make_method("verified.pci"),
            _make_properties(), body,
        )

        mock_channel.basic_ack.assert_called_once()
        args = runner.run.call_args.args[0]
        assert "tag:pci" in args
        vars_json = args[args.index("--vars") + 1]
        vars_dict = json.loads(vars_json)
        assert vars_dict["routes"] == ["01001"]

    def test_u4_full_refresh_flag(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.return_value = _make_result(success=True, node_count=1)

        body = json.dumps({
            "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
            "routing_key": "verified.rni",
            "year": 2025,
            "semester": 2,
            "full_refresh": True,
            "emitted_at": "2026-08-21T09:30:00Z",
        }).encode()

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        mock_channel.basic_ack.assert_called_once()
        args = runner.run.call_args.args[0]
        assert "--full-refresh" in args


class TestU5ToU9InvalidPayloads:
    def test_u5_malformed_json_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), b"not json{",
        )

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        mock_channel.basic_ack.assert_not_called()

    def test_u6_missing_semester_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer

        body = json.dumps({
            "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
            "routing_key": "verified.rni",
            "year": 2025,
            "emitted_at": "2026-08-21T09:30:00Z",
        }).encode()

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )

    def test_u7_semester_out_of_range_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer

        body = json.dumps({
            "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
            "routing_key": "verified.rni",
            "year": 2025,
            "semester": 3,
            "emitted_at": "2026-08-21T09:30:00Z",
        }).encode()

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )

    def test_u8_unknown_routing_key_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer

        cons.on_message(mock_channel, _make_method("verified.xyz"),
                        _make_properties(), _valid_body(routing_key="verified.xyz"))

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )

    def test_u9_routing_key_mismatch_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer

        body = _valid_body(routing_key="verified.iri")

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )


class TestU10DbtRunFailed:
    def test_u10_dbt_failure_nack_no_retry(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.return_value = _make_result(
            success=False, node_count=2, exception=None
        )

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        mock_channel.basic_publish.assert_not_called()


class TestU11ToU13TransientRetry:
    def test_u11_first_retry_no_header(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.side_effect = RuntimeError("Oracle connection lost")

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_publish.assert_called_once()
        pub_call = mock_channel.basic_publish.call_args
        assert pub_call.kwargs["exchange"] == "validation.events.retry"
        assert pub_call.kwargs["routing_key"] == "verified.rni"

        pub_props = pub_call.kwargs["properties"]
        assert pub_props.expiration == "2000"
        assert pub_props.headers[RETRY_HEADER] == 1

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_channel.basic_nack.assert_not_called()

    def test_u12_retry_count_2_of_3(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.side_effect = RuntimeError("Oracle timeout")

        props = _make_properties(headers={RETRY_HEADER: 2})

        cons.on_message(mock_channel, _make_method("verified.iri"),
                        props, _valid_body(routing_key="verified.iri"))

        pub_props = mock_channel.basic_publish.call_args.kwargs["properties"]
        assert pub_props.expiration == "8000"
        assert pub_props.headers[RETRY_HEADER] == 3

        mock_channel.basic_ack.assert_called_once()

    def test_u13_retry_saturated_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.side_effect = RuntimeError("Oracle still down")

        props = _make_properties(headers={RETRY_HEADER: 3})

        cons.on_message(mock_channel, _make_method("verified.pci"),
                        props, _valid_body(routing_key="verified.pci"))

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        mock_channel.basic_publish.assert_not_called()


class TestU14EmptySelection:
    def test_u14_empty_selection_nack(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.return_value = _make_result(success=True, node_count=0)

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        mock_channel.basic_ack.assert_not_called()


class TestRoutingToSelectMap:
    def test_map_contains_all_three_keys(self):
        assert ROUTING_TO_SELECT["verified.rni"] == "stg_rni_combined+"
        assert ROUTING_TO_SELECT["verified.iri"] == "tag:iri"
        assert ROUTING_TO_SELECT["verified.pci"] == "tag:pci"


class TestRunnerInvokeException:
    def test_runner_raises_treated_as_transient(self, consumer, mock_channel):
        cons, settings, runner = consumer
        runner.run.side_effect = RuntimeError("FFI crash")

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_publish.assert_called_once()
        mock_channel.basic_ack.assert_called_once()


class TestRetryPreservesProperties:
    def test_retry_publish_preserves_body_and_content_type(
        self, consumer, mock_channel
    ):
        cons, settings, runner = consumer
        runner.run.side_effect = RuntimeError("transient")

        body = _valid_body()
        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        pub_call = mock_channel.basic_publish.call_args
        assert pub_call.kwargs["body"] == body
        pub_props = pub_call.kwargs["properties"]
        assert pub_props.content_type == "application/json"
        assert pub_props.delivery_mode == 2


class TestTracingFailures:
    def test_rejected_message_marks_span_as_error(
        self, consumer, mock_channel
    ):
        cons, settings, runner = consumer
        span = MagicMock()

        cons._reject_message(
            mock_channel,
            1,
            span,
            "event-1",
            "verified.rni",
            "dbt_run_failed",
            "dbt run failed",
        )

        status = span.set_status.call_args.args[0]
        assert status.status_code == StatusCode.ERROR
        assert status.description == "dbt_run_failed"

    def test_rejected_exception_is_recorded_on_span(
        self, consumer, mock_channel
    ):
        cons, settings, runner = consumer
        span = MagicMock()

        cons._parse_or_reject(
            mock_channel, 1, "verified.rni", b"not json{", span
        )

        exception = span.record_exception.call_args.args[0]
        assert isinstance(exception, json.JSONDecodeError)
        status = span.set_status.call_args.args[0]
        assert status.status_code == StatusCode.ERROR

    def test_transient_exception_is_recorded_on_span(
        self, consumer, mock_channel
    ):
        cons, settings, runner = consumer
        span = MagicMock()
        exception = RuntimeError("Oracle timeout")

        cons._handle_transient(
            mock_channel,
            1,
            _make_properties(),
            _valid_body(),
            "verified.rni",
            "event-1",
            exception,
            span,
        )

        span.record_exception.assert_called_once_with(exception)
        status = span.set_status.call_args.args[0]
        assert status.status_code == StatusCode.ERROR


class TestDbtRunChildSpan:
    def _consumer_with_test_tracer(self, consumer):
        cons, settings, runner = consumer
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        cons._tracer = provider.get_tracer("test")
        return cons, runner, exporter

    def test_success_creates_dbt_run_child_span(
        self, consumer, mock_channel
    ):
        cons, runner, exporter = self._consumer_with_test_tracer(consumer)
        runner.run.return_value = _make_result(success=True, node_count=2)

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), _valid_body(),
        )

        spans = exporter.get_finished_spans()
        assert [s.name for s in spans] == ["dbt_run", "consume_message"]

        dbt_span, parent_span = spans
        assert dbt_span.parent.span_id == parent_span.context.span_id
        assert dbt_span.attributes["dbt.selection"] == "stg_rni_combined+"
        assert dbt_span.attributes["dbt.year"] == 2025
        assert dbt_span.attributes["dbt.semester"] == 2
        assert dbt_span.status.status_code == StatusCode.UNSET
        assert parent_span.status.status_code == StatusCode.UNSET
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_runner_exception_recorded_on_child_span(
        self, consumer, mock_channel
    ):
        cons, runner, exporter = self._consumer_with_test_tracer(consumer)
        runner.run.side_effect = RuntimeError("Oracle connection lost")

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), _valid_body(),
        )

        spans = exporter.get_finished_spans()
        dbt_span = next(s for s in spans if s.name == "dbt_run")
        parent_span = next(s for s in spans if s.name == "consume_message")

        assert dbt_span.status.status_code == StatusCode.ERROR
        exception_events = [
            e for e in dbt_span.events if e.name == "exception"
        ]
        assert len(exception_events) == 1
        attributes = exception_events[0].attributes
        assert attributes["exception.type"] == "RuntimeError"
        assert "Oracle connection lost" in attributes["exception.message"]
        assert attributes["exception.stacktrace"]

        assert parent_span.status.status_code == StatusCode.ERROR
        mock_channel.basic_publish.assert_called_once()
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_result_exception_recorded_on_child_span(
        self, consumer, mock_channel
    ):
        cons, runner, exporter = self._consumer_with_test_tracer(consumer)
        try:
            raise RuntimeError("dbt build crashed")
        except RuntimeError as exc:
            dbt_exc = exc
        runner.run.return_value = _make_result(
            success=False, exception=dbt_exc,
        )

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), _valid_body(),
        )

        spans = exporter.get_finished_spans()
        dbt_span = next(s for s in spans if s.name == "dbt_run")
        parent_span = next(s for s in spans if s.name == "consume_message")

        assert dbt_span.status.status_code == StatusCode.ERROR
        exception_events = [
            e for e in dbt_span.events if e.name == "exception"
        ]
        assert len(exception_events) == 1
        assert "dbt build crashed" in (
            exception_events[0].attributes["exception.message"]
        )
        assert exception_events[0].attributes["exception.stacktrace"]

        assert parent_span.status.status_code == StatusCode.ERROR
        mock_channel.basic_publish.assert_called_once()
