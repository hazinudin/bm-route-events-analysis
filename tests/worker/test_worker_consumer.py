import json
import logging
from unittest.mock import MagicMock

import pytest
import pika

from worker.consumer import EventConsumer, RETRY_HEADER
from worker.outcomes import PermanentFailure, Success, TransientFailure
from worker.schema import TriggerMessage


@pytest.fixture
def mock_channel():
    channel = MagicMock()
    channel.connection = MagicMock()
    return channel


@pytest.fixture
def consumer(make_worker_settings, mock_channel):
    settings = make_worker_settings(
        rabbitmq_routing_keys=("verified.rni", "verified.rtc"),
    )
    logger = logging.getLogger("events_consumer.test")
    handler = MagicMock()
    return EventConsumer(settings, handler, logger), settings, handler


def _make_properties(**overrides):
    props = MagicMock()
    props.content_type = "application/json"
    props.content_encoding = "utf-8"
    props.delivery_mode = 2
    props.message_id = "test-msg-id"
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


class TestValidPayloads:
    def test_success_ack(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.return_value = Success(metrics={"row_count": 2})

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), _valid_body(),
        )

        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_channel.basic_nack.assert_not_called()
        assert handler.handle.call_args.args[1] == "verified.rni"

    def test_handler_receives_trigger_message(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.return_value = Success()

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), _valid_body(routes=["01001"]),
        )

        msg = handler.handle.call_args.args[0]
        assert isinstance(msg, TriggerMessage)
        assert msg.job_id == "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12"
        assert msg.year == 2025
        assert msg.semester == 2
        assert msg.routes == ["01001"]


class TestInvalidPayloads:
    def test_malformed_json_nack(self, consumer, mock_channel):
        cons, settings, handler = consumer

        cons.on_message(
            mock_channel, _make_method("verified.rni"),
            _make_properties(), b"not json{",
        )

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        handler.handle.assert_not_called()

    def test_missing_field_nack(self, consumer, mock_channel):
        cons, settings, handler = consumer
        body = json.dumps({
            "job_id": "x",
            "routing_key": "verified.rni",
            "year": 2025,
            "emitted_at": "2026-08-21T09:30:00Z",
        }).encode()

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )

    def test_unknown_routing_key_nack(self, consumer, mock_channel):
        cons, settings, handler = consumer

        cons.on_message(mock_channel, _make_method("verified.xyz"),
                        _make_properties(), _valid_body(routing_key="verified.xyz"))

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )

    def test_routing_key_mismatch_nack(self, consumer, mock_channel):
        cons, settings, handler = consumer
        body = _valid_body(routing_key="verified.iri")

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )


class TestOutcomeMapping:
    def test_permanent_failure_nack(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.return_value = PermanentFailure(
            reason="bad_data", detail="schema mismatch"
        )

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        mock_channel.basic_publish.assert_not_called()

    def test_transient_failure_republishes(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.return_value = TransientFailure(RuntimeError("Oracle down"))

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_publish.assert_called_once()
        pub_call = mock_channel.basic_publish.call_args
        assert pub_call.kwargs["exchange"] == "events.worker.retry.exchange"
        assert pub_call.kwargs["routing_key"] == "verified.rni"
        pub_props = pub_call.kwargs["properties"]
        assert pub_props.expiration == "2000"
        assert pub_props.headers[RETRY_HEADER] == 1
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_handler_exception_treated_as_transient(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.side_effect = RuntimeError("boom")

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), _valid_body())

        mock_channel.basic_publish.assert_called_once()
        mock_channel.basic_ack.assert_called_once()


class TestRetryBackOff:
    def test_retry_count_2_of_3(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.return_value = TransientFailure(RuntimeError("timeout"))

        props = _make_properties(headers={RETRY_HEADER: 2})

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        props, _valid_body())

        pub_props = mock_channel.basic_publish.call_args.kwargs["properties"]
        assert pub_props.expiration == "8000"
        assert pub_props.headers[RETRY_HEADER] == 3
        mock_channel.basic_ack.assert_called_once()

    def test_retry_saturated_nack(self, consumer, mock_channel):
        cons, settings, handler = consumer
        handler.handle.return_value = TransientFailure(RuntimeError("still down"))

        props = _make_properties(headers={RETRY_HEADER: 3})

        cons.on_message(mock_channel, _make_method("verified.rni"),
                        props, _valid_body())

        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=1, requeue=False
        )
        mock_channel.basic_publish.assert_not_called()


class TestRetryPreservesProperties:
    def test_retry_publish_preserves_body_and_content_type(
        self, consumer, mock_channel
    ):
        cons, settings, handler = consumer
        handler.handle.return_value = TransientFailure(RuntimeError("transient"))

        body = _valid_body()
        cons.on_message(mock_channel, _make_method("verified.rni"),
                        _make_properties(), body)

        pub_call = mock_channel.basic_publish.call_args
        assert pub_call.kwargs["body"] == body
        pub_props = pub_call.kwargs["properties"]
        assert pub_props.content_type == "application/json"
        assert pub_props.delivery_mode == 2
