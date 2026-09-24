from __future__ import annotations

import json
import logging
from typing import Any

from worker.consumer import RETRY_HEADER
from worker.consumer import EventConsumer as WorkerEventConsumer
from worker.settings import WorkerSettings

from dbt_events_consumer.handler import DbtMessageHandler, ROUTING_TO_SELECT

__all__ = ["EventConsumer", "RETRY_HEADER", "ROUTING_TO_SELECT"]


class EventConsumer:
    """
    Backward-compatible wrapper around :class:`worker.consumer.EventConsumer`.

    Existing tests construct this class with ``(settings, runner, logger)``;
    the wrapper turns ``runner`` into a :class:`DbtMessageHandler` and
    delegates to the generic framework consumer.
    """

    def __init__(
        self,
        settings: WorkerSettings,
        runner: Any,
        logger: logging.Logger,
    ) -> None:
        self._handler = DbtMessageHandler(settings, runner)
        self._consumer = WorkerEventConsumer(
            settings,
            self._handler,
            logger,
            tracer_name="dbt_events_consumer",
        )

    def on_message(
        self,
        channel: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        return self._consumer.on_message(channel, method, properties, body)

    @property
    def _tracer(self):
        return self._consumer._tracer

    @_tracer.setter
    def _tracer(self, value: Any) -> None:
        self._consumer._tracer = value
        self._handler._tracer = value

    def __getattr__(self, name: str) -> Any:
        return getattr(self._consumer, name)
