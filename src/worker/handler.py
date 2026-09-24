from __future__ import annotations

from typing import Any, Protocol

from worker.outcomes import HandlerOutcome
from worker.schema import TriggerMessage


class MessageHandler(Protocol):
    def handle(
        self,
        msg: TriggerMessage,
        routing_key: str,
        connection: Any,
    ) -> HandlerOutcome: ...
