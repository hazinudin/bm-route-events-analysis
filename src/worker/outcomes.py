from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Success:
    metrics: dict[str, Any] | None = None


@dataclass(frozen=True)
class PermanentFailure:
    reason: str
    detail: str
    metrics: dict[str, Any] | None = None


@dataclass(frozen=True)
class TransientFailure:
    exc: Exception


HandlerOutcome = Success | PermanentFailure | TransientFailure
