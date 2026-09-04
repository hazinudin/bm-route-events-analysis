from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class TriggerMessage(BaseModel):
    job_id: str
    routing_key: str
    year: int
    semester: Literal[1, 2]
    routes: list[str] | None = None
    full_refresh: bool = False
    emitted_at: datetime
