from __future__ import annotations

import threading
from typing import Any, Callable


def run_with_heartbeat(fn: Callable[[], Any], connection: Any) -> Any:
    """
    Run ``fn`` in a background thread while pumping RabbitMQ heartbeats.

    This lets long-running, blocking domain work (dbt runs, AADT calculation)
    keep the AMQP connection alive.
    """
    if connection is None:
        return fn()

    result: dict[str, Any] = {}

    def worker() -> None:
        try:
            result["value"] = fn()
        except Exception as exc:
            result["error"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    while thread.is_alive():
        connection.process_data_events(time_limit=0)
        thread.join(timeout=1.0)

    if "error" in result:
        raise result["error"]
    return result["value"]
