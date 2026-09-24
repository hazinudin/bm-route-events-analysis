from __future__ import annotations

from traffic.consumer.config import load_settings
from traffic.consumer.handler import TrafficMessageHandler
from worker.app import run_worker


def main() -> None:
    settings = load_settings()
    run_worker(
        settings,
        lambda: TrafficMessageHandler(settings),
        logger_name="traffic_events_consumer",
    )


if __name__ == "__main__":
    main()
