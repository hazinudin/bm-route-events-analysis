from __future__ import annotations

import signal
import sys
from typing import Any

from worker.app import _mask_url
from worker.app import run_worker

from dbt_events_consumer.config import DbtWorkerSettings, load_settings
from dbt_events_consumer.handler import DbtMessageHandler
from dbt_events_consumer.runner import DbtRunnerWrapper


def _check_dirs(settings: DbtWorkerSettings, logger: Any) -> None:
    if not settings.dbt_project_dir.is_dir():
        logger.error(
            "startup check failed",
            extra={"check": "dbt_project_dir", "path": str(settings.dbt_project_dir)},
        )
        sys.exit(1)
    if not settings.dbt_profiles_dir.is_dir():
        logger.error(
            "startup check failed",
            extra={"check": "dbt_profiles_dir", "path": str(settings.dbt_profiles_dir)},
        )
        sys.exit(1)


def main() -> None:
    settings = load_settings()

    def startup_checks(s: DbtWorkerSettings, logger: Any) -> None:
        _check_dirs(s, logger)

    run_worker(
        settings,
        lambda: DbtMessageHandler(settings, DbtRunnerWrapper(settings)),
        startup_checks=startup_checks,
        logger_name="dbt_events_consumer",
    )


if __name__ == "__main__":
    main()
