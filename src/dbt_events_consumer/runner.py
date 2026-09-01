from __future__ import annotations

import threading
from typing import Any

from dbt.cli.main import dbtRunner

from dbt_events_consumer.config import Settings


class DbtRunnerWrapper:
    def __init__(self, settings: Settings):
        self._runner = dbtRunner()
        self._project_dir = str(settings.dbt_project_dir)
        self._profiles_dir = str(settings.dbt_profiles_dir)

    def run(
        self,
        cli_args: list[str],
        connection: Any | None = None,
    ) -> Any:
        """
        Ran the dbtRunner function along with its CLI arguments.
        """
        full_args = list(cli_args) + [
            "--project-dir",
            self._project_dir,
            "--profiles-dir",
            self._profiles_dir,
        ]

        # If the RabbitMQ connection is not provided
        if connection is None:
            return self._runner.invoke(full_args)

        result: dict[str, Any] = {}

        def worker() -> None:
            try:
                result["value"] = self._runner.invoke(full_args)
            except Exception as exc:
                result["error"] = exc

        # If the RabbitMQ connection is provided, ran the dbt model build in separate thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

        # Send RabbitMQ heartbeat while the dbt model build is running
        while thread.is_alive():
            connection.process_data_events(time_limit=0)
            thread.join(timeout=1.0)

        if "error" in result:
            raise result["error"]

        return result["value"]
