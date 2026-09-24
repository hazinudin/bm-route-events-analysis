from __future__ import annotations

from typing import Any

from dbt.cli.main import dbtRunner

from dbt_events_consumer.config import DbtWorkerSettings
from worker.heartbeat import run_with_heartbeat


class DbtRunnerWrapper:
    def __init__(self, settings: DbtWorkerSettings):
        self._runner = dbtRunner()
        self._project_dir = str(settings.dbt_project_dir)
        self._profiles_dir = str(settings.dbt_profiles_dir)

    def run(
        self,
        cli_args: list[str],
        connection: Any | None = None,
    ) -> Any:
        """
        Run the dbtRunner function along with its CLI arguments.
        """
        full_args = list(cli_args) + [
            "--project-dir",
            self._project_dir,
            "--profiles-dir",
            self._profiles_dir,
        ]

        return run_with_heartbeat(
            lambda: self._runner.invoke(full_args),
            connection,
        )
