from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from worker.settings import WorkerSettings, load_worker_settings


@dataclass(frozen=True)
class DbtWorkerSettings(WorkerSettings):
    dbt_project_dir: Path
    dbt_profiles_dir: Path


# Backward-compatible alias used by the existing test suite.
Settings = DbtWorkerSettings


def load_settings() -> DbtWorkerSettings:
    load_dotenv()
    base = load_worker_settings(
        default_queue="dbt.events.worker",
        default_dlq="dbt.events.worker.dlq",
        default_routing_keys=("verified.rni", "verified.iri", "verified.pci"),
        default_service_name="dbt-events-consumer",
    )
    kwargs = dataclasses.asdict(base)
    kwargs["dbt_project_dir"] = Path(
        os.getenv("DBT_PROJECT_DIR", "./events_analysis")
    )
    kwargs["dbt_profiles_dir"] = Path(
        os.getenv("DBT_PROFILES_DIR", "~/.dbt")
    ).expanduser()
    return DbtWorkerSettings(**kwargs)
