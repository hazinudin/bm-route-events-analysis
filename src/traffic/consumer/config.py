from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from worker.settings import WorkerSettings, load_worker_settings


@dataclass(frozen=True)
class TrafficWorkerSettings(WorkerSettings):
    traffic_target_table: str
    oracle_host: str
    oracle_port: int
    oracle_service: str
    oracle_user: str
    oracle_password: str
    oracle_client_dir: str | None


def load_settings() -> TrafficWorkerSettings:
    load_dotenv()
    base = load_worker_settings(
        default_queue="traffic.events.worker",
        default_dlq="traffic.events.worker.dlq",
        default_routing_keys=("verified.rtc",),
        default_service_name="traffic-events-consumer",
    )

    oracle_creds = _load_oracle_credentials()

    kwargs = dataclasses.asdict(base)
    kwargs["traffic_target_table"] = os.getenv("TRAFFIC_TARGET_TABLE", "SMD.AADT")
    kwargs.update(oracle_creds)
    return TrafficWorkerSettings(**kwargs)


def _load_oracle_credentials() -> dict[str, Any]:
    """Read Oracle credentials from environment or from dbt profiles.yml."""
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    service = os.getenv("ORACLE_SERVICE")
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    client_dir = os.getenv("ORACLE_CLIENT_DIR")

    if all((host, port, service, user, password)):
        return {
            "oracle_host": host,
            "oracle_port": int(port),
            "oracle_service": service,
            "oracle_user": user,
            "oracle_password": password,
            "oracle_client_dir": client_dir or None,
        }

    profile_path = (
        Path(os.getenv("DBT_PROFILES_DIR", "~/.dbt")).expanduser()
        / "profiles.yml"
    )
    if profile_path.exists():
        profile_name = os.getenv("DBT_PROFILE_NAME", "events_analysis")
        try:
            profiles = yaml.safe_load(profile_path.read_text())
            cfg = profiles[profile_name]["outputs"][profiles[profile_name]["target"]]
            return {
                "oracle_host": cfg["host"],
                "oracle_port": int(cfg.get("port", 1521)),
                "oracle_service": cfg.get("service") or cfg.get("dbname"),
                "oracle_user": cfg["user"],
                "oracle_password": cfg["password"],
                "oracle_client_dir": client_dir
                or cfg.get("oracle_client_dir")
                or None,
            }
        except (KeyError, TypeError):
            pass

    raise ValueError(
        "Oracle credentials are required. Set ORACLE_HOST, ORACLE_PORT, "
        "ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD or provide a dbt profile."
    )
