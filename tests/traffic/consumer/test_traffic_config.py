from pathlib import Path
from unittest.mock import patch

import pytest

from traffic.consumer.config import TrafficWorkerSettings, load_settings


def test_missing_rabbitmq_url_raises(clean_traffic_env):
    with pytest.raises(ValueError, match="RABBITMQ_URL"):
        load_settings()


def test_missing_oracle_credentials_raises(clean_traffic_env, tmp_path):
    clean_traffic_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_traffic_env.setenv("DBT_PROFILES_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="Oracle credentials"):
        load_settings()


def test_loads_from_environment(clean_traffic_env):
    clean_traffic_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_traffic_env.setenv("ORACLE_HOST", "db.example.com")
    clean_traffic_env.setenv("ORACLE_PORT", "1521")
    clean_traffic_env.setenv("ORACLE_SERVICE", "SVC")
    clean_traffic_env.setenv("ORACLE_USER", "u")
    clean_traffic_env.setenv("ORACLE_PASSWORD", "p")
    clean_traffic_env.setenv("TRAFFIC_TARGET_TABLE", "SMD.AADT_TEST")

    settings = load_settings()

    assert isinstance(settings, TrafficWorkerSettings)
    assert settings.rabbitmq_queue == "traffic.events.worker"
    assert settings.rabbitmq_routing_keys == ("verified.rtc",)
    assert settings.traffic_target_table == "SMD.AADT_TEST"
    assert settings.oracle_host == "db.example.com"
    assert settings.oracle_port == 1521
    assert settings.oracle_service == "SVC"
    assert settings.oracle_user == "u"
    assert settings.oracle_password == "p"


def test_loads_from_dbt_profile(clean_traffic_env, tmp_path):
    clean_traffic_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_traffic_env.setenv("DBT_PROFILES_DIR", str(tmp_path))

    profiles = tmp_path / "profiles.yml"
    profiles.write_text(
        """
events_analysis:
  target: dev
  outputs:
    dev:
      type: oracle
      host: profile-host
      port: 1522
      service: PROFILE_SVC
      user: profile-user
      password: profile-pass
"""
    )

    settings = load_settings()

    assert settings.oracle_host == "profile-host"
    assert settings.oracle_port == 1522
    assert settings.oracle_service == "PROFILE_SVC"
    assert settings.oracle_user == "profile-user"
    assert settings.oracle_password == "profile-pass"


def test_environment_overrides_profile(clean_traffic_env, tmp_path):
    clean_traffic_env.setenv("RABBITMQ_URL", "amqp://x")
    clean_traffic_env.setenv("ORACLE_HOST", "env-host")
    clean_traffic_env.setenv("ORACLE_PORT", "1523")
    clean_traffic_env.setenv("ORACLE_SERVICE", "ENV_SVC")
    clean_traffic_env.setenv("ORACLE_USER", "env-user")
    clean_traffic_env.setenv("ORACLE_PASSWORD", "env-pass")
    clean_traffic_env.setenv("DBT_PROFILES_DIR", str(tmp_path))

    profiles = tmp_path / "profiles.yml"
    profiles.write_text(
        """
events_analysis:
  target: dev
  outputs:
    dev:
      type: oracle
      host: profile-host
      port: 1522
      service: PROFILE_SVC
      user: profile-user
      password: profile-pass
"""
    )

    settings = load_settings()

    assert settings.oracle_host == "env-host"
    assert settings.oracle_port == 1523
    assert settings.oracle_service == "ENV_SVC"
    assert settings.oracle_user == "env-user"
    assert settings.oracle_password == "env-pass"
