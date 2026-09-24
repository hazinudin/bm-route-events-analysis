from unittest.mock import MagicMock, patch

import oracledb
import pytest

from worker.db import create_oracle_engine


def test_url_format():
    with patch("worker.db.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        engine = create_oracle_engine(
            host="db.example.com",
            port=1521,
            service="SVC",
            user="u",
            password="p",
        )

        assert engine is mock_engine
        url = mock_create_engine.call_args.args[0]
        assert url == "oracle+oracledb://u:p@db.example.com:1521/?service_name=SVC"


def test_passes_engine_kwargs():
    with patch("worker.db.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        create_oracle_engine(
            host="db.example.com",
            port="1521",
            service="SVC",
            user="u",
            password="p",
            pool_size=5,
        )

        assert mock_create_engine.call_args.kwargs["pool_size"] == 5


def test_thick_mode_fallback_on_database_error():
    with patch("worker.db.create_engine") as mock_create_engine, patch(
        "worker.db.oracledb.init_oracle_client"
    ) as mock_init:
        second_engine = MagicMock()
        mock_create_engine.side_effect = [
            oracledb.DatabaseError("DPI-1047"),
            second_engine,
        ]

        engine = create_oracle_engine(
            host="db.example.com",
            port=1521,
            service="SVC",
            user="u",
            password="p",
            client_dir="/opt/oracle/instantclient",
        )

        assert engine is second_engine
        mock_init.assert_called_once_with("/opt/oracle/instantclient")
        assert mock_create_engine.call_count == 2


def test_raises_without_client_dir():
    with patch("worker.db.create_engine") as mock_create_engine:
        mock_create_engine.side_effect = oracledb.DatabaseError("DPI-1047")

        with pytest.raises(oracledb.DatabaseError):
            create_oracle_engine(
                host="db.example.com",
                port=1521,
                service="SVC",
                user="u",
                password="p",
            )
