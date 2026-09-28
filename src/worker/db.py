from __future__ import annotations

from typing import Any

import oracledb
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def to_python(value: Any) -> Any:
    """Convert numpy/pandas scalars to plain Python objects for Oracle."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def create_oracle_engine(
    *,
    host: str,
    port: str | int,
    service: str,
    user: str,
    password: str,
    client_dir: str | None = None,
    **engine_kwargs: Any,
) -> Engine:
    """
    Build a SQLAlchemy engine for Oracle using the python-oracledb driver.

    If the first connection attempt raises ``oracledb.DatabaseError`` and
    ``client_dir`` is provided, the Oracle thick client is initialised and
    a second engine is created.  This mirrors the fallback behaviour of the
    original ``traffic.vcr_aadt_adt.db_conn`` module.
    """
    try:
        engine = _build_engine(host, port, service, user, password, **engine_kwargs)
        _validate_connection(engine)
        return engine
    except oracledb.DatabaseError:
        if client_dir is None:
            raise
        oracledb.init_oracle_client(client_dir)
        engine = _build_engine(host, port, service, user, password, **engine_kwargs)
        _validate_connection(engine)
        return engine


def _build_engine(
    host: str,
    port: str | int,
    service: str,
    user: str,
    password: str,
    **engine_kwargs: Any,
) -> Engine:
    url = (
        f"oracle+oracledb://{user}:{password}@{host}:{port}/"
        f"?service_name={service}"
    )
    return create_engine(url, **engine_kwargs)


def _validate_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1 FROM DUAL"))
        conn.commit()
