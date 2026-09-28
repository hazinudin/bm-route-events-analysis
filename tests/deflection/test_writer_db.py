"""Opt-in database round-trip test for the deflection writer.

Skipped by default.  It only runs when ``DEFLECTION_DB_WRITE_TEST=1`` is set and
a dbt profile is available, and it only ever touches the scratch table
``SMD.DEFL_WRITER_TEST`` (created and dropped within the test).  The production
tables ``SMD.FWD`` / ``SMD.LWD`` / ``SMD.BB`` are never referenced.

Run explicitly (writes to the scratch table only)::

    DEFLECTION_DB_WRITE_TEST=1 uv run pytest tests/deflection/test_writer_db.py -v
"""

import os
from pathlib import Path

import pandas as pd
import pytest
import yaml
from sqlalchemy import create_engine, text

from src.deflection.writer import write_deflection_results

DBT_PROFILES = Path.home() / ".dbt" / "profiles.yml"
DBT_PROFILE_NAME = "events_analysis"
SCRATCH_TABLE = "SMD.DEFL_WRITER_TEST"

pytestmark = pytest.mark.skipif(
    os.getenv("DEFLECTION_DB_WRITE_TEST") != "1" or not DBT_PROFILES.exists(),
    reason=(
        "opt-in DB write test; set DEFLECTION_DB_WRITE_TEST=1 and provide "
        f"{DBT_PROFILES}"
    ),
)

CREATE_TABLE_SQL = f"""
CREATE TABLE {SCRATCH_TABLE} (
    OBJECTID NUMBER NOT NULL,
    LINKID VARCHAR2(50),
    STA NUMBER,
    YEAR NUMBER,
    SEMESTER NUMBER,
    NORM_FWD_D1 NUMBER,
    CORR_FWD_D1 NUMBER,
    D0_D200 NUMBER
)
"""
DROP_TABLE_SQL = f"DROP TABLE {SCRATCH_TABLE}"


class _SequentialIds:
    """Deterministic OBJECTID provider (scratch table is not geodatabase-registered)."""

    def __init__(self, start: int = 900_000):
        self._next = start

    def __call__(self, cursor, table, count):
        ids = list(range(self._next, self._next + count))
        self._next += count
        return ids


def _engine_from_dbt_profile():
    with open(DBT_PROFILES) as fh:
        profile = yaml.safe_load(fh)[DBT_PROFILE_NAME]
    out = profile["outputs"][profile["target"]]
    return create_engine(
        f"oracle+oracledb://{out['user']}:{out['password']}"
        f"@{out['host']}:{out['port']}/{out['service']}"
    )


@pytest.fixture
def engine():
    eng = _engine_from_dbt_profile()
    try:
        yield eng
    finally:
        eng.dispose()


def _drop_scratch_table(engine):
    with engine.begin() as conn:
        try:
            conn.execute(text(DROP_TABLE_SQL))
        except Exception:
            pass


def _make_fwd_df():
    return pd.DataFrame(
        {
            "LINKID": ["R1", "R2"],
            "STA": [0.0, 10.0],
            "NORM_FWD_D1": [0.1, 0.12],
            "CORR_FWD_D1": [0.11, 0.13],
            "D0_D200": [0.02, 0.03],
        }
    )


def _write(engine, ids, routes, year=2020, semester=2):
    connection = engine.raw_connection()
    try:
        return write_deflection_results(
            connection,
            _make_fwd_df(),
            "FWD",
            year,
            routes,
            semester,
            target_table=SCRATCH_TABLE,
            object_id_provider=ids,
        )
    finally:
        connection.close()


def test_writer_roundtrip_and_idempotency(engine):
    _drop_scratch_table(engine)
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))

    ids = _SequentialIds()
    routes = ["R1", "R2"]
    try:
        assert _write(engine, ids, routes) == 2

        first = pd.read_sql(
            f"SELECT * FROM {SCRATCH_TABLE} ORDER BY LINKID", con=engine
        ).rename(columns=str.upper)
        assert len(first) == 2
        assert first["LINKID"].tolist() == ["R1", "R2"]
        assert first["NORM_FWD_D1"].round(6).tolist() == [0.1, 0.12]
        assert first["CORR_FWD_D1"].round(6).tolist() == [0.11, 0.13]
        assert first["D0_D200"].round(6).tolist() == [0.02, 0.03]
        assert first["YEAR"].tolist() == [2020, 2020]
        assert first["SEMESTER"].tolist() == [2, 2]
        assert first["OBJECTID"].notna().all()

        # Same routes again: the writer deletes the year's rows first, so the
        # table must still hold exactly two rows (no duplicates).
        assert _write(engine, ids, routes) == 2
        second = pd.read_sql(f"SELECT * FROM {SCRATCH_TABLE}", con=engine)
        assert len(second) == 2
    finally:
        _drop_scratch_table(engine)
