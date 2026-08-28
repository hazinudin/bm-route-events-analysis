import os

import pandas as pd
import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.deflection.deflection import Deflection

load_dotenv("tests/dev.env")

_GDB_HOST = os.getenv("GDB_HOST")
_SMD_USER = os.getenv("SMD_USER")
_SMD_PWD = os.getenv("SMD_PWD")

pytestmark = pytest.mark.skipif(
    not (_GDB_HOST and _SMD_USER and _SMD_PWD),
    reason="Oracle credentials (GDB_HOST/SMD_USER/SMD_PWD) not set; skipping DB-dependent test",
)


@pytest.fixture
def engine():
    return create_engine(f"oracle+oracledb://{_SMD_USER}:{_SMD_PWD}@{_GDB_HOST}:1521/geodbbm")


def test_fwd(engine):
    df = pd.read_sql(
        "select * from fwd_2_2025 where linkid not in (select linkid from fwd where year = 2025)",
        con=engine,
    ).rename(columns=str.upper)

    defl = Deflection(
        df=df,
        force_col="FORCE",
        data_type="FWD",
        d0_col="FWD_D1",
        d200_col="FWD_D2",
        asp_temp="ASPHALT_TEMP",
        from_m_col=None,
        to_m_col=None,
        sta_col="STA",
        conn=engine,
    )

    assert not defl.sorted.empty
    assert not defl.sorted["CORR_D0_D200"].isnull().any()
