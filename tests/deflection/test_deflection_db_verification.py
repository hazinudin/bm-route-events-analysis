"""
Characterization tests: verify that the current ``Deflection`` implementation
reproduces the calculated values stored in the production tables
``SMD.FWD``, ``SMD.LWD`` and ``SMD.BB``.

Pipeline under test:

    staging table (FWD_2_2020 / BB_2_2020)  --Deflection-->  target table (SMD.FWD / SMD.BB)

Verified behavior encoded here:

* FWD (YEAR=2020): fully reproducible — drop selection (closest-to-40kN),
  normalization, curvature and temperature correction all match the stored
  values (max deviation ~1e-7).
* BB (YEAR=2020): fully reproducible — BB_D0 / BB_D0_D200 match exactly;
  the temperature-corrected and FWD-converted columns match within storage
  rounding (<= 2e-4).
* LWD (YEAR=2021): ``SMD.LWD`` raw deflections differ from the ``LWD_2_2021``
  staging table (it was loaded from another source), so a staging-to-target
  comparison is impossible.  A round-trip on the stored rows verifies the
  full calculation (normalization with reference load 40 + temperature
  correction) — it matches the stored values (max deviation ~6e-9).

Known data issue (not asserted here): the ``SMD.FWD`` YEAR=2025 rows with
``UPDATE_DATE = 2026-01-05`` do not match the current implementation's CORR_*
output (only ~15% agreement), while the 2026-01-06 batch matches 100% — that
batch appears to have been loaded with different lookup-table values.

Oracle credentials are taken from the local dbt profile
(``~/.dbt/profiles.yml``, profile ``events_analysis``); the whole module is
skipped when the profile is not available.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from sqlalchemy import create_engine

from src.deflection.deflection import Deflection

DBT_PROFILES = Path.home() / ".dbt" / "profiles.yml"
DBT_PROFILE_NAME = "events_analysis"

pytestmark = pytest.mark.skipif(
    not DBT_PROFILES.exists(),
    reason=f"dbt profiles not found at {DBT_PROFILES}; skipping DB verification tests",
)


def _engine_from_dbt_profile():
    with open(DBT_PROFILES) as fh:
        profile = yaml.safe_load(fh)[DBT_PROFILE_NAME]
    out = profile["outputs"][profile["target"]]
    return create_engine(
        f"oracle+oracledb://{out['user']}:{out['password']}"
        f"@{out['host']}:{out['port']}/{out['service']}"
    )


@pytest.fixture(scope="module")
def engine():
    eng = _engine_from_dbt_profile()
    yield eng
    eng.dispose()


def _read(engine, sql):
    return pd.read_sql(sql, con=engine).rename(columns=str.upper)


def _assert_close(merged, calc_col, target_col, tol):
    """Assert two merged columns agree element-wise within ``tol`` (no NaNs)."""
    assert not merged[calc_col].isna().any(), f"{calc_col} contains NaN"
    assert not merged[target_col].isna().any(), f"{target_col} contains NaN"
    diff = (merged[calc_col] - merged[target_col]).abs()
    n_bad = int((diff > tol).sum())
    assert n_bad == 0, (
        f"{calc_col} vs {target_col}: {n_bad}/{len(merged)} rows differ "
        f"(max diff {diff.max():.3e}, tol {tol:.0e})"
    )


# --------------------------------------------------------------------------
# FWD: staging FWD_2_2020 -> SMD.FWD (YEAR=2020)
# --------------------------------------------------------------------------

FWD_SAMPLE_LINKIDS_SQL = """
    SELECT LINKID FROM (
        SELECT DISTINCT LINKID FROM SMD.FWD WHERE YEAR = 2020
    ) WHERE ROWNUM <= 5
"""

FWD_CALC_COLUMNS = [
    "NORM_FWD_D1", "NORM_FWD_D2", "D0_D200",
    "CORR_FWD_D1", "CORR_FWD_D2", "CORR_D0_D200",
]


def test_fwd_matches_smd_fwd(engine):
    linkids = _read(engine, FWD_SAMPLE_LINKIDS_SQL)["LINKID"].tolist()
    assert linkids, "no FWD linkids found for YEAR=2020"
    in_list = ",".join(repr(link) for link in linkids)

    staging = _read(engine, f"SELECT * FROM SMD.FWD_2_2020 WHERE LINKID IN ({in_list})")
    defl = Deflection(
        df=staging,
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

    target = _read(
        engine,
        f"""SELECT LINKID, STA, SURVEY_DIREC, FORCE, FWD_D1, FWD_D2,
                   {", ".join(FWD_CALC_COLUMNS)}
            FROM SMD.FWD WHERE YEAR = 2020 AND LINKID IN ({in_list})""",
    )

    merged = defl.sorted.merge(
        target, on=["LINKID", "STA", "SURVEY_DIREC"], suffixes=("_CALC", "_DB")
    )
    assert len(merged) == len(target), (
        f"selection mismatch: {len(merged)} calculated rows vs {len(target)} stored rows"
    )

    # The sorter must pick the same drop (raw columns) as the stored rows.
    for raw_col in ("FORCE", "FWD_D1", "FWD_D2"):
        _assert_close(merged, f"{raw_col}_CALC", f"{raw_col}_DB", tol=1e-9)

    # Normalized / corrected results must match the stored values.
    for col in FWD_CALC_COLUMNS:
        _assert_close(merged, f"{col}_CALC", f"{col}_DB", tol=1e-6)


# --------------------------------------------------------------------------
# BB: staging BB_2_2020 -> SMD.BB (YEAR=2020)
# --------------------------------------------------------------------------

BB_MEASUREMENT_KEYS = [
    "LINKID", "STA", "BB_D1", "BB_D2", "BB_D3", "ASPHALT_TEMP", "SURF_THICKNESS",
]
BB_EXACT_COLUMNS = ["BB_D0", "BB_D0_D200"]
BB_ROUNDED_COLUMNS = ["BB_D0_CORR", "BB_D0_D200_CORR", "FWD_D0", "FWD_D0_D200"]


def test_bb_matches_smd_bb(engine):
    staging = _read(engine, "SELECT * FROM SMD.BB_2_2020")
    defl = Deflection(
        df=staging,
        force_col="LOAD_TON",
        data_type="BB",
        d0_col="BB_D3",
        d200_col="BB_D2",
        asp_temp="ASPHALT_TEMP",
        conn=engine,
    )

    target = _read(
        engine,
        f"""SELECT {", ".join(BB_MEASUREMENT_KEYS)},
                   {", ".join(BB_EXACT_COLUMNS + BB_ROUNDED_COLUMNS)}
            FROM SMD.BB WHERE YEAR = 2020""",
    )

    # Staging contains repeated measurements per station; join on the full
    # measurement signature so each stored row is compared against the
    # staging row it was actually calculated from.
    merged = defl.sorted.merge(target, on=BB_MEASUREMENT_KEYS, suffixes=("_CALC", "_DB"))
    merged = merged.drop_duplicates(subset=BB_MEASUREMENT_KEYS + ["BB_D0_CALC"])
    assert len(merged) == len(target), (
        f"coverage mismatch: {len(merged)} calculated rows vs {len(target)} stored rows"
    )

    for col in BB_EXACT_COLUMNS:
        _assert_close(merged, f"{col}_CALC", f"{col}_DB", tol=1e-9)
    for col in BB_ROUNDED_COLUMNS:
        _assert_close(merged, f"{col}_CALC", f"{col}_DB", tol=2e-4)


# --------------------------------------------------------------------------
# LWD: round-trip on SMD.LWD (YEAR=2021)
# --------------------------------------------------------------------------

LWD_NORM_COLUMNS = ["NORM_LWD_D0", "NORM_LWD_D1", "D0_D200"]
LWD_CORR_COLUMNS = ["CORR_LWD_D0", "CORR_LWD_D1", "CORR_D0_D200"]


@pytest.fixture(scope="module")
def lwd_roundtrip(engine):
    stored = _read(
        engine,
        """SELECT * FROM SMD.LWD
           WHERE YEAR = 2021 AND LOAD_KG IS NOT NULL AND LWD_D0 IS NOT NULL""",
    )
    # SMD.LWD has no SURVEY_DATE column but the implementation unconditionally
    # drops it after calculation; provide a dummy so the round-trip works.
    stored["SURVEY_DATE"] = pd.NaT
    defl = Deflection(
        df=stored,
        force_col="LOAD_KG",
        data_type="LWD",
        d0_col="LWD_D0",
        d200_col="LWD_D1",
        asp_temp="ASPHALT_TEMP",
        from_m_col=None,
        to_m_col=None,
        sta_col="STA",
        conn=engine,
    )
    merged = defl.sorted.merge(
        stored[["LINKID", "STA"] + LWD_NORM_COLUMNS + LWD_CORR_COLUMNS],
        on=["LINKID", "STA"],
        suffixes=("_CALC", "_DB"),
    )
    assert len(merged) == len(stored), (
        f"round-trip lost rows: {len(merged)} calculated vs {len(stored)} stored"
    )
    return merged


def test_lwd_normalization_matches_smd_lwd(lwd_roundtrip):
    for col in LWD_NORM_COLUMNS:
        _assert_close(lwd_roundtrip, f"{col}_CALC", f"{col}_DB", tol=1e-6)


def test_lwd_temp_correction_matches_smd_lwd(lwd_roundtrip):
    for col in LWD_CORR_COLUMNS:
        _assert_close(lwd_roundtrip, f"{col}_CALC", f"{col}_DB", tol=1e-6)
