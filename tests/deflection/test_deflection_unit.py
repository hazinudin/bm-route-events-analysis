"""Fast, DB-free unit tests for the refactored ``src.deflection`` package.

All database-backed lookups are stubbed via monkeypatch so these tests need no
Oracle connection.
"""

import numpy as np
import pandas as pd
import pytest

from src.deflection.bb import BbDeflection
from src.deflection.columns import BbColumns, FwdLwdColumns
from src.deflection.deflection import Deflection
from src.deflection.factory import create_deflection
from src.deflection.fwd import FwdDeflection
from src.deflection.lwd import LwdDeflection

_DATES = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"])


def _fwd_df():
    return pd.DataFrame(
        {
            "LINKID": ["L1", "L1", "L1", "L2"],
            "STA": [0.0, 0.0, 10.0, 0.0],
            "SURVEY_DIREC": ["N", "N", "N", "N"],
            "FORCE": [30.0, 42.0, 39.0, 45.0],
            "FWD_D1": [100.0, 110.0, 120.0, 130.0],
            "FWD_D2": [80.0, 88.0, 95.0, 100.0],
            "ASPHALT_TEMP": [20.0, 20.0, 20.0, 20.0],
            "SURF_THICKNESS": [100.0, 100.0, 100.0, 100.0],
            "OBJECTID": [1, 2, 3, 4],
            "SURVEY_DATE": _DATES,
            "UPDATE_DATE": _DATES,
        }
    )


def _bb_df():
    return pd.DataFrame(
        {
            "LINKID": ["L1", "L1"],
            "STA": [0.0, 10.0],
            "SURVEY_DIREC": ["N", "N"],
            "BB_D1": [100.0, 110.0],
            "BB_D2": [200.0, 210.0],
            "BB_D3": [300.0, 310.0],
            "ASPHALT_TEMP": [20.0, 20.0],
            "SURF_THICKNESS": [100.0, 100.0],
            "OBJECTID": [1, 2],
            "SURVEY_DATE": _DATES[:2],
            "UPDATE_DATE": _DATES[:2],
        }
    )


def _lwd_df():
    return pd.DataFrame(
        {
            "LINKID": ["L1", "L1"],
            "STA": [0.0, 10.0],
            "SURVEY_DIREC": ["N", "N"],
            "LOAD_KG": [40.0, 50.0],
            "LWD_D0": [100.0, 110.0],
            "LWD_D1": [80.0, 88.0],
            "ASPHALT_TEMP": [20.0, 20.0],
            "SURF_THICKNESS": [100.0, 100.0],
            "OBJECTID": [1, 2],
            "UPDATE_DATE": _DATES[:2],
        }
    )


def _tiny_df():
    return pd.DataFrame(
        {
            "LINKID": ["L1"],
            "STA": [0.0],
            "SURVEY_DIREC": ["N"],
            "FORCE": [40.0],
            "FWD_D1": [100.0],
            "FWD_D2": [80.0],
            "LOAD_KG": [40.0],
            "LWD_D0": [100.0],
            "LWD_D1": [80.0],
            "BB_D1": [100.0],
            "BB_D2": [200.0],
            "BB_D3": [300.0],
            "ASPHALT_TEMP": [20.0],
            "SURF_THICKNESS": [100.0],
            "OBJECTID": [1],
            "SURVEY_DATE": _DATES[:1],
            "UPDATE_DATE": _DATES[:1],
        }
    )


_LWD_COLUMNS = FwdLwdColumns(force="LOAD_KG", d0="LWD_D0", d200="LWD_D1")


def _stub_temp_correction(self, lookup_table, source_col, corrected_col):
    """Identity temperature correction (no DB lookup)."""
    self.sorted[corrected_col] = self.sorted[source_col]


def _stub_conversion(self, lookup_table, source_col, output_col):
    """Identity conversion (no DB lookup)."""
    self.sorted[output_col] = self.sorted[source_col]


@pytest.fixture(autouse=True)
def stub_db(monkeypatch):
    monkeypatch.setattr(FwdDeflection, "_apply_temp_correction", _stub_temp_correction)
    monkeypatch.setattr(LwdDeflection, "_apply_temp_correction", _stub_temp_correction)
    monkeypatch.setattr(BbDeflection, "_apply_temp_correction", _stub_temp_correction)
    monkeypatch.setattr(BbDeflection, "_apply_conversion", _stub_conversion)


def test_factory_returns_correct_subclass():
    df = _tiny_df()
    assert isinstance(create_deflection("FWD", df), FwdDeflection)
    assert isinstance(create_deflection("LWD", df), LwdDeflection)
    assert isinstance(create_deflection("BB", df), BbDeflection)

    with pytest.raises(ValueError):
        create_deflection("XYZ", df)


def test_fwd_requires_survey_direc():
    columns = FwdLwdColumns(
        force="FORCE", d0="FWD_D1", d200="FWD_D2", survey_direc=None
    )
    with pytest.raises(ValueError):
        FwdDeflection(_fwd_df(), columns=columns)


def test_empty_input_does_not_crash():
    cases = [
        ("FWD", _fwd_df().iloc[0:0], None),
        ("LWD", _lwd_df().iloc[0:0], _LWD_COLUMNS),
        ("BB", _bb_df().iloc[0:0], None),
    ]
    for data_type, df, columns in cases:
        defl = create_deflection(data_type, df, columns=columns)
        assert isinstance(defl.sorted, pd.DataFrame)
        assert len(defl.sorted) == 0


def test_all_null_force_returns_empty():
    df = _fwd_df()
    df["FORCE"] = np.nan
    defl = create_deflection("FWD", df)
    assert isinstance(defl.sorted, pd.DataFrame)
    assert len(defl.sorted) == 0


def test_lwd_input_without_survey_date():
    df = _lwd_df()
    assert "SURVEY_DATE" not in df.columns

    defl = create_deflection("LWD", df, columns=_LWD_COLUMNS)

    assert len(defl.sorted) > 0
    for col in ("OBJECTID", "SURVEY_DATE", "UPDATE_DATE"):
        assert col not in defl.sorted.columns


def test_fwd_normalization_and_curvature():
    defl = create_deflection("FWD", _fwd_df())
    assert len(defl.sorted) > 0

    expected_norm_d1 = (40.0 / defl.sorted["FORCE"]) * (defl.sorted["FWD_D1"] / 1000.0)
    expected_norm_d2 = (40.0 / defl.sorted["FORCE"]) * (defl.sorted["FWD_D2"] / 1000.0)
    assert np.allclose(defl.sorted["NORM_FWD_D1"], expected_norm_d1)
    assert np.allclose(defl.sorted["NORM_FWD_D2"], expected_norm_d2)
    assert np.allclose(
        defl.sorted["D0_D200"],
        defl.sorted["NORM_FWD_D1"] - defl.sorted["NORM_FWD_D2"],
    )


def test_bb_raw_deflection_formulas():
    defl = create_deflection("BB", _bb_df())
    assert len(defl.sorted) > 0

    assert np.allclose(defl.sorted["BB_D0"], defl.sorted["BB_D3"] - defl.sorted["BB_D1"])
    assert np.allclose(
        defl.sorted["BB_D0_D200"], defl.sorted["BB_D2"] - defl.sorted["BB_D1"]
    )


def test_bb_columns_have_no_force():
    assert not hasattr(BbColumns(), "force")

    cols = FwdLwdColumns(force="FORCE", d0="FWD_D1", d200="FWD_D2")
    assert cols.force == "FORCE"
    assert cols.d0 == "FWD_D1"
    assert cols.d200 == "FWD_D2"


def test_sort_only_skips_calculation():
    defl = create_deflection("FWD", _fwd_df(), sort_only=True)
    assert len(defl.sorted) > 0
    assert "NORM_FWD_D1" not in defl.sorted.columns


def test_legacy_shim_backward_compatible():
    defl = Deflection(
        df=_fwd_df(),
        force_col="FORCE",
        data_type="FWD",
        d0_col="FWD_D1",
        d200_col="FWD_D2",
        asp_temp="ASPHALT_TEMP",
        from_m_col=None,
        to_m_col=None,
        sta_col="STA",
        conn=None,
    )

    assert len(defl.sorted) > 0
    assert "NORM_FWD_D1" in defl.sorted.columns
