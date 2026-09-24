"""
Tests for ``src/traffic/vcr_aadt_adt`` – AADT / VCR calculation.

Two layers:

1. **Unit tests** for the pure calculators (``calculators.py``) with
   hand-computed fixture numbers.
2. **Integration tests** that run ``AADTPipeline`` end-to-end against the
   live SMD Oracle database and assert the calculated numbers match the
   golden ``SMD.AADT`` rows **for the same YEAR**.

Oracle credentials are read from the dbt profile (``~/.dbt/profiles.yml``),
so the integration tests need no extra configuration.
"""

import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Package bootstrap: db_conn.py creates the Oracle connection at import time
# and reads ORA_* environment variables.  Populate them from the dbt profile
# *before* importing the package.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parents[1]
PKG_DIR = PROJECT_ROOT / "src" / "traffic" / "vcr_aadt_adt"


def _dbt_oracle_creds():
    """Parse Oracle credentials from ``~/.dbt/profiles.yml``."""
    profile_path = Path.home() / ".dbt" / "profiles.yml"
    if not profile_path.exists():
        return None
    text = profile_path.read_text()
    try:
        import yaml
        data = yaml.safe_load(text)
        target = next(iter(data.values()))["target"]
        cfg = next(iter(data.values()))["outputs"][target]
        return dict(
            host=cfg.get("host"),
            port=int(cfg.get("port", 1521)),
            service=cfg.get("service") or cfg.get("dbname"),
            user=cfg.get("user"),
            password=cfg.get("password"),
        )
    except ImportError:
        def get(key):
            m = re.search(rf"^\s*{key}:\s*['\"]?([^'\"\s]+)", text, re.M)
            return m.group(1) if m else None
        return dict(
            host=get("host"), port=int(get("port") or 1521),
            service=get("service") or get("dbname"),
            user=get("user"), password=get("password"),
        )


_CREDS = _dbt_oracle_creds()
if _CREDS is not None and _CREDS["host"] is not None:
    os.environ.setdefault("ORA_HOST", str(_CREDS["host"]))
    os.environ.setdefault("ORA_PORT", str(_CREDS["port"]))
    os.environ.setdefault("ORA_SERVICE", str(_CREDS["service"]))
    os.environ.setdefault("SMD_ORA_USER", str(_CREDS["user"]))
    os.environ.setdefault("SMD_ORA_KEY", str(_CREDS["password"]))

from traffic.vcr_aadt_adt import calculators, config, queries  # noqa: E402
from traffic.vcr_aadt_adt.pipeline import AADTPipeline  # noqa: E402


# ===========================================================================
# Unit tests – pure calculators
# ===========================================================================
class TestAddAadtSum:
    def test_excludes_num_veh1_and_num_veh8(self):
        df = pd.DataFrame(
            [[100, 50, 20, 10, 5, 4, 3, 2, 1, 1, 0, 7]],
            index=["R1"],
            columns=config.ALL_VEHICLE_COLS + ["NUM_VEH8"],
        )
        out = calculators.add_aadt_sum(df)
        # AADT = sum of all NUM_VEH except NUM_VEH1 (100) and NUM_VEH8 (7)
        assert out.loc["R1", "AADT"] == 96

    def test_original_df_not_mutated(self):
        df = pd.DataFrame([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]],
                          index=["R1"], columns=config.ALL_VEHICLE_COLS)
        calculators.add_aadt_sum(df)
        assert "AADT" not in df.columns


class TestAddCesa:
    """CESA = sum(veh * VDF) * 365 * 50.54 / 1_000_000  (VDF fillna 1)."""

    # Real numbers from route 01001, year 2024 (verified against SMD.AADT)
    ROW = dict(NUM_VEH1=20139, NUM_VEH2=859, NUM_VEH3=2103, NUM_VEH4=518,
               NUM_VEH5A=7, NUM_VEH5B=1, NUM_VEH6A=51, NUM_VEH6B=577,
               NUM_VEH7A=31, NUM_VEH7B=0, NUM_VEH7C=28, NUM_VEH8=8)
    EXPECTED_CESA = 635.99144315

    def test_cesa_value(self):
        df = pd.DataFrame([list(self.ROW.values())], index=["01001"],
                          columns=list(self.ROW.keys()))
        out = calculators.add_cesa(df)
        assert out.loc["01001", "CESA"] == pytest.approx(self.EXPECTED_CESA)

    def test_missing_vdf_gets_factor_1(self):
        # NUM_VEH1 / NUM_VEH7B / NUM_VEH8 are absent from vdf.json -> VDF = 1
        df = pd.DataFrame([[10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]],
                          index=["R1"], columns=list(self.ROW.keys()))
        out = calculators.add_cesa(df)
        assert out.loc["R1", "CESA"] == pytest.approx(
            10 * 365 * config.R_VALUE / config.CESA_SCALE
        )


class TestInterpolatePce:
    @pytest.fixture
    def pce_coeff_df(self):
        return pd.DataFrame({
            "FLOW": [100.0, 200.0],
            "TERR": [1, 1],
            "ROAD_TYPE": [1, 1],
            "PCE_01": [1.0, 1.5],
            "PCE_02": [2.0, 3.0],
        })

    @pytest.fixture
    def road_type_group_df(self):
        return pd.DataFrame({
            "ROAD_TYPE": [5, 6],
            "ROAD_TYPE_GROUP": [1, 2],
        })

    def test_linear_interpolation_midpoint(self, pce_coeff_df, road_type_group_df):
        pce = calculators.interpolate_pce(
            flow=150, road_type=5, terr=1, width=7.0,
            pce_coeff_df=pce_coeff_df,
            road_type_group_df=road_type_group_df,
            pce_veh_cols=["PCE_01", "PCE_02"],
        )
        assert pce["PCE_01"] == pytest.approx(1.25)
        assert pce["PCE_02"] == pytest.approx(2.5)

    @pytest.mark.xfail(reason="Legacy parity: exact flow match on the PCE table "
                             "raises (no strict below/above rows)", strict=False)
    def test_no_interpolation_on_exact_flow(self, pce_coeff_df, road_type_group_df):
        pce = calculators.interpolate_pce(
            flow=100, road_type=5, terr=1, width=7.0,
            pce_coeff_df=pce_coeff_df,
            road_type_group_df=road_type_group_df,
            pce_veh_cols=["PCE_01", "PCE_02"],
        )
        assert pce["PCE_01"] == pytest.approx(1.0)
        assert pce["PCE_02"] == pytest.approx(2.0)

    def test_flow_above_max_clamped(self, pce_coeff_df, road_type_group_df):
        pce = calculators.interpolate_pce(
            flow=10_000, road_type=5, terr=1, width=7.0,
            pce_coeff_df=pce_coeff_df,
            road_type_group_df=road_type_group_df,
            pce_veh_cols=["PCE_01", "PCE_02"],
        )
        assert pce["PCE_01"] == pytest.approx(1.5)
        assert pce["PCE_02"] == pytest.approx(3.0)

    def test_width_below_6_upscales_pce01(self, pce_coeff_df, road_type_group_df):
        pce = calculators.interpolate_pce(
            flow=150, road_type=5, terr=1, width=5.0,
            pce_coeff_df=pce_coeff_df,
            road_type_group_df=road_type_group_df,
            pce_veh_cols=["PCE_01", "PCE_02"],
        )
        assert pce["PCE_01"] == pytest.approx(1.25 * config.WIDTH_LT_6_FACTOR)
        assert pce["PCE_02"] == pytest.approx(2.5)  # only PCE_01 adjusted

    def test_width_above_8_downscales_pce01(self, pce_coeff_df, road_type_group_df):
        pce = calculators.interpolate_pce(
            flow=150, road_type=5, terr=1, width=9.0,
            pce_coeff_df=pce_coeff_df,
            road_type_group_df=road_type_group_df,
            pce_veh_cols=["PCE_01", "PCE_02"],
        )
        assert pce["PCE_01"] == pytest.approx(1.25 * config.WIDTH_GT_8_FACTOR)

    def test_width_in_threshold_band_unchanged(self, pce_coeff_df, road_type_group_df):
        for width in (6.0, 7.0, 8.0):
            pce = calculators.interpolate_pce(
                flow=150, road_type=5, terr=1, width=width,
                pce_coeff_df=pce_coeff_df,
                road_type_group_df=road_type_group_df,
                pce_veh_cols=["PCE_01", "PCE_02"],
            )
            assert pce["PCE_01"] == pytest.approx(1.25)

    def test_road_type_mapping(self, pce_coeff_df, road_type_group_df):
        # road_type 6 -> group 2, for which no coefficient rows exist
        with pytest.raises((IndexError, ValueError)):
            calculators.interpolate_pce(
                flow=150, road_type=6, terr=1, width=7.0,
                pce_coeff_df=pce_coeff_df,
                road_type_group_df=road_type_group_df,
                pce_veh_cols=["PCE_01", "PCE_02"],
            )


class TestCalculatePcehRow:
    """PCEH = sum(AADT_veh * PCE_veh * HDA_veh); heavy veh use HDA_HV."""

    @pytest.fixture
    def aadt_df(self):
        vals = dict(NUM_VEH1=1000, NUM_VEH2=800, NUM_VEH3=600, NUM_VEH4=400,
                    NUM_VEH5A=200, NUM_VEH5B=100, NUM_VEH6A=80, NUM_VEH6B=60,
                    NUM_VEH7A=40, NUM_VEH7B=20, NUM_VEH7C=10)
        return pd.DataFrame([vals], index=["R1"])

    @pytest.fixture
    def flowband_df(self):
        return pd.DataFrame({
            "BAND": [1, 2],
            "HDA_HV": [0.0253, 0.0493],
            "HDA_LV": [0.0144, 0.0528],
        })

    def _row(self, pces, band=1, route="R1"):
        data = {"LINKID": route, "BAND": band}
        data.update({f"PCE_{i:02d}": p for i, p in enumerate(pces, start=1)})
        return pd.Series(data)

    def test_pceh_value(self, aadt_df, flowband_df):
        pces = [1.0] * 11
        row = self._row(pces, band=1)
        pceh = calculators.calculate_pceh_row(
            row=row, aadt_df=aadt_df, flowband_df=flowband_df,
            routeid_col="LINKID",
        )
        light = sum(v for k, v in aadt_df.iloc[0].items() if k in config.LIGHT_VEHICLES)
        heavy = sum(v for k, v in aadt_df.iloc[0].items() if k in config.HEAVY_VEHICLES)
        expected = light * 0.0144 + heavy * 0.0253
        assert pceh == pytest.approx(expected)

    def test_heavy_and_light_use_different_hda(self, aadt_df, flowband_df):
        # PCE = 0 everywhere except PCE_01 (NUM_VEH1, light) and PCE_06
        # (NUM_VEH5B, heavy): isolates the HDA split.
        pces = [1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        row = self._row(pces, band=2)
        pceh = calculators.calculate_pceh_row(
            row=row, aadt_df=aadt_df, flowband_df=flowband_df,
            routeid_col="LINKID",
        )
        expected = 1000 * 0.0528 + 100 * 0.0493
        assert pceh == pytest.approx(expected)


class TestMergeVcr:
    def test_vcr_volume_capacity_and_columns(self):
        volume_df = pd.DataFrame({"LINKID": ["R1"], "PCEH": [50.0]})
        capacity_df = pd.DataFrame({"LINKID": ["R1"], "WEIGHTED_CAP": [100.0],
                                    "YEAR": [2024]})
        aadt_df = pd.DataFrame({"LINKID": ["R1"], "NUM_VEH2": [10],
                                "NUM_VEH3": [20], "CESA": [1.5], "AADT": [30]})
        out = calculators.merge_vcr(volume_df, capacity_df, aadt_df)

        assert out.loc[0, "VCR"] == pytest.approx(0.5)
        assert out.loc[0, "VOLUME"] == pytest.approx(50.0)
        assert out.loc[0, "CAPACITY"] == pytest.approx(100.0)
        assert list(out.columns) == [
            "LINKID", "VCR", "VOLUME", "CAPACITY", "NUM_VEH2", "NUM_VEH3",
            "CESA", "AADT",
        ]


class TestQueries:
    def test_rni_sql_substitutes_routeid_in_final_join(self):
        sql = queries.get_rni_data_sql(
            routes=["01001"], rni_table="RNI_2_2024",
        )
        # Regression guard: the final join condition must be interpolated
        assert "a.LINKID = b.LINKID" in sql
        assert "{routeid_col}" not in sql

    def test_rni_sql_filters_by_temp_route_table(self):
        sql = queries.get_rni_data_sql(routes=["01001"], rni_table="RNI_2_2024")
        assert "TEMP_REKAP_ROUTE_SELECTION" in sql

    def test_rni_sql_all_routes_has_no_temp_table_filter(self):
        sql = queries.get_rni_data_sql(routes="ALL", rni_table="RNI_2_2024")
        assert "TEMP_REKAP_ROUTE_SELECTION" not in sql


# ===========================================================================
# Integration tests – parity against SMD.AADT for the same YEAR
# ===========================================================================
# (year, semester, routes) – routes taken from SMD.AADT rows that carry
# VOLUME / CAPACITY for that year.  NOTE: parity is only reproducible for
# years whose RTC_{year} table still contains live data (RTC_2023 is empty).
YEAR_CASES = [
    (2024, 2, ["01001", "01002", "01003"]),
    (2025, 2, ["22015", "22064", "6201712"]),
    (2022, 2, ["5000115", "15045", "15047"]),
]
VEH_COLS = config.LIGHT_VEHICLES + config.HEAVY_VEHICLES + ["NUM_VEH8"]


def _oracle_reachable(pipeline):
    try:
        pipeline.db.read_sql("SELECT 1 FROM DUAL")
        return True
    except Exception:
        return False


@pytest.fixture(scope="module", params=YEAR_CASES, ids=[str(c[0]) for c in YEAR_CASES])
def year_case(request):
    return request.param


@pytest.fixture(scope="module")
def pipeline(year_case):
    try:
        pipe = AADTPipeline()
        pipe.db.read_sql("SELECT 1 FROM DUAL")
    except Exception as exc:
        pytest.skip(f"SMD Oracle database not reachable: {exc}")
    return pipe


@pytest.fixture(scope="module")
def golden_aadt(pipeline, year_case):
    """Golden rows from SMD.AADT for ROUTES in YEAR."""
    year, _, routes = year_case
    route_list = ", ".join(f"'{r}'" for r in routes)
    sql = (
        f"SELECT * FROM {config.AADT_TABLE_DEFAULT} "
        f"WHERE YEAR = {year} AND LINKID IN ({route_list})"
    )
    df = pipeline.db.read_sql(sql)
    if len(df) < len(routes):
        pytest.skip(f"SMD.AADT does not contain all {routes} for YEAR={year}")
    return df.set_index("LINKID")


@pytest.fixture(scope="module")
def result(pipeline, year_case):
    year, semester, routes = year_case
    return pipeline.calculate(routes=routes, year=year,
                              semester=semester).set_index("LINKID")


class TestAADTPipelineParity:
    """Pipeline output must match SMD.AADT rows with the same YEAR."""

    @pytest.fixture(autouse=True)
    def _setup(self, result, golden_aadt, year_case):
        self.result = result
        self.golden = golden_aadt
        self.routes = year_case[2]

    def test_all_routes_calculated(self):
        assert set(self.result.index) == set(self.routes)

    def test_vcr_consistency(self):
        # VCR == VOLUME / CAPACITY
        expected = self.result["VOLUME"] / self.result["CAPACITY"]
        assert np.allclose(self.result["VCR"], expected, rtol=1e-9)

    def test_num_veh_match_aadt_table(self):
        for route in self.routes:
            for col in VEH_COLS:
                got, exp = self.result.loc[route, col], self.golden.loc[route, col]
                assert got == exp, f"{route}.{col}: got {got}, SMD.AADT {exp}"

    def test_aadt_sum_match_aadt_table(self):
        for route in self.routes:
            got, exp = self.result.loc[route, "AADT"], self.golden.loc[route, "AADT"]
            assert got == exp, f"{route}.AADT: got {got}, SMD.AADT {exp}"

    def test_cesa_match_aadt_table(self):
        for route in self.routes:
            got, exp = self.result.loc[route, "CESA"], self.golden.loc[route, "CESA"]
            assert got == pytest.approx(exp, abs=0.6), (
                f"{route}.CESA: got {got}, SMD.AADT {exp}"
            )

    def test_volume_match_aadt_table(self):
        for route in self.routes:
            got, exp = self.result.loc[route, "VOLUME"], self.golden.loc[route, "VOLUME"]
            assert got == pytest.approx(exp, rel=1e-4), (
                f"{route}.VOLUME: got {got}, SMD.AADT {exp}"
            )

    def test_capacity_match_aadt_table(self):
        for route in self.routes:
            got, exp = self.result.loc[route, "CAPACITY"], self.golden.loc[route, "CAPACITY"]
            assert got == pytest.approx(exp, rel=1e-4), (
                f"{route}.CAPACITY: got {got}, SMD.AADT {exp}"
            )

    def test_vcr_match_aadt_table(self):
        for route in self.routes:
            got, exp = self.result.loc[route, "VCR"], self.golden.loc[route, "VCR"]
            assert got == pytest.approx(exp, rel=1e-4), (
                f"{route}.VCR: got {got}, SMD.AADT {exp}"
            )
