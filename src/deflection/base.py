"""Base classes for the per-type deflection calculations.

``DeflectionBase`` holds route filtering, the reference-force sorter and the
database lookup engine shared by every deflection family.  Subclasses declare
their schema (``COLUMNS``) and behavior flags, then implement ``_calculate``.

``NormalizedDeflectionBase`` adds the shared FWD/LWD normalization and
temperature-correction pipeline on top.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import Engine

from .writer import write_deflection_results


class DeflectionBase:
    DATA_TYPE: str = ""
    TARGET_TABLE: str = ""
    REQUIRES_SORTING: bool = False
    REQUIRES_SURVEY_DIREC: bool = False
    COLUMNS = None
    METADATA_COLS_TO_DROP = ("OBJECTID", "SURVEY_DATE", "UPDATE_DATE")

    def __init__(
        self,
        df: pd.DataFrame,
        *,
        columns=None,
        force_ref: int = 40,
        routes="ALL",
        sort_only: bool = False,
        conn: Engine = None,
    ):
        self.conn = conn
        self.columns = columns or self.COLUMNS

        self.force_col = getattr(self.columns, "force", None)
        self.route_col = self.columns.routeid
        self.from_m = self.columns.from_m
        self.to_m = self.columns.to_m
        self.sta_m = self.columns.sta
        self.survey_direc = self.columns.survey_direc
        self.surf_thickness_col = self.columns.surf_thickness
        self.asp_temp = self.columns.asphalt_temp
        self.d0_col = getattr(self.columns, "d0", None)
        self.d200_col = getattr(self.columns, "d200", None)
        self.force_ref = force_ref
        self.sort_only = sort_only
        self.data_type = self.DATA_TYPE

        self._validate()

        self.df = self._filter_routes(df, routes)
        self._routes = routes
        self.sorted = self._sort_if_needed()

        if not sort_only:
            self._calculate()
            self.sorted.drop(
                list(self.METADATA_COLS_TO_DROP), axis=1, inplace=True, errors="ignore"
            )

    def _validate(self):
        if self.REQUIRES_SURVEY_DIREC and self.survey_direc is None:
            raise ValueError("Type is FWD but survey_direc is None")

        if (self.from_m is None or self.to_m is None) and self.sta_m is None:
            raise ValueError("from_m or to_m is None and sta_m column is also None.")

    def _filter_routes(self, df: pd.DataFrame, routes) -> pd.DataFrame:
        if routes == "ALL":
            return df.copy(deep=True)
        if isinstance(routes, list):
            return df.loc[df[self.route_col].isin(routes)].copy(deep=True)
        return df.loc[df[self.route_col] == routes].copy(deep=True)

    def _sort_if_needed(self) -> pd.DataFrame:
        if self.df.empty:
            return self.df.copy()
        if not self.REQUIRES_SORTING:
            return self.df
        return self._sort_by_reference_force()

    def _sort_by_reference_force(self) -> pd.DataFrame:
        if self.df[self.force_col].isnull().all():
            return self.df.iloc[0:0].copy()

        self.df["_ref_diff"] = self.df[self.force_col] - self.force_ref
        self.df["_ref_diff"] = self.df["_ref_diff"].abs()

        if (self.from_m is None) or (self.to_m is None):
            grouped = self.df.groupby([self.route_col, self.sta_m, self.survey_direc])
        else:
            grouped = self.df.groupby(
                [self.route_col, self.survey_direc, self.from_m, self.to_m]
            )

        closest_index = grouped["_ref_diff"].idxmin()
        closest_row = self.df.loc[closest_index].drop("_ref_diff", axis=1)
        return closest_row.reset_index(drop=True)

    def _calculate(self):
        raise NotImplementedError

    def _ampt_tlap(self) -> pd.Series:
        return 41 / abs(self.sorted[self.asp_temp])

    def _read_lookup_table(self, table: str) -> pd.DataFrame:
        lookup_df = pd.read_sql(f"SELECT * FROM {table}", con=self.conn).rename(
            columns=str.upper
        )
        if "OBJECTID" in lookup_df.columns:
            lookup_df = lookup_df.drop("OBJECTID", axis=1)
        return lookup_df

    @staticmethod
    def _tenths_key(series: pd.Series) -> pd.Series:
        rounded = series.round(1)
        scaled = (rounded * 10).round()
        scaled = scaled.where(rounded < 1.8, 18)  # values >=1.8 AND NaN map to 18
        return scaled.astype(int)

    def _apply_temp_correction(self, lookup_table, source_col, corrected_col):
        lookup_df = self._read_lookup_table(lookup_table)
        lookup_df.set_index("AMPT_TLAP", inplace=True)
        lookup_df.index = lookup_df.index.map(float)
        lookup_df.columns = lookup_df.columns.str.replace("TH", "")
        lookup_df.columns = lookup_df.columns.map(int)
        lookup_thickness = list(lookup_df)

        lookup_melt = pd.melt(lookup_df, ignore_index=False).reset_index()
        lookup_melt = lookup_melt.rename(columns={"AMPT_TLAP": "KEY"})
        lookup_melt["KEY"] = self._tenths_key(lookup_melt["KEY"])

        ampt_key = self._tenths_key(self._ampt_tlap())
        thickness = self.sorted[self.columns.surf_thickness].apply(
            lambda x: lookup_thickness[np.argmin([abs(_ - x) for _ in lookup_thickness])]
        )
        input_df = pd.concat([ampt_key, thickness], axis=1)
        input_df.columns = [self.columns.asphalt_temp, self.columns.surf_thickness]

        temp_factor = input_df.merge(
            lookup_melt,
            left_on=[self.columns.asphalt_temp, self.columns.surf_thickness],
            right_on=["KEY", "variable"],
            how="left",
        )["value"]

        self.sorted[corrected_col] = self.sorted[source_col] * temp_factor
        return self

    def _apply_conversion(self, lookup_table, source_col, output_col):
        lookup_df = self._read_lookup_table(lookup_table)
        lookup_thickness = lookup_df["SURF_THICKNESS"].tolist()
        lookup_factor = lookup_df["FACTOR"].tolist()

        conversion_factor = self.sorted[self.columns.surf_thickness].apply(
            lambda x: lookup_factor[np.argmin([abs(_ - x) for _ in lookup_thickness])]
        )

        self.sorted[output_col] = self.sorted[source_col] * conversion_factor
        return self

    def save_results(self, year: int, semester: int | None = None) -> int:
        """Persist ``self.sorted`` to the subclass target table; returns rows written."""
        if self.conn is None:
            raise ValueError("save_results requires a database connection (conn=...)")
        if self.sorted is None or self.sorted.empty:
            raise ValueError("No calculation results to save (sorted is empty)")

        connection = self.conn.raw_connection()
        try:
            return write_deflection_results(
                connection, self.sorted, self.DATA_TYPE, year, self._routes, semester
            )
        finally:
            connection.close()


class NormalizedDeflectionBase(DeflectionBase):
    def __init__(
        self,
        df: pd.DataFrame,
        *,
        columns=None,
        force_ref: int = 40,
        routes="ALL",
        sort_only: bool = False,
        conn: Engine = None,
    ):
        super().__init__(
            df,
            columns=columns,
            force_ref=force_ref,
            routes=routes,
            sort_only=sort_only,
            conn=conn,
        )
        self._set_derived_names()

    def _set_derived_names(self):
        self.norm_d0 = f"NORM_{self.d0_col}"
        self.norm_d200 = f"NORM_{self.d200_col}"
        self.corr_d0 = f"CORR_{self.d0_col}"
        self.corr_d200 = f"CORR_{self.d200_col}"
        self.curvature = "D0_D200"
        self.corr_curvature = "CORR_D0_D200"

    def _calculate(self):
        self._set_derived_names()
        if self.sorted.empty:
            return

        result = self.sorted[[self.d0_col, self.d200_col, self.force_col]].apply(
            lambda x: (self.force_ref / x[self.force_col]) * (x / 1000), axis=1
        )
        self.sorted[[self.norm_d0, self.norm_d200]] = result[
            [self.d0_col, self.d200_col]
        ]
        self.sorted[self.curvature] = (
            self.sorted[self.norm_d0] - self.sorted[self.norm_d200]
        )

        self._apply_temp_correction(
            "D200_TEMP_CORRECTION", self.norm_d200, self.corr_d200
        )
        self._apply_temp_correction("D0_TEMP_CORRECTION", self.norm_d0, self.corr_d0)
        self.sorted[self.corr_curvature] = (
            self.sorted[self.corr_d0] - self.sorted[self.corr_d200]
        )
