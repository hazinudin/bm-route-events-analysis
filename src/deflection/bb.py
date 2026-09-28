"""Benkelman Beam (BB) deflection calculation."""
from __future__ import annotations

from .base import DeflectionBase
from .columns import BbColumns


class BbDeflection(DeflectionBase):
    DATA_TYPE = "BB"
    TARGET_TABLE = "SMD.BB"
    COLUMNS = BbColumns()

    def _calculate(self):
        if self.sorted.empty:
            return

        self.sorted["BB_D0"] = (
            self.sorted[self.columns.bb_d3] - self.sorted[self.columns.bb_d1]
        )
        self.sorted["BB_D0_D200"] = (
            self.sorted[self.columns.bb_d2] - self.sorted[self.columns.bb_d1]
        )

        self._apply_temp_correction(
            "BB_D0_TEMP_CORRECTION", "BB_D0", "BB_D0_CORR"
        )
        self._apply_temp_correction(
            "BB_D0_D200_TEMP_CORRECTION", "BB_D0_D200", "BB_D0_D200_CORR"
        )

        self._apply_conversion("BB_D0_FWD_CONVERSION", "BB_D0_CORR", "FWD_D0")
        self._apply_conversion(
            "BB_D0_D200_FWD_CONVERSION", "BB_D0_D200_CORR", "FWD_D0_D200"
        )
