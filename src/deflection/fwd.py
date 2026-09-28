"""FWD deflection calculation."""
from __future__ import annotations

from .base import NormalizedDeflectionBase
from .columns import FwdLwdColumns


class FwdDeflection(NormalizedDeflectionBase):
    DATA_TYPE = "FWD"
    TARGET_TABLE = "SMD.FWD"
    REQUIRES_SORTING = True
    REQUIRES_SURVEY_DIREC = True
    COLUMNS = FwdLwdColumns(force="FORCE", d0="FWD_D1", d200="FWD_D2")
