"""LWD deflection calculation."""
from __future__ import annotations

from .base import NormalizedDeflectionBase
from .columns import FwdLwdColumns


class LwdDeflection(NormalizedDeflectionBase):
    DATA_TYPE = "LWD"
    TARGET_TABLE = "SMD.LWD"
    COLUMNS = FwdLwdColumns(force="LOAD_KG", d0="LWD_D0", d200="LWD_D1")
