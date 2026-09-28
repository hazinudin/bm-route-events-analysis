"""Frozen per-type column configuration for the deflection classes."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FwdLwdColumns:
    force: str
    d0: str
    d200: str
    routeid: str = "LINKID"
    sta: str | None = "STA"
    from_m: str | None = None
    to_m: str | None = None
    survey_direc: str | None = "SURVEY_DIREC"
    asphalt_temp: str = "ASPHALT_TEMP"
    surf_thickness: str = "SURF_THICKNESS"


@dataclass(frozen=True)
class BbColumns:
    routeid: str = "LINKID"
    sta: str | None = "STA"
    from_m: str | None = None
    to_m: str | None = None
    survey_direc: str | None = "SURVEY_DIREC"
    asphalt_temp: str = "ASPHALT_TEMP"
    surf_thickness: str = "SURF_THICKNESS"
    bb_d1: str = "BB_D1"
    bb_d2: str = "BB_D2"
    bb_d3: str = "BB_D3"
