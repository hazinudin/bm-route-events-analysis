"""Backward-compatible ``Deflection`` shim.

Maps the legacy flat kwargs onto the per-type column dataclasses and delegates
to :func:`create_deflection`.  New code should use the subclasses / factory
directly.
"""
from __future__ import annotations

from .columns import BbColumns, FwdLwdColumns
from .factory import create_deflection


def Deflection(
    df=None,
    *,
    force_col=None,
    data_type=None,
    d0_col=None,
    d200_col=None,
    asp_temp=None,
    routeid_col="LINKID",
    from_m_col="FROM_STA",
    to_m_col="TO_STA",
    survey_direc="SURVEY_DIREC",
    surf_thickness_col="SURF_THICKNESS",
    force_ref=40,
    routes="ALL",
    sort_only=False,
    sta_col=None,
    conn=None,
    **kwargs,
):
    if data_type == "BB":
        columns = BbColumns(
            routeid=routeid_col,
            sta=sta_col,
            from_m=from_m_col,
            to_m=to_m_col,
            survey_direc=survey_direc,
            asphalt_temp=asp_temp,
            surf_thickness=surf_thickness_col,
        )
    else:
        columns = FwdLwdColumns(
            force=force_col,
            d0=d0_col,
            d200=d200_col,
            routeid=routeid_col,
            sta=sta_col,
            from_m=from_m_col,
            to_m=to_m_col,
            survey_direc=survey_direc,
            asphalt_temp=asp_temp,
            surf_thickness=surf_thickness_col,
        )

    return create_deflection(
        data_type,
        df,
        columns=columns,
        force_ref=force_ref,
        routes=routes,
        sort_only=sort_only,
        conn=conn,
    )
