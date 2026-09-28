"""Factory for building the correct deflection subclass from a data type."""
from __future__ import annotations

from .bb import BbDeflection
from .fwd import FwdDeflection
from .lwd import LwdDeflection


def create_deflection(
    data_type,
    df,
    *,
    columns=None,
    force_ref=40,
    routes="ALL",
    sort_only=False,
    conn=None,
):
    classes = {"FWD": FwdDeflection, "LWD": LwdDeflection, "BB": BbDeflection}
    if data_type not in classes:
        raise ValueError(f"Unknown data_type: {data_type!r}")
    return classes[data_type](
        df,
        columns=columns,
        force_ref=force_ref,
        routes=routes,
        sort_only=sort_only,
        conn=conn,
    )
