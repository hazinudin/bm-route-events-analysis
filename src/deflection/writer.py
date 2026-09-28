"""Idempotent database writeback for deflection calculation results.

Mirrors the retry-safe pattern of ``traffic.vcr_aadt_adt.writer``: existing
rows for the target ``YEAR`` (narrowed by route) are deleted before the new
rows are inserted, so redeliveries and retry copies are safe.
"""
from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from worker.db import to_python

TARGET_TABLES = {"FWD": "SMD.FWD", "LWD": "SMD.LWD", "BB": "SMD.BB"}

TARGET_COLUMNS = {
    "FWD": frozenset({
        "OBJECTID", "AIR_TEMP", "TO_STA", "TEAM_LEAD_ID", "SURVEY_YEAR", "SURVEY_DATE",
        "SURVEY_DIREC", "FROM_STA", "FWD_D9", "STRESS", "FORCE", "CONSULTANT_ID",
        "NORM_FWD_D1", "NORM_FWD_D2", "SURF_TEMP", "ASPHALT_TEMP", "BM_PROV_ID",
        "SURF_THICKNESS", "FWD_D8", "DEFL_LAT", "FWD_D4", "FWD_D5", "FWD_D6", "FWD_D7",
        "FWD_D1", "FWD_D2", "FWD_D3", "YEAR", "BALAI_ID", "D0_D200", "DEFL_LONG",
        "LINKID", "CORR_FWD_D1", "CORR_FWD_D2", "CORR_D0_D200", "SEGMENT_LENGTH",
        "SATKER_PPK_ID", "SURV_TOOL_ID", "DROP_ID", "UPDATE_DATE", "STA", "LANE_CODE",
        "SEMESTER", "COPIED",
    }),
    "LWD": frozenset({
        "OBJECTID", "AIR_TEMP", "TO_STA", "TEAM_LEAD_ID", "SURVEY_YEAR", "FROM_STA",
        "SURF_TEMP", "ASPHALT_TEMP", "BM_PROV_ID", "SURF_THICKNESS", "DEFL_LAT",
        "CORR_LWD_D0", "CORR_LWD_D1", "CONSULTANT_ID", "CORR_D0_D200", "D0_D200",
        "LOAD_KG", "YEAR", "BALAI_ID", "EV_D1", "EV_D0", "NORM_LWD_D0", "NORM_LWD_D1",
        "LWD_D2", "SURVEY_DIREC", "LWD_D1", "DEFL_LONG", "LINKID", "SEGMENT_LENGTH",
        "SATKER_PPK_ID", "SURV_TOOL_ID", "D0_D2", "LWD_D0", "UPDATE_DATE", "LANE_CODE",
        "STA", "SEMESTER",
    }),
    "BB": frozenset({
        "OBJECTID", "BB_D2", "SURVEY_YEAR", "TO_STA", "BB_D1", "TEAM_LEAD_ID", "OFFSET1",
        "OFFSET2", "BB_D3", "BB_D0", "FROM_STA", "BB_D0_CORR", "STA", "AIR_TEMP",
        "BB_D0_D200_CORR", "OFFSET3", "FWD_D0_D200", "BB_D0_D200", "SURF_TEMP",
        "ASPHALT_TEMP", "BM_PROV_ID", "SURF_THICKNESS", "DEFL_LAT", "CONSULTANT_ID",
        "FWD_D0", "YEAR", "BALAI_ID", "LOAD_TON", "SURVEY_DIREC", "DEFL_LONG", "LINKID",
        "LANE_CODE", "SEGMENT_LENGTH", "SATKER_PPK_ID", "SURV_TOOL_ID", "UPDATE_DATE",
        "SEMESTER",
    }),
}

_IDENTITY_COLUMNS = ("OBJECTID", "YEAR", "SEMESTER")


def write_deflection_results(
    connection: Any,
    df: pd.DataFrame | None,
    data_type: str,
    year: int,
    routes: Any = None,
    semester: int | None = None,
    *,
    target_table: str | None = None,
    object_id_provider: Callable[[Any, str, int], list[int]] | None = None,
) -> int:
    """Persist ``df`` to the per-type deflection target table; returns row count.

    The write is idempotent: rows matching ``YEAR`` (narrowed by ``routes`` when
    given) are deleted first, then the new rows are inserted.  ``data_type``
    selects both the default target table and the column whitelist; ``routes``
    values are written verbatim as bind parameters.
    """
    if data_type not in TARGET_TABLES:
        raise ValueError(f"Unknown data_type: {data_type!r}")
    if df is None or df.empty:
        return 0

    table = target_table or TARGET_TABLES[data_type]
    provider = object_id_provider or _allocate_object_ids

    cursor = connection.cursor()
    try:
        _delete_existing(cursor, table, year, routes)
        count = _insert_results(
            cursor, table, data_type, df, year, semester, provider
        )
        connection.commit()
        return count
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def next_object_id(cursor: Any, table: str) -> int:
    """Allocate the next geodatabase OBJECTID for ``table`` (bare name)."""
    # next_rowid takes TWO args (owner, table) and is the only way to get a
    # valid OBJECTID -- the target tables have no identity/default/trigger.
    cursor.execute("SELECT sde.gdb_util.next_rowid('SMD', :1) FROM DUAL", [table])
    return int(cursor.fetchone()[0])


def _allocate_object_ids(cursor: Any, table: str, count: int) -> list[int]:
    return [next_object_id(cursor, table) for _ in range(count)]


def _delete_existing(cursor: Any, table: str, year: int, routes: Any) -> None:
    sql = f"DELETE FROM {table} WHERE YEAR = :1"
    params: list[Any] = [year]

    narrow = routes is not None and routes != "ALL"
    if narrow and isinstance(routes, (list, tuple, set)):
        route_list = list(routes)
        if route_list:
            placeholders = ", ".join(f":{i + 2}" for i in range(len(route_list)))
            sql += f" AND LINKID IN ({placeholders})"
            params.extend(route_list)
    elif narrow:
        sql += " AND LINKID = :2"
        params.append(routes)

    cursor.execute(sql, params)


def _insert_results(
    cursor: Any,
    table: str,
    data_type: str,
    df: pd.DataFrame,
    year: int,
    semester: int | None,
    object_id_provider: Callable[[Any, str, int], list[int]],
) -> int:
    write_columns = [
        col
        for col in df.columns
        if col in TARGET_COLUMNS[data_type] and col not in _IDENTITY_COLUMNS
    ]
    insert_columns = ["OBJECTID", *write_columns, "YEAR", "SEMESTER"]

    object_ids = object_id_provider(cursor, data_type, len(df))

    col_str = ", ".join(insert_columns)
    placeholders = ", ".join(f":{i + 1}" for i in range(len(insert_columns)))
    insert_sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

    values = [
        (oid, *(to_python(value) for value in row), year, semester)
        for oid, row in zip(object_ids, df[write_columns].to_numpy())
    ]

    cursor.executemany(insert_sql, values)
    return len(df)
