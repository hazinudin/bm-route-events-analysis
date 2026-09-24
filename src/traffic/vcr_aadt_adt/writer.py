"""
DataFrame persistence helpers for AADT / VCR calculation results.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def write_aadt_results(
    connection: Any,
    df: pd.DataFrame,
    year: int,
    semester: int,
    routes: list[str] | None,
    target_table: str,
) -> int:
    """
    Persist ``AADTPipeline`` output to an Oracle target table.

    The operation is idempotent: existing rows for ``(year, semester)`` are
    deleted first (narrowed by ``routes`` when provided), then the new rows
    are inserted.  This makes redeliveries and retry copies safe.

    Parameters
    ----------
    connection : DB-API connection
        Oracle connection with an active transaction.
    df : pd.DataFrame
        Output from ``AADTPipeline.calculate``.
    year : int
        Calculation year.
    semester : int
        Calculation semester.
    routes : list[str] | None
        Route filter used for the calculation.  ``None`` or ``'ALL'`` means
        no additional narrowing.
    target_table : str
        Fully-qualified table name, e.g. ``SMD.AADT``.

    Returns
    -------
    int
        Number of rows inserted.
    """
    cursor = connection.cursor()
    try:
        _delete_existing(cursor, target_table, year, semester, routes)
        row_count = _insert_results(cursor, df, target_table, year, semester)
        connection.commit()
        return row_count
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def _delete_existing(
    cursor: Any,
    target_table: str,
    year: int,
    semester: int,
    routes: list[str] | None,
) -> None:
    where = "WHERE YEAR = :1 AND SEMESTER = :2"
    params: list[Any] = [year, semester]

    if routes and str(routes) != "ALL":
        route_list = list(routes)
        placeholders = ", ".join(f":{i + 3}" for i in range(len(route_list)))
        where += f" AND LINKID IN ({placeholders})"
        params.extend(route_list)

    cursor.execute(f"DELETE FROM {target_table} {where}", params)


def _insert_results(
    cursor: Any,
    df: pd.DataFrame,
    target_table: str,
    year: int,
    semester: int,
) -> int:
    write_df = df.copy()
    write_df["YEAR"] = year
    write_df["SEMESTER"] = semester

    # Normalise LINKID to uppercase strings.
    if "LINKID" in write_df.columns:
        write_df["LINKID"] = write_df["LINKID"].astype(str).str.upper()

    columns = write_df.columns.tolist()
    col_str = ", ".join(columns)
    placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
    insert_sql = f"INSERT INTO {target_table} ({col_str}) VALUES ({placeholders})"

    values = [
        tuple(_to_python(v) for v in row)
        for row in write_df[columns].to_numpy()
    ]

    cursor.executemany(insert_sql, values)
    return len(write_df)


def _to_python(value: Any) -> Any:
    """Convert numpy/pandas scalars to plain Python objects for Oracle."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value
