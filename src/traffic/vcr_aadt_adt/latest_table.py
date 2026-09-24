"""
Standalone latest-table resolver (replaces SMD_Package.FindLatest.LatestTable).

Discovers the newest available table following the naming convention
``{prefix}_{semester}_{year}``.
"""

import pandas as pd
from .smd_config import SMDConfigs
from .db_conn import smd_connection


def _table_exists(table_name, connection):
    """
    Check whether *table_name* exists in Oracle by querying ``all_tables``.

    Parameters
    ----------
    table_name : str
        Table name, optionally schema-qualified (e.g. ``SMD.RNI_2_2021``).
    connection : cx_Oracle.Connection

    Returns
    -------
    bool
    """
    if '.' in table_name:
        owner, name = table_name.split('.', 1)
        sql = (
            "SELECT 1 FROM all_tables "
            "WHERE owner = :1 AND table_name = :2"
        )
        params = [owner.upper(), name.upper()]
    else:
        sql = "SELECT 1 FROM all_tables WHERE table_name = :1"
        params = [table_name.upper()]

    df = pd.read_sql(sql, con=connection, params=params)
    return not df.empty


class LatestTable(object):
    """
    Find the latest existing table for a given prefix and year.

    Mirrors the original ``SMD_Package.FindLatest.LatestTable`` logic
    byte-for-byte, but uses Oracle ``all_tables`` instead of ``arcpy.Exists``.
    """

    def __init__(self, year, table_prefix):
        self.semester = 2
        self.input_year = year
        self.latest_year = year

        while not _table_exists(
            table_prefix + "_{0}_{1}".format(self.semester, self.latest_year),
            smd_connection,
        ):
            if self.semester == 1:
                self.latest_year = year - 1
                self.semester = 2
            else:
                self.semester = 1

            if self.latest_year < 2019:
                raise ValueError("Previous table does not exist.")

        self.latest_table = table_prefix + "_{0}_{1}".format(self.semester, self.latest_year)

    def in_year(self):
        """Return *True* if the resolved table belongs to the requested year."""
        return self.input_year == self.latest_year
