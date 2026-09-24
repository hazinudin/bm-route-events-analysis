"""
Database context for aadt_recalc.

Centralises connection handling and temp-table bookkeeping so that
calculator classes stay side-effect-free.
"""

import pandas as pd
from .db_conn import smd_connection
from . import config


class DBContext:
    """
    Thin wrapper around the shared Oracle connection.

    Responsibilities
    ----------------
    * Provide the raw ``cx_Oracle`` connection / cursor.
    * Populate ``TEMP_REKAP_ROUTE_SELECTION`` (and ``TEMP_RTC`` when needed).
    * Run SQL queries and return ``pandas.DataFrame`` objects.
    """

    def __init__(self, connection=None):
        if connection is None:
            connection = smd_connection
        self.connection = connection
        self.cursor = connection.cursor()

    # ------------------------------------------------------------------
    # Temp-table helpers
    # ------------------------------------------------------------------
    def clear_temp_routes(self, table=config.TEMP_ROUTE_TABLE, col=config.ROUTEID_COL):
        """Purge the global temporary table used for route filtering."""
        self.cursor.execute(f"DELETE FROM {table}")

    def insert_temp_routes(self, routes, table=config.TEMP_ROUTE_TABLE, col=config.ROUTEID_COL):
        """
        Insert a list of route IDs into the global temporary table.

        Parameters
        ----------
        routes : list[str] or str
            Route identifier(s).  The string ``'ALL'`` is a no-op.
        """
        if str(routes) == 'ALL':
            return

        if isinstance(routes, str):
            routes = [routes]
        else:
            routes = list(routes)

        self.clear_temp_routes(table, col)
        data = [[r] for r in routes]
        self.cursor.executemany(
            f"INSERT INTO {table} ({col}) VALUES (:1)",
            data
        )

    def insert_temp_rtc(self, df, table=config.TEMP_RTC_TABLE):
        """
        Insert a user-supplied RTC DataFrame into a temporary table.
        (Mirrors ``TrafficSummary._sql_insert_temp_rtc``.)
        """
        # Drop columns that should not be inserted
        drop_cols = ['SURV_TOOL_ID', 'CONSULTANT_ID', 'TEAM_LEAD_ID']
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        columns = df.columns.tolist()
        col_str = str(columns).replace("'", "").replace("[", "(").replace("]", ")")
        value_bind = ",".join([f":{i + 1}" for i in range(len(columns))])

        insert = f"INSERT INTO {table} {col_str} VALUES ({value_bind})"
        values = [row.values for _, row in df.iterrows()]
        self.cursor.executemany(insert, values)

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------
    def read_sql(self, sql):
        """Execute *sql* and return a ``pandas.DataFrame``."""
        return pd.read_sql(sql, con=self.connection)

    def commit(self):
        """Commit the current transaction."""
        self.connection.commit()

    def close(self):
        """Close the cursor (the shared connection is left open)."""
        self.cursor.close()
