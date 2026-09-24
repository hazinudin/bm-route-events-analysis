"""
Standalone database connection module (replaces SMD_Package.db_conn).

Keeps the same global-connection pattern so that ``AADTPipeline`` can be
dropped in as a replacement without changing caller code.
"""

import os
from dotenv import load_dotenv
from .smd_config import SMDConfigs

# ---------------------------------------------------------------------------
# Load environment variables exactly the same way as the original module.
# ---------------------------------------------------------------------------
conf = SMDConfigs()
_env_path = os.path.join(conf.smd_dir(), 'SMD_Package', '.env')
if os.path.isfile(_env_path):
    load_dotenv(_env_path)

ORA_HOST = os.getenv('ORA_HOST')
ORA_PORT = os.getenv('ORA_PORT')
ORA_SERVICE = os.getenv('ORA_SERVICE')
ORA_CLIENT_DIR = os.getenv('ORA_CLIENT_DIR')

SMD_USER = os.getenv('SMD_ORA_USER')
SMD_PASS = os.getenv('SMD_ORA_KEY')


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------
def connect_to_ora(host, port, service, user, password):
    """
    Create a connection to Oracle Database using ``oracledb`` (cx_Oracle).

    Parameters
    ----------
    host, port, service, user, password : str
    """
    import oracledb as cx_Oracle

    dsn_tns = cx_Oracle.makedsn(host, port, service_name=service)
    try:
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
    except cx_Oracle.DatabaseError:
        cx_Oracle.init_oracle_client(ORA_CLIENT_DIR)
        connection = cx_Oracle.connect(user, password, dsn_tns)
    return connection


# ---------------------------------------------------------------------------
# Global connections (lazy initialisation so imports never fail)
# ---------------------------------------------------------------------------
_smd_connection = None


def get_smd_connection():
    """Return the shared SMD Oracle connection, creating it on first call."""
    global _smd_connection
    if _smd_connection is None:
        if None in (ORA_HOST, ORA_PORT, ORA_SERVICE, SMD_USER, SMD_PASS):
            raise RuntimeError(
                "Oracle credentials are not configured.  "
                "Ensure SMD_Package/.env exists or set ORA_HOST, ORA_PORT, "
                "ORA_SERVICE, SMD_ORA_USER and SMD_ORA_KEY environment variables."
            )
        _smd_connection = connect_to_ora(
            ORA_HOST, ORA_PORT, ORA_SERVICE, SMD_USER, SMD_PASS
        )
    return _smd_connection


# Mirror the original module-level name.
smd_connection = get_smd_connection()
