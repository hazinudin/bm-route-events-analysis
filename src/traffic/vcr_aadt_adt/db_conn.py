"""
Standalone database connection module (replaces SMD_Package.db_conn).

Keeps the same global-connection pattern so that ``AADTPipeline`` can be
dropped in as a replacement without changing caller code, but delays the
actual Oracle connection until first use.
"""

import os
from dotenv import load_dotenv

from worker.db import create_oracle_engine
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
# Lazy connection proxy
# ---------------------------------------------------------------------------
class _LazyConnection:
    """
    Proxy that creates the real Oracle connection on first attribute access.

    This preserves the module-level ``smd_connection`` symbol used by the
    rest of the package while avoiding import-time side effects.
    """

    def __init__(self, factory):
        self._factory = factory
        self._connection = None

    def _connect(self):
        if self._connection is None:
            self._connection = self._factory()
        return self._connection

    def __getattr__(self, name):
        return getattr(self._connect(), name)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------
_engine = None


def get_smd_engine():
    """Return the shared SQLAlchemy engine, creating it on first call."""
    global _engine
    if _engine is None:
        if None in (ORA_HOST, ORA_PORT, ORA_SERVICE, SMD_USER, SMD_PASS):
            raise RuntimeError(
                "Oracle credentials are not configured.  "
                "Ensure SMD_Package/.env exists or set ORA_HOST, ORA_PORT, "
                "ORA_SERVICE, SMD_ORA_USER and SMD_ORA_KEY environment variables."
            )
        _engine = create_oracle_engine(
            host=ORA_HOST,
            port=ORA_PORT,
            service=ORA_SERVICE,
            user=SMD_USER,
            password=SMD_PASS,
            client_dir=ORA_CLIENT_DIR or None,
        )
    return _engine


def get_smd_connection():
    """Return a DB-API connection from the shared engine."""
    return get_smd_engine().raw_connection()


# Mirror the original module-level name, but lazily.
smd_connection = _LazyConnection(get_smd_connection)
