"""
Shared configuration for APS Database scripts.

All connection parameters and data paths are configurable via environment
variables, with sensible defaults that match the current deployment.
To override, either export the variable or create a .env file.
"""

import os

# ── Database Connection ─────────────────────────────────────────────────────
DB_HOST = os.environ.get("APS_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("APS_DB_PORT", "5435"))
DB_NAME = os.environ.get("APS_DB_NAME", "mosfets")
DB_USER = os.environ.get("APS_DB_USER", "postgres")
DB_PASSWORD = os.environ.get("APS_DB_PASSWORD", "APSLab")

# ── Superset API ────────────────────────────────────────────────────────────
SUPERSET_URL = os.environ.get("APS_SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("APS_SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("APS_SUPERSET_PASS", "admin")

# ── Data Paths ──────────────────────────────────────────────────────────────
DATA_ROOT = os.environ.get("APS_DATA_ROOT", "/home/arodrigues/APS_Database")
NAS_ROOT = os.environ.get("APS_NAS_ROOT", "/home/arodrigues/NAS/Common_Files")


def get_db_params():
    """Return connection parameters as a dict for psycopg2.connect()."""
    return dict(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD)


def get_connection():
    """Return a new psycopg2 connection to the mosfets database."""
    import psycopg2
    return psycopg2.connect(**get_db_params())
