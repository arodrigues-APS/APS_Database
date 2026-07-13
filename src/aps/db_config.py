"""Compatibility facade for legacy APS modules.

New code should import Settings/get_settings from aps.config and pass settings
at its execution boundary. These module constants remain while scripts migrate
away from import-time configuration.
"""

from aps.config import (
    ConfigurationError,
    Settings,
    get_settings,
    reset_settings_cache,
)


_SETTINGS = get_settings()

# Legacy imports remain stable while callers move to Settings.
DB_HOST = _SETTINGS.db_host
DB_PORT = _SETTINGS.db_port
DB_NAME = _SETTINGS.db_name
DB_USER = _SETTINGS.db_user
DB_PASSWORD = _SETTINGS.db_password or ""
SUPERSET_URL = _SETTINGS.superset_url
SUPERSET_USER = _SETTINGS.superset_user
SUPERSET_PASS = _SETTINGS.superset_password or ""
DATA_ROOT = str(_SETTINGS.data_root) if _SETTINGS.data_root else ""
NAS_ROOT = str(_SETTINGS.nas_root) if _SETTINGS.nas_root else ""


def get_db_params() -> dict[str, object]:
    """Return validated connection parameters for psycopg2.connect()."""
    return get_settings().database_params()


def get_connection():
    """Return a new PostgreSQL connection after validating APS configuration."""
    import psycopg2

    return psycopg2.connect(**get_db_params())
