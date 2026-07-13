from pathlib import Path

import pytest

from aps.config import ConfigurationError, Settings


def test_explicit_environment_overrides_env_file(tmp_path: Path):
    env_file = tmp_path / "aps.env"
    env_file.write_text(
        "APS_PROFILE=test\n"
        "APS_DB_HOST=file-db\n"
        "APS_DB_PORT=5434\n"
        "APS_DB_PASSWORD=file-secret\n"
        "APS_DATA_ROOT=/file/data\n"
    )

    settings = Settings.from_environ(
        {
            "APS_ENV_FILE": str(env_file),
            "APS_DB_HOST": "explicit-db",
            "APS_DB_PASSWORD": "explicit-secret",
        }
    )

    assert settings.profile == "test"
    assert settings.db_host == "explicit-db"
    assert settings.db_port == 5434
    assert settings.database_params()["password"] == "explicit-secret"
    assert settings.data_root == Path("/file/data")


def test_database_password_is_required_at_connection_boundary():
    settings = Settings.from_environ({"APS_PROFILE": "production"})

    with pytest.raises(ConfigurationError, match="APS_DB_PASSWORD"):
        settings.database_params()


def test_placeholder_secrets_are_not_treated_as_configuration():
    settings = Settings.from_environ(
        {
            "APS_PROFILE": "production",
            "APS_DB_PASSWORD": "change-me",
            "APS_SUPERSET_PASS": "replace-me",
            "APS_FLASK_SECRET_KEY": "change-me",
        }
    )

    with pytest.raises(ConfigurationError, match="APS_DB_PASSWORD"):
        settings.database_params()
    with pytest.raises(ConfigurationError, match="APS_SUPERSET_PASS"):
        settings.superset_credentials()
    with pytest.raises(ConfigurationError, match="APS_FLASK_SECRET_KEY"):
        settings.require_flask_secret_key()


def test_invalid_port_fails_closed():
    with pytest.raises(ConfigurationError, match="APS_DB_PORT"):
        Settings.from_environ({"APS_DB_PORT": "not-a-port"})


def test_invalid_superset_timeout_fails_closed():
    with pytest.raises(ConfigurationError, match="APS_SUPERSET_READ_TIMEOUT_SECONDS"):
        Settings.from_environ({"APS_SUPERSET_READ_TIMEOUT_SECONDS": "0"})


def test_redacted_summary_never_contains_secret_values():
    settings = Settings.from_environ(
        {
            "APS_DB_PASSWORD": "db-secret",
            "APS_SUPERSET_PASS": "superset-secret",
            "APS_FLASK_SECRET_KEY": "flask-secret",
        }
    )

    summary = settings.redacted_summary()
    assert "db-secret" not in repr(summary)
    assert "superset-secret" not in repr(summary)
    assert "flask-secret" not in repr(summary)
    assert summary["db_password_configured"] is True
    assert summary["flask_secret_key_configured"] is True
    assert summary["superset_connect_timeout_seconds"] == 5.0


def test_nightly_validation_checks_secrets_and_mounted_roots(tmp_path: Path):
    data_root = tmp_path / "data"
    nas_root = tmp_path / "nas"
    data_root.mkdir()
    nas_root.mkdir()
    settings = Settings.from_environ(
        {
            "APS_PROFILE": "test",
            "APS_DB_PASSWORD": "db-secret",
            "APS_SUPERSET_PASS": "superset-secret",
            "APS_DATA_ROOT": str(data_root),
            "APS_NAS_ROOT": str(nas_root),
        }
    )

    settings.validate_nightly()
    assert settings.require_data_root() == data_root
    assert settings.require_nas_root() == nas_root


def test_nightly_validation_rejects_an_unmounted_source_root(tmp_path: Path):
    settings = Settings.from_environ(
        {
            "APS_PROFILE": "test",
            "APS_DB_PASSWORD": "db-secret",
            "APS_SUPERSET_PASS": "superset-secret",
            "APS_DATA_ROOT": str(tmp_path / "missing-data"),
            "APS_NAS_ROOT": str(tmp_path),
        }
    )

    with pytest.raises(ConfigurationError, match="APS_DATA_ROOT"):
        settings.validate_nightly()
