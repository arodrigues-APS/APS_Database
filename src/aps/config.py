"""Validated runtime settings for APS commands and applications.

The package deliberately does not discover a repository-local .env file by
accident. Local callers may opt in through APS_ENV_FILE; deployed services
should use their systemd or Compose environment-file mechanism instead.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Mapping


class ConfigurationError(RuntimeError):
    """Raised when a command lacks a safe, explicit runtime configuration."""


_VALID_PROFILES = frozenset({"development", "test", "production"})
_PLACEHOLDER_VALUES = frozenset({"", "change-me", "changeme", "replace-me"})


def _optional_value(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _secret_value(value: str | None) -> str | None:
    value = _optional_value(value)
    if value is None or value.lower() in _PLACEHOLDER_VALUES:
        return None
    return value


def _path_value(value: str | None) -> Path | None:
    value = _optional_value(value)
    return Path(value).expanduser() if value else None


def require_directory(
    path: Path | str | None,
    variable: str,
    *,
    writable: bool = False,
) -> Path:
    """Return an accessible directory or raise an actionable config error.

    Merely parsing a path is not a useful production preflight: an absent NAS
    mount otherwise looks like an empty source tree. Read/execute access is
    required for every source directory; callers that publish artifacts may
    additionally request write access.
    """
    if path is None or not str(path).strip():
        raise ConfigurationError(f"{variable} is required for this command.")
    resolved = Path(path).expanduser()
    try:
        is_directory = resolved.is_dir()
    except OSError as exc:
        raise ConfigurationError(
            f"{variable} cannot be inspected at {resolved}: {exc}"
        ) from exc
    if not is_directory:
        raise ConfigurationError(
            f"{variable} must reference an existing directory: {resolved}"
        )
    required_mode = os.R_OK | os.X_OK | (os.W_OK if writable else 0)
    if not os.access(resolved, required_mode):
        access = "read/write" if writable else "read"
        raise ConfigurationError(
            f"{variable} is not accessible for {access}: {resolved}"
        )
    return resolved


def _read_env_file(path: Path) -> dict[str, str]:
    """Read a small KEY=VALUE environment file without adding a dependency."""
    if not path.is_file():
        raise ConfigurationError(f"APS_ENV_FILE does not exist: {path}")

    values: dict[str, str] = {}
    for line_no, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ConfigurationError(
                f"invalid environment assignment in {path}:{line_no}"
            )
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or not key.replace("_", "").isalnum():
            raise ConfigurationError(f"invalid environment key in {path}:{line_no}")
        values[key] = value.strip().strip("'").strip('"')
    return values


def _integer(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0 or value > 65535:
        raise ConfigurationError(f"{name} must be between 1 and 65535, got {value}")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero, got {value}")
    return value


def _boolean(values: Mapping[str, str], name: str, default: bool) -> bool:
    raw = values.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(
        f"{name} must be one of 1/0, true/false, yes/no, or on/off; got {raw!r}"
    )


@dataclass(frozen=True)
class Settings:
    """Environment-derived settings with explicit checks at use boundaries."""

    profile: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str | None
    superset_url: str
    superset_user: str
    superset_password: str | None
    superset_connect_timeout_seconds: float
    superset_read_timeout_seconds: float
    flask_secret_key: str | None
    data_root: Path | None
    nas_root: Path | None
    iv_damage_artifact_root: Path | None
    iv_damage_governance_root: Path | None
    enable_legacy_cv_dpt: bool

    @classmethod
    def from_environ(
        cls, environ: Mapping[str, str] | None = None
    ) -> "Settings":
        explicit = dict(os.environ if environ is None else environ)
        env_file = _optional_value(explicit.get("APS_ENV_FILE"))
        values: dict[str, str] = {}
        if env_file:
            values.update(_read_env_file(Path(env_file).expanduser()))
        values.update(explicit)

        profile = values.get("APS_PROFILE", "development").strip().lower()
        if profile not in _VALID_PROFILES:
            allowed = ", ".join(sorted(_VALID_PROFILES))
            raise ConfigurationError(
                f"APS_PROFILE must be one of {allowed}, got {profile!r}"
            )

        return cls(
            profile=profile,
            db_host=values.get("APS_DB_HOST", "localhost").strip() or "localhost",
            db_port=_integer(values, "APS_DB_PORT", 5435),
            db_name=values.get("APS_DB_NAME", "mosfets").strip() or "mosfets",
            db_user=values.get("APS_DB_USER", "postgres").strip() or "postgres",
            db_password=_secret_value(values.get("APS_DB_PASSWORD")),
            superset_url=(
                values.get("APS_SUPERSET_URL", "http://localhost:8088").strip()
                or "http://localhost:8088"
            ),
            superset_user=(
                values.get("APS_SUPERSET_USER", "admin").strip() or "admin"
            ),
            superset_password=_secret_value(values.get("APS_SUPERSET_PASS")),
            superset_connect_timeout_seconds=_positive_float(
                values, "APS_SUPERSET_CONNECT_TIMEOUT_SECONDS", 5.0
            ),
            superset_read_timeout_seconds=_positive_float(
                values, "APS_SUPERSET_READ_TIMEOUT_SECONDS", 60.0
            ),
            flask_secret_key=_secret_value(values.get("APS_FLASK_SECRET_KEY")),
            data_root=_path_value(values.get("APS_DATA_ROOT")),
            nas_root=_path_value(values.get("APS_NAS_ROOT")),
            iv_damage_artifact_root=_path_value(
                values.get("APS_IV_DAMAGE_ARTIFACT_ROOT")
            ),
            iv_damage_governance_root=_path_value(
                values.get("APS_IV_DAMAGE_GOVERNANCE_ROOT")
            ),
            enable_legacy_cv_dpt=_boolean(
                values, "APS_ENABLE_LEGACY_CV_DPT", False
            ),
        )

    def _require_secret(self, value: str | None, variable: str) -> str:
        if value is None:
            raise ConfigurationError(
                f"{variable} is required for APS_PROFILE={self.profile}. "
                "Set it through the service environment or APS_ENV_FILE."
            )
        return value

    def database_params(self) -> dict[str, object]:
        """Return psycopg2 parameters, requiring a configured DB password."""
        return {
            "host": self.db_host,
            "port": self.db_port,
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self._require_secret(
                self.db_password, "APS_DB_PASSWORD"
            ),
        }

    def superset_credentials(self) -> tuple[str, str, str]:
        """Return API URL and credentials only when they are configured."""
        return (
            self.superset_url,
            self.superset_user,
            self._require_secret(self.superset_password, "APS_SUPERSET_PASS"),
        )

    def require_flask_secret_key(self) -> str:
        """Return the Flask signing key only when explicitly provisioned."""
        return self._require_secret(
            self.flask_secret_key,
            "APS_FLASK_SECRET_KEY",
        )

    def require_data_root(self) -> Path:
        return require_directory(self.data_root, "APS_DATA_ROOT")

    def require_nas_root(self) -> Path:
        return require_directory(self.nas_root, "APS_NAS_ROOT")

    def require_iv_damage_artifact_root(self, *, writable: bool = False) -> Path:
        """Return the shared, checkout-independent V3 artifact directory."""
        return require_directory(
            self.iv_damage_artifact_root,
            "APS_IV_DAMAGE_ARTIFACT_ROOT",
            writable=writable,
        )

    def require_iv_damage_governance_root(self, *, writable: bool = False) -> Path:
        """Return the durable root for signed plans and governance records."""
        return require_directory(
            self.iv_damage_governance_root,
            "APS_IV_DAMAGE_GOVERNANCE_ROOT",
            writable=writable,
        )

    def require_legacy_cv_dpt_enabled(self) -> None:
        """Refuse the legacy snapshot feature unless it is explicitly enabled."""
        if not self.enable_legacy_cv_dpt:
            raise ConfigurationError(
                "legacy CV/DPT is disabled. Keep APS_ENABLE_LEGACY_CV_DPT=0 "
                "until the canonical importer and parity record are accepted; "
                "set it to 1 only for an explicitly approved compatibility run."
            )

    def validate_nightly(self) -> None:
        """Fail before a nightly run can mistake missing config for no data."""
        self.database_params()
        self.superset_credentials()
        self.require_data_root()
        self.require_nas_root()

    def redacted_summary(self) -> dict[str, object]:
        """Return a safe configuration fingerprint for run/release manifests."""
        return {
            "profile": self.profile,
            "db_host": self.db_host,
            "db_port": self.db_port,
            "db_name": self.db_name,
            "db_user": self.db_user,
            "db_password_configured": self.db_password is not None,
            "superset_url": self.superset_url,
            "superset_user": self.superset_user,
            "superset_password_configured": self.superset_password is not None,
            "superset_connect_timeout_seconds": self.superset_connect_timeout_seconds,
            "superset_read_timeout_seconds": self.superset_read_timeout_seconds,
            "flask_secret_key_configured": self.flask_secret_key is not None,
            "data_root": str(self.data_root) if self.data_root else None,
            "nas_root": str(self.nas_root) if self.nas_root else None,
            "iv_damage_artifact_root": (
                str(self.iv_damage_artifact_root)
                if self.iv_damage_artifact_root else None
            ),
            "iv_damage_governance_root": (
                str(self.iv_damage_governance_root)
                if self.iv_damage_governance_root else None
            ),
            "enable_legacy_cv_dpt": self.enable_legacy_cv_dpt,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load process settings once; callers needing test injection use from_environ."""
    return Settings.from_environ()


def reset_settings_cache() -> None:
    """Clear cached process settings for controlled tests and CLI bootstrap."""
    get_settings.cache_clear()
