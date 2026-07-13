"""Registry and audited builder for repeatable APS analytical models.

Views and materialized views are not forward migrations: their SQL may be
rebuilt after upstream ingestion, and their build history needs model-specific
timing, provenance, and output statistics. Keeping this boundary explicit
prevents dashboards and extractors from quietly rebuilding expensive models.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from aps.config import Settings, get_settings
from aps.paths import REPO_ROOT, SCHEMA_DIR
from aps.provenance import (
    collect_source_provenance,
    require_clean_production_source,
)


class ModelError(RuntimeError):
    """Base error for repeatable analytical model operations."""


class UnknownModelError(ModelError):
    """Raised when a caller requests an unregistered model."""


class ModelPrerequisiteError(ModelError):
    """Raised when an explicitly declared input relation is missing."""


class ModelBuildError(ModelError):
    """Raised when a model build transaction fails after recording its result."""


@dataclass(frozen=True)
class ModelDefinition:
    """Owned repeatable model bundle and its declared contract."""

    name: str
    description: str
    sql_paths: tuple[Path, ...]
    dependencies: tuple[str, ...] = ()
    required_relations: tuple[str, ...] = ()
    expected_relations: tuple[str, ...] = ()
    build_mode: str = "replace"


@dataclass(frozen=True)
class ModelPlan:
    """Offline description of the exact SQL that a build would execute."""

    name: str
    checksum: str
    files: tuple[str, ...]
    dependencies: tuple[str, ...]
    required_relations: tuple[str, ...]
    expected_relations: tuple[str, ...]
    build_mode: str


@dataclass(frozen=True)
class ModelBuildResult:
    """Auditable result of a model build or dry-run plan."""

    model: str
    checksum: str
    status: str
    build_id: int | None
    object_stats: Mapping[str, Mapping[str, int | str | None]]


PROXY_ANALYTICS = ModelDefinition(
    name="proxy-analytics",
    description=(
        "Proxy-readiness, mechanistic-energy, and concordance views used by "
        "the proxy dashboard and exports."
    ),
    sql_paths=(
        SCHEMA_DIR / "025_proxy_readiness_waveforms.sql",
        SCHEMA_DIR / "028_mechanistic_energy_proxy.sql",
        SCHEMA_DIR / "029_proxy_viz_support.sql",
    ),
    dependencies=(
        "single-event extraction",
        "radiation stress-dose model",
        "SC/irradiation equivalence model",
    ),
    expected_relations=(
        "stress_test_context_view",
        "stress_proxy_candidate_ranked_view",
        "stress_energy_equivalence_features",
        "stress_proxy_candidate_energy_v2",
        "stress_proxy_candidate_combined_v3",
    ),
)

MODEL_REGISTRY = {
    definition.name: definition for definition in (PROXY_ANALYTICS,)
}

MODEL_BUILDS_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS aps_model_builds (
    id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    code_sha TEXT NOT NULL,
    config_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    object_stats JSONB,
    error TEXT
);
CREATE INDEX IF NOT EXISTS aps_model_builds_model_started_idx
    ON aps_model_builds (model_name, started_at DESC);
ALTER TABLE aps_model_builds
    ADD COLUMN IF NOT EXISTS source_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb;
"""

_LOCK_PREFIX = "aps_model_build:"
_INSERT_BUILD_SQL = """
INSERT INTO aps_model_builds (
    model_name, checksum, code_sha, config_fingerprint, source_fingerprint,
    status, started_at
)
VALUES (%s, %s, %s, %s, %s, 'running', now())
RETURNING id
"""
_MARK_BUILD_SUCCEEDED_SQL = """
UPDATE aps_model_builds
SET status = 'succeeded', completed_at = now(), object_stats = %s, error = NULL
WHERE id = %s
"""
_MARK_BUILD_FAILED_SQL = """
UPDATE aps_model_builds
SET status = 'failed', completed_at = now(), error = %s
WHERE id = %s
"""


def _normalise_model_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")


def get_model(name: str) -> ModelDefinition:
    """Resolve a stable model name, accepting underscores as CLI convenience."""
    try:
        return MODEL_REGISTRY[_normalise_model_name(name)]
    except KeyError as exc:
        available = ", ".join(sorted(MODEL_REGISTRY))
        raise UnknownModelError(
            f"unknown APS model {name!r}; available models: {available}"
        ) from exc


def list_models() -> tuple[ModelDefinition, ...]:
    """Return model definitions in deterministic CLI/display order."""
    return tuple(MODEL_REGISTRY[name] for name in sorted(MODEL_REGISTRY))


def model_checksum(model: ModelDefinition) -> str:
    """Hash filenames and contents so the ledger identifies the exact bundle."""
    digest = hashlib.sha256()
    for path in model.sql_paths:
        if not path.is_file():
            raise ModelError(f"model {model.name} is missing SQL asset: {path}")
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def model_plan(name: str | ModelDefinition) -> ModelPlan:
    """Build an offline plan without requiring configuration or a database."""
    model = get_model(name) if isinstance(name, str) else name
    return ModelPlan(
        name=model.name,
        checksum=model_checksum(model),
        files=tuple(str(path.relative_to(REPO_ROOT)) for path in model.sql_paths),
        dependencies=model.dependencies,
        required_relations=model.required_relations,
        expected_relations=model.expected_relations,
        build_mode=model.build_mode,
    )


def _acquire_lock(conn, model_name: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT pg_advisory_lock(hashtext(%s))",
            (f"{_LOCK_PREFIX}{model_name}",),
        )
    finally:
        cur.close()


def _release_lock(conn, model_name: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))",
            (f"{_LOCK_PREFIX}{model_name}",),
        )
    finally:
        cur.close()


def _ensure_ledger(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(MODEL_BUILDS_LEDGER_SQL)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def assert_model_prerequisites(cur, model: ModelDefinition) -> None:
    """Verify declared relation inputs before model SQL can mutate anything."""
    missing = []
    for relation in model.required_relations:
        cur.execute("SELECT to_regclass(%s)", (relation,))
        if cur.fetchone()[0] is None:
            missing.append(relation)
    if missing:
        raise ModelPrerequisiteError(
            f"model {model.name} cannot run because required relation(s) are missing: "
            + ", ".join(missing)
        )


def _relation_stats(cur, relations: Iterable[str]) -> dict[str, dict[str, int | str | None]]:
    """Read cheap catalog estimates instead of expensive full-table counts."""
    stats = {}
    for relation in relations:
        cur.execute(
            """
            SELECT
                class.relkind,
                class.reltuples::BIGINT,
                pg_total_relation_size(class.oid)::BIGINT
            FROM pg_catalog.pg_class class
            WHERE class.oid = to_regclass(%s)
            """,
            (relation,),
        )
        row = cur.fetchone()
        if row is None:
            raise ModelBuildError(
                f"model output {relation} was not created by the SQL bundle"
            )
        stats[relation] = {
            "kind": row[0],
            "estimated_rows": row[1],
            "total_bytes": row[2],
        }
    return stats


def _mark_build_failed(conn, build_id: int, exc: Exception) -> None:
    cur = conn.cursor()
    try:
        message = f"{type(exc).__name__}: {exc}"
        cur.execute(_MARK_BUILD_FAILED_SQL, (message[:8000], build_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def build_model(
    conn,
    name: str | ModelDefinition,
    *,
    settings: Settings | None = None,
    dry_run: bool = False,
) -> ModelBuildResult:
    """Build one model bundle in a single database transaction.

    A failed SQL bundle rolls back its DDL and only the separate build ledger
    transaction is retained as a failed attempt. The model SQL itself remains
    responsible for whether it uses in-place replacement or a staged swap;
    the ledger makes that policy visible instead of pretending it is a
    migration.
    """
    model = get_model(name) if isinstance(name, str) else name
    plan = model_plan(model)
    if dry_run:
        return ModelBuildResult(
            model=model.name,
            checksum=plan.checksum,
            status="planned",
            build_id=None,
            object_stats={},
        )

    from psycopg2.extras import Json

    active_settings = settings or get_settings()
    source = collect_source_provenance()
    require_clean_production_source(
        active_settings,
        source,
        operation=f"aps models build {model.name}",
    )

    _acquire_lock(conn, model.name)
    try:
        _ensure_ledger(conn)
        config_fingerprint = active_settings.redacted_summary()
        cur = conn.cursor()
        try:
            cur.execute(
                _INSERT_BUILD_SQL,
                (
                    model.name,
                    plan.checksum,
                    source.code_sha,
                    Json(config_fingerprint),
                    Json(source.as_dict()),
                ),
            )
            build_id = int(cur.fetchone()[0])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

        try:
            cur = conn.cursor()
            try:
                assert_model_prerequisites(cur, model)
                for path in model.sql_paths:
                    cur.execute(path.read_text())
                stats = _relation_stats(cur, model.expected_relations)
                cur.execute(_MARK_BUILD_SUCCEEDED_SQL, (Json(stats), build_id))
                conn.commit()
            finally:
                cur.close()
        except Exception as exc:
            conn.rollback()
            _mark_build_failed(conn, build_id, exc)
            if isinstance(exc, ModelError):
                raise
            raise ModelBuildError(
                f"model {model.name} failed; its SQL transaction was rolled back "
                "and the build ledger records the error"
            ) from exc

        return ModelBuildResult(
            model=model.name,
            checksum=plan.checksum,
            status="succeeded",
            build_id=build_id,
            object_stats=stats,
        )
    finally:
        _release_lock(conn, model.name)


def model_status(conn) -> tuple[tuple[object, ...], ...]:
    """Return the latest build outcome per registered model without creating state."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT to_regclass(%s)", ("public.aps_model_builds",))
        if cur.fetchone()[0] is None:
            return ()
        cur.execute(
            """
            SELECT DISTINCT ON (model_name)
                model_name, checksum, status, started_at, completed_at, error
            FROM aps_model_builds
            ORDER BY model_name, started_at DESC
            """
        )
        return tuple(cur.fetchall())
    finally:
        cur.close()
