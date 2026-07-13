"""Forward-only structural migration support.

This module deliberately does not reuse aps.common.apply_schema. The latter
remains a compatibility helper for the historical idempotent schema bundle,
where an edited file is allowed to run again. New structural migrations need
the opposite contract: a filename can be recorded once, and changing its
content after application must stop the release.

The runner is intentionally generic while the repository completes the
incremental move from schema/ into dedicated migration assets. Files in
schema/ with the apply_schema: pipeline-owned marker are excluded: those are
repeatable derived models and belong in aps.db.models.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from aps.paths import SCHEMA_DIR


PIPELINE_SCHEMA_MARKER = "apply_schema: pipeline-owned"
_MIGRATION_NAME = re.compile(r"^\d{3,4}_[A-Za-z0-9_.-]+\.sql$")
_LOCK_NAME = "aps_forward_migrations"


class MigrationError(RuntimeError):
    """Base error for forward migration operations."""


class MigrationBaselineRequired(MigrationError):
    """Raised before historical files could be applied to an existing database."""


class MigrationBaselineSelectionError(MigrationError):
    """Raised when an adoption cutoff is absent, unknown, or no longer valid."""


class MigrationChecksumMismatch(MigrationError):
    """Raised when an already-recorded migration was edited."""


class MigrationExecutionError(MigrationError):
    """Raised after a migration transaction fails and the failure is recorded."""


@dataclass(frozen=True)
class Migration:
    """An immutable SQL asset selected for the forward migration stream."""

    filename: str
    path: Path
    checksum: str


@dataclass(frozen=True)
class RecordedMigration:
    """The latest ledger record for an individual forward migration."""

    filename: str
    checksum: str
    status: str
    completed_at: object | None = None
    error: str | None = None


@dataclass(frozen=True)
class MigrationPlanItem:
    """One migration's state relative to the local SQL asset."""

    migration: Migration
    state: str
    recorded: RecordedMigration | None


@dataclass(frozen=True)
class MigrationRunResult:
    """Summary returned by a successful migration or baseline operation."""

    applied: tuple[str, ...]
    baselined: tuple[str, ...]
    plan: tuple[MigrationPlanItem, ...]


FORWARD_MIGRATIONS_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS aps_forward_migrations (
    filename TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'applied', 'failed', 'baselined')),
    applied_by TEXT NOT NULL DEFAULT current_user,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    error TEXT
);
"""

_LEDGER_SELECT_SQL = """
SELECT filename, checksum, status, completed_at, error
FROM aps_forward_migrations
ORDER BY filename
"""
_MARK_RUNNING_SQL = """
INSERT INTO aps_forward_migrations (filename, checksum, status, started_at, completed_at, error)
VALUES (%s, %s, 'running', now(), NULL, NULL)
ON CONFLICT (filename) DO UPDATE
SET checksum = EXCLUDED.checksum,
    status = 'running',
    started_at = now(),
    completed_at = NULL,
    error = NULL,
    applied_by = current_user
"""
_MARK_APPLIED_SQL = """
UPDATE aps_forward_migrations
SET status = 'applied', completed_at = now(), error = NULL
WHERE filename = %s
"""
_MARK_FAILED_SQL = """
UPDATE aps_forward_migrations
SET status = 'failed', completed_at = now(), error = %s
WHERE filename = %s
"""
_MARK_BASELINED_SQL = """
INSERT INTO aps_forward_migrations (filename, checksum, status, started_at, completed_at)
VALUES (%s, %s, 'baselined', now(), now())
"""
_DATABASE_HAS_OBJECTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class relation
    JOIN pg_catalog.pg_namespace namespace
      ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'public'
      AND relation.relname <> 'aps_forward_migrations'
      AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
)
"""


def migration_checksum(sql_text: str) -> str:
    """Return the checksum stored for a migration's exact SQL bytes."""
    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()


def is_pipeline_owned(sql_text: str) -> bool:
    """Return whether an SQL asset belongs to a repeatable pipeline/model."""
    return PIPELINE_SCHEMA_MARKER in sql_text[:500]


def discover_migrations(directory: Path | str | None = None) -> tuple[Migration, ...]:
    """Discover numbered, non-pipeline SQL assets in deterministic order.

    The compatibility directory currently contains a mix of legacy scripts,
    source-table DDL, and model SQL. Numbered files without the ownership
    marker are the only files selected. Unnumbered historical helpers stay
    outside this new contract until they are assigned a deliberate migration
    number rather than silently becoming a release action.
    """
    root = Path(directory) if directory is not None else SCHEMA_DIR
    if not root.is_dir():
        raise MigrationError(f"migration directory does not exist: {root}")

    migrations = []
    for path in sorted(root.glob("*.sql")):
        if not _MIGRATION_NAME.match(path.name):
            continue
        sql_text = path.read_text()
        if is_pipeline_owned(sql_text):
            continue
        migrations.append(
            Migration(
                filename=path.name,
                path=path,
                checksum=migration_checksum(sql_text),
            )
        )
    return tuple(migrations)


def _recorded_by_filename(
    recorded: Iterable[RecordedMigration] | Mapping[str, RecordedMigration],
) -> dict[str, RecordedMigration]:
    if isinstance(recorded, Mapping):
        return dict(recorded)
    return {row.filename: row for row in recorded}


def plan_migrations(
    migrations: Iterable[Migration],
    recorded: Iterable[RecordedMigration] | Mapping[str, RecordedMigration],
) -> tuple[MigrationPlanItem, ...]:
    """Classify local assets without connecting to a database."""
    by_filename = _recorded_by_filename(recorded)
    plan = []
    for migration in migrations:
        row = by_filename.get(migration.filename)
        if row is None:
            state = "pending"
        elif row.checksum != migration.checksum:
            state = "checksum_mismatch"
        elif row.status == "applied":
            state = "applied"
        elif row.status == "baselined":
            state = "baselined"
        elif row.status == "failed":
            state = "retryable_failure"
        else:
            state = "interrupted"
        plan.append(MigrationPlanItem(migration=migration, state=state, recorded=row))
    return tuple(plan)


def assert_plan_is_safe(plan: Iterable[MigrationPlanItem]) -> None:
    """Fail closed when a historical migration file no longer matches its ledger."""
    mismatches = [item.migration.filename for item in plan if item.state == "checksum_mismatch"]
    if mismatches:
        joined = ", ".join(mismatches)
        raise MigrationChecksumMismatch(
            "an applied APS forward migration was edited: "
            f"{joined}. Create a new migration instead of modifying an applied file."
        )


def _read_ledger(conn) -> tuple[RecordedMigration, ...]:
    cur = conn.cursor()
    try:
        cur.execute(_LEDGER_SELECT_SQL)
        return tuple(
            RecordedMigration(
                filename=row[0],
                checksum=row[1],
                status=row[2],
                completed_at=row[3],
                error=row[4],
            )
            for row in cur.fetchall()
        )
    finally:
        cur.close()


def migration_status(
    conn, directory: Path | str | None = None
) -> tuple[MigrationPlanItem, ...]:
    """Return migration state without creating a ledger or changing the database."""
    migrations = discover_migrations(directory)
    cur = conn.cursor()
    try:
        cur.execute("SELECT to_regclass(%s)", ("public.aps_forward_migrations",))
        ledger_exists = cur.fetchone()[0] is not None
    finally:
        cur.close()
    recorded = _read_ledger(conn) if ledger_exists else ()
    return plan_migrations(migrations, recorded)


def _acquire_lock(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (_LOCK_NAME,))
    finally:
        cur.close()


def _release_lock(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (_LOCK_NAME,))
    finally:
        cur.close()


def _ensure_ledger(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(FORWARD_MIGRATIONS_LEDGER_SQL)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _database_has_existing_objects(conn) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(_DATABASE_HAS_OBJECTS_SQL)
        return bool(cur.fetchone()[0])
    finally:
        cur.close()


def _baseline_existing_database(conn, migrations: Iterable[Migration]) -> tuple[str, ...]:
    cur = conn.cursor()
    try:
        names = []
        for migration in migrations:
            cur.execute(_MARK_BASELINED_SQL, (migration.filename, migration.checksum))
            names.append(migration.filename)
        conn.commit()
        return tuple(names)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def select_baseline_migrations(
    migrations: Iterable[Migration],
    baseline_through: str,
) -> tuple[Migration, ...]:
    """Select an explicit historical prefix for first-ledger adoption.

    A boolean "baseline everything" switch is unsafe once a release contains a
    genuinely new migration: it would record the new file without executing
    it. Requiring the exact last historical filename makes the adoption
    boundary reviewable and lets newer files execute normally in the same run.
    """
    ordered = tuple(migrations)
    names = [migration.filename for migration in ordered]
    if baseline_through not in names:
        available = ", ".join(names) or "<none>"
        raise MigrationBaselineSelectionError(
            f"unknown baseline cutoff {baseline_through!r}; "
            f"select an exact discovered filename from: {available}"
        )
    cutoff = names.index(baseline_through)
    return ordered[: cutoff + 1]


def _record_failure(conn, filename: str, exc: Exception) -> None:
    cur = conn.cursor()
    try:
        message = f"{type(exc).__name__}: {exc}"
        cur.execute(_MARK_FAILED_SQL, (message[:8000], filename))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def run_migrations(
    conn,
    *,
    directory: Path | str | None = None,
    baseline_existing_through: str | None = None,
) -> MigrationRunResult:
    """Apply pending forward migrations exactly once.

    A database that already has public objects but no forward-migration ledger
    cannot be inferred to be blank. It must be explicitly baselined before
    new migrations can run, preventing accidental replay of historical DDL
    against the production database.
    """
    migrations = discover_migrations(directory)
    _acquire_lock(conn)
    try:
        _ensure_ledger(conn)
        recorded = _read_ledger(conn)
        plan = plan_migrations(migrations, recorded)
        assert_plan_is_safe(plan)

        baselined: tuple[str, ...] = ()
        database_has_objects = _database_has_existing_objects(conn)
        if not recorded and database_has_objects:
            if baseline_existing_through is None:
                raise MigrationBaselineRequired(
                    "this database has public objects but no aps_forward_migrations "
                    "ledger. Refusing to replay historical schema SQL. Review the "
                    "plan, identify the exact last historical migration already "
                    "represented by the database, then rerun with "
                    "--baseline-existing-through FILENAME."
                )
            historical = select_baseline_migrations(
                migrations, baseline_existing_through
            )
            baselined = _baseline_existing_database(conn, historical)
            plan = plan_migrations(migrations, _read_ledger(conn))
        elif baseline_existing_through is not None:
            if recorded:
                raise MigrationBaselineSelectionError(
                    "the forward-migration ledger already exists; an adoption "
                    "baseline can only be selected once"
                )
            raise MigrationBaselineSelectionError(
                "the database has no existing public objects; do not baseline a "
                "blank database—apply every migration normally"
            )

        applied = []
        for item in plan:
            if item.state not in {"pending", "retryable_failure", "interrupted"}:
                continue
            migration = item.migration
            cur = conn.cursor()
            try:
                cur.execute(_MARK_RUNNING_SQL, (migration.filename, migration.checksum))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

            try:
                sql_text = migration.path.read_text()
                cur = conn.cursor()
                try:
                    cur.execute(sql_text)
                    cur.execute(_MARK_APPLIED_SQL, (migration.filename,))
                    conn.commit()
                finally:
                    cur.close()
            except Exception as exc:
                conn.rollback()
                _record_failure(conn, migration.filename, exc)
                raise MigrationExecutionError(
                    f"forward migration {migration.filename} failed; the transaction "
                    "was rolled back and the ledger records the error"
                ) from exc
            applied.append(migration.filename)

        final_plan = plan_migrations(migrations, _read_ledger(conn))
        return MigrationRunResult(
            applied=tuple(applied),
            baselined=baselined,
            plan=final_plan,
        )
    finally:
        _release_lock(conn)
