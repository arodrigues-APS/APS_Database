from __future__ import annotations

import os
import uuid
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import parse_dsn

import aps.db.models as model_module
from aps.config import Settings
from aps.db.migrations import (
    discover_migrations,
    MigrationBaselineRequired,
    MigrationChecksumMismatch,
    MigrationExecutionError,
    run_migrations,
)
from aps.db.models import ModelDefinition, build_model
from aps.paths import SCHEMA_DIR
from aps.pipelines.nightly import PostgresRunLedger, Step
from aps.provenance import SourceProvenance


pytestmark = pytest.mark.integration


@pytest.fixture
def postgres_connection():
    dsn = os.environ.get("APS_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("APS_TEST_POSTGRES_DSN is required for integration tests")

    params = parse_dsn(dsn)
    admin_params = dict(params)
    admin_params["dbname"] = "postgres"
    database_name = f"aps_arch_{uuid.uuid4().hex[:20]}"

    admin = psycopg2.connect(**admin_params)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
            )
        connection_params = dict(params)
        connection_params["dbname"] = database_name
        connection = psycopg2.connect(**connection_params)
        try:
            yield connection
        finally:
            connection.close()
    finally:
        with admin.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid <> pg_backend_pid()
                """,
                (database_name,),
            )
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(database_name)
                )
            )
        admin.close()


def test_blank_forward_migrations_are_transactional_and_idempotent(
    postgres_connection,
    tmp_path: Path,
):
    (tmp_path / "001_create_demo.sql").write_text(
        "CREATE TABLE lifecycle_demo (id INTEGER PRIMARY KEY);"
    )
    (tmp_path / "002_extend_demo.sql").write_text(
        "ALTER TABLE lifecycle_demo ADD COLUMN label TEXT;"
    )

    first = run_migrations(postgres_connection, directory=tmp_path)
    second = run_migrations(postgres_connection, directory=tmp_path)

    assert first.applied == ("001_create_demo.sql", "002_extend_demo.sql")
    assert first.baselined == ()
    assert second.applied == ()
    assert {item.state for item in second.plan} == {"applied"}


def test_existing_database_adopts_exact_history_then_applies_new_migration(
    postgres_connection,
):
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE baselines_metadata (id SERIAL PRIMARY KEY)"
        )
    postgres_connection.commit()

    with pytest.raises(MigrationBaselineRequired):
        run_migrations(postgres_connection, directory=SCHEMA_DIR)

    result = run_migrations(
        postgres_connection,
        directory=SCHEMA_DIR,
        baseline_existing_through="026_irradiation_energy_windows.sql",
    )

    assert result.baselined[-1] == "026_irradiation_energy_windows.sql"
    expected_forward = tuple(
        migration.filename for migration in discover_migrations(SCHEMA_DIR)
        if migration.filename > "026_irradiation_energy_windows.sql"
    )
    assert result.applied == expected_forward
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                to_regclass('public.avalanche_campaigns'),
                EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'baselines_metadata'
                      AND column_name = 'avalanche_family'
                )
            """
        )
        relation, column_exists = cursor.fetchone()
    assert relation == "avalanche_campaigns"
    assert column_exists is True


def test_checksum_mismatch_and_failed_migration_rollback_are_recorded(
    postgres_connection,
    tmp_path: Path,
):
    first = tmp_path / "001_create_demo.sql"
    first.write_text("CREATE TABLE checksum_demo (id INTEGER PRIMARY KEY);")
    run_migrations(postgres_connection, directory=tmp_path)

    first.write_text(
        "CREATE TABLE checksum_demo (id INTEGER PRIMARY KEY, changed BOOLEAN);"
    )
    with pytest.raises(MigrationChecksumMismatch):
        run_migrations(postgres_connection, directory=tmp_path)
    first.write_text("CREATE TABLE checksum_demo (id INTEGER PRIMARY KEY);")

    broken = tmp_path / "002_retryable.sql"
    broken.write_text(
        """
        CREATE TABLE rollback_marker (id INTEGER PRIMARY KEY);
        INSERT INTO migration_prerequisite (id) VALUES (1);
        """
    )
    with pytest.raises(MigrationExecutionError):
        run_migrations(postgres_connection, directory=tmp_path)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT to_regclass('public.rollback_marker'),
                   status
            FROM aps_forward_migrations
            WHERE filename = '002_retryable.sql'
            """
        )
        relation, status = cursor.fetchone()
        cursor.execute(
            "CREATE TABLE migration_prerequisite (id INTEGER PRIMARY KEY)"
        )
    postgres_connection.commit()

    assert relation is None
    assert status == "failed"
    retry = run_migrations(postgres_connection, directory=tmp_path)
    assert retry.applied == ("002_retryable.sql",)


def test_model_and_pipeline_ledgers_persist_real_transitions(
    postgres_connection,
    tmp_path: Path,
    monkeypatch,
):
    model_sql = tmp_path / "demo_model.sql"
    model_sql.write_text(
        "SELECT pg_sleep(0.05);"
        "CREATE OR REPLACE VIEW demo_model_view AS SELECT 1 AS value;"
    )
    model = ModelDefinition(
        name="demo-model",
        description="integration model",
        sql_paths=(model_sql,),
        expected_relations=("demo_model_view",),
    )
    source = SourceProvenance(
        code_sha="a" * 40,
        dirty=False,
        fingerprint="integration-source",
        changed_paths=(),
    )
    monkeypatch.setattr(model_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(model_module, "collect_source_provenance", lambda: source)
    settings = Settings.from_environ({"APS_PROFILE": "test"})

    build = build_model(
        postgres_connection,
        model,
        settings=settings,
    )
    assert build.status == "succeeded"

    ledger = PostgresRunLedger(
        postgres_connection,
        source_provenance=source.as_dict(),
    )
    run_id = ledger.start_run(settings=settings, step_names=("demo-step",))
    step_id = ledger.start_step(
        run_id,
        Step("demo-step", ("-m", "demo"), "integration step"),
    )
    ledger.finish_step(step_id, status="succeeded", duration_ms=3)
    ledger.finish_run(run_id, "succeeded")

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status, source_fingerprint ->> 'fingerprint',
                   completed_at - started_at
            FROM aps_model_builds
            WHERE id = %s
            """,
            (build.build_id,),
        )
        model_status, model_source, model_duration = cursor.fetchone()
        cursor.execute(
            """
            SELECT pipeline.status, step.status,
                   pipeline.source_fingerprint ->> 'fingerprint'
            FROM pipeline_runs pipeline
            JOIN pipeline_step_runs step
              ON step.pipeline_run_id = pipeline.id
            WHERE pipeline.id = %s
            """,
            (run_id,),
        )
        pipeline_status, step_status, pipeline_source = cursor.fetchone()

    assert (model_status, model_source) == ("succeeded", "integration-source")
    assert model_duration.total_seconds() >= 0.05
    assert (pipeline_status, step_status, pipeline_source) == (
        "succeeded",
        "succeeded",
        "integration-source",
    )
