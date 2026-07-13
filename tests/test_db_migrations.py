from pathlib import Path

import pytest

from aps.db.migrations import (
    MigrationBaselineSelectionError,
    MigrationChecksumMismatch,
    RecordedMigration,
    assert_plan_is_safe,
    discover_migrations,
    plan_migrations,
    select_baseline_migrations,
)


def test_discovery_selects_only_numbered_non_pipeline_assets(tmp_path: Path):
    (tmp_path / "001_core.sql").write_text("CREATE TABLE demo (id integer);")
    (tmp_path / "002_model.sql").write_text(
        "-- apply_schema: pipeline-owned\nCREATE VIEW demo_view AS SELECT 1;"
    )
    (tmp_path / "device_mapping_rules.sql").write_text("SELECT 1;")

    migrations = discover_migrations(tmp_path)

    assert [migration.filename for migration in migrations] == ["001_core.sql"]


def test_plan_marks_applied_and_checksum_mismatch(tmp_path: Path):
    path = tmp_path / "001_core.sql"
    path.write_text("SELECT 1;")
    migration = discover_migrations(tmp_path)[0]

    applied = plan_migrations(
        (migration,),
        (RecordedMigration(migration.filename, migration.checksum, "applied"),),
    )
    assert applied[0].state == "applied"

    changed = plan_migrations(
        (migration,),
        (RecordedMigration(migration.filename, "different", "applied"),),
    )
    assert changed[0].state == "checksum_mismatch"
    with pytest.raises(MigrationChecksumMismatch, match="001_core.sql"):
        assert_plan_is_safe(changed)


def test_failed_migration_is_explicitly_retryable(tmp_path: Path):
    (tmp_path / "001_core.sql").write_text("SELECT 1;")
    migration = discover_migrations(tmp_path)[0]

    plan = plan_migrations(
        (migration,),
        (RecordedMigration(migration.filename, migration.checksum, "failed"),),
    )

    assert plan[0].state == "retryable_failure"


def test_baseline_selection_requires_an_exact_discovered_prefix(tmp_path: Path):
    for filename in ("001_first.sql", "002_second.sql", "003_new.sql"):
        (tmp_path / filename).write_text("SELECT 1;")
    migrations = discover_migrations(tmp_path)

    selected = select_baseline_migrations(migrations, "002_second.sql")

    assert [migration.filename for migration in selected] == [
        "001_first.sql",
        "002_second.sql",
    ]
    with pytest.raises(MigrationBaselineSelectionError, match="unknown baseline cutoff"):
        select_baseline_migrations(migrations, "002")
