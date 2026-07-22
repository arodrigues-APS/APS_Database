from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import uuid

import psycopg2
from psycopg2 import errors, sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json, execute_values
import pytest

from aps.ml.iv_damage_research_dataset import audit_pair, deterministic_split_assignments
from aps.ml.iv_damage_research_repository import (
    freeze_snapshot,
    load_frozen_snapshot_pairs,
    pair_ids,
    persist_assignments,
)
from tests.test_iv_damage_research_dataset import make_pair


pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
CERTIFIED_MIGRATIONS = tuple(
    path
    for path in sorted((ROOT / "schema").glob("*.sql"))
    if "032_iv_damage_prediction.sql" <= path.name <= "045_iv_damage_activation_readiness.sql"
)
RESEARCH_MIGRATION = ROOT / "schema/046_iv_damage_research_prediction.sql"


@pytest.fixture
def postgres_connection():
    dsn = os.environ.get("APS_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("APS_TEST_POSTGRES_DSN is required for integration tests")
    params = parse_dsn(dsn)
    admin_params = {**params, "dbname": "postgres"}
    database_name = f"aps_research_{uuid.uuid4().hex[:18]}"
    admin = psycopg2.connect(**admin_params)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        connection = psycopg2.connect(**{**params, "dbname": database_name})
        try:
            yield connection
        finally:
            connection.close()
    finally:
        with admin.cursor() as cursor:
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",
                (database_name,),
            )
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name)))
        admin.close()


def _apply_through_046(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE baselines_metadata (
            id SERIAL PRIMARY KEY, device_id TEXT, device_type TEXT,
            manufacturer TEXT, file_hash TEXT
        );
        CREATE TABLE baselines_measurements (
            id BIGSERIAL PRIMARY KEY,
            metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id),
            point_index INTEGER NOT NULL, v_gate DOUBLE PRECISION,
            v_drain DOUBLE PRECISION, i_drain DOUBLE PRECISION
        );
        """
    )
    for migration in CERTIFIED_MIGRATIONS:
        cursor.execute(migration.read_text())
    cursor.execute(
        """
        CREATE TABLE irradiation_campaigns (id SERIAL PRIMARY KEY, campaign_name TEXT);
        CREATE TABLE irradiation_runs (id SERIAL PRIMARY KEY);
        CREATE TABLE iv_physical_curve_features (
            id BIGSERIAL PRIMARY KEY,
            metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id)
        );
        CREATE TABLE iv_physical_response_pairs (
            id BIGSERIAL PRIMARY KEY,
            pre_feature_id BIGINT REFERENCES iv_physical_curve_features(id),
            post_feature_id BIGINT REFERENCES iv_physical_curve_features(id),
            pre_metadata_id INTEGER REFERENCES baselines_metadata(id),
            post_metadata_id INTEGER REFERENCES baselines_metadata(id)
        );
        """
    )
    cursor.execute(RESEARCH_MIGRATION.read_text())
    connection.commit()


def _seed_frozen_snapshot(connection):
    pairs = [
        make_pair(1, device="device-1", run="run-1", campaign="campaign-1", shift=0.1),
        make_pair(2, device="device-2", run="run-2", campaign="campaign-2", shift=0.2),
    ]
    cursor = connection.cursor()
    execute_values(
        cursor,
        "INSERT INTO baselines_metadata (id,device_id,device_type,manufacturer,file_hash) VALUES %s",
        [
            (pair.pre_metadata_id, pair.physical_device_key, pair.device_type, pair.manufacturer, f"pre-{pair.pair_key}")
            for pair in pairs
        ]
        + [
            (pair.post_metadata_id, pair.physical_device_key, pair.device_type, pair.manufacturer, f"post-{pair.pair_key}")
            for pair in pairs
        ],
    )
    execute_values(
        cursor,
        "INSERT INTO iv_physical_curve_features (id,metadata_id) VALUES %s",
        [
            (pair.pre_feature_id, pair.pre_metadata_id)
            for pair in pairs
        ]
        + [
            (pair.post_feature_id, pair.post_metadata_id)
            for pair in pairs
        ],
    )
    execute_values(
        cursor,
        """INSERT INTO iv_physical_response_pairs
           (id,pre_feature_id,post_feature_id,pre_metadata_id,post_metadata_id)
           VALUES %s""",
        [
            (
                pair.source_pair_id,
                pair.pre_feature_id,
                pair.post_feature_id,
                pair.pre_metadata_id,
                pair.post_metadata_id,
            )
            for pair in pairs
        ],
    )
    measurement_rows = []
    for pair in pairs:
        for metadata_id, points in (
            (pair.pre_metadata_id, pair.pre_points),
            (pair.post_metadata_id, pair.post_points),
        ):
            measurement_rows.extend(
                (
                    point.source_point_id,
                    metadata_id,
                    point.point_index,
                    point.v_gate_v,
                    point.v_drain_v,
                    point.i_drain_a,
                )
                for point in points
            )
    execute_values(
        cursor,
        """INSERT INTO baselines_measurements
           (id,metadata_id,point_index,v_gate,v_drain,i_drain) VALUES %s""",
        measurement_rows,
    )
    connection.commit()

    audited = [audit_pair(pair) for pair in pairs]
    result = freeze_snapshot(
        connection,
        audited,
        snapshot_version="research-pg-v1",
        target_current_a=0.01,
        source_code_sha="a" * 40,
        source_fingerprint="b" * 64,
        actor="pytest-preparer",
        extraction_audit={"selected_target_current_a": 0.01},
        source_cutoff=datetime(2026, 7, 22, tzinfo=timezone.utc),
    )
    return int(result["snapshot_id"]), audited


def test_migration_046_is_additive_zero_state_and_certified_isolated(
    postgres_connection,
):
    _apply_through_046(postgres_connection)
    cursor = postgres_connection.cursor()
    cursor.execute(
        """
        SELECT to_regclass('iv_damage_research_snapshots'),
               to_regclass('iv_damage_research_scalar_validation_view'),
               to_regclass('iv_damage_research_curve_plot_view')
        """
    )
    assert all(cursor.fetchone())
    cursor.execute("SELECT count(*) FROM iv_damage_research_status_view")
    assert cursor.fetchone()[0] == 0
    cursor.execute("SELECT count(*) FROM iv_damage_decision_eligible_prediction_view")
    assert cursor.fetchone()[0] == 0


def test_snapshot_is_append_only(postgres_connection):
    _apply_through_046(postgres_connection)
    cursor = postgres_connection.cursor()
    cursor.execute(
        """INSERT INTO iv_damage_research_snapshots
        (snapshot_version,snapshot_hash,reference_policy,research_protocol_id,
         target_current_a,source_cutoff,source_query,source_code_sha,
         source_fingerprint,pair_count,device_count,campaign_count,run_count,
         extraction_audit,limitations,created_by)
        VALUES ('s1',%s,'same_device','research-v1',0.01,now(),'SELECT 1',
                'code',%s,0,0,0,0,%s,%s,'pytest') RETURNING id""",
        ("a" * 64, "b" * 64, Json({}), Json({"horizon": "unknown"})),
    )
    identity = cursor.fetchone()[0]
    postgres_connection.commit()
    with pytest.raises(errors.RaiseException, match="append-only"):
        cursor.execute(
            "UPDATE iv_damage_research_snapshots SET created_by='changed' WHERE id=%s",
            (identity,),
        )
    postgres_connection.rollback()



def test_training_loader_uses_frozen_rows_after_live_sources_change(postgres_connection):
    _apply_through_046(postgres_connection)
    snapshot_id, audited = _seed_frozen_snapshot(postgres_connection)

    original_current = audited[0].candidate.pre_points[0].i_drain_a
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE baselines_measurements SET i_drain=i_drain * 10 WHERE id=%s",
            (audited[0].candidate.pre_points[0].source_point_id,),
        )
    postgres_connection.commit()

    frozen = load_frozen_snapshot_pairs(postgres_connection, snapshot_id)
    assert frozen[0].candidate.pre_points[0].i_drain_a == pytest.approx(original_current)
    assert frozen[0].pair_payload_hash == audited[0].pair_payload_hash


def test_database_enforces_model_split_scalar_and_curve_provenance(postgres_connection):
    _apply_through_046(postgres_connection)
    snapshot_id, audited = _seed_frozen_snapshot(postgres_connection)
    assignments = deterministic_split_assignments(audited, "leave_device")
    ids = pair_ids(postgres_connection, snapshot_id)
    persist_assignments(postgres_connection, snapshot_id, ids, assignments)
    held = next(row for row in assignments if row.pair_key == audited[0].candidate.pair_key)

    cursor = postgres_connection.cursor()
    model_insert = """INSERT INTO iv_damage_research_model_runs
        (run_version,snapshot_id,model_family,method,validation_scheme,
         feature_mode,feature_contract,estimator_config,random_seed,
         source_code_sha,source_fingerprint,created_by)
        VALUES (%s,%s,%s,%s,'leave_device','physics_only',%s,%s,17,%s,%s,'pytest')
        RETURNING id"""

    with pytest.raises(errors.RaiseException, match="must enter lifecycle as candidates"):
        cursor.execute(
            """INSERT INTO iv_damage_research_model_runs
               (run_version,snapshot_id,model_family,method,validation_scheme,
                feature_mode,feature_contract,estimator_config,random_seed,
                source_code_sha,source_fingerprint,created_by,
                development_status,completed_at)
               VALUES (%s,%s,'scalar','huber','leave_device','physics_only',
                       %s,%s,17,%s,%s,'pytest','evaluated',clock_timestamp())""",
            (
                "invalid-evaluated",
                snapshot_id,
                Json({}),
                Json({}),
                "a" * 40,
                "b" * 64,
            ),
        )
    postgres_connection.rollback()

    cursor.execute(
        model_insert,
        (
            "scalar-candidate",
            snapshot_id,
            "scalar",
            "huber",
            Json({}),
            Json({}),
            "a" * 40,
            "b" * 64,
        ),
    )
    scalar_model_id = int(cursor.fetchone()[0])
    cursor.execute(
        """INSERT INTO iv_damage_research_fold_manifests
           (model_run_id,fold_number,held_out_group_key,training_device_keys,
            training_device_hash,preprocessing_manifest)
           VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",
        (
            scalar_model_id,
            held.fold_number,
            held.held_out_group_key,
            Json(["device-2"]),
            "c" * 64,
            Json({"fit_scope": "outer_training_fold_only"}),
        ),
    )
    manifest_id = int(cursor.fetchone()[0])
    postgres_connection.commit()

    scalar_insert = """INSERT INTO iv_damage_research_scalar_predictions
        (model_run_id,curve_pair_id,fold_manifest_id,validation_scheme,
         fold_number,held_out_group_key,observed_delta_vth_v,
         predicted_delta_vth_v,residual_v,absolute_error_v,support_status)
        VALUES (%s,%s,%s,'leave_device',%s,%s,%s,0.05,0.0,0.0,'supported')
        RETURNING id"""
    with pytest.raises(errors.RaiseException, match="truth does not match"):
        cursor.execute(
            scalar_insert,
            (
                scalar_model_id,
                ids[held.pair_key],
                manifest_id,
                held.fold_number,
                held.held_out_group_key,
                float(audited[0].observed_delta_vth_v) + 1.0,
            ),
        )
    postgres_connection.rollback()

    cursor.execute(
        scalar_insert,
        (
            scalar_model_id,
            ids[held.pair_key],
            manifest_id,
            held.fold_number,
            held.held_out_group_key,
            float(audited[0].observed_delta_vth_v),
        ),
    )
    scalar_prediction_id = int(cursor.fetchone()[0])
    cursor.execute(
        model_insert,
        (
            "curve-candidate",
            snapshot_id,
            "hybrid_curve",
            "hybrid_huber",
            Json({}),
            Json({}),
            "a" * 40,
            "b" * 64,
        ),
    )
    curve_model_id = int(cursor.fetchone()[0])
    postgres_connection.commit()

    other_pair_key = audited[1].candidate.pair_key
    with pytest.raises(errors.RaiseException, match="does not match scalar out-of-fold provenance"):
        cursor.execute(
            """INSERT INTO iv_damage_research_curve_predictions
               (model_run_id,scalar_prediction_id,curve_pair_id,validation_scheme,
                fold_number,held_out_group_key,scalar_shift_v,scalar_shift_source,
                correction_applied,fallback_reason,support_status)
               VALUES (%s,%s,%s,'leave_device',%s,%s,0.05,'out_of_fold_predicted',
                       false,'test fallback','fallback')""",
            (
                curve_model_id,
                scalar_prediction_id,
                ids[other_pair_key],
                held.fold_number,
                held.held_out_group_key,
            ),
        )
    postgres_connection.rollback()

    cursor.execute("SELECT count(*) FROM iv_damage_decision_eligible_prediction_view")
    assert cursor.fetchone()[0] == 0
