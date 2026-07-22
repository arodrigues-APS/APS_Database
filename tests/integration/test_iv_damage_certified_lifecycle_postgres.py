from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path
import uuid

import psycopg2
from psycopg2 import errors, sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json
import pytest

from aps.common import apply_schema
from aps.enrich.iv_parameters.contracts import ExtractionConfig
from aps.ml.iv_damage_evidence import (
    AcceptancePolicySpec,
    DamageEvidenceError,
    approve_acceptance_policy,
    approve_extraction_method,
    create_acceptance_policy,
    register_extraction_method,
)
from aps.ml.iv_damage_manifest import (
    EvidenceManifestError,
    apply_evidence,
    approve_evidence,
    evidence_status,
    plan_evidence,
    sha256,
    write_plan,
)


pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    path for path in sorted((ROOT / "schema").glob("*.sql"))
    if path.name >= "032_iv_damage_prediction.sql"
    and path.name <= "045_iv_damage_activation_readiness.sql"
)


@pytest.fixture
def postgres_connection():
    dsn = os.environ.get("APS_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("APS_TEST_POSTGRES_DSN is required for integration tests")
    params = parse_dsn(dsn)
    admin_params = dict(params)
    admin_params["dbname"] = "postgres"
    database_name = f"aps_damage_{uuid.uuid4().hex[:18]}"
    admin = psycopg2.connect(**admin_params)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
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
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                (database_name,),
            )
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database_name)))
        admin.close()


def _base_schema(cursor):
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


def _apply(cursor):
    for migration in MIGRATIONS:
        cursor.execute(migration.read_text())


def _certifiable_curve_policy():
    return {
        "max_median_abs_error": 0.1,
        "max_p90_abs_error": 0.2,
        "max_abs_bias": 0.1,
        "max_catastrophic_error_rate": 0.05,
        "max_mean_interval_width": 0.5,
        "catastrophic_error_threshold": 0.5,
        "curve_grid_points": 32,
        "curve_pca_components": 4,
        "curve_ridge_alpha": 1.0,
        "curve_interval_coverage": 0.8,
        "curve_min_development_curves": 6,
        "curve_min_development_devices": 3,
        "curve_min_external_curves": 6,
        "curve_min_external_devices": 3,
        "curve_max_mean_mae_a": 0.01,
        "curve_max_p90_error_a": 0.02,
        "curve_max_normalized_rmse": 0.25,
        "curve_min_band_coverage": 0.75,
        "projection_min_development_curves": 6,
        "projection_min_development_devices": 3,
        "projection_min_external_curves": 6,
        "projection_min_external_devices": 3,
        "projection_max_mean_mae_a": 0.01,
        "projection_max_p90_error_a": 0.02,
        "projection_max_normalized_rmse": 0.25,
        "projection_min_band_coverage": 0.75,
    }


def test_full_damage_migration_chain_is_transactional_and_rolls_back(postgres_connection):
    cursor = postgres_connection.cursor()
    _base_schema(cursor)
    postgres_connection.commit()
    _apply(cursor)
    cursor.execute(
        """
        SELECT to_regclass('iv_damage_model_runs'),
               to_regclass('iv_damage_curve_model_runs'),
               to_regclass('iv_damage_curve_projection_view')
        """
    )
    assert all(cursor.fetchone())
    postgres_connection.rollback()
    cursor.execute(
        "SELECT to_regclass('iv_damage_model_runs'), to_regclass('iv_damage_curve_model_runs')"
    )
    assert cursor.fetchone() == (None, None)
    cursor.close()


def test_legacy_apply_schema_does_not_replay_forward_damage_migrations(
    postgres_connection, tmp_path: Path
):
    cursor = postgres_connection.cursor()
    _base_schema(cursor)
    _apply(cursor)
    postgres_connection.commit()

    for migration in MIGRATIONS:
        (tmp_path / migration.name).write_text(migration.read_text())
    (tmp_path / "legacy_repeatable.sql").write_text(
        "CREATE TABLE IF NOT EXISTS legacy_repeatable_probe (id INTEGER);\n"
    )

    apply_schema(postgres_connection, schema_dir=tmp_path)
    apply_schema(
        postgres_connection, include_pipeline=True, schema_dir=tmp_path
    )

    cursor.execute(
        """
        SELECT to_regclass('iv_damage_curve_model_runs'),
               to_regclass('legacy_repeatable_probe')
        """
    )
    assert all(cursor.fetchone())
    cursor.execute(
        "SELECT DISTINCT filename FROM schema_migrations ORDER BY filename"
    )
    assert cursor.fetchall() == [("legacy_repeatable.sql",)]
    cursor.close()


def test_governed_writer_persists_and_approves_certifiable_curve_policy(
    postgres_connection,
):
    cursor = postgres_connection.cursor()
    _base_schema(cursor)
    _apply(cursor)
    postgres_connection.commit()
    policy_id = create_acceptance_policy(
        postgres_connection,
        AcceptancePolicySpec(
            "curve-policy-v1",
            "irradiation",
            "delta_vth_v",
            _certifiable_curve_policy(),
        ),
    )
    assert approve_acceptance_policy(
        postgres_connection,
        policy_version="curve-policy-v1",
        approved_by="independent-policy-owner",
    ) == policy_id
    cursor.execute(
        """
        SELECT approved, requirements->>'curve_max_mean_mae_a',
               requirements->>'curve_min_external_devices',
               requirements->>'projection_max_p90_error_a',
               requirements->>'projection_min_development_curves'
        FROM iv_damage_acceptance_policies WHERE id = %s
        """,
        (policy_id,),
    )
    assert cursor.fetchone() == (True, "0.01", "3", "0.02", "6")
    cursor.close()


def test_authoritative_evidence_certification_shadow_and_promotion_guards(postgres_connection):
    cursor = postgres_connection.cursor()
    _base_schema(cursor)
    _apply(cursor)
    postgres_connection.commit()
    cursor.execute(
        """
        SELECT count(*) FROM pg_constraint
        WHERE conname IN (
            'iv_damage_prediction_outcomes_request_id_key',
            'iv_damage_prediction_outcomes_response_unit_id_key'
        )
        """
    )
    assert cursor.fetchone()[0] == 0
    cursor.execute(
        """
        SELECT count(*) FROM pg_constraint
        WHERE conname = 'iv_damage_outcome_prediction_uq'
        """
    )
    assert cursor.fetchone()[0] == 1

    cursor.execute(
        """
        INSERT INTO baselines_metadata (
            device_id, device_type, manufacturer, file_hash
        ) VALUES ('device-1', 'MOSFET-X', 'Lab', 'file-pre'),
                 ('device-1', 'MOSFET-X', 'Lab', 'file-post')
        RETURNING id
        """
    )
    pre_metadata, post_metadata = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        """
        INSERT INTO iv_damage_acquisitions (
            acquisition_key, metadata_id, physical_device_key, device_type,
            manufacturer, measurement_protocol_id, curve_family, measured_at,
            source_file_hash, point_payload_hash, point_count, identity_source
        ) VALUES
          ('acq-pre', %s, 'device-1', 'MOSFET-X', 'Lab', 'idvg-v1', 'IdVg',
           '2025-01-01T10:00:00Z', 'file-pre', %s, 3, 'metadata_exact'),
          ('acq-post', %s, 'device-1', 'MOSFET-X', 'Lab', 'idvg-v1', 'IdVg',
           '2025-01-02T10:00:00Z', 'file-post', %s, 3, 'metadata_exact')
        RETURNING id
        """,
        (pre_metadata, "a" * 64, post_metadata, "b" * 64),
    )
    pre_acquisition, post_acquisition = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        """
        INSERT INTO iv_damage_extraction_methods (
            method_version, config_version, metric_name, target_type,
            configuration, approved
        ) VALUES ('method-v1', 'config-v1', 'vth_v', 'delta_vth_v',
                  '{}'::jsonb, FALSE)
        RETURNING id
        """
    )
    method_id = cursor.fetchone()[0]
    cursor.execute(
        """
        UPDATE iv_damage_extraction_methods
        SET approved = TRUE, approved_by = 'reviewer',
            approved_at = clock_timestamp()
        WHERE id = %s
        """,
        (method_id,),
    )

    cursor.execute("SAVEPOINT wrong_source")
    with pytest.raises(errors.RaiseException, match="authoritative acquisition payload"):
        cursor.execute(
            """
            INSERT INTO iv_damage_metric_observations (
                observation_key, metadata_id, extraction_method_id,
                measurement_protocol_id, metric_name, value, unit,
                accepted_point_count, replicate_group_key, quality_status,
                source_fingerprint, measured_at
            ) VALUES ('wrong', %s, %s, 'idvg-v1', 'vth_v', 3.0, 'V', 3,
                      'pre', 'usable', %s, '2025-01-01T10:00:00Z')
            """,
            (pre_metadata, method_id, Json({"acquisition_point_payload_hash": "f" * 64})),
        )
    cursor.execute("ROLLBACK TO SAVEPOINT wrong_source")

    observation_ids = []
    for key, metadata_id, acquisition_hash, measured_at, value in (
        ("pre", pre_metadata, "a" * 64, "2025-01-01T10:00:00Z", 3.0),
        ("post", post_metadata, "b" * 64, "2025-01-02T10:00:00Z", 3.2),
    ):
        cursor.execute(
            """
            INSERT INTO iv_damage_metric_observations (
                observation_key, metadata_id, extraction_method_id,
                measurement_protocol_id, metric_name, value, unit,
                accepted_point_count, replicate_group_key, quality_status,
                source_fingerprint, measured_at
            ) VALUES (%s, %s, %s, 'idvg-v1', 'vth_v', %s, 'V', 3,
                      %s, 'usable', %s, %s) RETURNING id, acquisition_id
            """,
            (key, metadata_id, method_id, value, key, Json({"acquisition_point_payload_hash": acquisition_hash}), measured_at),
        )
        observation_id, bound_acquisition = cursor.fetchone()
        observation_ids.append(observation_id)
        assert bound_acquisition in {pre_acquisition, post_acquisition}

    features = {"stress_condition_key": "condition-1", "pre_value": 3.0}
    cursor.execute(
        """
        INSERT INTO iv_damage_stress_sessions (
            stress_session_key, physical_device_key, stress_type, campaign_key,
            run_key, stress_condition_key, stress_features, identity_source
        ) VALUES ('session-1', 'device-1', 'irradiation', 'campaign-1',
                  'run-1', 'condition-1', %s, 'campaign_registry')
        RETURNING id
        """,
        (Json(features),),
    )
    stress_session_id = cursor.fetchone()[0]
    cursor.execute(
        """
        INSERT INTO iv_damage_response_units (
            unit_key, physical_device_key, stress_session_key, stress_type,
            target_type, device_type, measurement_protocol_id, campaign_key,
            run_key, pre_observation_ids, post_observation_ids, pre_value,
            post_value, response_value, pre_replicate_count,
            post_replicate_count, reference_policy, stress_features,
            required_features_complete, quality_status, pre_measured_at,
            post_measured_at
        ) VALUES ('unit-1', 'device-1', 'session-1', 'irradiation',
                  'delta_vth_v', 'MOSFET-X', 'idvg-v1', 'campaign-1', 'run-1',
                  %s, %s, 3.0, 3.2, 0.2, 1, 1, 'same_device', %s,
                  FALSE, 'usable', '2025-01-01T10:00:00Z', '2025-01-02T10:00:00Z')
        RETURNING id, stress_session_id
        """,
        ([observation_ids[0]], [observation_ids[1]], Json(features)),
    )
    response_id, bound_session = cursor.fetchone()
    assert bound_session == stress_session_id
    cursor.execute("SELECT count(*) FROM iv_damage_response_observations WHERE response_unit_id = %s", (response_id,))
    assert cursor.fetchone()[0] == 2

    cursor.execute(
        """
        INSERT INTO iv_damage_acceptance_policies (
            policy_version, stress_type, target_type, approved, requirements
        ) VALUES ('policy-v1', 'irradiation', 'delta_vth_v', FALSE,
                  '{}'::jsonb) RETURNING id
        """
    )
    policy_id = cursor.fetchone()[0]
    cursor.execute(
        """
        UPDATE iv_damage_acceptance_policies
        SET approved = TRUE, approved_by = 'reviewer',
            approved_at = clock_timestamp()
        WHERE id = %s
        """,
        (policy_id,),
    )
    frozen = {
        "unit_key": "unit-1", "stress_type": "irradiation",
        "target_type": "delta_vth_v", "response_value": 0.2,
    }
    cursor.execute(
        """
        INSERT INTO iv_damage_dataset_snapshots (
            snapshot_version, snapshot_hash, extraction_method_versions,
            source_query, source_code_sha, row_count, independent_group_count,
            domain_summary
        ) VALUES ('snapshot-v1', %s, '{}'::jsonb, 'frozen', 'code', 1, 1,
                  '{"stress_type":"irradiation","target_type":"delta_vth_v"}'::jsonb)
        RETURNING id
        """,
        ("c" * 64,),
    )
    snapshot_id = cursor.fetchone()[0]
    cursor.execute(
        """
        INSERT INTO iv_damage_dataset_snapshot_members (
            dataset_snapshot_id, response_unit_id, frozen_payload, payload_hash
        ) VALUES (%s, %s, %s, %s)
        """,
        (snapshot_id, response_id, Json(frozen), "d" * 64),
    )
    cursor.execute(
        """
        INSERT INTO iv_damage_split_assignments (
            dataset_snapshot_id, response_unit_id, split_scheme, split_role,
            group_key
        ) VALUES (%s, %s, 'frozen_release', 'external_test', 'device-1')
        """,
        (snapshot_id, response_id),
    )
    cursor.execute("SAVEPOINT legacy_candidate")
    with pytest.raises(errors.RaiseException, match="external certification not accessed"):
        cursor.execute(
            """
            INSERT INTO iv_damage_model_runs (
                model_version, model_name, stress_type, target_type,
                dataset_snapshot_id, acceptance_policy_id, algorithm,
                feature_schema, model_config, released_domain, validation_metrics,
                code_sha, environment_fingerprint, artifact_path,
                artifact_checksum, release_status
            ) VALUES ('legacy-combined-v1', 'legacy', 'irradiation', 'delta_vth_v',
                      %s, %s, 'huber', '{}'::jsonb, '{}'::jsonb,
                      '{"stress_type":"irradiation","target_type":"delta_vth_v"}'::jsonb,
                      '{"development_gate_eligible":true,"development_gate_checks":{}}'::jsonb,
                      'code', '{}'::jsonb, '/tmp/legacy', 'checksum', 'candidate')
            """,
            (snapshot_id, policy_id),
        )
    cursor.execute("ROLLBACK TO SAVEPOINT legacy_candidate")
    cursor.execute(
        """
        INSERT INTO iv_damage_model_runs (
            model_version, model_name, stress_type, target_type,
            dataset_snapshot_id, acceptance_policy_id, algorithm,
            feature_schema, model_config, released_domain, validation_metrics,
            code_sha, environment_fingerprint, artifact_path,
            artifact_checksum, release_status
        ) VALUES ('model-v1', 'certified', 'irradiation', 'delta_vth_v',
                  %s, %s, 'huber', '{}'::jsonb, '{}'::jsonb,
                  '{"stress_type":"irradiation","target_type":"delta_vth_v","measurement_protocol_ids":["idvg-v1"]}'::jsonb,
                  '{"release_gate_eligible":true,"development_gate_eligible":true,"development_gate_checks":{},"external_certification":"not_accessed"}'::jsonb,
                  'code', '{}'::jsonb, '/tmp/model', 'checksum', 'candidate')
        RETURNING id
        """,
        (snapshot_id, policy_id),
    )
    model_id = cursor.fetchone()[0]
    cursor.execute(
        """
        INSERT INTO iv_damage_model_selections (
            model_run_id, dataset_snapshot_id, stress_type, target_type,
            selection_protocol, selected_by
        ) VALUES (%s, %s, 'irradiation', 'delta_vth_v', '{}'::jsonb, 'selector')
        RETURNING id
        """,
        (model_id, snapshot_id),
    )
    selection_id = cursor.fetchone()[0]
    cursor.execute(
        """
        INSERT INTO iv_damage_external_certifications (
            selection_id, model_run_id, dataset_snapshot_id,
            evaluation_protocol, metrics, subgroup_metrics, gate_checks,
            passed, certified_by
        ) VALUES (%s, %s, %s, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                  '{}'::jsonb, TRUE, 'certifier')
        """,
        (selection_id, model_id, snapshot_id),
    )
    cursor.execute("UPDATE iv_damage_model_runs SET release_status = 'validated', validated_at = clock_timestamp() WHERE id = %s", (model_id,))
    cursor.execute(
        """
        INSERT INTO iv_damage_model_deployments (
            model_run_id, stress_type, target_type, deployment_mode, activated_by
        ) VALUES (%s, 'irradiation', 'delta_vth_v', 'shadow', 'operator')
        RETURNING id
        """,
        (model_id,),
    )
    deployment_id = cursor.fetchone()[0]
    cursor.execute("UPDATE iv_damage_model_runs SET release_status = 'shadow' WHERE id = %s", (model_id,))

    cursor.execute("SAVEPOINT premature_release")
    with pytest.raises(errors.RaiseException, match="prospective shadow assessment"):
        cursor.execute(
            """
            INSERT INTO iv_damage_model_releases (
                model_run_id, stress_type, target_type, active,
                activated_at, activated_by
            ) VALUES (%s, 'irradiation', 'delta_vth_v', TRUE,
                      clock_timestamp(), 'operator')
            """,
            (model_id,),
        )
    cursor.execute("ROLLBACK TO SAVEPOINT premature_release")

    cursor.execute(
        """
        INSERT INTO iv_damage_monitoring_assessments (
            model_run_id, deployment_id, assessment_kind, window_start,
            window_end, policy, metrics, checks, passed, assessed_by
        ) VALUES (%s, %s, 'shadow_promotion', '2025-01-01', '2025-02-01',
                  '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, TRUE, 'reviewer')
        """,
        (model_id, deployment_id),
    )
    cursor.execute(
        """
        INSERT INTO iv_damage_model_releases (
            model_run_id, stress_type, target_type, active,
            activated_at, activated_by
        ) VALUES (%s, 'irradiation', 'delta_vth_v', TRUE,
                  clock_timestamp(), 'operator') RETURNING id
        """,
        (model_id,),
    )
    assert cursor.fetchone()[0]
    cursor.execute(
        """
        UPDATE iv_damage_model_deployments
        SET active = FALSE, deactivated_by = 'operator',
            deactivated_at = clock_timestamp(), deactivation_reason = 'promoted'
        WHERE id = %s
        """,
        (deployment_id,),
    )
    cursor.execute("UPDATE iv_damage_model_runs SET release_status = 'released', released_at = clock_timestamp() WHERE id = %s", (model_id,))
    cursor.execute(
        """
        INSERT INTO iv_damage_model_deployments (
            model_run_id, stress_type, target_type, deployment_mode, activated_by
        ) VALUES (%s, 'irradiation', 'delta_vth_v', 'decision', 'operator')
        """,
        (model_id,),
    )
    postgres_connection.commit()


def test_activation_readiness_is_populated_before_any_evidence(postgres_connection):
    with postgres_connection.cursor() as cursor:
        _base_schema(cursor)
        _apply(cursor)
        cursor.execute(
            """
            SELECT claim_type, evidence_count, prediction_count, blocking_stage
            FROM iv_damage_claim_activation_status_view
            ORDER BY claim_type, stress_type, target_type NULLS LAST, curve_family NULLS LAST
            """
        )
        rows = cursor.fetchall()
        assert len(rows) == 8
        assert sum(row[0] == "scalar" for row in rows) == 4
        assert sum(row[0] == "curve" for row in rows) == 4
        assert all(row[1:] == (0, 0, "evidence") for row in rows)
        cursor.execute("SELECT count(*) FROM iv_damage_scalar_prediction_provenance_view")
        assert cursor.fetchone()[0] == 0
        cursor.execute("SAVEPOINT same_actor")
        with pytest.raises(errors.CheckViolation):
            cursor.execute(
                """
                INSERT INTO iv_damage_evidence_batches (
                    batch_key, manifest_version, plan_sha, manifest, plan_report,
                    prepared_by, prepared_at, approved_by, approved_at
                ) VALUES ('batch', 1, %s, '{}'::jsonb, '{}'::jsonb,
                          'same-actor', clock_timestamp(), 'same-actor', clock_timestamp())
                """, ("a" * 64,),
            )
        cursor.execute("ROLLBACK TO SAVEPOINT same_actor")
    postgres_connection.commit()


def _manifest_from_raw_measurements(postgres_connection):
    features = {
        "beam_energy_mev": 100.0,
        "let_surface": 10.0,
        "range_um": 20.0,
        "fluence_or_dose": 1.0e9,
        "irradiation_bias_v": 0.0,
        "temperature_c": 25.0,
        "post_measurement_delay_s": 3600.0,
        "prediction_horizon_s": 3600.0,
        "stress_condition_key": "ca-100mev",
    }
    acquisitions = []
    observations = []
    measurement_times = (
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T01:00:00+00:00",
        "2026-01-02T00:00:00+00:00",
        "2026-01-02T01:00:00+00:00",
    )
    thresholds = (2.0, 2.1, 2.5, 2.6)
    with postgres_connection.cursor() as cursor:
        for index, (measured_at, threshold) in enumerate(
            zip(measurement_times, thresholds, strict=True),
            start=1,
        ):
            file_hash = f"{index:064x}"
            cursor.execute(
                """
                INSERT INTO baselines_metadata (
                    device_id, device_type, manufacturer, file_hash
                ) VALUES ('device-1', 'IFX-Trench', 'Infineon', %s)
                RETURNING id
                """,
                (file_hash,),
            )
            metadata_id = int(cursor.fetchone()[0])
            raw_rows = []
            for point_index, (v_gate, current) in enumerate(
                (
                    (threshold - 1.0, 1.0e-4),
                    (threshold, 1.0e-3),
                    (threshold + 1.0, 1.0e-2),
                )
            ):
                cursor.execute(
                    """
                    INSERT INTO baselines_measurements (
                        metadata_id, point_index, v_gate, v_drain, i_drain
                    ) VALUES (%s, %s, %s, 0.1, %s)
                    RETURNING id
                    """,
                    (metadata_id, point_index, v_gate, current),
                )
                point_id = int(cursor.fetchone()[0])
                raw_rows.append(
                    {
                        "point_index": point_index,
                        "v_gate": float(v_gate),
                        "v_drain": 0.1,
                        "i_drain": float(current),
                        "point_id": point_id,
                    }
                )
            acquisition_key = f"acq-{index}"
            acquisitions.append(
                {
                    "item_key": f"acquisition-{index}",
                    "acquisition_key": acquisition_key,
                    "metadata_id": metadata_id,
                    "physical_device_key": "device-1",
                    "measurement_protocol_id": "idvg-vth-fixed-1h-v1",
                    "curve_family": "IdVg",
                    "measured_at": measured_at,
                    "identity_source": "metadata_exact",
                    "source_relation": "baselines_metadata",
                    "source_checksum": file_hash,
                }
            )
            phase = "pre" if index <= 2 else "post"
            observations.append(
                {
                    "item_key": f"observation-{index}",
                    "acquisition_key": acquisition_key,
                    "replicate_group_key": f"device-1-{phase}",
                    "source_relation": "baselines_measurements",
                    "source_checksum": sha256(
                        [
                            {
                                key: value
                                for key, value in row.items()
                                if key != "point_id"
                            }
                            for row in raw_rows
                        ]
                    ),
                    "source_row_ids": [
                        row["point_id"] for row in raw_rows
                    ],
                }
            )
    postgres_connection.commit()
    return {
        "manifest_version": 1,
        "batch_key": "irradiation-dvth-history-001",
        "prepared_by": "scientist-a",
        "prepared_at": "2026-01-04T00:00:00+00:00",
        "source_cutoff": "2026-01-03T00:00:00+00:00",
        "claim": {
            "stress_type": "irradiation",
            "target_type": "delta_vth_v",
            "intended_split_role": "train",
            "reference_policy": "same_device",
            "measurement_protocol_id": "idvg-vth-fixed-1h-v1",
            "prediction_horizon_s": 3600.0,
            "fixed_horizon": True,
        },
        "extraction_config": {
            "config_version": "vth-fixed-1h-v1",
            "target_type": "delta_vth_v",
            "target_current_a": 0.001,
            "required_vds_v": 0.1,
        },
        "acquisitions": acquisitions,
        "stress_sessions": [
            {
                "item_key": "stress-session-1",
                "stress_session_key": "stress-1",
                "physical_device_key": "device-1",
                "stress_type": "irradiation",
                "campaign_key": "campaign-1",
                "run_key": "run-1",
                "stress_condition_key": "ca-100mev",
                "stress_features": features,
                "identity_source": "campaign_registry",
            }
        ],
        "observations": observations,
        "response_units": [
            {
                "item_key": "response-1",
                "unit_key": "unit-1",
                "physical_device_key": "device-1",
                "stress_session_key": "stress-1",
                "stress_type": "irradiation",
                "target_type": "delta_vth_v",
                "device_type": "IFX-Trench",
                "manufacturer": "Infineon",
                "measurement_protocol_id": "idvg-vth-fixed-1h-v1",
                "campaign_key": "campaign-1",
                "run_key": "run-1",
                "ion_species": "Ca",
                "pre_observation_keys": [
                    "observation-1",
                    "observation-2",
                ],
                "post_observation_keys": [
                    "observation-3",
                    "observation-4",
                ],
                "stress_features": features,
                "reference_policy": "same_device",
                "minimum_replicates": 2,
            }
        ],
    }


def test_manifest_plan_approval_failure_resume_apply_and_immutability(
    postgres_connection,
    tmp_path: Path,
):
    with postgres_connection.cursor() as cursor:
        _base_schema(cursor)
        _apply(cursor)
    postgres_connection.commit()
    manifest = _manifest_from_raw_measurements(postgres_connection)

    invalid = deepcopy(manifest)
    invalid["observations"][0]["source_checksum"] = "0" * 64
    invalid_report = plan_evidence(postgres_connection, invalid)
    assert not invalid_report["admissible"]
    assert {
        exclusion["reason"] for exclusion in invalid_report["exclusions"]
    } == {"measurement_checksum_mismatch"}

    report = plan_evidence(postgres_connection, manifest)
    assert report["admissible"]
    assert report["collection_deficits"] == {
        "training_groups": 29,
        "calibration_groups": 10,
        "calibration_devices": 10,
        "sealed_external_groups": 30,
        "external_devices": 10,
    }
    calibration_manifest = deepcopy(manifest)
    calibration_manifest["claim"]["intended_split_role"] = "calibration"
    calibration_report = plan_evidence(
        postgres_connection, calibration_manifest
    )
    assert calibration_report["collection_deficits"] == {
        "training_groups": 30,
        "calibration_groups": 9,
        "calibration_devices": 9,
        "sealed_external_groups": 30,
        "external_devices": 10,
    }
    plan_path = write_plan(
        report,
        tmp_path,
        tmp_path / "evidence-report.json",
    )
    assert plan_path.is_file()

    with pytest.raises(
        EvidenceManifestError,
        match="different actor",
    ):
        approve_evidence(
            postgres_connection,
            governance_root=tmp_path,
            batch_key=manifest["batch_key"],
            expected_plan_sha=report["manifest_sha"],
            actor=manifest["prepared_by"],
        )

    approved = approve_evidence(
        postgres_connection,
        governance_root=tmp_path,
        batch_key=manifest["batch_key"],
        expected_plan_sha=report["manifest_sha"],
        actor="scientist-b",
    )
    assert approved["status"] == "approved"
    assert not approved["idempotent"]

    competing_params = parse_dsn(os.environ["APS_TEST_POSTGRES_DSN"])
    competing_params["dbname"] = postgres_connection.info.dbname
    competing = psycopg2.connect(**competing_params)
    try:
        with competing.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_advisory_lock(
                    hashtextextended(%s, 0)
                )
                """,
                (manifest["batch_key"],),
            )
        competing.commit()
        with pytest.raises(
            EvidenceManifestError,
            match="already being applied",
        ):
            apply_evidence(
                postgres_connection,
                batch_key=manifest["batch_key"],
                expected_plan_sha=report["manifest_sha"],
                actor="operator-a",
                source_provenance={"git_commit": "test"},
            )
    finally:
        with competing.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_advisory_unlock(
                    hashtextextended(%s, 0)
                )
                """,
                (manifest["batch_key"],),
            )
        competing.commit()
        competing.close()

    with pytest.raises(
        DamageEvidenceError,
        match="not registered",
    ):
        apply_evidence(
            postgres_connection,
            batch_key=manifest["batch_key"],
            expected_plan_sha=report["manifest_sha"],
            actor="operator-a",
            source_provenance={"git_commit": "test"},
        )
    failed = evidence_status(postgres_connection, manifest["batch_key"])
    assert failed["status"] == "failed"
    assert {
        (row["item_type"], row["status"], row["count"])
        for row in failed["item_counts"]
    } == {
        ("acquisition", "applied", 4),
        ("stress_session", "applied", 1),
        ("observation", "failed", 1),
        ("observation", "pending", 3),
        ("response_unit", "pending", 1),
    }

    config = ExtractionConfig(**manifest["extraction_config"])
    register_extraction_method(postgres_connection, config)
    approve_extraction_method(
        postgres_connection,
        method_version="iv-parameters-v3.0",
        config_version=config.config_version,
        metric_name="vth_v",
        approved_by="scientist-c",
    )

    applied = apply_evidence(
        postgres_connection,
        batch_key=manifest["batch_key"],
        expected_plan_sha=report["manifest_sha"],
        actor="operator-a",
        source_provenance={"git_commit": "test"},
    )
    assert applied == {
        "batch_id": approved["batch_id"],
        "status": "applied",
        "idempotent": False,
    }
    repeated = apply_evidence(
        postgres_connection,
        batch_key=manifest["batch_key"],
        expected_plan_sha=report["manifest_sha"],
        actor="operator-a",
        source_provenance={"git_commit": "test"},
    )
    assert repeated["idempotent"]

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                (SELECT count(*) FROM iv_damage_acquisitions),
                (SELECT count(*) FROM iv_damage_metric_observations),
                (SELECT count(*) FROM iv_damage_response_units),
                (SELECT response_value FROM iv_damage_response_units)
            """
        )
        acquisition_count, observation_count, response_count, response = (
            cursor.fetchone()
        )
        assert (acquisition_count, observation_count, response_count) == (
            4,
            4,
            1,
        )
        assert response == pytest.approx(0.5)

        cursor.execute("SAVEPOINT immutable_manifest")
        with pytest.raises(
            errors.RaiseException,
            match="manifest identity and payload are immutable",
        ):
            cursor.execute(
                """
                UPDATE iv_damage_evidence_batches
                SET manifest = manifest || '{"tampered": true}'::jsonb
                WHERE batch_key = %s
                """,
                (manifest["batch_key"],),
            )
        cursor.execute("ROLLBACK TO SAVEPOINT immutable_manifest")

        cursor.execute("SAVEPOINT immutable_item")
        with pytest.raises(
            errors.RaiseException,
            match="item identity and payload are immutable",
        ):
            cursor.execute(
                """
                UPDATE iv_damage_evidence_batch_items
                SET payload_sha = %s
                WHERE batch_id = %s
                """,
                ("f" * 64, approved["batch_id"]),
            )
        cursor.execute("ROLLBACK TO SAVEPOINT immutable_item")
    postgres_connection.commit()
