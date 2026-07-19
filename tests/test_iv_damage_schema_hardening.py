from pathlib import Path


SQL = Path("schema/034_iv_damage_hardening.sql").read_text()
LOWER = SQL.lower()


def test_hardening_is_a_forward_migration_with_frozen_members():
    assert "alter table iv_damage_dataset_snapshots" not in LOWER
    assert "create table iv_damage_dataset_snapshot_members" in LOWER
    assert "frozen_payload jsonb not null" in LOWER
    assert "payload_hash text not null" in LOWER
    assert "iv_damage_split_frozen_member_fk" in LOWER
    assert "drop table" not in LOWER
    assert "truncate" not in LOWER


def test_hardening_uses_acquisition_time_and_exact_prediction_outcomes():
    assert "add column measured_at timestamptz not null" in LOWER
    assert "add column pre_measured_at timestamptz not null" in LOWER
    assert "add column post_measured_at timestamptz not null" in LOWER
    assert "add column prediction_id bigint not null" in LOWER
    assert "facts.post_measured_at <= facts.predicted_at" in LOWER
    assert "outcome.prediction_id = prediction.id" in LOWER


def test_hardening_enforces_canonical_prediction_and_release_boundaries():
    assert "iv_damage_request_horizon_ck" in LOWER
    assert "iv_damage_prediction_decision_outputs_ck" in LOWER
    assert "iv_damage_prediction_insert_guard" in LOWER
    assert "iv_damage_release_lifecycle_guard" in LOWER
    assert "deactivation_kind in ('superseded', 'rollback', 'emergency')" in LOWER
    assert "prediction.created_at >= release.activated_at" in LOWER
    assert "measurement_protocol_ids" in LOWER
    assert LOWER.count("release_gate_eligible") >= 4
    assert "and policy.approved" in LOWER
    assert "create view iv_damage_prediction_backlog_view" in LOWER
    assert "request.request_status = 'pending'" in LOWER
    assert "clock_timestamp() - request.created_at as request_age" in LOWER
    assert LOWER.count("model.model_version") >= 2


def test_hardening_guards_scientific_records_and_safe_json_casts():
    for trigger in (
        "iv_damage_observations_immutable",
        "iv_damage_responses_immutable",
        "iv_damage_snapshots_immutable",
        "iv_damage_snapshot_members_immutable",
        "iv_damage_splits_immutable",
        "iv_damage_validation_immutable",
        "iv_damage_predictions_immutable",
        "iv_damage_outcomes_immutable",
    ):
        assert trigger in LOWER
    assert "create function iv_damage_is_finite" in LOWER
    assert "create function iv_damage_try_float8" in LOWER
    assert "iv_damage_try_float8(prediction.stress_features->>'beam_energy_mev')" in LOWER
