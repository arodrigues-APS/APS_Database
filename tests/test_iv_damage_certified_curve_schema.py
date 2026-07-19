from pathlib import Path


SQL = Path("schema/035_iv_damage_certified_curves.sql").read_text().lower()


def test_authoritative_acquisition_identity_is_relationally_enforced():
    assert "create table iv_damage_acquisitions" in SQL
    assert "metadata_id integer not null unique" in SQL
    assert "add column acquisition_id bigint" in SQL
    assert "alter column acquisition_id set not null" in SQL
    assert "iv_damage_observation_acquisition_guard" in SQL
    assert "iv_damage_response_observation_guard" in SQL
    assert "create table iv_damage_stress_sessions" in SQL


def test_external_certification_is_selected_sealed_and_single_use():
    assert "create table iv_damage_model_selections" in SQL
    assert "create table iv_damage_external_certifications" in SQL
    assert "dataset_snapshot_id bigint not null unique" in SQL
    assert "one-time sealed external certification" in SQL
    assert "grouped_test" in SQL
    assert "evaluation_kind" in SQL


def test_shadow_and_decision_deployments_are_distinct():
    assert "deployment_mode text not null check (deployment_mode in ('shadow', 'decision'))" in SQL
    assert "iv_damage_active_deployment_uq" in SQL
    assert "iv_damage_monitoring_assessments" in SQL


def test_projection_and_learned_curve_claims_have_separate_registries():
    assert "create table iv_damage_curve_projections" in SQL
    assert "create table iv_damage_curve_model_runs" in SQL
    assert "create table iv_damage_curve_external_certifications" in SQL
    assert "create table iv_damage_curve_prediction_points" in SQL
    assert "functional full-curve claim" in SQL


def test_dashboard_contracts_expose_units_gates_and_time():
    assert "create view iv_damage_release_gate_check_view" in SQL
    assert "response_unit" in SQL
    assert "create view iv_damage_temporal_monitoring_view" in SQL
    assert "date_trunc('week'" in SQL
    assert "create view iv_damage_curve_prediction_view" in SQL
    assert "predicted_i_drain_a" in SQL
