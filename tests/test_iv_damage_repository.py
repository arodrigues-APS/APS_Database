from pathlib import Path

import pytest

from aps.db.migrations import discover_migrations
from aps.ml.iv_damage_repository import (
    PredictionRequest,
    dataset_snapshot_hash,
    post_value_from_response,
    request_key,
)


def request(**overrides):
    values = {
        "physical_device_key": "device-1",
        "device_type": "part-a",
        "measurement_protocol_id": "protocol-v1",
        "stress_type": "irradiation",
        "target_type": "delta_vth_v",
        "pre_value": 2.0,
        "pre_uncertainty": 0.01,
        "reference_policy": "same_device",
        "stress_features": {
            "ion_species": "Ni",
            "fluence_or_dose": 1e8,
            "post_measurement_delay_s": 60.0,
        },
        "request_source": "unit-test",
    }
    values.update(overrides)
    return PredictionRequest(**values)


def test_forward_migration_discovers_v3_schema_after_031():
    names = [
        migration.filename for migration in discover_migrations()
        if migration.filename >= "031_flask_avalanche_admin.sql"
    ]
    assert names == [
        "031_flask_avalanche_admin.sql",
        "032_iv_damage_prediction.sql",
        "033_iv_damage_downstream.sql",
        "034_iv_damage_hardening.sql",
        "034_iv_damage_z_curve_append_only_compat.sql",
        "035_iv_damage_certified_curves.sql",
        "036_iv_damage_certification_guards.sql",
        "037_iv_damage_curve_operations.sql",
        "038_iv_damage_authoritative_writer_guards.sql",
        "039_iv_damage_dashboard_contracts.sql",
        "040_iv_damage_scalar_shadow.sql",
        "041_iv_damage_promotion_guard.sql",
        "042_iv_damage_session_and_projection_release.sql",
        "043_iv_damage_multi_model_outcomes.sql",
        "044_iv_damage_release_observability.sql",
    ]


def test_schema_is_append_only_and_request_table_has_no_outcome_columns():
    sql = Path("schema/032_iv_damage_prediction.sql").read_text()
    upper = sql.upper()
    assert "TRUNCATE " not in upper
    assert "DROP TABLE" not in upper
    request_sql = sql.split("CREATE TABLE iv_damage_prediction_requests", 1)[1].split("CREATE INDEX", 1)[0]
    assert "post_metadata" not in request_sql
    assert "observed_response" not in request_sql
    assert "response_value" not in request_sql
    assert "CREATE VIEW iv_damage_decision_eligible_prediction_view" in sql
    assert "request.reference_policy = 'same_device'" in sql


def test_request_rejects_any_post_outcome_feature():
    with pytest.raises(ValueError, match="post-outcome"):
        request(stress_features={"fluence_or_dose": 1e8, "observed_response": 0.2})


def test_request_key_is_stable_and_sensitive_to_inputs():
    first = request()
    assert first.requested_prediction_horizon_s == 60.0
    assert request_key(first) == request_key(first)
    assert request_key(first) != request(pre_value=2.1)


def test_request_rejects_conflicting_prediction_horizon():
    with pytest.raises(ValueError, match="must equal"):
        request(requested_prediction_horizon_s=30.0)


def test_request_rejects_boolean_or_nonphysical_pre_values():
    with pytest.raises(ValueError, match="pre_value must be finite"):
        request(pre_value=True)
    with pytest.raises(ValueError, match=r"Rds\(on\) must be positive"):
        request(target_type="log_rdson_ratio", pre_value=0.0)


def test_dataset_hash_is_order_independent_but_provenance_sensitive():
    rows = [{"unit_key": "b", "response": 2}, {"unit_key": "a", "response": 1}]
    kwargs = {
        "extraction_versions": {"vth": "v3"},
        "source_query": "SELECT ...",
        "source_code_sha": "abc",
    }
    first = dataset_snapshot_hash(unit_records=rows, **kwargs)
    second = dataset_snapshot_hash(unit_records=list(reversed(rows)), **kwargs)
    changed = dataset_snapshot_hash(unit_records=rows, **{**kwargs, "source_code_sha": "def"})
    assert first == second
    assert first != changed


def test_post_value_transform_is_target_correct():
    assert post_value_from_response("delta_vth_v", 2.0, 0.1) == pytest.approx(2.1)
    assert post_value_from_response("log_rdson_ratio", 20.0, 0.0) == pytest.approx(20.0)
    assert post_value_from_response("log_rdson_ratio", 20.0, 0.6931471805599453) == pytest.approx(40.0)
    with pytest.raises(ValueError, match="must be positive"):
        post_value_from_response("log_rdson_ratio", 0.0, 0.1)
