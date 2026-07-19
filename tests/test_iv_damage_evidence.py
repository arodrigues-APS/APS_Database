from datetime import datetime, timedelta, timezone

import pytest

from aps.enrich.iv_parameters.contracts import ExtractionConfig, MetricResult
from aps.ml.iv_damage_evidence import (
    AcceptancePolicySpec,
    DamageEvidenceConflict,
    DamageEvidenceError,
    ObservationContext,
    ResponseUnitSpec,
    StoredObservation,
    approve_acceptance_policy,
    approve_extraction_method,
    build_response_payload,
    create_acceptance_policy,
    persist_metric_observation,
    register_extraction_method,
    resolved_policy_requirements,
    validate_metric_observation,
    validate_policy_spec,
)


UTC = timezone.utc
T0 = datetime(2025, 1, 1, tzinfo=UTC)


def vth_config():
    return ExtractionConfig(
        config_version="vth-protocol-v1",
        target_type="delta_vth_v",
        target_current_a=0.01,
        required_vds_v=1.0,
    )


def result(**overrides):
    values = {
        "metric_name": "vth_v",
        "value": 2.0,
        "unit": "V",
        "method_version": "iv-parameters-v3.0",
        "config_version": "vth-protocol-v1",
        "quality_status": "usable",
        "uncertainty": 0.01,
        "n_points": 4,
        "diagnostics": {"bracket": [1, 2]},
    }
    values.update(overrides)
    return MetricResult(**values)


def context(**overrides):
    values = {
        "metadata_id": 10,
        "measurement_protocol_id": "protocol-v1",
        "replicate_group_key": "pre-device-1",
        "measured_at": T0,
        "source_fingerprint": {"sha256": "abc"},
    }
    values.update(overrides)
    return ObservationContext(**values)


def observation(
    identifier,
    value,
    measured_at,
    group,
    *,
    target_type="delta_vth_v",
    metric_name="vth_v",
    unit="V",
    uncertainty=0.01,
    protocol="protocol-v1",
    method_id=7,
    config_version="vth-protocol-v1",
    source_fingerprint=None,
):
    return StoredObservation(
        id=identifier,
        metadata_id=identifier + 100,
        extraction_method_id=method_id,
        measurement_protocol_id=protocol,
        metric_name=metric_name,
        value=value,
        unit=unit,
        uncertainty=uncertainty,
        accepted_point_count=4,
        replicate_group_key=group,
        quality_status="usable",
        quality_reasons=(),
        diagnostics={},
        source_fingerprint=(
            {"sha256": str(identifier)}
            if source_fingerprint is None
            else source_fingerprint
        ),
        measured_at=measured_at,
        method_version="iv-parameters-v3.0",
        config_version=config_version,
        target_type=target_type,
        method_approved=True,
    )


def response_spec(**overrides):
    values = {
        "unit_key": "unit-1",
        "physical_device_key": "device-1",
        "stress_session_key": "session-1",
        "stress_type": "sc",
        "target_type": "delta_vth_v",
        "device_type": "C2M",
        "measurement_protocol_id": "protocol-v1",
        "campaign_key": "campaign-1",
        "run_key": "run-1",
        "pre_observation_ids": [1, 2],
        "post_observation_ids": [3, 4],
        "stress_features": {
            "sc_voltage_v": 500.0,
            "sc_duration_us": 10.0,
            "peak_current_a": 20.0,
            "deposited_energy_j": 0.2,
            "pulse_count": 1,
            "gate_drive_v": 15.0,
            "temperature_c": 25.0,
            "stress_condition_key": "500V-10us-20A",
        },
    }
    values.update(overrides)
    return ResponseUnitSpec(**values)


def response_observations():
    return [
        observation(1, 2.0, T0, "pre"),
        observation(2, 2.0, T0 + timedelta(seconds=1), "pre"),
        observation(3, 2.2, T0 + timedelta(hours=1), "post", uncertainty=0.02),
        observation(
            4, 2.2, T0 + timedelta(hours=1, seconds=1), "post",
            uncertainty=0.02,
        ),
    ]


class ScriptedCursor:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.current = None
        self.executions = []
        self.closed = False

    def execute(self, sql, parameters=None):
        self.executions.append((" ".join(sql.split()), parameters))
        self.current = next(self.responses)

    def fetchone(self):
        return self.current

    def close(self):
        self.closed = True


class ScriptedConnection:
    def __init__(self, responses):
        self.cursor_instance = ScriptedCursor(responses)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def complete_policy_requirements():
    return {
        "max_median_abs_error": 0.1,
        "max_p90_abs_error": 0.2,
        "max_abs_bias": 0.1,
        "max_catastrophic_error_rate": 0.05,
        "max_mean_interval_width": 0.5,
        "catastrophic_error_threshold": 0.5,
    }


def complete_curve_claim_requirements():
    return {
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


def test_observation_requires_authoritative_timezone_aware_measurement_time():
    with pytest.raises(DamageEvidenceError, match="timezone-aware"):
        validate_metric_observation(
            result(),
            vth_config(),
            context(measured_at=datetime(2025, 1, 1)),
        )


def test_observation_rejects_metric_or_configuration_reinterpretation():
    with pytest.raises(DamageEvidenceError, match="config_version"):
        validate_metric_observation(
            result(config_version="other"), vth_config(), context(),
        )
    with pytest.raises(DamageEvidenceError, match="metric/unit"):
        validate_metric_observation(
            result(metric_name="rdson_mohm", unit="mohm"),
            vth_config(),
            context(),
        )


def test_response_is_recomputed_from_replicates_with_propagated_uncertainty():
    payload = build_response_payload(response_spec(), response_observations())
    assert payload.pre_value == pytest.approx(2.0)
    assert payload.post_value == pytest.approx(2.2)
    assert payload.response_value == pytest.approx(0.2)
    assert payload.response_uncertainty == pytest.approx(
        (0.01**2 + 0.02**2) ** 0.5
    )
    assert payload.pre_measured_at == T0 + timedelta(seconds=1)
    assert payload.post_measured_at == T0 + timedelta(hours=1)
    assert payload.required_features_complete
    assert payload.quality_status == "usable"


def test_response_uses_canonical_physical_feature_validation():
    payload = build_response_payload(
        response_spec(
            stress_features={
                **response_spec().stress_features,
                "pulse_count": 1.5,
            },
        ),
        response_observations(),
    )
    assert not payload.required_features_complete
    assert "outside_physical_bounds:pulse_count" in payload.quality_reasons


def test_response_requires_leave_condition_identity():
    features = dict(response_spec().stress_features)
    features.pop("stress_condition_key")
    with pytest.raises(DamageEvidenceError, match="stress_condition_key"):
        build_response_payload(
            response_spec(stress_features=features),
            response_observations(),
        )


def test_observation_quality_reasons_are_carried_into_response_audit():
    rows = response_observations()
    rows[0] = StoredObservation(
        **{
            **rows[0].__dict__,
            "quality_reasons": ("operator_reviewed_warning",),
        }
    )
    payload = build_response_payload(response_spec(), rows)
    assert "operator_reviewed_warning" in payload.quality_reasons


def test_log_ratio_response_and_relative_uncertainty_are_target_correct():
    rows = [
        observation(
            1, 20.0, T0, "pre", metric_name="rdson_mohm",
            unit="mohm", target_type="log_rdson_ratio", uncertainty=1.0,
        ),
        observation(
            2, 40.0, T0 + timedelta(hours=1), "post",
            metric_name="rdson_mohm", unit="mohm",
            target_type="log_rdson_ratio", uncertainty=2.0,
        ),
    ]
    payload = build_response_payload(
        response_spec(
            target_type="log_rdson_ratio",
            pre_observation_ids=[1],
            post_observation_ids=[2],
            minimum_replicates=1,
        ),
        rows,
    )
    assert payload.response_value == pytest.approx(0.6931471805599453)
    assert payload.response_uncertainty == pytest.approx(
        ((1.0 / 20.0) ** 2 + (2.0 / 40.0) ** 2) ** 0.5
    )


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            lambda: [
                *response_observations()[:3],
                observation(
                    4, 2.2, T0 + timedelta(hours=1), "post",
                    protocol="other",
                ),
            ],
            "protocol",
        ),
        (
            lambda: [
                *response_observations()[:3],
                observation(
                    4, 2.2, T0 + timedelta(hours=1), "post",
                    method_id=8,
                ),
            ],
            "method/config",
        ),
        (
            lambda: [
                *response_observations()[:3],
                observation(
                    4, 2.2, T0 + timedelta(hours=1), "post",
                    config_version="other",
                ),
            ],
            "method/config",
        ),
        (
            lambda: [
                *response_observations()[:2],
                observation(3, 2.2, T0, "post"),
                observation(4, 2.2, T0 + timedelta(seconds=2), "post"),
            ],
            "acquired after",
        ),
    ],
)
def test_response_fails_closed_on_protocol_method_or_chronology(rows, message):
    with pytest.raises(DamageEvidenceError, match=message):
        build_response_payload(response_spec(), rows())


def test_single_replicates_are_persistable_but_screening_only():
    payload = build_response_payload(
        response_spec(
            pre_observation_ids=[1],
            post_observation_ids=[3],
        ),
        [response_observations()[0], response_observations()[2]],
    )
    assert payload.quality_status == "screening_only"
    assert payload.quality_reasons == ("insufficient_replicates",)


def test_pre_and_post_cannot_reuse_one_replicate_group_identity():
    rows = [
        observation(1, 2.0, T0, "same-group"),
        observation(3, 2.2, T0 + timedelta(hours=1), "same-group"),
    ]
    with pytest.raises(DamageEvidenceError, match="distinct replicate groups"):
        build_response_payload(
            response_spec(
                pre_observation_ids=[1],
                post_observation_ids=[3],
                minimum_replicates=1,
            ),
            rows,
        )


def test_register_method_is_idempotent_only_for_identical_configuration():
    config = vth_config()
    connection = ScriptedConnection(
        [None, (7, "delta_vth_v", dict(config.__dict__))],
    )
    assert register_extraction_method(connection, config) == 7
    assert connection.commits == 1

    changed = dict(config.__dict__)
    changed["target_current_a"] = 0.02
    connection = ScriptedConnection(
        [None, (7, "delta_vth_v", changed)],
    )
    with pytest.raises(DamageEvidenceConflict):
        register_extraction_method(connection, config)
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "config",
    [
        lambda: ExtractionConfig(
            "bad-current", "delta_vth_v",
            target_current_a=0.0, required_vds_v=1.0,
        ),
        lambda: ExtractionConfig(
            "missing-vds", "delta_vth_v", target_current_a=0.01,
        ),
        lambda: ExtractionConfig(
            "bad-tolerance", "delta_vth_v",
            target_current_a=0.01, required_vds_v=1.0,
            vds_tolerance_v=-0.1,
        ),
        lambda: ExtractionConfig(
            "bad-window", "log_rdson_ratio",
            required_vgs_v=15.0, linear_vds_min_v=2.0,
            linear_vds_max_v=1.0,
        ),
        lambda: ExtractionConfig(
            "bad-range", "delta_vth_v",
            target_current_a=0.01, required_vds_v=1.0,
            valid_min=2.0, valid_max=1.0,
        ),
        lambda: ExtractionConfig(
            "nonfinite", "delta_vth_v",
            target_current_a=float("nan"), required_vds_v=1.0,
        ),
    ],
)
def test_method_registration_rejects_illogical_configuration(config):
    with pytest.raises(DamageEvidenceError):
        register_extraction_method(ScriptedConnection([]), config())


def test_persist_observation_requires_approved_method_and_records_measured_at():
    config = vth_config()
    connection = ScriptedConnection(
        [(7, "delta_vth_v", dict(config.__dict__), True), (42,)],
    )
    assert persist_metric_observation(
        connection, config=config, result=result(), context=context(),
    ) == 42
    insert_sql, parameters = connection.cursor_instance.executions[1]
    assert "measured_at" in insert_sql
    assert parameters[-1] == T0
    assert connection.commits == 1

    connection = ScriptedConnection(
        [(7, "delta_vth_v", dict(config.__dict__), False)],
    )
    with pytest.raises(DamageEvidenceError, match="not approved"):
        persist_metric_observation(
            connection, config=config, result=result(), context=context(),
        )
    assert connection.rollbacks == 1


def test_method_approval_records_actor_and_is_transactional():
    connection = ScriptedConnection([(7, False), None])
    assert approve_extraction_method(
        connection,
        method_version="iv-parameters-v3.0",
        config_version="vth-protocol-v1",
        metric_name="vth_v",
        approved_by="lab-owner",
    ) == 7
    update_sql, parameters = connection.cursor_instance.executions[1]
    assert "approved_by" in update_sql
    assert parameters == ("lab-owner", 7)
    assert connection.commits == 1


def test_policy_approval_requires_complete_finite_limits_before_update():
    incomplete = AcceptancePolicySpec(
        "policy-v1", "sc", "delta_vth_v", {},
    )
    with pytest.raises(DamageEvidenceError, match="without limit"):
        validate_policy_spec(incomplete, for_approval=True)
    limits_without_catastrophic_threshold = complete_policy_requirements()
    limits_without_catastrophic_threshold.pop("catastrophic_error_threshold")
    with pytest.raises(
        DamageEvidenceError,
        match="catastrophic_error_threshold",
    ):
        validate_policy_spec(
            AcceptancePolicySpec(
                "policy-v1",
                "sc",
                "delta_vth_v",
                limits_without_catastrophic_threshold,
            ),
            for_approval=True,
        )

    requirements = complete_policy_requirements()
    connection = ScriptedConnection(
        [(8, "sc", "delta_vth_v", requirements, False), None],
    )
    assert approve_acceptance_policy(
        connection, policy_version="policy-v1", approved_by="model-owner",
    ) == 8
    update_sql, parameters = connection.cursor_instance.executions[1]
    assert "approved_by" in update_sql
    assert parameters == ("model-owner", 8)
    assert connection.commits == 1


def test_policy_creation_is_idempotent_only_for_identical_definition():
    requirements = complete_policy_requirements()
    resolved = resolved_policy_requirements(
        AcceptancePolicySpec(
            "policy-v1", "sc", "delta_vth_v", requirements,
        )
    )
    spec = AcceptancePolicySpec(
        "policy-v1", "sc", "delta_vth_v", requirements,
    )
    connection = ScriptedConnection(
        [None, (8, "sc", "delta_vth_v", resolved)],
    )
    assert create_acceptance_policy(connection, spec) == 8
    inserted_requirements = connection.cursor_instance.executions[0][1][3]
    assert inserted_requirements.adapted == resolved
    assert connection.commits == 1

    connection = ScriptedConnection(
        [None, (8, "irradiation", "delta_vth_v", resolved)],
    )
    with pytest.raises(DamageEvidenceConflict):
        create_acceptance_policy(connection, spec)
    assert connection.rollbacks == 1


def test_policy_rejects_unknown_requirements():
    with pytest.raises(DamageEvidenceError, match="unknown policy"):
        validate_policy_spec(
            AcceptancePolicySpec(
                "policy-v1", "sc", "delta_vth_v", {"mystery_gate": 1},
            )
        )


def test_policy_writer_accepts_and_freezes_complete_curve_claim_blocks():
    requirements = {
        **complete_policy_requirements(),
        **complete_curve_claim_requirements(),
    }
    spec = AcceptancePolicySpec(
        "curve-policy-v1", "sc", "delta_vth_v", requirements,
    )
    resolved = resolved_policy_requirements(spec)
    validate_policy_spec(spec, for_approval=True)
    connection = ScriptedConnection(
        [None, (9, "sc", "delta_vth_v", resolved)],
    )
    assert create_acceptance_policy(connection, spec) == 9
    stored = connection.cursor_instance.executions[0][1][3].adapted
    assert stored["curve_max_mean_mae_a"] == 0.01
    assert stored["curve_min_external_devices"] == 3
    assert stored["projection_max_p90_error_a"] == 0.02
    assert stored["projection_min_development_curves"] == 6


@pytest.mark.parametrize(
    ("requirements", "message"),
    (
        ({"curve_max_mean_mae_a": 0.01}, "full-curve claim policy is incomplete"),
        (
            {"projection_max_mean_mae_a": 0.01},
            "deterministic-projection claim policy is incomplete",
        ),
        (
            {
                **complete_curve_claim_requirements(),
                "curve_min_external_devices": 7,
            },
            "device minima cannot exceed curve minima",
        ),
        (
            {
                **complete_curve_claim_requirements(),
                "projection_max_p90_error_a": -0.1,
            },
            "finite and nonnegative",
        ),
    ),
)
def test_policy_writer_rejects_partial_or_nonphysical_curve_claims(
    requirements,
    message,
):
    with pytest.raises(DamageEvidenceError, match=message):
        validate_policy_spec(
            AcceptancePolicySpec(
                "curve-policy-v1",
                "sc",
                "delta_vth_v",
                {**complete_policy_requirements(), **requirements},
            ),
            for_approval=True,
        )


def test_policy_rejects_non_numeric_counts_and_out_of_range_error_rate():
    with pytest.raises(DamageEvidenceError, match="positive integers"):
        validate_policy_spec(
            AcceptancePolicySpec(
                "policy-v1", "sc", "delta_vth_v",
                {"min_training_groups": None},
            )
        )
    requirements = complete_policy_requirements()
    requirements["max_catastrophic_error_rate"] = 1.1
    with pytest.raises(DamageEvidenceError, match="finite and nonnegative"):
        validate_policy_spec(
            AcceptancePolicySpec(
                "policy-v1", "sc", "delta_vth_v", requirements,
            ),
            for_approval=True,
        )


def test_resolved_policy_freezes_training_defaults_and_validates_them():
    requirements = complete_policy_requirements()
    resolved = resolved_policy_requirements(
        AcceptancePolicySpec(
            "policy-v1", "sc", "delta_vth_v", requirements,
        )
    )
    assert resolved["required_grouped_schemes"] == [
        "leave_device", "leave_condition", "leave_campaign",
    ]
    assert resolved["interval_coverage"] == 0.8
    assert resolved["ood_quantile"] == 0.95
    assert resolved["min_neighbor_devices"] == 2
    assert resolved["min_independent_groups"] == 70

    for name, value in (
        ("required_grouped_schemes", ["unknown"]),
        ("interval_coverage", 0.1),
        ("ood_quantile", 1.0),
        ("min_neighbor_devices", 0),
    ):
        with pytest.raises(DamageEvidenceError):
            validate_policy_spec(
                AcceptancePolicySpec(
                    "policy-v1",
                    "sc",
                    "delta_vth_v",
                    {**requirements, name: value},
                ),
                for_approval=True,
            )
