import pytest

from aps.ml.iv_damage_model import (
    CalibratedDamageModel,
    DamageExample,
    DamageRequest,
    validate_request_features,
)
from aps.ml.iv_damage_policy import EvidenceStatus, ReferencePolicy


def features(x):
    return {
        "pre_value": 3.0 + 0.05 * x,
        "beam_energy_mev": 100.0 + x,
        "let_surface": 20.0 + 0.5 * x,
        "range_um": 10.0 + 0.2 * x,
        "fluence_or_dose": 1e7 * (1.0 + 0.05 * x),
        "irradiation_bias_v": 100.0 + 2.0 * x,
        "temperature_c": 25.0 + 0.1 * x,
        "post_measurement_delay_s": 60.0 + x,
    }


def response(x):
    return 0.02 + 0.003 * x


def example(prefix, x):
    return DamageExample(
        response_unit_key=f"{prefix}-{x}",
        physical_device_key=f"device-{prefix}-{x}",
        stress_session_key=f"session-{prefix}-{x}",
        stress_type="irradiation",
        target_type="delta_vth_v",
        device_type="C2M1000170D",
        ion_species="Xe",
        manufacturer="Wolfspeed",
        observed_response=response(x),
        features=features(x),
        protocol_signature="protocol-v1",
        prediction_horizon_s=features(x)["post_measurement_delay_s"],
    )


def request(x, **changes):
    values = {
        "stress_type": "irradiation",
        "target_type": "delta_vth_v",
        "device_type": "C2M1000170D",
        "ion_species": "Xe",
        "manufacturer": "Wolfspeed",
        "features": features(x),
        "protocol_signature": "protocol-v1",
        "prediction_horizon_s": features(x)["post_measurement_delay_s"],
    }
    values.update(changes)
    return DamageRequest(**values)


def fitted_model(kind="huber"):
    model = CalibratedDamageModel(
        stress_type="irradiation",
        target_type="delta_vth_v",
        estimator_kind=kind,
        min_neighbor_devices=1,
        min_calibration_groups=3,
        ood_quantile=0.95,
        random_state=7,
    )
    model.fit([example("train", x) for x in range(20)])
    model.calibrate([example("cal", x + 0.25) for x in range(4, 10)])
    return model


def test_feature_contract_rejects_missing_nonfinite_and_physical_impossibility():
    broken = features(1)
    broken.pop("let_surface")
    broken["temperature_c"] = float("nan")
    broken["beam_energy_mev"] = -1
    reasons = validate_request_features(stress_type="irradiation", features=broken)
    assert "missing_or_nonfinite:let_surface" in reasons
    assert "missing_or_nonfinite:temperature_c" in reasons
    assert "outside_physical_bounds:beam_energy_mev" in reasons


@pytest.mark.parametrize("kind", ["huber", "extra_trees"])
def test_candidate_estimators_fit_calibrate_and_emit_bounded_intervals(kind):
    prediction = fitted_model(kind).predict(request(8.5))
    assert prediction.in_domain
    assert prediction.evidence_status == EvidenceStatus.DECISION_ELIGIBLE
    assert prediction.interval_lower <= prediction.predicted_response <= prediction.interval_upper
    assert prediction.interval_lower <= response(8.5) <= prediction.interval_upper


def test_calibration_is_independent_and_has_minimum_supported_groups():
    model = CalibratedDamageModel(
        stress_type="irradiation",
        target_type="delta_vth_v",
        min_neighbor_devices=1,
        min_calibration_groups=3,
    ).fit([example("train", x) for x in range(8)])
    overlapping = [example("cal", x + 0.2) for x in range(3)]
    overlapping[0] = DamageExample(
        **{
            **overlapping[0].__dict__,
            "physical_device_key": "device-train-0",
        }
    )
    with pytest.raises(ValueError, match="independent"):
        model.calibrate(overlapping)
    with pytest.raises(ValueError, match="too few"):
        model.calibrate([example("cal", 4.2), example("cal", 5.2)])

    repeated_device = [
        DamageExample(
            **{**example("repeat", x).__dict__, "physical_device_key": "one-device"}
        )
        for x in (4.2, 5.2, 6.2)
    ]
    with pytest.raises(ValueError, match="calibration devices"):
        model.calibrate(repeated_device)


def test_unseen_category_and_far_numeric_point_abstain():
    model = fitted_model()
    category = model.predict(request(8, ion_species="Kr"))
    assert not category.in_domain
    assert category.predicted_response is None
    assert category.evidence_status == EvidenceStatus.OUT_OF_DOMAIN
    assert "unseen_category:ion_species" in category.reasons

    far = model.predict(request(1000))
    assert not far.in_domain
    assert far.predicted_response is None
    assert "insufficient_local_device_support" in far.reasons


def test_invalid_request_abstains_with_invalid_input_status():
    model = fitted_model()
    broken = features(8)
    broken.pop("range_um")
    prediction = model.predict(request(8, features=broken))
    assert prediction.evidence_status == EvidenceStatus.INVALID_INPUT
    assert prediction.predicted_response is None


def test_protocol_and_prediction_horizon_are_part_of_domain_assessment():
    model = fitted_model()
    protocol = model.predict(request(8, protocol_signature="protocol-v2"))
    assert protocol.evidence_status == EvidenceStatus.OUT_OF_DOMAIN
    assert "unseen_protocol_signature" in protocol.reasons

    horizon = model.predict(request(8, prediction_horizon_s=999))
    assert horizon.evidence_status == EvidenceStatus.INVALID_INPUT
    assert "prediction_horizon_conflict" in horizon.reasons


def test_single_short_circuit_pulse_is_physically_valid():
    sc_features = {
        "pre_value": 1.0, "sc_voltage_v": 100.0, "sc_duration_us": 1.0,
        "peak_current_a": 10.0, "deposited_energy_j": 0.1,
        "pulse_count": 1, "gate_drive_v": 15.0, "temperature_c": 25.0,
    }
    assert validate_request_features(stress_type="sc", features=sc_features) == ()


def test_library_reference_can_be_scored_but_never_decision_eligible():
    prediction = fitted_model().predict(
        request(8, reference_policy=ReferencePolicy.LIBRARY_SCREENING)
    )
    assert prediction.in_domain
    assert prediction.evidence_status == EvidenceStatus.SCREENING_ONLY
    assert prediction.reasons == ("library_reference",)


def test_model_rejects_duplicate_units_and_too_few_physical_devices():
    one = example("train", 0)
    model = CalibratedDamageModel(
        stress_type="irradiation",
        target_type="delta_vth_v",
        min_neighbor_devices=1,
    )
    with pytest.raises(ValueError, match="unique"):
        model.fit([one, one])
    same_device = [
        DamageExample(**{**example("x", i).__dict__, "physical_device_key": "one"})
        for i in range(3)
    ]
    with pytest.raises(ValueError, match="too few independent"):
        model.fit(same_device)


def test_model_requires_a_positive_unique_device_calibration_minimum():
    with pytest.raises(ValueError, match="min_calibration_groups"):
        CalibratedDamageModel(
            stress_type="irradiation",
            target_type="delta_vth_v",
            min_calibration_groups=0,
        )


def test_artifact_manifest_is_complete_and_deterministic():
    model = fitted_model()
    first = model.artifact_manifest()
    second = model.artifact_manifest()
    assert first == second
    assert len(first["manifest_sha256"]) == 64
    assert first["training_groups"] == 20
    assert first["calibration_groups"] == 6
