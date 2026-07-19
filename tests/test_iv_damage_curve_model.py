import numpy as np
import pytest

from aps.ml.iv_damage_curve_model import (
    CurveExample,
    CurveModelError,
    CurveRequest,
    FunctionalCurveDamageModel,
    deterministic_curve_projection,
)


def features(value):
    return {
        "pre_value": 3.0 + value * 0.01,
        "beam_energy_mev": 100.0 + value,
        "let_surface": 20.0 + value * 0.1,
        "range_um": 10.0 + value * 0.05,
        "fluence_or_dose": 1e7 * (1.0 + value * 0.01),
        "irradiation_bias_v": 100.0 + value,
        "temperature_c": 25.0 + value * 0.02,
        "post_measurement_delay_s": 60.0 + value,
    }


def example(prefix, value):
    x = np.linspace(0.0, 10.0, 41)
    pre = (1.0 + 0.01 * value) * np.log1p(np.exp((x - 3.0) * 1.4)) * 1e-3
    # Shape change includes threshold movement, transconductance loss, and a
    # voltage-dependent curvature term. It cannot be represented by one rigid
    # horizontal shift or one global current multiplier.
    post = (
        (0.92 - 0.002 * value)
        * np.log1p(np.exp((x - (3.15 + 0.004 * value)) * (1.25 - 0.002 * value)))
        * 1e-3
        + (x / 10.0) ** 2 * (2e-5 + value * 2e-7)
    )
    return CurveExample(
        pair_key=f"pair-{prefix}-{value}",
        physical_device_key=f"device-{prefix}-{value}",
        stress_session_key=f"session-{prefix}-{value}",
        stress_type="irradiation",
        curve_family="IdVg",
        measurement_protocol_id="idvg-v1",
        device_type="C2M1000170D",
        manufacturer="Wolfspeed",
        ion_species="Xe",
        features=features(value),
        prediction_horizon_s=features(value)["post_measurement_delay_s"],
        pre_x_v=x,
        pre_i_a=pre,
        post_x_v=x,
        post_i_a=post,
    )


def request(value, **changes):
    row = example("request", value)
    values = {
        "stress_type": row.stress_type,
        "curve_family": row.curve_family,
        "measurement_protocol_id": row.measurement_protocol_id,
        "device_type": row.device_type,
        "manufacturer": row.manufacturer,
        "ion_species": row.ion_species,
        "features": row.features,
        "prediction_horizon_s": row.prediction_horizon_s,
        "pre_x_v": row.pre_x_v,
        "pre_i_a": row.pre_i_a,
    }
    values.update(changes)
    return CurveRequest(**values)


def fitted_model():
    model = FunctionalCurveDamageModel(
        stress_type="irradiation",
        curve_family="IdVg",
        measurement_protocol_id="idvg-v1",
        grid_points=32,
        pca_components=5,
        min_neighbor_devices=1,
        min_calibration_devices=3,
    )
    model.fit([example("train", value) for value in range(18)])
    model.calibrate([example("cal", value + 0.25) for value in range(4, 9)])
    return model


def test_functional_model_predicts_a_true_shape_changing_curve_and_band():
    model = fitted_model()
    prediction = model.predict(request(8.5))
    assert prediction.in_domain
    assert prediction.evidence_status == "decision_eligible"
    assert len(prediction.x_v) == 32
    assert np.all(np.asarray(prediction.lower_i_a) <= prediction.predicted_i_a)
    assert np.all(np.asarray(prediction.predicted_i_a) <= prediction.upper_i_a)
    truth = example("truth", 8.5)
    metrics = model.error_metrics(prediction, truth.post_x_v, truth.post_i_a)
    assert metrics.mae_a < 5e-4
    assert metrics.normalized_rmse < 0.15
    assert model.artifact_manifest()["claim_class"] == "learned_full_curve"
    assert "simultaneous" in model.artifact_manifest()["interval_type"]


def test_functional_model_abstains_for_protocol_and_grid_mismatch():
    model = fitted_model()
    protocol = model.predict(request(8, measurement_protocol_id="idvg-v2"))
    assert not protocol.in_domain
    assert "unseen_protocol_signature" in protocol.reasons

    x = np.linspace(2.0, 8.0, 20)
    narrow = model.predict(request(8, pre_x_v=x, pre_i_a=np.ones(20) * 1e-3))
    assert not narrow.in_domain
    assert any("certified_voltage_grid" in reason for reason in narrow.reasons)

    incomplete = features(8)
    incomplete.pop("beam_energy_mev")
    missing = model.predict(request(8, features=incomplete))
    assert not missing.in_domain
    assert missing.evidence_status == "invalid_input"
    assert "missing_or_nonfinite:beam_energy_mev" in missing.reasons


def test_functional_calibration_requires_independent_physical_devices():
    model = FunctionalCurveDamageModel(
        stress_type="irradiation", curve_family="IdVg",
        measurement_protocol_id="idvg-v1", min_neighbor_devices=1,
        min_calibration_devices=2,
    ).fit([example("train", value) for value in range(6)])
    overlap = example("cal", 2.2)
    overlap = CurveExample(**{**overlap.__dict__, "physical_device_key": "device-train-0"})
    with pytest.raises(CurveModelError, match="overlap"):
        model.calibrate([overlap, example("cal", 3.2)])


def test_rigid_shift_projection_drops_extrapolated_points_and_labels_constraint():
    x = np.linspace(0.0, 10.0, 11)
    current = x * 1e-3
    result = deterministic_curve_projection(
        projection_kind="rigid_vth_shift", x_v=x, pre_i_a=current,
        response=1.0, response_lower=0.5, response_upper=1.5,
    )
    assert result.in_domain
    assert len(result.x_v) < len(x)
    assert result.evidence_status == "screening_only"
    assert result.reasons == ("shape_constrained_scalar_projection",)


def test_rdson_projection_uses_inverse_resistance_ratio_and_orders_band():
    x = np.linspace(0.0, 2.0, 6)
    current = x * 0.5
    result = deterministic_curve_projection(
        projection_kind="linear_rdson_scale", x_v=x, pre_i_a=current,
        response=np.log(2.0), response_lower=np.log(1.5), response_upper=np.log(2.5),
    )
    assert np.allclose(result.predicted_i_a, current / 2.0)
    assert np.all(np.asarray(result.lower_i_a) <= result.predicted_i_a)
    assert np.all(np.asarray(result.predicted_i_a) <= result.upper_i_a)
