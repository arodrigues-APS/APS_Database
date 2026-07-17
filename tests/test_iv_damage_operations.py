
import pytest

from aps.ml.iv_damage_model import CalibratedDamageModel, DamageExample
from aps.ml.iv_damage_operations import (
    DamageOperationError,
    MonitoringPolicy,
    PendingPredictionRequest,
    assess_monitoring,
    load_model_artifact,
    save_model_artifact,
    score_request,
)
from aps.ml.iv_damage_policy import EvidenceStatus, ReferencePolicy


def features(x):
    return {
        "pre_value": 3.0,
        "beam_energy_mev": 100.0 + x,
        "let_surface": 20.0 + x,
        "range_um": 10.0 + x,
        "fluence_or_dose": 1e7 * (1 + x / 100),
        "irradiation_bias_v": 100.0 + x,
        "temperature_c": 25.0,
        "post_measurement_delay_s": 60.0 + x,
        "ion_species": "Xe",
    }


def example(prefix, x):
    return DamageExample(
        f"{prefix}-{x}", f"device-{prefix}-{x}", f"session-{prefix}-{x}",
        "irradiation", "delta_vth_v", "C2M", 0.01 * x,
        features(x), ion_species="Xe", manufacturer="Wolfspeed",
    )


def model():
    candidate = CalibratedDamageModel(
        stress_type="irradiation", target_type="delta_vth_v",
        min_neighbor_devices=1, min_calibration_groups=3,
    )
    candidate.fit([example("train", x) for x in range(20)])
    candidate.calibrate([example("cal", x + 0.1) for x in range(5, 10)])
    return candidate


def request(x=8, reference_policy=ReferencePolicy.SAME_DEVICE):
    raw = features(x)
    raw.pop("pre_value")
    return PendingPredictionRequest(
        1, "request-1", "new-device", "C2M", "Wolfspeed", "protocol-v1",
        "irradiation", "delta_vth_v", 3.0, 0.01, reference_policy, raw,
    )


def test_artifact_is_immutable_checksummed_and_tamper_evident(tmp_path):
    path = tmp_path / "model.joblib"
    artifact = save_model_artifact(model(), path, metadata={"code_sha": "abc"})
    loaded = load_model_artifact(path, artifact.checksum)
    assert loaded.is_calibrated
    with pytest.raises(DamageOperationError, match="immutable"):
        save_model_artifact(model(), path, metadata={})
    with path.open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(DamageOperationError, match="checksum mismatch"):
        load_model_artifact(path, artifact.checksum)


def test_scoring_converts_response_to_post_value_and_enforces_reference_boundary():
    candidate = model()
    scored = score_request(candidate, request())
    assert scored.in_domain
    assert scored.evidence_status == EvidenceStatus.DECISION_ELIGIBLE
    assert scored.decision_eligible
    assert scored.predicted_post_value == pytest.approx(3.0 + scored.predicted_response)
    assert scored.feature_completeness == {"complete": True, "missing": []}

    library = score_request(candidate, request(reference_policy=ReferencePolicy.LIBRARY_SCREENING))
    assert library.evidence_status == EvidenceStatus.SCREENING_ONLY
    assert not library.decision_eligible


def test_scoring_abstains_for_out_of_domain_request():
    scored = score_request(model(), request(1000))
    assert not scored.in_domain
    assert scored.predicted_response is None
    assert not scored.decision_eligible
    assert scored.support_status == "out_of_domain"


def test_monitoring_is_insufficient_then_healthy_or_alert_against_explicit_limits():
    policy = MonitoringPolicy(
        min_matched_outcomes=10, max_mae=0.2, max_abs_bias=0.1,
        min_interval_coverage=0.75, max_abstention_fraction=0.25,
    )
    insufficient = assess_monitoring(
        {"predictions": 100, "matched_outcomes": 9}, policy
    )
    assert insufficient.status == "insufficient_outcomes"

    healthy = assess_monitoring(
        {"predictions": 100, "matched_outcomes": 50, "abstentions": 10,
         "mae": 0.1, "bias": -0.02, "interval_coverage": 0.8},
        policy,
    )
    assert healthy.status == "healthy"

    alert = assess_monitoring(
        {"predictions": 100, "matched_outcomes": 50, "abstentions": 30,
         "mae": 0.3, "bias": 0.2, "interval_coverage": 0.6},
        policy,
    )
    assert alert.status == "alert"
    assert set(alert.reasons) == {"mae", "absolute_bias", "interval_coverage", "abstention_fraction"}
