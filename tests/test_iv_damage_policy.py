import pytest

from aps.ml.iv_damage_policy import (
    AcceptancePolicy,
    EvidenceStatus,
    ReferencePolicy,
    ReleaseStatus,
    ValidationEvidence,
    evaluate_release,
    prediction_decision_eligible,
)


def evidence(**overrides):
    values = {
        "training_groups": 40,
        "external_groups": 35,
        "campaigns": 4,
        "smallest_released_subgroup_groups": 12,
        "supported_fraction": 0.85,
        "median_abs_error": 0.10,
        "p90_abs_error": 0.30,
        "abs_bias": 0.03,
        "candidate_mae": 0.15,
        "best_baseline_mae": 0.25,
        "interval_coverage": 0.82,
        "mean_interval_width": 0.50,
        "catastrophic_error_rate": 0.01,
        "external_test_passed": True,
        "required_features_complete": True,
        "leakage_checks_passed": True,
    }
    values.update(overrides)
    return ValidationEvidence(**values)


def approved_policy(**overrides):
    values = {
        "policy_version": "test-v1",
        "approved": True,
        "max_median_abs_error": 0.20,
        "max_p90_abs_error": 0.50,
        "max_abs_bias": 0.10,
        "max_catastrophic_error_rate": 0.05,
        "max_mean_interval_width": 0.75,
    }
    values.update(overrides)
    return AcceptancePolicy(**values)


def test_provisional_policy_cannot_release_even_with_good_metrics():
    result = evaluate_release(evidence(), AcceptancePolicy("provisional-v1"))

    assert not result.eligible
    assert "policy_approved" in result.reasons
    assert "policy_limits_complete" in result.reasons


def test_approved_complete_policy_passes_only_complete_evidence():
    assert evaluate_release(evidence(), approved_policy()).eligible

    result = evaluate_release(
        evidence(external_groups=2, interval_coverage=0.40),
        approved_policy(),
    )
    assert not result.eligible
    assert set(result.reasons) >= {"external_groups", "interval_coverage"}


def test_candidate_must_beat_nonzero_best_baseline():
    result = evaluate_release(
        evidence(candidate_mae=0.24, best_baseline_mae=0.25),
        approved_policy(),
    )
    assert not result.eligible
    assert "baseline_improvement" in result.reasons


@pytest.mark.parametrize(
    "override",
    [
        {"release_status": ReleaseStatus.SHADOW},
        {"evidence_status": EvidenceStatus.SCREENING_ONLY},
        {"reference_policy": ReferencePolicy.LIBRARY_SCREENING},
        {"in_domain": False},
        {"validation_gate_passed": False},
    ],
)
def test_decision_boundary_fails_closed(override):
    values = {
        "release_status": ReleaseStatus.RELEASED,
        "evidence_status": EvidenceStatus.DECISION_ELIGIBLE,
        "reference_policy": ReferencePolicy.SAME_DEVICE,
        "in_domain": True,
        "validation_gate_passed": True,
    }
    values.update(override)
    assert not prediction_decision_eligible(**values)


def test_only_released_same_device_in_domain_prediction_is_eligible():
    assert prediction_decision_eligible(
        release_status=ReleaseStatus.RELEASED,
        evidence_status=EvidenceStatus.DECISION_ELIGIBLE,
        reference_policy=ReferencePolicy.SAME_DEVICE,
        in_domain=True,
        validation_gate_passed=True,
    )
