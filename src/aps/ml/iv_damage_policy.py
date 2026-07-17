"""Release and decision-eligibility contracts for the V3 IV damage predictor.

The policy is deliberately independent of a particular estimator.  Dataset,
validation, training, dashboard, and downstream code all consume the same
contract so that an algorithm cannot silently redefine what "validated" or
"decision eligible" means.

Numerical error limits are intentionally nullable until the lab and downstream
owners approve tolerances derived from measurement repeatability and intended
use.  An unapproved or incomplete policy always fails closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite


class ReleaseStatus(StrEnum):
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    SHADOW = "shadow"
    RELEASED = "released"
    RETIRED = "retired"
    FAILED = "failed"


class EvidenceStatus(StrEnum):
    DECISION_ELIGIBLE = "decision_eligible"
    SCREENING_ONLY = "screening_only"
    OUT_OF_DOMAIN = "out_of_domain"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    INVALID_INPUT = "invalid_input"


class ReferencePolicy(StrEnum):
    SAME_DEVICE = "same_device"
    LIBRARY_SCREENING = "library_screening"


@dataclass(frozen=True)
class TargetContract:
    target_type: str
    response_unit: str
    post_value_unit: str
    response_definition: str


TARGET_CONTRACTS = {
    "delta_vth_v": TargetContract(
        target_type="delta_vth_v",
        response_unit="V",
        post_value_unit="V",
        response_definition="post_vth_v - pre_vth_v",
    ),
    "log_rdson_ratio": TargetContract(
        target_type="log_rdson_ratio",
        response_unit="natural_log_ratio",
        post_value_unit="mohm",
        response_definition="ln(post_rdson_mohm / pre_rdson_mohm)",
    ),
}


@dataclass(frozen=True)
class AcceptancePolicy:
    """Versioned requirements for one stress/target release domain."""

    policy_version: str
    approved: bool = False
    min_training_groups: int = 30
    min_external_groups: int = 30
    min_campaigns: int = 3
    min_subgroup_groups: int = 10
    min_supported_fraction: float = 0.80
    min_baseline_improvement_fraction: float = 0.10
    min_interval_coverage: float = 0.75
    max_interval_coverage: float = 0.90
    max_median_abs_error: float | None = None
    max_p90_abs_error: float | None = None
    max_abs_bias: float | None = None
    max_catastrophic_error_rate: float | None = None
    max_mean_interval_width: float | None = None


@dataclass(frozen=True)
class ValidationEvidence:
    """Estimator-independent evidence supplied to the release gate."""

    training_groups: int
    external_groups: int
    campaigns: int
    smallest_released_subgroup_groups: int
    supported_fraction: float
    median_abs_error: float
    p90_abs_error: float
    abs_bias: float
    candidate_mae: float
    best_baseline_mae: float
    interval_coverage: float
    mean_interval_width: float
    catastrophic_error_rate: float
    external_test_passed: bool
    required_features_complete: bool
    leakage_checks_passed: bool


@dataclass(frozen=True)
class GateResult:
    eligible: bool
    checks: dict[str, bool]
    reasons: tuple[str, ...]


def _finite_between(value: float, lower: float, upper: float) -> bool:
    return isfinite(value) and lower <= value <= upper


def evaluate_release(
    evidence: ValidationEvidence,
    policy: AcceptancePolicy,
) -> GateResult:
    """Evaluate every release gate and fail closed on incomplete policy."""
    limits_complete = all(
        value is not None
        for value in (
            policy.max_median_abs_error,
            policy.max_p90_abs_error,
            policy.max_abs_bias,
            policy.max_catastrophic_error_rate,
            policy.max_mean_interval_width,
        )
    )
    baseline_improvement = (
        (evidence.best_baseline_mae - evidence.candidate_mae)
        / evidence.best_baseline_mae
        if isfinite(evidence.best_baseline_mae)
        and evidence.best_baseline_mae > 0.0
        and isfinite(evidence.candidate_mae)
        else float("-inf")
    )
    checks = {
        "policy_approved": policy.approved,
        "policy_limits_complete": limits_complete,
        "training_groups": evidence.training_groups >= policy.min_training_groups,
        "external_groups": evidence.external_groups >= policy.min_external_groups,
        "campaigns": evidence.campaigns >= policy.min_campaigns,
        "subgroup_groups": (
            evidence.smallest_released_subgroup_groups >= policy.min_subgroup_groups
        ),
        "supported_fraction": _finite_between(
            evidence.supported_fraction, policy.min_supported_fraction, 1.0
        ),
        "baseline_improvement": (
            baseline_improvement >= policy.min_baseline_improvement_fraction
        ),
        "interval_coverage": _finite_between(
            evidence.interval_coverage,
            policy.min_interval_coverage,
            policy.max_interval_coverage,
        ),
        "external_test": evidence.external_test_passed,
        "required_features": evidence.required_features_complete,
        "leakage_checks": evidence.leakage_checks_passed,
        "median_abs_error": (
            limits_complete
            and isfinite(evidence.median_abs_error)
            and evidence.median_abs_error <= policy.max_median_abs_error
        ),
        "p90_abs_error": (
            limits_complete
            and isfinite(evidence.p90_abs_error)
            and evidence.p90_abs_error <= policy.max_p90_abs_error
        ),
        "absolute_bias": (
            limits_complete
            and isfinite(evidence.abs_bias)
            and evidence.abs_bias <= policy.max_abs_bias
        ),
        "catastrophic_error_rate": (
            limits_complete
            and isfinite(evidence.catastrophic_error_rate)
            and evidence.catastrophic_error_rate
            <= policy.max_catastrophic_error_rate
        ),
        "interval_width": (
            limits_complete
            and isfinite(evidence.mean_interval_width)
            and evidence.mean_interval_width <= policy.max_mean_interval_width
        ),
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    return GateResult(eligible=not reasons, checks=checks, reasons=reasons)


def prediction_decision_eligible(
    *,
    release_status: str,
    evidence_status: str,
    reference_policy: str,
    in_domain: bool,
    validation_gate_passed: bool,
) -> bool:
    """Single fail-closed boundary used by persistence and downstream views."""
    return all(
        (
            release_status == ReleaseStatus.RELEASED,
            evidence_status == EvidenceStatus.DECISION_ELIGIBLE,
            reference_policy == ReferencePolicy.SAME_DEVICE,
            in_domain,
            validation_gate_passed,
        )
    )
