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
from numbers import Real
from typing import Mapping


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


class ClaimPolicyError(ValueError):
    """A curve or projection claim policy is absent, partial, or nonphysical."""


CURVE_CLAIM_POLICY_FIELDS = frozenset({
    "curve_grid_points",
    "curve_pca_components",
    "curve_ridge_alpha",
    "curve_interval_coverage",
    "curve_min_development_curves",
    "curve_min_development_devices",
    "curve_min_external_curves",
    "curve_min_external_devices",
    "curve_max_mean_mae_a",
    "curve_max_p90_error_a",
    "curve_max_normalized_rmse",
    "curve_min_band_coverage",
})

PROJECTION_CLAIM_POLICY_FIELDS = frozenset({
    "projection_min_development_curves",
    "projection_min_development_devices",
    "projection_min_external_curves",
    "projection_min_external_devices",
    "projection_max_mean_mae_a",
    "projection_max_p90_error_a",
    "projection_max_normalized_rmse",
    "projection_min_band_coverage",
})


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, Real)
        and not isinstance(value, bool)
        and isfinite(float(value))
    )


def _positive_integer(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 1
    )


def _require_complete_claim(
    requirements: Mapping[str, object],
    fields: frozenset[str],
    claim: str,
    *,
    required: bool,
) -> bool:
    present = fields.intersection(requirements)
    if not present and not required:
        return False
    missing = fields - present
    if missing:
        raise ClaimPolicyError(
            f"{claim} claim policy is incomplete; missing: "
            + ", ".join(sorted(missing))
        )
    return True


def validate_curve_claim_requirements(
    requirements: Mapping[str, object],
    *,
    required: bool = False,
) -> None:
    """Validate the complete policy block for learned functional curves."""
    if not _require_complete_claim(
        requirements, CURVE_CLAIM_POLICY_FIELDS, "full-curve", required=required,
    ):
        return
    integer_fields = (
        "curve_grid_points",
        "curve_pca_components",
        "curve_min_development_curves",
        "curve_min_development_devices",
        "curve_min_external_curves",
        "curve_min_external_devices",
    )
    if any(not _positive_integer(requirements[name]) for name in integer_fields):
        raise ClaimPolicyError(
            "full-curve grid, component, curve, and device counts must be positive integers"
        )
    grid_points = int(requirements["curve_grid_points"])
    components = int(requirements["curve_pca_components"])
    if grid_points < 8 or components > grid_points:
        raise ClaimPolicyError(
            "curve_grid_points must be at least 8 and curve_pca_components cannot exceed it"
        )
    if (
        int(requirements["curve_min_development_devices"])
        > int(requirements["curve_min_development_curves"])
        or int(requirements["curve_min_external_devices"])
        > int(requirements["curve_min_external_curves"])
    ):
        raise ClaimPolicyError("full-curve device minima cannot exceed curve minima")
    ridge_alpha = requirements["curve_ridge_alpha"]
    if not _finite_number(ridge_alpha) or float(ridge_alpha) < 0.0:
        raise ClaimPolicyError("curve_ridge_alpha must be finite and nonnegative")
    interval = requirements["curve_interval_coverage"]
    if not _finite_number(interval) or not 0.5 < float(interval) < 1.0:
        raise ClaimPolicyError("curve_interval_coverage must be between 0.5 and 1")
    for name in (
        "curve_max_mean_mae_a",
        "curve_max_p90_error_a",
        "curve_max_normalized_rmse",
    ):
        value = requirements[name]
        if not _finite_number(value) or float(value) < 0.0:
            raise ClaimPolicyError(f"{name} must be finite and nonnegative")
    band_coverage = requirements["curve_min_band_coverage"]
    if (
        not _finite_number(band_coverage)
        or not 0.0 <= float(band_coverage) <= float(interval)
    ):
        raise ClaimPolicyError(
            "curve_min_band_coverage must be between zero and curve_interval_coverage"
        )


def validate_projection_claim_requirements(
    requirements: Mapping[str, object],
    *,
    required: bool = False,
) -> None:
    """Validate the complete policy block for deterministic curve projection."""
    if not _require_complete_claim(
        requirements,
        PROJECTION_CLAIM_POLICY_FIELDS,
        "deterministic-projection",
        required=required,
    ):
        return
    integer_fields = (
        "projection_min_development_curves",
        "projection_min_development_devices",
        "projection_min_external_curves",
        "projection_min_external_devices",
    )
    if any(not _positive_integer(requirements[name]) for name in integer_fields):
        raise ClaimPolicyError(
            "projection curve and device counts must be positive integers"
        )
    if (
        int(requirements["projection_min_development_devices"])
        > int(requirements["projection_min_development_curves"])
        or int(requirements["projection_min_external_devices"])
        > int(requirements["projection_min_external_curves"])
    ):
        raise ClaimPolicyError("projection device minima cannot exceed curve minima")
    for name in (
        "projection_max_mean_mae_a",
        "projection_max_p90_error_a",
        "projection_max_normalized_rmse",
    ):
        value = requirements[name]
        if not _finite_number(value) or float(value) < 0.0:
            raise ClaimPolicyError(f"{name} must be finite and nonnegative")
    band_coverage = requirements["projection_min_band_coverage"]
    if (
        not _finite_number(band_coverage)
        or not 0.0 <= float(band_coverage) <= 1.0
    ):
        raise ClaimPolicyError(
            "projection_min_band_coverage must be between zero and one"
        )


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
