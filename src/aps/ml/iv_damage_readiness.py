"""Independent-evidence and required-feature readiness for V3 damage domains."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from math import isfinite
from numbers import Real
from typing import Mapping


DOMAIN_REQUIRED_FEATURES = {
    "irradiation": frozenset(
        {
            "pre_value",
            "beam_energy_mev",
            "let_surface",
            "range_um",
            "fluence_or_dose",
            "irradiation_bias_v",
            "temperature_c",
            "post_measurement_delay_s",
        }
    ),
    "sc": frozenset(
        {
            "pre_value",
            "sc_voltage_v",
            "sc_duration_us",
            "peak_current_a",
            "deposited_energy_j",
            "pulse_count",
            "gate_drive_v",
            "temperature_c",
        }
    ),
}

# Lower bounds are exclusive unless the feature is listed as inclusive. Keep
# this contract here so readiness and runtime scoring cannot disagree about
# whether an evidence row/request is physically valid.
FEATURE_BOUNDS: Mapping[str, tuple[float | None, float | None]] = {
    "pre_value": (0.0, None),
    "beam_energy_mev": (0.0, None),
    "let_surface": (0.0, None),
    "range_um": (0.0, None),
    "fluence_or_dose": (0.0, None),
    "irradiation_bias_v": (None, None),
    "post_measurement_delay_s": (0.0, None),
    "sc_voltage_v": (0.0, None),
    "sc_duration_us": (0.0, None),
    "peak_current_a": (0.0, None),
    "deposited_energy_j": (0.0, None),
    "pulse_count": (1.0, None),
    "gate_drive_v": (None, None),
    "temperature_c": (-273.15, 500.0),
}
INCLUSIVE_LOWER_BOUNDS = frozenset({"pulse_count"})


@dataclass(frozen=True)
class EvidenceUnit:
    """One independent physical-device/stress-session/target outcome."""

    unit_key: str
    physical_device_key: str
    stress_session_key: str
    stress_type: str
    target_type: str
    device_type: str
    campaign_key: str
    run_key: str
    measurement_protocol_id: str
    response_value: float | None
    response_uncertainty: float | None
    replicate_count: int
    split_role: str
    ion_species: str | None = None
    features: Mapping[str, object] = field(default_factory=dict)
    quality_status: str = "usable"

    @property
    def independent_group_key(self) -> str:
        return "|".join(
            (
                self.physical_device_key,
                self.stress_session_key,
                self.target_type,
            )
        )


@dataclass(frozen=True)
class ReadinessRequirements:
    min_independent_groups: int = 30
    min_physical_devices: int = 10
    min_campaigns: int = 3
    min_external_groups: int = 10
    min_calibration_groups: int = 10
    min_replicates: int = 2
    max_campaign_share: float = 0.50
    required_feature_fraction: float = 1.0


@dataclass(frozen=True)
class ReadinessReport:
    stress_type: str
    target_type: str
    status: str
    checks: Mapping[str, bool]
    blockers: tuple[str, ...]
    independent_groups: int
    physical_devices: int
    campaigns: int
    external_groups: int
    calibration_groups: int
    complete_feature_groups: int
    required_feature_fraction: float
    largest_campaign_share: float
    missing_feature_counts: Mapping[str, int]


def _finite_number(value: object) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool) and isfinite(float(value))


def validate_required_features(
    *, stress_type: str, features: Mapping[str, object]
) -> tuple[str, ...]:
    """Return canonical missing/non-finite/physical-bound feature failures."""

    try:
        required = DOMAIN_REQUIRED_FEATURES[stress_type]
    except KeyError as exc:
        raise ValueError(f"unsupported stress_type: {stress_type}") from exc
    reasons: list[str] = []
    for name in sorted(required):
        value = features.get(name)
        if not _finite_number(value):
            reasons.append(f"missing_or_nonfinite:{name}")
            continue
        number = float(value)
        lower, upper = FEATURE_BOUNDS[name]
        if lower is not None:
            outside_lower = (
                number < lower if name in INCLUSIVE_LOWER_BOUNDS else number <= lower
            )
            if outside_lower:
                reasons.append(f"outside_physical_bounds:{name}")
        if upper is not None and number > upper:
            reasons.append(f"outside_physical_bounds:{name}")
        if name == "pulse_count" and not number.is_integer():
            reasons.append("outside_physical_bounds:pulse_count")
    return tuple(sorted(set(reasons)))


def missing_required_features(unit: EvidenceUnit) -> tuple[str, ...]:
    return validate_required_features(
        stress_type=unit.stress_type, features=unit.features
    )


def assess_readiness(
    units: list[EvidenceUnit],
    *,
    stress_type: str,
    target_type: str,
    requirements: ReadinessRequirements | None = None,
) -> ReadinessReport:
    """Assess one advertised domain using independent groups, not file rows."""
    req = requirements or ReadinessRequirements()
    candidates = [
        unit for unit in units
        if unit.stress_type == stress_type and unit.target_type == target_type
        and unit.quality_status == "usable" and unit.response_value is not None
    ]
    # Fail closed if a caller accidentally supplies duplicate aggregate rows.
    by_group: dict[str, EvidenceUnit] = {}
    conflicting_groups = set()
    for unit in candidates:
        key = unit.independent_group_key
        if key in by_group and by_group[key].unit_key != unit.unit_key:
            conflicting_groups.add(key)
            continue
        by_group.setdefault(key, unit)
    independent = list(by_group.values())

    missing_counts: Counter[str] = Counter()
    complete_groups = 0
    for unit in independent:
        missing = missing_required_features(unit)
        if missing:
            missing_counts.update(reason.split(":", 1)[-1] for reason in missing)
        else:
            complete_groups += 1

    campaign_counts = Counter(unit.campaign_key for unit in independent)
    total = len(independent)
    feature_fraction = complete_groups / total if total else 0.0
    largest_share = max(campaign_counts.values(), default=0) / total if total else 0.0
    external = sum(unit.split_role == "external_test" for unit in independent)
    calibration = len({
        unit.physical_device_key
        for unit in independent
        if unit.split_role == "calibration"
    })
    response_values = {float(unit.response_value) for unit in independent}
    checks = {
        "no_duplicate_independent_groups": not conflicting_groups,
        "independent_groups": total >= req.min_independent_groups,
        "physical_devices": (
            len({unit.physical_device_key for unit in independent})
            >= req.min_physical_devices
        ),
        "campaigns": len(campaign_counts) >= req.min_campaigns,
        "external_groups": external >= req.min_external_groups,
        "calibration_groups": calibration >= req.min_calibration_groups,
        "replicate_support": all(
            unit.replicate_count >= req.min_replicates for unit in independent
        ) if independent else False,
        "required_features": feature_fraction >= req.required_feature_fraction,
        "campaign_balance": largest_share <= req.max_campaign_share,
        "response_range": len(response_values) >= 3,
        "protocol_identity": all(
            bool(unit.measurement_protocol_id.strip()) for unit in independent
        ) if independent else False,
    }
    blockers = tuple(name for name, passed in checks.items() if not passed)
    return ReadinessReport(
        stress_type=stress_type,
        target_type=target_type,
        status="model_ready" if not blockers else "data_blocked",
        checks=checks,
        blockers=blockers,
        independent_groups=total,
        physical_devices=len({unit.physical_device_key for unit in independent}),
        campaigns=len(campaign_counts),
        external_groups=external,
        calibration_groups=calibration,
        complete_feature_groups=complete_groups,
        required_feature_fraction=feature_fraction,
        largest_campaign_share=largest_share,
        missing_feature_counts=dict(sorted(missing_counts.items())),
    )
