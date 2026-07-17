"""Independent-evidence and required-feature readiness for V3 damage domains."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from math import isfinite
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


def _present(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (int, float)):
        return isfinite(float(value))
    return True


def missing_required_features(unit: EvidenceUnit) -> tuple[str, ...]:
    try:
        required = DOMAIN_REQUIRED_FEATURES[unit.stress_type]
    except KeyError as exc:
        raise ValueError(f"unsupported stress_type: {unit.stress_type}") from exc
    return tuple(sorted(name for name in required if not _present(unit.features.get(name))))


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
            missing_counts.update(missing)
        else:
            complete_groups += 1

    campaign_counts = Counter(unit.campaign_key for unit in independent)
    total = len(independent)
    feature_fraction = complete_groups / total if total else 0.0
    largest_share = max(campaign_counts.values(), default=0) / total if total else 0.0
    external = sum(unit.split_role == "external_test" for unit in independent)
    calibration = sum(unit.split_role == "calibration" for unit in independent)
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
