"""Contracts and invariants for retrospective IV-damage research.

The research lane deliberately has a lower claim than the certified V3 lane.
These values are constants rather than caller-controlled labels so research
materialization cannot accidentally become decision-capable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import Mapping, Sequence


CLAIM_CLASS = "retrospective_research"
PREDICTION_CONTEXT = "historical_out_of_fold"
EVIDENCE_STATUS = "exploratory"
HORIZON_STATUS = "unknown_or_heterogeneous"
VALIDATION_SCHEMES = ("leave_device", "leave_run", "leave_campaign")
SCALAR_METHODS = ("zero_damage", "v2_donor", "huber", "extra_trees")
RESEARCH_PROTOCOL_ID = "historical-idvg-vds1v-unknown-horizon-research-v1"

FORBIDDEN_FEATURE_EXACT = frozenset(
    {
        "delta_vth_v",
        "post_metadata_id",
        "post_feature_id",
        "pair_key",
        "physical_device_key",
        "split_group",
        "fold_number",
    }
)
FORBIDDEN_FEATURE_PREFIXES = ("post_", "observed_", "response_", "residual_")
FORBIDDEN_FEATURE_MARKERS = ("validation", "prediction_id", "v2_prediction", "v2_residual")


class ResearchContractError(ValueError):
    """A research input would violate claim, leakage, or reproducibility rules."""


def require_finite(value: float, name: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ResearchContractError(f"{name} must be finite")
    return number


def validate_feature_names(names: Sequence[str]) -> tuple[str, ...]:
    """Return normalized feature names or fail closed on leakage markers."""
    normalized = tuple(str(name).strip() for name in names)
    if not normalized or any(not name for name in normalized):
        raise ResearchContractError("a non-empty feature contract is required")
    if len(normalized) != len(set(normalized)):
        raise ResearchContractError("feature names must be unique")
    forbidden = []
    for name in normalized:
        lowered = name.lower()
        if (
            lowered in FORBIDDEN_FEATURE_EXACT
            or lowered.startswith(FORBIDDEN_FEATURE_PREFIXES)
            or any(marker in lowered for marker in FORBIDDEN_FEATURE_MARKERS)
        ):
            forbidden.append(name)
    if forbidden:
        raise ResearchContractError(
            "post-outcome or identity leakage features are forbidden: " + ", ".join(sorted(forbidden))
        )
    return normalized


@dataclass(frozen=True)
class ResearchPoint:
    source_point_id: int
    point_index: int
    v_gate_v: float
    i_drain_a: float
    v_drain_v: float | None = None

    def __post_init__(self) -> None:
        if self.source_point_id <= 0 or self.point_index < 0:
            raise ResearchContractError("point identities must be positive/zero-based")
        require_finite(self.v_gate_v, "v_gate_v")
        require_finite(self.i_drain_a, "i_drain_a")
        if self.v_drain_v is not None:
            require_finite(self.v_drain_v, "v_drain_v")


@dataclass(frozen=True)
class ResearchPair:
    source_pair_id: int
    pair_key: str
    pre_feature_id: int
    post_feature_id: int
    pre_metadata_id: int
    post_metadata_id: int
    physical_device_key: str
    device_type: str
    manufacturer: str | None
    campaign_key: str | None
    run_key: str | None
    ion_species: str | None
    beam_energy_mev: float | None
    let_surface: float | None
    range_um: float | None
    beam_type: str | None
    fluence: float | None
    pre_vds_v: float | None
    post_vds_v: float | None
    pre_points: tuple[ResearchPoint, ...]
    post_points: tuple[ResearchPoint, ...]

    def __post_init__(self) -> None:
        for name in (
            "source_pair_id",
            "pre_feature_id",
            "post_feature_id",
            "pre_metadata_id",
            "post_metadata_id",
        ):
            if int(getattr(self, name)) <= 0:
                raise ResearchContractError(f"{name} must be positive")
        for name in ("pair_key", "physical_device_key", "device_type"):
            if not str(getattr(self, name) or "").strip():
                raise ResearchContractError(f"{name} is required")
        if not self.pre_points or not self.post_points:
            raise ResearchContractError("both raw curves are required")


@dataclass(frozen=True)
class AuditedPair:
    candidate: ResearchPair
    admitted: bool
    exclusion_reasons: tuple[str, ...]
    pre_point_hash: str
    post_point_hash: str
    pair_payload_hash: str
    pre_vth_v: float | None
    post_vth_v: float | None
    observed_delta_vth_v: float | None
    extraction_diagnostics: Mapping[str, object] = field(default_factory=dict)
    common_grid_point_count: int = 0


@dataclass(frozen=True)
class SplitAssignment:
    pair_key: str
    validation_scheme: str
    fold_number: int
    held_out_group_key: str
    physical_device_key: str
    assignment_hash: str


@dataclass(frozen=True)
class ScalarOOFPrediction:
    pair_key: str
    validation_scheme: str
    fold_number: int
    held_out_group_key: str
    observed_delta_vth_v: float
    predicted_delta_vth_v: float | None
    training_device_keys: tuple[str, ...]
    support_status: str = "supported"
    support_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_finite(self.observed_delta_vth_v, "observed_delta_vth_v")
        if self.predicted_delta_vth_v is not None:
            require_finite(self.predicted_delta_vth_v, "predicted_delta_vth_v")
        if self.support_status not in {"supported", "abstained"}:
            raise ResearchContractError("invalid scalar support_status")


@dataclass(frozen=True)
class ArtifactIdentity:
    path: Path
    checksum: str
