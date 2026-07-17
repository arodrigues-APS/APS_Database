"""Shared contracts for canonical IV parameter extraction."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from statistics import median
from typing import Iterable, Mapping

EXTRACTION_METHOD_VERSION = "iv-parameters-v3.0"


@dataclass(frozen=True)
class SweepPoint:
    point_index: int
    v_gate: float | None
    v_drain: float | None
    i_drain: float | None
    compliance_limited: bool = False


@dataclass(frozen=True)
class ExtractionConfig:
    """Versioned measurement-protocol settings for one target."""

    config_version: str
    target_type: str
    target_current_a: float | None = None
    required_vds_v: float | None = None
    vds_tolerance_v: float = 0.05
    required_vgs_v: float | None = None
    vgs_tolerance_v: float = 0.10
    linear_vds_min_v: float = 0.0
    linear_vds_max_v: float = 2.0
    sweep_direction: str = "ascending"
    log_current_interpolation: bool = True
    fit_intercept: bool = True
    minimum_points: int = 3
    valid_min: float | None = None
    valid_max: float | None = None

    def __post_init__(self) -> None:
        if self.target_type not in {"delta_vth_v", "log_rdson_ratio"}:
            raise ValueError(f"unsupported target_type: {self.target_type}")
        if self.sweep_direction not in {"ascending", "descending"}:
            raise ValueError("sweep_direction must be ascending or descending")
        if self.minimum_points < 2:
            raise ValueError("minimum_points must be at least 2")


@dataclass(frozen=True)
class MetricResult:
    metric_name: str
    value: float | None
    unit: str
    method_version: str
    config_version: str
    quality_status: str
    quality_reasons: tuple[str, ...] = ()
    uncertainty: float | None = None
    n_points: int = 0
    diagnostics: Mapping[str, object] = field(default_factory=dict)

    @property
    def usable(self) -> bool:
        return self.quality_status == "usable" and finite(self.value)


@dataclass(frozen=True)
class ReplicateAggregate:
    metric_name: str
    value: float | None
    uncertainty: float | None
    unit: str
    replicate_count: int
    quality_status: str
    quality_reasons: tuple[str, ...]
    method_version: str
    config_versions: tuple[str, ...]


def finite(value: object) -> bool:
    if value is None:
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and abs(number) < 1e30


def valid_point(point: SweepPoint) -> bool:
    return (
        not point.compliance_limited
        and finite(point.i_drain)
        and (point.v_gate is None or finite(point.v_gate))
        and (point.v_drain is None or finite(point.v_drain))
    )


def enforce_value_range(value: float, config: ExtractionConfig) -> str | None:
    if config.valid_min is not None and value < config.valid_min:
        return "below_protocol_valid_range"
    if config.valid_max is not None and value > config.valid_max:
        return "above_protocol_valid_range"
    return None


def aggregate_replicates(
    results: Iterable[MetricResult],
    *,
    minimum_replicates: int = 2,
) -> ReplicateAggregate:
    """Aggregate compatible repetitions instead of treating files as samples."""
    rows = list(results)
    usable = [row for row in rows if row.usable]
    metric_names = {row.metric_name for row in usable}
    units = {row.unit for row in usable}
    methods = {row.method_version for row in usable}
    if len(metric_names) > 1 or len(units) > 1 or len(methods) > 1:
        return ReplicateAggregate(
            next(iter(metric_names), "unknown"), None, None,
            next(iter(units), "unknown"), len(usable), "invalid",
            ("incompatible_replicate_contracts",),
            next(iter(methods), EXTRACTION_METHOD_VERSION),
            tuple(sorted({row.config_version for row in usable})),
        )
    if not usable:
        return ReplicateAggregate(
            rows[0].metric_name if rows else "unknown", None, None,
            rows[0].unit if rows else "unknown", 0, "invalid",
            ("no_usable_replicates",),
            rows[0].method_version if rows else EXTRACTION_METHOD_VERSION,
            tuple(sorted({row.config_version for row in rows})),
        )

    values = [float(row.value) for row in usable]
    center = median(values)
    mad = median(abs(value - center) for value in values)
    sigma = 1.4826 * mad
    point_uncertainties = [
        float(row.uncertainty) for row in usable
        if finite(row.uncertainty) and float(row.uncertainty) >= 0.0
    ]
    within = median(point_uncertainties) if point_uncertainties else 0.0
    uncertainty = math.sqrt((sigma / math.sqrt(len(values))) ** 2 + within**2)
    sufficient = len(values) >= minimum_replicates
    return ReplicateAggregate(
        usable[0].metric_name, center, uncertainty, usable[0].unit, len(values),
        "usable" if sufficient else "screening_only",
        () if sufficient else ("insufficient_replicates",),
        usable[0].method_version,
        tuple(sorted({row.config_version for row in usable})),
    )
