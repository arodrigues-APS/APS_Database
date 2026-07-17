"""Fixed-current, interpolated threshold-voltage extraction."""

from __future__ import annotations

import math
from typing import Iterable

from aps.enrich.iv_parameters.contracts import (
    EXTRACTION_METHOD_VERSION, ExtractionConfig, MetricResult, SweepPoint,
    enforce_value_range, finite, valid_point,
)


def _invalid(config: ExtractionConfig, reason: str, n_points: int = 0) -> MetricResult:
    return MetricResult(
        "vth_v", None, "V", EXTRACTION_METHOD_VERSION, config.config_version,
        "invalid", (reason,), n_points=n_points,
    )


def _interpolate(first: SweepPoint, second: SweepPoint, target_current: float,
                 log_current: bool) -> float | None:
    v1, v2 = float(first.v_gate), float(second.v_gate)
    i1, i2 = float(first.i_drain), float(second.i_drain)
    if i1 == i2:
        return None
    if log_current:
        if min(i1, i2, target_current) <= 0.0:
            return None
        y1, y2, target = math.log(i1), math.log(i2), math.log(target_current)
    else:
        y1, y2, target = i1, i2, target_current
    fraction = (target - y1) / (y2 - y1)
    return v1 + fraction * (v2 - v1) if 0.0 <= fraction <= 1.0 else None


def extract_vth(points: Iterable[SweepPoint], config: ExtractionConfig) -> MetricResult:
    if config.target_type != "delta_vth_v":
        raise ValueError("Vth extraction requires target_type=delta_vth_v")
    if not finite(config.target_current_a) or float(config.target_current_a) <= 0.0:
        return _invalid(config, "missing_or_invalid_target_current")

    accepted = []
    for point in points:
        if not valid_point(point) or not finite(point.v_gate):
            continue
        if config.required_vds_v is not None:
            if not finite(point.v_drain):
                continue
            if abs(float(point.v_drain) - config.required_vds_v) > config.vds_tolerance_v:
                continue
        if float(point.i_drain) > 0.0:
            accepted.append(point)
    accepted.sort(key=lambda point: point.point_index)
    if len(accepted) < 2:
        return _invalid(config, "insufficient_protocol_matched_points", len(accepted))

    target = float(config.target_current_a)
    bracket = None
    for first, second in zip(accepted, accepted[1:]):
        i1, i2 = float(first.i_drain), float(second.i_drain)
        if config.sweep_direction == "ascending" and i1 <= target <= i2:
            bracket = first, second
            break
        if config.sweep_direction == "descending" and i1 >= target >= i2:
            bracket = first, second
            break
    if bracket is None:
        return _invalid(config, "target_current_not_bracketed", len(accepted))

    value = _interpolate(*bracket, target, config.log_current_interpolation)
    if value is None or not finite(value):
        return _invalid(config, "threshold_interpolation_failed", len(accepted))
    range_reason = enforce_value_range(value, config)
    if range_reason:
        return _invalid(config, range_reason, len(accepted))

    voltage_step = abs(float(bracket[1].v_gate) - float(bracket[0].v_gate))
    return MetricResult(
        "vth_v", value, "V", EXTRACTION_METHOD_VERSION,
        config.config_version, "usable",
        uncertainty=voltage_step / math.sqrt(12.0), n_points=len(accepted),
        diagnostics={
            "target_current_a": target,
            "required_vds_v": config.required_vds_v,
            "bracket_point_indices": [bracket[0].point_index, bracket[1].point_index],
            "bracket_voltage_step_v": voltage_step,
            "interpolation": "log_current" if config.log_current_interpolation else "linear_current",
            "sweep_direction": config.sweep_direction,
        },
    )
