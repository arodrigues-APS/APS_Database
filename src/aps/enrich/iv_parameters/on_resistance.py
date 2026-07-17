"""Protocol-matched robust on-resistance extraction."""

from __future__ import annotations

import math
from statistics import median
from typing import Iterable

from aps.enrich.iv_parameters.contracts import (
    EXTRACTION_METHOD_VERSION, ExtractionConfig, MetricResult, SweepPoint,
    enforce_value_range, finite, valid_point,
)


def _invalid(config: ExtractionConfig, reason: str, n_points: int = 0) -> MetricResult:
    return MetricResult(
        "rdson_mohm", None, "mohm", EXTRACTION_METHOD_VERSION,
        config.config_version, "invalid", (reason,), n_points=n_points,
    )


def _linear_fit(rows: list[SweepPoint], fit_intercept: bool) -> tuple[float, float] | None:
    """Fit V = intercept + R*I and return (intercept, R_ohm)."""
    currents = [float(row.i_drain) for row in rows]
    voltages = [float(row.v_drain) for row in rows]
    if fit_intercept:
        mean_i, mean_v = sum(currents) / len(currents), sum(voltages) / len(voltages)
        denominator = sum((value - mean_i) ** 2 for value in currents)
        if denominator <= 0.0:
            return None
        slope = sum((i - mean_i) * (v - mean_v)
                    for i, v in zip(currents, voltages)) / denominator
        return mean_v - slope * mean_i, slope
    denominator = sum(current * current for current in currents)
    if denominator <= 0.0:
        return None
    return 0.0, sum(i * v for i, v in zip(currents, voltages)) / denominator


def _residuals(rows: list[SweepPoint], intercept: float, slope: float) -> list[float]:
    return [float(row.v_drain) - intercept - slope * float(row.i_drain) for row in rows]


def extract_rdson(points: Iterable[SweepPoint], config: ExtractionConfig) -> MetricResult:
    if config.target_type != "log_rdson_ratio":
        raise ValueError("Rds(on) extraction requires target_type=log_rdson_ratio")
    if config.required_vgs_v is None or not finite(config.required_vgs_v):
        return _invalid(config, "missing_required_vgs")

    accepted = [
        point for point in points
        if valid_point(point) and finite(point.v_gate) and finite(point.v_drain)
        and abs(float(point.v_gate) - config.required_vgs_v) <= config.vgs_tolerance_v
        and config.linear_vds_min_v <= float(point.v_drain) <= config.linear_vds_max_v
        and float(point.i_drain) > 0.0
    ]
    if len(accepted) < config.minimum_points:
        return _invalid(config, "insufficient_protocol_matched_points", len(accepted))

    initial = _linear_fit(accepted, config.fit_intercept)
    if initial is None:
        return _invalid(config, "singular_linear_fit", len(accepted))
    residuals = _residuals(accepted, *initial)
    center = median(residuals)
    sigma = 1.4826 * median(abs(value - center) for value in residuals)
    retained = ([row for row, residual in zip(accepted, residuals)
                 if abs(residual - center) <= 3.5 * sigma]
                if sigma > 0.0 else accepted)
    if len(retained) < config.minimum_points:
        return _invalid(config, "insufficient_points_after_outlier_filter", len(retained))

    fitted = _linear_fit(retained, config.fit_intercept)
    if fitted is None:
        return _invalid(config, "singular_linear_fit", len(retained))
    intercept, resistance_ohm = fitted
    resistance_mohm = resistance_ohm * 1000.0
    if resistance_mohm <= 0.0 or not finite(resistance_mohm):
        return _invalid(config, "nonpositive_or_invalid_resistance", len(retained))
    range_reason = enforce_value_range(resistance_mohm, config)
    if range_reason:
        return _invalid(config, range_reason, len(retained))

    final_residuals = _residuals(retained, intercept, resistance_ohm)
    dof = max(len(retained) - (2 if config.fit_intercept else 1), 1)
    residual_rmse = math.sqrt(sum(value**2 for value in final_residuals) / dof)
    current_span = max(float(row.i_drain) for row in retained) - min(float(row.i_drain) for row in retained)
    uncertainty = residual_rmse / current_span * 1000.0 if current_span > 0.0 else None
    return MetricResult(
        "rdson_mohm", resistance_mohm, "mohm", EXTRACTION_METHOD_VERSION,
        config.config_version, "usable", uncertainty=uncertainty,
        n_points=len(retained), diagnostics={
            "required_vgs_v": config.required_vgs_v,
            "vds_window_v": [config.linear_vds_min_v, config.linear_vds_max_v],
            "fit_intercept": config.fit_intercept,
            "intercept_v": intercept,
            "residual_rmse_v": residual_rmse,
            "initial_point_count": len(accepted),
            "retained_point_count": len(retained),
        },
    )
