"""Hybrid deterministic-shift plus residual-shape research modeling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
from sklearn.decomposition import PCA
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import HuberRegressor, Ridge
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from aps.ml.iv_damage_research_contracts import AuditedPair, ResearchContractError, ResearchPoint, ScalarOOFPrediction
from aps.ml.iv_damage_research_scalar import DEFAULT_FEATURES, feature_record


@dataclass(frozen=True)
class CurvePredictionResult:
    pair_key: str
    grid_v: tuple[float, ...]
    deterministic_i_a: tuple[float, ...]
    hybrid_i_a: tuple[float, ...]
    actual_post_i_a: tuple[float | None, ...]
    correction_applied: bool
    correction_norm: float
    fallback_reason: str | None
    comparison_series: Mapping[str, tuple[float | None, ...]]
    pca_explained_variance: tuple[float, ...]
    metrics: Mapping[str, float]


def _xy(points: Sequence[ResearchPoint]) -> tuple[np.ndarray, np.ndarray]:
    ordered: dict[float, list[float]] = {}
    for point in points:
        ordered.setdefault(float(point.v_gate_v), []).append(float(point.i_drain_a))
    x = np.asarray(sorted(ordered), dtype=float)
    y = np.asarray([float(np.median(ordered[value])) for value in x], dtype=float)
    if len(x) < 2 or not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        raise ResearchContractError("curve requires at least two finite unique voltage points")
    return x, y


def supported_grid(
    pre_points: Sequence[ResearchPoint],
    post_points: Sequence[ResearchPoint],
    shift_v: float,
    *,
    n_points: int = 64,
) -> np.ndarray:
    pre_x, _ = _xy(pre_points)
    post_x, _ = _xy(post_points)
    # Projection evaluates I_pre(Vg - shift), so Vg support is pre_x + shift.
    lower = max(float(pre_x.min() + shift_v), float(post_x.min()))
    upper = min(float(pre_x.max() + shift_v), float(post_x.max()))
    if n_points < 2 or upper <= lower:
        raise ResearchContractError("no non-extrapolated common grid support")
    return np.linspace(lower, upper, n_points)


def prediction_grid(
    pre_points: Sequence[ResearchPoint], shift_v: float, *, n_points: int = 64
) -> np.ndarray:
    """Build prediction support without consulting held-out post measurements."""
    pre_x, _ = _xy(pre_points)
    if n_points < 2:
        raise ResearchContractError("prediction grid requires at least two points")
    return np.linspace(float(pre_x.min() + shift_v), float(pre_x.max() + shift_v), n_points)


def partial_interpolate_curve(
    points: Sequence[ResearchPoint], grid_v: Sequence[float]
) -> tuple[float | None, ...]:
    """Join truth after prediction and leave unsupported evaluation edges absent."""
    x, y = _xy(points)
    return tuple(
        float(np.interp(value, x, y)) if float(x.min()) <= value <= float(x.max()) else None
        for value in np.asarray(grid_v, dtype=float)
    )


def interpolate_curve(points: Sequence[ResearchPoint], grid_v: Sequence[float]) -> np.ndarray:
    x, y = _xy(points)
    grid = np.asarray(grid_v, dtype=float)
    # ``supported_grid`` constructs endpoints by adding the shift and the
    # projection subtracts it again. Those inverse floating-point operations
    # can differ from the measured endpoint by a few ulps. Treat only that
    # numerical round-off as supported; retain the fail-closed behavior for
    # any material extrapolation.
    scale = max(float(np.max(np.abs(x))), float(np.max(np.abs(grid))), 1.0)
    tolerance = np.finfo(float).eps * scale * 16.0
    if grid.min() < x.min() - tolerance or grid.max() > x.max() + tolerance:
        raise ResearchContractError("curve interpolation would extrapolate")
    return np.interp(np.clip(grid, x.min(), x.max()), x, y)


def deterministic_projection(
    pre_points: Sequence[ResearchPoint], grid_v: Sequence[float], shift_v: float
) -> np.ndarray:
    return interpolate_curve(pre_points, np.asarray(grid_v, dtype=float) - float(shift_v))


def partial_deterministic_projection(
    pre_points: Sequence[ResearchPoint],
    grid_v: Sequence[float],
    shift_v: float,
) -> tuple[float | None, ...]:
    """Project only supported voltages, representing unsupported edges as None."""
    x, y = _xy(pre_points)
    source = np.asarray(grid_v, dtype=float) - float(shift_v)
    return tuple(
        float(np.interp(value, x, y)) if float(x.min()) <= value <= float(x.max()) else None for value in source
    )


def current_scale(curves: Sequence[np.ndarray]) -> float:
    values = np.concatenate([np.abs(np.asarray(curve, dtype=float)) for curve in curves])
    positive = values[values > 0]
    return max(float(np.median(positive)) if positive.size else 1e-12, 1e-12)


def transformed_residual_target(
    pair: AuditedPair,
    *,
    grid_points: int = 64,
    scale_a: float | None = None,
) -> tuple[np.ndarray, np.ndarray, float]:
    """Use observed ΔVth only as a training residual target definition."""
    if pair.observed_delta_vth_v is None:
        raise ResearchContractError("observed shift is required for a residual target")
    grid = supported_grid(
        pair.candidate.pre_points, pair.candidate.post_points, float(pair.observed_delta_vth_v), n_points=grid_points
    )
    projected = deterministic_projection(pair.candidate.pre_points, grid, float(pair.observed_delta_vth_v))
    actual = interpolate_curve(pair.candidate.post_points, grid)
    scale = current_scale([projected, actual]) if scale_a is None else float(scale_a)
    residual = np.arcsinh(actual / scale) - np.arcsinh(projected / scale)
    return grid, residual, scale


def _normalized_grid_residual(pair: AuditedPair, scale_a: float, n_points: int) -> np.ndarray:
    grid, residual, _ = transformed_residual_target(pair, grid_points=n_points, scale_a=scale_a)
    # Residual PCA uses normalized voltage so varying raw overlap does not leak
    # held-out post-grid coordinates into predictor features.
    normalized = np.linspace(0.0, 1.0, n_points)
    source = (grid - grid[0]) / (grid[-1] - grid[0])
    return np.interp(normalized, source, residual)


def _residual_regressor(method: str, seed: int):
    if method == "hybrid_huber":
        return Pipeline(
            [
                ("vectorizer", DictVectorizer(sparse=False, sort=True)),
                ("scaler", RobustScaler()),
                ("regressor", MultiOutputRegressor(HuberRegressor(epsilon=1.35, alpha=0.05, max_iter=500))),
            ]
        )
    if method == "hybrid_extra_trees":
        return Pipeline(
            [
                ("vectorizer", DictVectorizer(sparse=False, sort=True)),
                (
                    "regressor",
                    ExtraTreesRegressor(
                        n_estimators=300, min_samples_leaf=2, max_features=0.75, random_state=seed, n_jobs=1
                    ),
                ),
            ]
        )
    if method == "ridge_residual":
        return Pipeline(
            [
                ("vectorizer", DictVectorizer(sparse=False, sort=True)),
                ("scaler", RobustScaler()),
                ("regressor", Ridge(alpha=10.0)),
            ]
        )
    raise ResearchContractError(f"unsupported hybrid method: {method}")


def curve_metrics(
    predicted: np.ndarray,
    actual: np.ndarray,
    scale_a: float,
    supported_voltage_fraction: float,
) -> dict[str, float]:
    error = np.asarray(predicted) - np.asarray(actual)
    rmse = float(np.sqrt(np.mean(error**2)))
    span = max(float(np.max(actual) - np.min(actual)), scale_a)
    return {
        "mae_a": float(np.mean(np.abs(error))),
        "max_abs_error_a": float(np.max(np.abs(error))),
        "normalized_rmse": rmse / span,
        "transformed_mae": float(np.mean(np.abs(np.arcsinh(predicted / scale_a) - np.arcsinh(actual / scale_a)))),
        "supported_voltage_fraction": float(supported_voltage_fraction),
    }


def fit_predict_hybrid_fold(
    training_pairs: Sequence[AuditedPair],
    held_pair: AuditedPair,
    scalar_prediction: ScalarOOFPrediction,
    *,
    method: str,
    feature_names: Sequence[str] = DEFAULT_FEATURES,
    grid_points: int = 64,
    max_components: int = 3,
    seed: int = 17,
) -> CurvePredictionResult:
    """Fit all transforms on training devices and score one held-out pair."""
    held_device = held_pair.candidate.physical_device_key
    if held_device in {row.candidate.physical_device_key for row in training_pairs}:
        raise ResearchContractError("held-out device is present in residual-model training rows")
    if scalar_prediction.pair_key != held_pair.candidate.pair_key:
        raise ResearchContractError("scalar and curve pair identities differ")
    if held_device in scalar_prediction.training_device_keys:
        raise ResearchContractError("held-out device is present in scalar training manifest")
    if scalar_prediction.predicted_delta_vth_v is None:
        raise ResearchContractError("hybrid requires a supported out-of-fold scalar prediction")

    training_actual = []
    for row in training_pairs:
        grid = supported_grid(
            row.candidate.pre_points, row.candidate.post_points, float(row.observed_delta_vth_v), n_points=grid_points
        )
        training_actual.append(interpolate_curve(row.candidate.post_points, grid))
    scale_a = current_scale(training_actual)
    residual_matrix = np.vstack([_normalized_grid_residual(row, scale_a, grid_points) for row in training_pairs])
    components = min(max_components, residual_matrix.shape[0] - 1, residual_matrix.shape[1])
    shift = float(scalar_prediction.predicted_delta_vth_v)
    held_grid = prediction_grid(held_pair.candidate.pre_points, shift, n_points=grid_points)
    deterministic = deterministic_projection(held_pair.candidate.pre_points, held_grid, shift)
    fallback = None
    correction = np.zeros(grid_points, dtype=float)
    explained_variance: tuple[float, ...] = ()
    if components < 1:
        fallback = "insufficient_training_devices_for_residual_pca"
    else:
        pca = PCA(n_components=components, svd_solver="full")
        scores = pca.fit_transform(residual_matrix)
        explained_variance = tuple(float(value) for value in pca.explained_variance_ratio_)
        regressor = _residual_regressor(method, seed)
        try:
            regressor.fit([feature_record(row, feature_names) for row in training_pairs], scores)
            predicted_scores = np.asarray(regressor.predict([feature_record(held_pair, feature_names)])[0], dtype=float)
            correction = pca.inverse_transform(predicted_scores)
            norms = np.linalg.norm(residual_matrix, axis=1)
            limit = max(float(np.quantile(norms, 0.95)) * 1.5, 1e-9)
            if not np.all(np.isfinite(correction)):
                fallback = "nonfinite_residual_correction"
            elif float(np.linalg.norm(correction)) > limit:
                fallback = "residual_correction_outside_training_support"
        except (ValueError, FloatingPointError) as exc:
            fallback = f"residual_model_failed:{type(exc).__name__}"
    if fallback:
        correction = np.zeros(grid_points, dtype=float)
    hybrid = np.sinh(np.arcsinh(deterministic / scale_a) + correction) * scale_a
    actual = partial_interpolate_curve(held_pair.candidate.post_points, held_grid)
    evaluation_mask = np.asarray([value is not None for value in actual], dtype=bool)
    if not np.any(evaluation_mask):
        raise ResearchContractError("held-out post curve has no overlap with prediction support")
    actual_evaluation = np.asarray([value for value in actual if value is not None], dtype=float)
    predicted_evaluation = hybrid[evaluation_mask]
    return CurvePredictionResult(
        pair_key=held_pair.candidate.pair_key,
        grid_v=tuple(float(value) for value in held_grid),
        deterministic_i_a=tuple(float(value) for value in deterministic),
        hybrid_i_a=tuple(float(value) for value in hybrid),
        actual_post_i_a=actual,
        correction_applied=fallback is None,
        correction_norm=float(np.linalg.norm(correction)),
        fallback_reason=fallback,
        pca_explained_variance=explained_variance,
        comparison_series={
            "pre_measured": partial_deterministic_projection(held_pair.candidate.pre_points, held_grid, 0.0),
            "zero_damage": partial_deterministic_projection(held_pair.candidate.pre_points, held_grid, 0.0),
        },
        metrics=curve_metrics(
            predicted_evaluation,
            actual_evaluation,
            scale_a,
            float(np.mean(evaluation_mask)),
        ),
    )
