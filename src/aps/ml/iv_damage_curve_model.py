"""Protocol-aligned deterministic and learned V3 full-curve prediction.

The functional model predicts a vector-valued post-stress curve.  It does not
derive a curve from a scalar Vth/RDS(on) estimate: it learns the complete
post-minus-pre current shape on a frozen voltage grid.  Calibration is grouped
by physical device and uses each device's worst pointwise residual to create a
simultaneous (whole-curve) conformal band.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping, Sequence

import numpy as np
from sklearn.decomposition import PCA
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import Ridge
from sklearn.preprocessing import RobustScaler

from aps.ml.iv_damage_readiness import (
    DOMAIN_REQUIRED_FEATURES,
    validate_required_features,
)


class CurveModelError(ValueError):
    """A curve cannot enter this model without changing its meaning."""


@dataclass(frozen=True)
class CurveExample:
    pair_key: str
    physical_device_key: str
    stress_session_key: str
    stress_type: str
    curve_family: str
    measurement_protocol_id: str
    device_type: str
    features: Mapping[str, object]
    pre_x_v: Sequence[float]
    pre_i_a: Sequence[float]
    post_x_v: Sequence[float]
    post_i_a: Sequence[float]
    manufacturer: str | None = None
    ion_species: str | None = None
    prediction_horizon_s: float | None = None


@dataclass(frozen=True)
class CurveRequest:
    stress_type: str
    curve_family: str
    measurement_protocol_id: str
    device_type: str
    features: Mapping[str, object]
    pre_x_v: Sequence[float]
    pre_i_a: Sequence[float]
    manufacturer: str | None = None
    ion_species: str | None = None
    prediction_horizon_s: float | None = None


@dataclass(frozen=True)
class CurvePrediction:
    x_v: tuple[float, ...]
    pre_i_a: tuple[float, ...]
    predicted_i_a: tuple[float, ...]
    lower_i_a: tuple[float, ...]
    upper_i_a: tuple[float, ...]
    in_domain: bool
    evidence_status: str
    reasons: tuple[str, ...]
    ood_score: float | None = None
    ood_threshold: float | None = None
    neighbor_devices: int = 0


@dataclass(frozen=True)
class CurveErrorMetrics:
    mae_a: float
    max_abs_error_a: float
    normalized_rmse: float
    simultaneous_band_hit: bool


def _finite_vector(values: Sequence[float], name: str) -> np.ndarray:
    result = np.asarray(values, dtype=float)
    if result.ndim != 1 or len(result) < 3 or not np.all(np.isfinite(result)):
        raise CurveModelError(f"{name} must contain at least three finite values")
    return result


def _curve(x_values: Sequence[float], currents: Sequence[float], name: str) -> tuple[np.ndarray, np.ndarray]:
    x = _finite_vector(x_values, f"{name}_x_v")
    current = _finite_vector(currents, f"{name}_i_a")
    if len(x) != len(current):
        raise CurveModelError(f"{name} voltage/current lengths differ")
    if not np.all(np.diff(x) > 0.0):
        raise CurveModelError(f"{name} voltage grid must be strictly increasing")
    return x, current


def deterministic_curve_projection(
    *,
    projection_kind: str,
    x_v: Sequence[float],
    pre_i_a: Sequence[float],
    response: float,
    response_lower: float,
    response_upper: float,
) -> CurvePrediction:
    """Project a scalar prediction while making the shape constraint explicit."""
    x, current = _curve(x_v, pre_i_a, "pre")
    values = np.asarray([response, response_lower, response_upper], dtype=float)
    if not np.all(np.isfinite(values)) or response_lower > response or response > response_upper:
        raise CurveModelError("response interval must be finite and ordered")
    candidates: list[np.ndarray] = []
    if projection_kind == "rigid_vth_shift":
        # post(Vg) = pre(Vg - delta_Vth); NaN marks prohibited extrapolation.
        for shift in values:
            candidates.append(np.interp(x - shift, x, current, left=np.nan, right=np.nan))
    elif projection_kind == "linear_rdson_scale":
        # log(Rpost/Rpre)=r and I is inversely proportional to R in this regime.
        candidates = [current * math.exp(-float(value)) for value in values]
    else:
        raise CurveModelError(f"unsupported projection_kind: {projection_kind}")
    matrix = np.vstack(candidates)
    supported = np.all(np.isfinite(matrix), axis=0)
    if supported.sum() < 3:
        return CurvePrediction((), (), (), (), (), False, "insufficient_evidence", ("projection_grid_overlap_too_small",))
    x = x[supported]
    current = current[supported]
    matrix = matrix[:, supported]
    return CurvePrediction(
        tuple(x), tuple(current), tuple(matrix[0]),
        tuple(np.min(matrix, axis=0)), tuple(np.max(matrix, axis=0)),
        True, "screening_only", ("shape_constrained_scalar_projection",),
    )


class FunctionalCurveDamageModel:
    """Functional PCA/Ridge model for full post-stress IV-curve shape."""

    def __init__(
        self,
        *,
        stress_type: str,
        curve_family: str,
        measurement_protocol_id: str,
        grid_points: int = 64,
        pca_components: int = 8,
        ridge_alpha: float = 1.0,
        interval_coverage: float = 0.80,
        ood_quantile: float = 0.95,
        min_neighbor_devices: int = 2,
        min_calibration_devices: int = 10,
    ) -> None:
        if stress_type not in DOMAIN_REQUIRED_FEATURES:
            raise CurveModelError(f"unsupported stress_type: {stress_type}")
        if curve_family not in {"IdVg", "IdVd"}:
            raise CurveModelError(f"unsupported curve_family: {curve_family}")
        if not str(measurement_protocol_id).strip():
            raise CurveModelError("measurement_protocol_id is required")
        if grid_points < 8 or pca_components < 1:
            raise CurveModelError("grid_points >= 8 and pca_components >= 1 are required")
        if not 0.5 < interval_coverage < 1.0 or not 0.5 <= ood_quantile < 1.0:
            raise CurveModelError("invalid calibration or OOD quantile")
        self.stress_type = stress_type
        self.curve_family = curve_family
        self.measurement_protocol_id = measurement_protocol_id
        self.grid_points = grid_points
        self.pca_components = pca_components
        self.ridge_alpha = ridge_alpha
        self.interval_coverage = interval_coverage
        self.ood_quantile = ood_quantile
        self.min_neighbor_devices = min_neighbor_devices
        self.min_calibration_devices = min_calibration_devices
        self.required_features = tuple(sorted(DOMAIN_REQUIRED_FEATURES[stress_type]))
        self._grid: np.ndarray | None = None
        self._current_scale: float | None = None
        self._vectorizer = DictVectorizer(sparse=False)
        self._input_scaler = RobustScaler()
        self._pca: PCA | None = None
        self._regressor: Ridge | None = None
        self._support_center: np.ndarray | None = None
        self._support_scale: np.ndarray | None = None
        self._support_matrix: np.ndarray | None = None
        self._training_devices: tuple[str, ...] = ()
        self._training_device_keys: frozenset[str] = frozenset()
        self._training_session_keys: frozenset[str] = frozenset()
        self._known_categories: dict[str, frozenset[str]] = {}
        self._ood_threshold: float | None = None
        self._conformal_radius_a: float | None = None

    @property
    def is_fitted(self) -> bool:
        return self._regressor is not None and self._ood_threshold is not None

    @property
    def is_calibrated(self) -> bool:
        return self.is_fitted and self._conformal_radius_a is not None

    def _categories(self, row: CurveExample | CurveRequest) -> dict[str, str]:
        result = {"device_type": str(row.device_type).strip()}
        if row.manufacturer:
            result["manufacturer"] = str(row.manufacturer).strip()
        if self.stress_type == "irradiation":
            result["ion_species"] = str(row.ion_species or "").strip()
        return result

    def _validate_domain(self, row: CurveExample | CurveRequest) -> list[str]:
        reasons = list(validate_required_features(stress_type=row.stress_type, features=row.features))
        if row.stress_type != self.stress_type:
            reasons.append("wrong_stress_type")
        if row.curve_family != self.curve_family:
            reasons.append("wrong_curve_family")
        if row.measurement_protocol_id != self.measurement_protocol_id:
            reasons.append("unseen_protocol_signature")
        if not str(row.device_type).strip():
            reasons.append("missing_category:device_type")
        if self.stress_type == "irradiation" and not str(row.ion_species or "").strip():
            reasons.append("missing_category:ion_species")
        delay = row.features.get("post_measurement_delay_s")
        if self.stress_type == "irradiation" and (
            row.prediction_horizon_s is None
            or not math.isfinite(float(row.prediction_horizon_s))
            or not isinstance(delay, (int, float))
            or float(row.prediction_horizon_s) != float(delay)
        ):
            reasons.append("prediction_horizon_conflict")
        return reasons

    def _context(self, row: CurveExample | CurveRequest) -> dict[str, object]:
        values: dict[str, object] = {
            name: float(row.features[name]) for name in self.required_features
        }
        values.update({f"category={key}": value for key, value in self._categories(row).items()})
        return values

    def _transformed_pre(self, row: CurveExample | CurveRequest) -> np.ndarray:
        x, current = _curve(row.pre_x_v, row.pre_i_a, "pre")
        if self._grid is None or self._current_scale is None:
            raise RuntimeError("model grid is not fitted")
        if x[0] > self._grid[0] or x[-1] < self._grid[-1]:
            raise CurveModelError("pre-curve does not cover the certified voltage grid")
        return np.arcsinh(np.interp(self._grid, x, current) / self._current_scale)

    def _design(self, rows: Sequence[CurveExample | CurveRequest], *, fit: bool) -> np.ndarray:
        contexts = [self._context(row) for row in rows]
        encoded = self._vectorizer.fit_transform(contexts) if fit else self._vectorizer.transform(contexts)
        pre = np.vstack([self._transformed_pre(row) for row in rows])
        raw = np.hstack((encoded, pre))
        return self._input_scaler.fit_transform(raw) if fit else self._input_scaler.transform(raw)

    def fit(self, rows: Sequence[CurveExample]) -> "FunctionalCurveDamageModel":
        if not rows:
            raise CurveModelError("training curves are required")
        if len({row.pair_key for row in rows}) != len(rows):
            raise CurveModelError("pair_key must be unique")
        devices = {row.physical_device_key for row in rows}
        if len(devices) < self.min_neighbor_devices + 1:
            raise CurveModelError("too few independent training devices")
        for row in rows:
            reasons = self._validate_domain(row)
            if reasons:
                raise CurveModelError(f"invalid training curve {row.pair_key}: {sorted(set(reasons))}")
        curves = [(_curve(row.pre_x_v, row.pre_i_a, "pre"), _curve(row.post_x_v, row.post_i_a, "post")) for row in rows]
        grid_min = max(max(pre[0][0], post[0][0]) for pre, post in curves)
        grid_max = min(min(pre[0][-1], post[0][-1]) for pre, post in curves)
        if not grid_max > grid_min:
            raise CurveModelError("training curves have no common voltage support")
        self._grid = np.linspace(grid_min, grid_max, self.grid_points)
        nonzero = [abs(float(value)) for pair in curves for curve in pair for value in curve[1] if value != 0]
        self._current_scale = max(float(np.median(nonzero)) if nonzero else 0.0, 1e-15)
        design = self._design(rows, fit=True)
        post = np.vstack([
            np.arcsinh(np.interp(self._grid, post_curve[0], post_curve[1]) / self._current_scale)
            for _, post_curve in curves
        ])
        pre = np.vstack([self._transformed_pre(row) for row in rows])
        deltas = post - pre
        components = min(self.pca_components, len(rows) - 1, self.grid_points)
        if components < 1:
            raise CurveModelError("at least two independent training curves are required")
        self._pca = PCA(n_components=components, svd_solver="full").fit(deltas)
        scores = self._pca.transform(deltas)
        self._regressor = Ridge(alpha=self.ridge_alpha).fit(design, scores)
        center = np.median(design, axis=0)
        q75, q25 = np.percentile(design, [75, 25], axis=0)
        scale = np.where((q75 - q25) > 1e-12, (q75 - q25) / 1.349, 1.0)
        support = (design - center) / scale
        device_order = tuple(row.physical_device_key for row in rows)
        kth: list[float] = []
        for index, point in enumerate(support):
            by_device: dict[str, float] = {}
            for other_index, other in enumerate(support):
                device = device_order[other_index]
                if other_index == index or device == device_order[index]:
                    continue
                distance = float(np.linalg.norm(point - other) / math.sqrt(len(point)))
                by_device[device] = min(distance, by_device.get(device, math.inf))
            kth.append(sorted(by_device.values())[self.min_neighbor_devices - 1])
        self._support_center, self._support_scale, self._support_matrix = center, scale, support
        self._training_devices = device_order
        self._ood_threshold = float(np.quantile(kth, self.ood_quantile, method="higher") * 1.05 + 1e-12)
        self._training_device_keys = frozenset(devices)
        self._training_session_keys = frozenset(row.stress_session_key for row in rows)
        categories: dict[str, set[str]] = {}
        for row in rows:
            for key, value in self._categories(row).items():
                categories.setdefault(key, set()).add(value)
        self._known_categories = {key: frozenset(values) for key, values in categories.items()}
        self._conformal_radius_a = None
        return self

    def _raw_prediction(self, row: CurveExample | CurveRequest) -> tuple[np.ndarray, np.ndarray]:
        if not self.is_fitted or self._pca is None or self._regressor is None:
            raise RuntimeError("fit the model before prediction")
        design = self._design([row], fit=False)
        delta = self._pca.inverse_transform(self._regressor.predict(design))[0]
        pre = self._transformed_pre(row)
        return np.sinh(pre + delta) * self._current_scale, design[0]

    def _domain_assessment(self, request: CurveRequest) -> tuple[bool, tuple[str, ...], float | None, int]:
        reasons = self._validate_domain(request)
        for key, value in self._categories(request).items():
            if value and value not in self._known_categories.get(key, frozenset()):
                reasons.append(f"unseen_category:{key}")
        if reasons:
            return False, tuple(sorted(set(reasons))), None, 0
        try:
            _, design = self._raw_prediction(request)
        except CurveModelError as exc:
            reasons.append(str(exc).replace(" ", "_"))
            return False, tuple(sorted(set(reasons))), None, 0
        point = (design - self._support_center) / self._support_scale
        by_device: dict[str, float] = {}
        for train, device in zip(self._support_matrix, self._training_devices):
            distance = float(np.linalg.norm(point - train) / math.sqrt(len(point)))
            by_device[device] = min(distance, by_device.get(device, math.inf))
        distances = sorted(by_device.values())
        score = distances[self.min_neighbor_devices - 1] if len(distances) >= self.min_neighbor_devices else None
        neighbors = sum(value <= self._ood_threshold for value in distances)
        if score is None or score > self._ood_threshold:
            return False, ("insufficient_local_device_support",), score, neighbors
        return True, (), score, neighbors

    def calibrate(self, rows: Sequence[CurveExample]) -> "FunctionalCurveDamageModel":
        if not self.is_fitted:
            raise RuntimeError("fit before calibration")
        if self._training_device_keys.intersection(row.physical_device_key for row in rows):
            raise CurveModelError("calibration devices overlap training devices")
        if self._training_session_keys.intersection(row.stress_session_key for row in rows):
            raise CurveModelError("calibration sessions overlap training sessions")
        residuals: dict[str, list[float]] = {}
        for row in rows:
            if self._validate_domain(row):
                raise CurveModelError(f"invalid calibration curve: {row.pair_key}")
            predicted, _ = self._raw_prediction(row)
            x, observed = _curve(row.post_x_v, row.post_i_a, "post")
            if x[0] > self._grid[0] or x[-1] < self._grid[-1]:
                continue
            observed_grid = np.interp(self._grid, x, observed)
            residuals.setdefault(row.physical_device_key, []).append(float(np.max(np.abs(observed_grid - predicted))))
        clustered = [max(values) for values in residuals.values()]
        if len(clustered) < self.min_calibration_devices:
            raise CurveModelError(
                f"too few independent calibration devices: {len(clustered)} < {self.min_calibration_devices}"
            )
        ordered = sorted(clustered)
        rank = math.ceil((len(ordered) + 1) * self.interval_coverage) - 1
        self._conformal_radius_a = float(ordered[min(rank, len(ordered) - 1)])
        return self

    def predict(self, request: CurveRequest) -> CurvePrediction:
        if not self.is_calibrated:
            raise RuntimeError("fit and calibrate before prediction")
        in_domain, reasons, score, neighbors = self._domain_assessment(request)
        if not in_domain:
            invalid = any(reason.startswith(("missing_", "wrong_", "prediction_horizon", "pre-curve")) for reason in reasons)
            return CurvePrediction((), (), (), (), (), False, "invalid_input" if invalid else "out_of_domain", reasons, score, self._ood_threshold, neighbors)
        predicted, _ = self._raw_prediction(request)
        pre = np.sinh(self._transformed_pre(request)) * self._current_scale
        radius = float(self._conformal_radius_a)
        return CurvePrediction(
            tuple(self._grid), tuple(pre), tuple(predicted),
            tuple(predicted - radius), tuple(predicted + radius),
            True, "decision_eligible", (), score, self._ood_threshold, neighbors,
        )

    def error_metrics(self, prediction: CurvePrediction, x_v: Sequence[float], i_a: Sequence[float]) -> CurveErrorMetrics:
        if not prediction.in_domain:
            raise CurveModelError("cannot evaluate an abstained prediction")
        x, observed = _curve(x_v, i_a, "observed")
        grid = np.asarray(prediction.x_v)
        if x[0] > grid[0] or x[-1] < grid[-1]:
            raise CurveModelError("observed curve does not cover prediction grid")
        truth = np.interp(grid, x, observed)
        estimate = np.asarray(prediction.predicted_i_a)
        residual = estimate - truth
        scale = max(float(np.ptp(truth)), float(np.max(np.abs(truth))), 1e-15)
        return CurveErrorMetrics(
            float(np.mean(np.abs(residual))), float(np.max(np.abs(residual))),
            float(np.sqrt(np.mean(residual ** 2)) / scale),
            bool(np.all((truth >= prediction.lower_i_a) & (truth <= prediction.upper_i_a))),
        )

    def artifact_manifest(self) -> Mapping[str, object]:
        if not self.is_calibrated:
            raise RuntimeError("only calibrated models have a complete manifest")
        return {
            "format_version": "iv-damage-functional-curve-v1",
            "claim_class": "learned_full_curve",
            "stress_type": self.stress_type,
            "curve_family": self.curve_family,
            "measurement_protocol_id": self.measurement_protocol_id,
            "required_features": self.required_features,
            "grid_v": self._grid.tolist(),
            "current_unit": "A",
            "current_transform": "asinh",
            "current_scale_a": self._current_scale,
            "pca_components": int(self._pca.n_components_),
            "interval_type": "physical-device-clustered simultaneous conformal band",
            "interval_coverage": self.interval_coverage,
            "conformal_radius_a": self._conformal_radius_a,
            "ood_threshold": self._ood_threshold,
            "known_categories": {key: sorted(values) for key, values in self._known_categories.items()},
        }
