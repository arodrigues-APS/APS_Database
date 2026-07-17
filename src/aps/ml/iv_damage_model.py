"""Calibrated, abstaining candidate models for prospective IV damage.

This module intentionally does not query the database or activate releases.
It implements a serializable estimator boundary that can be trained only from
request-time features, calibrated on independent physical devices, and made to
abstain when local training support is insufficient.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Mapping, Sequence

import numpy as np
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import HuberRegressor
from sklearn.preprocessing import RobustScaler

from aps.ml.iv_damage_policy import EvidenceStatus, ReferencePolicy
from aps.ml.iv_damage_readiness import DOMAIN_REQUIRED_FEATURES


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


@dataclass(frozen=True)
class DamageExample:
    response_unit_key: str
    physical_device_key: str
    stress_session_key: str
    stress_type: str
    target_type: str
    device_type: str
    observed_response: float
    features: Mapping[str, object]
    ion_species: str | None = None
    manufacturer: str | None = None


@dataclass(frozen=True)
class DamageRequest:
    stress_type: str
    target_type: str
    device_type: str
    features: Mapping[str, object]
    ion_species: str | None = None
    manufacturer: str | None = None
    reference_policy: str = ReferencePolicy.SAME_DEVICE


@dataclass(frozen=True)
class DomainAssessment:
    in_domain: bool
    reasons: tuple[str, ...]
    neighbor_distance: float | None = None
    neighbor_devices: int = 0


@dataclass(frozen=True)
class DamagePrediction:
    predicted_response: float | None
    interval_lower: float | None
    interval_upper: float | None
    in_domain: bool
    evidence_status: str
    reasons: tuple[str, ...]
    neighbor_distance: float | None = None
    neighbor_devices: int = 0


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float, np.integer, np.floating))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def validate_request_features(
    *, stress_type: str, features: Mapping[str, object]
) -> tuple[str, ...]:
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
        lower, upper = FEATURE_BOUNDS[name]
        number = float(value)
        if lower is not None and number <= lower:
            reasons.append(f"outside_physical_bounds:{name}")
        if upper is not None and number > upper:
            reasons.append(f"outside_physical_bounds:{name}")
    return tuple(reasons)


class CalibratedDamageModel:
    """Huber or Extra-Trees response model with local-support abstention."""

    ESTIMATOR_KINDS = frozenset({"huber", "extra_trees"})

    def __init__(
        self,
        *,
        stress_type: str,
        target_type: str,
        estimator_kind: str = "huber",
        interval_coverage: float = 0.80,
        ood_quantile: float = 0.95,
        min_neighbor_devices: int = 2,
        min_calibration_groups: int = 10,
        random_state: int = 0,
    ) -> None:
        if stress_type not in DOMAIN_REQUIRED_FEATURES:
            raise ValueError(f"unsupported stress_type: {stress_type}")
        if estimator_kind not in self.ESTIMATOR_KINDS:
            raise ValueError(f"unsupported estimator_kind: {estimator_kind}")
        if not 0.5 < interval_coverage < 1.0:
            raise ValueError("interval_coverage must be between 0.5 and 1")
        if not 0.5 <= ood_quantile < 1.0:
            raise ValueError("ood_quantile must be in [0.5, 1)")
        if min_neighbor_devices < 1:
            raise ValueError("min_neighbor_devices must be positive")
        self.stress_type = stress_type
        self.target_type = target_type
        self.estimator_kind = estimator_kind
        self.interval_coverage = interval_coverage
        self.ood_quantile = ood_quantile
        self.min_neighbor_devices = min_neighbor_devices
        self.min_calibration_groups = min_calibration_groups
        self.random_state = random_state
        self.required_features = tuple(sorted(DOMAIN_REQUIRED_FEATURES[stress_type]))
        self._vectorizer = DictVectorizer(sparse=False)
        self._scaler: RobustScaler | None = None
        self._estimator: HuberRegressor | ExtraTreesRegressor | None = None
        self._numeric_center: np.ndarray | None = None
        self._numeric_scale: np.ndarray | None = None
        self._training_numeric: np.ndarray | None = None
        self._training_devices: tuple[str, ...] = ()
        self._known_categories: dict[str, frozenset[str]] = {}
        self._ood_threshold: float | None = None
        self._conformal_radius: float | None = None
        self._training_device_keys: frozenset[str] = frozenset()
        self._training_session_keys: frozenset[str] = frozenset()
        self.training_groups = 0
        self.calibration_groups = 0

    @property
    def is_fitted(self) -> bool:
        return self._estimator is not None and self._ood_threshold is not None

    @property
    def is_calibrated(self) -> bool:
        return self.is_fitted and self._conformal_radius is not None

    def _categories(self, row: DamageExample | DamageRequest) -> dict[str, str]:
        categories = {"device_type": str(row.device_type).strip()}
        if row.manufacturer:
            categories["manufacturer"] = str(row.manufacturer).strip()
        if self.stress_type == "irradiation":
            categories["ion_species"] = str(row.ion_species or "").strip()
        return categories

    def _encoded_row(self, row: DamageExample | DamageRequest) -> dict[str, object]:
        encoded: dict[str, object] = {
            name: float(row.features[name]) for name in self.required_features
        }
        encoded.update({f"category={key}": value for key, value in self._categories(row).items()})
        return encoded

    def _numeric_row(self, row: DamageExample | DamageRequest) -> np.ndarray:
        return np.asarray([float(row.features[name]) for name in self.required_features])

    def _validate_training_rows(self, rows: Sequence[DamageExample]) -> None:
        if not rows:
            raise ValueError("training data must not be empty")
        keys = [row.response_unit_key for row in rows]
        if len(keys) != len(set(keys)):
            raise ValueError("response_unit_key must be unique; aggregate replicates first")
        for row in rows:
            if row.stress_type != self.stress_type or row.target_type != self.target_type:
                raise ValueError("training row does not match model domain")
            reasons = validate_request_features(
                stress_type=row.stress_type, features=row.features
            )
            if reasons:
                raise ValueError(f"invalid training features for {row.response_unit_key}: {reasons}")
            if not row.device_type.strip():
                raise ValueError("device_type is required")
            if self.stress_type == "irradiation" and not str(row.ion_species or "").strip():
                raise ValueError("ion_species is required for irradiation")
            if not _finite_number(row.observed_response):
                raise ValueError("observed_response must be finite")

    def _new_estimator(self) -> HuberRegressor | ExtraTreesRegressor:
        if self.estimator_kind == "huber":
            return HuberRegressor(epsilon=1.35, alpha=0.01, max_iter=1000)
        return ExtraTreesRegressor(
            n_estimators=300,
            min_samples_leaf=3,
            max_features=0.8,
            random_state=self.random_state,
            n_jobs=1,
        )

    def fit(self, training_rows: Sequence[DamageExample]) -> "CalibratedDamageModel":
        self._validate_training_rows(training_rows)
        device_keys = {row.physical_device_key for row in training_rows}
        if len(device_keys) < self.min_neighbor_devices + 1:
            raise ValueError(
                "training data has too few independent physical devices for OOD support"
            )
        encoded = [self._encoded_row(row) for row in training_rows]
        matrix = self._vectorizer.fit_transform(encoded)
        estimator = self._new_estimator()
        if self.estimator_kind == "huber":
            self._scaler = RobustScaler().fit(matrix)
            fit_matrix = self._scaler.transform(matrix)
        else:
            self._scaler = None
            fit_matrix = matrix
        estimator.fit(fit_matrix, [float(row.observed_response) for row in training_rows])
        self._estimator = estimator

        numeric = np.vstack([self._numeric_row(row) for row in training_rows])
        center = np.median(numeric, axis=0)
        q75, q25 = np.percentile(numeric, [75, 25], axis=0)
        robust_scale = (q75 - q25) / 1.349
        constant_tolerance = np.maximum(np.abs(center) * 0.01, 1e-9)
        scale = np.where(robust_scale > 0, robust_scale, constant_tolerance)
        standardized = (numeric - center) / scale
        devices = tuple(row.physical_device_key for row in training_rows)
        kth_distances: list[float] = []
        for index, point in enumerate(standardized):
            by_device: dict[str, float] = {}
            for other_index, other in enumerate(standardized):
                other_device = devices[other_index]
                if other_index == index or other_device == devices[index]:
                    continue
                distance = float(np.linalg.norm(point - other) / math.sqrt(len(point)))
                by_device[other_device] = min(distance, by_device.get(other_device, math.inf))
            ordered = sorted(by_device.values())
            kth_distances.append(ordered[self.min_neighbor_devices - 1])
        # A small margin prevents numerical boundary instability while retaining
        # a threshold derived solely from training-device neighborhoods.
        self._ood_threshold = float(
            np.quantile(kth_distances, self.ood_quantile, method="higher") * 1.05 + 1e-12
        )
        self._numeric_center = center
        self._numeric_scale = scale
        self._training_numeric = standardized
        self._training_devices = devices
        categories: dict[str, set[str]] = {}
        for row in training_rows:
            for name, value in self._categories(row).items():
                categories.setdefault(name, set()).add(value)
        self._known_categories = {
            name: frozenset(values) for name, values in categories.items()
        }
        self._training_device_keys = frozenset(device_keys)
        self._training_session_keys = frozenset(
            row.stress_session_key for row in training_rows
        )
        self.training_groups = len(training_rows)
        self.calibration_groups = 0
        self._conformal_radius = None
        return self

    def _point_prediction(self, row: DamageExample | DamageRequest) -> float:
        if not self.is_fitted:
            raise RuntimeError("model must be fitted before prediction")
        matrix = self._vectorizer.transform([self._encoded_row(row)])
        if self._scaler is not None:
            matrix = self._scaler.transform(matrix)
        return float(self._estimator.predict(matrix)[0])

    def assess_domain(self, request: DamageRequest) -> DomainAssessment:
        if not self.is_fitted:
            raise RuntimeError("model must be fitted before domain assessment")
        reasons = list(
            validate_request_features(
                stress_type=request.stress_type, features=request.features
            )
        )
        if request.stress_type != self.stress_type:
            reasons.append("wrong_stress_type")
        if request.target_type != self.target_type:
            reasons.append("wrong_target_type")
        categories = self._categories(request)
        if not categories.get("device_type"):
            reasons.append("missing_category:device_type")
        if self.stress_type == "irradiation" and not categories.get("ion_species"):
            reasons.append("missing_category:ion_species")
        for name, value in categories.items():
            if value and value not in self._known_categories.get(name, frozenset()):
                reasons.append(f"unseen_category:{name}")
        if reasons:
            return DomainAssessment(False, tuple(sorted(set(reasons))))

        point = (self._numeric_row(request) - self._numeric_center) / self._numeric_scale
        by_device: dict[str, float] = {}
        for train_point, device in zip(self._training_numeric, self._training_devices):
            distance = float(
                np.linalg.norm(point - train_point) / math.sqrt(len(self.required_features))
            )
            by_device[device] = min(distance, by_device.get(device, math.inf))
        distances = sorted(by_device.values())
        neighbor_devices = sum(value <= self._ood_threshold for value in distances)
        kth_distance = (
            distances[self.min_neighbor_devices - 1]
            if len(distances) >= self.min_neighbor_devices
            else None
        )
        if kth_distance is None or kth_distance > self._ood_threshold:
            return DomainAssessment(
                False,
                ("insufficient_local_device_support",),
                kth_distance,
                neighbor_devices,
            )
        return DomainAssessment(True, (), kth_distance, neighbor_devices)

    def calibrate(
        self, calibration_rows: Sequence[DamageExample]
    ) -> "CalibratedDamageModel":
        if not self.is_fitted:
            raise RuntimeError("fit training data before calibration")
        self._validate_training_rows(calibration_rows)
        overlap_devices = self._training_device_keys.intersection(
            row.physical_device_key for row in calibration_rows
        )
        overlap_sessions = self._training_session_keys.intersection(
            row.stress_session_key for row in calibration_rows
        )
        if overlap_devices or overlap_sessions:
            raise ValueError("calibration devices and sessions must be independent of training")
        residuals: list[float] = []
        for row in calibration_rows:
            request = DamageRequest(
                stress_type=row.stress_type,
                target_type=row.target_type,
                device_type=row.device_type,
                ion_species=row.ion_species,
                manufacturer=row.manufacturer,
                features=row.features,
            )
            if self.assess_domain(request).in_domain:
                residuals.append(abs(self._point_prediction(row) - row.observed_response))
        if len(residuals) < self.min_calibration_groups:
            raise ValueError(
                "too few independent in-domain calibration groups: "
                f"{len(residuals)} < {self.min_calibration_groups}"
            )
        ordered = sorted(residuals)
        rank = math.ceil((len(ordered) + 1) * self.interval_coverage) - 1
        self._conformal_radius = float(ordered[min(rank, len(ordered) - 1)])
        self.calibration_groups = len(residuals)
        return self

    def predict(self, request: DamageRequest) -> DamagePrediction:
        if not self.is_calibrated:
            raise RuntimeError("model must be fitted and calibrated before prediction")
        assessment = self.assess_domain(request)
        if not assessment.in_domain:
            invalid = any(
                reason.startswith(("missing_", "outside_physical_bounds", "wrong_"))
                for reason in assessment.reasons
            )
            status = EvidenceStatus.INVALID_INPUT if invalid else EvidenceStatus.OUT_OF_DOMAIN
            return DamagePrediction(
                None,
                None,
                None,
                False,
                status,
                assessment.reasons,
                assessment.neighbor_distance,
                assessment.neighbor_devices,
            )
        point = self._point_prediction(request)
        status = (
            EvidenceStatus.DECISION_ELIGIBLE
            if request.reference_policy == ReferencePolicy.SAME_DEVICE
            else EvidenceStatus.SCREENING_ONLY
        )
        reasons = () if status == EvidenceStatus.DECISION_ELIGIBLE else ("library_reference",)
        return DamagePrediction(
            point,
            point - self._conformal_radius,
            point + self._conformal_radius,
            True,
            status,
            reasons,
            assessment.neighbor_distance,
            assessment.neighbor_devices,
        )

    def artifact_manifest(self) -> Mapping[str, object]:
        if not self.is_calibrated:
            raise RuntimeError("only calibrated models have a complete artifact manifest")
        manifest = {
            "stress_type": self.stress_type,
            "target_type": self.target_type,
            "estimator_kind": self.estimator_kind,
            "required_features": self.required_features,
            "interval_coverage": self.interval_coverage,
            "ood_quantile": self.ood_quantile,
            "ood_threshold": self._ood_threshold,
            "min_neighbor_devices": self.min_neighbor_devices,
            "conformal_radius": self._conformal_radius,
            "training_groups": self.training_groups,
            "calibration_groups": self.calibration_groups,
            "known_categories": {
                name: sorted(values) for name, values in sorted(self._known_categories.items())
            },
        }
        payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
        return {**manifest, "manifest_sha256": hashlib.sha256(payload.encode()).hexdigest()}
