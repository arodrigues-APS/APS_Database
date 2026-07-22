"""Leakage-safe grouped scalar benchmarks for retrospective ΔVth research."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import math
from statistics import median
from typing import Mapping, Sequence

import numpy as np
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import HuberRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from aps.ml.iv_damage_research_contracts import (
    AuditedPair,
    ResearchContractError,
    ScalarOOFPrediction,
    SplitAssignment,
    validate_feature_names,
)


DEFAULT_FEATURES = (
    "pre_vth_v",
    "device_type",
    "manufacturer",
    "ion_species",
    "beam_energy_mev",
    "let_surface",
    "range_um",
    "beam_type",
    "fluence_missing",
)


@dataclass(frozen=True)
class ScalarRunResult:
    method: str
    validation_scheme: str
    predictions: tuple[ScalarOOFPrediction, ...]
    metrics: Mapping[str, object]
    fold_manifests: tuple[Mapping[str, object], ...]
    feature_names: tuple[str, ...]
    estimator_config: Mapping[str, object]


def _clean_category(value: object) -> str:
    return str(value).strip() if value is not None and str(value).strip() else "__missing__"


def feature_record(pair: AuditedPair, feature_names: Sequence[str] = DEFAULT_FEATURES) -> dict[str, object]:
    names = validate_feature_names(feature_names)
    source = pair.candidate
    values: dict[str, object] = {
        "pre_vth_v": pair.pre_vth_v,
        "device_type": source.device_type,
        "manufacturer": source.manufacturer,
        "ion_species": source.ion_species,
        "beam_energy_mev": source.beam_energy_mev,
        "let_surface": source.let_surface,
        "range_um": source.range_um,
        "beam_type": source.beam_type,
        "fluence_missing": source.fluence is None,
    }
    result = {}
    for name in names:
        value = values.get(name)
        if isinstance(value, str) or name in {"device_type", "manufacturer", "ion_species", "beam_type"}:
            result[name] = _clean_category(value)
        elif isinstance(value, bool):
            result[name] = float(value)
        elif value is None:
            result[f"{name}_missing"] = 1.0
            result[name] = 0.0
        else:
            number = float(value)
            if not math.isfinite(number):
                raise ResearchContractError(f"nonfinite feature {name}")
            result[name] = number
            result[f"{name}_missing"] = 0.0
    return result


def _model(method: str, seed: int):
    if method == "huber":
        return Pipeline(
            [
                ("vectorizer", DictVectorizer(sparse=False, sort=True)),
                ("scaler", RobustScaler()),
                ("regressor", HuberRegressor(epsilon=1.35, alpha=0.01, max_iter=500)),
            ]
        ), {"epsilon": 1.35, "alpha": 0.01, "max_iter": 500}
    if method == "extra_trees":
        config = {"n_estimators": 300, "min_samples_leaf": 2, "max_features": 0.75, "random_state": seed, "n_jobs": 1}
        return Pipeline(
            [
                ("vectorizer", DictVectorizer(sparse=False, sort=True)),
                ("regressor", ExtraTreesRegressor(**config)),
            ]
        ), config
    raise ResearchContractError(f"unsupported learned scalar method: {method}")


def _v2_pair_record(row: AuditedPair) -> dict[str, object]:
    pair = row.candidate
    return {
        "id": pair.source_pair_id,
        "pair_key": pair.pair_key,
        "split_group": pair.physical_device_key,
        "stress_type": "irradiation",
        "target_type": "delta_vth_v",
        "reference_tier": "strict_pre_irrad",
        "device_type": pair.device_type,
        "ion_species": pair.ion_species,
        "irrad_run_id": pair.run_key,
        "beam_energy_mev": pair.beam_energy_mev,
        "let_surface": pair.let_surface,
        "let_bragg_peak": None,
        "range_um": pair.range_um,
        "fluence_at_meas": pair.fluence,
        "delta_vth_v": row.observed_delta_vth_v,
    }


def _v2_fold_safe_prediction(training: Sequence[AuditedPair], held: AuditedPair) -> tuple[float | None, str | None]:
    """Run V2's weighted-median donor scorer on outer-training rows only."""
    from aps.ml import ml_post_iv_physical_prediction as v2

    training_records = [_v2_pair_record(row) for row in training]
    config = v2.feature_config(v2.fit_feature_scales(training_records), training_records)
    prediction = v2.predict_from_donors(
        _v2_pair_record(held),
        training_records,
        config,
        validation_mode="within_condition",
    )
    if prediction.get("support_status") != "ok":
        return None, str(prediction.get("unsupported_reason") or "v2_donor_unsupported")
    return float(prediction["predicted_value"]), None


def _p90(values: Sequence[float]) -> float | None:
    return float(np.quantile(values, 0.9, method="higher")) if values else None


def scalar_metrics(predictions: Sequence[ScalarOOFPrediction], devices: Mapping[str, str]) -> dict[str, object]:
    supported = [
        row for row in predictions if row.support_status == "supported" and row.predicted_delta_vth_v is not None
    ]
    by_device: dict[str, list[float]] = defaultdict(list)
    residuals = []
    squared = []
    for row in supported:
        error = float(row.predicted_delta_vth_v) - row.observed_delta_vth_v
        residuals.append(error)
        squared.append(error * error)
        by_device[devices[row.pair_key]].append(abs(error))
    device_mae = [sum(values) / len(values) for values in by_device.values()]
    return {
        "supported_pairs": len(supported),
        "abstained_pairs": len(predictions) - len(supported),
        "supported_devices": len(by_device),
        "device_macro_mae_v": sum(device_mae) / len(device_mae) if device_mae else None,
        "device_macro_median_absolute_error_v": median(device_mae) if device_mae else None,
        "device_macro_p90_absolute_error_v": _p90(device_mae),
        "pair_weighted_bias_v": sum(residuals) / len(residuals) if residuals else None,
        "pair_weighted_rmse_v": math.sqrt(sum(squared) / len(squared)) if squared else None,
        "catastrophic_error_count_0_2v": sum(abs(value) >= 0.2 for value in residuals),
        "denominator": "physical-device macro; pair rows retained for transparency",
    }


def run_grouped_scalar_benchmark(
    pairs: Sequence[AuditedPair],
    assignments: Sequence[SplitAssignment],
    *,
    method: str,
    validation_scheme: str,
    feature_names: Sequence[str] = DEFAULT_FEATURES,
    seed: int = 17,
) -> ScalarRunResult:
    admitted = {row.candidate.pair_key: row for row in pairs if row.admitted}
    selected = [row for row in assignments if row.validation_scheme == validation_scheme]
    if set(admitted) != {row.pair_key for row in selected}:
        raise ResearchContractError("split assignments must cover every admitted pair exactly once")
    names = validate_feature_names(feature_names)
    predictions = []
    manifests = []
    estimator_config: Mapping[str, object] = {"strategy": method}
    for fold in sorted({row.fold_number for row in selected}):
        held_keys = {row.pair_key for row in selected if row.fold_number == fold}
        training = [row for key, row in admitted.items() if key not in held_keys]
        held = [admitted[key] for key in sorted(held_keys)]
        training_devices = tuple(sorted({row.candidate.physical_device_key for row in training}))
        held_devices = {row.candidate.physical_device_key for row in held}
        if held_devices.intersection(training_devices):
            raise ResearchContractError("held-out physical device appears in outer-fold training rows")
        device_hash = hashlib.sha256("\n".join(training_devices).encode()).hexdigest()
        held_group = sorted({row.held_out_group_key for row in selected if row.fold_number == fold})
        manifests.append(
            {
                "fold_number": fold,
                "held_out_group_keys": held_group,
                "training_device_keys": list(training_devices),
                "training_device_hash": device_hash,
            }
        )

        fitted = None
        if method in {"huber", "extra_trees"}:
            fitted, estimator_config = _model(method, seed)
            fitted.fit(
                [feature_record(row, names) for row in training], [float(row.observed_delta_vth_v) for row in training]
            )
        for row in held:
            assignment = next(item for item in selected if item.pair_key == row.candidate.pair_key)
            predicted = None
            reasons: tuple[str, ...] = ()
            if method == "zero_damage":
                predicted = 0.0
            elif method == "v2_donor":
                predicted, reason = _v2_fold_safe_prediction(training, row)
                if predicted is None:
                    reasons = (reason or "v2_donor_unsupported",)
            else:
                try:
                    predicted = float(fitted.predict([feature_record(row, names)])[0])
                except (ValueError, FloatingPointError) as exc:
                    reasons = (f"prediction_failed:{type(exc).__name__}",)
            predictions.append(
                ScalarOOFPrediction(
                    pair_key=row.candidate.pair_key,
                    validation_scheme=validation_scheme,
                    fold_number=fold,
                    held_out_group_key=assignment.held_out_group_key,
                    observed_delta_vth_v=float(row.observed_delta_vth_v),
                    predicted_delta_vth_v=predicted,
                    training_device_keys=training_devices,
                    support_status="supported" if predicted is not None else "abstained",
                    support_reasons=reasons,
                )
            )
    predictions.sort(key=lambda row: row.pair_key)
    devices = {key: row.candidate.physical_device_key for key, row in admitted.items()}
    return ScalarRunResult(
        method,
        validation_scheme,
        tuple(predictions),
        scalar_metrics(predictions, devices),
        tuple(manifests),
        names,
        dict(estimator_config),
    )


def preference_decision(results: Sequence[ScalarRunResult]) -> dict[str, object]:
    """Apply the conservative research preference rule without release semantics."""
    by_method = {row.method: row for row in results if row.validation_scheme == "leave_device"}
    zero = by_method.get("zero_damage")
    donor = by_method.get("v2_donor")
    if zero is None or donor is None:
        return {"preferred_method": None, "reason": "zero and V2 benchmarks are required"}
    zero_mae = zero.metrics.get("device_macro_mae_v")
    donor_mae = donor.metrics.get("device_macro_mae_v")
    eligible = []
    for method in ("huber", "extra_trees"):
        result = by_method.get(method)
        if result is None:
            continue
        mae = result.metrics.get("device_macro_mae_v")
        p90 = result.metrics.get("device_macro_p90_absolute_error_v")
        zero_p90 = zero.metrics.get("device_macro_p90_absolute_error_v")
        if (
            None not in (mae, zero_mae, donor_mae)
            and mae < zero_mae
            and mae <= donor_mae * 1.05
            and (zero_p90 is None or p90 <= zero_p90 * 1.10)
        ):
            eligible.append((float(mae), method))
    if not eligible:
        return {"preferred_method": None, "reason": "no learned model satisfied the predeclared research rule"}
    return {"preferred_method": min(eligible)[1], "reason": "lowest eligible leave-device device-macro MAE"}
