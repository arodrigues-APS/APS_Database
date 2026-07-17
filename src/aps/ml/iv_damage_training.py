"""Snapshot-driven training and grouped validation orchestration for V3."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, fields
import math
import platform
from pathlib import Path
import sys
from typing import Mapping, Sequence

import numpy as np
import sklearn
from psycopg2.extras import Json

from aps.ml.iv_damage_model import (
    CalibratedDamageModel,
    DamageExample,
    DamageRequest,
)
from aps.ml.iv_damage_operations import save_model_artifact
from aps.ml.iv_damage_policy import (
    AcceptancePolicy,
    ValidationEvidence,
    evaluate_release,
)
from aps.ml.iv_damage_readiness import (
    EvidenceUnit,
    ReadinessRequirements,
    assess_readiness,
)
from aps.ml.iv_damage_validation import (
    BaselinePredictor,
    FoldAssignment,
    PredictionRecord,
    SPLIT_SCHEMES,
    ValidationMetrics,
    ValidationUnit,
    assert_no_group_leakage,
    evaluate_predictions,
)
from aps.paths import REPO_ROOT


class DamageTrainingError(RuntimeError):
    """Raised before an invalid dataset can produce a persisted model run."""


@dataclass(frozen=True)
class SnapshotExample:
    response_unit_id: int
    damage: DamageExample
    validation: ValidationUnit
    evidence: EvidenceUnit
    campaign_key: str
    split_role: str
    fold_number: int | None


@dataclass(frozen=True)
class PartitionPrediction:
    response_unit_id: int
    response_unit_key: str
    observed: float
    predicted: float | None
    lower: float | None
    upper: float | None
    residual: float | None
    abs_residual: float | None
    interval_hit: bool | None
    support_status: str
    ood_score: float | None
    support_reasons: tuple[str, ...]
    baseline_predictions: Mapping[str, float]


@dataclass(frozen=True)
class PartitionEvaluation:
    model: CalibratedDamageModel
    metrics: ValidationMetrics
    best_baseline: str
    baseline_maes: Mapping[str, float]
    predictions: tuple[PartitionPrediction, ...]


@dataclass(frozen=True)
class TrainingRunResult:
    model_run_id: int
    model_version: str
    release_gate_eligible: bool
    release_gate_reasons: tuple[str, ...]
    artifact_path: str
    artifact_checksum: str
    external_metrics: ValidationMetrics
    grouped_metrics: Mapping[str, ValidationMetrics]


BASELINE_STRATEGIES = (
    "zero",
    "global_median",
    "device_median",
    "device_ion_median",
)


def _request(example: DamageExample) -> DamageRequest:
    return DamageRequest(
        stress_type=example.stress_type,
        target_type=example.target_type,
        device_type=example.device_type,
        manufacturer=example.manufacturer,
        ion_species=example.ion_species,
        features=example.features,
    )


def evaluate_partition(
    training: Sequence[SnapshotExample],
    calibration: Sequence[SnapshotExample],
    test: Sequence[SnapshotExample],
    *,
    stress_type: str,
    target_type: str,
    estimator_kind: str,
    interval_coverage: float,
    ood_quantile: float,
    min_neighbor_devices: int,
    min_calibration_groups: int,
    catastrophic_error_threshold: float | None,
    random_state: int,
) -> PartitionEvaluation:
    if not training or not calibration or not test:
        raise DamageTrainingError("training, calibration, and held-out partitions are required")
    model = CalibratedDamageModel(
        stress_type=stress_type,
        target_type=target_type,
        estimator_kind=estimator_kind,
        interval_coverage=interval_coverage,
        ood_quantile=ood_quantile,
        min_neighbor_devices=min_neighbor_devices,
        min_calibration_groups=min_calibration_groups,
        random_state=random_state,
    )
    model.fit([row.damage for row in training])
    model.calibrate([row.damage for row in calibration])
    baselines = {
        name: BaselinePredictor(name).fit([row.validation for row in training]) for name in BASELINE_STRATEGIES
    }
    raw: list[tuple[SnapshotExample, object, dict[str, float]]] = []
    for row in test:
        prediction = model.predict(_request(row.damage))
        baseline_values = {name: baseline.predict(row.validation) for name, baseline in baselines.items()}
        raw.append((row, prediction, baseline_values))

    supported = [item for item in raw if item[1].predicted_response is not None]
    baseline_maes = {
        name: (
            sum(abs(values[name] - row.damage.observed_response) for row, _, values in supported) / len(supported)
            if supported
            else math.inf
        )
        for name in BASELINE_STRATEGIES
    }
    best_baseline = min(BASELINE_STRATEGIES, key=lambda name: (baseline_maes[name], name))
    records: list[PredictionRecord] = []
    persisted: list[PartitionPrediction] = []
    for row, prediction, baseline_values in raw:
        point = prediction.predicted_response
        residual = point - row.damage.observed_response if point is not None else None
        absolute = abs(residual) if residual is not None else None
        hit = (
            prediction.interval_lower <= row.damage.observed_response <= prediction.interval_upper
            if prediction.interval_lower is not None and prediction.interval_upper is not None
            else None
        )
        records.append(
            PredictionRecord(
                response_unit_key=row.damage.response_unit_key,
                observed=row.damage.observed_response,
                predicted=point,
                baseline_predicted=baseline_values[best_baseline],
                interval_lower=prediction.interval_lower,
                interval_upper=prediction.interval_upper,
                supported=point is not None,
            )
        )
        persisted.append(
            PartitionPrediction(
                response_unit_id=row.response_unit_id,
                response_unit_key=row.damage.response_unit_key,
                observed=row.damage.observed_response,
                predicted=point,
                lower=prediction.interval_lower,
                upper=prediction.interval_upper,
                residual=residual,
                abs_residual=absolute,
                interval_hit=hit,
                support_status="in_domain" if point is not None else str(prediction.evidence_status),
                ood_score=prediction.neighbor_distance,
                support_reasons=prediction.reasons,
                baseline_predictions=baseline_values,
            )
        )
    metrics = evaluate_predictions(records, catastrophic_error_limit=catastrophic_error_threshold)
    return PartitionEvaluation(
        model=model,
        metrics=metrics,
        best_baseline=best_baseline,
        baseline_maes=baseline_maes,
        predictions=tuple(persisted),
    )


def _policy(policy_version: str, approved: bool, requirements: Mapping[str, object]) -> AcceptancePolicy:
    names = {field.name for field in fields(AcceptancePolicy)} - {"policy_version", "approved"}
    values = {name: requirements[name] for name in names if name in requirements}
    return AcceptancePolicy(policy_version=policy_version, approved=approved, **values)


def _readiness_requirements(policy: AcceptancePolicy, requirements: Mapping[str, object]) -> ReadinessRequirements:
    return ReadinessRequirements(
        min_independent_groups=int(
            requirements.get(
                "min_independent_groups",
                policy.min_training_groups
                + policy.min_external_groups
                + int(requirements.get("min_calibration_groups", 10)),
            )
        ),
        min_physical_devices=int(requirements.get("min_physical_devices", 10)),
        min_campaigns=policy.min_campaigns,
        min_external_groups=policy.min_external_groups,
        min_calibration_groups=int(requirements.get("min_calibration_groups", 10)),
        min_replicates=int(requirements.get("min_replicates", 2)),
        max_campaign_share=float(requirements.get("max_campaign_share", 0.5)),
        required_feature_fraction=1.0,
    )


def _snapshot_rows(
    conn, *, snapshot_id: int, split_scheme: str, stress_type: str, target_type: str
) -> list[SnapshotExample]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT unit.id, unit.unit_key, unit.physical_device_key,
                   unit.stress_session_key, unit.stress_type, unit.target_type,
                   unit.device_type, unit.manufacturer, unit.campaign_key, unit.run_key,
                   unit.ion_species, unit.measurement_protocol_id, unit.response_value,
                   unit.response_uncertainty, unit.pre_replicate_count,
                   unit.post_replicate_count, unit.reference_policy,
                   unit.baseline_reference_group_key, unit.stress_features,
                   unit.pre_value, unit.quality_status,
                   assignment.split_role, assignment.fold_number
            FROM iv_damage_split_assignments assignment
            JOIN iv_damage_response_units unit ON unit.id = assignment.response_unit_id
            WHERE assignment.dataset_snapshot_id = %s
              AND assignment.split_scheme = %s
              AND unit.stress_type = %s
              AND unit.target_type = %s
            ORDER BY unit.unit_key
            """,
            (snapshot_id, split_scheme, stress_type, target_type),
        )
        result = []
        for row in cursor.fetchall():
            stress_features = dict(row[18])
            stress_features["pre_value"] = float(row[19])
            damage = DamageExample(
                response_unit_key=row[1],
                physical_device_key=row[2],
                stress_session_key=row[3],
                stress_type=row[4],
                target_type=row[5],
                device_type=row[6],
                manufacturer=row[7],
                ion_species=row[10],
                observed_response=float(row[12]),
                features=stress_features,
            )
            validation = ValidationUnit(
                response_unit_key=row[1],
                physical_device_key=row[2],
                stress_session_key=row[3],
                target_type=row[5],
                observed_response=float(row[12]),
                run_key=row[9],
                campaign_key=row[8],
                ion_species=row[10],
                baseline_reference_group_key=row[17],
                device_type=row[6],
                stress_condition_key=str(stress_features.get("stress_condition_key") or "") or None,
            )
            evidence = EvidenceUnit(
                unit_key=row[1],
                physical_device_key=row[2],
                stress_session_key=row[3],
                stress_type=row[4],
                target_type=row[5],
                device_type=row[6],
                campaign_key=row[8],
                run_key=row[9],
                ion_species=row[10],
                measurement_protocol_id=row[11],
                response_value=float(row[12]),
                response_uncertainty=row[13],
                replicate_count=min(int(row[14]), int(row[15])),
                split_role=row[21],
                features=stress_features,
                quality_status=row[20],
            )
            result.append(SnapshotExample(int(row[0]), damage, validation, evidence, row[8], row[21], row[22]))
        return result
    finally:
        cursor.close()


def _assert_role_isolation(rows: Sequence[SnapshotExample]) -> None:
    role_fold = {"train": 0, "calibration": 1, "external_test": 2}
    assignments = [
        FoldAssignment(row.damage.response_unit_key, role_fold[row.split_role], row.split_role) for row in rows
    ]
    assert_no_group_leakage([row.validation for row in rows], assignments, "leave_device")


def _grouped_cv(
    rows: Sequence[SnapshotExample],
    *,
    split_scheme: str,
    model_kwargs: Mapping[str, object],
) -> tuple[ValidationMetrics, tuple[tuple[int, PartitionPrediction], ...]]:
    if split_scheme not in SPLIT_SCHEMES:
        raise DamageTrainingError(f"required grouped scheme is unsupported: {split_scheme}")
    folds = sorted({row.fold_number for row in rows if row.fold_number is not None})
    if len(folds) < 3:
        raise DamageTrainingError(f"{split_scheme} requires at least three folds")
    assignments = [FoldAssignment(row.damage.response_unit_key, int(row.fold_number), split_scheme) for row in rows]
    assert_no_group_leakage([row.validation for row in rows], assignments, split_scheme)
    predictions: list[tuple[int, PartitionPrediction]] = []
    records: list[PredictionRecord] = []
    for index, test_fold in enumerate(folds):
        calibration_fold = folds[(index + 1) % len(folds)]
        training = [row for row in rows if row.fold_number not in {test_fold, calibration_fold}]
        calibration = [row for row in rows if row.fold_number == calibration_fold]
        test = [row for row in rows if row.fold_number == test_fold]
        evaluation = evaluate_partition(training, calibration, test, **model_kwargs, random_state=int(test_fold))
        for prediction in evaluation.predictions:
            predictions.append((int(test_fold), prediction))
            records.append(
                PredictionRecord(
                    prediction.response_unit_key,
                    prediction.observed,
                    prediction.predicted,
                    prediction.baseline_predictions[evaluation.best_baseline],
                    prediction.lower,
                    prediction.upper,
                    supported=prediction.predicted is not None,
                )
            )
    metrics = evaluate_predictions(
        records,
        catastrophic_error_limit=model_kwargs["catastrophic_error_threshold"],
    )
    return metrics, tuple(predictions)


def _metrics_meet_policy(metrics: ValidationMetrics, policy: AcceptancePolicy) -> bool:
    limits = (
        policy.max_median_abs_error,
        policy.max_p90_abs_error,
        policy.max_abs_bias,
        policy.max_catastrophic_error_rate,
        policy.max_mean_interval_width,
    )
    if any(value is None for value in limits):
        return False
    return all(
        (
            metrics.supported_fraction >= policy.min_supported_fraction,
            metrics.baseline_improvement is not None
            and metrics.baseline_improvement >= policy.min_baseline_improvement_fraction,
            metrics.median_absolute_error is not None and metrics.median_absolute_error <= policy.max_median_abs_error,
            metrics.p90_absolute_error is not None and metrics.p90_absolute_error <= policy.max_p90_abs_error,
            metrics.bias is not None and abs(metrics.bias) <= policy.max_abs_bias,
            metrics.catastrophic_error_rate is not None
            and metrics.catastrophic_error_rate <= policy.max_catastrophic_error_rate,
            metrics.interval_coverage is not None
            and policy.min_interval_coverage <= metrics.interval_coverage <= policy.max_interval_coverage,
            metrics.mean_interval_width is not None and metrics.mean_interval_width <= policy.max_mean_interval_width,
        )
    )


def train_snapshot_candidate(
    conn,
    *,
    snapshot_version: str,
    policy_version: str,
    model_version: str,
    stress_type: str,
    target_type: str,
    estimator_kind: str = "huber",
    release_split_scheme: str = "frozen_release",
    code_sha: str,
    artifact_directory: Path | None = None,
) -> TrainingRunResult:
    """Train, validate, persist, and leave a candidate awaiting explicit release."""

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM iv_damage_model_runs WHERE model_version = %s", (model_version,))
        if cursor.fetchone() is not None:
            raise DamageTrainingError(f"model_version already exists: {model_version}")
        cursor.execute(
            """
            SELECT snapshot.id, policy.id, policy.approved, policy.requirements
            FROM iv_damage_dataset_snapshots snapshot
            CROSS JOIN iv_damage_acceptance_policies policy
            WHERE snapshot.snapshot_version = %s AND policy.policy_version = %s
              AND policy.stress_type = %s AND policy.target_type = %s
            """,
            (snapshot_version, policy_version, stress_type, target_type),
        )
        identity = cursor.fetchone()
    finally:
        cursor.close()
    if identity is None:
        raise DamageTrainingError("snapshot/policy/domain combination does not exist")
    snapshot_id, policy_id, approved, requirement_values = identity
    requirements = dict(requirement_values)
    acceptance = _policy(policy_version, bool(approved), requirements)
    rows = _snapshot_rows(
        conn,
        snapshot_id=int(snapshot_id),
        split_scheme=release_split_scheme,
        stress_type=stress_type,
        target_type=target_type,
    )
    if not rows:
        raise DamageTrainingError("release split contains no response units")
    readiness = assess_readiness(
        [row.evidence for row in rows],
        stress_type=stress_type,
        target_type=target_type,
        requirements=_readiness_requirements(acceptance, requirements),
    )
    if readiness.status != "model_ready":
        raise DamageTrainingError("evidence readiness failed: " + ", ".join(readiness.blockers))
    _assert_role_isolation(rows)
    training = [row for row in rows if row.split_role == "train"]
    calibration = [row for row in rows if row.split_role == "calibration"]
    external = [row for row in rows if row.split_role == "external_test"]
    catastrophic_threshold = requirements.get("catastrophic_error_threshold")
    if catastrophic_threshold is None:
        raise DamageTrainingError("policy requires catastrophic_error_threshold")
    model_kwargs = {
        "stress_type": stress_type,
        "target_type": target_type,
        "estimator_kind": estimator_kind,
        "interval_coverage": float(requirements.get("interval_coverage", 0.8)),
        "ood_quantile": float(requirements.get("ood_quantile", 0.95)),
        "min_neighbor_devices": int(requirements.get("min_neighbor_devices", 2)),
        "min_calibration_groups": int(requirements.get("min_calibration_groups", 10)),
        "catastrophic_error_threshold": float(catastrophic_threshold),
    }
    external_evaluation = evaluate_partition(training, calibration, external, **model_kwargs, random_state=0)

    required_schemes = tuple(
        requirements.get(
            "required_grouped_schemes",
            ("leave_device", "leave_condition", "leave_campaign"),
        )
    )
    grouped_metrics: dict[str, ValidationMetrics] = {}
    grouped_predictions: dict[str, tuple[tuple[int, PartitionPrediction], ...]] = {}
    for scheme in required_schemes:
        scheme_rows = _snapshot_rows(
            conn,
            snapshot_id=int(snapshot_id),
            split_scheme=scheme,
            stress_type=stress_type,
            target_type=target_type,
        )
        if len(scheme_rows) != len(rows):
            raise DamageTrainingError(f"{scheme} assignments do not cover the frozen release population")
        metrics, predictions = _grouped_cv(scheme_rows, split_scheme=scheme, model_kwargs=model_kwargs)
        grouped_metrics[scheme] = metrics
        grouped_predictions[scheme] = predictions

    external_metrics = external_evaluation.metrics
    subgroup_counts = Counter((row.damage.device_type, row.damage.ion_species or "none") for row in external)
    required_schemes_pass = all(_metrics_meet_policy(metrics, acceptance) for metrics in grouped_metrics.values())
    evidence = ValidationEvidence(
        training_groups=len(training),
        external_groups=len(external),
        campaigns=len({row.campaign_key for row in rows}),
        smallest_released_subgroup_groups=min(subgroup_counts.values(), default=0),
        supported_fraction=external_metrics.supported_fraction,
        median_abs_error=external_metrics.median_absolute_error or math.inf,
        p90_abs_error=external_metrics.p90_absolute_error or math.inf,
        abs_bias=abs(external_metrics.bias) if external_metrics.bias is not None else math.inf,
        candidate_mae=external_metrics.mae or math.inf,
        best_baseline_mae=external_metrics.baseline_mae or math.inf,
        interval_coverage=external_metrics.interval_coverage or 0.0,
        mean_interval_width=external_metrics.mean_interval_width or math.inf,
        catastrophic_error_rate=external_metrics.catastrophic_error_rate or 0.0,
        external_test_passed=required_schemes_pass,
        required_features_complete=readiness.checks["required_features"],
        leakage_checks_passed=True,
    )
    gate = evaluate_release(evidence, acceptance)
    artifact_dir = artifact_directory or (REPO_ROOT / "out" / "iv_damage_models")
    artifact = save_model_artifact(
        external_evaluation.model,
        artifact_dir / f"{model_version}.joblib",
        metadata={
            "model_version": model_version,
            "snapshot_version": snapshot_version,
            "snapshot_id": int(snapshot_id),
            "policy_version": policy_version,
            "code_sha": code_sha,
            "release_split_scheme": release_split_scheme,
            "required_grouped_schemes": required_schemes,
        },
    )
    try:
        relative_artifact = str(artifact.path.relative_to(REPO_ROOT))
    except ValueError:
        relative_artifact = str(artifact.path)
    validation_metrics = {
        "release_gate_eligible": gate.eligible,
        "release_gate_checks": gate.checks,
        "release_gate_reasons": gate.reasons,
        "readiness": asdict(readiness),
        "external": asdict(external_metrics),
        "grouped": {name: asdict(metrics) for name, metrics in grouped_metrics.items()},
        "best_baseline": external_evaluation.best_baseline,
        "baseline_maes": external_evaluation.baseline_maes,
    }
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "numpy": np.__version__,
        "scikit_learn": sklearn.__version__,
    }
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO iv_damage_model_runs (
                model_version, model_name, stress_type, target_type,
                dataset_snapshot_id, acceptance_policy_id, algorithm,
                feature_schema, model_config, released_domain, validation_metrics,
                code_sha, environment_fingerprint, artifact_path, artifact_checksum,
                release_status, validated_at
            ) VALUES (
                %s, 'iv_damage_v3', %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, CASE WHEN %s THEN clock_timestamp() ELSE NULL END
            ) RETURNING id
            """,
            (
                model_version,
                stress_type,
                target_type,
                snapshot_id,
                policy_id,
                estimator_kind,
                Json({"required_features": external_evaluation.model.required_features}),
                Json(model_kwargs),
                Json({"stress_type": stress_type, "target_type": target_type}),
                Json(validation_metrics),
                code_sha,
                Json(environment),
                relative_artifact,
                artifact.checksum,
                "validated" if gate.eligible else "candidate",
                gate.eligible,
            ),
        )
        model_run_id = int(cursor.fetchone()[0])

        def persist(scheme: str, role: str, fold: int | None, prediction: PartitionPrediction) -> None:
            cursor.execute(
                """
                INSERT INTO iv_damage_validation_results (
                    model_run_id, response_unit_id, split_scheme, fold_number,
                    split_role, group_key, observed_value, predicted_value,
                    predicted_lower, predicted_upper, baseline_predictions,
                    residual, abs_residual, interval_hit, support_status,
                    ood_score, support_reasons
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    model_run_id,
                    prediction.response_unit_id,
                    scheme,
                    fold,
                    role,
                    prediction.response_unit_key,
                    prediction.observed,
                    prediction.predicted,
                    prediction.lower,
                    prediction.upper,
                    Json(dict(prediction.baseline_predictions)),
                    prediction.residual,
                    prediction.abs_residual,
                    prediction.interval_hit,
                    prediction.support_status,
                    prediction.ood_score,
                    list(prediction.support_reasons),
                ),
            )

        for prediction in external_evaluation.predictions:
            persist(release_split_scheme, "external_test", None, prediction)
        for scheme, values in grouped_predictions.items():
            for fold, prediction in values:
                persist(scheme, "external_test", fold, prediction)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
    return TrainingRunResult(
        model_run_id,
        model_version,
        gate.eligible,
        gate.reasons,
        relative_artifact,
        artifact.checksum,
        external_metrics,
        grouped_metrics,
    )


def readiness_from_database(
    conn,
    *,
    stress_type: str,
    target_type: str,
    requirements: ReadinessRequirements | None = None,
):
    """Report readiness across persisted response units without mutating state."""

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, unit_key, physical_device_key, stress_session_key,
                   stress_type, target_type, device_type, campaign_key, run_key,
                   measurement_protocol_id, response_value, response_uncertainty,
                   LEAST(pre_replicate_count, post_replicate_count), ion_species,
                   stress_features || jsonb_build_object('pre_value', pre_value),
                   quality_status
            FROM iv_damage_response_units
            WHERE stress_type = %s AND target_type = %s
            ORDER BY unit_key
            """,
            (stress_type, target_type),
        )
        units = [
            EvidenceUnit(
                unit_key=row[1],
                physical_device_key=row[2],
                stress_session_key=row[3],
                stress_type=row[4],
                target_type=row[5],
                device_type=row[6],
                campaign_key=row[7],
                run_key=row[8],
                measurement_protocol_id=row[9],
                response_value=row[10],
                response_uncertainty=row[11],
                replicate_count=row[12],
                ion_species=row[13],
                features=dict(row[14]),
                quality_status=row[15],
                split_role="unassigned",
            )
            for row in cursor.fetchall()
        ]
    finally:
        cursor.close()
    pool_requirements = requirements or ReadinessRequirements(
        min_external_groups=0,
        min_calibration_groups=0,
    )
    return assess_readiness(
        units,
        stress_type=stress_type,
        target_type=target_type,
        requirements=pool_requirements,
    )
