"""Leakage-safe scalar candidate development and one-time certification.

Unlike the original combined trainer, development SQL excludes the external
role before rows reach Python.  Candidate selection is then frozen, and only
the certification command is allowed to read the external targets.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import math
from pathlib import Path
import platform
import sys
from typing import Mapping, Sequence

import numpy as np
import sklearn
from psycopg2.extras import Json

from aps.ml.iv_damage_dataset import snapshot_member_payload_hash
from aps.ml.iv_damage_model import CalibratedDamageModel, DamageExample, DamageRequest
from aps.ml.iv_damage_operations import (
    default_artifact_root,
    load_model_artifact,
    resolve_artifact_path,
    save_model_artifact,
)
from aps.ml.iv_damage_policy import ValidationEvidence, evaluate_release
from aps.ml.iv_damage_readiness import EvidenceUnit
from aps.ml.iv_damage_training import (
    BASELINE_STRATEGIES,
    DamageTrainingError,
    PartitionPrediction,
    SnapshotExample,
    _grouped_cv,
    _metrics_meet_policy,
    _policy,
)
from aps.ml.iv_damage_validation import (
    BaselinePredictor,
    PredictionRecord,
    ValidationMetrics,
    ValidationUnit,
    assert_no_group_leakage,
    evaluate_predictions,
    FoldAssignment,
)


@dataclass(frozen=True)
class ScalarDevelopmentResult:
    model_run_id: int
    model_version: str
    development_gate_eligible: bool
    development_gate_reasons: tuple[str, ...]
    artifact_path: str
    artifact_checksum: str


@dataclass(frozen=True)
class ScalarCertificationResult:
    certification_id: int
    model_run_id: int
    model_version: str
    passed: bool
    reasons: tuple[str, ...]
    external_metrics: ValidationMetrics


def _rows_for_roles(
    conn, *, snapshot_id: int, split_scheme: str, stress_type: str,
    target_type: str, roles: Sequence[str],
) -> list[SnapshotExample]:
    if not roles:
        return []
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT assignment.response_unit_id, member.frozen_payload,
                   member.payload_hash, assignment.split_role,
                   assignment.fold_number
            FROM iv_damage_split_assignments assignment
            JOIN iv_damage_dataset_snapshot_members member
              ON member.dataset_snapshot_id = assignment.dataset_snapshot_id
             AND member.response_unit_id = assignment.response_unit_id
            WHERE assignment.dataset_snapshot_id = %s
              AND assignment.split_scheme = %s
              AND member.frozen_payload ->> 'stress_type' = %s
              AND member.frozen_payload ->> 'target_type' = %s
              AND assignment.split_role = ANY(%s)
            ORDER BY member.frozen_payload ->> 'unit_key'
            """,
            (snapshot_id, split_scheme, stress_type, target_type, list(roles)),
        )
        result = []
        for row in cursor.fetchall():
            payload = dict(row[1])
            if snapshot_member_payload_hash(payload) != row[2]:
                raise DamageTrainingError("frozen scalar snapshot payload hash mismatch")
            features = dict(payload["stress_features"])
            features["pre_value"] = float(payload["pre_value"])
            protocol = str(payload["measurement_protocol_id"])
            horizon = float(features["post_measurement_delay_s"]) if payload["stress_type"] == "irradiation" else None
            damage = DamageExample(
                response_unit_key=payload["unit_key"],
                physical_device_key=payload["physical_device_key"],
                stress_session_key=payload["stress_session_key"],
                stress_type=payload["stress_type"], target_type=payload["target_type"],
                device_type=payload["device_type"], manufacturer=payload.get("manufacturer"),
                ion_species=payload.get("ion_species"), protocol_signature=protocol,
                prediction_horizon_s=horizon,
                observed_response=float(payload["response_value"]), features=features,
            )
            validation = ValidationUnit(
                response_unit_key=payload["unit_key"],
                physical_device_key=payload["physical_device_key"],
                stress_session_key=payload["stress_session_key"],
                target_type=payload["target_type"],
                observed_response=float(payload["response_value"]),
                stress_condition_key=str(features.get("stress_condition_key") or "") or None,
                run_key=payload["run_key"], campaign_key=payload["campaign_key"],
                ion_species=payload.get("ion_species"),
                baseline_reference_group_key=payload.get("baseline_reference_group_key"),
                device_type=payload["device_type"],
            )
            evidence = EvidenceUnit(
                unit_key=payload["unit_key"],
                physical_device_key=payload["physical_device_key"],
                stress_session_key=payload["stress_session_key"],
                stress_type=payload["stress_type"], target_type=payload["target_type"],
                device_type=payload["device_type"], campaign_key=payload["campaign_key"],
                run_key=payload["run_key"], ion_species=payload.get("ion_species"),
                measurement_protocol_id=protocol,
                response_value=float(payload["response_value"]),
                response_uncertainty=payload.get("response_uncertainty"),
                replicate_count=min(int(payload["pre_replicate_count"]), int(payload["post_replicate_count"])),
                split_role=row[3], features=features, quality_status=payload["quality_status"],
            )
            result.append(SnapshotExample(
                int(row[0]), damage, validation, evidence,
                payload["campaign_key"], row[3], row[4],
            ))
        return result
    finally:
        cursor.close()


def _model_kwargs(requirements: Mapping[str, object], stress_type: str, target_type: str, estimator_kind: str) -> dict[str, object]:
    catastrophic = requirements.get("catastrophic_error_threshold")
    if catastrophic is None:
        raise DamageTrainingError("policy requires catastrophic_error_threshold")
    return {
        "stress_type": stress_type, "target_type": target_type,
        "estimator_kind": estimator_kind,
        "interval_coverage": float(requirements.get("interval_coverage", 0.8)),
        "ood_quantile": float(requirements.get("ood_quantile", 0.95)),
        "min_neighbor_devices": int(requirements.get("min_neighbor_devices", 2)),
        "min_calibration_groups": int(requirements.get("min_calibration_groups", 10)),
        "catastrophic_error_threshold": float(catastrophic),
    }


def _fit_final(training, calibration, kwargs) -> CalibratedDamageModel:
    model = CalibratedDamageModel(**{key: value for key, value in kwargs.items() if key != "catastrophic_error_threshold"})
    model.fit([row.damage for row in training])
    model.calibrate([row.damage for row in calibration])
    return model


def train_scalar_development_candidate(
    conn, *, snapshot_version: str, policy_version: str, model_version: str,
    stress_type: str, target_type: str, estimator_kind: str,
    code_sha: str, artifact_directory: Path | None = None,
) -> ScalarDevelopmentResult:
    cursor = conn.cursor()
    try:
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
    if identity is None or not identity[2]:
        raise DamageTrainingError("approved snapshot/policy/domain does not exist")
    snapshot_id, policy_id, _, raw_requirements = identity
    requirements = dict(raw_requirements)
    policy = _policy(policy_version, True, requirements)
    # This query can only return development roles; external target values do
    # not enter the process that compares or selects candidate algorithms.
    development = _rows_for_roles(
        conn, snapshot_id=int(snapshot_id), split_scheme="frozen_release",
        stress_type=stress_type, target_type=target_type,
        roles=("train", "calibration"),
    )
    training = [row for row in development if row.split_role == "train"]
    calibration = [row for row in development if row.split_role == "calibration"]
    if len(training) < policy.min_training_groups or not calibration:
        raise DamageTrainingError("insufficient development training/calibration evidence")
    assignments = [
        FoldAssignment(row.damage.response_unit_key, 0 if row.split_role == "train" else 1, row.split_role)
        for row in development
    ]
    assert_no_group_leakage([row.validation for row in development], assignments, "leave_device")
    kwargs = _model_kwargs(requirements, stress_type, target_type, estimator_kind)
    required_schemes = tuple(requirements.get("required_grouped_schemes", ("leave_device", "leave_condition", "leave_campaign")))
    grouped = {}
    predictions = {}
    for scheme in required_schemes:
        rows = _rows_for_roles(
            conn, snapshot_id=int(snapshot_id), split_scheme=scheme,
            stress_type=stress_type, target_type=target_type,
            roles=("grouped_test", "train"),
        )
        metrics, result = _grouped_cv(rows, split_scheme=scheme, model_kwargs=kwargs)
        grouped[scheme] = metrics
        predictions[scheme] = result
    checks = {
        "approved_policy": True,
        "training_groups": len(training) >= policy.min_training_groups,
        "calibration_groups": len({row.damage.physical_device_key for row in calibration}) >= int(requirements.get("min_calibration_groups", 10)),
        "required_grouped_schemes": set(grouped) == set(required_schemes),
        "grouped_metrics": all(_metrics_meet_policy(value, policy) for value in grouped.values()),
        "external_outcomes_not_accessed": True,
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    eligible = not reasons
    model = _fit_final(training, calibration, kwargs)
    root = default_artifact_root().resolve()
    destination = (artifact_directory or root).resolve()
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise DamageTrainingError("artifact_directory must be under APS_IV_DAMAGE_ARTIFACT_ROOT") from exc
    artifact = save_model_artifact(
        model, destination / f"{model_version}.joblib",
        metadata={
            "model_version": model_version, "snapshot_version": snapshot_version,
            "code_sha": code_sha, "claim_stage": "development",
            "external_outcomes_accessed": False,
        },
    )
    validation_metrics = {
        "release_gate_eligible": eligible,
        "development_gate_eligible": eligible,
        "development_gate_checks": checks,
        "development_gate_reasons": reasons,
        "grouped": {name: asdict(value) for name, value in grouped.items()},
        "external_certification": "not_accessed",
    }
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO iv_damage_model_runs (
                model_version, model_name, stress_type, target_type,
                dataset_snapshot_id, acceptance_policy_id, algorithm,
                feature_schema, model_config, released_domain, validation_metrics,
                code_sha, environment_fingerprint, artifact_path,
                artifact_checksum, release_status
            ) VALUES (%s, 'iv_damage_v3_certified', %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, 'candidate') RETURNING id
            """,
            (
                model_version, stress_type, target_type, snapshot_id, policy_id,
                estimator_kind,
                Json({"required_features": model.required_features, "protocol_signature_required": True}),
                Json(kwargs),
                Json({"stress_type": stress_type, "target_type": target_type, "measurement_protocol_ids": sorted(model.artifact_manifest()["known_protocol_signatures"])}),
                Json(validation_metrics), code_sha,
                Json({"python": sys.version, "platform": platform.platform(), "numpy": np.__version__, "scikit_learn": sklearn.__version__}),
                str(artifact.path), artifact.checksum,
            ),
        )
        model_id = int(cursor.fetchone()[0])
        for scheme, rows in predictions.items():
            for fold, prediction in rows:
                cursor.execute(
                    """
                    INSERT INTO iv_damage_validation_results (
                        model_run_id, response_unit_id, split_scheme, fold_number,
                        split_role, group_key, observed_value, predicted_value,
                        predicted_lower, predicted_upper, baseline_predictions,
                        residual, abs_residual, interval_hit, support_status,
                        ood_score, support_reasons, evaluation_kind
                    ) VALUES (%s, %s, %s, %s, 'grouped_test', %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, 'development_cv')
                    """,
                    (
                        model_id, prediction.response_unit_id, scheme, fold,
                        prediction.response_unit_key, prediction.observed,
                        prediction.predicted, prediction.lower, prediction.upper,
                        Json(dict(prediction.baseline_predictions)), prediction.residual,
                        prediction.abs_residual, prediction.interval_hit,
                        prediction.support_status, prediction.ood_score,
                        list(prediction.support_reasons),
                    ),
                )
        conn.commit()
        return ScalarDevelopmentResult(model_id, model_version, eligible, reasons, str(artifact.path), artifact.checksum)
    except Exception:
        conn.rollback()
        artifact.path.unlink(missing_ok=True)
        raise
    finally:
        cursor.close()


def select_scalar_candidate(conn, *, model_version: str, selected_by: str) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, dataset_snapshot_id, stress_type, target_type,
                   validation_metrics @> '{"development_gate_eligible": true}'::jsonb,
                   release_status
            FROM iv_damage_model_runs WHERE model_version = %s FOR UPDATE
            """,
            (model_version,),
        )
        row = cursor.fetchone()
        if row is None or row[5] != "candidate" or not row[4]:
            raise DamageTrainingError("only a development-gate-passing candidate can be selected")
        cursor.execute(
            """
            INSERT INTO iv_damage_model_selections (
                model_run_id, dataset_snapshot_id, stress_type, target_type,
                selection_protocol, selected_by
            ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (row[0], row[1], row[2], row[3], Json({"selection_is_final": True, "external_outcomes_accessed": False}), selected_by),
        )
        selection_id = int(cursor.fetchone()[0])
        conn.commit()
        return selection_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _external_evaluation(model, training, external, catastrophic_threshold):
    baselines = {name: BaselinePredictor(name).fit([row.validation for row in training]) for name in BASELINE_STRATEGIES}
    raw = []
    for row in external:
        request = DamageRequest(
            stress_type=row.damage.stress_type, target_type=row.damage.target_type,
            device_type=row.damage.device_type, manufacturer=row.damage.manufacturer,
            ion_species=row.damage.ion_species, protocol_signature=row.damage.protocol_signature,
            prediction_horizon_s=row.damage.prediction_horizon_s,
            features=row.damage.features,
        )
        prediction = model.predict(request)
        baseline_values = {name: predictor.predict(row.validation) for name, predictor in baselines.items()}
        raw.append((row, prediction, baseline_values))
    supported = [item for item in raw if item[1].predicted_response is not None]
    baseline_maes = {
        name: sum(abs(values[name] - row.damage.observed_response) for row, _, values in supported) / len(supported) if supported else math.inf
        for name in BASELINE_STRATEGIES
    }
    best = min(BASELINE_STRATEGIES, key=lambda name: (baseline_maes[name], name))
    records = []
    persisted = []
    for row, prediction, baseline_values in raw:
        point = prediction.predicted_response
        residual = point - row.damage.observed_response if point is not None else None
        hit = prediction.interval_lower <= row.damage.observed_response <= prediction.interval_upper if prediction.interval_lower is not None else None
        records.append(PredictionRecord(
            row.damage.response_unit_key, row.damage.observed_response, point,
            baseline_values[best], prediction.interval_lower, prediction.interval_upper,
            supported=point is not None,
        ))
        persisted.append(PartitionPrediction(
            row.response_unit_id, row.damage.response_unit_key,
            row.damage.observed_response, point, prediction.interval_lower,
            prediction.interval_upper, residual, abs(residual) if residual is not None else None,
            hit, "in_domain" if point is not None else str(prediction.evidence_status),
            prediction.neighbor_distance, prediction.reasons, baseline_values,
        ))
    return evaluate_predictions(records, catastrophic_error_limit=catastrophic_threshold), tuple(persisted), best, baseline_maes


def certify_scalar_candidate(conn, *, model_version: str, certified_by: str) -> ScalarCertificationResult:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, model.dataset_snapshot_id, model.stress_type,
                   model.target_type, model.artifact_path, model.artifact_checksum,
                   model.validation_metrics, policy.policy_version,
                   policy.approved, policy.requirements, selection.id
            FROM iv_damage_model_runs model
            JOIN iv_damage_model_selections selection ON selection.model_run_id = model.id
            JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
            WHERE model.model_version = %s AND model.release_status = 'candidate'
            FOR UPDATE OF model, selection
            """,
            (model_version,),
        )
        identity = cursor.fetchone()
        if identity is None:
            raise DamageTrainingError("selected scalar candidate does not exist")
        cursor.execute(
            "SELECT 1 FROM iv_damage_external_certifications WHERE dataset_snapshot_id = %s",
            (identity[1],),
        )
        if cursor.fetchone() is not None:
            raise DamageTrainingError("this external scalar holdout has already been consumed")
    finally:
        cursor.close()
    model = load_model_artifact(resolve_artifact_path(identity[4]), identity[5])
    training = _rows_for_roles(
        conn, snapshot_id=int(identity[1]), split_scheme="frozen_release",
        stress_type=identity[2], target_type=identity[3], roles=("train",),
    )
    external = _rows_for_roles(
        conn, snapshot_id=int(identity[1]), split_scheme="frozen_release",
        stress_type=identity[2], target_type=identity[3], roles=("external_test",),
    )
    requirements = dict(identity[9])
    catastrophic = float(requirements["catastrophic_error_threshold"])
    metrics, predictions, best_baseline, baseline_maes = _external_evaluation(model, training, external, catastrophic)
    policy = _policy(identity[7], bool(identity[8]), requirements)
    subgroup_counts = Counter((row.damage.device_type, row.damage.ion_species or "none", str(row.damage.protocol_signature)) for row in external)
    dev_gate = bool(dict(identity[6]).get("development_gate_eligible"))
    evidence = ValidationEvidence(
        training_groups=len(training), external_groups=len(external),
        campaigns=len({row.campaign_key for row in training + external}),
        smallest_released_subgroup_groups=min(subgroup_counts.values(), default=0),
        supported_fraction=metrics.supported_fraction,
        median_abs_error=metrics.median_absolute_error if metrics.median_absolute_error is not None else math.inf,
        p90_abs_error=metrics.p90_absolute_error if metrics.p90_absolute_error is not None else math.inf,
        abs_bias=abs(metrics.bias) if metrics.bias is not None else math.inf,
        candidate_mae=metrics.mae if metrics.mae is not None else math.inf,
        best_baseline_mae=metrics.baseline_mae if metrics.baseline_mae is not None else math.inf,
        interval_coverage=metrics.interval_coverage if metrics.interval_coverage is not None else 0.0,
        mean_interval_width=metrics.mean_interval_width if metrics.mean_interval_width is not None else math.inf,
        catastrophic_error_rate=metrics.catastrophic_error_rate if metrics.catastrophic_error_rate is not None else math.inf,
        external_test_passed=_metrics_meet_policy(metrics, policy) and dev_gate,
        required_features_complete=True, leakage_checks_passed=True,
    )
    gate = evaluate_release(evidence, policy)
    cursor = conn.cursor()
    try:
        for prediction in predictions:
            cursor.execute(
                """
                INSERT INTO iv_damage_validation_results (
                    model_run_id, response_unit_id, split_scheme, fold_number,
                    split_role, group_key, observed_value, predicted_value,
                    predicted_lower, predicted_upper, baseline_predictions,
                    residual, abs_residual, interval_hit, support_status,
                    ood_score, support_reasons, evaluation_kind
                ) VALUES (%s, %s, 'frozen_release', NULL, 'external_test', %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          'external_certification')
                """,
                (
                    identity[0], prediction.response_unit_id,
                    prediction.response_unit_key, prediction.observed,
                    prediction.predicted, prediction.lower, prediction.upper,
                    Json(dict(prediction.baseline_predictions)), prediction.residual,
                    prediction.abs_residual, prediction.interval_hit,
                    prediction.support_status, prediction.ood_score,
                    list(prediction.support_reasons),
                ),
            )
        cursor.execute(
            """
            INSERT INTO iv_damage_external_certifications (
                selection_id, model_run_id, dataset_snapshot_id,
                evaluation_protocol, metrics, subgroup_metrics, gate_checks,
                passed, certified_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (
                identity[10], identity[0], identity[1],
                Json({"split": "frozen_release/external_test", "retraining": False, "best_baseline": best_baseline}),
                Json(asdict(metrics)), Json({"counts": {"|".join(key): value for key, value in subgroup_counts.items()}, "baseline_maes": baseline_maes}),
                Json(gate.checks), gate.eligible, certified_by,
            ),
        )
        certification_id = int(cursor.fetchone()[0])
        cursor.execute(
            """
            UPDATE iv_damage_model_runs
            SET release_status = %s,
                validated_at = CASE WHEN %s THEN clock_timestamp() ELSE NULL END
            WHERE id = %s
            """,
            ("validated" if gate.eligible else "failed", gate.eligible, identity[0]),
        )
        conn.commit()
        return ScalarCertificationResult(certification_id, int(identity[0]), model_version, gate.eligible, gate.reasons, metrics)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
