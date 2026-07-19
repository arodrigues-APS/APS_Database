"""Frozen-dataset training, sealed certification, and scoring for full curves."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import sys
from typing import Mapping, Sequence
from uuid import uuid4

import joblib
import numpy as np
import sklearn
from psycopg2.extras import Json, execute_values

from aps.ml.iv_damage_curve_model import (
    CurveErrorMetrics,
    CurveExample,
    CurvePrediction,
    CurveRequest,
    FunctionalCurveDamageModel,
)
from aps.ml.iv_damage_operations import default_artifact_root, sha256_file
from aps.ml.iv_damage_policy import (
    ClaimPolicyError,
    validate_curve_claim_requirements,
)


class CurveTrainingError(RuntimeError):
    """A functional curve lifecycle invariant was not satisfied."""


@dataclass(frozen=True)
class FrozenCurveExample:
    response_unit_id: int
    curve_response_pair_id: int
    split_role: str
    fold_number: int | None
    campaign_key: str
    example: CurveExample


@dataclass(frozen=True)
class CurveEvaluation:
    total_curves: int
    supported_curves: int
    physical_devices: int
    supported_fraction: float
    mean_mae_a: float | None
    median_max_abs_error_a: float | None
    p90_max_abs_error_a: float | None
    mean_normalized_rmse: float | None
    simultaneous_band_coverage: float | None


@dataclass(frozen=True)
class EvaluatedCurve:
    row: FrozenCurveExample
    prediction: CurvePrediction
    metrics: CurveErrorMetrics | None


@dataclass(frozen=True)
class CurveTrainingResult:
    curve_model_run_id: int
    model_version: str
    development_gate_eligible: bool
    development_gate_reasons: tuple[str, ...]
    artifact_path: str
    artifact_checksum: str


@dataclass(frozen=True)
class CurveCertificationResult:
    certification_id: int
    curve_model_run_id: int
    model_version: str
    passed: bool
    reasons: tuple[str, ...]
    metrics: CurveEvaluation


@dataclass(frozen=True)
class CurveScoreBatch:
    selected_requests: int
    inserted_predictions: int
    decision_eligible_predictions: int
    abstentions: int


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _payload_hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def freeze_curve_snapshot_members(conn, *, snapshot_version: str) -> int:
    """Freeze every paired curve belonging to an already-frozen scalar unit."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id, row_count FROM iv_damage_dataset_snapshots WHERE snapshot_version = %s FOR SHARE",
            (snapshot_version,),
        )
        snapshot = cursor.fetchone()
        if snapshot is None:
            raise CurveTrainingError("dataset snapshot does not exist")
        snapshot_id, declared_rows = int(snapshot[0]), int(snapshot[1])
        cursor.execute(
            """
            SELECT member.response_unit_id, member.frozen_payload,
                   pair.id, pair.pair_key, pair.curve_family,
                   pair.measurement_protocol_id,
                   pre.point_payload_hash, post.point_payload_hash,
                   (SELECT jsonb_agg(jsonb_build_array(point.x_value, point.i_drain_a)
                                     ORDER BY point.point_index)
                      FROM iv_damage_curve_snapshot_points point
                     WHERE point.curve_snapshot_id = pair.pre_curve_snapshot_id),
                   (SELECT jsonb_agg(jsonb_build_array(point.x_value, point.i_drain_a)
                                     ORDER BY point.point_index)
                      FROM iv_damage_curve_snapshot_points point
                     WHERE point.curve_snapshot_id = pair.post_curve_snapshot_id)
            FROM iv_damage_dataset_snapshot_members member
            JOIN iv_damage_curve_response_pairs pair
              ON pair.response_unit_id = member.response_unit_id
             AND pair.quality_status = 'usable'
            JOIN iv_damage_curve_snapshots pre ON pre.id = pair.pre_curve_snapshot_id
            JOIN iv_damage_curve_snapshots post ON post.id = pair.post_curve_snapshot_id
            WHERE member.dataset_snapshot_id = %s
            ORDER BY member.response_unit_id
            """,
            (snapshot_id,),
        )
        rows = cursor.fetchall()
        if len(rows) != declared_rows:
            raise CurveTrainingError(
                "full-curve snapshot requires one usable curve pair for every scalar snapshot member"
            )
        values = []
        for row in rows:
            response = dict(row[1])
            pre_points = [[float(point[0]), float(point[1])] for point in row[8]]
            post_points = [[float(point[0]), float(point[1])] for point in row[9]]
            payload = {
                "format_version": "iv-damage-curve-snapshot-v1",
                "response": response,
                "response_unit_id": int(row[0]),
                "curve_response_pair_id": int(row[2]),
                "pair_key": row[3],
                "curve_family": row[4],
                "measurement_protocol_id": row[5],
                "pre_point_payload_hash": row[6],
                "post_point_payload_hash": row[7],
                "pre_points": pre_points,
                "post_points": post_points,
            }
            values.append((snapshot_id, int(row[2]), int(row[0]), Json(payload), _payload_hash(payload)))
        execute_values(
            cursor,
            """
            INSERT INTO iv_damage_curve_snapshot_members (
                dataset_snapshot_id, curve_response_pair_id, response_unit_id,
                frozen_payload, payload_hash
            ) VALUES %s ON CONFLICT (dataset_snapshot_id, curve_response_pair_id) DO NOTHING
            """,
            values,
        )
        cursor.execute(
            "SELECT count(*) FROM iv_damage_curve_snapshot_members WHERE dataset_snapshot_id = %s",
            (snapshot_id,),
        )
        count = int(cursor.fetchone()[0])
        if count != declared_rows:
            raise CurveTrainingError("curve snapshot member population is incomplete")
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _example(payload: Mapping[str, object], role: str, fold: int | None) -> FrozenCurveExample:
    response = dict(payload["response"])
    features = dict(response["stress_features"])
    features["pre_value"] = float(response["pre_value"])
    pre = list(payload["pre_points"])
    post = list(payload["post_points"])
    horizon = float(features["post_measurement_delay_s"]) if response["stress_type"] == "irradiation" else None
    curve = CurveExample(
        pair_key=str(payload["pair_key"]),
        physical_device_key=response["physical_device_key"],
        stress_session_key=response["stress_session_key"],
        stress_type=response["stress_type"],
        curve_family=str(payload["curve_family"]),
        measurement_protocol_id=str(payload["measurement_protocol_id"]),
        device_type=response["device_type"],
        manufacturer=response.get("manufacturer"),
        ion_species=response.get("ion_species"),
        features=features,
        prediction_horizon_s=horizon,
        pre_x_v=[float(point[0]) for point in pre],
        pre_i_a=[float(point[1]) for point in pre],
        post_x_v=[float(point[0]) for point in post],
        post_i_a=[float(point[1]) for point in post],
    )
    return FrozenCurveExample(
        int(payload["response_unit_id"]),
        int(payload["curve_response_pair_id"]), role,
        fold, response["campaign_key"], curve,
    )


def _snapshot_rows(
    conn,
    *,
    snapshot_id: int,
    split_scheme: str,
    stress_type: str,
    curve_family: str,
    measurement_protocol_id: str,
    include_external: bool,
) -> list[FrozenCurveExample]:
    # The role predicate is part of SQL so development processes never fetch
    # the sealed external outcomes into memory.
    role_predicate = "" if include_external else "AND assignment.split_role <> 'external_test'"
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT member.frozen_payload, member.payload_hash,
                   assignment.split_role, assignment.fold_number
            FROM iv_damage_curve_snapshot_members member
            JOIN iv_damage_split_assignments assignment
              ON assignment.dataset_snapshot_id = member.dataset_snapshot_id
             AND assignment.response_unit_id = member.response_unit_id
            WHERE member.dataset_snapshot_id = %s
              AND assignment.split_scheme = %s
              AND member.frozen_payload #>> '{{response,stress_type}}' = %s
              AND member.frozen_payload ->> 'curve_family' = %s
              AND member.frozen_payload ->> 'measurement_protocol_id' = %s
              {role_predicate}
            ORDER BY member.curve_response_pair_id
            """,
            (snapshot_id, split_scheme, stress_type, curve_family, measurement_protocol_id),
        )
        result = []
        for payload_value, expected_hash, role, fold in cursor.fetchall():
            payload = dict(payload_value)
            if _payload_hash(payload) != expected_hash:
                raise CurveTrainingError("frozen curve snapshot payload hash mismatch")
            result.append(_example(payload, role, fold))
        return result
    finally:
        cursor.close()


def _curve_model_kwargs(requirements: Mapping[str, object]) -> dict[str, object]:
    return {
        "grid_points": int(requirements["curve_grid_points"]),
        "pca_components": int(requirements["curve_pca_components"]),
        "ridge_alpha": float(requirements["curve_ridge_alpha"]),
        "interval_coverage": float(requirements["curve_interval_coverage"]),
        "ood_quantile": float(requirements.get("ood_quantile", 0.95)),
        "min_neighbor_devices": int(requirements.get("min_neighbor_devices", 2)),
        "min_calibration_devices": int(requirements.get("min_calibration_groups", 10)),
    }


def _curve_requirements(value: Mapping[str, object]) -> dict[str, object]:
    requirements = dict(value)
    try:
        validate_curve_claim_requirements(requirements, required=True)
    except ClaimPolicyError as exc:
        raise CurveTrainingError(str(exc)) from exc
    return requirements


def _fit_model(
    training: Sequence[FrozenCurveExample],
    calibration: Sequence[FrozenCurveExample],
    *, stress_type: str, curve_family: str, protocol: str,
    requirements: Mapping[str, object],
) -> FunctionalCurveDamageModel:
    model = FunctionalCurveDamageModel(
        stress_type=stress_type, curve_family=curve_family,
        measurement_protocol_id=protocol, **_curve_model_kwargs(requirements),
    )
    model.fit([row.example for row in training])
    model.calibrate([row.example for row in calibration])
    return model


def _request(row: CurveExample) -> CurveRequest:
    return CurveRequest(
        stress_type=row.stress_type, curve_family=row.curve_family,
        measurement_protocol_id=row.measurement_protocol_id,
        device_type=row.device_type, manufacturer=row.manufacturer,
        ion_species=row.ion_species, features=row.features,
        prediction_horizon_s=row.prediction_horizon_s,
        pre_x_v=row.pre_x_v, pre_i_a=row.pre_i_a,
    )


def _evaluate(model: FunctionalCurveDamageModel, rows: Sequence[FrozenCurveExample]) -> tuple[CurveEvaluation, tuple[EvaluatedCurve, ...]]:
    evaluated = []
    for row in rows:
        prediction = model.predict(_request(row.example))
        metrics = model.error_metrics(prediction, row.example.post_x_v, row.example.post_i_a) if prediction.in_domain else None
        evaluated.append(EvaluatedCurve(row, prediction, metrics))
    supported_rows = [row for row in evaluated if row.metrics is not None]
    supported = [row.metrics for row in supported_rows]
    maximums = sorted(row.max_abs_error_a for row in supported)
    quantile = None
    if maximums:
        position = int(math.ceil(0.9 * len(maximums)) - 1)
        quantile = maximums[max(0, min(position, len(maximums) - 1))]
    return CurveEvaluation(
        len(rows), len(supported),
        len({row.row.example.physical_device_key for row in supported_rows}),
        len(supported) / len(rows) if rows else 0.0,
        float(np.mean([row.mae_a for row in supported])) if supported else None,
        float(np.median(maximums)) if supported else None,
        quantile,
        float(np.mean([row.normalized_rmse for row in supported])) if supported else None,
        float(np.mean([row.simultaneous_band_hit for row in supported])) if supported else None,
    ), tuple(evaluated)


def _development_gate(metrics: Mapping[str, CurveEvaluation], requirements: Mapping[str, object]) -> tuple[bool, dict[str, bool], tuple[str, ...]]:
    required = tuple(requirements.get("required_grouped_schemes", ("leave_device", "leave_condition", "leave_campaign")))
    checks = {
        "required_grouped_schemes": all(name in metrics for name in required),
        "supported_fraction": all(
            value.supported_fraction >= float(requirements.get("min_supported_fraction", 0.8))
            for value in metrics.values()
        ),
        "development_curves": all(
            value.supported_curves >= int(requirements["curve_min_development_curves"])
            for value in metrics.values()
        ),
        "development_devices": all(
            value.physical_devices >= int(requirements["curve_min_development_devices"])
            for value in metrics.values()
        ),
        "curve_mae": all(
            value.mean_mae_a is not None
            and value.mean_mae_a <= float(requirements["curve_max_mean_mae_a"])
            for value in metrics.values()
        ),
        "curve_p90_max_error": all(
            value.p90_max_abs_error_a is not None
            and value.p90_max_abs_error_a <= float(requirements["curve_max_p90_error_a"])
            for value in metrics.values()
        ),
        "curve_normalized_rmse": all(
            value.mean_normalized_rmse is not None
            and value.mean_normalized_rmse <= float(requirements["curve_max_normalized_rmse"])
            for value in metrics.values()
        ),
        "curve_simultaneous_band_coverage": all(
            value.simultaneous_band_coverage is not None
            and value.simultaneous_band_coverage
            >= float(requirements["curve_min_band_coverage"])
            for value in metrics.values()
        ),
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    return not reasons, checks, reasons


def _save_artifact(model: FunctionalCurveDamageModel, path: Path, metadata: Mapping[str, object]) -> tuple[str, str]:
    destination = path.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        joblib.dump({
            "format_version": "iv-damage-functional-curve-artifact-v1",
            "model": model, "manifest": dict(model.artifact_manifest()),
            "metadata": dict(metadata),
        }, temporary, compress=3)
        try:
            os.link(temporary, destination)
        except FileExistsError as exc:
            raise CurveTrainingError(f"immutable curve artifact already exists: {destination}") from exc
    finally:
        if temporary.exists():
            temporary.unlink()
    return str(destination), sha256_file(destination)


def load_curve_artifact(path: Path, checksum: str) -> FunctionalCurveDamageModel:
    if not path.is_file() or sha256_file(path) != checksum:
        raise CurveTrainingError("curve artifact is missing or checksum does not match")
    payload = joblib.load(path)
    if not isinstance(payload, dict) or payload.get("format_version") != "iv-damage-functional-curve-artifact-v1":
        raise CurveTrainingError("unsupported curve artifact format")
    model = payload.get("model")
    if not isinstance(model, FunctionalCurveDamageModel) or not model.is_calibrated:
        raise CurveTrainingError("artifact does not contain a calibrated functional model")
    return model


def train_curve_candidate(
    conn, *, snapshot_version: str, policy_version: str, model_version: str,
    stress_type: str, curve_family: str, measurement_protocol_id: str,
    code_sha: str, artifact_directory: Path | None = None,
) -> CurveTrainingResult:
    """Develop a candidate without selecting any external-holdout row."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT snapshot.id, policy.id, policy.approved, policy.requirements
            FROM iv_damage_dataset_snapshots snapshot
            CROSS JOIN iv_damage_acceptance_policies policy
            WHERE snapshot.snapshot_version = %s AND policy.policy_version = %s
              AND policy.stress_type = %s
            """,
            (snapshot_version, policy_version, stress_type),
        )
        identity = cursor.fetchone()
    finally:
        cursor.close()
    if identity is None or not identity[2]:
        raise CurveTrainingError("approved snapshot/policy combination does not exist")
    snapshot_id, policy_id, _, requirements_value = identity
    requirements = _curve_requirements(requirements_value)
    release_rows = _snapshot_rows(
        conn, snapshot_id=int(snapshot_id), split_scheme="frozen_release",
        stress_type=stress_type, curve_family=curve_family,
        measurement_protocol_id=measurement_protocol_id, include_external=False,
    )
    training = [row for row in release_rows if row.split_role == "train"]
    calibration = [row for row in release_rows if row.split_role == "calibration"]
    if not training or not calibration:
        raise CurveTrainingError("development training and calibration curves are required")
    required_schemes = tuple(requirements.get("required_grouped_schemes", ("leave_device", "leave_condition", "leave_campaign")))
    grouped: dict[str, CurveEvaluation] = {}
    persisted: list[tuple[str, int, EvaluatedCurve]] = []
    for scheme in required_schemes:
        rows = _snapshot_rows(
            conn, snapshot_id=int(snapshot_id), split_scheme=scheme,
            stress_type=stress_type, curve_family=curve_family,
            measurement_protocol_id=measurement_protocol_id,
            include_external=False,
        )
        folds = sorted({row.fold_number for row in rows if row.fold_number is not None})
        if len(folds) < 3:
            raise CurveTrainingError(f"{scheme} requires at least three grouped folds")
        all_evaluated = []
        for index, test_fold in enumerate(folds):
            calibration_fold = folds[(index + 1) % len(folds)]
            model = _fit_model(
                [row for row in rows if row.fold_number not in {test_fold, calibration_fold}],
                [row for row in rows if row.fold_number == calibration_fold],
                stress_type=stress_type, curve_family=curve_family,
                protocol=measurement_protocol_id, requirements=requirements,
            )
            _, evaluated = _evaluate(model, [row for row in rows if row.fold_number == test_fold])
            all_evaluated.extend(evaluated)
            persisted.extend((scheme, int(test_fold), row) for row in evaluated)
        supported_rows = [row for row in all_evaluated if row.metrics is not None]
        supported = [row.metrics for row in supported_rows]
        grouped[scheme] = CurveEvaluation(
            len(all_evaluated), len(supported),
            len({row.row.example.physical_device_key for row in supported_rows}),
            len(supported) / len(all_evaluated),
            float(np.mean([row.mae_a for row in supported])) if supported else None,
            float(np.median([row.max_abs_error_a for row in supported])) if supported else None,
            float(np.quantile([row.max_abs_error_a for row in supported], 0.9)) if supported else None,
            float(np.mean([row.normalized_rmse for row in supported])) if supported else None,
            float(np.mean([row.simultaneous_band_hit for row in supported])) if supported else None,
        )
    eligible, checks, reasons = _development_gate(grouped, requirements)
    final_model = _fit_model(
        training, calibration, stress_type=stress_type, curve_family=curve_family,
        protocol=measurement_protocol_id, requirements=requirements,
    )
    root = default_artifact_root().resolve()
    destination = (artifact_directory or root).resolve()
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise CurveTrainingError("artifact_directory must be under APS_IV_DAMAGE_ARTIFACT_ROOT") from exc
    artifact_path, checksum = _save_artifact(
        final_model, destination / f"{model_version}.curve.joblib",
        {"model_version": model_version, "snapshot_version": snapshot_version, "code_sha": code_sha},
    )
    validation_metrics = {
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
            INSERT INTO iv_damage_curve_model_runs (
                model_version, stress_type, curve_family, measurement_protocol_id,
                dataset_snapshot_id, acceptance_policy_id, algorithm, grid_spec,
                model_config, released_domain, validation_metrics, artifact_path,
                artifact_checksum, code_sha, environment_fingerprint, release_status
            ) VALUES (%s, %s, %s, %s, %s, %s, 'functional_pca_ridge', %s, %s,
                      %s, %s, %s, %s, %s, %s, 'candidate') RETURNING id
            """,
            (
                model_version, stress_type, curve_family, measurement_protocol_id,
                snapshot_id, policy_id, Json({"grid_v": final_model.artifact_manifest()["grid_v"], "unit": "V"}),
                Json(_curve_model_kwargs(requirements)),
                Json({"stress_type": stress_type, "curve_family": curve_family, "measurement_protocol_id": measurement_protocol_id}),
                Json(validation_metrics), artifact_path, checksum, code_sha,
                Json({"python": sys.version, "platform": platform.platform(), "numpy": np.__version__, "scikit_learn": sklearn.__version__}),
            ),
        )
        model_id = int(cursor.fetchone()[0])
        for scheme, _fold, evaluated in persisted:
            metric = evaluated.metrics
            cursor.execute(
                """
                INSERT INTO iv_damage_curve_validation_results (
                    curve_model_run_id, curve_response_pair_id, split_scheme,
                    split_role, evaluation_kind, physical_device_key, point_count,
                    mae_a, max_abs_error_a, normalized_rmse, simultaneous_band_hit,
                    support_status, ood_score, reasons
                ) VALUES (%s, %s, %s, 'grouped_test', 'development_cv', %s, %s,
                          %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    model_id, evaluated.row.curve_response_pair_id, scheme,
                    evaluated.row.example.physical_device_key,
                    len(evaluated.prediction.x_v) if evaluated.prediction.in_domain else 0,
                    metric.mae_a if metric else None, metric.max_abs_error_a if metric else None,
                    metric.normalized_rmse if metric else None,
                    metric.simultaneous_band_hit if metric else None,
                    "in_domain" if metric else evaluated.prediction.evidence_status,
                    evaluated.prediction.ood_score, list(evaluated.prediction.reasons),
                ),
            )
        conn.commit()
        return CurveTrainingResult(model_id, model_version, eligible, reasons, artifact_path, checksum)
    except Exception:
        conn.rollback()
        Path(artifact_path).unlink(missing_ok=True)
        raise
    finally:
        cursor.close()


def select_curve_candidate(conn, *, model_version: str, selected_by: str) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, dataset_snapshot_id,
                   validation_metrics @> '{"development_gate_eligible": true}'::jsonb,
                   release_status
            FROM iv_damage_curve_model_runs WHERE model_version = %s FOR UPDATE
            """,
            (model_version,),
        )
        row = cursor.fetchone()
        if row is None or row[3] != "candidate" or not row[2]:
            raise CurveTrainingError("only a development-gate-passing candidate can be selected")
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_model_selections (
                curve_model_run_id, dataset_snapshot_id, selection_protocol, selected_by
            ) VALUES (%s, %s, %s, %s) RETURNING id
            """,
            (row[0], row[1], Json({"external_outcomes_accessed": False, "selection_is_final": True}), selected_by),
        )
        selection_id = int(cursor.fetchone()[0])
        conn.commit()
        return selection_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def certify_curve_candidate(conn, *, model_version: str, certified_by: str) -> CurveCertificationResult:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, model.dataset_snapshot_id, model.stress_type,
                   model.curve_family, model.measurement_protocol_id,
                   model.artifact_path, model.artifact_checksum, policy.requirements,
                   selection.id
            FROM iv_damage_curve_model_runs model
            JOIN iv_damage_curve_model_selections selection ON selection.curve_model_run_id = model.id
            JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
            WHERE model.model_version = %s AND model.release_status = 'candidate'
            FOR UPDATE OF model, selection
            """,
            (model_version,),
        )
        identity = cursor.fetchone()
        if identity is None:
            raise CurveTrainingError("selected curve candidate does not exist")
        cursor.execute(
            "SELECT 1 FROM iv_damage_curve_external_certifications WHERE dataset_snapshot_id = %s",
            (identity[1],),
        )
        if cursor.fetchone() is not None:
            raise CurveTrainingError("this external curve holdout has already been consumed")
    finally:
        cursor.close()
    requirements = _curve_requirements(identity[7])
    model = load_curve_artifact(Path(identity[5]), identity[6])
    external = _snapshot_rows(
        conn, snapshot_id=int(identity[1]), split_scheme="frozen_release",
        stress_type=identity[2], curve_family=identity[3],
        measurement_protocol_id=identity[4], include_external=True,
    )
    external = [row for row in external if row.split_role == "external_test"]
    metrics, evaluated = _evaluate(model, external)
    checks = {
        "external_curves": metrics.supported_curves >= int(requirements["curve_min_external_curves"]),
        "external_devices": metrics.physical_devices >= int(requirements["curve_min_external_devices"]),
        "supported_fraction": metrics.supported_fraction >= float(requirements.get("min_supported_fraction", 0.8)),
        "mean_mae_a": metrics.mean_mae_a is not None and metrics.mean_mae_a <= float(requirements["curve_max_mean_mae_a"]),
        "p90_max_error_a": metrics.p90_max_abs_error_a is not None and metrics.p90_max_abs_error_a <= float(requirements["curve_max_p90_error_a"]),
        "normalized_rmse": metrics.mean_normalized_rmse is not None and metrics.mean_normalized_rmse <= float(requirements["curve_max_normalized_rmse"]),
        "simultaneous_band_coverage": metrics.simultaneous_band_coverage is not None and metrics.simultaneous_band_coverage >= float(requirements["curve_min_band_coverage"]),
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    passed = not reasons
    cursor = conn.cursor()
    try:
        for value in evaluated:
            metric = value.metrics
            cursor.execute(
                """
                INSERT INTO iv_damage_curve_validation_results (
                    curve_model_run_id, curve_response_pair_id, split_scheme,
                    split_role, evaluation_kind, physical_device_key, point_count,
                    mae_a, max_abs_error_a, normalized_rmse, simultaneous_band_hit,
                    support_status, ood_score, reasons
                ) VALUES (%s, %s, 'frozen_release', 'external_test',
                          'external_certification', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    identity[0], value.row.curve_response_pair_id,
                    value.row.example.physical_device_key,
                    len(value.prediction.x_v) if value.prediction.in_domain else 0,
                    metric.mae_a if metric else None, metric.max_abs_error_a if metric else None,
                    metric.normalized_rmse if metric else None,
                    metric.simultaneous_band_hit if metric else None,
                    "in_domain" if metric else value.prediction.evidence_status,
                    value.prediction.ood_score, list(value.prediction.reasons),
                ),
            )
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_external_certifications (
                selection_id, curve_model_run_id, dataset_snapshot_id, metrics,
                gate_checks, passed, certified_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (identity[8], identity[0], identity[1], Json(asdict(metrics)), Json(checks), passed, certified_by),
        )
        certification_id = int(cursor.fetchone()[0])
        cursor.execute(
            """
            UPDATE iv_damage_curve_model_runs
            SET release_status = %s,
                validated_at = CASE WHEN %s THEN clock_timestamp() ELSE NULL END
            WHERE id = %s
            """,
            ("validated" if passed else "failed", passed, identity[0]),
        )
        conn.commit()
        return CurveCertificationResult(certification_id, int(identity[0]), model_version, passed, reasons, metrics)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
