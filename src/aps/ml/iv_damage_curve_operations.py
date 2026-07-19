"""Prospective request, shadow monitoring, and release operations for curves."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping

from psycopg2.extras import Json, execute_values

from aps.ml.iv_damage_curve_model import CurveRequest
from aps.ml.iv_damage_curve_training import (
    CurveScoreBatch,
    load_curve_artifact,
)
from aps.ml.iv_damage_curves import load_curve_snapshot


class CurveOperationError(RuntimeError):
    """A curve lifecycle transition is unsafe or inconsistent."""


@dataclass(frozen=True)
class CurvePredictionRequest:
    request_key: str
    pre_curve_snapshot_id: int
    physical_device_key: str
    device_type: str
    stress_type: str
    curve_family: str
    measurement_protocol_id: str
    stress_features: Mapping[str, object]
    request_source: str
    manufacturer: str | None = None
    requested_by: str | None = None
    prediction_horizon_s: float | None = None


@dataclass(frozen=True)
class CurveMonitoringPolicy:
    min_matched_curves: int = 30
    max_mean_mae_a: float | None = None
    max_p90_max_error_a: float | None = None
    min_simultaneous_band_coverage: float = 0.75
    max_abstention_fraction: float = 0.40


def _required(value: object, name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise CurveOperationError(f"{name} is required")
    return text


def submit_curve_request(conn, request: CurvePredictionRequest) -> int:
    for name in (
        "request_key", "physical_device_key", "device_type", "stress_type",
        "curve_family", "measurement_protocol_id", "request_source",
    ):
        _required(getattr(request, name), name)
    curve = load_curve_snapshot(conn, request.pre_curve_snapshot_id)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT acquisition.physical_device_key
            FROM iv_damage_curve_snapshots curve
            JOIN iv_damage_acquisitions acquisition ON acquisition.id = curve.acquisition_id
            WHERE curve.id = %s
            """,
            (request.pre_curve_snapshot_id,),
        )
        identity = cursor.fetchone()
        if identity is None or identity[0] != request.physical_device_key:
            raise CurveOperationError("request device must come from the pre-curve acquisition")
        if curve.curve_family != request.curve_family or curve.measurement_protocol_id != request.measurement_protocol_id:
            raise CurveOperationError("request family/protocol must match the pre-curve snapshot")
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_prediction_requests (
                request_key, pre_curve_snapshot_id, physical_device_key,
                device_type, manufacturer, stress_type, curve_family,
                measurement_protocol_id, stress_features, prediction_horizon_s,
                request_source, requested_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (request_key) DO NOTHING RETURNING id
            """,
            (
                request.request_key, request.pre_curve_snapshot_id,
                request.physical_device_key, request.device_type,
                request.manufacturer, request.stress_type, request.curve_family,
                request.measurement_protocol_id, Json(dict(request.stress_features)),
                request.prediction_horizon_s, request.request_source,
                request.requested_by,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is None:
            cursor.execute(
                """
                SELECT id, pre_curve_snapshot_id, physical_device_key,
                       device_type, manufacturer, stress_type, curve_family,
                       measurement_protocol_id, stress_features,
                       prediction_horizon_s, request_source, requested_by
                FROM iv_damage_curve_prediction_requests
                WHERE request_key = %s
                """,
                (request.request_key,),
            )
            existing = cursor.fetchone()
            if existing is None:
                raise CurveOperationError("request conflict could not be resolved")
            expected = (
                request.pre_curve_snapshot_id, request.physical_device_key,
                request.device_type, request.manufacturer, request.stress_type,
                request.curve_family, request.measurement_protocol_id,
                dict(request.stress_features), request.prediction_horizon_s,
                request.request_source, request.requested_by,
            )
            if tuple(existing[1:]) != expected:
                raise CurveOperationError(
                    "curve request_key already exists with different immutable inputs"
                )
            conn.commit()
            return int(existing[0])
        request_id = int(inserted[0])
        conn.commit()
        return request_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def start_curve_shadow(conn, *, model_version: str, activated_by: str) -> int:
    _required(activated_by, "activated_by")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, model.stress_type, model.curve_family,
                   model.measurement_protocol_id, model.release_status,
                   certification.passed
            FROM iv_damage_curve_model_runs model
            JOIN iv_damage_curve_external_certifications certification
              ON certification.curve_model_run_id = model.id
            WHERE model.model_version = %s FOR UPDATE OF model
            """,
            (model_version,),
        )
        row = cursor.fetchone()
        if row is None or row[4] != "validated" or not row[5]:
            raise CurveOperationError("shadow deployment requires a certified validated curve model")
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-curve-shadow:{row[1]}:{row[2]}:{row[3]}",),
        )
        cursor.execute(
            """
            UPDATE iv_damage_curve_model_deployments
            SET active = FALSE, deactivated_by = %s,
                deactivated_at = clock_timestamp(),
                deactivation_reason = 'superseded shadow'
            WHERE stress_type = %s AND curve_family = %s
              AND measurement_protocol_id = %s
              AND deployment_mode = 'shadow' AND active
            """,
            (activated_by, row[1], row[2], row[3]),
        )
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_model_deployments (
                curve_model_run_id, stress_type, curve_family,
                measurement_protocol_id, deployment_mode, activated_by
            ) VALUES (%s, %s, %s, %s, 'shadow', %s) RETURNING id
            """,
            (row[0], row[1], row[2], row[3], activated_by),
        )
        deployment_id = int(cursor.fetchone()[0])
        cursor.execute(
            "UPDATE iv_damage_curve_model_runs SET release_status = 'shadow' WHERE id = %s",
            (row[0],),
        )
        conn.commit()
        return deployment_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def score_curve_requests(conn, *, limit: int = 500) -> CurveScoreBatch:
    if limit <= 0:
        raise CurveOperationError("limit must be positive")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT request.id, request.pre_curve_snapshot_id,
                   request.physical_device_key, request.device_type,
                   request.manufacturer, request.stress_type, request.curve_family,
                   request.measurement_protocol_id, request.stress_features,
                   request.prediction_horizon_s, deployment.deployment_mode,
                   model.id, model.release_status, model.artifact_path,
                   model.artifact_checksum,
                   EXISTS (
                       SELECT 1 FROM iv_damage_curve_external_certifications certification
                       WHERE certification.curve_model_run_id = model.id AND certification.passed
                   )
            FROM iv_damage_curve_prediction_requests request
            JOIN iv_damage_curve_model_deployments deployment
              ON deployment.stress_type = request.stress_type
             AND deployment.curve_family = request.curve_family
             AND deployment.measurement_protocol_id = request.measurement_protocol_id
             AND deployment.active
            JOIN iv_damage_curve_model_runs model
              ON model.id = deployment.curve_model_run_id
            WHERE request.request_status = 'pending'
            ORDER BY request.created_at, request.id, deployment.deployment_mode
            LIMIT %s FOR UPDATE OF request SKIP LOCKED
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        models = {}
        curve_cache = {}
        request_ids = set()
        eligible = abstentions = inserted = 0
        for row in rows:
            request_id, snapshot_id, model_id = int(row[0]), int(row[1]), int(row[11])
            request_ids.add(request_id)
            curve = curve_cache.get(snapshot_id)
            if curve is None:
                curve = load_curve_snapshot(conn, snapshot_id)
                curve_cache[snapshot_id] = curve
            model = models.get(model_id)
            if model is None:
                model = load_curve_artifact(Path(row[13]), row[14])
                models[model_id] = model
            features = dict(row[8])
            prediction = model.predict(CurveRequest(
                stress_type=row[5], curve_family=row[6], measurement_protocol_id=row[7],
                device_type=row[3], manufacturer=row[4],
                ion_species=str(features.get("ion_species") or "") or None,
                features=features, prediction_horizon_s=row[9],
                pre_x_v=curve.x_v, pre_i_a=curve.i_drain_a,
            ))
            decision = bool(
                row[10] == "decision" and row[12] == "released" and row[15]
                and prediction.in_domain and prediction.evidence_status == "decision_eligible"
            )
            evidence_status = (
                prediction.evidence_status if row[10] == "decision"
                else "screening_only" if prediction.in_domain else prediction.evidence_status
            )
            reasons = list(prediction.reasons)
            if row[10] == "shadow":
                reasons.append("shadow_prediction_not_for_decision_use")
            cursor.execute(
                """
                INSERT INTO iv_damage_curve_predictions (
                    request_id, curve_model_run_id, deployment_mode,
                    support_status, evidence_status, in_domain,
                    certification_gate_passed, decision_eligible, ood_score,
                    ood_threshold, reasons
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id, curve_model_run_id) DO NOTHING RETURNING id
                """,
                (
                    request_id, model_id, row[10],
                    "in_domain" if prediction.in_domain else prediction.evidence_status,
                    evidence_status, prediction.in_domain, bool(row[15]), decision,
                    prediction.ood_score, prediction.ood_threshold, reasons,
                ),
            )
            result = cursor.fetchone()
            if result is None:
                continue
            prediction_id = int(result[0])
            inserted += 1
            if prediction.in_domain:
                execute_values(
                    cursor,
                    """
                    INSERT INTO iv_damage_curve_prediction_points (
                        curve_prediction_id, point_index, x_value_v,
                        pre_i_drain_a, predicted_i_drain_a,
                        predicted_lower_a, predicted_upper_a
                    ) VALUES %s
                    """,
                    [
                        (prediction_id, index, x, pre, point, lower, upper)
                        for index, (x, pre, point, lower, upper) in enumerate(zip(
                            prediction.x_v, prediction.pre_i_a,
                            prediction.predicted_i_a, prediction.lower_i_a,
                            prediction.upper_i_a,
                        ))
                    ],
                )
            else:
                abstentions += 1
            eligible += int(decision)
        if request_ids:
            cursor.execute(
                "UPDATE iv_damage_curve_prediction_requests SET request_status = 'scored' WHERE id = ANY(%s)",
                (list(request_ids),),
            )
        conn.commit()
        return CurveScoreBatch(len(request_ids), inserted, eligible, abstentions)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def record_curve_outcome(
    conn, *, curve_prediction_id: int, post_curve_snapshot_id: int,
    match_method: str, reviewed_by: str,
) -> int:
    _required(match_method, "match_method")
    _required(reviewed_by, "reviewed_by")
    post = load_curve_snapshot(conn, post_curve_snapshot_id)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT prediction.curve_model_run_id, prediction.created_at,
                   request.physical_device_key, request.curve_family,
                   request.measurement_protocol_id, model.artifact_path,
                   model.artifact_checksum, acquisition.physical_device_key,
                   acquisition.measured_at
            FROM iv_damage_curve_predictions prediction
            JOIN iv_damage_curve_prediction_requests request ON request.id = prediction.request_id
            JOIN iv_damage_curve_model_runs model ON model.id = prediction.curve_model_run_id
            JOIN iv_damage_curve_snapshots curve ON curve.id = %s
            JOIN iv_damage_acquisitions acquisition ON acquisition.id = curve.acquisition_id
            WHERE prediction.id = %s FOR SHARE OF prediction, request, model, curve, acquisition
            """,
            (post_curve_snapshot_id, curve_prediction_id),
        )
        row = cursor.fetchone()
        if row is None or row[2] != row[7] or row[3] != post.curve_family or row[4] != post.measurement_protocol_id or row[8] <= row[1]:
            raise CurveOperationError("outcome must match device/family/protocol and be measured after prediction")
        cursor.execute(
            """
            SELECT x_value_v, predicted_i_drain_a, predicted_lower_a, predicted_upper_a
            FROM iv_damage_curve_prediction_points
            WHERE curve_prediction_id = %s ORDER BY point_index
            """,
            (curve_prediction_id,),
        )
        points = cursor.fetchall()
        if not points:
            raise CurveOperationError("abstained curve prediction has no outcome error")
        import numpy as np
        grid = np.asarray([value[0] for value in points], dtype=float)
        truth = np.interp(grid, post.x_v, post.i_drain_a)
        predicted = np.asarray([value[1] for value in points], dtype=float)
        lower = np.asarray([value[2] for value in points], dtype=float)
        upper = np.asarray([value[3] for value in points], dtype=float)
        residual = predicted - truth
        mae = float(np.mean(np.abs(residual)))
        maximum = float(np.max(np.abs(residual)))
        hit = bool(np.all((truth >= lower) & (truth <= upper)))
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_prediction_outcomes (
                curve_prediction_id, post_curve_snapshot_id, mae_a,
                max_abs_error_a, simultaneous_band_hit, match_method, reviewed_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (curve_prediction_id, post_curve_snapshot_id, mae, maximum, hit, match_method, reviewed_by),
        )
        outcome_id = int(cursor.fetchone()[0])
        conn.commit()
        return outcome_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def assess_curve_monitoring(
    conn, *, model_version: str, policy: CurveMonitoringPolicy,
    window_start: datetime, window_end: datetime, assessed_by: str,
) -> tuple[int, bool, tuple[str, ...]]:
    if window_end <= window_start:
        raise CurveOperationError("monitoring window must be ordered")
    if policy.max_mean_mae_a is None or policy.max_p90_max_error_a is None:
        raise CurveOperationError("monitoring error limits must be explicit")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, deployment.id,
                   COUNT(DISTINCT prediction.id),
                   COUNT(DISTINCT outcome.id),
                   AVG(outcome.mae_a),
                   percentile_cont(0.9) WITHIN GROUP (ORDER BY outcome.max_abs_error_a),
                   AVG(outcome.simultaneous_band_hit::integer),
                   AVG((NOT prediction.in_domain)::integer)
            FROM iv_damage_curve_model_runs model
            JOIN iv_damage_curve_model_deployments deployment
              ON deployment.curve_model_run_id = model.id
             AND deployment.deployment_mode = 'shadow'
            JOIN iv_damage_curve_predictions prediction
              ON prediction.curve_model_run_id = model.id
             AND prediction.deployment_mode = 'shadow'
             AND prediction.created_at >= %s AND prediction.created_at < %s
            LEFT JOIN iv_damage_curve_prediction_outcomes outcome
              ON outcome.curve_prediction_id = prediction.id
            WHERE model.model_version = %s
            GROUP BY model.id, deployment.id
            ORDER BY deployment.id DESC LIMIT 1
            """,
            (window_start, window_end, model_version),
        )
        row = cursor.fetchone()
        if row is None:
            raise CurveOperationError("shadow deployment has no predictions in monitoring window")
        metrics = {
            "predictions": int(row[2]), "matched_curves": int(row[3]),
            "mean_mae_a": row[4], "p90_max_abs_error_a": row[5],
            "simultaneous_band_coverage": row[6], "abstention_fraction": row[7],
        }
        checks = {
            "matched_curves": metrics["matched_curves"] >= policy.min_matched_curves,
            "mean_mae_a": metrics["mean_mae_a"] is not None and metrics["mean_mae_a"] <= policy.max_mean_mae_a,
            "p90_max_abs_error_a": metrics["p90_max_abs_error_a"] is not None and metrics["p90_max_abs_error_a"] <= policy.max_p90_max_error_a,
            "simultaneous_band_coverage": metrics["simultaneous_band_coverage"] is not None and metrics["simultaneous_band_coverage"] >= policy.min_simultaneous_band_coverage,
            "abstention_fraction": metrics["abstention_fraction"] is not None and metrics["abstention_fraction"] <= policy.max_abstention_fraction,
        }
        reasons = tuple(name for name, passed in checks.items() if not passed)
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_monitoring_assessments (
                curve_model_run_id, deployment_id, assessment_kind,
                window_start, window_end, policy, metrics, checks, passed, assessed_by
            ) VALUES (%s, %s, 'shadow_promotion', %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (row[0], row[1], window_start, window_end, Json(asdict(policy)), Json(metrics), Json(checks), not reasons, assessed_by),
        )
        assessment_id = int(cursor.fetchone()[0])
        conn.commit()
        return assessment_id, not reasons, reasons
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def promote_curve_model(conn, *, model_version: str, activated_by: str) -> int:
    """Promote only after external certification and passed shadow monitoring."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, model.stress_type, model.curve_family,
                   model.measurement_protocol_id, deployment.id
            FROM iv_damage_curve_model_runs model
            JOIN iv_damage_curve_external_certifications certification
              ON certification.curve_model_run_id = model.id AND certification.passed
            JOIN iv_damage_curve_model_deployments deployment
              ON deployment.curve_model_run_id = model.id
             AND deployment.deployment_mode = 'shadow' AND deployment.active
            WHERE model.model_version = %s AND model.release_status = 'shadow'
              AND EXISTS (
                  SELECT 1 FROM iv_damage_curve_monitoring_assessments assessment
                  WHERE assessment.curve_model_run_id = model.id
                    AND assessment.deployment_id = deployment.id
                    AND assessment.assessment_kind = 'shadow_promotion'
                    AND assessment.passed
              )
            FOR UPDATE OF model, deployment
            """,
            (model_version,),
        )
        row = cursor.fetchone()
        if row is None:
            raise CurveOperationError("promotion requires certified shadow model and passed monitoring assessment")
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-curve-decision:{row[1]}:{row[2]}:{row[3]}",),
        )
        cursor.execute(
            """
            UPDATE iv_damage_curve_model_deployments
            SET active = FALSE, deactivated_by = %s, deactivated_at = clock_timestamp(),
                deactivation_reason = 'promoted to decision'
            WHERE id = %s
            """,
            (activated_by, row[4]),
        )
        cursor.execute(
            """
            UPDATE iv_damage_curve_model_deployments
            SET active = FALSE, deactivated_by = %s, deactivated_at = clock_timestamp(),
                deactivation_reason = 'superseded decision release'
            WHERE stress_type = %s AND curve_family = %s
              AND measurement_protocol_id = %s AND deployment_mode = 'decision' AND active
            """,
            (activated_by, row[1], row[2], row[3]),
        )
        cursor.execute(
            """
            UPDATE iv_damage_curve_model_runs retired
            SET release_status = 'retired', retired_at = clock_timestamp()
            WHERE retired.id IN (
                SELECT curve_model_run_id FROM iv_damage_curve_model_deployments
                WHERE stress_type = %s AND curve_family = %s
                  AND measurement_protocol_id = %s AND deployment_mode = 'decision'
                  AND curve_model_run_id <> %s
            ) AND retired.release_status = 'released'
            """,
            (row[1], row[2], row[3], row[0]),
        )
        cursor.execute(
            "UPDATE iv_damage_curve_model_runs SET release_status = 'released', released_at = clock_timestamp() WHERE id = %s",
            (row[0],),
        )
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_model_deployments (
                curve_model_run_id, stress_type, curve_family,
                measurement_protocol_id, deployment_mode, activated_by
            ) VALUES (%s, %s, %s, %s, 'decision', %s) RETURNING id
            """,
            (row[0], row[1], row[2], row[3], activated_by),
        )
        deployment_id = int(cursor.fetchone()[0])
        conn.commit()
        return deployment_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
