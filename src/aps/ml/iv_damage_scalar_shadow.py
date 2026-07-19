"""Scalar shadow scoring, monitoring assessment, and gated promotion."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from psycopg2.extras import Json

from aps.ml.iv_damage_operations import (
    DamageOperationError,
    MonitoringPolicy,
    PendingPredictionRequest,
    ScoreBatchResult,
    assess_monitoring,
    load_model_artifact,
    resolve_artifact_path,
    score_request,
)


def start_scalar_shadow(conn, *, model_version: str, activated_by: str) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, model.stress_type, model.target_type, model.release_status,
                   certification.passed
            FROM iv_damage_model_runs model
            JOIN iv_damage_external_certifications certification
              ON certification.model_run_id = model.id
            WHERE model.model_version = %s FOR UPDATE OF model
            """,
            (model_version,),
        )
        row = cursor.fetchone()
        if row is None or row[3] != "validated" or not row[4]:
            raise DamageOperationError("shadow deployment requires a certified validated model")
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-shadow:{row[1]}:{row[2]}",),
        )
        cursor.execute(
            """
            UPDATE iv_damage_model_deployments
            SET active = FALSE, deactivated_by = %s,
                deactivated_at = clock_timestamp(),
                deactivation_reason = 'superseded shadow'
            WHERE stress_type = %s AND target_type = %s
              AND deployment_mode = 'shadow' AND active
            """,
            (activated_by, row[1], row[2]),
        )
        cursor.execute(
            """
            INSERT INTO iv_damage_model_deployments (
                model_run_id, stress_type, target_type, deployment_mode, activated_by
            ) VALUES (%s, %s, %s, 'shadow', %s) RETURNING id
            """,
            (row[0], row[1], row[2], activated_by),
        )
        deployment_id = int(cursor.fetchone()[0])
        cursor.execute(
            "UPDATE iv_damage_model_runs SET release_status = 'shadow' WHERE id = %s",
            (row[0],),
        )
        conn.commit()
        return deployment_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def score_scalar_shadow_requests(conn, *, limit: int = 500) -> ScoreBatchResult:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT request.id, request.request_key, request.physical_device_key,
                   request.device_type, request.manufacturer,
                   request.measurement_protocol_id, request.stress_type,
                   request.target_type, request.pre_value, request.pre_uncertainty,
                   request.reference_policy, request.stress_features,
                   request.requested_prediction_horizon_s,
                   model.id, model.artifact_path, model.artifact_checksum
            FROM iv_damage_prediction_requests request
            JOIN iv_damage_model_deployments deployment
              ON deployment.stress_type = request.stress_type
             AND deployment.target_type = request.target_type
             AND deployment.deployment_mode = 'shadow' AND deployment.active
            JOIN iv_damage_model_runs model
              ON model.id = deployment.model_run_id AND model.release_status = 'shadow'
            WHERE request.request_status = 'pending'
              AND NOT EXISTS (
                  SELECT 1 FROM iv_damage_predictions existing
                  WHERE existing.request_id = request.id
                    AND existing.model_run_id = model.id
              )
            ORDER BY request.created_at, request.id LIMIT %s
            FOR UPDATE OF request SKIP LOCKED
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        models = {}
        abstentions = 0
        for row in rows:
            model_id = int(row[13])
            model = models.get(model_id)
            if model is None:
                model = load_model_artifact(
                    resolve_artifact_path(row[14]), row[15]
                )
                models[model_id] = model
            request = PendingPredictionRequest(
                request_id=int(row[0]), request_key=row[1],
                physical_device_key=row[2], device_type=row[3], manufacturer=row[4],
                measurement_protocol_id=row[5], stress_type=row[6], target_type=row[7],
                pre_value=float(row[8]), pre_uncertainty=row[9],
                reference_policy=row[10], stress_features=dict(row[11]),
                requested_prediction_horizon_s=row[12],
            )
            scored = score_request(model, request)
            evidence_status = "screening_only" if scored.in_domain else scored.evidence_status
            reasons = sorted(set((*scored.reasons, "shadow_prediction_not_for_decision_use")))
            cursor.execute(
                """
                INSERT INTO iv_damage_predictions (
                    request_id, model_run_id, predicted_response,
                    predicted_response_lower, predicted_response_upper,
                    predicted_post_value, predicted_post_lower, predicted_post_upper,
                    support_status, evidence_status, in_domain,
                    validation_gate_passed, decision_eligible, ood_score,
                    ood_threshold, reasons, feature_completeness, deployment_mode
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          TRUE, FALSE, %s, %s, %s, %s, 'shadow')
                """,
                (
                    request.request_id, model_id, scored.predicted_response,
                    scored.predicted_response_lower, scored.predicted_response_upper,
                    scored.predicted_post_value, scored.predicted_post_lower,
                    scored.predicted_post_upper, scored.support_status,
                    evidence_status, scored.in_domain, scored.ood_score,
                    scored.ood_threshold, reasons,
                    Json(dict(scored.feature_completeness)),
                ),
            )
            abstentions += int(not scored.in_domain)
        conn.commit()
        return ScoreBatchResult(len(rows), len(rows), 0, abstentions)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def assess_scalar_shadow(
    conn, *, model_version: str, policy: MonitoringPolicy,
    window_start: datetime, window_end: datetime, assessed_by: str,
) -> tuple[int, bool, tuple[str, ...]]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, deployment.id, COUNT(*) AS predictions,
                   COUNT(*) FILTER (WHERE outcome.id IS NOT NULL) AS matched_outcomes,
                   COUNT(*) FILTER (WHERE prediction.support_status <> 'in_domain') AS abstentions,
                   AVG(abs(prediction.predicted_response - outcome.observed_response))
                     FILTER (WHERE outcome.id IS NOT NULL) AS mae,
                   AVG(prediction.predicted_response - outcome.observed_response)
                     FILTER (WHERE outcome.id IS NOT NULL) AS bias,
                   AVG((outcome.observed_response BETWEEN prediction.predicted_response_lower
                       AND prediction.predicted_response_upper)::integer)
                     FILTER (WHERE outcome.id IS NOT NULL) AS interval_coverage
            FROM iv_damage_model_runs model
            JOIN iv_damage_model_deployments deployment
              ON deployment.model_run_id = model.id
             AND deployment.deployment_mode = 'shadow'
            JOIN iv_damage_predictions prediction
              ON prediction.model_run_id = model.id
             AND prediction.deployment_mode = 'shadow'
             AND prediction.created_at >= %s AND prediction.created_at < %s
            LEFT JOIN iv_damage_prediction_outcomes outcome
              ON outcome.prediction_id = prediction.id
            WHERE model.model_version = %s
            GROUP BY model.id, deployment.id
            ORDER BY deployment.id DESC LIMIT 1
            """,
            (window_start, window_end, model_version),
        )
        row = cursor.fetchone()
        if row is None:
            raise DamageOperationError("shadow deployment has no predictions in window")
        summary = {
            "predictions": int(row[2]), "matched_outcomes": int(row[3]),
            "abstentions": int(row[4]), "mae": row[5], "bias": row[6],
            "interval_coverage": row[7],
        }
        assessment = assess_monitoring(summary, policy)
        passed = assessment.status == "healthy"
        cursor.execute(
            """
            INSERT INTO iv_damage_monitoring_assessments (
                model_run_id, deployment_id, assessment_kind, window_start,
                window_end, policy, metrics, checks, passed, assessed_by
            ) VALUES (%s, %s, 'shadow_promotion', %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                row[0], row[1], window_start, window_end, Json(asdict(policy)),
                Json(summary), Json(dict(assessment.checks)), passed, assessed_by,
            ),
        )
        assessment_id = int(cursor.fetchone()[0])
        conn.commit()
        return assessment_id, passed, assessment.reasons
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def record_scalar_outcomes_for_all_models(
    conn, *, request_key: str, response_unit_key: str, match_method: str,
    reviewed_by: str, review_notes: str | None = None,
) -> tuple[int, ...]:
    """Attach one authoritative prospective response to every earlier prediction."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT request.id, response.id, response.response_value,
                   response.post_measured_at,
                   request.physical_device_key = response.physical_device_key,
                   request.stress_type = response.stress_type,
                   request.target_type = response.target_type,
                   request.measurement_protocol_id = response.measurement_protocol_id,
                   response.stress_features @> request.stress_features
            FROM iv_damage_prediction_requests request
            JOIN iv_damage_response_units response ON response.unit_key = %s
            WHERE request.request_key = %s
            FOR UPDATE OF request, response
            """,
            (response_unit_key, request_key),
        )
        facts = cursor.fetchone()
        if facts is None or not all(facts[4:9]):
            raise DamageOperationError(
                "outcome must match device/stress/target/protocol/stress conditions"
            )
        cursor.execute(
            """
            SELECT id FROM iv_damage_predictions
            WHERE request_id = %s AND created_at < %s
            ORDER BY created_at, id FOR UPDATE
            """,
            (facts[0], facts[3]),
        )
        predictions = [int(row[0]) for row in cursor.fetchall()]
        if not predictions:
            raise DamageOperationError(
                "no prediction exists from before the post measurement"
            )
        outcome_ids = []
        for prediction_id in predictions:
            cursor.execute(
                """
                INSERT INTO iv_damage_prediction_outcomes (
                    request_id, response_unit_id, prediction_id, observed_response,
                    match_method, reviewed_by, review_notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (prediction_id) DO NOTHING RETURNING id
                """,
                (
                    facts[0], facts[1], prediction_id, facts[2], match_method,
                    reviewed_by, review_notes,
                ),
            )
            inserted = cursor.fetchone()
            if inserted is not None:
                outcome_ids.append(int(inserted[0]))
        conn.commit()
        return tuple(outcome_ids)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def promote_scalar_model(conn, *, model_version: str, activated_by: str) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model.id, model.stress_type, model.target_type, deployment.id
            FROM iv_damage_model_runs model
            JOIN iv_damage_external_certifications certification
              ON certification.model_run_id = model.id AND certification.passed
            JOIN iv_damage_model_deployments deployment
              ON deployment.model_run_id = model.id
             AND deployment.deployment_mode = 'shadow' AND deployment.active
            WHERE model.model_version = %s AND model.release_status = 'shadow'
              AND EXISTS (
                  SELECT 1 FROM iv_damage_monitoring_assessments assessment
                  WHERE assessment.model_run_id = model.id
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
            raise DamageOperationError("promotion requires certified shadow model and passed monitoring")
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-release:{row[1]}:{row[2]}",),
        )
        cursor.execute(
            """
            SELECT model_run_id FROM iv_damage_model_releases
            WHERE stress_type = %s AND target_type = %s AND active FOR UPDATE
            """,
            (row[1], row[2]),
        )
        previous = cursor.fetchone()
        if previous:
            cursor.execute(
                """
                UPDATE iv_damage_model_releases
                SET active = FALSE, deactivated_at = clock_timestamp(),
                    deactivated_by = %s, deactivation_reason = 'superseded after monitored promotion',
                    deactivation_kind = 'superseded'
                WHERE stress_type = %s AND target_type = %s AND active
                """,
                (activated_by, row[1], row[2]),
            )
            cursor.execute(
                "UPDATE iv_damage_model_runs SET release_status = 'retired', retired_at = clock_timestamp() WHERE id = %s",
                (previous[0],),
            )
        cursor.execute(
            """
            INSERT INTO iv_damage_model_releases (
                model_run_id, stress_type, target_type, active,
                activated_at, activated_by, release_notes
            ) VALUES (%s, %s, %s, TRUE, clock_timestamp(), %s,
                      'external certification and prospective shadow gates passed')
            RETURNING id
            """,
            (row[0], row[1], row[2], activated_by),
        )
        release_id = int(cursor.fetchone()[0])
        cursor.execute(
            """
            UPDATE iv_damage_model_deployments
            SET active = FALSE, deactivated_at = clock_timestamp(),
                deactivated_by = %s, deactivation_reason = 'promoted to decision'
            WHERE id = %s
            """,
            (activated_by, row[3]),
        )
        cursor.execute(
            "UPDATE iv_damage_model_runs SET release_status = 'released', released_at = clock_timestamp() WHERE id = %s",
            (row[0],),
        )
        cursor.execute(
            """
            INSERT INTO iv_damage_model_deployments (
                model_run_id, stress_type, target_type, deployment_mode, activated_by
            ) VALUES (%s, %s, %s, 'decision', %s)
            """,
            (row[0], row[1], row[2], activated_by),
        )
        conn.commit()
        return release_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
