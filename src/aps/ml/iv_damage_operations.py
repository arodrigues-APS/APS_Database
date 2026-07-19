"""Artifact, release, scoring, outcome, and monitoring operations for V3.

Every database mutation is explicit and transactional.  Nightly automation may
score with an already-active model, but this module contains no automatic
training or release transition.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
import os
from pathlib import Path
from typing import Mapping
from uuid import uuid4

import joblib
from psycopg2.extras import Json

from aps.ml.iv_damage_model import (
    CalibratedDamageModel,
    DamageRequest,
    validate_request_features,
)
from aps.ml.iv_damage_policy import (
    EvidenceStatus,
    ReleaseStatus,
    prediction_decision_eligible,
)
from aps.ml.iv_damage_repository import post_value_from_response
from aps.config import get_settings


class DamageOperationError(RuntimeError):
    """Raised when an operational safety invariant is not satisfied."""


@dataclass(frozen=True)
class ArtifactRecord:
    path: Path
    checksum: str
    metadata: Mapping[str, object]


@dataclass(frozen=True)
class ReleaseResult:
    model_run_id: int
    model_version: str
    stress_type: str
    target_type: str
    previous_model_run_id: int | None


@dataclass(frozen=True)
class DeactivationResult:
    model_run_id: int
    stress_type: str
    target_type: str
    deactivated_by: str


@dataclass(frozen=True)
class PendingPredictionRequest:
    request_id: int
    request_key: str
    physical_device_key: str
    device_type: str
    manufacturer: str | None
    measurement_protocol_id: str
    stress_type: str
    target_type: str
    pre_value: float
    pre_uncertainty: float | None
    reference_policy: str
    stress_features: Mapping[str, object]
    requested_prediction_horizon_s: float | None = None


@dataclass(frozen=True)
class ScoredPrediction:
    predicted_response: float | None
    predicted_response_lower: float | None
    predicted_response_upper: float | None
    predicted_post_value: float | None
    predicted_post_lower: float | None
    predicted_post_upper: float | None
    support_status: str
    evidence_status: str
    in_domain: bool
    decision_eligible: bool
    ood_score: float | None
    ood_threshold: float
    reasons: tuple[str, ...]
    feature_completeness: Mapping[str, object]


@dataclass(frozen=True)
class ScoreBatchResult:
    selected_requests: int
    inserted_predictions: int
    decision_eligible_predictions: int
    abstentions: int


@dataclass(frozen=True)
class MonitoringPolicy:
    min_matched_outcomes: int = 30
    max_mae: float | None = None
    max_abs_bias: float | None = None
    min_interval_coverage: float = 0.75
    max_abstention_fraction: float = 0.40


@dataclass(frozen=True)
class MonitoringAssessment:
    status: str
    checks: Mapping[str, bool]
    reasons: tuple[str, ...]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_model_artifact(
    model: CalibratedDamageModel,
    path: Path,
    *,
    metadata: Mapping[str, object],
) -> ArtifactRecord:
    """Write an immutable joblib artifact atomically and return its checksum."""

    if not model.is_calibrated:
        raise DamageOperationError("only fitted and calibrated models may be saved")
    destination = path.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{uuid4().hex}.tmp"
    )
    payload = {
        "format_version": "iv-damage-artifact-v1",
        "model": model,
        "manifest": dict(model.artifact_manifest()),
        "metadata": dict(metadata),
    }
    try:
        joblib.dump(payload, temporary, compress=3)
        try:
            # A hard-link publication is atomic and fails if another trainer
            # already claimed this immutable version.  os.replace() would
            # silently overwrite the winner after both processes passed an
            # exists() preflight.
            os.link(temporary, destination)
        except FileExistsError as exc:
            raise DamageOperationError(
                f"artifact already exists and is immutable: {destination}"
            ) from exc
    finally:
        if temporary.exists():
            temporary.unlink()
    return ArtifactRecord(destination, sha256_file(destination), dict(metadata))


def load_model_artifact(path: Path, expected_checksum: str) -> CalibratedDamageModel:
    resolved = path.resolve()
    if not resolved.is_file():
        raise DamageOperationError(f"model artifact is missing: {resolved}")
    actual = sha256_file(resolved)
    if actual != expected_checksum:
        raise DamageOperationError(
            f"model artifact checksum mismatch: expected {expected_checksum}, got {actual}"
        )
    payload = joblib.load(resolved)
    if not isinstance(payload, dict) or payload.get("format_version") != "iv-damage-artifact-v1":
        raise DamageOperationError("unsupported or invalid model artifact format")
    model = payload.get("model")
    if not isinstance(model, CalibratedDamageModel) or not model.is_calibrated:
        raise DamageOperationError("artifact does not contain a calibrated damage model")
    return model


def default_artifact_root(*, writable: bool = True) -> Path:
    """Return the configured shared artifact root; never infer a checkout path."""
    return get_settings().require_iv_damage_artifact_root(writable=writable)


def resolve_artifact_path(stored_path: str) -> Path:
    path = Path(stored_path)
    if not path.is_absolute():
        raise DamageOperationError(
            "model artifact_path must be absolute under APS_IV_DAMAGE_ARTIFACT_ROOT"
        )
    resolved = path.resolve()
    root = default_artifact_root(writable=False).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise DamageOperationError(
            "model artifact_path is outside APS_IV_DAMAGE_ARTIFACT_ROOT"
        ) from exc
    return resolved


_RELEASE_CANDIDATE_SQL = """
SELECT model.id, model.model_version, model.release_status, model.stress_type,
       model.target_type, model.artifact_path, model.artifact_checksum,
       policy.approved,
       model.validation_metrics @> '{"release_gate_eligible": true}'::jsonb
FROM iv_damage_model_runs model
JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
WHERE model.model_version = %s
FOR UPDATE OF model
"""

_RELEASE_IDENTITY_SQL = """
SELECT id, stress_type, target_type
FROM iv_damage_model_runs
WHERE model_version = %s
"""


def _validate_releasable(row, *, allowed_statuses: set[str]) -> None:
    if row is None:
        raise DamageOperationError("model version does not exist")
    if row[2] not in allowed_statuses:
        raise DamageOperationError(f"model lifecycle state is not releasable: {row[2]}")
    if not row[7]:
        raise DamageOperationError("acceptance policy is not approved")
    if not row[8]:
        raise DamageOperationError("persisted release validation gate did not pass")
    load_model_artifact(resolve_artifact_path(row[5]), row[6])


def release_model(
    conn,
    *,
    model_version: str,
    activated_by: str,
    release_notes: str | None = None,
) -> ReleaseResult:
    """Activate a validated model for exactly one stress/target domain."""

    if not activated_by.strip():
        raise DamageOperationError("activated_by is required")
    cursor = conn.cursor()
    try:
        cursor.execute(_RELEASE_IDENTITY_SQL, (model_version,))
        identity = cursor.fetchone()
        if identity is None:
            raise DamageOperationError("model version does not exist")
        _, stress_type, target_type = identity
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-release:{stress_type}:{target_type}",),
        )
        cursor.execute(_RELEASE_CANDIDATE_SQL, (model_version,))
        candidate = cursor.fetchone()
        _validate_releasable(
            candidate,
            allowed_statuses={ReleaseStatus.VALIDATED, ReleaseStatus.SHADOW},
        )
        model_id = int(candidate[0])
        cursor.execute(
            """
            SELECT model_run_id
            FROM iv_damage_model_releases
            WHERE stress_type = %s AND target_type = %s AND active
            FOR UPDATE
            """,
            (stress_type, target_type),
        )
        active = cursor.fetchone()
        previous_id = int(active[0]) if active else None
        if previous_id == model_id:
            raise DamageOperationError("model is already the active release")
        if previous_id is not None:
            cursor.execute(
                """
                UPDATE iv_damage_model_releases
                SET active = false, deactivated_at = clock_timestamp(),
                    deactivated_by = %s,
                    deactivation_reason = %s,
                    deactivation_kind = 'superseded'
                WHERE stress_type = %s AND target_type = %s AND active
                """,
                (
                    activated_by,
                    f"superseded by model_version={model_version}",
                    stress_type,
                    target_type,
                ),
            )
            cursor.execute(
                """
                UPDATE iv_damage_model_runs
                SET release_status = 'retired', retired_at = clock_timestamp()
                WHERE id = %s
                """,
                (previous_id,),
            )
        cursor.execute(
            """
            INSERT INTO iv_damage_model_releases (
                model_run_id, stress_type, target_type, active, activated_at,
                activated_by, release_notes
            ) VALUES (%s, %s, %s, true, clock_timestamp(), %s, %s)
            """,
            (model_id, stress_type, target_type, activated_by, release_notes),
        )
        cursor.execute(
            """
            UPDATE iv_damage_model_runs
            SET release_status = 'released', released_at = clock_timestamp(), retired_at = NULL
            WHERE id = %s
            """,
            (model_id,),
        )
        conn.commit()
        return ReleaseResult(model_id, model_version, stress_type, target_type, previous_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def rollback_release(
    conn,
    *,
    stress_type: str,
    target_type: str,
    activated_by: str,
    to_model_version: str | None = None,
    release_notes: str | None = None,
) -> ReleaseResult:
    """Create a new release event pointing to a previously validated artifact."""

    if not activated_by.strip():
        raise DamageOperationError("activated_by is required")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-release:{stress_type}:{target_type}",),
        )
        cursor.execute(
            """
            SELECT model_run_id
            FROM iv_damage_model_releases
            WHERE stress_type = %s AND target_type = %s AND active
            FOR UPDATE
            """,
            (stress_type, target_type),
        )
        active = cursor.fetchone()
        if active is None:
            raise DamageOperationError("domain has no active release to roll back")
        current_id = int(active[0])
        if to_model_version:
            cursor.execute(_RELEASE_CANDIDATE_SQL, (to_model_version,))
        else:
            cursor.execute(
                """
                SELECT model.id, model.model_version, model.release_status, model.stress_type,
                       model.target_type, model.artifact_path, model.artifact_checksum,
                       policy.approved,
                       model.validation_metrics @> '{"release_gate_eligible": true}'::jsonb
                FROM iv_damage_model_releases history
                JOIN iv_damage_model_runs model ON model.id = history.model_run_id
                JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
                WHERE history.stress_type = %s AND history.target_type = %s
                  AND history.model_run_id <> %s AND NOT history.active
                ORDER BY history.activated_at DESC, history.id DESC
                LIMIT 1
                FOR UPDATE OF model
                """,
                (stress_type, target_type, current_id),
            )
        candidate = cursor.fetchone()
        _validate_releasable(
            candidate,
            allowed_statuses={ReleaseStatus.RETIRED, ReleaseStatus.VALIDATED, ReleaseStatus.SHADOW},
        )
        if candidate[3] != stress_type or candidate[4] != target_type:
            raise DamageOperationError("rollback model does not match requested domain")
        if int(candidate[0]) == current_id:
            raise DamageOperationError("rollback target is already active")
        cursor.execute(
            """
            UPDATE iv_damage_model_releases
            SET active = false, deactivated_at = clock_timestamp(),
                deactivated_by = %s,
                deactivation_reason = %s,
                deactivation_kind = 'rollback'
            WHERE stress_type = %s AND target_type = %s AND active
            """,
            (
                activated_by,
                release_notes or f"rollback to model_version={candidate[1]}",
                stress_type,
                target_type,
            ),
        )
        cursor.execute(
            """
            UPDATE iv_damage_model_runs
            SET release_status = 'retired', retired_at = clock_timestamp()
            WHERE id = %s
            """,
            (current_id,),
        )
        cursor.execute(
            """
            INSERT INTO iv_damage_model_releases (
                model_run_id, stress_type, target_type, active, activated_at,
                activated_by, release_notes
            ) VALUES (%s, %s, %s, true, clock_timestamp(), %s, %s)
            """,
            (
                candidate[0], stress_type, target_type, activated_by,
                release_notes or f"rollback from model_run_id={current_id}",
            ),
        )
        cursor.execute(
            """
            UPDATE iv_damage_model_runs
            SET release_status = 'released', released_at = clock_timestamp(), retired_at = NULL
            WHERE id = %s
            """,
            (candidate[0],),
        )
        conn.commit()
        return ReleaseResult(
            int(candidate[0]), candidate[1], stress_type, target_type, current_id
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def deactivate_release(
    conn,
    *,
    stress_type: str,
    target_type: str,
    deactivated_by: str,
    reason: str,
) -> DeactivationResult:
    """Stop decision use for a domain without requiring a replacement model."""
    if not deactivated_by.strip() or not reason.strip():
        raise DamageOperationError("deactivated_by and reason are required")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"iv-damage-release:{stress_type}:{target_type}",),
        )
        cursor.execute(
            """
            SELECT id, model_run_id
            FROM iv_damage_model_releases
            WHERE stress_type = %s AND target_type = %s AND active
            FOR UPDATE
            """,
            (stress_type, target_type),
        )
        active = cursor.fetchone()
        if active is None:
            raise DamageOperationError("domain has no active release to deactivate")
        release_id, model_run_id = int(active[0]), int(active[1])
        cursor.execute(
            """
            UPDATE iv_damage_model_releases
            SET active = false, deactivated_at = clock_timestamp(),
                deactivated_by = %s, deactivation_reason = %s,
                deactivation_kind = 'emergency'
            WHERE id = %s
            """,
            (deactivated_by, reason, release_id),
        )
        cursor.execute(
            """
            UPDATE iv_damage_model_runs
            SET release_status = 'retired', retired_at = clock_timestamp()
            WHERE id = %s
            """,
            (model_run_id,),
        )
        conn.commit()
        return DeactivationResult(
            model_run_id, stress_type, target_type, deactivated_by
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def score_request(
    model: CalibratedDamageModel,
    request: PendingPredictionRequest,
) -> ScoredPrediction:
    features = dict(request.stress_features)
    features["pre_value"] = request.pre_value
    model_request = DamageRequest(
        stress_type=request.stress_type,
        target_type=request.target_type,
        device_type=request.device_type,
        manufacturer=request.manufacturer,
        ion_species=str(features.get("ion_species") or "") or None,
        features=features,
        reference_policy=request.reference_policy,
        protocol_signature=request.measurement_protocol_id,
        prediction_horizon_s=(
            request.requested_prediction_horizon_s
            if request.requested_prediction_horizon_s is not None
            else features.get("post_measurement_delay_s")
        ),
    )
    prediction = model.predict(model_request)
    manifest = model.artifact_manifest()
    ood_threshold = float(manifest["ood_threshold"])
    predicted_post = post_lower = post_upper = None
    reasons = list(prediction.reasons)
    evidence_status = str(prediction.evidence_status)
    support_status = "in_domain" if prediction.in_domain else evidence_status
    if support_status not in {
        "in_domain", "out_of_domain", "insufficient_evidence", "invalid_input"
    }:
        support_status = "insufficient_evidence"
    if prediction.predicted_response is not None:
        try:
            predicted_post = post_value_from_response(
                request.target_type, request.pre_value, prediction.predicted_response
            )
            post_lower = post_value_from_response(
                request.target_type, request.pre_value, prediction.interval_lower
            )
            post_upper = post_value_from_response(
                request.target_type, request.pre_value, prediction.interval_upper
            )
            if not all(math.isfinite(value) for value in (predicted_post, post_lower, post_upper)):
                raise ValueError("non-finite physical output")
        except (OverflowError, ValueError):
            reasons.append("nonphysical_model_output")
            evidence_status = EvidenceStatus.INSUFFICIENT_EVIDENCE
            support_status = "insufficient_evidence"
            predicted_post = post_lower = post_upper = None
    gate_passed = True
    decision_eligible = prediction_decision_eligible(
        release_status=ReleaseStatus.RELEASED,
        evidence_status=evidence_status,
        reference_policy=request.reference_policy,
        in_domain=prediction.in_domain and predicted_post is not None,
        validation_gate_passed=gate_passed,
    )
    feature_failures = validate_request_features(
        stress_type=request.stress_type,
        features=features,
    )
    missing = sorted(
        {
            reason.split(":", 1)[1]
            for reason in feature_failures
            if reason.startswith("missing_or_nonfinite:")
        }
    )
    return ScoredPrediction(
        predicted_response=prediction.predicted_response,
        predicted_response_lower=prediction.interval_lower,
        predicted_response_upper=prediction.interval_upper,
        predicted_post_value=predicted_post,
        predicted_post_lower=post_lower,
        predicted_post_upper=post_upper,
        support_status=support_status,
        evidence_status=evidence_status,
        in_domain=prediction.in_domain,
        decision_eligible=decision_eligible,
        ood_score=prediction.neighbor_distance,
        ood_threshold=ood_threshold,
        reasons=tuple(sorted(set(reasons))),
        feature_completeness={
            "complete": not feature_failures,
            "missing": missing,
            "failures": list(feature_failures),
        },
    )


_PENDING_REQUESTS_SQL = """
SELECT request.id, request.request_key, request.physical_device_key,
       request.device_type, request.manufacturer, request.measurement_protocol_id,
       request.stress_type, request.target_type, request.pre_value,
       request.pre_uncertainty, request.reference_policy, request.stress_features,
       request.requested_prediction_horizon_s,
       model.id, model.model_version, model.artifact_path, model.artifact_checksum,
       release.id
FROM iv_damage_prediction_requests request
JOIN iv_damage_model_releases release
  ON release.stress_type = request.stress_type
 AND release.target_type = request.target_type
 AND release.active
JOIN iv_damage_model_runs model
  ON model.id = release.model_run_id
 AND model.release_status = 'released'
WHERE request.request_status = 'pending'
ORDER BY request.created_at, request.id
LIMIT %s
FOR UPDATE OF request SKIP LOCKED
"""


def score_pending_requests(conn, *, limit: int = 500) -> ScoreBatchResult:
    """Score pending requests only where an exact-domain active release exists."""

    if limit <= 0:
        raise DamageOperationError("score limit must be positive")
    cursor = conn.cursor()
    try:
        cursor.execute(_PENDING_REQUESTS_SQL, (limit,))
        rows = cursor.fetchall()
        models: dict[int, CalibratedDamageModel] = {}
        locked_releases: set[int] = set()
        eligible = abstentions = 0
        for row in rows:
            model_id = int(row[13])
            release_id = int(row[17])
            if release_id not in locked_releases:
                cursor.execute(
                    """
                    SELECT model_run_id, active
                    FROM iv_damage_model_releases
                    WHERE id = %s
                    FOR SHARE
                    """,
                    (release_id,),
                )
                locked_release = cursor.fetchone()
                if (
                    locked_release is None
                    or int(locked_release[0]) != model_id
                    or not locked_release[1]
                ):
                    raise DamageOperationError(
                        "active release changed while requests were being scored"
                    )
                locked_releases.add(release_id)
            model = models.get(model_id)
            if model is None:
                model = load_model_artifact(resolve_artifact_path(row[15]), row[16])
                if model.stress_type != row[6] or model.target_type != row[7]:
                    raise DamageOperationError("active artifact domain does not match request")
                models[model_id] = model
            request = PendingPredictionRequest(
                request_id=int(row[0]), request_key=row[1], physical_device_key=row[2],
                device_type=row[3], manufacturer=row[4], measurement_protocol_id=row[5],
                stress_type=row[6], target_type=row[7], pre_value=float(row[8]),
                pre_uncertainty=row[9], reference_policy=row[10],
                stress_features=dict(row[11]),
                requested_prediction_horizon_s=row[12],
            )
            scored = score_request(model, request)
            cursor.execute(
                """
                INSERT INTO iv_damage_predictions (
                    request_id, model_run_id, predicted_response,
                    predicted_response_lower, predicted_response_upper,
                    predicted_post_value, predicted_post_lower, predicted_post_upper,
                    support_status, evidence_status, in_domain,
                    validation_gate_passed, decision_eligible, ood_score, ood_threshold,
                    reasons, feature_completeness
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, true, %s, %s, %s, %s, %s
                )
                """,
                (
                    request.request_id, model_id, scored.predicted_response,
                    scored.predicted_response_lower, scored.predicted_response_upper,
                    scored.predicted_post_value, scored.predicted_post_lower,
                    scored.predicted_post_upper, scored.support_status,
                    scored.evidence_status, scored.in_domain, scored.decision_eligible,
                    scored.ood_score, scored.ood_threshold, list(scored.reasons),
                    Json(dict(scored.feature_completeness)),
                ),
            )
            cursor.execute(
                """
                UPDATE iv_damage_prediction_requests
                SET request_status = %s
                WHERE id = %s
                """,
                (
                    "invalid" if scored.evidence_status == EvidenceStatus.INVALID_INPUT else "scored",
                    request.request_id,
                ),
            )
            if scored.decision_eligible:
                eligible += 1
            if not scored.in_domain or scored.predicted_post_value is None:
                abstentions += 1
        conn.commit()
        return ScoreBatchResult(len(rows), len(rows), eligible, abstentions)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def record_prediction_outcome(
    conn,
    *,
    request_key: str,
    response_unit_key: str,
    match_method: str,
    reviewed_by: str,
    review_notes: str | None = None,
) -> int:
    """Append an outcome linked to the prediction that preceded acquisition."""

    if not match_method.strip() or not reviewed_by.strip():
        raise DamageOperationError("match_method and reviewed_by are required")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT request.id, response.id, response.response_value,
                   request.physical_device_key = response.physical_device_key,
                   request.stress_type = response.stress_type,
                   request.target_type = response.target_type,
                   request.measurement_protocol_id = response.measurement_protocol_id,
                   response.stress_features @> request.stress_features,
                   response.post_measured_at
            FROM iv_damage_prediction_requests request
            JOIN iv_damage_response_units response ON response.unit_key = %s
            WHERE request.request_key = %s
            FOR UPDATE OF request, response
            """,
            (response_unit_key, request_key),
        )
        row = cursor.fetchone()
        if row is None:
            raise DamageOperationError("request or response unit does not exist")
        if not all(row[3:8]) or row[8] is None:
            raise DamageOperationError(
                "outcome must match device/stress/target/protocol/stress conditions "
                "and have an authoritative post-measurement timestamp"
            )
        cursor.execute(
            """
            SELECT id
            FROM iv_damage_predictions
            WHERE request_id = %s
              AND created_at < %s
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            FOR UPDATE
            """,
            (row[0], row[8]),
        )
        prediction = cursor.fetchone()
        if prediction is None:
            raise DamageOperationError(
                "no prediction exists from before the post measurement"
            )
        cursor.execute(
            """
            INSERT INTO iv_damage_prediction_outcomes (
                request_id, response_unit_id, prediction_id, observed_response,
                match_method, reviewed_by, review_notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                row[0], row[1], prediction[0], row[2], match_method,
                reviewed_by, review_notes,
            ),
        )
        outcome_id = int(cursor.fetchone()[0])
        conn.commit()
        return outcome_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def monitoring_summary(conn) -> list[dict[str, object]]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT model_run_id, stress_type, target_type,
                   COUNT(*) AS predictions,
                   COUNT(*) FILTER (WHERE observed_response IS NOT NULL) AS matched_outcomes,
                   COUNT(*) FILTER (WHERE support_status <> 'in_domain') AS abstentions,
                   AVG(abs_residual) FILTER (WHERE abs_residual IS NOT NULL) AS mae,
                   AVG(residual) FILTER (WHERE residual IS NOT NULL) AS bias,
                   AVG(interval_hit::integer) FILTER (WHERE interval_hit IS NOT NULL)
                       AS interval_coverage
            FROM iv_damage_prediction_monitoring_view
            GROUP BY model_run_id, stress_type, target_type
            ORDER BY stress_type, target_type, model_run_id
            """
        )
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def assess_monitoring(
    summary: Mapping[str, object], policy: MonitoringPolicy
) -> MonitoringAssessment:
    matched = int(summary.get("matched_outcomes") or 0)
    predictions = int(summary.get("predictions") or 0)
    abstentions = int(summary.get("abstentions") or 0)
    if matched < policy.min_matched_outcomes:
        return MonitoringAssessment(
            "insufficient_outcomes",
            {"matched_outcomes": False},
            ("matched_outcomes",),
        )
    checks = {
        "matched_outcomes": True,
        "mae": policy.max_mae is not None
        and summary.get("mae") is not None
        and float(summary["mae"]) <= policy.max_mae,
        "absolute_bias": policy.max_abs_bias is not None
        and summary.get("bias") is not None
        and abs(float(summary["bias"])) <= policy.max_abs_bias,
        "interval_coverage": summary.get("interval_coverage") is not None
        and float(summary["interval_coverage"]) >= policy.min_interval_coverage,
        "abstention_fraction": predictions > 0
        and abstentions / predictions <= policy.max_abstention_fraction,
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    return MonitoringAssessment("healthy" if not reasons else "alert", checks, reasons)
