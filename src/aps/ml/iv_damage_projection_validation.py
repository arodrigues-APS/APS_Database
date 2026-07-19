"""Curve-level validation and one-time certification for scalar projections."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Mapping, Sequence

import numpy as np
from psycopg2.extras import Json

from aps.ml.iv_damage_curve_model import deterministic_curve_projection
from aps.ml.iv_damage_policy import (
    ClaimPolicyError,
    validate_projection_claim_requirements,
)


class ProjectionValidationError(RuntimeError):
    """A deterministic projection lacks valid independent curve evidence."""


@dataclass(frozen=True)
class ProjectionCurveMetric:
    curve_response_pair_id: int
    mae_a: float
    max_abs_error_a: float
    normalized_rmse: float
    simultaneous_band_hit: bool


@dataclass(frozen=True)
class ProjectionMetrics:
    curves: int
    physical_devices: int
    mean_mae_a: float | None
    p90_max_abs_error_a: float | None
    mean_normalized_rmse: float | None
    simultaneous_band_coverage: float | None


def _projection_requirements(
    value: Mapping[str, object],
) -> dict[str, object]:
    requirements = dict(value)
    try:
        validate_projection_claim_requirements(requirements, required=True)
    except ClaimPolicyError as exc:
        raise ProjectionValidationError(str(exc)) from exc
    return requirements


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _rows(
    conn, *, snapshot_id: int, split_scheme: str, roles: Sequence[str],
) -> list[tuple[Mapping[str, object], str, int | None]]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT member.frozen_payload, member.payload_hash,
                   assignment.split_role, assignment.fold_number
            FROM iv_damage_curve_snapshot_members member
            JOIN iv_damage_split_assignments assignment
              ON assignment.dataset_snapshot_id = member.dataset_snapshot_id
             AND assignment.response_unit_id = member.response_unit_id
            WHERE member.dataset_snapshot_id = %s
              AND assignment.split_scheme = %s
              AND assignment.split_role = ANY(%s)
            ORDER BY member.curve_response_pair_id
            """,
            (snapshot_id, split_scheme, list(roles)),
        )
        result = []
        for payload_value, expected_hash, role, fold in cursor.fetchall():
            payload = dict(payload_value)
            if _hash(payload) != expected_hash:
                raise ProjectionValidationError("curve snapshot payload hash mismatch")
            result.append((payload, role, fold))
        return result
    finally:
        cursor.close()


def _metric(payload: Mapping[str, object], projection_kind: str) -> ProjectionCurveMetric:
    response = dict(payload["response"])
    uncertainty = float(response.get("response_uncertainty") or 0.0)
    value = float(response["response_value"])
    pre = list(payload["pre_points"])
    post = list(payload["post_points"])
    projection = deterministic_curve_projection(
        projection_kind=projection_kind,
        x_v=[float(point[0]) for point in pre],
        pre_i_a=[float(point[1]) for point in pre],
        response=value, response_lower=value - uncertainty,
        response_upper=value + uncertainty,
    )
    if not projection.in_domain:
        raise ProjectionValidationError(
            f"projection abstained for curve pair {payload['pair_key']}: {projection.reasons}"
        )
    post_x = np.asarray([float(point[0]) for point in post])
    post_i = np.asarray([float(point[1]) for point in post])
    grid = np.asarray(projection.x_v)
    if post_x[0] > grid[0] or post_x[-1] < grid[-1]:
        raise ProjectionValidationError("post curve does not cover projected grid")
    truth = np.interp(grid, post_x, post_i)
    predicted = np.asarray(projection.predicted_i_a)
    residual = predicted - truth
    scale = max(float(np.ptp(truth)), float(np.max(np.abs(truth))), 1e-15)
    return ProjectionCurveMetric(
        int(payload["curve_response_pair_id"]),
        float(np.mean(np.abs(residual))), float(np.max(np.abs(residual))),
        float(np.sqrt(np.mean(residual ** 2)) / scale),
        bool(np.all((truth >= projection.lower_i_a) & (truth <= projection.upper_i_a))),
    )


def _summary(rows: Sequence[tuple[Mapping[str, object], ProjectionCurveMetric]]) -> ProjectionMetrics:
    metrics = [metric for _, metric in rows]
    devices = {dict(payload["response"])["physical_device_key"] for payload, _ in rows}
    return ProjectionMetrics(
        len(rows), len(devices),
        float(np.mean([row.mae_a for row in metrics])) if metrics else None,
        float(np.quantile([row.max_abs_error_a for row in metrics], 0.9)) if metrics else None,
        float(np.mean([row.normalized_rmse for row in metrics])) if metrics else None,
        float(np.mean([row.simultaneous_band_hit for row in metrics])) if metrics else None,
    )


def _gate(metrics: ProjectionMetrics, requirements: Mapping[str, object], *, external: bool) -> tuple[bool, dict[str, bool], tuple[str, ...]]:
    checks = {
        "curves": metrics.curves >= int(requirements[
            "projection_min_external_curves"
            if external else "projection_min_development_curves"
        ]),
        "physical_devices": metrics.physical_devices >= int(requirements[
            "projection_min_external_devices"
            if external else "projection_min_development_devices"
        ]),
        "mean_mae_a": metrics.mean_mae_a is not None and metrics.mean_mae_a <= float(requirements["projection_max_mean_mae_a"]),
        "p90_max_abs_error_a": metrics.p90_max_abs_error_a is not None and metrics.p90_max_abs_error_a <= float(requirements["projection_max_p90_error_a"]),
        "normalized_rmse": metrics.mean_normalized_rmse is not None and metrics.mean_normalized_rmse <= float(requirements["projection_max_normalized_rmse"]),
        "simultaneous_band_coverage": metrics.simultaneous_band_coverage is not None and metrics.simultaneous_band_coverage >= float(requirements["projection_min_band_coverage"]),
    }
    reasons = tuple(name for name, passed in checks.items() if not passed)
    return not reasons, checks, reasons


def _identity(conn, method_version: str, snapshot_version: str):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT method.id, method.projection_kind, method.approved,
                   snapshot.id, policy.requirements
            FROM iv_damage_curve_projection_methods method
            CROSS JOIN iv_damage_dataset_snapshots snapshot
            JOIN iv_damage_acceptance_policies policy
              ON policy.stress_type = snapshot.domain_summary->>'stress_type'
             AND policy.target_type = method.target_type
             AND policy.approved
            WHERE method.method_version = %s AND snapshot.snapshot_version = %s
            ORDER BY policy.approved_at DESC LIMIT 1
            """,
            (method_version, snapshot_version),
        )
        result = cursor.fetchone()
        if result is None or not result[2]:
            raise ProjectionValidationError("approved method/snapshot/policy combination does not exist")
        return result
    finally:
        cursor.close()


def validate_projection_development(conn, *, method_version: str, snapshot_version: str) -> dict[str, ProjectionMetrics]:
    method_id, kind, _, snapshot_id, requirements_value = _identity(conn, method_version, snapshot_version)
    requirements = _projection_requirements(requirements_value)
    schemes = tuple(requirements.get("required_grouped_schemes", ("leave_device", "leave_condition", "leave_campaign")))
    summaries = {}
    cursor = conn.cursor()
    try:
        for scheme in schemes:
            source = _rows(conn, snapshot_id=int(snapshot_id), split_scheme=scheme, roles=("grouped_test", "train"))
            evaluated = [(payload, _metric(payload, kind)) for payload, _, _ in source]
            summary = _summary(evaluated)
            passed, _, reasons = _gate(summary, requirements, external=False)
            if not passed:
                raise ProjectionValidationError(f"{scheme} projection gate failed: {', '.join(reasons)}")
            summaries[scheme] = summary
            for payload, metric in evaluated:
                cursor.execute(
                    """
                    INSERT INTO iv_damage_curve_projection_validations (
                        projection_method_id, curve_response_pair_id,
                        dataset_snapshot_id, split_scheme, split_role,
                        evaluation_kind, mae_a, max_abs_error_a,
                        normalized_rmse, simultaneous_band_hit
                    ) VALUES (%s, %s, %s, %s, 'grouped_test',
                              'development_cv', %s, %s, %s, %s)
                    ON CONFLICT (projection_method_id, curve_response_pair_id, split_scheme)
                    DO NOTHING
                    """,
                    (
                        method_id, metric.curve_response_pair_id, snapshot_id,
                        scheme, metric.mae_a, metric.max_abs_error_a,
                        metric.normalized_rmse, metric.simultaneous_band_hit,
                    ),
                )
        conn.commit()
        return summaries
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def certify_projection(conn, *, method_version: str, snapshot_version: str, certified_by: str) -> tuple[int, bool, tuple[str, ...], ProjectionMetrics]:
    method_id, kind, _, snapshot_id, requirements_value = _identity(conn, method_version, snapshot_version)
    requirements = _projection_requirements(requirements_value)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT count(DISTINCT split_scheme)
            FROM iv_damage_curve_projection_validations
            WHERE projection_method_id = %s AND dataset_snapshot_id = %s
              AND evaluation_kind = 'development_cv'
            """,
            (method_id, snapshot_id),
        )
        if int(cursor.fetchone()[0]) < len(requirements.get("required_grouped_schemes", ("leave_device", "leave_condition", "leave_campaign"))):
            raise ProjectionValidationError("development projection diagnostics are incomplete")
        cursor.execute(
            "SELECT 1 FROM iv_damage_curve_projection_certifications WHERE dataset_snapshot_id = %s",
            (snapshot_id,),
        )
        if cursor.fetchone() is not None:
            raise ProjectionValidationError("external curve holdout has already been consumed")
    finally:
        cursor.close()
    source = _rows(conn, snapshot_id=int(snapshot_id), split_scheme="frozen_release", roles=("external_test",))
    evaluated = [(payload, _metric(payload, kind)) for payload, _, _ in source]
    summary = _summary(evaluated)
    passed, checks, reasons = _gate(summary, requirements, external=True)
    cursor = conn.cursor()
    try:
        for _, metric in evaluated:
            cursor.execute(
                """
                INSERT INTO iv_damage_curve_projection_validations (
                    projection_method_id, curve_response_pair_id,
                    dataset_snapshot_id, split_scheme, split_role,
                    evaluation_kind, mae_a, max_abs_error_a,
                    normalized_rmse, simultaneous_band_hit
                ) VALUES (%s, %s, %s, 'frozen_release', 'external_test',
                          'external_certification', %s, %s, %s, %s)
                """,
                (
                    method_id, metric.curve_response_pair_id, snapshot_id,
                    metric.mae_a, metric.max_abs_error_a,
                    metric.normalized_rmse, metric.simultaneous_band_hit,
                ),
            )
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_projection_certifications (
                projection_method_id, dataset_snapshot_id, metrics,
                gate_checks, passed, certified_by
            ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (method_id, snapshot_id, Json(asdict(summary)), Json(checks), passed, certified_by),
        )
        certification_id = int(cursor.fetchone()[0])
        conn.commit()
        return certification_id, passed, reasons, summary
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
