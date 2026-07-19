"""Authoritative acquisition, immutable curve, pairing, and projection writers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import hashlib
import json
import math
from typing import Sequence

from psycopg2.extras import Json, execute_values

from aps.enrich.iv_parameters.contracts import SweepPoint
from aps.ml.iv_damage_curve_model import CurvePrediction, deterministic_curve_projection


class CurveEvidenceError(RuntimeError):
    """Curve evidence is missing, conflicting, or not authoritative."""


@dataclass(frozen=True)
class AcquisitionSpec:
    acquisition_key: str
    metadata_id: int
    physical_device_key: str
    measurement_protocol_id: str
    curve_family: str
    measured_at: datetime
    identity_source: str = "metadata_exact"
    reviewed_by: str | None = None
    review_reason: str | None = None


@dataclass(frozen=True)
class AcquisitionRecord:
    id: int
    acquisition_key: str
    metadata_id: int
    physical_device_key: str
    device_type: str
    manufacturer: str | None
    measurement_protocol_id: str
    curve_family: str
    measured_at: datetime
    point_payload_hash: str
    point_count: int


@dataclass(frozen=True)
class CurveSnapshotRecord:
    id: int
    curve_snapshot_key: str
    acquisition_id: int
    curve_family: str
    measurement_protocol_id: str
    x_v: tuple[float, ...]
    i_drain_a: tuple[float, ...]
    point_payload_hash: str


@dataclass(frozen=True)
class CurvePairSpec:
    pair_key: str
    response_unit_id: int
    pre_curve_snapshot_id: int
    post_curve_snapshot_id: int
    quality_status: str = "usable"
    quality_reasons: Sequence[str] = ()


def _required(value: object, name: str) -> str:
    result = str(value).strip() if value is not None else ""
    if not result:
        raise CurveEvidenceError(f"{name} is required")
    return result


def _aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CurveEvidenceError(f"{name} must be timezone-aware")
    return value


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _transaction(conn, operation):
    cursor = conn.cursor()
    try:
        result = operation(cursor)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _measurement_rows(cursor, metadata_id: int) -> list[tuple[int, float | None, float | None, float]]:
    cursor.execute(
        """
        SELECT point_index, v_gate, v_drain, i_drain
        FROM baselines_measurements
        WHERE metadata_id = %s
        ORDER BY point_index, id
        """,
        (metadata_id,),
    )
    rows = []
    indexes = set()
    for point_index, v_gate, v_drain, i_drain in cursor.fetchall():
        if point_index in indexes:
            raise CurveEvidenceError("baseline acquisition contains duplicate point_index")
        indexes.add(point_index)
        values = (v_gate, v_drain, i_drain)
        if i_drain is None or not math.isfinite(float(i_drain)):
            raise CurveEvidenceError("baseline acquisition contains non-finite drain current")
        if any(value is not None and not math.isfinite(float(value)) for value in values):
            raise CurveEvidenceError("baseline acquisition contains non-finite voltage/current")
        rows.append((int(point_index), v_gate, v_drain, float(i_drain)))
    if len(rows) < 3:
        raise CurveEvidenceError("baseline acquisition requires at least three points")
    return rows


def _point_payload(rows: Sequence[tuple[int, float | None, float | None, float]]) -> list[dict[str, object]]:
    return [
        {
            "point_index": index,
            "v_gate": None if vg is None else float(vg),
            "v_drain": None if vd is None else float(vd),
            "i_drain": float(current),
        }
        for index, vg, vd, current in rows
    ]


def register_acquisition(conn, spec: AcquisitionSpec) -> AcquisitionRecord:
    """Bind one metadata row and its exact point payload to reviewed identity."""
    _required(spec.acquisition_key, "acquisition_key")
    _required(spec.physical_device_key, "physical_device_key")
    _required(spec.measurement_protocol_id, "measurement_protocol_id")
    _aware(spec.measured_at, "measured_at")
    if spec.metadata_id <= 0:
        raise CurveEvidenceError("metadata_id must be positive")
    if spec.curve_family not in {"IdVg", "IdVd"}:
        raise CurveEvidenceError("curve_family must be IdVg or IdVd")
    if spec.identity_source not in {"metadata_exact", "manual_review"}:
        raise CurveEvidenceError("identity_source must be metadata_exact or manual_review")
    if spec.identity_source == "manual_review" and (
        not str(spec.reviewed_by or "").strip() or not str(spec.review_reason or "").strip()
    ):
        raise CurveEvidenceError("manual identity requires reviewed_by and review_reason")

    def operation(cursor):
        cursor.execute(
            """
            SELECT device_id, device_type, manufacturer, file_hash
            FROM baselines_metadata WHERE id = %s FOR SHARE
            """,
            (spec.metadata_id,),
        )
        metadata = cursor.fetchone()
        if metadata is None:
            raise CurveEvidenceError("baselines_metadata row does not exist")
        metadata_device, device_type, manufacturer, file_hash = metadata
        if not str(device_type or "").strip():
            raise CurveEvidenceError("metadata device_type is required")
        if not str(file_hash or "").strip():
            raise CurveEvidenceError("metadata file_hash is required")
        if spec.identity_source == "metadata_exact" and str(metadata_device or "").strip() != spec.physical_device_key:
            raise CurveEvidenceError(
                "physical_device_key differs from metadata device_id; use a reviewed manual identity"
            )
        rows = _measurement_rows(cursor, spec.metadata_id)
        payload_hash = _hash(_point_payload(rows))
        identity_evidence = {
            "metadata_device_id": metadata_device,
            "asserted_physical_device_key": spec.physical_device_key,
            "review_reason": spec.review_reason,
        }
        cursor.execute(
            """
            INSERT INTO iv_damage_acquisitions (
                acquisition_key, metadata_id, physical_device_key, device_type,
                manufacturer, measurement_protocol_id, curve_family, measured_at,
                source_file_hash, point_payload_hash, point_count, identity_source,
                identity_evidence, reviewed_by, reviewed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, CASE WHEN %s = 'manual_review' THEN clock_timestamp() ELSE NULL END)
            ON CONFLICT (metadata_id) DO NOTHING
            RETURNING id
            """,
            (
                spec.acquisition_key, spec.metadata_id, spec.physical_device_key,
                device_type, manufacturer, spec.measurement_protocol_id,
                spec.curve_family, spec.measured_at, file_hash, payload_hash,
                len(rows), spec.identity_source, Json(identity_evidence),
                spec.reviewed_by, spec.identity_source,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None and spec.identity_source == "manual_review":
            cursor.execute(
                """
                INSERT INTO iv_damage_acquisition_identity_reviews (
                    acquisition_id, metadata_device_id, asserted_physical_device_key,
                    decision, reason, reviewed_by
                ) VALUES (%s, %s, %s, 'accepted', %s, %s)
                """,
                (inserted[0], metadata_device, spec.physical_device_key, spec.review_reason, spec.reviewed_by),
            )
        cursor.execute(
            """
            SELECT id, acquisition_key, metadata_id, physical_device_key,
                   device_type, manufacturer, measurement_protocol_id, curve_family,
                   measured_at, point_payload_hash, point_count, source_file_hash,
                   identity_source
            FROM iv_damage_acquisitions WHERE metadata_id = %s
            """,
            (spec.metadata_id,),
        )
        existing = cursor.fetchone()
        expected = (
            spec.acquisition_key, spec.metadata_id, spec.physical_device_key,
            device_type, manufacturer, spec.measurement_protocol_id,
            spec.curve_family, spec.measured_at, payload_hash, len(rows),
            file_hash, spec.identity_source,
        )
        if existing is None or tuple(existing[1:]) != expected:
            raise CurveEvidenceError("metadata_id is already bound to different acquisition evidence")
        return AcquisitionRecord(
            int(existing[0]), existing[1], int(existing[2]), existing[3], existing[4],
            existing[5], existing[6], existing[7], existing[8], existing[9], int(existing[10]),
        )

    return _transaction(conn, operation)


def get_acquisition(conn, acquisition_key: str) -> AcquisitionRecord:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id, acquisition_key, metadata_id, physical_device_key,
                   device_type, manufacturer, measurement_protocol_id, curve_family,
                   measured_at, point_payload_hash, point_count
            FROM iv_damage_acquisitions WHERE acquisition_key = %s
            """,
            (acquisition_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise CurveEvidenceError("authoritative acquisition does not exist")
        return AcquisitionRecord(
            int(row[0]), row[1], int(row[2]), row[3], row[4], row[5], row[6], row[7],
            row[8], row[9], int(row[10]),
        )
    finally:
        cursor.close()


def load_acquisition_sweep_points(conn, acquisition_key: str) -> tuple[AcquisitionRecord, list[SweepPoint]]:
    acquisition = get_acquisition(conn, acquisition_key)
    cursor = conn.cursor()
    try:
        rows = _measurement_rows(cursor, acquisition.metadata_id)
    finally:
        cursor.close()
    if _hash(_point_payload(rows)) != acquisition.point_payload_hash:
        raise CurveEvidenceError("baseline points changed after acquisition registration")
    points = [SweepPoint(index, vg, vd, current, False) for index, vg, vd, current in rows]
    return acquisition, points


def freeze_curve_snapshot(conn, *, acquisition_key: str, curve_snapshot_key: str) -> CurveSnapshotRecord:
    """Copy an authoritative acquisition into append-only curve point evidence."""
    acquisition, points = load_acquisition_sweep_points(conn, acquisition_key)
    axis = [point.v_gate if acquisition.curve_family == "IdVg" else point.v_drain for point in points]
    if any(value is None or not math.isfinite(float(value)) for value in axis):
        raise CurveEvidenceError("curve family axis is missing or non-finite")
    ordered = sorted(zip((float(value) for value in axis), (float(point.i_drain) for point in points)))
    if any(first[0] >= second[0] for first, second in zip(ordered, ordered[1:])):
        raise CurveEvidenceError("curve voltage axis must be unique and strictly increasing")
    payload = [{"point_index": i, "x_value_v": x, "i_drain_a": current} for i, (x, current) in enumerate(ordered)]
    payload_hash = _hash(payload)

    def operation(cursor):
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_snapshots (
                curve_snapshot_key, acquisition_id, curve_family,
                measurement_protocol_id, x_unit, current_unit, point_count,
                point_payload_hash
            ) VALUES (%s, %s, %s, %s, 'V', 'A', %s, %s)
            ON CONFLICT (acquisition_id) DO NOTHING RETURNING id
            """,
            (
                curve_snapshot_key, acquisition.id, acquisition.curve_family,
                acquisition.measurement_protocol_id, len(payload), payload_hash,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            execute_values(
                cursor,
                """
                INSERT INTO iv_damage_curve_snapshot_points (
                    curve_snapshot_id, point_index, x_value, i_drain_a
                ) VALUES %s
                """,
                [(inserted[0], row["point_index"], row["x_value_v"], row["i_drain_a"]) for row in payload],
            )
            snapshot_id = int(inserted[0])
        else:
            cursor.execute(
                """
                SELECT id, curve_snapshot_key, point_payload_hash, point_count
                FROM iv_damage_curve_snapshots WHERE acquisition_id = %s
                """,
                (acquisition.id,),
            )
            row = cursor.fetchone()
            if row is None or (row[1], row[2], int(row[3])) != (curve_snapshot_key, payload_hash, len(payload)):
                raise CurveEvidenceError("acquisition already has a different curve snapshot")
            snapshot_id = int(row[0])
        return CurveSnapshotRecord(
            snapshot_id, curve_snapshot_key, acquisition.id,
            acquisition.curve_family, acquisition.measurement_protocol_id,
            tuple(row["x_value_v"] for row in payload),
            tuple(row["i_drain_a"] for row in payload), payload_hash,
        )

    return _transaction(conn, operation)


def load_curve_snapshot(conn, snapshot_id: int) -> CurveSnapshotRecord:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT curve.id, curve.curve_snapshot_key, curve.acquisition_id,
                   curve.curve_family, curve.measurement_protocol_id,
                   curve.point_payload_hash, point.point_index, point.x_value,
                   point.i_drain_a
            FROM iv_damage_curve_snapshots curve
            JOIN iv_damage_curve_snapshot_points point ON point.curve_snapshot_id = curve.id
            WHERE curve.id = %s ORDER BY point.point_index
            """,
            (snapshot_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            raise CurveEvidenceError("curve snapshot does not exist")
        payload = [
            {"point_index": int(row[6]), "x_value_v": float(row[7]), "i_drain_a": float(row[8])}
            for row in rows
        ]
        if _hash(payload) != rows[0][5]:
            raise CurveEvidenceError("curve snapshot point hash mismatch")
        first = rows[0]
        return CurveSnapshotRecord(
            int(first[0]), first[1], int(first[2]), first[3], first[4],
            tuple(row["x_value_v"] for row in payload),
            tuple(row["i_drain_a"] for row in payload), first[5],
        )
    finally:
        cursor.close()


def materialize_curve_pair(conn, spec: CurvePairSpec) -> int:
    if spec.quality_status not in {"usable", "screening_only", "invalid"}:
        raise CurveEvidenceError("invalid curve-pair quality_status")

    def operation(cursor):
        cursor.execute(
            """
            SELECT pre.curve_family, pre.measurement_protocol_id,
                   post.curve_family, post.measurement_protocol_id
            FROM iv_damage_curve_snapshots pre
            CROSS JOIN iv_damage_curve_snapshots post
            WHERE pre.id = %s AND post.id = %s
            """,
            (spec.pre_curve_snapshot_id, spec.post_curve_snapshot_id),
        )
        identity = cursor.fetchone()
        if identity is None or identity[0] != identity[2] or identity[1] != identity[3]:
            raise CurveEvidenceError("pre/post curve snapshots must share family and protocol")
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_response_pairs (
                pair_key, response_unit_id, pre_curve_snapshot_id,
                post_curve_snapshot_id, curve_family, measurement_protocol_id,
                quality_status, quality_reasons
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (response_unit_id) DO NOTHING RETURNING id
            """,
            (
                spec.pair_key, spec.response_unit_id, spec.pre_curve_snapshot_id,
                spec.post_curve_snapshot_id, identity[0], identity[1],
                spec.quality_status, list(spec.quality_reasons),
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            return int(inserted[0])
        cursor.execute(
            """
            SELECT id, pair_key, pre_curve_snapshot_id, post_curve_snapshot_id,
                   quality_status, quality_reasons
            FROM iv_damage_curve_response_pairs WHERE response_unit_id = %s
            """,
            (spec.response_unit_id,),
        )
        row = cursor.fetchone()
        expected = (
            spec.pair_key, spec.pre_curve_snapshot_id, spec.post_curve_snapshot_id,
            spec.quality_status, list(spec.quality_reasons),
        )
        if row is None or tuple(row[1:]) != expected:
            raise CurveEvidenceError("response unit already has different curve-pair evidence")
        return int(row[0])

    return _transaction(conn, operation)


def project_scalar_prediction(
    conn,
    *,
    prediction_id: int,
    pre_curve_snapshot_id: int,
    method_version: str,
) -> tuple[int, CurvePrediction]:
    """Persist an approved deterministic projection; never certify it implicitly."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT prediction.predicted_response, prediction.predicted_response_lower,
                   prediction.predicted_response_upper, prediction.evidence_status,
                   prediction.decision_eligible, request.physical_device_key,
                   request.measurement_protocol_id, request.target_type,
                   method.id, method.projection_kind, method.curve_family, method.approved,
                   acquisition.physical_device_key, curve.measurement_protocol_id,
                   curve.curve_family,
                   EXISTS (
                       SELECT 1
                       FROM iv_damage_curve_projection_certifications certification
                       WHERE certification.projection_method_id = method.id
                         AND certification.passed
                   ) AS projection_certified
            FROM iv_damage_predictions prediction
            JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
            JOIN iv_damage_curve_projection_methods method ON method.method_version = %s
            JOIN iv_damage_curve_snapshots curve ON curve.id = %s
            JOIN iv_damage_acquisitions acquisition ON acquisition.id = curve.acquisition_id
            WHERE prediction.id = %s
            FOR SHARE OF prediction, request, method, curve, acquisition
            """,
            (method_version, pre_curve_snapshot_id, prediction_id),
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None:
        raise CurveEvidenceError("prediction, curve, or projection method does not exist")
    if not row[11]:
        raise CurveEvidenceError("curve projection method is not approved")
    if row[5] != row[12] or row[6] != row[13] or row[10] != row[14]:
        raise CurveEvidenceError("projection curve does not match request device/protocol/family")
    if any(value is None for value in row[:3]):
        raise CurveEvidenceError("abstained scalar prediction cannot project a curve")
    curve = load_curve_snapshot(conn, pre_curve_snapshot_id)
    projected = deterministic_curve_projection(
        projection_kind=row[9], x_v=curve.x_v, pre_i_a=curve.i_drain_a,
        response=float(row[0]), response_lower=float(row[1]), response_upper=float(row[2]),
    )
    decision_eligible = bool(row[4] and row[15] and projected.in_domain)
    projection_evidence = (
        "decision_eligible" if decision_eligible
        else "screening_only" if projected.in_domain
        else projected.evidence_status
    )
    projection_reasons = tuple(projected.reasons)
    if projected.in_domain and not row[15]:
        projection_reasons = (*projection_reasons, "projection_not_externally_certified")
    projected = replace(
        projected, evidence_status=projection_evidence,
        reasons=tuple(sorted(set(projection_reasons))),
    )

    def operation(cursor):
        cursor.execute(
            """
            INSERT INTO iv_damage_curve_projections (
                prediction_id, pre_curve_snapshot_id, projection_method_id,
                projection_status, evidence_status, decision_eligible, reasons
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (prediction_id, pre_curve_snapshot_id, projection_method_id)
            DO NOTHING RETURNING id
            """,
            (
                prediction_id, pre_curve_snapshot_id, row[8],
                "projected" if projected.in_domain else "abstained",
                projected.evidence_status, decision_eligible,
                list(projected.reasons),
            ),
        )
        inserted = cursor.fetchone()
        if inserted is None:
            cursor.execute(
                """
                SELECT id FROM iv_damage_curve_projections
                WHERE prediction_id = %s AND pre_curve_snapshot_id = %s
                  AND projection_method_id = %s
                """,
                (prediction_id, pre_curve_snapshot_id, row[8]),
            )
            existing = cursor.fetchone()
            if existing is None:
                raise CurveEvidenceError("projection conflict could not be resolved")
            return int(existing[0])
        projection_id = int(inserted[0])
        if projected.in_domain:
            execute_values(
                cursor,
                """
                INSERT INTO iv_damage_curve_projection_points (
                    curve_projection_id, point_index, x_value_v, pre_i_drain_a,
                    predicted_i_drain_a, predicted_lower_a, predicted_upper_a
                ) VALUES %s
                """,
                [
                    (projection_id, index, x, pre, predicted, lower, upper)
                    for index, (x, pre, predicted, lower, upper) in enumerate(zip(
                        projected.x_v, projected.pre_i_a, projected.predicted_i_a,
                        projected.lower_i_a, projected.upper_i_a,
                    ))
                ],
            )
        return projection_id

    return _transaction(conn, operation), projected
