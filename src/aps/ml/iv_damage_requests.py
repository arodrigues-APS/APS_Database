"""Idempotent submission boundary for prospective prediction requests."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg2.extras import Json

from aps.ml.iv_damage_repository import PredictionRequest, request_key


@dataclass(frozen=True)
class SubmittedRequest:
    request_id: int
    request_key: str
    created: bool


def submit_prediction_request(conn, request: PredictionRequest) -> SubmittedRequest:
    """Insert a prospective request once; an exact replay returns the same id."""

    key = request_key(request)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO iv_damage_prediction_requests (
                request_key, physical_device_key, device_type, manufacturer,
                measurement_protocol_id, stress_type, target_type, pre_value,
                pre_uncertainty, reference_policy, stress_features,
                requested_prediction_horizon_s, request_source, requested_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (request_key) DO NOTHING
            RETURNING id
            """,
            (
                key, request.physical_device_key, request.device_type,
                request.manufacturer, request.measurement_protocol_id,
                request.stress_type, request.target_type, request.pre_value,
                request.pre_uncertainty, request.reference_policy,
                Json(dict(request.stress_features)),
                request.requested_prediction_horizon_s, request.request_source,
                request.requested_by,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            request_id = int(inserted[0])
            created = True
        else:
            cursor.execute(
                "SELECT id FROM iv_damage_prediction_requests WHERE request_key = %s",
                (key,),
            )
            request_id = int(cursor.fetchone()[0])
            created = False
        conn.commit()
        return SubmittedRequest(request_id, key, created)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
