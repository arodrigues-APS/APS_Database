"""Governed persistence and materialization of V3 IV-damage evidence.

Pure extractors know nothing about PostgreSQL. This module binds their results
to approved methods, records authoritative acquisition provenance, and builds
responses only from the observation rows actually stored in the database.

Migration 034 makes evidence append-only. Idempotent writes use INSERT with
ON CONFLICT DO NOTHING followed by exact identity verification; they never
update or silently reinterpret evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from numbers import Real
from typing import Iterable, Mapping, Sequence

from psycopg2.extras import Json

from aps.enrich.iv_parameters.contracts import (
    EXTRACTION_METHOD_VERSION,
    ExtractionConfig,
    MetricResult,
    ReplicateAggregate,
    SweepPoint,
    aggregate_replicates,
    finite,
)
from aps.enrich.iv_parameters.on_resistance import extract_rdson
from aps.enrich.iv_parameters.threshold_voltage import extract_vth
from aps.ml.iv_damage_policy import (
    AcceptancePolicy,
    CURVE_CLAIM_POLICY_FIELDS,
    PROJECTION_CLAIM_POLICY_FIELDS,
    ClaimPolicyError,
    validate_curve_claim_requirements,
    validate_projection_claim_requirements,
)
from aps.ml.iv_damage_readiness import (
    DOMAIN_REQUIRED_FEATURES,
    validate_required_features,
)
from aps.ml.iv_damage_validation import SPLIT_SCHEMES


TARGET_METRICS = {
    "delta_vth_v": ("vth_v", "V"),
    "log_rdson_ratio": ("rdson_mohm", "mohm"),
}
QUALITY_STATUSES = frozenset({"usable", "screening_only", "invalid"})


class DamageEvidenceError(RuntimeError):
    """Evidence cannot be governed or materialized without changing meaning."""


class DamageEvidenceConflict(DamageEvidenceError):
    """An idempotency identity already exists with different content."""


@dataclass(frozen=True)
class ObservationContext:
    metadata_id: int
    measurement_protocol_id: str
    replicate_group_key: str
    measured_at: datetime
    source_fingerprint: Mapping[str, object]


@dataclass(frozen=True)
class StoredObservation:
    id: int
    metadata_id: int
    extraction_method_id: int
    measurement_protocol_id: str
    metric_name: str
    value: float | None
    unit: str
    uncertainty: float | None
    accepted_point_count: int
    replicate_group_key: str
    quality_status: str
    quality_reasons: tuple[str, ...]
    diagnostics: Mapping[str, object]
    source_fingerprint: Mapping[str, object]
    measured_at: datetime
    method_version: str
    config_version: str
    target_type: str
    method_approved: bool


@dataclass(frozen=True)
class ResponseUnitSpec:
    unit_key: str
    physical_device_key: str
    stress_session_key: str
    stress_type: str
    target_type: str
    device_type: str
    measurement_protocol_id: str
    campaign_key: str
    run_key: str
    pre_observation_ids: Sequence[int]
    post_observation_ids: Sequence[int]
    stress_features: Mapping[str, object]
    reference_policy: str = "same_device"
    manufacturer: str | None = None
    ion_species: str | None = None
    baseline_reference_group_key: str | None = None
    minimum_replicates: int = 2


@dataclass(frozen=True)
class ResponseUnitPayload:
    pre_value: float
    pre_uncertainty: float | None
    post_value: float
    post_uncertainty: float | None
    response_value: float
    response_uncertainty: float | None
    pre_replicate_count: int
    post_replicate_count: int
    pre_measured_at: datetime
    post_measured_at: datetime
    required_features_complete: bool
    quality_status: str
    quality_reasons: tuple[str, ...]
    extraction_method_id: int
    method_version: str
    config_version: str


@dataclass(frozen=True)
class AcceptancePolicySpec:
    policy_version: str
    stress_type: str
    target_type: str
    requirements: Mapping[str, object]


def _required_text(value: object, name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise DamageEvidenceError(f"{name} is required")
    return text


def _aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise DamageEvidenceError(f"{name} must be timezone-aware")
    return value


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(item) for item in value]
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(_plain_json(value), sort_keys=True, separators=(",", ":"), default=str)


def _same_float(first: object, second: object) -> bool:
    if first is None or second is None:
        return first is None and second is None
    return float(first) == float(second)


def _real_number(value: object) -> bool:
    return (
        isinstance(value, Real)
        and not isinstance(value, bool)
        and finite(value)
    )


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


def extraction_configuration(config: ExtractionConfig) -> dict[str, object]:
    return asdict(config)


def _target_contract(target_type: str) -> tuple[str, str]:
    try:
        return TARGET_METRICS[target_type]
    except KeyError as exc:
        raise DamageEvidenceError(f"unsupported target_type: {target_type}") from exc


def _validate_method_config(config: ExtractionConfig) -> tuple[str, str]:
    contract = _target_contract(config.target_type)
    _required_text(config.config_version, "config_version")
    numeric_fields = (
        "target_current_a",
        "required_vds_v",
        "vds_tolerance_v",
        "required_vgs_v",
        "vgs_tolerance_v",
        "linear_vds_min_v",
        "linear_vds_max_v",
        "valid_min",
        "valid_max",
    )
    for name in numeric_fields:
        value = getattr(config, name)
        if value is not None and (
            not _real_number(value)
        ):
            raise DamageEvidenceError(
                f"extraction configuration {name} must be finite"
            )
    if config.vds_tolerance_v < 0.0 or config.vgs_tolerance_v < 0.0:
        raise DamageEvidenceError(
            "extraction configuration tolerances must be nonnegative"
        )
    if (
        config.valid_min is not None
        and config.valid_max is not None
        and config.valid_min > config.valid_max
    ):
        raise DamageEvidenceError(
            "extraction configuration valid_min exceeds valid_max"
        )
    if config.target_type == "delta_vth_v":
        if config.target_current_a is None or config.target_current_a <= 0.0:
            raise DamageEvidenceError(
                "Vth extraction requires a positive target_current_a"
            )
        if config.required_vds_v is None:
            raise DamageEvidenceError(
                "Vth extraction requires a finite required_vds_v"
            )
    else:
        if config.required_vgs_v is None:
            raise DamageEvidenceError(
                "Rds(on) extraction requires a finite required_vgs_v"
            )
        if (
            config.linear_vds_min_v < 0.0
            or config.linear_vds_max_v <= config.linear_vds_min_v
        ):
            raise DamageEvidenceError(
                "Rds(on) extraction requires an ordered nonnegative Vds window"
            )
    return contract


def register_extraction_method(
    conn,
    config: ExtractionConfig,
    *,
    method_version: str = EXTRACTION_METHOD_VERSION,
) -> int:
    """Register an unapproved method, or verify an identical registration."""
    metric_name, _ = _validate_method_config(config)
    method_version = _required_text(method_version, "method_version")
    configuration = extraction_configuration(config)

    def operation(cursor):
        cursor.execute(
            """
            INSERT INTO iv_damage_extraction_methods (
                method_version, config_version, metric_name, target_type,
                configuration, approved
            ) VALUES (%s, %s, %s, %s, %s, FALSE)
            ON CONFLICT (method_version, config_version, metric_name) DO NOTHING
            RETURNING id
            """,
            (
                method_version, config.config_version, metric_name,
                config.target_type, Json(configuration),
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            return int(inserted[0])
        cursor.execute(
            """
            SELECT id, target_type, configuration
            FROM iv_damage_extraction_methods
            WHERE method_version = %s AND config_version = %s AND metric_name = %s
            """,
            (method_version, config.config_version, metric_name),
        )
        existing = cursor.fetchone()
        if existing is None:
            raise DamageEvidenceConflict("extraction method conflict could not be resolved")
        if existing[1] != config.target_type or _plain_json(existing[2]) != configuration:
            raise DamageEvidenceConflict(
                "extraction method identity has a different target or configuration"
            )
        return int(existing[0])

    return _transaction(conn, operation)


def approve_extraction_method(
    conn,
    *,
    method_version: str,
    config_version: str,
    metric_name: str,
    approved_by: str,
) -> int:
    """Apply the sole permitted lifecycle transition to a registered method."""
    method_version = _required_text(method_version, "method_version")
    config_version = _required_text(config_version, "config_version")
    approved_by = _required_text(approved_by, "approved_by")
    if metric_name not in {contract[0] for contract in TARGET_METRICS.values()}:
        raise DamageEvidenceError(f"unsupported metric_name: {metric_name}")

    def operation(cursor):
        cursor.execute(
            """
            SELECT id, approved
            FROM iv_damage_extraction_methods
            WHERE method_version = %s AND config_version = %s AND metric_name = %s
            FOR UPDATE
            """,
            (method_version, config_version, metric_name),
        )
        row = cursor.fetchone()
        if row is None:
            raise DamageEvidenceError("extraction method is not registered")
        method_id, approved = int(row[0]), bool(row[1])
        if not approved:
            cursor.execute(
                """
                UPDATE iv_damage_extraction_methods
                SET approved = TRUE, approved_by = %s,
                    approved_at = clock_timestamp()
                WHERE id = %s
                """,
                (approved_by, method_id),
            )
        return method_id

    return _transaction(conn, operation)


def validate_metric_observation(
    result: MetricResult,
    config: ExtractionConfig,
    context: ObservationContext,
) -> None:
    """Validate extractor output and acquisition provenance before any SQL."""
    metric_name, unit = _validate_method_config(config)
    if result.method_version != EXTRACTION_METHOD_VERSION:
        raise DamageEvidenceError("unexpected extractor method_version")
    if result.config_version != config.config_version:
        raise DamageEvidenceError("extractor config_version does not match configuration")
    if result.metric_name != metric_name or result.unit != unit:
        raise DamageEvidenceError("extractor metric/unit does not match target contract")
    if result.quality_status not in QUALITY_STATUSES:
        raise DamageEvidenceError("unsupported extractor quality_status")
    if result.n_points < 0:
        raise DamageEvidenceError("accepted point count cannot be negative")
    if result.value is not None and not finite(result.value):
        raise DamageEvidenceError("metric value must be finite or null")
    if result.quality_status == "usable" and not finite(result.value):
        raise DamageEvidenceError("usable metric result requires a finite value")
    if result.uncertainty is not None and (
        not finite(result.uncertainty) or float(result.uncertainty) < 0.0
    ):
        raise DamageEvidenceError("metric uncertainty must be finite and nonnegative")
    if context.metadata_id <= 0:
        raise DamageEvidenceError("metadata_id must be positive")
    _required_text(context.measurement_protocol_id, "measurement_protocol_id")
    _required_text(context.replicate_group_key, "replicate_group_key")
    _aware(context.measured_at, "measured_at")
    if not context.source_fingerprint:
        raise DamageEvidenceError("source_fingerprint is required")


def _observation_key(context: ObservationContext, result: MetricResult) -> str:
    payload = {
        "metadata_id": context.metadata_id,
        "method_version": result.method_version,
        "config_version": result.config_version,
        "metric_name": result.metric_name,
    }
    return "obs-" + hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def persist_metric_observation(
    conn,
    *,
    config: ExtractionConfig,
    result: MetricResult,
    context: ObservationContext,
) -> int:
    """Persist one pure extractor result under an approved method contract."""
    validate_metric_observation(result, config, context)
    configuration = extraction_configuration(config)
    key = _observation_key(context, result)

    def operation(cursor):
        cursor.execute(
            """
            SELECT id, target_type, configuration, approved
            FROM iv_damage_extraction_methods
            WHERE method_version = %s AND config_version = %s AND metric_name = %s
            """,
            (result.method_version, result.config_version, result.metric_name),
        )
        method = cursor.fetchone()
        if method is None:
            raise DamageEvidenceError("extractor method/configuration is not registered")
        if method[1] != config.target_type or _plain_json(method[2]) != configuration:
            raise DamageEvidenceConflict("registered extraction configuration does not match")
        if not bool(method[3]):
            raise DamageEvidenceError("extraction method is not approved")
        method_id = int(method[0])
        cursor.execute(
            """
            INSERT INTO iv_damage_metric_observations (
                observation_key, metadata_id, extraction_method_id,
                measurement_protocol_id, metric_name, value, unit, uncertainty,
                accepted_point_count, replicate_group_key, quality_status,
                quality_reasons, diagnostics, source_fingerprint, measured_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (metadata_id, extraction_method_id, metric_name) DO NOTHING
            RETURNING id
            """,
            (
                key, context.metadata_id, method_id, context.measurement_protocol_id,
                result.metric_name, result.value, result.unit, result.uncertainty,
                result.n_points, context.replicate_group_key, result.quality_status,
                list(result.quality_reasons), Json(dict(result.diagnostics)),
                Json(dict(context.source_fingerprint)), context.measured_at,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            return int(inserted[0])
        cursor.execute(
            """
            SELECT id, observation_key, measurement_protocol_id, value, unit,
                   uncertainty, accepted_point_count, replicate_group_key,
                   quality_status, quality_reasons, diagnostics,
                   source_fingerprint, measured_at
            FROM iv_damage_metric_observations
            WHERE metadata_id = %s AND extraction_method_id = %s AND metric_name = %s
            """,
            (context.metadata_id, method_id, result.metric_name),
        )
        existing = cursor.fetchone()
        if existing is None:
            raise DamageEvidenceConflict("observation conflict could not be resolved")
        if not (
            existing[1] == key
            and existing[2] == context.measurement_protocol_id
            and _same_float(existing[3], result.value)
            and existing[4] == result.unit
            and _same_float(existing[5], result.uncertainty)
            and existing[6] == result.n_points
            and existing[7] == context.replicate_group_key
            and existing[8] == result.quality_status
            and tuple(existing[9]) == tuple(result.quality_reasons)
            and _plain_json(existing[10]) == _plain_json(result.diagnostics)
            and _plain_json(existing[11]) == _plain_json(context.source_fingerprint)
            and existing[12] == context.measured_at
        ):
            raise DamageEvidenceConflict(
                "observation identity already exists with different evidence"
            )
        return int(existing[0])

    return _transaction(conn, operation)


def extract_and_persist_vth(
    conn,
    *,
    points: Iterable[SweepPoint],
    config: ExtractionConfig,
    context: ObservationContext,
) -> tuple[int, MetricResult]:
    result = extract_vth(points, config)
    return persist_metric_observation(
        conn, config=config, result=result, context=context,
    ), result


def extract_and_persist_rdson(
    conn,
    *,
    points: Iterable[SweepPoint],
    config: ExtractionConfig,
    context: ObservationContext,
) -> tuple[int, MetricResult]:
    result = extract_rdson(points, config)
    return persist_metric_observation(
        conn, config=config, result=result, context=context,
    ), result


def _validate_response_spec(spec: ResponseUnitSpec) -> tuple[str, str]:
    for name in (
        "unit_key", "physical_device_key", "stress_session_key", "device_type",
        "measurement_protocol_id", "campaign_key", "run_key",
    ):
        _required_text(getattr(spec, name), name)
    if spec.stress_type not in DOMAIN_REQUIRED_FEATURES:
        raise DamageEvidenceError(f"unsupported stress_type: {spec.stress_type}")
    contract = _target_contract(spec.target_type)
    if spec.reference_policy not in {"same_device", "library_screening"}:
        raise DamageEvidenceError(f"unsupported reference_policy: {spec.reference_policy}")
    if spec.reference_policy == "library_screening" and not spec.baseline_reference_group_key:
        raise DamageEvidenceError("library screening requires baseline_reference_group_key")
    if spec.minimum_replicates < 1:
        raise DamageEvidenceError("minimum_replicates must be positive")
    pre_ids, post_ids = list(spec.pre_observation_ids), list(spec.post_observation_ids)
    if not pre_ids or not post_ids:
        raise DamageEvidenceError("pre and post observation ids are required")
    if any(not isinstance(value, int) or value <= 0 for value in pre_ids + post_ids):
        raise DamageEvidenceError("observation ids must be positive integers")
    if len(set(pre_ids + post_ids)) != len(pre_ids) + len(post_ids):
        raise DamageEvidenceError("pre and post observation ids must be distinct")
    return contract


def _metric_result(observation: StoredObservation) -> MetricResult:
    return MetricResult(
        metric_name=observation.metric_name,
        value=observation.value,
        unit=observation.unit,
        method_version=observation.method_version,
        config_version=observation.config_version,
        quality_status=observation.quality_status,
        quality_reasons=observation.quality_reasons,
        uncertainty=observation.uncertainty,
        n_points=observation.accepted_point_count,
        diagnostics=observation.diagnostics,
    )


def _response_value(
    target_type: str,
    pre: ReplicateAggregate,
    post: ReplicateAggregate,
) -> tuple[float, float | None]:
    pre_value, post_value = float(pre.value), float(post.value)
    if target_type == "delta_vth_v":
        response = post_value - pre_value
        terms = [value for value in (pre.uncertainty, post.uncertainty) if value is not None]
        uncertainty = math.sqrt(sum(float(value) ** 2 for value in terms)) if terms else None
        return response, uncertainty
    if pre_value <= 0.0 or post_value <= 0.0:
        raise DamageEvidenceError("Rds(on) pre/post values must be positive")
    response = math.log(post_value / pre_value)
    if pre.uncertainty is None and post.uncertainty is None:
        uncertainty = None
    else:
        uncertainty = math.sqrt(
            (float(pre.uncertainty or 0.0) / pre_value) ** 2
            + (float(post.uncertainty or 0.0) / post_value) ** 2
        )
    return response, uncertainty


def build_response_payload(
    spec: ResponseUnitSpec,
    observations: Sequence[StoredObservation],
) -> ResponseUnitPayload:
    """Validate queried observations and recompute the canonical response."""
    metric_name, unit = _validate_response_spec(spec)
    expected_ids = set(spec.pre_observation_ids) | set(spec.post_observation_ids)
    by_id = {row.id: row for row in observations}
    if set(by_id) != expected_ids or len(by_id) != len(observations):
        raise DamageEvidenceError("database did not return each observation exactly once")
    rows = list(by_id.values())
    if any(not row.method_approved for row in rows):
        raise DamageEvidenceError("response units require approved extraction methods")
    method_contracts = {
        (row.extraction_method_id, row.method_version, row.config_version)
        for row in rows
    }
    if len(method_contracts) != 1:
        raise DamageEvidenceError("pre/post observations must use one method/config")
    if any(row.target_type != spec.target_type for row in rows):
        raise DamageEvidenceError("observation target_type does not match response target")
    if any(row.metric_name != metric_name or row.unit != unit for row in rows):
        raise DamageEvidenceError("observation metric/unit does not match response target")
    if any(row.measurement_protocol_id != spec.measurement_protocol_id for row in rows):
        raise DamageEvidenceError("pre/post observations must use the response protocol")
    if any(row.quality_status != "usable" or not finite(row.value) for row in rows):
        raise DamageEvidenceError("response units require finite, usable observations")
    if any(
        row.accepted_point_count <= 0
        or not row.source_fingerprint
        or (
            row.uncertainty is not None
            and (
                not finite(row.uncertainty)
                or float(row.uncertainty) < 0.0
            )
        )
        for row in rows
    ):
        raise DamageEvidenceError(
            "response observations require point, source, and uncertainty provenance"
        )
    if len({row.metadata_id for row in rows}) != len(rows):
        raise DamageEvidenceError("one acquisition cannot count as multiple replicates")

    pre_rows = [by_id[value] for value in spec.pre_observation_ids]
    post_rows = [by_id[value] for value in spec.post_observation_ids]
    if len({row.replicate_group_key for row in pre_rows}) != 1:
        raise DamageEvidenceError("pre observations do not form one replicate group")
    if len({row.replicate_group_key for row in post_rows}) != 1:
        raise DamageEvidenceError("post observations do not form one replicate group")
    if pre_rows[0].replicate_group_key == post_rows[0].replicate_group_key:
        raise DamageEvidenceError("pre and post must be distinct replicate groups")
    for row in rows:
        _aware(row.measured_at, f"observation {row.id} measured_at")
    pre_measured_at = max(row.measured_at for row in pre_rows)
    post_measured_at = min(row.measured_at for row in post_rows)
    if post_measured_at <= pre_measured_at:
        raise DamageEvidenceError(
            "every post observation must be acquired after every pre observation"
        )

    pre = aggregate_replicates(
        [_metric_result(row) for row in pre_rows],
        minimum_replicates=spec.minimum_replicates,
    )
    post = aggregate_replicates(
        [_metric_result(row) for row in post_rows],
        minimum_replicates=spec.minimum_replicates,
    )
    if pre.value is None or post.value is None:
        raise DamageEvidenceError("replicate aggregation did not produce finite values")
    response, response_uncertainty = _response_value(spec.target_type, pre, post)
    if not finite(response) or (
        response_uncertainty is not None and not finite(response_uncertainty)
    ):
        raise DamageEvidenceError("computed response or uncertainty is not finite")

    reasons = [
        reason
        for observation in rows
        for reason in observation.quality_reasons
    ]
    reasons.extend(pre.quality_reasons)
    reasons.extend(post.quality_reasons)
    if spec.reference_policy == "library_screening":
        reasons.append("library_reference_not_training_truth")
    quality_status = (
        "usable"
        if pre.quality_status == post.quality_status == "usable"
        and spec.reference_policy == "same_device"
        else "screening_only"
    )
    complete_values = dict(spec.stress_features)
    complete_values["pre_value"] = float(pre.value)
    stress_condition_key = complete_values.get("stress_condition_key")
    if not isinstance(stress_condition_key, str) or not stress_condition_key.strip():
        raise DamageEvidenceError(
            "stress_features requires a nonempty stress_condition_key"
        )
    feature_reasons = validate_required_features(
        stress_type=spec.stress_type,
        features=complete_values,
    )
    required_complete = not feature_reasons
    reasons.extend(feature_reasons)
    first = rows[0]
    return ResponseUnitPayload(
        pre_value=float(pre.value),
        pre_uncertainty=pre.uncertainty,
        post_value=float(post.value),
        post_uncertainty=post.uncertainty,
        response_value=response,
        response_uncertainty=response_uncertainty,
        pre_replicate_count=pre.replicate_count,
        post_replicate_count=post.replicate_count,
        pre_measured_at=pre_measured_at,
        post_measured_at=post_measured_at,
        required_features_complete=required_complete,
        quality_status=quality_status,
        quality_reasons=tuple(dict.fromkeys(reasons)),
        extraction_method_id=first.extraction_method_id,
        method_version=first.method_version,
        config_version=first.config_version,
    )


_OBSERVATION_SELECT = """
SELECT observation.id, observation.metadata_id, observation.extraction_method_id,
       observation.measurement_protocol_id, observation.metric_name,
       observation.value, observation.unit, observation.uncertainty,
       observation.accepted_point_count, observation.replicate_group_key,
       observation.quality_status, observation.quality_reasons,
       observation.diagnostics, observation.source_fingerprint,
       observation.measured_at, method.method_version, method.config_version,
       method.target_type, method.approved
FROM iv_damage_metric_observations observation
JOIN iv_damage_extraction_methods method ON method.id = observation.extraction_method_id
WHERE observation.id = ANY(%s)
ORDER BY observation.id
FOR SHARE OF observation, method
"""


def _stored_observation(row) -> StoredObservation:
    return StoredObservation(
        id=int(row[0]),
        metadata_id=int(row[1]),
        extraction_method_id=int(row[2]),
        measurement_protocol_id=row[3],
        metric_name=row[4],
        value=row[5],
        unit=row[6],
        uncertainty=row[7],
        accepted_point_count=int(row[8]),
        replicate_group_key=row[9],
        quality_status=row[10],
        quality_reasons=tuple(row[11]),
        diagnostics=dict(row[12]),
        source_fingerprint=dict(row[13]),
        measured_at=row[14],
        method_version=row[15],
        config_version=row[16],
        target_type=row[17],
        method_approved=bool(row[18]),
    )


def _same_response(existing, spec: ResponseUnitSpec, payload: ResponseUnitPayload) -> bool:
    expected_scalars = (
        spec.unit_key,
        spec.physical_device_key,
        spec.stress_session_key,
        spec.stress_type,
        spec.target_type,
        spec.device_type,
        spec.manufacturer,
        spec.measurement_protocol_id,
        spec.campaign_key,
        spec.run_key,
        spec.ion_species,
        list(spec.pre_observation_ids),
        list(spec.post_observation_ids),
    )
    numeric_pairs = (
        (existing[14], payload.pre_value),
        (existing[15], payload.pre_uncertainty),
        (existing[16], payload.post_value),
        (existing[17], payload.post_uncertainty),
        (existing[18], payload.response_value),
        (existing[19], payload.response_uncertainty),
    )
    return (
        tuple(existing[1:14]) == expected_scalars
        and all(_same_float(first, second) for first, second in numeric_pairs)
        and existing[20] == payload.pre_replicate_count
        and existing[21] == payload.post_replicate_count
        and existing[22] == spec.reference_policy
        and existing[23] == spec.baseline_reference_group_key
        and _plain_json(existing[24]) == _plain_json(spec.stress_features)
        and bool(existing[25]) == payload.required_features_complete
        and existing[26] == payload.quality_status
        and tuple(existing[27]) == payload.quality_reasons
        and existing[28] == payload.pre_measured_at
        and existing[29] == payload.post_measured_at
    )


def materialize_response_unit(
    conn,
    spec: ResponseUnitSpec,
) -> tuple[int, ResponseUnitPayload]:
    """Build and append one response unit from persisted observations."""
    _validate_response_spec(spec)
    observation_ids = list(spec.pre_observation_ids) + list(spec.post_observation_ids)

    def operation(cursor):
        cursor.execute(_OBSERVATION_SELECT, (observation_ids,))
        payload = build_response_payload(
            spec, [_stored_observation(row) for row in cursor.fetchall()],
        )
        cursor.execute(
            """
            INSERT INTO iv_damage_response_units (
                unit_key, physical_device_key, stress_session_key, stress_type,
                target_type, device_type, manufacturer, measurement_protocol_id,
                campaign_key, run_key, ion_species, pre_observation_ids,
                post_observation_ids, pre_value, pre_uncertainty, post_value,
                post_uncertainty, response_value, response_uncertainty,
                pre_replicate_count, post_replicate_count, reference_policy,
                baseline_reference_group_key, stress_features,
                required_features_complete, quality_status, quality_reasons,
                pre_measured_at, post_measured_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (physical_device_key, stress_session_key, target_type) DO NOTHING
            RETURNING id
            """,
            (
                spec.unit_key,
                spec.physical_device_key,
                spec.stress_session_key,
                spec.stress_type,
                spec.target_type,
                spec.device_type,
                spec.manufacturer,
                spec.measurement_protocol_id,
                spec.campaign_key,
                spec.run_key,
                spec.ion_species,
                list(spec.pre_observation_ids),
                list(spec.post_observation_ids),
                payload.pre_value,
                payload.pre_uncertainty,
                payload.post_value,
                payload.post_uncertainty,
                payload.response_value,
                payload.response_uncertainty,
                payload.pre_replicate_count,
                payload.post_replicate_count,
                spec.reference_policy,
                spec.baseline_reference_group_key,
                Json(dict(spec.stress_features)),
                payload.required_features_complete,
                payload.quality_status,
                list(payload.quality_reasons),
                payload.pre_measured_at,
                payload.post_measured_at,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            return int(inserted[0]), payload
        cursor.execute(
            """
            SELECT id, unit_key, physical_device_key, stress_session_key, stress_type,
                   target_type, device_type, manufacturer, measurement_protocol_id,
                   campaign_key, run_key, ion_species, pre_observation_ids,
                   post_observation_ids, pre_value, pre_uncertainty, post_value,
                   post_uncertainty, response_value, response_uncertainty,
                   pre_replicate_count, post_replicate_count, reference_policy,
                   baseline_reference_group_key, stress_features,
                   required_features_complete, quality_status, quality_reasons,
                   pre_measured_at, post_measured_at
            FROM iv_damage_response_units
            WHERE physical_device_key = %s AND stress_session_key = %s AND target_type = %s
            """,
            (spec.physical_device_key, spec.stress_session_key, spec.target_type),
        )
        existing = cursor.fetchone()
        if existing is None or not _same_response(existing, spec, payload):
            raise DamageEvidenceConflict(
                "response-unit identity already exists with different evidence"
            )
        return int(existing[0]), payload

    return _transaction(conn, operation)


_POLICY_FIELDS = {field.name for field in fields(AcceptancePolicy)} - {
    "policy_version", "approved",
}
_REQUIRED_POLICY_LIMITS = frozenset(
    {
        "max_median_abs_error",
        "max_p90_abs_error",
        "max_abs_bias",
        "max_catastrophic_error_rate",
        "max_mean_interval_width",
    }
)
_EXTRA_POLICY_FIELDS = frozenset(
    {
        "min_independent_groups",
        "min_physical_devices",
        "min_calibration_groups",
        "min_replicates",
        "max_campaign_share",
        "catastrophic_error_threshold",
        "required_grouped_schemes",
        "interval_coverage",
        "ood_quantile",
        "min_neighbor_devices",
    }
) | CURVE_CLAIM_POLICY_FIELDS | PROJECTION_CLAIM_POLICY_FIELDS

_TRAINING_POLICY_DEFAULTS = {
    "min_physical_devices": 10,
    "min_calibration_groups": 10,
    "min_replicates": 2,
    "max_campaign_share": 0.5,
    "required_grouped_schemes": [
        "leave_device",
        "leave_condition",
        "leave_campaign",
    ],
    "interval_coverage": 0.8,
    "ood_quantile": 0.95,
    "min_neighbor_devices": 2,
}


def resolved_policy_requirements(
    spec: AcceptancePolicySpec,
) -> dict[str, object]:
    """Freeze every code default that contributes to training or release."""
    policy_values = {
        name: spec.requirements[name]
        for name in _POLICY_FIELDS
        if name in spec.requirements
    }
    try:
        policy = AcceptancePolicy(
            policy_version=spec.policy_version,
            **policy_values,
        )
    except TypeError as exc:
        raise DamageEvidenceError(f"invalid acceptance policy: {exc}") from exc
    resolved = asdict(policy)
    resolved.pop("policy_version")
    resolved.pop("approved")
    resolved.update(_TRAINING_POLICY_DEFAULTS)
    resolved.update(dict(spec.requirements))
    if "min_independent_groups" not in spec.requirements:
        try:
            resolved["min_independent_groups"] = (
                policy.min_training_groups
                + policy.min_external_groups
                + int(resolved["min_calibration_groups"])
            )
        except (TypeError, ValueError, OverflowError):
            resolved["min_independent_groups"] = None
    schemes = resolved.get("required_grouped_schemes")
    if isinstance(schemes, tuple):
        resolved["required_grouped_schemes"] = list(schemes)
    return resolved


def validate_policy_spec(
    spec: AcceptancePolicySpec,
    *,
    for_approval: bool = False,
) -> None:
    _required_text(spec.policy_version, "policy_version")
    if spec.stress_type not in DOMAIN_REQUIRED_FEATURES:
        raise DamageEvidenceError(f"unsupported stress_type: {spec.stress_type}")
    _target_contract(spec.target_type)
    unknown = set(spec.requirements) - _POLICY_FIELDS - _EXTRA_POLICY_FIELDS
    if unknown:
        raise DamageEvidenceError(
            "unknown policy requirement(s): " + ", ".join(sorted(unknown))
        )
    resolved = resolved_policy_requirements(spec)
    policy = AcceptancePolicy(
        policy_version=spec.policy_version,
        **{name: resolved[name] for name in _POLICY_FIELDS},
    )
    positive_counts = (
        policy.min_training_groups,
        policy.min_external_groups,
        policy.min_campaigns,
        policy.min_subgroup_groups,
        *(
            resolved.get(name, 1)
            for name in (
                "min_independent_groups",
                "min_physical_devices",
                "min_calibration_groups",
                "min_replicates",
            )
        ),
    )
    try:
        valid_counts = all(
            not isinstance(value, bool) and int(value) == value and int(value) >= 1
            for value in positive_counts
        )
    except (TypeError, ValueError, OverflowError):
        valid_counts = False
    if not valid_counts:
        raise DamageEvidenceError(
            "policy group/count requirements must be positive integers"
        )
    fractions = (
        policy.min_supported_fraction,
        policy.min_baseline_improvement_fraction,
        policy.min_interval_coverage,
        policy.max_interval_coverage,
        resolved["max_campaign_share"],
    )
    if any(
        not _real_number(value) or not 0.0 <= float(value) <= 1.0
        for value in fractions
    ):
        raise DamageEvidenceError(
            "policy fraction requirements must be between zero and one"
        )
    if policy.min_interval_coverage > policy.max_interval_coverage:
        raise DamageEvidenceError("minimum interval coverage exceeds maximum")
    interval_coverage = resolved["interval_coverage"]
    if (
        not _real_number(interval_coverage)
        or not 0.5 < float(interval_coverage) < 1.0
        or not (
            policy.min_interval_coverage
            <= float(interval_coverage)
            <= policy.max_interval_coverage
        )
    ):
        raise DamageEvidenceError(
            "interval_coverage must be within policy bounds and between 0.5 and 1"
        )
    ood_quantile = resolved["ood_quantile"]
    if (
        not _real_number(ood_quantile)
        or not 0.5 <= float(ood_quantile) < 1.0
    ):
        raise DamageEvidenceError("ood_quantile must be in [0.5, 1)")
    min_neighbors = resolved["min_neighbor_devices"]
    if (
        isinstance(min_neighbors, bool)
        or not isinstance(min_neighbors, int)
        or min_neighbors < 1
    ):
        raise DamageEvidenceError("min_neighbor_devices must be a positive integer")
    schemes = resolved["required_grouped_schemes"]
    valid_schemes = (
        isinstance(schemes, (str, bytes))
        or not isinstance(schemes, Sequence)
    )
    if not valid_schemes:
        valid_schemes = (
            bool(schemes)
            and all(
                isinstance(scheme, str) and scheme in SPLIT_SCHEMES
                for scheme in schemes
            )
            and len(set(schemes)) == len(schemes)
        )
    else:
        valid_schemes = False
    if not valid_schemes:
        raise DamageEvidenceError(
            "required_grouped_schemes must be unique supported split schemes"
        )
    try:
        validate_curve_claim_requirements(resolved)
        validate_projection_claim_requirements(resolved)
    except ClaimPolicyError as exc:
        raise DamageEvidenceError(str(exc)) from exc
    if for_approval:
        required_values = _REQUIRED_POLICY_LIMITS | {
            "catastrophic_error_threshold",
        }
        missing = {
            name
            for name in required_values
            if resolved.get(name) is None
        }
        if missing:
            raise DamageEvidenceError(
                "policy cannot be approved without limit(s): "
                + ", ".join(sorted(missing))
            )
        for name in required_values:
            value = resolved[name]
            if (
                not _real_number(value)
                or float(value) < 0.0
                or (
                    name == "catastrophic_error_threshold"
                    and float(value) <= 0.0
                )
                or (
                    name == "max_catastrophic_error_rate"
                    and float(value) > 1.0
                )
            ):
                raise DamageEvidenceError(
                    f"{name} must be finite and nonnegative"
                )


def create_acceptance_policy(conn, spec: AcceptancePolicySpec) -> int:
    """Create an unapproved immutable policy definition idempotently."""
    resolved = resolved_policy_requirements(spec)
    governed_spec = AcceptancePolicySpec(
        spec.policy_version,
        spec.stress_type,
        spec.target_type,
        resolved,
    )
    validate_policy_spec(governed_spec, for_approval=True)

    def operation(cursor):
        cursor.execute(
            """
            INSERT INTO iv_damage_acceptance_policies (
                policy_version, stress_type, target_type, approved, requirements
            ) VALUES (%s, %s, %s, FALSE, %s)
            ON CONFLICT (policy_version) DO NOTHING
            RETURNING id
            """,
            (
                spec.policy_version,
                spec.stress_type,
                spec.target_type,
                Json(resolved),
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            return int(inserted[0])
        cursor.execute(
            """
            SELECT id, stress_type, target_type, requirements
            FROM iv_damage_acceptance_policies WHERE policy_version = %s
            """,
            (spec.policy_version,),
        )
        existing = cursor.fetchone()
        if existing is None:
            raise DamageEvidenceConflict(
                "acceptance policy conflict could not be resolved"
            )
        if (
            existing[1] != spec.stress_type
            or existing[2] != spec.target_type
            or _plain_json(existing[3]) != _plain_json(resolved)
        ):
            raise DamageEvidenceConflict(
                "policy_version already has a different definition"
            )
        return int(existing[0])

    return _transaction(conn, operation)


def approve_acceptance_policy(
    conn,
    *,
    policy_version: str,
    approved_by: str,
) -> int:
    """Validate all release limits, then perform one-way policy approval."""
    policy_version = _required_text(policy_version, "policy_version")
    approved_by = _required_text(approved_by, "approved_by")

    def operation(cursor):
        cursor.execute(
            """
            SELECT id, stress_type, target_type, requirements, approved
            FROM iv_damage_acceptance_policies
            WHERE policy_version = %s
            FOR UPDATE
            """,
            (policy_version,),
        )
        row = cursor.fetchone()
        if row is None:
            raise DamageEvidenceError("acceptance policy is not registered")
        spec = AcceptancePolicySpec(
            policy_version, row[1], row[2], dict(row[3]),
        )
        validate_policy_spec(spec, for_approval=True)
        policy_id, approved = int(row[0]), bool(row[4])
        if not approved:
            cursor.execute(
                """
                UPDATE iv_damage_acceptance_policies
                SET approved = TRUE, approved_by = %s,
                    approved_at = clock_timestamp()
                WHERE id = %s
                """,
                (approved_by, policy_id),
            )
        return policy_id

    return _transaction(conn, operation)
