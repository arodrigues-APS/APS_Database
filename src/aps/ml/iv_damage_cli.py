"""Operational CLI for the prospective IV Damage Predictor V3."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

from psycopg2 import Error as DatabaseError

from aps.config import ConfigurationError, get_settings
from aps.db_config import get_connection
from aps.enrich.iv_parameters.contracts import ExtractionConfig, SweepPoint
from aps.ml.iv_damage_dataset import (
    DatasetSnapshotError,
    create_dataset_snapshot,
)
from aps.ml.iv_damage_evidence import (
    AcceptancePolicySpec,
    DamageEvidenceError,
    ObservationContext,
    ResponseUnitSpec,
    approve_acceptance_policy,
    approve_extraction_method,
    create_acceptance_policy,
    extract_and_persist_rdson,
    extract_and_persist_vth,
    materialize_response_unit,
    register_extraction_method,
)
from aps.ml.iv_damage_operations import (
    DamageOperationError,
    deactivate_release,
    monitoring_summary,
    record_prediction_outcome,
    release_model,
    rollback_release,
    score_pending_requests,
)
from aps.ml.iv_damage_repository import PredictionRequest
from aps.ml.iv_damage_requests import submit_prediction_request
from aps.ml.iv_damage_training import (
    DamageTrainingError,
    readiness_from_database,
    train_snapshot_candidate,
)
from aps.provenance import (
    collect_source_provenance,
    require_clean_production_source,
)


MUTATING_COMMANDS = frozenset(
    {
        "snapshot",
        "register-method",
        "approve-method",
        "extract-observation",
        "materialize-response",
        "create-policy",
        "approve-policy",
        "train",
        "release",
        "rollback",
        "request",
        "score",
        "record-outcome",
    }
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aps-damage", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    register_method = commands.add_parser(
        "register-method",
        help="register an immutable, initially unapproved extraction configuration",
    )
    register_method.add_argument("--config-json", required=True)
    register_method.add_argument("--method-version", default="iv-parameters-v3.0")

    approve_method = commands.add_parser(
        "approve-method",
        help="approve one registered extraction method/configuration",
    )
    approve_method.add_argument("--method-version", required=True)
    approve_method.add_argument("--config-version", required=True)
    approve_method.add_argument(
        "--metric-name", choices=("vth_v", "rdson_mohm"), required=True
    )
    approve_method.add_argument("--actor", required=True)

    observation = commands.add_parser(
        "extract-observation",
        help="extract and persist one governed metric observation from sweep points",
    )
    observation.add_argument("--config-json", required=True)
    observation.add_argument("--context-json", required=True)
    observation.add_argument("--points-json", required=True)

    response = commands.add_parser(
        "materialize-response",
        help="aggregate persisted pre/post observations into one response unit",
    )
    response.add_argument("--spec-json", required=True)

    policy = commands.add_parser(
        "create-policy",
        help="register an immutable, initially unapproved acceptance policy",
    )
    policy.add_argument("--spec-json", required=True)

    approve_policy = commands.add_parser(
        "approve-policy",
        help="approve a complete registered acceptance policy",
    )
    approve_policy.add_argument("--policy-version", required=True)
    approve_policy.add_argument("--actor", required=True)

    snapshot = commands.add_parser("snapshot", help="freeze a dataset and leakage-safe splits")
    snapshot.add_argument("--snapshot-version", required=True)
    snapshot.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    snapshot.add_argument("--target-type", choices=("delta_vth_v", "log_rdson_ratio"), required=True)
    snapshot.add_argument("--extraction-versions-json", required=True)
    snapshot.add_argument("--external-campaign", action="append", required=True)
    snapshot.add_argument("--calibration-campaign", action="append", required=True)
    snapshot.add_argument("--grouped-scheme", action="append", default=[])
    snapshot.add_argument("--n-splits", type=int, default=5)
    snapshot.add_argument("--seed", type=int, default=0)

    readiness = commands.add_parser("readiness", help="inspect persisted evidence readiness")
    readiness.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    readiness.add_argument("--target-type", choices=("delta_vth_v", "log_rdson_ratio"), required=True)

    train = commands.add_parser("train", help="train and validate an immutable snapshot candidate")
    train.add_argument("--snapshot-version", required=True)
    train.add_argument("--policy-version", required=True)
    train.add_argument("--model-version", required=True)
    train.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    train.add_argument("--target-type", choices=("delta_vth_v", "log_rdson_ratio"), required=True)
    train.add_argument("--estimator", choices=("huber", "extra_trees"), default="huber")
    train.add_argument("--release-split-scheme", default="frozen_release")
    train.add_argument("--artifact-directory", type=Path)

    release = commands.add_parser("release", help="explicitly activate a gate-passing model")
    release.add_argument("--model-version", required=True)
    release.add_argument("--actor", required=True)
    release.add_argument("--notes")

    rollback = commands.add_parser("rollback", help="reactivate a prior validated model")
    rollback.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    rollback.add_argument("--target-type", choices=("delta_vth_v", "log_rdson_ratio"), required=True)
    rollback.add_argument("--actor", required=True)
    rollback.add_argument("--to-model-version")
    rollback.add_argument("--notes")

    deactivate = commands.add_parser(
        "deactivate",
        help="emergency-stop an active domain without activating a replacement",
    )
    deactivate.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    deactivate.add_argument(
        "--target-type",
        choices=("delta_vth_v", "log_rdson_ratio"),
        required=True,
    )
    deactivate.add_argument("--actor", required=True)
    deactivate.add_argument("--reason", required=True)

    request = commands.add_parser("request", help="submit an idempotent prospective request")
    request.add_argument("--physical-device-key", required=True)
    request.add_argument("--device-type", required=True)
    request.add_argument("--manufacturer")
    request.add_argument("--measurement-protocol-id", required=True)
    request.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    request.add_argument("--target-type", choices=("delta_vth_v", "log_rdson_ratio"), required=True)
    request.add_argument("--pre-value", required=True, type=float)
    request.add_argument("--pre-uncertainty", type=float)
    request.add_argument("--reference-policy", choices=("same_device", "library_screening"), default="same_device")
    request.add_argument("--features-json", required=True, help="JSON object or @path/to/file.json")
    request.add_argument("--request-source", required=True)
    request.add_argument("--requested-by")
    request.add_argument("--prediction-horizon-s", type=float)

    score = commands.add_parser("score", help="score pending requests with active models only")
    score.add_argument("--limit", type=int, default=500)

    outcome = commands.add_parser("record-outcome", help="append a post-prediction observed outcome")
    outcome.add_argument("--request-key", required=True)
    outcome.add_argument("--response-unit-key", required=True)
    outcome.add_argument("--match-method", required=True)
    outcome.add_argument("--reviewed-by", required=True)
    outcome.add_argument("--notes")

    commands.add_parser("monitor", help="summarize prospective outcomes and abstentions")
    return parser


def _json_features(value: str) -> dict[str, object]:
    parsed, _ = _json_document(value)
    if not isinstance(parsed, dict):
        raise ValueError("features JSON must be an object")
    return parsed


def _json_document(value: str) -> tuple[object, str]:
    payload = (
        Path(value[1:]).read_bytes()
        if value.startswith("@")
        else value.encode("utf-8")
    )
    return json.loads(payload), hashlib.sha256(payload).hexdigest()


def _json_object(value: str, name: str) -> dict[str, object]:
    parsed, _ = _json_document(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} JSON must be an object")
    return parsed


def _extraction_config(value: str) -> ExtractionConfig:
    return ExtractionConfig(**_json_object(value, "config"))


def _observation_context(
    value: str,
    *,
    input_sha256: str,
    source_provenance: dict[str, object],
) -> ObservationContext:
    values = _json_object(value, "context")
    try:
        measured_at = datetime.fromisoformat(
            str(values.pop("measured_at")).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise ValueError(
            "context measured_at must be an ISO-8601 timestamp with timezone"
        ) from exc
    values.pop("source_fingerprint", None)
    return ObservationContext(
        **values,
        measured_at=measured_at,
        source_fingerprint={
            "input_sha256": input_sha256,
            "source_provenance": source_provenance,
        },
    )


def _require_hardened_schema(connection) -> None:
    """Fail with an actionable message instead of a PostgreSQL traceback."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT to_regclass(%s)",
            ("public.iv_damage_dataset_snapshot_members",),
        )
        prepared = cursor.fetchone()[0]
    if prepared is None:
        raise DamageOperationError(
            "V3 damage prediction is not prepared. Apply forward migrations "
            "through schema/034_iv_damage_hardening.sql with aps db migrate."
        )


def _source_for_command(command: str):
    source = collect_source_provenance()
    if command in MUTATING_COMMANDS:
        require_clean_production_source(
            get_settings(),
            source,
            operation=f"aps-damage {command}",
        )
    return source


def dispatch(args: argparse.Namespace):
    source = _source_for_command(args.command)
    if args.command == "register-method":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            method_id = register_extraction_method(
                connection,
                _extraction_config(args.config_json),
                method_version=args.method_version,
            )
        return {"extraction_method_id": method_id}
    if args.command == "approve-method":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            method_id = approve_extraction_method(
                connection,
                method_version=args.method_version,
                config_version=args.config_version,
                metric_name=args.metric_name,
                approved_by=args.actor,
            )
        return {"extraction_method_id": method_id, "approved": True}
    if args.command == "extract-observation":
        config = _extraction_config(args.config_json)
        points_value, points_sha256 = _json_document(args.points_json)
        if not isinstance(points_value, list) or not all(
            isinstance(row, dict) for row in points_value
        ):
            raise ValueError("points JSON must be an array of objects")
        points = [SweepPoint(**row) for row in points_value]
        context = _observation_context(
            args.context_json,
            input_sha256=points_sha256,
            source_provenance=source.as_dict(),
        )
        with get_connection() as connection:
            _require_hardened_schema(connection)
            operation = (
                extract_and_persist_vth
                if config.target_type == "delta_vth_v"
                else extract_and_persist_rdson
            )
            observation_id, result = operation(
                connection, points=points, config=config, context=context
            )
        return {"observation_id": observation_id, "result": asdict(result)}
    if args.command == "materialize-response":
        spec = ResponseUnitSpec(**_json_object(args.spec_json, "response spec"))
        with get_connection() as connection:
            _require_hardened_schema(connection)
            response_id, payload = materialize_response_unit(connection, spec)
        return {"response_unit_id": response_id, "response": asdict(payload)}
    if args.command == "create-policy":
        spec = AcceptancePolicySpec(**_json_object(args.spec_json, "policy spec"))
        with get_connection() as connection:
            _require_hardened_schema(connection)
            policy_id = create_acceptance_policy(connection, spec)
        return {"acceptance_policy_id": policy_id}
    if args.command == "approve-policy":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            policy_id = approve_acceptance_policy(
                connection,
                policy_version=args.policy_version,
                approved_by=args.actor,
            )
        return {"acceptance_policy_id": policy_id, "approved": True}
    if args.command == "snapshot":
        schemes = args.grouped_scheme or [
            "leave_device", "leave_condition", "leave_campaign"
        ]
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(create_dataset_snapshot(
                connection, snapshot_version=args.snapshot_version,
                stress_type=args.stress_type, target_type=args.target_type,
                extraction_method_versions=_json_features(args.extraction_versions_json),
                source_code_sha=str(source.code_sha),
                source_provenance=source.as_dict(),
                external_campaigns=args.external_campaign,
                calibration_campaigns=args.calibration_campaign,
                grouped_schemes=schemes, n_splits=args.n_splits, seed=args.seed,
            ))
    if args.command == "readiness":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(readiness_from_database(
                connection, stress_type=args.stress_type, target_type=args.target_type
            ))
    if args.command == "train":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(train_snapshot_candidate(
                connection, snapshot_version=args.snapshot_version,
                policy_version=args.policy_version, model_version=args.model_version,
                stress_type=args.stress_type, target_type=args.target_type,
                estimator_kind=args.estimator,
                release_split_scheme=args.release_split_scheme,
                code_sha=str(source.code_sha), artifact_directory=args.artifact_directory,
                source_provenance=source.as_dict(),
            ))
    if args.command == "release":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(release_model(
                connection, model_version=args.model_version,
                activated_by=args.actor, release_notes=args.notes,
            ))
    if args.command == "rollback":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(rollback_release(
                connection, stress_type=args.stress_type, target_type=args.target_type,
                activated_by=args.actor, to_model_version=args.to_model_version,
                release_notes=args.notes,
            ))
    if args.command == "deactivate":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(deactivate_release(
                connection,
                stress_type=args.stress_type,
                target_type=args.target_type,
                deactivated_by=args.actor,
                reason=args.reason,
            ))
    if args.command == "request":
        prospective = PredictionRequest(
            physical_device_key=args.physical_device_key,
            device_type=args.device_type, manufacturer=args.manufacturer,
            measurement_protocol_id=args.measurement_protocol_id,
            stress_type=args.stress_type, target_type=args.target_type,
            pre_value=args.pre_value, pre_uncertainty=args.pre_uncertainty,
            reference_policy=args.reference_policy,
            stress_features=_json_features(args.features_json),
            request_source=args.request_source, requested_by=args.requested_by,
            requested_prediction_horizon_s=args.prediction_horizon_s,
        )
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(submit_prediction_request(connection, prospective))
    if args.command == "score":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return asdict(score_pending_requests(connection, limit=args.limit))
    if args.command == "record-outcome":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            outcome_id = record_prediction_outcome(
                connection, request_key=args.request_key,
                response_unit_key=args.response_unit_key,
                match_method=args.match_method, reviewed_by=args.reviewed_by,
                review_notes=args.notes,
            )
        return {"outcome_id": outcome_id}
    if args.command == "monitor":
        with get_connection() as connection:
            _require_hardened_schema(connection)
            return {"domains": monitoring_summary(connection)}
    raise AssertionError(f"unknown command: {args.command}")


def main(argv=None) -> int:
    parser = build_parser()
    try:
        result = dispatch(parser.parse_args(argv))
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    except (
        DamageOperationError, DamageTrainingError, DatasetSnapshotError,
        DamageEvidenceError,
        ConfigurationError, DatabaseError, OSError,
        TypeError, ValueError, json.JSONDecodeError,
    ) as exc:
        print(f"aps-damage: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
