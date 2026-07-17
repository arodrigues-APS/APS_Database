"""Operational CLI for the prospective IV Damage Predictor V3."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

from aps.db_config import get_connection
from aps.ml.iv_damage_dataset import (
    DatasetSnapshotError,
    create_dataset_snapshot,
)
from aps.ml.iv_damage_operations import (
    DamageOperationError,
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
from aps.provenance import collect_source_provenance


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aps-damage", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

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
    if value.startswith("@"):
        payload = Path(value[1:]).read_text()
    else:
        payload = value
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("features JSON must be an object")
    return parsed


def dispatch(args: argparse.Namespace):
    if args.command == "snapshot":
        source = collect_source_provenance()
        schemes = args.grouped_scheme or [
            "leave_device", "leave_condition", "leave_campaign"
        ]
        with get_connection() as connection:
            return asdict(create_dataset_snapshot(
                connection, snapshot_version=args.snapshot_version,
                stress_type=args.stress_type, target_type=args.target_type,
                extraction_method_versions=_json_features(args.extraction_versions_json),
                source_code_sha=str(source.code_sha),
                external_campaigns=args.external_campaign,
                calibration_campaigns=args.calibration_campaign,
                grouped_schemes=schemes, n_splits=args.n_splits, seed=args.seed,
            ))
    if args.command == "readiness":
        with get_connection() as connection:
            return asdict(readiness_from_database(
                connection, stress_type=args.stress_type, target_type=args.target_type
            ))
    if args.command == "train":
        source = collect_source_provenance()
        with get_connection() as connection:
            return asdict(train_snapshot_candidate(
                connection, snapshot_version=args.snapshot_version,
                policy_version=args.policy_version, model_version=args.model_version,
                stress_type=args.stress_type, target_type=args.target_type,
                estimator_kind=args.estimator,
                release_split_scheme=args.release_split_scheme,
                code_sha=str(source.code_sha), artifact_directory=args.artifact_directory,
            ))
    if args.command == "release":
        with get_connection() as connection:
            return asdict(release_model(
                connection, model_version=args.model_version,
                activated_by=args.actor, release_notes=args.notes,
            ))
    if args.command == "rollback":
        with get_connection() as connection:
            return asdict(rollback_release(
                connection, stress_type=args.stress_type, target_type=args.target_type,
                activated_by=args.actor, to_model_version=args.to_model_version,
                release_notes=args.notes,
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
            return asdict(submit_prediction_request(connection, prospective))
    if args.command == "score":
        with get_connection() as connection:
            return asdict(score_pending_requests(connection, limit=args.limit))
    if args.command == "record-outcome":
        with get_connection() as connection:
            outcome_id = record_prediction_outcome(
                connection, request_key=args.request_key,
                response_unit_key=args.response_unit_key,
                match_method=args.match_method, reviewed_by=args.reviewed_by,
                review_notes=args.notes,
            )
        return {"outcome_id": outcome_id}
    if args.command == "monitor":
        with get_connection() as connection:
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
        ValueError, json.JSONDecodeError,
    ) as exc:
        print(f"aps-damage: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
