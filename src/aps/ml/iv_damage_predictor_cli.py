"""Complete certified V3 CLI: governed evidence, scalar, and full curves."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
import sys

from psycopg2 import Error as DatabaseError

from aps.config import ConfigurationError, get_settings
from aps.db_config import get_connection
from aps.ml import iv_damage_v3_cli as curve_cli
from aps.ml.iv_damage_operations import (
    DamageOperationError, MonitoringPolicy, score_pending_requests,
)
from aps.ml.iv_damage_scalar_certification import (
    certify_scalar_candidate,
    select_scalar_candidate,
    train_scalar_development_candidate,
)
from aps.ml.iv_damage_scalar_shadow import (
    assess_scalar_shadow,
    promote_scalar_model,
    record_scalar_outcomes_for_all_models,
    score_scalar_shadow_requests,
    start_scalar_shadow,
)
from aps.ml.iv_damage_training import DamageTrainingError
from aps.ml.iv_damage_curves import CurveEvidenceError
from aps.ml.iv_damage_curve_training import CurveTrainingError
from aps.ml.iv_damage_curve_operations import CurveOperationError
from aps.ml.iv_damage_evidence import DamageEvidenceError
from aps.ml.iv_damage_manifest import (
    EvidenceManifestError,
    apply_evidence,
    approve_evidence,
    evidence_status,
    plan_evidence,
    write_plan,
)
from aps.ml.iv_damage_projection_validation import ProjectionValidationError
from aps.provenance import collect_source_provenance, require_clean_production_source


SCALAR_COMMANDS = {
    "train-scalar-development", "select-scalar", "certify-scalar",
    "start-scalar-shadow", "score-scalar-shadow", "score-scalar-all",
    "assess-scalar-shadow",
    "promote-scalar", "record-scalar-outcomes",
}
EVIDENCE_COMMANDS = {
    "evidence-plan", "evidence-approve", "evidence-apply", "evidence-status",
}


def build_parser() -> argparse.ArgumentParser:
    parser = curve_cli.build_parser()
    parser.prog = "aps-damage-predictor"
    subparsers = next(
        action for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    train = subparsers.add_parser(
        "train-scalar-development",
        help="train/select using development roles without fetching external outcomes",
    )
    train.add_argument("--snapshot-version", required=True)
    train.add_argument("--policy-version", required=True)
    train.add_argument("--model-version", required=True)
    train.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    train.add_argument("--target-type", choices=("delta_vth_v", "log_rdson_ratio"), required=True)
    train.add_argument("--estimator", choices=("huber", "extra_trees"), default="huber")
    train.add_argument("--artifact-directory")

    select = subparsers.add_parser("select-scalar")
    select.add_argument("--model-version", required=True)
    select.add_argument("--actor", required=True)

    certify = subparsers.add_parser("certify-scalar")
    certify.add_argument("--model-version", required=True)
    certify.add_argument("--actor", required=True)

    shadow = subparsers.add_parser("start-scalar-shadow")
    shadow.add_argument("--model-version", required=True)
    shadow.add_argument("--actor", required=True)

    score = subparsers.add_parser("score-scalar-shadow")
    score.add_argument("--limit", type=int, default=500)

    score_all = subparsers.add_parser(
        "score-scalar-all",
        help="score shadow first without consuming requests, then active decision models",
    )
    score_all.add_argument("--limit", type=int, default=500)

    assess = subparsers.add_parser("assess-scalar-shadow")
    assess.add_argument("--model-version", required=True)
    assess.add_argument("--policy-json", required=True)
    assess.add_argument("--window-start", required=True)
    assess.add_argument("--window-end", required=True)
    assess.add_argument("--actor", required=True)

    outcomes = subparsers.add_parser(
        "record-scalar-outcomes",
        help="attach one prospective response to every earlier model prediction",
    )
    outcomes.add_argument("--request-key", required=True)
    outcomes.add_argument("--response-unit-key", required=True)
    outcomes.add_argument("--match-method", required=True)
    outcomes.add_argument("--actor", required=True)
    outcomes.add_argument("--notes")

    promote = subparsers.add_parser("promote-scalar")
    promote.add_argument("--model-version", required=True)
    promote.add_argument("--actor", required=True)

    evidence_plan = subparsers.add_parser(
        "evidence-plan",
        help="audit a manifest without mutating PostgreSQL and persist its canonical plan",
    )
    evidence_plan.add_argument("--manifest", required=True)
    evidence_plan.add_argument("--report-json", type=Path, required=True)

    for name in ("evidence-approve", "evidence-apply"):
        evidence = subparsers.add_parser(name)
        evidence.add_argument("--batch-key", required=True)
        evidence.add_argument("--expected-plan-sha", required=True)
        evidence.add_argument("--actor", required=True)

    evidence_status_parser = subparsers.add_parser("evidence-status")
    evidence_status_parser.add_argument("--batch-key", required=True)
    return parser


def _timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("monitoring timestamps require a timezone")
    return result


def dispatch(args: argparse.Namespace):
    if args.command in EVIDENCE_COMMANDS:
        settings = get_settings()
        source = collect_source_provenance()
        if args.command in {"evidence-approve", "evidence-apply"}:
            require_clean_production_source(
                settings, source, operation=f"aps-damage-predictor {args.command}"
            )
        with get_connection() as connection:
            if args.command == "evidence-plan":
                manifest = curve_cli._object(args.manifest, "evidence manifest")
                report = plan_evidence(connection, manifest)
                plan_path = write_plan(
                    report,
                    settings.require_iv_damage_governance_root(writable=True),
                    args.report_json,
                )
                return {**report, "canonical_plan_path": str(plan_path)}
            if args.command == "evidence-approve":
                return approve_evidence(
                    connection,
                    governance_root=settings.require_iv_damage_governance_root(),
                    batch_key=args.batch_key, expected_plan_sha=args.expected_plan_sha,
                    actor=args.actor,
                )
            if args.command == "evidence-apply":
                return apply_evidence(
                    connection,
                    batch_key=args.batch_key, expected_plan_sha=args.expected_plan_sha,
                    actor=args.actor, source_provenance=source.as_dict(),
                )
            return evidence_status(connection, args.batch_key)

    if args.command not in SCALAR_COMMANDS:
        return curve_cli.dispatch(args)
    source = collect_source_provenance()
    require_clean_production_source(
        get_settings(), source, operation=f"aps-damage-predictor {args.command}"
    )
    with get_connection() as connection:
        if args.command == "train-scalar-development":
            return asdict(train_scalar_development_candidate(
                connection, snapshot_version=args.snapshot_version,
                policy_version=args.policy_version, model_version=args.model_version,
                stress_type=args.stress_type, target_type=args.target_type,
                estimator_kind=args.estimator,
                code_sha=f"{source.code_sha}:{source.fingerprint}",
                artifact_directory=Path(args.artifact_directory) if args.artifact_directory else None,
            ))
        if args.command == "select-scalar":
            return {"selection_id": select_scalar_candidate(
                connection, model_version=args.model_version, selected_by=args.actor
            )}
        if args.command == "certify-scalar":
            return asdict(certify_scalar_candidate(
                connection, model_version=args.model_version, certified_by=args.actor
            ))
        if args.command == "start-scalar-shadow":
            return {"deployment_id": start_scalar_shadow(
                connection, model_version=args.model_version, activated_by=args.actor
            )}
        if args.command == "score-scalar-shadow":
            return asdict(score_scalar_shadow_requests(connection, limit=args.limit))
        if args.command == "score-scalar-all":
            shadow = score_scalar_shadow_requests(connection, limit=args.limit)
            decision = score_pending_requests(connection, limit=args.limit)
            return {"shadow": asdict(shadow), "decision": asdict(decision)}
        if args.command == "assess-scalar-shadow":
            policy_values = curve_cli._object(args.policy_json, "monitoring policy")
            assessment_id, passed, reasons = assess_scalar_shadow(
                connection, model_version=args.model_version,
                policy=MonitoringPolicy(**policy_values),
                window_start=_timestamp(args.window_start),
                window_end=_timestamp(args.window_end), assessed_by=args.actor,
            )
            return {"assessment_id": assessment_id, "passed": passed, "reasons": reasons}
        if args.command == "record-scalar-outcomes":
            outcome_ids = record_scalar_outcomes_for_all_models(
                connection, request_key=args.request_key,
                response_unit_key=args.response_unit_key,
                match_method=args.match_method, reviewed_by=args.actor,
                review_notes=args.notes,
            )
            return {"outcome_ids": outcome_ids, "predictions_evaluated": len(outcome_ids)}
        if args.command == "promote-scalar":
            return {"release_id": promote_scalar_model(
                connection, model_version=args.model_version, activated_by=args.actor
            )}
    raise AssertionError(args.command)


def main(argv=None) -> int:
    try:
        result = dispatch(build_parser().parse_args(argv))
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    except (
        ConfigurationError, DatabaseError, DamageTrainingError,
        DamageOperationError, CurveEvidenceError, CurveTrainingError,
        CurveOperationError, DamageEvidenceError, ProjectionValidationError,
        OSError, TypeError, ValueError,
        EvidenceManifestError,
        json.JSONDecodeError,
    ) as exc:
        print(f"aps-damage-predictor: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
