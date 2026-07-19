"""Certified V3 scalar-evidence and full-curve lifecycle CLI.

This is intentionally separate from the legacy-compatible ``iv_damage_cli``:
every extraction starts from registered database points, and full-curve claims
have their own selection, certification, shadow, and promotion commands.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
import sys

from psycopg2 import Error as DatabaseError
from psycopg2.extras import Json

from aps.config import ConfigurationError, get_settings
from aps.db_config import get_connection
from aps.enrich.iv_parameters.contracts import ExtractionConfig
from aps.ml.iv_damage_curve_operations import (
    CurveMonitoringPolicy,
    CurveOperationError,
    CurvePredictionRequest,
    assess_curve_monitoring,
    promote_curve_model,
    record_curve_outcome,
    score_curve_requests,
    start_curve_shadow,
    submit_curve_request,
)
from aps.ml.iv_damage_curve_training import (
    CurveTrainingError,
    certify_curve_candidate,
    freeze_curve_snapshot_members,
    select_curve_candidate,
    train_curve_candidate,
)
from aps.ml.iv_damage_curves import (
    AcquisitionSpec,
    CurveEvidenceError,
    CurvePairSpec,
    freeze_curve_snapshot,
    load_acquisition_sweep_points,
    materialize_curve_pair,
    project_scalar_prediction,
    register_acquisition,
)
from aps.ml.iv_damage_evidence import (
    DamageEvidenceError,
    ObservationContext,
    ResponseUnitSpec,
    extract_and_persist_rdson,
    extract_and_persist_vth,
    materialize_response_unit,
)
from aps.ml.iv_damage_projection_validation import (
    ProjectionValidationError,
    certify_projection,
    validate_projection_development,
)
from aps.provenance import collect_source_provenance, require_clean_production_source


def _document(value: str):
    raw = Path(value[1:]).read_text() if value.startswith("@") else value
    return json.loads(raw)


def _object(value: str, name: str) -> dict[str, object]:
    result = _document(value)
    if not isinstance(result, dict):
        raise ValueError(f"{name} must be a JSON object")
    return result


def _timestamp(value: object, name: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be ISO-8601") from exc
    if result.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aps-damage-v3", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    acquisition = commands.add_parser("register-acquisition")
    acquisition.add_argument("--spec-json", required=True)

    session = commands.add_parser("register-stress-session")
    session.add_argument("--spec-json", required=True)

    observation = commands.add_parser("extract-observation")
    observation.add_argument("--config-json", required=True)
    observation.add_argument("--acquisition-key", required=True)
    observation.add_argument("--replicate-group-key", required=True)

    response = commands.add_parser("materialize-response")
    response.add_argument("--spec-json", required=True)

    snapshot = commands.add_parser("freeze-curve")
    snapshot.add_argument("--acquisition-key", required=True)
    snapshot.add_argument("--curve-snapshot-key", required=True)

    pair = commands.add_parser("pair-curves")
    pair.add_argument("--spec-json", required=True)

    freeze = commands.add_parser("freeze-curve-dataset")
    freeze.add_argument("--snapshot-version", required=True)

    train = commands.add_parser("train-curve")
    train.add_argument("--snapshot-version", required=True)
    train.add_argument("--policy-version", required=True)
    train.add_argument("--model-version", required=True)
    train.add_argument("--stress-type", choices=("sc", "irradiation"), required=True)
    train.add_argument("--curve-family", choices=("IdVg", "IdVd"), required=True)
    train.add_argument("--measurement-protocol-id", required=True)
    train.add_argument("--artifact-directory", type=Path)

    select = commands.add_parser("select-curve")
    select.add_argument("--model-version", required=True)
    select.add_argument("--actor", required=True)

    certify = commands.add_parser("certify-curve")
    certify.add_argument("--model-version", required=True)
    certify.add_argument("--actor", required=True)

    shadow = commands.add_parser("start-curve-shadow")
    shadow.add_argument("--model-version", required=True)
    shadow.add_argument("--actor", required=True)

    request = commands.add_parser("request-curve")
    request.add_argument("--spec-json", required=True)

    score = commands.add_parser("score-curves")
    score.add_argument("--limit", type=int, default=500)

    outcome = commands.add_parser("record-curve-outcome")
    outcome.add_argument("--curve-prediction-id", type=int, required=True)
    outcome.add_argument("--post-curve-snapshot-id", type=int, required=True)
    outcome.add_argument("--match-method", required=True)
    outcome.add_argument("--actor", required=True)

    monitor = commands.add_parser("assess-curve-shadow")
    monitor.add_argument("--model-version", required=True)
    monitor.add_argument("--policy-json", required=True)
    monitor.add_argument("--window-start", required=True)
    monitor.add_argument("--window-end", required=True)
    monitor.add_argument("--actor", required=True)

    promote = commands.add_parser("promote-curve")
    promote.add_argument("--model-version", required=True)
    promote.add_argument("--actor", required=True)

    method = commands.add_parser("register-projection-method")
    method.add_argument("--method-version", required=True)
    method.add_argument("--projection-kind", choices=("rigid_vth_shift", "linear_rdson_scale"), required=True)
    method.add_argument("--configuration-json", default="{}")

    approve = commands.add_parser("approve-projection-method")
    approve.add_argument("--method-version", required=True)
    approve.add_argument("--actor", required=True)

    validate_projection = commands.add_parser("validate-projection-development")
    validate_projection.add_argument("--method-version", required=True)
    validate_projection.add_argument("--snapshot-version", required=True)

    certify_projection_parser = commands.add_parser("certify-projection")
    certify_projection_parser.add_argument("--method-version", required=True)
    certify_projection_parser.add_argument("--snapshot-version", required=True)
    certify_projection_parser.add_argument("--actor", required=True)

    project = commands.add_parser("project-scalar-curve")
    project.add_argument("--prediction-id", type=int, required=True)
    project.add_argument("--pre-curve-snapshot-id", type=int, required=True)
    project.add_argument("--method-version", required=True)
    return parser


def _register_session(conn, values: dict[str, object]) -> int:
    required = (
        "stress_session_key", "physical_device_key", "stress_type",
        "campaign_key", "run_key", "stress_condition_key", "stress_features",
        "identity_source",
    )
    if any(not values.get(name) for name in required):
        raise ValueError("stress-session spec is incomplete")
    if values["identity_source"] == "manual_review" and (
        not values.get("reviewed_by") or not values.get("review_reason")
    ):
        raise ValueError("manual stress-session identity needs reviewer and reason")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO iv_damage_stress_sessions (
                stress_session_key, physical_device_key, stress_type,
                campaign_key, run_key, stress_condition_key, stress_features,
                started_at, ended_at, identity_source, reviewed_by, reviewed_at,
                review_reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      CASE WHEN %s = 'manual_review' THEN clock_timestamp() ELSE NULL END, %s)
            RETURNING id
            """,
            (
                values["stress_session_key"], values["physical_device_key"],
                values["stress_type"], values["campaign_key"], values["run_key"],
                values["stress_condition_key"], Json(values["stress_features"]),
                _timestamp(values["started_at"], "started_at") if values.get("started_at") else None,
                _timestamp(values["ended_at"], "ended_at") if values.get("ended_at") else None,
                values["identity_source"], values.get("reviewed_by"),
                values["identity_source"], values.get("review_reason"),
            ),
        )
        session_id = int(cursor.fetchone()[0])
    conn.commit()
    return session_id


def dispatch(args: argparse.Namespace):
    source = collect_source_provenance()
    require_clean_production_source(
        get_settings(), source, operation=f"aps-damage-v3 {args.command}"
    )
    with get_connection() as connection:
        if args.command == "register-acquisition":
            values = _object(args.spec_json, "acquisition spec")
            values["measured_at"] = _timestamp(values["measured_at"], "measured_at")
            return asdict(register_acquisition(connection, AcquisitionSpec(**values)))
        if args.command == "register-stress-session":
            return {"stress_session_id": _register_session(connection, _object(args.spec_json, "stress-session spec"))}
        if args.command == "extract-observation":
            config = ExtractionConfig(**_object(args.config_json, "extraction config"))
            acquisition, points = load_acquisition_sweep_points(connection, args.acquisition_key)
            context = ObservationContext(
                metadata_id=acquisition.metadata_id,
                measurement_protocol_id=acquisition.measurement_protocol_id,
                replicate_group_key=args.replicate_group_key,
                measured_at=acquisition.measured_at,
                source_fingerprint={
                    "acquisition_id": acquisition.id,
                    "acquisition_point_payload_hash": acquisition.point_payload_hash,
                    "source_provenance": source.as_dict(),
                },
            )
            operation = extract_and_persist_vth if config.target_type == "delta_vth_v" else extract_and_persist_rdson
            observation_id, result = operation(connection, points=points, config=config, context=context)
            return {"observation_id": observation_id, "result": asdict(result)}
        if args.command == "materialize-response":
            response_id, response = materialize_response_unit(
                connection, ResponseUnitSpec(**_object(args.spec_json, "response spec"))
            )
            return {"response_unit_id": response_id, "response": asdict(response)}
        if args.command == "freeze-curve":
            return asdict(freeze_curve_snapshot(
                connection, acquisition_key=args.acquisition_key,
                curve_snapshot_key=args.curve_snapshot_key,
            ))
        if args.command == "pair-curves":
            return {"curve_response_pair_id": materialize_curve_pair(
                connection, CurvePairSpec(**_object(args.spec_json, "curve pair spec"))
            )}
        if args.command == "freeze-curve-dataset":
            return {"curve_snapshot_members": freeze_curve_snapshot_members(
                connection, snapshot_version=args.snapshot_version
            )}
        if args.command == "train-curve":
            return asdict(train_curve_candidate(
                connection, snapshot_version=args.snapshot_version,
                policy_version=args.policy_version, model_version=args.model_version,
                stress_type=args.stress_type, curve_family=args.curve_family,
                measurement_protocol_id=args.measurement_protocol_id,
                code_sha=f"{source.code_sha}:{source.fingerprint}",
                artifact_directory=args.artifact_directory,
            ))
        if args.command == "select-curve":
            return {"selection_id": select_curve_candidate(connection, model_version=args.model_version, selected_by=args.actor)}
        if args.command == "certify-curve":
            return asdict(certify_curve_candidate(connection, model_version=args.model_version, certified_by=args.actor))
        if args.command == "start-curve-shadow":
            return {"deployment_id": start_curve_shadow(connection, model_version=args.model_version, activated_by=args.actor)}
        if args.command == "request-curve":
            return {"request_id": submit_curve_request(connection, CurvePredictionRequest(**_object(args.spec_json, "curve request")))}
        if args.command == "score-curves":
            return asdict(score_curve_requests(connection, limit=args.limit))
        if args.command == "record-curve-outcome":
            return {"outcome_id": record_curve_outcome(
                connection, curve_prediction_id=args.curve_prediction_id,
                post_curve_snapshot_id=args.post_curve_snapshot_id,
                match_method=args.match_method, reviewed_by=args.actor,
            )}
        if args.command == "assess-curve-shadow":
            assessment, passed, reasons = assess_curve_monitoring(
                connection, model_version=args.model_version,
                policy=CurveMonitoringPolicy(**_object(args.policy_json, "monitoring policy")),
                window_start=_timestamp(args.window_start, "window_start"),
                window_end=_timestamp(args.window_end, "window_end"),
                assessed_by=args.actor,
            )
            return {"assessment_id": assessment, "passed": passed, "reasons": reasons}
        if args.command == "promote-curve":
            return {"deployment_id": promote_curve_model(connection, model_version=args.model_version, activated_by=args.actor)}
        if args.command == "register-projection-method":
            target, family = (
                ("delta_vth_v", "IdVg") if args.projection_kind == "rigid_vth_shift"
                else ("log_rdson_ratio", "IdVd")
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO iv_damage_curve_projection_methods (
                        method_version, projection_kind, target_type, curve_family,
                        configuration
                    ) VALUES (%s, %s, %s, %s, %s) RETURNING id
                    """,
                    (args.method_version, args.projection_kind, target, family, Json(_object(args.configuration_json, "configuration"))),
                )
                method_id = int(cursor.fetchone()[0])
            connection.commit()
            return {"projection_method_id": method_id}
        if args.command == "approve-projection-method":
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE iv_damage_curve_projection_methods
                    SET approved = TRUE, approved_by = %s, approved_at = clock_timestamp()
                    WHERE method_version = %s AND NOT approved RETURNING id
                    """,
                    (args.actor, args.method_version),
                )
                row = cursor.fetchone()
                if row is None:
                    raise CurveOperationError("unapproved projection method does not exist")
            connection.commit()
            return {"projection_method_id": int(row[0]), "approved": True}
        if args.command == "validate-projection-development":
            return {
                "schemes": {name: asdict(metrics) for name, metrics in
                            validate_projection_development(
                                connection, method_version=args.method_version,
                                snapshot_version=args.snapshot_version,
                            ).items()}
            }
        if args.command == "certify-projection":
            certification_id, passed, reasons, metrics = certify_projection(
                connection, method_version=args.method_version,
                snapshot_version=args.snapshot_version, certified_by=args.actor,
            )
            return {
                "certification_id": certification_id, "passed": passed,
                "reasons": reasons, "metrics": asdict(metrics),
            }
        if args.command == "project-scalar-curve":
            projection_id, projection = project_scalar_prediction(
                connection, prediction_id=args.prediction_id,
                pre_curve_snapshot_id=args.pre_curve_snapshot_id,
                method_version=args.method_version,
            )
            return {"curve_projection_id": projection_id, "projection": asdict(projection)}
    raise AssertionError(args.command)


def main(argv=None) -> int:
    try:
        result = dispatch(build_parser().parse_args(argv))
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    except (
        ConfigurationError, DatabaseError, CurveEvidenceError,
        CurveTrainingError, CurveOperationError, DamageEvidenceError,
        ProjectionValidationError,
        OSError, TypeError, ValueError, json.JSONDecodeError,
    ) as exc:
        print(f"aps-damage-v3: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
