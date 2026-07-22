"""Attended CLI for retrospective V3 research audit, training, and status."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
from typing import Sequence

from aps.config import get_settings
from aps.db_config import get_connection
from aps.ml.iv_damage_research_contracts import ScalarOOFPrediction, VALIDATION_SCHEMES
from aps.ml.iv_damage_research_curve import (
    fit_predict_hybrid_fold,
    partial_deterministic_projection,
)
from aps.ml.iv_damage_research_dataset import (
    audit_pair,
    audit_report,
    deterministic_split_assignments,
    load_candidate_pairs,
    target_current_sensitivity,
)
from aps.ml.iv_damage_research_operations import (
    save_artifact_immutable,
    status,
)
from aps.ml.iv_damage_research_repository import (
    freeze_snapshot,
    load_frozen_snapshot_pairs,
    pair_ids,
    persist_assignments,
    persist_curve_run,
    persist_scalar_run,
    scalar_prediction_ids,
    snapshot_identity,
)
from aps.ml.iv_damage_research_scalar import (
    preference_decision,
    run_grouped_scalar_benchmark,
)
from aps.provenance import collect_source_provenance, require_clean_production_source


def _json(value: object) -> None:
    print(json.dumps(value, sort_keys=True, indent=2, default=str))


def _cohort(
    conn, target_current_a: float | None = None, *, source_cutoff: datetime | None = None
):
    candidates = load_candidate_pairs(conn, source_cutoff=source_cutoff)
    sensitivity = target_current_sensitivity(candidates)
    selected = (
        float(target_current_a) if target_current_a is not None else float(sensitivity["selected_target_current_a"])
    )
    audited = [audit_pair(pair, target_current_a=selected) for pair in candidates]
    return audited, sensitivity, selected


def _mutation_provenance(operation: str):
    settings = get_settings()
    provenance = collect_source_provenance()
    require_clean_production_source(settings, provenance, operation=operation)
    return settings, provenance


def command_audit(args) -> dict[str, object]:
    with get_connection() as conn:
        audited, sensitivity, _ = _cohort(conn, args.target_current)
    report = audit_report(audited, sensitivity)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(report, sort_keys=True, indent=2, default=str) + "\n")
    return report


def command_freeze(args) -> dict[str, object]:
    settings, provenance = _mutation_provenance("freeze research snapshot")
    del settings
    cutoff = datetime.fromisoformat(args.source_cutoff.replace("Z", "+00:00"))
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("--source-cutoff must include an explicit timezone")
    with get_connection() as conn:
        audited, sensitivity, selected = _cohort(conn, args.target_current, source_cutoff=cutoff)
        report = audit_report(audited, sensitivity)
        if args.dry_run:
            return {"dry_run": True, **report}
        result = freeze_snapshot(
            conn,
            audited,
            snapshot_version=args.snapshot_version,
            target_current_a=selected,
            source_code_sha=provenance.code_sha,
            source_fingerprint=provenance.fingerprint,
            actor=args.actor,
            extraction_audit=sensitivity,
            source_cutoff=cutoff,
        )
        for scheme in VALIDATION_SCHEMES:
            assignments = deterministic_split_assignments(audited, scheme)
            persist_assignments(
                conn,
                int(result["snapshot_id"]),
                pair_ids(conn, int(result["snapshot_id"])),
                assignments,
            )
    return {**result, "audit": report}


def _artifact_root(settings) -> Path:
    return settings.require_iv_damage_artifact_root(writable=True) / "research"


def command_train_scalar(args) -> dict[str, object]:
    settings, provenance = _mutation_provenance("train research scalar models")
    created = []
    comparison_results = []
    leave_device_run_ids = {}
    preference = {"preferred_method": None, "reason": "leave-device comparison not requested"}
    with get_connection() as conn:
        snapshot_id, _ = snapshot_identity(conn, args.snapshot_version)
        audited = load_frozen_snapshot_pairs(conn, snapshot_id)
        ids = pair_ids(conn, snapshot_id)
        methods = args.methods or ["zero_damage", "v2_donor", "huber", "extra_trees"]
        schemes = args.validation_schemes or list(VALIDATION_SCHEMES)
        for scheme in schemes:
            assignments = deterministic_split_assignments(audited, scheme)
            persist_assignments(conn, snapshot_id, ids, assignments)
            for method in methods:
                result = run_grouped_scalar_benchmark(
                    audited,
                    assignments,
                    method=method,
                    validation_scheme=scheme,
                    seed=args.seed,
                )
                version = f"{args.run_prefix}-{scheme}-{method}"
                artifact = save_artifact_immutable(
                    result,
                    _artifact_root(settings) / args.snapshot_version / f"{version}.joblib",
                    root=settings.require_iv_damage_artifact_root(writable=True),
                )
                run_id = persist_scalar_run(
                    conn,
                    snapshot_id=snapshot_id,
                    ids=ids,
                    run_version=version,
                    result=result,
                    artifact=artifact,
                    source_code_sha=provenance.code_sha,
                    source_fingerprint=provenance.fingerprint,
                    actor=args.actor,
                    seed=args.seed,
                )
                created.append({"run_id": run_id, "run_version": version, "metrics": result.metrics})
                if scheme == "leave_device":
                    comparison_results.append(result)
                    leave_device_run_ids[method] = run_id
        if comparison_results:
            preference = preference_decision(comparison_results)
            preferred_method = preference["preferred_method"]
            if preferred_method is not None:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """UPDATE iv_damage_research_model_runs
                           SET development_status='preferred'
                           WHERE id=%s AND development_status='evaluated'""",
                        (leave_device_run_ids[preferred_method],),
                    )
                conn.commit()
    return {
        "snapshot_version": args.snapshot_version,
        "runs": created,
        "preference": preference,
    }


def _load_scalar_run(conn, run_version: str):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT model.id,model.snapshot_id,model.validation_scheme,model.method,
                      pair.pair_key,pair.physical_device_key,prediction.fold_number,
                      prediction.held_out_group_key,prediction.observed_delta_vth_v,
                      prediction.predicted_delta_vth_v,prediction.support_status,
                      prediction.support_reasons,manifest.training_device_keys
               FROM iv_damage_research_model_runs model
               JOIN iv_damage_research_scalar_predictions prediction
                 ON prediction.model_run_id=model.id
               JOIN iv_damage_research_curve_pairs pair ON pair.id=prediction.curve_pair_id
               JOIN iv_damage_research_fold_manifests manifest
                 ON manifest.id=prediction.fold_manifest_id
               WHERE model.run_version=%s ORDER BY pair.pair_key""",
            (run_version,),
        )
        rows = cursor.fetchall()
        if not rows:
            raise RuntimeError(f"scalar run has no predictions: {run_version}")
        predictions = [
            ScalarOOFPrediction(
                pair_key=str(row[4]),
                validation_scheme=str(row[2]),
                fold_number=int(row[6]),
                held_out_group_key=str(row[7]),
                observed_delta_vth_v=float(row[8]),
                predicted_delta_vth_v=None if row[9] is None else float(row[9]),
                training_device_keys=tuple(row[12]),
                support_status=str(row[10]),
                support_reasons=tuple(row[11]),
            )
            for row in rows
        ]
        return int(rows[0][0]), int(rows[0][1]), str(rows[0][2]), str(rows[0][3]), predictions
    finally:
        cursor.close()


def _comparison_shifts(conn, snapshot_id: int, scheme: str):
    series_by_method = {
        "zero_damage": "zero_damage",
        "v2_donor": "v2_donor_projection",
        "huber": "huber_scalar_projection",
        "extra_trees": "extra_trees_scalar_projection",
    }
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT DISTINCT ON (pair.pair_key,model.method)
                      pair.pair_key,model.method,prediction.predicted_delta_vth_v
               FROM iv_damage_research_model_runs model
               JOIN iv_damage_research_scalar_predictions prediction
                 ON prediction.model_run_id=model.id
               JOIN iv_damage_research_curve_pairs pair
                 ON pair.id=prediction.curve_pair_id
               WHERE model.snapshot_id=%s AND model.validation_scheme=%s
                 AND prediction.support_status='supported'
                 AND prediction.predicted_delta_vth_v IS NOT NULL
               ORDER BY pair.pair_key,model.method,model.created_at DESC""",
            (snapshot_id, scheme),
        )
        result = {}
        for pair_key, method, shift in cursor.fetchall():
            series_name = series_by_method.get(str(method))
            if series_name:
                result.setdefault(str(pair_key), {})[series_name] = float(shift)
        return result
    finally:
        cursor.close()


def command_train_hybrid(args) -> dict[str, object]:
    settings, provenance = _mutation_provenance("train research hybrid curves")
    with get_connection() as conn:
        scalar_run_id, snapshot_id, scheme, scalar_method, predictions = _load_scalar_run(conn, args.scalar_run_version)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT snapshot_version FROM iv_damage_research_snapshots WHERE id=%s",
                (snapshot_id,),
            )
            snapshot_version = cursor.fetchone()[0]
        finally:
            cursor.close()
        audited = load_frozen_snapshot_pairs(conn, snapshot_id)
        by_key = {row.candidate.pair_key: row for row in audited if row.admitted}
        comparison_shifts = _comparison_shifts(conn, snapshot_id, scheme)
        results = []
        for scalar in predictions:
            if scalar.predicted_delta_vth_v is None:
                continue
            held = by_key[scalar.pair_key]
            training = [
                row for row in by_key.values() if row.candidate.physical_device_key in scalar.training_device_keys
            ]
            curve = fit_predict_hybrid_fold(
                training,
                held,
                scalar,
                method=args.method,
                seed=args.seed,
            )
            comparison_series = dict(curve.comparison_series)
            for series_name, shift in comparison_shifts.get(scalar.pair_key, {}).items():
                comparison_series[series_name] = partial_deterministic_projection(
                    held.candidate.pre_points, curve.grid_v, shift
                )
            curve = replace(curve, comparison_series=comparison_series)
            results.append((curve, scalar))
        version = args.run_version
        artifact = save_artifact_immutable(
            results,
            _artifact_root(settings) / str(snapshot_version) / f"{version}.joblib",
            root=settings.require_iv_damage_artifact_root(writable=True),
        )
        run_id = persist_curve_run(
            conn,
            snapshot_id=snapshot_id,
            ids=pair_ids(conn, snapshot_id),
            scalar_ids=scalar_prediction_ids(conn, scalar_run_id),
            run_version=version,
            method=args.method,
            validation_scheme=scheme,
            results=results,
            artifact=artifact,
            source_code_sha=provenance.code_sha,
            source_fingerprint=provenance.fingerprint,
            actor=args.actor,
            seed=args.seed,
        )
    return {
        "run_id": run_id,
        "run_version": version,
        "scalar_method": scalar_method,
        "curves": len(results),
    }


def command_status(_args) -> dict[str, object]:
    with get_connection() as conn:
        return status(conn)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    audit = commands.add_parser("audit-cohort")
    audit.add_argument("--target-current", type=float)
    audit.add_argument("--json-output")
    audit.set_defaults(handler=command_audit)

    freeze = commands.add_parser("freeze-snapshot")
    freeze.add_argument("--snapshot-version", required=True)
    freeze.add_argument("--target-current", type=float)
    freeze.add_argument("--actor", required=True)
    freeze.add_argument("--dry-run", action="store_true")
    freeze.set_defaults(handler=command_freeze)

    scalar = commands.add_parser("train-scalar")
    scalar.add_argument("--snapshot-version", required=True)
    scalar.add_argument("--run-prefix", required=True)
    scalar.add_argument("--actor", required=True)
    scalar.add_argument("--seed", type=int, default=17)
    scalar.add_argument("--methods", nargs="+", choices=["zero_damage", "v2_donor", "huber", "extra_trees"])
    scalar.add_argument("--validation-schemes", nargs="+", choices=list(VALIDATION_SCHEMES))
    scalar.set_defaults(handler=command_train_scalar)

    freeze.add_argument("--source-cutoff", required=True, help="fixed ISO-8601 source boundary")
    hybrid = commands.add_parser("train-hybrid")
    hybrid.add_argument("--scalar-run-version", required=True)
    hybrid.add_argument("--run-version", required=True)
    hybrid.add_argument("--method", required=True, choices=["hybrid_huber", "hybrid_extra_trees"])
    hybrid.add_argument("--actor", required=True)
    hybrid.add_argument("--seed", type=int, default=17)
    hybrid.set_defaults(handler=command_train_hybrid)

    score = commands.add_parser("score-historical")
    score.add_argument("--scalar-run-version", required=True)
    score.add_argument("--run-version", required=True)
    score.add_argument("--method", required=True, choices=["hybrid_huber", "hybrid_extra_trees"])
    score.add_argument("--actor", required=True)
    score.add_argument("--seed", type=int, default=17)
    score.set_defaults(handler=command_train_hybrid)

    commands.add_parser("status").set_defaults(handler=command_status)
    return root


def main(argv: Sequence[str] | None = None) -> None:
    args = parser().parse_args(argv)
    _json(args.handler(args))


if __name__ == "__main__":
    main()
