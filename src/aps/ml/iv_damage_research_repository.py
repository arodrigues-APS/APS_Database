"""Transactional repository for immutable V3 research evidence."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping, Sequence

from psycopg2.extras import Json, execute_values

from aps.ml.iv_damage_research_contracts import (
    AuditedPair,
    ArtifactIdentity,
    RESEARCH_PROTOCOL_ID,
    ResearchPair,
    ResearchPoint,
)
from aps.ml.iv_damage_research_curve import CurvePredictionResult
from aps.ml.iv_damage_research_dataset import (
    CANDIDATE_QUERY,
    extraction_config_payload,
    pair_identity_payload,
    point_payload,
    point_payload_hash,
    sha256_payload,
    snapshot_payload,
)
from aps.ml.iv_damage_research_operations import (
    ResearchOperationError,
    require_schema,
)
from aps.ml.iv_damage_research_scalar import ScalarRunResult


def _ranges(row: AuditedPair):
    pre = [point.v_gate_v for point in row.candidate.pre_points]
    post = [point.v_gate_v for point in row.candidate.post_points]
    return min(pre), max(pre), min(post), max(post)


def snapshot_identity(conn, snapshot_version: str) -> tuple[int, float]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id, target_current_a FROM iv_damage_research_snapshots WHERE snapshot_version=%s",
            (snapshot_version,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ResearchOperationError(f"unknown research snapshot: {snapshot_version}")
        return int(row[0]), float(row[1])
    finally:
        cursor.close()


def pair_ids(conn, snapshot_id: int) -> dict[str, int]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT pair_key,id FROM iv_damage_research_curve_pairs "
            "WHERE snapshot_id=%s AND admission_status='admitted'",
            (snapshot_id,),
        )
        return {str(key): int(identity) for key, identity in cursor.fetchall()}
    finally:
        cursor.close()


def verify_snapshot_sources(conn, snapshot_id: int, audited: Sequence[AuditedPair]) -> None:
    """Fail closed when frozen membership or either raw point payload changed."""
    current = {
        row.candidate.pair_key: (
            row.pre_point_hash,
            row.post_point_hash,
            row.pair_payload_hash,
        )
        for row in audited
        if row.admitted
    }
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT pair_key,pre_point_hash,post_point_hash,pair_payload_hash
               FROM iv_damage_research_curve_pairs
               WHERE snapshot_id=%s AND admission_status='admitted'""",
            (snapshot_id,),
        )
        frozen = {str(row[0]): (str(row[1]), str(row[2]), str(row[3])) for row in cursor.fetchall()}
    finally:
        cursor.close()
    if current.keys() != frozen.keys():
        raise ResearchOperationError("current raw cohort membership differs from the immutable snapshot")
    changed = sorted(key for key in current if current[key] != frozen[key])
    if changed:
        raise ResearchOperationError("raw point payload changed after snapshot freeze: " + ", ".join(changed[:8]))


def load_frozen_snapshot_pairs(conn, snapshot_id: int) -> list[AuditedPair]:
    """Reconstruct and verify the only data permitted for snapshot training."""
    require_schema(conn)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT snapshot_version,research_protocol_id,target_current_a,
                      source_cutoff,source_code_sha,source_fingerprint,snapshot_hash,
                      pair_count,device_count,campaign_count,run_count
               FROM iv_damage_research_snapshots WHERE id=%s""",
            (snapshot_id,),
        )
        snapshot = cursor.fetchone()
        if snapshot is None:
            raise ResearchOperationError(f"unknown research snapshot id: {snapshot_id}")
        if str(snapshot[1]) != RESEARCH_PROTOCOL_ID:
            raise ResearchOperationError("frozen snapshot protocol does not match the research contract")
        target_current_a = float(snapshot[2])
        cursor.execute(
            """SELECT id,source_pair_id,pair_key,pre_feature_id,post_feature_id,
                      pre_metadata_id,post_metadata_id,pre_point_hash,post_point_hash,
                      pair_payload_hash,physical_device_key,device_type,manufacturer,
                      campaign_key,run_key,ion_species,beam_energy_mev,let_surface,
                      range_um,beam_type,fluence,pre_vds_v,post_vds_v,
                      extraction_config,pre_vth_v,post_vth_v,observed_delta_vth_v,
                      extraction_diagnostics,common_grid_point_count,admission_status,
                      exclusion_reasons
               FROM iv_damage_research_curve_pairs
               WHERE snapshot_id=%s ORDER BY pair_key""",
            (snapshot_id,),
        )
        pair_rows = cursor.fetchall()
        audited: list[AuditedPair] = []
        for stored in pair_rows:
            cursor.execute(
                """SELECT source_point_id,source_point_index,point_order,v_gate_v,
                          v_drain_v,i_drain_a,point_hash,curve_role
                   FROM iv_damage_research_curve_pair_points
                   WHERE curve_pair_id=%s ORDER BY curve_role,point_order""",
                (stored[0],),
            )
            by_role: dict[str, list[ResearchPoint]] = {"pre": [], "post": []}
            for point_row in cursor.fetchall():
                point = ResearchPoint(
                    int(point_row[0]),
                    int(point_row[1]),
                    float(point_row[3]),
                    float(point_row[5]),
                    None if point_row[4] is None else float(point_row[4]),
                )
                if sha256_payload(point_payload((point,))[0]) != str(point_row[6]):
                    raise ResearchOperationError(f"frozen point hash mismatch for pair {stored[2]}")
                by_role[str(point_row[7])].append(point)
            pre_points = tuple(by_role["pre"])
            post_points = tuple(by_role["post"])
            if not pre_points or not post_points:
                raise ResearchOperationError(f"frozen pair is missing curve points: {stored[2]}")
            pair = ResearchPair(
                source_pair_id=int(stored[1]),
                pair_key=str(stored[2]),
                pre_feature_id=int(stored[3]),
                post_feature_id=int(stored[4]),
                pre_metadata_id=int(stored[5]),
                post_metadata_id=int(stored[6]),
                physical_device_key=str(stored[10]),
                device_type=str(stored[11]),
                manufacturer=stored[12],
                campaign_key=stored[13],
                run_key=stored[14],
                ion_species=stored[15],
                beam_energy_mev=stored[16],
                let_surface=stored[17],
                range_um=stored[18],
                beam_type=stored[19],
                fluence=stored[20],
                pre_vds_v=stored[21],
                post_vds_v=stored[22],
                pre_points=pre_points,
                post_points=post_points,
            )
            expected_config = extraction_config_payload(target_current_a)
            if dict(stored[23]) != expected_config:
                raise ResearchOperationError(f"frozen extraction contract mismatch for pair {pair.pair_key}")
            pre_hash = point_payload_hash(pre_points)
            post_hash = point_payload_hash(post_points)
            if pre_hash != str(stored[7]) or post_hash != str(stored[8]):
                raise ResearchOperationError(f"frozen curve payload hash mismatch for pair {pair.pair_key}")
            diagnostics = dict(stored[27])
            reasons = tuple(stored[30])
            admitted = str(stored[29]) == "admitted"
            pre_vth = None if stored[24] is None else float(stored[24])
            post_vth = None if stored[25] is None else float(stored[25])
            delta = None if stored[26] is None else float(stored[26])
            common_count = int(stored[28])
            pair_hash = sha256_payload(
                pair_identity_payload(
                    pair,
                    target_current_a=target_current_a,
                    pre_point_hash=pre_hash,
                    post_point_hash=post_hash,
                    pre_vth_v=pre_vth,
                    post_vth_v=post_vth,
                    observed_delta_vth_v=delta,
                    extraction_diagnostics=diagnostics,
                    common_grid_point_count=common_count,
                    admitted=admitted,
                    exclusion_reasons=reasons,
                )
            )
            if pair_hash != str(stored[9]):
                raise ResearchOperationError(f"frozen pair identity hash mismatch for {pair.pair_key}")
            audited.append(
                AuditedPair(
                    pair,
                    admitted,
                    reasons,
                    pre_hash,
                    post_hash,
                    pair_hash,
                    pre_vth,
                    post_vth,
                    delta,
                    diagnostics,
                    common_count,
                )
            )

        identity = snapshot_payload(
            audited,
            snapshot_version=str(snapshot[0]),
            target_current_a=target_current_a,
            source_code_sha=str(snapshot[4]),
            source_fingerprint=str(snapshot[5]),
            source_cutoff=snapshot[3],
        )
        if identity["snapshot_hash"] != str(snapshot[6]):
            raise ResearchOperationError("frozen snapshot identity hash mismatch")
        admitted_rows = [row for row in audited if row.admitted]
        counts = (
            len(admitted_rows),
            len({row.candidate.physical_device_key for row in admitted_rows}),
            len({row.candidate.campaign_key for row in admitted_rows if row.candidate.campaign_key}),
            len({row.candidate.run_key for row in admitted_rows if row.candidate.run_key}),
        )
        if counts != tuple(int(value) for value in snapshot[7:11]):
            raise ResearchOperationError("frozen snapshot denominator counts do not match its members")
        return audited
    finally:
        cursor.close()


def freeze_snapshot(
    conn,
    audited: Sequence[AuditedPair],
    *,
    snapshot_version: str,
    target_current_a: float,
    source_code_sha: str,
    source_fingerprint: str,
    actor: str,
    extraction_audit: Mapping[str, object],
    source_cutoff: datetime,
) -> dict[str, object]:
    require_schema(conn)
    lineage_constraints = (
        "iv_damage_research_pair_source_fk",
        "iv_damage_research_pre_feature_fk",
        "iv_damage_research_post_feature_fk",
        "iv_damage_research_source_point_fk",
    )
    lineage_cursor = conn.cursor()
    try:
        lineage_cursor.execute(
            "SELECT count(*) FROM pg_constraint WHERE conname = ANY(%s)",
            (list(lineage_constraints),),
        )
        lineage_count = int(lineage_cursor.fetchone()[0])
    finally:
        lineage_cursor.close()
    if lineage_count != len(lineage_constraints):
        raise ResearchOperationError(
            "research snapshot freeze requires all V2/raw ON DELETE RESTRICT "
            "lineage constraints; rebuild the empty research schema after "
            "pipeline source tables exist"
        )
    cutoff = source_cutoff
    identity = snapshot_payload(
        audited,
        snapshot_version=snapshot_version,
        target_current_a=target_current_a,
        source_code_sha=source_code_sha,
        source_fingerprint=source_fingerprint,
        source_cutoff=cutoff,
    )
    admitted = [row for row in audited if row.admitted]
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id,snapshot_hash FROM iv_damage_research_snapshots WHERE snapshot_version=%s",
            (snapshot_version,),
        )
        existing = cursor.fetchone()
        if existing:
            if str(existing[1]) != identity["snapshot_hash"]:
                raise ResearchOperationError("snapshot version conflicts with different source payload hashes")
            return {**identity, "snapshot_id": int(existing[0]), "status": "existing"}

        limitations = {
            "measurement_horizon": "unknown_or_heterogeneous",
            "fluence": "missing values remain missing; no zero imputation",
            "replicates": "repeated files are not controlled replicates",
            "identity": "raw operational identities have not all been lab-reviewed",
            "claim": "retrospective research only; not decision eligible",
        }
        cursor.execute(
            """
            INSERT INTO iv_damage_research_snapshots (
                snapshot_version,snapshot_hash,reference_policy,research_protocol_id,
                target_current_a,source_cutoff,source_query,source_code_sha,
                source_fingerprint,pair_count,device_count,campaign_count,run_count,
                extraction_audit,limitations,created_by
            ) VALUES (%s,%s,'same_device',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                snapshot_version,
                identity["snapshot_hash"],
                "historical-idvg-vds1v-unknown-horizon-research-v1",
                target_current_a,
                cutoff,
                CANDIDATE_QUERY.strip(),
                source_code_sha,
                source_fingerprint,
                len(admitted),
                len({row.candidate.physical_device_key for row in admitted}),
                len({row.candidate.campaign_key for row in admitted if row.candidate.campaign_key}),
                len({row.candidate.run_key for row in admitted if row.candidate.run_key}),
                Json(dict(extraction_audit)),
                Json(limitations),
                actor,
            ),
        )
        snapshot_id = int(cursor.fetchone()[0])
        for row in audited:
            pair = row.candidate
            pre_min, pre_max, post_min, post_max = _ranges(row)
            compatible = not any("vds_protocol_mismatch" in reason for reason in row.exclusion_reasons)
            cursor.execute(
                """
                INSERT INTO iv_damage_research_curve_pairs (
                    snapshot_id,source_pair_id,pair_key,pre_feature_id,post_feature_id,
                    pre_metadata_id,post_metadata_id,pre_point_hash,post_point_hash,
                    pair_payload_hash,physical_device_key,device_type,manufacturer,
                    campaign_key,run_key,ion_species,beam_energy_mev,let_surface,range_um,
                    beam_type,fluence,fluence_missing,pre_vds_v,post_vds_v,
                    protocol_compatible,extraction_config,pre_vth_v,post_vth_v,
                    observed_delta_vth_v,extraction_diagnostics,pre_vg_min,pre_vg_max,
                    post_vg_min,post_vg_max,pre_point_count,post_point_count,
                    common_grid_point_count,admission_status,exclusion_reasons
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ) RETURNING id
                """,
                (
                    snapshot_id,
                    pair.source_pair_id,
                    pair.pair_key,
                    pair.pre_feature_id,
                    pair.post_feature_id,
                    pair.pre_metadata_id,
                    pair.post_metadata_id,
                    row.pre_point_hash,
                    row.post_point_hash,
                    row.pair_payload_hash,
                    pair.physical_device_key,
                    pair.device_type,
                    pair.manufacturer,
                    pair.campaign_key,
                    pair.run_key,
                    pair.ion_species,
                    pair.beam_energy_mev,
                    pair.let_surface,
                    pair.range_um,
                    pair.beam_type,
                    pair.fluence,
                    pair.fluence is None,
                    pair.pre_vds_v,
                    pair.post_vds_v,
                    compatible,
                    Json(extraction_config_payload(target_current_a)),
                    row.pre_vth_v,
                    row.post_vth_v,
                    row.observed_delta_vth_v,
                    Json(dict(row.extraction_diagnostics)),
                    pre_min,
                    pre_max,
                    post_min,
                    post_max,
                    len(pair.pre_points),
                    len(pair.post_points),
                    row.common_grid_point_count,
                    "admitted" if row.admitted else "excluded",
                    list(row.exclusion_reasons),
                ),
            )
            pair_id = int(cursor.fetchone()[0])
            points = []
            for role, curve in (("pre", pair.pre_points), ("post", pair.post_points)):
                for order, point in enumerate(sorted(curve, key=lambda item: (item.point_index, item.source_point_id))):
                    payload = point_payload((point,))[0]
                    points.append(
                        (
                            pair_id,
                            role,
                            order,
                            point.point_index,
                            point.source_point_id,
                            point.v_gate_v,
                            point.v_drain_v,
                            point.i_drain_a,
                            sha256_payload(payload),
                        )
                    )
            execute_values(
                cursor,
                """INSERT INTO iv_damage_research_curve_pair_points
                (curve_pair_id,curve_role,point_order,source_point_index,source_point_id,
                 v_gate_v,v_drain_v,i_drain_a,point_hash) VALUES %s""",
                points,
            )
        conn.commit()
        return {**identity, "snapshot_id": snapshot_id, "status": "created"}
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def persist_assignments(conn, snapshot_id: int, ids, assignments) -> None:
    rows = [
        (
            snapshot_id,
            ids[row.pair_key],
            row.validation_scheme,
            row.fold_number,
            row.held_out_group_key,
            row.physical_device_key,
            row.assignment_hash,
        )
        for row in assignments
    ]
    cursor = conn.cursor()
    try:
        execute_values(
            cursor,
            """INSERT INTO iv_damage_research_split_assignments
            (snapshot_id,curve_pair_id,validation_scheme,fold_number,
             held_out_group_key,physical_device_key,assignment_hash) VALUES %s
            ON CONFLICT (snapshot_id,curve_pair_id,validation_scheme) DO NOTHING""",
            rows,
        )
        schemes = sorted({row.validation_scheme for row in assignments})
        cursor.execute(
            """SELECT curve_pair_id,validation_scheme,fold_number,
                      held_out_group_key,physical_device_key,assignment_hash
               FROM iv_damage_research_split_assignments
               WHERE snapshot_id=%s AND validation_scheme = ANY(%s)""",
            (snapshot_id, schemes),
        )
        stored = {
            (int(pair_id), scheme, int(fold), group, device, str(digest))
            for pair_id, scheme, fold, group, device, digest in cursor.fetchall()
        }
        expected = {
            (pair_id, scheme, fold, group, device, str(digest))
            for _snapshot, pair_id, scheme, fold, group, device, digest in rows
        }
        if stored != expected:
            raise ResearchOperationError("split assignment replay conflicts with immutable stored assignments")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _existing_run_id(
    cursor,
    *,
    run_version: str,
    snapshot_id: int,
    family: str,
    method: str,
    validation_scheme: str,
    seed: int,
    artifact: ArtifactIdentity,
    source_code_sha: str,
    source_fingerprint: str,
    actor: str,
) -> int | None:
    cursor.execute(
        """SELECT id,snapshot_id,model_family,method,validation_scheme,random_seed,
                  artifact_path,artifact_checksum,source_code_sha,source_fingerprint,
                  created_by,development_status
           FROM iv_damage_research_model_runs WHERE run_version=%s""",
        (run_version,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    expected = (
        snapshot_id,
        family,
        method,
        validation_scheme,
        seed,
        str(artifact.path),
        artifact.checksum,
        source_code_sha,
        source_fingerprint,
        actor,
    )
    if tuple(row[1:11]) != expected or str(row[11]) not in {"evaluated", "preferred"}:
        raise ResearchOperationError(f"research run replay conflicts with stored run: {run_version}")
    return int(row[0])


def persist_scalar_run(
    conn,
    *,
    snapshot_id: int,
    ids: Mapping[str, int],
    run_version: str,
    result: ScalarRunResult,
    artifact: ArtifactIdentity,
    source_code_sha: str,
    source_fingerprint: str,
    actor: str,
    seed: int,
) -> int:
    cursor = conn.cursor()
    try:
        family = "baseline" if result.method in {"zero_damage", "v2_donor"} else "scalar"
        existing_id = _existing_run_id(
            cursor,
            run_version=run_version,
            snapshot_id=snapshot_id,
            family=family,
            method=result.method,
            validation_scheme=result.validation_scheme,
            seed=seed,
            artifact=artifact,
            source_code_sha=source_code_sha,
            source_fingerprint=source_fingerprint,
            actor=actor,
        )
        if existing_id is not None:
            conn.rollback()
            return existing_id
        cursor.execute(
            """INSERT INTO iv_damage_research_model_runs
            (run_version,snapshot_id,model_family,method,validation_scheme,
             feature_mode,feature_contract,estimator_config,random_seed,
             artifact_path,artifact_checksum,source_code_sha,source_fingerprint,created_by)
            VALUES (%s,%s,%s,%s,%s,'physics_only',%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id""",
            (
                run_version,
                snapshot_id,
                family,
                result.method,
                result.validation_scheme,
                Json({"features": list(result.feature_names)}),
                Json(dict(result.estimator_config)),
                seed,
                str(artifact.path),
                artifact.checksum,
                source_code_sha,
                source_fingerprint,
                actor,
            ),
        )
        run_id = int(cursor.fetchone()[0])
        manifest_ids = {}
        for manifest in result.fold_manifests:
            groups = ",".join(manifest["held_out_group_keys"])
            cursor.execute(
                """INSERT INTO iv_damage_research_fold_manifests
                (model_run_id,fold_number,held_out_group_key,training_device_keys,
                 training_device_hash,preprocessing_manifest)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    run_id,
                    manifest["fold_number"],
                    groups,
                    Json(manifest["training_device_keys"]),
                    manifest["training_device_hash"],
                    Json({"fit_scope": "outer_training_fold_only"}),
                ),
            )
            manifest_ids[int(manifest["fold_number"])] = int(cursor.fetchone()[0])
        for prediction in result.predictions:
            predicted = prediction.predicted_delta_vth_v
            residual = None if predicted is None else predicted - prediction.observed_delta_vth_v
            cursor.execute(
                """INSERT INTO iv_damage_research_scalar_predictions
                (model_run_id,curve_pair_id,fold_manifest_id,validation_scheme,
                 fold_number,held_out_group_key,observed_delta_vth_v,
                 predicted_delta_vth_v,residual_v,absolute_error_v,support_status,
                 support_reasons)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    run_id,
                    ids[prediction.pair_key],
                    manifest_ids[prediction.fold_number],
                    prediction.validation_scheme,
                    prediction.fold_number,
                    prediction.held_out_group_key,
                    prediction.observed_delta_vth_v,
                    predicted,
                    residual,
                    None if residual is None else abs(residual),
                    prediction.support_status,
                    list(prediction.support_reasons),
                ),
            )
        for metric_name, metric_value in result.metrics.items():
            if (
                isinstance(metric_value, (int, float))
                and not isinstance(metric_value, bool)
                and metric_value is not None
            ):
                cursor.execute(
                    """INSERT INTO iv_damage_research_metrics
                    (model_run_id,aggregation_level,metric_name,metric_value,
                     supported_pairs,supported_devices,abstained_pairs,
                     denominator_note)
                    VALUES (%s,'device_macro',%s,%s,%s,%s,%s,%s)""",
                    (
                        run_id,
                        metric_name,
                        metric_value,
                        result.metrics["supported_pairs"],
                        result.metrics["supported_devices"],
                        result.metrics["abstained_pairs"],
                        result.metrics["denominator"],
                    ),
                )
        cursor.execute(
            """UPDATE iv_damage_research_model_runs
               SET development_status='evaluated',metrics=%s,limitations=%s,
                   completed_at=clock_timestamp()
               WHERE id=%s AND development_status='candidate'""",
            (
                Json(dict(result.metrics)),
                Json(
                    {
                        "claim": "retrospective_research",
                        "horizon": "unknown_or_heterogeneous",
                    }
                ),
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ResearchOperationError("scalar research run did not finalize from candidate state")
        conn.commit()
        return run_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def scalar_prediction_ids(conn, run_id: int) -> dict[str, int]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT pair.pair_key,prediction.id
            FROM iv_damage_research_scalar_predictions prediction
            JOIN iv_damage_research_curve_pairs pair ON pair.id=prediction.curve_pair_id
            WHERE prediction.model_run_id=%s""",
            (run_id,),
        )
        return {str(key): int(identity) for key, identity in cursor.fetchall()}
    finally:
        cursor.close()


def persist_curve_run(
    conn,
    *,
    snapshot_id: int,
    ids: Mapping[str, int],
    scalar_ids: Mapping[str, int],
    run_version: str,
    method: str,
    validation_scheme: str,
    results: Sequence[tuple[CurvePredictionResult, object]],
    artifact: ArtifactIdentity,
    source_code_sha: str,
    source_fingerprint: str,
    actor: str,
    seed: int,
) -> int:
    cursor = conn.cursor()
    try:
        existing_id = _existing_run_id(
            cursor,
            run_version=run_version,
            snapshot_id=snapshot_id,
            family="hybrid_curve",
            method=method,
            validation_scheme=validation_scheme,
            seed=seed,
            artifact=artifact,
            source_code_sha=source_code_sha,
            source_fingerprint=source_fingerprint,
            actor=actor,
        )
        if existing_id is not None:
            conn.rollback()
            return existing_id
        cursor.execute(
            """INSERT INTO iv_damage_research_model_runs
            (run_version,snapshot_id,model_family,method,validation_scheme,
             feature_mode,feature_contract,estimator_config,random_seed,
             artifact_path,artifact_checksum,source_code_sha,source_fingerprint,created_by)
            VALUES (%s,%s,'hybrid_curve',%s,%s,'physics_only',%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id""",
            (
                run_version,
                snapshot_id,
                method,
                validation_scheme,
                Json({"post_features": [], "scalar_shift": "out_of_fold_predicted"}),
                Json(
                    {
                        "grid_points": 64,
                        "residual_basis": "training_fold_pca",
                        "residual_pca_explained_variance": {
                            result.pair_key: list(result.pca_explained_variance) for result, _scalar in results
                        },
                    }
                ),
                seed,
                str(artifact.path),
                artifact.checksum,
                source_code_sha,
                source_fingerprint,
                actor,
            ),
        )
        run_id = int(cursor.fetchone()[0])
        for result, scalar in results:
            cursor.execute(
                """INSERT INTO iv_damage_research_curve_predictions
                (model_run_id,scalar_prediction_id,curve_pair_id,validation_scheme,
                 fold_number,held_out_group_key,scalar_shift_v,scalar_shift_source,
                 correction_applied,correction_norm,fallback_reason,support_status,
                 mae_a,max_abs_error_a,normalized_rmse,transformed_mae,
                 supported_voltage_fraction)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'out_of_fold_predicted',%s,%s,%s,%s,
                        %s,%s,%s,%s,%s) RETURNING id""",
                (
                    run_id,
                    scalar_ids[result.pair_key],
                    ids[result.pair_key],
                    validation_scheme,
                    scalar.fold_number,
                    scalar.held_out_group_key,
                    scalar.predicted_delta_vth_v,
                    result.correction_applied,
                    result.correction_norm,
                    result.fallback_reason,
                    "supported" if result.correction_applied else "fallback",
                    result.metrics["mae_a"],
                    result.metrics["max_abs_error_a"],
                    result.metrics["normalized_rmse"],
                    result.metrics["transformed_mae"],
                    result.metrics["supported_voltage_fraction"],
                ),
            )
            prediction_id = int(cursor.fetchone()[0])
            point_rows = []
            series = {
                "post_measured": result.actual_post_i_a,
                "hybrid_huber" if method == "hybrid_huber" else "hybrid_extra_trees": result.hybrid_i_a,
            }
            series.update(result.comparison_series)
            for series_name, currents in series.items():
                for order, (voltage, current) in enumerate(zip(result.grid_v, currents, strict=True)):
                    if current is None:
                        continue
                    point_rows.append(
                        (
                            prediction_id,
                            order,
                            voltage,
                            series_name,
                            current,
                            series_name == "post_measured",
                        )
                    )
            execute_values(
                cursor,
                """INSERT INTO iv_damage_research_curve_prediction_points
                (curve_prediction_id,point_order,v_gate_v,series_name,i_drain_a,
                 truth_only) VALUES %s""",
                point_rows,
            )
        cursor.execute(
            """UPDATE iv_damage_research_model_runs
               SET development_status='evaluated',metrics=%s,limitations=%s,
                   completed_at=clock_timestamp()
               WHERE id=%s AND development_status='candidate'""",
            (
                Json({"curves": len(results), "denominator": "curve/device"}),
                Json({"bands": "not claimed", "horizon": "unknown_or_heterogeneous"}),
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ResearchOperationError("curve research run did not finalize from candidate state")
        conn.commit()
        return run_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
