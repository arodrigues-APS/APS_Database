"""Immutable dataset-snapshot and split-manifest creation for V3."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from typing import Mapping, Sequence

from psycopg2.extras import Json, execute_values

from aps.ml.iv_damage_repository import dataset_snapshot_hash
from aps.ml.iv_damage_validation import (
    FoldAssignment,
    ValidationUnit,
    assert_no_group_leakage,
    assign_grouped_folds,
)
from aps.provenance import collect_source_provenance


class DatasetSnapshotError(RuntimeError):
    """Raised when frozen evidence cannot support leakage-safe partitions."""


@dataclass(frozen=True)
class DatasetUnit:
    response_unit_id: int
    unit_key: str
    physical_device_key: str
    stress_session_key: str
    target_type: str
    observed_response: float
    campaign_key: str
    run_key: str
    device_type: str
    ion_species: str | None
    baseline_reference_group_key: str | None
    stress_condition_key: str | None
    record: Mapping[str, object]

    def validation_unit(self) -> ValidationUnit:
        return ValidationUnit(
            response_unit_key=self.unit_key,
            physical_device_key=self.physical_device_key,
            stress_session_key=self.stress_session_key,
            target_type=self.target_type,
            observed_response=self.observed_response,
            stress_condition_key=self.stress_condition_key,
            run_key=self.run_key,
            campaign_key=self.campaign_key,
            ion_species=self.ion_species,
            baseline_reference_group_key=self.baseline_reference_group_key,
            device_type=self.device_type,
        )


@dataclass(frozen=True)
class PlannedAssignment:
    response_unit_id: int
    unit_key: str
    split_scheme: str
    fold_number: int | None
    split_role: str
    group_key: str


@dataclass(frozen=True)
class DatasetPlan:
    assignments: tuple[PlannedAssignment, ...]
    domain_summary: Mapping[str, object]


@dataclass(frozen=True)
class DatasetSnapshotResult:
    snapshot_id: int
    snapshot_version: str
    snapshot_hash: str
    row_count: int
    assignments: int


def plan_dataset_snapshot(
    units: Sequence[DatasetUnit],
    *,
    external_campaigns: Sequence[str],
    calibration_campaigns: Sequence[str],
    grouped_schemes: Sequence[str] = (
        "leave_device",
        "leave_condition",
        "leave_campaign",
    ),
    n_splits: int = 5,
    seed: int = 0,
) -> DatasetPlan:
    if not units:
        raise DatasetSnapshotError("snapshot population is empty")
    keys = [unit.unit_key for unit in units]
    if len(keys) != len(set(keys)):
        raise DatasetSnapshotError("unit_key must be unique")
    external = set(external_campaigns)
    calibration = set(calibration_campaigns)
    if not external or not calibration:
        raise DatasetSnapshotError("explicit external and calibration campaigns are required")
    if external.intersection(calibration):
        raise DatasetSnapshotError("external and calibration campaign sets overlap")
    known_campaigns = {unit.campaign_key for unit in units}
    missing = (external | calibration) - known_campaigns
    if missing:
        raise DatasetSnapshotError(
            "requested holdout campaigns are absent: " + ", ".join(sorted(missing))
        )

    def role(unit: DatasetUnit) -> str:
        if unit.campaign_key in external:
            return "external_test"
        if unit.campaign_key in calibration:
            return "calibration"
        return "train"

    role_by_key = {unit.unit_key: role(unit) for unit in units}
    role_counts = Counter(role_by_key.values())
    if any(role_counts[name] == 0 for name in ("train", "calibration", "external_test")):
        raise DatasetSnapshotError("frozen release split must contain all three roles")
    role_folds = {"train": 0, "calibration": 1, "external_test": 2}
    validation_units = [unit.validation_unit() for unit in units]
    frozen_folds = [
        FoldAssignment(unit.unit_key, role_folds[role_by_key[unit.unit_key]], role_by_key[unit.unit_key])
        for unit in units
    ]
    try:
        assert_no_group_leakage(validation_units, frozen_folds, "leave_device")
    except ValueError as exc:
        raise DatasetSnapshotError(f"frozen release split leaks protected groups: {exc}") from exc

    assignments = [
        PlannedAssignment(
            unit.response_unit_id,
            unit.unit_key,
            "frozen_release",
            None,
            role_by_key[unit.unit_key],
            (
                f"baseline:{unit.baseline_reference_group_key}"
                if unit.baseline_reference_group_key
                else f"device:{unit.physical_device_key}"
            ),
        )
        for unit in units
    ]
    # The external campaign is a single-use release holdout. It must never be
    # rotated through grouped-CV training or calibration folds.
    diagnostic_units = [
        unit for unit in units if role_by_key[unit.unit_key] != "external_test"
    ]
    diagnostic_validation_units = [unit.validation_unit() for unit in diagnostic_units]
    for scheme in grouped_schemes:
        try:
            folds = assign_grouped_folds(
                diagnostic_validation_units, scheme, n_splits=n_splits, seed=seed
            )
            assert_no_group_leakage(diagnostic_validation_units, folds, scheme)
        except ValueError as exc:
            raise DatasetSnapshotError(f"cannot create {scheme} diagnostic: {exc}") from exc
        by_key = {row.response_unit_key: row for row in folds}
        assignments.extend(
            PlannedAssignment(
                unit.response_unit_id,
                unit.unit_key,
                scheme,
                by_key[unit.unit_key].fold,
                "grouped_test",
                by_key[unit.unit_key].component_key,
            )
            for unit in diagnostic_units
        )
    summary = {
        "response_units": len(units),
        "physical_devices": len({unit.physical_device_key for unit in units}),
        "campaigns": len(known_campaigns),
        "role_counts": dict(sorted(role_counts.items())),
        "campaign_role": {
            campaign: (
                "external_test" if campaign in external
                else "calibration" if campaign in calibration
                else "train"
            )
            for campaign in sorted(known_campaigns)
        },
        "grouped_schemes": list(grouped_schemes),
        "grouped_diagnostic_response_units": len(diagnostic_units),
        "grouped_diagnostic_role": "grouped_test",
        "external_excluded_from_grouped_diagnostics": True,
        "n_splits": n_splits,
        "seed": seed,
    }
    return DatasetPlan(tuple(assignments), summary)


SOURCE_QUERY = """
SELECT id, unit_key, physical_device_key, stress_session_key, stress_type,
       target_type, response_value, campaign_key, run_key, device_type,
       manufacturer, ion_species,
       baseline_reference_group_key, stress_features, measurement_protocol_id,
       pre_observation_ids, post_observation_ids, pre_value, post_value,
       response_uncertainty, pre_replicate_count, post_replicate_count,
       reference_policy, required_features_complete, quality_status,
       pre_uncertainty, post_uncertainty, quality_reasons, created_at,
       pre_measured_at, post_measured_at,
       (
           SELECT jsonb_agg(
               jsonb_build_object(
                   'observation_id', observation.id,
                   'observation_key', observation.observation_key,
                   'metadata_id', observation.metadata_id,
                   'metric_name', observation.metric_name,
                   'value', observation.value,
                   'unit', observation.unit,
                   'uncertainty', observation.uncertainty,
                   'accepted_point_count', observation.accepted_point_count,
                   'replicate_group_key', observation.replicate_group_key,
                   'method_version', method.method_version,
                   'config_version', method.config_version,
                   'method_configuration', method.configuration,
                   'method_approved', method.approved,
                   'method_approved_by', method.approved_by,
                   'method_approved_at', method.approved_at,
                   'measurement_protocol_id', observation.measurement_protocol_id,
                   'source_fingerprint', observation.source_fingerprint,
                   'diagnostics', observation.diagnostics,
                   'quality_status', observation.quality_status,
                   'quality_reasons', observation.quality_reasons,
                   'measured_at', observation.measured_at,
                   'extracted_at', observation.extracted_at
               ) ORDER BY observation.id
           )
           FROM iv_damage_metric_observations observation
           JOIN iv_damage_extraction_methods method
             ON method.id = observation.extraction_method_id
           WHERE observation.id = ANY(
               iv_damage_response_units.pre_observation_ids
               || iv_damage_response_units.post_observation_ids
           )
       ) AS observation_provenance
FROM iv_damage_response_units
WHERE stress_type = %(stress_type)s
  AND target_type = %(target_type)s
  AND quality_status = 'usable'
  AND reference_policy = 'same_device'
  AND required_features_complete
ORDER BY unit_key
"""


def snapshot_member_payload_hash(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _load_units(conn, *, stress_type: str, target_type: str) -> list[DatasetUnit]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            SOURCE_QUERY,
            {"stress_type": stress_type, "target_type": target_type},
        )
        units = []
        for row in cursor.fetchall():
            features = dict(row[13])
            record = {
                "unit_key": row[1], "physical_device_key": row[2],
                "stress_session_key": row[3], "stress_type": row[4],
                "target_type": row[5], "response_value": float(row[6]),
                "campaign_key": row[7], "run_key": row[8],
                "device_type": row[9], "manufacturer": row[10],
                "ion_species": row[11], "baseline_reference_group_key": row[12],
                "stress_features": features, "measurement_protocol_id": row[14],
                "pre_observation_ids": row[15], "post_observation_ids": row[16],
                "pre_value": float(row[17]), "post_value": float(row[18]),
                "response_uncertainty": (
                    float(row[19]) if row[19] is not None else None
                ),
                "pre_replicate_count": int(row[20]),
                "post_replicate_count": int(row[21]),
                "reference_policy": row[22],
                "required_features_complete": bool(row[23]),
                "quality_status": row[24],
                "pre_uncertainty": (
                    float(row[25]) if row[25] is not None else None
                ),
                "post_uncertainty": (
                    float(row[26]) if row[26] is not None else None
                ),
                "quality_reasons": list(row[27]),
                "response_unit_created_at": row[28].isoformat(),
                "pre_measured_at": row[29].isoformat(),
                "post_measured_at": row[30].isoformat(),
                "observation_provenance": list(row[31] or []),
            }
            units.append(
                DatasetUnit(
                    response_unit_id=int(row[0]), unit_key=row[1],
                    physical_device_key=row[2], stress_session_key=row[3],
                    target_type=row[5], observed_response=float(row[6]),
                    campaign_key=row[7], run_key=row[8], device_type=row[9],
                    ion_species=row[11], baseline_reference_group_key=row[12],
                    stress_condition_key=str(features.get("stress_condition_key") or "") or None,
                    record=record,
                )
            )
        return units
    finally:
        cursor.close()


def create_dataset_snapshot(
    conn,
    *,
    snapshot_version: str,
    stress_type: str,
    target_type: str,
    extraction_method_versions: Mapping[str, str],
    source_code_sha: str,
    external_campaigns: Sequence[str],
    calibration_campaigns: Sequence[str],
    grouped_schemes: Sequence[str] = (
        "leave_device",
        "leave_condition",
        "leave_campaign",
    ),
    n_splits: int = 5,
    seed: int = 0,
    source_provenance: Mapping[str, object] | None = None,
) -> DatasetSnapshotResult:
    provenance = dict(
        source_provenance or collect_source_provenance().as_dict()
    )
    if (
        provenance.get("code_sha") != source_code_sha
        or not provenance.get("fingerprint")
    ):
        raise DatasetSnapshotError(
            "source provenance must include a fingerprint matching source_code_sha"
        )
    source_identifier = f"{source_code_sha}:{provenance['fingerprint']}"
    units = _load_units(conn, stress_type=stress_type, target_type=target_type)
    observed_versions: dict[str, set[str]] = {}
    for unit in units:
        for observation in unit.record["observation_provenance"]:
            if not observation["method_approved"]:
                raise DatasetSnapshotError(
                    "snapshot includes an observation from an unapproved "
                    f"extraction method: {observation['observation_key']}"
                )
            observed_versions.setdefault(observation["metric_name"], set()).add(
                f"{observation['method_version']}/{observation['config_version']}"
            )
    mixed = {
        metric: versions
        for metric, versions in observed_versions.items()
        if len(versions) != 1
    }
    if mixed:
        raise DatasetSnapshotError(
            "snapshot mixes extraction configurations for metric(s): "
            + ", ".join(sorted(mixed))
        )
    authoritative_versions = {
        metric: next(iter(versions))
        for metric, versions in observed_versions.items()
    }
    if dict(extraction_method_versions) != authoritative_versions:
        raise DatasetSnapshotError(
            "extraction_method_versions do not match frozen observations: "
            f"expected {authoritative_versions!r}"
        )
    plan = plan_dataset_snapshot(
        units,
        external_campaigns=external_campaigns,
        calibration_campaigns=calibration_campaigns,
        grouped_schemes=grouped_schemes,
        n_splits=n_splits,
        seed=seed,
    )
    query_identity = SOURCE_QUERY.strip() + f"\n-- stress_type={stress_type}; target_type={target_type}"
    checksum = dataset_snapshot_hash(
        unit_records=[unit.record for unit in units],
        extraction_versions=extraction_method_versions,
        source_query=query_identity,
        source_code_sha=source_identifier,
    )
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO iv_damage_dataset_snapshots (
                snapshot_version, snapshot_hash, extraction_method_versions,
                source_query, source_code_sha, row_count, independent_group_count,
                domain_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                snapshot_version, checksum, Json(dict(extraction_method_versions)),
                query_identity, source_identifier, len(units),
                len({(unit.physical_device_key, unit.stress_session_key, unit.target_type) for unit in units}),
                Json({
                    **plan.domain_summary,
                    "stress_type": stress_type,
                    "target_type": target_type,
                    "source_provenance": provenance,
                }),
            ),
        )
        snapshot_id = int(cursor.fetchone()[0])
        execute_values(
            cursor,
            """
            INSERT INTO iv_damage_dataset_snapshot_members (
                dataset_snapshot_id, response_unit_id, frozen_payload, payload_hash
            ) VALUES %s
            """,
            [
                (
                    snapshot_id,
                    unit.response_unit_id,
                    Json(
                        dict(unit.record),
                        dumps=lambda value: json.dumps(value, default=str),
                    ),
                    snapshot_member_payload_hash(unit.record),
                )
                for unit in units
            ],
        )
        execute_values(
            cursor,
            """
            INSERT INTO iv_damage_split_assignments (
                dataset_snapshot_id, response_unit_id, split_scheme,
                fold_number, split_role, group_key
            ) VALUES %s
            """,
            [
                (
                    snapshot_id, assignment.response_unit_id,
                    assignment.split_scheme, assignment.fold_number,
                    assignment.split_role, assignment.group_key,
                )
                for assignment in plan.assignments
            ],
        )
        conn.commit()
        return DatasetSnapshotResult(
            snapshot_id, snapshot_version, checksum, len(units), len(plan.assignments)
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
