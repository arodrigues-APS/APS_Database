"""Immutable dataset-snapshot and split-manifest creation for V3."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Mapping, Sequence

from psycopg2.extras import Json, execute_values

from aps.ml.iv_damage_repository import dataset_snapshot_hash
from aps.ml.iv_damage_validation import (
    FoldAssignment,
    ValidationUnit,
    assert_no_group_leakage,
    assign_grouped_folds,
)


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
    for scheme in grouped_schemes:
        try:
            folds = assign_grouped_folds(
                validation_units, scheme, n_splits=n_splits, seed=seed
            )
            assert_no_group_leakage(validation_units, folds, scheme)
        except ValueError as exc:
            raise DatasetSnapshotError(f"cannot create {scheme} diagnostic: {exc}") from exc
        by_key = {row.response_unit_key: row for row in folds}
        assignments.extend(
            PlannedAssignment(
                unit.response_unit_id,
                unit.unit_key,
                scheme,
                by_key[unit.unit_key].fold,
                "train",
                by_key[unit.unit_key].component_key,
            )
            for unit in units
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
        "n_splits": n_splits,
        "seed": seed,
    }
    return DatasetPlan(tuple(assignments), summary)


SOURCE_QUERY = """
SELECT id, unit_key, physical_device_key, stress_session_key, target_type,
       response_value, campaign_key, run_key, device_type, ion_species,
       baseline_reference_group_key, stress_features, measurement_protocol_id,
       pre_observation_ids, post_observation_ids, pre_value, post_value,
       response_uncertainty, pre_replicate_count, post_replicate_count,
       reference_policy, required_features_complete, quality_status
FROM iv_damage_response_units
WHERE stress_type = %(stress_type)s
  AND target_type = %(target_type)s
  AND quality_status = 'usable'
  AND reference_policy = 'same_device'
  AND required_features_complete
ORDER BY unit_key
"""


def _load_units(conn, *, stress_type: str, target_type: str) -> list[DatasetUnit]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            SOURCE_QUERY,
            {"stress_type": stress_type, "target_type": target_type},
        )
        units = []
        for row in cursor.fetchall():
            features = dict(row[11])
            record = {
                "unit_key": row[1], "physical_device_key": row[2],
                "stress_session_key": row[3], "target_type": row[4],
                "response_value": row[5], "campaign_key": row[6], "run_key": row[7],
                "device_type": row[8], "ion_species": row[9],
                "baseline_reference_group_key": row[10], "stress_features": features,
                "measurement_protocol_id": row[12], "pre_observation_ids": row[13],
                "post_observation_ids": row[14], "pre_value": row[15], "post_value": row[16],
                "response_uncertainty": row[17], "pre_replicate_count": row[18],
                "post_replicate_count": row[19], "reference_policy": row[20],
            }
            units.append(
                DatasetUnit(
                    response_unit_id=int(row[0]), unit_key=row[1],
                    physical_device_key=row[2], stress_session_key=row[3],
                    target_type=row[4], observed_response=float(row[5]),
                    campaign_key=row[6], run_key=row[7], device_type=row[8],
                    ion_species=row[9], baseline_reference_group_key=row[10],
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
) -> DatasetSnapshotResult:
    units = _load_units(conn, stress_type=stress_type, target_type=target_type)
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
        source_code_sha=source_code_sha,
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
                query_identity, source_code_sha, len(units),
                len({(unit.physical_device_key, unit.stress_session_key, unit.target_type) for unit in units}),
                Json({**plan.domain_summary, "stress_type": stress_type, "target_type": target_type}),
            ),
        )
        snapshot_id = int(cursor.fetchone()[0])
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
