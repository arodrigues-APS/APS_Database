import pytest

from aps.ml.iv_damage_dataset import (
    DatasetSnapshotError,
    DatasetUnit,
    plan_dataset_snapshot,
    snapshot_member_payload_hash,
)


def unit(index, campaign, *, device=None, condition=None):
    return DatasetUnit(
        response_unit_id=index,
        unit_key=f"unit-{index}",
        physical_device_key=device or f"device-{index}",
        stress_session_key=f"session-{index}",
        target_type="delta_vth_v",
        observed_response=index / 10,
        campaign_key=campaign,
        run_key=f"run-{index}",
        device_type="C2M",
        ion_species="Xe",
        baseline_reference_group_key=None,
        stress_condition_key=condition or f"condition-{index}",
        record={"unit_key": f"unit-{index}", "response": index / 10},
    )


def population():
    campaigns = ["train-a", "train-b", "train-c", "cal", "external"]
    return [unit(index, campaigns[index % len(campaigns)]) for index in range(25)]


def test_snapshot_plan_freezes_roles_and_all_required_grouped_schemes():
    plan = plan_dataset_snapshot(
        population(), external_campaigns=["external"], calibration_campaigns=["cal"],
        grouped_schemes=["leave_device", "leave_condition", "leave_campaign"],
        n_splits=3, seed=7,
    )
    assert plan.domain_summary["role_counts"] == {
        "calibration": 5, "external_test": 5, "train": 15,
    }
    by_scheme = {}
    for row in plan.assignments:
        by_scheme.setdefault(row.split_scheme, []).append(row)
    assert set(by_scheme) == {
        "frozen_release", "leave_device", "leave_condition", "leave_campaign"
    }
    assert all(row.fold_number is None for row in by_scheme["frozen_release"])
    assert all(row.split_role == "grouped_test" for row in by_scheme["leave_device"])
    assert plan.domain_summary["grouped_diagnostic_role"] == "grouped_test"
    external_ids = {
        row.response_unit_id for row in by_scheme["frozen_release"]
        if row.split_role == "external_test"
    }
    assert all(
        not external_ids.intersection(row.response_unit_id for row in assignments)
        for scheme, assignments in by_scheme.items()
        if scheme != "frozen_release"
    )
    assert plan.domain_summary["external_excluded_from_grouped_diagnostics"] is True


def test_snapshot_plan_rejects_campaign_overlap_and_group_leakage():
    with pytest.raises(DatasetSnapshotError, match="overlap"):
        plan_dataset_snapshot(
            population(), external_campaigns=["external"],
            calibration_campaigns=["external"], n_splits=3,
        )
    leaking = population()
    leaking[0] = unit(0, "train-a", device="shared")
    leaking[-1] = unit(24, "external", device="shared")
    with pytest.raises(DatasetSnapshotError, match="leaks protected groups"):
        plan_dataset_snapshot(
            leaking, external_campaigns=["external"],
            calibration_campaigns=["cal"], n_splits=3,
        )


def test_snapshot_plan_fails_when_grouped_diagnostic_is_not_supported():
    units = [unit(index, campaign="same", condition="same") for index in range(10)]
    with pytest.raises(DatasetSnapshotError, match="holdout campaigns are absent"):
        plan_dataset_snapshot(
            units, external_campaigns=["external"], calibration_campaigns=["cal"],
            n_splits=3,
        )


def test_frozen_member_payload_hash_is_canonical_and_tamper_sensitive():
    first = {"unit_key": "u-1", "features": {"b": 2, "a": 1}}
    reordered = {"features": {"a": 1, "b": 2}, "unit_key": "u-1"}
    changed = {**first, "unit_key": "u-2"}
    assert snapshot_member_payload_hash(first) == snapshot_member_payload_hash(reordered)
    assert snapshot_member_payload_hash(first) != snapshot_member_payload_hash(changed)
