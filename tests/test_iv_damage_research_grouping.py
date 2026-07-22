from aps.ml.iv_damage_research_dataset import (
    audit_pair,
    deterministic_split_assignments,
)
from tests.test_iv_damage_research_dataset import make_pair


def test_leave_run_connects_runs_when_one_physical_device_spans_both():
    audited = [
        audit_pair(make_pair(1, device="shared", run="run-a", campaign="campaign-a")),
        audit_pair(make_pair(2, device="shared", run="run-b", campaign="campaign-b")),
        audit_pair(make_pair(3, device="other", run="run-c", campaign="campaign-c")),
    ]
    assignments = deterministic_split_assignments(audited, "leave_run")
    shared = [row for row in assignments if row.physical_device_key == "shared"]
    assert {row.fold_number for row in shared} == {shared[0].fold_number}
    assert shared[0].held_out_group_key == "run-a|run-b"


def test_leave_campaign_connects_campaigns_for_shared_device():
    audited = [
        audit_pair(make_pair(1, device="shared", run="run-a", campaign="campaign-a")),
        audit_pair(make_pair(2, device="shared", run="run-b", campaign="campaign-b")),
        audit_pair(make_pair(3, device="other", run="run-c", campaign="campaign-c")),
    ]
    assignments = deterministic_split_assignments(audited, "leave_campaign")
    shared = [row for row in assignments if row.physical_device_key == "shared"]
    assert len({row.fold_number for row in shared}) == 1
    assert shared[0].held_out_group_key == "campaign-a|campaign-b"
