from dataclasses import replace
import math

import pytest

from aps.ml.iv_damage_research_contracts import ResearchPair, ResearchPoint
from aps.ml.iv_damage_research_dataset import (
    audit_pair,
    deterministic_split_assignments,
    point_payload_hash,
    target_current_sensitivity,
)


def make_points(metadata_offset: int, shift: float = 0.0):
    points = []
    for index in range(81):
        voltage = index * 0.1
        current = 0.01 * math.exp((voltage - shift - 3.0) * 2.0)
        points.append(ResearchPoint(metadata_offset + index, index, voltage, current, 1.0))
    return tuple(points)


def make_pair(index: int, *, device: str, run: str, campaign: str, shift=0.1):
    return ResearchPair(
        index,
        f"pair-{index}",
        100 + index,
        200 + index,
        300 + index,
        400 + index,
        device,
        "IFX-Trench",
        "Infineon",
        campaign,
        run,
        "Ca",
        100.0,
        10.0,
        25.0,
        "ion",
        None,
        1.0,
        1.0,
        make_points(index * 1000),
        make_points(index * 1000 + 100, shift),
    )


def test_raw_point_hash_is_ordered_and_changed_source_fails_identity():
    points = make_points(1000)
    assert point_payload_hash(points) == point_payload_hash(tuple(points))
    changed = list(points)
    point = changed[20]
    changed[20] = ResearchPoint(point.source_point_id, point.point_index, point.v_gate_v, point.i_drain_a * 2, 1.0)
    assert point_payload_hash(points) != point_payload_hash(changed)


def test_audit_reextracts_delta_and_preserves_missingness():
    audited = audit_pair(make_pair(1, device="d1", run="r1", campaign="c1"))
    assert audited.admitted
    assert audited.observed_delta_vth_v == pytest.approx(0.1, abs=1e-10)
    assert audited.candidate.fluence is None
    assert audited.common_grid_point_count == 64


def test_target_current_selection_is_deterministic():
    pairs = [make_pair(i, device=f"d{i}", run=f"r{i}", campaign=f"c{i}") for i in range(1, 4)]
    first = target_current_sensitivity(pairs)
    assert first == target_current_sensitivity(pairs)
    assert first["selected_target_current_a"] in {0.001, 0.01, 0.1}


def test_grouped_splits_never_split_reused_device():
    audited = [
        audit_pair(make_pair(1, device="d1", run="r1", campaign="c1")),
        audit_pair(make_pair(2, device="d1", run="r1", campaign="c1")),
        audit_pair(make_pair(3, device="d2", run="r2", campaign="c2")),
    ]
    rows = deterministic_split_assignments(audited, "leave_device")
    assert {row.fold_number for row in rows if row.physical_device_key == "d1"} == {0}


def test_pair_identity_hash_covers_frozen_model_metadata():
    original = make_pair(11, device="d11", run="r1", campaign="c1")
    changed = replace(original, let_surface=99.0)

    assert audit_pair(original).pre_point_hash == audit_pair(changed).pre_point_hash
    assert audit_pair(original).pair_payload_hash != audit_pair(changed).pair_payload_hash
