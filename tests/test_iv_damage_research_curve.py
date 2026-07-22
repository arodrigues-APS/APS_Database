from dataclasses import replace

import numpy as np
import pytest

from aps.ml.iv_damage_research_contracts import (
    ResearchContractError,
    ScalarOOFPrediction,
)
from aps.ml.iv_damage_research_curve import (
    deterministic_projection,
    fit_predict_hybrid_fold,
    supported_grid,
    transformed_residual_target,
)
from aps.ml.iv_damage_research_dataset import audit_pair
from tests.test_iv_damage_research_dataset import make_pair


def test_v2_shift_sign_and_pure_shift_residual():
    pair = audit_pair(make_pair(1, device="held", run="r1", campaign="c1", shift=0.1))
    grid, residual, _ = transformed_residual_target(pair)
    projected = deterministic_projection(pair.candidate.pre_points, grid, 0.1)
    assert np.max(np.abs(residual)) < 1e-10
    assert np.allclose(
        projected,
        np.interp(
            grid,
            [point.v_gate_v for point in pair.candidate.post_points],
            [point.i_drain_a for point in pair.candidate.post_points],
        ),
    )


def test_unsupported_grid_refuses_extrapolation():
    pair = audit_pair(make_pair(1, device="d1", run="r1", campaign="c1"))
    with pytest.raises(ResearchContractError):
        supported_grid(
            pair.candidate.pre_points,
            pair.candidate.post_points,
            100.0,
        )


def test_projection_accepts_only_floating_point_endpoint_roundoff():
    pair = audit_pair(make_pair(1, device="d1", run="r1", campaign="c1"))
    pre_points = pair.candidate.pre_points
    pre_x = np.asarray([point.v_gate_v for point in pre_points])
    shift = -0.15658410708334802
    projected_grid = np.asarray([pre_x.min() + shift, pre_x.max() + shift])

    result = deterministic_projection(pre_points, projected_grid, shift)

    assert result == pytest.approx([pre_points[0].i_drain_a, pre_points[-1].i_drain_a])
    with pytest.raises(ResearchContractError, match="extrapolate"):
        deterministic_projection(pre_points, [projected_grid[0] - 1e-9, projected_grid[1]], shift)


def test_hybrid_uses_predicted_not_observed_scalar_shift():
    training = [
        audit_pair(make_pair(i, device=f"d{i}", run=f"r{i}", campaign=f"c{i}", shift=0.02 * i)) for i in range(1, 5)
    ]
    held = audit_pair(make_pair(10, device="held", run="rh", campaign="ch", shift=0.2))
    scalar = ScalarOOFPrediction(
        held.candidate.pair_key,
        "leave_device",
        10,
        "held",
        float(held.observed_delta_vth_v),
        0.05,
        tuple(row.candidate.physical_device_key for row in training),
    )
    result = fit_predict_hybrid_fold(training, held, scalar, method="hybrid_extra_trees")
    expected = deterministic_projection(held.candidate.pre_points, result.grid_v, 0.05)
    assert result.deterministic_i_a == pytest.approx(expected)
    assert result.actual_post_i_a != pytest.approx(result.deterministic_i_a)


def test_hybrid_rejects_device_leakage():
    held = audit_pair(make_pair(1, device="same", run="r1", campaign="c1"))
    scalar = ScalarOOFPrediction(
        held.candidate.pair_key,
        "leave_device",
        0,
        "same",
        float(held.observed_delta_vth_v),
        0.1,
        (),
    )
    with pytest.raises(ResearchContractError):
        fit_predict_hybrid_fold([held], held, scalar, method="hybrid_huber")



def test_held_post_support_does_not_change_prediction_grid_or_values():
    training = [
        audit_pair(make_pair(i, device=f"d{i}", run=f"r{i}", campaign=f"c{i}", shift=0.02 * i))
        for i in range(1, 5)
    ]
    held_full = audit_pair(make_pair(20, device="held", run="rh", campaign="ch", shift=0.2))
    held_narrow = audit_pair(
        replace(
            held_full.candidate,
            post_points=held_full.candidate.post_points[10:-10],
        )
    )

    def scalar_for(held):
        return ScalarOOFPrediction(
            held.candidate.pair_key,
            "leave_device",
            20,
            "held",
            float(held.observed_delta_vth_v),
            0.05,
            tuple(row.candidate.physical_device_key for row in training),
        )

    full = fit_predict_hybrid_fold(training, held_full, scalar_for(held_full), method="hybrid_extra_trees")
    narrow = fit_predict_hybrid_fold(training, held_narrow, scalar_for(held_narrow), method="hybrid_extra_trees")

    assert narrow.grid_v == pytest.approx(full.grid_v)
    assert narrow.deterministic_i_a == pytest.approx(full.deterministic_i_a)
    assert narrow.hybrid_i_a == pytest.approx(full.hybrid_i_a)
    assert narrow.metrics["supported_voltage_fraction"] < full.metrics["supported_voltage_fraction"]
