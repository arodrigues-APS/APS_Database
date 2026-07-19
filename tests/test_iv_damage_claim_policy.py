from aps.ml.iv_damage_curve_training import CurveEvaluation, _development_gate
from aps.ml.iv_damage_projection_validation import (
    ProjectionMetrics,
    _gate as projection_gate,
)


def requirements():
    return {
        "required_grouped_schemes": [
            "leave_device", "leave_condition", "leave_campaign",
        ],
        "min_supported_fraction": 0.8,
        "curve_grid_points": 32,
        "curve_pca_components": 4,
        "curve_ridge_alpha": 1.0,
        "curve_interval_coverage": 0.8,
        "curve_min_development_curves": 6,
        "curve_min_development_devices": 3,
        "curve_min_external_curves": 6,
        "curve_min_external_devices": 3,
        "curve_max_mean_mae_a": 0.01,
        "curve_max_p90_error_a": 0.02,
        "curve_max_normalized_rmse": 0.25,
        "curve_min_band_coverage": 0.75,
        "projection_min_development_curves": 6,
        "projection_min_development_devices": 3,
        "projection_min_external_curves": 6,
        "projection_min_external_devices": 3,
        "projection_max_mean_mae_a": 0.01,
        "projection_max_p90_error_a": 0.02,
        "projection_max_normalized_rmse": 0.25,
        "projection_min_band_coverage": 0.75,
    }


def test_complete_governed_full_curve_policy_can_pass_development_gate():
    passing = CurveEvaluation(
        total_curves=8,
        supported_curves=8,
        physical_devices=4,
        supported_fraction=1.0,
        mean_mae_a=0.005,
        median_max_abs_error_a=0.008,
        p90_max_abs_error_a=0.015,
        mean_normalized_rmse=0.1,
        simultaneous_band_coverage=0.75,
    )
    eligible, checks, reasons = _development_gate(
        {
            "leave_device": passing,
            "leave_condition": passing,
            "leave_campaign": passing,
        },
        requirements(),
    )
    assert eligible
    assert all(checks.values())
    assert reasons == ()


def test_complete_governed_projection_policy_can_pass_both_gates():
    passing = ProjectionMetrics(
        curves=8,
        physical_devices=4,
        mean_mae_a=0.005,
        p90_max_abs_error_a=0.015,
        mean_normalized_rmse=0.1,
        simultaneous_band_coverage=0.75,
    )
    for external in (False, True):
        eligible, checks, reasons = projection_gate(
            passing, requirements(), external=external,
        )
        assert eligible
        assert all(checks.values())
        assert reasons == ()
