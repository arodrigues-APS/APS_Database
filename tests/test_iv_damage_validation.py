import math

import pytest

from aps.ml.iv_damage_validation import (
    BaselinePredictor,
    FoldAssignment,
    PredictionRecord,
    ValidationUnit,
    assert_no_group_leakage,
    assign_grouped_folds,
    evaluate_predictions,
    fold_manifest,
)


def unit(
    key,
    *,
    device=None,
    session=None,
    condition=None,
    run=None,
    campaign=None,
    ion="Xe",
    baseline=None,
    response=1.0,
    device_type="C2M",
):
    return ValidationUnit(
        response_unit_key=key,
        physical_device_key=device or f"device-{key}",
        stress_session_key=session or f"session-{key}",
        target_type="delta_vth_v",
        observed_response=response,
        stress_condition_key=condition,
        run_key=run,
        campaign_key=campaign,
        ion_species=ion,
        baseline_reference_group_key=baseline,
        device_type=device_type,
    )


def test_linked_device_session_and_baseline_groups_never_cross_folds():
    units = [
        unit("a", device="d1", session="s1", condition="c1"),
        unit("b", device="d1", session="s2", condition="c2"),
        unit("c", device="d2", session="s3", condition="c3", baseline="base-x"),
        unit("d", device="d3", session="s4", condition="c4", baseline="base-x"),
        unit("e", condition="c5"),
        unit("f", condition="c6"),
    ]
    assignments = assign_grouped_folds(units, "leave_device", n_splits=3, seed=11)
    assert_no_group_leakage(units, assignments, "leave_device")
    by_key = {row.response_unit_key: row.fold for row in assignments}
    assert by_key["a"] == by_key["b"]
    assert by_key["c"] == by_key["d"]


def test_requested_holdout_dimension_is_also_isolated():
    units = [
        unit("a", condition="shared"),
        unit("b", condition="shared"),
        unit("c", condition="c2"),
        unit("d", condition="c3"),
    ]
    assignments = assign_grouped_folds(units, "leave_condition", n_splits=3)
    assert_no_group_leakage(units, assignments, "leave_condition")
    by_key = {row.response_unit_key: row.fold for row in assignments}
    assert by_key["a"] == by_key["b"]


def test_split_fails_when_independent_components_cannot_support_requested_cv():
    units = [unit("a", campaign="only"), unit("b", campaign="only")]
    with pytest.raises(ValueError, match="only 1 independent components"):
        assign_grouped_folds(units, "leave_campaign", n_splits=2)


def test_split_is_deterministic_and_manifest_is_persistence_ready():
    units = [unit(str(index)) for index in range(8)]
    first = assign_grouped_folds(units, "leave_device", n_splits=4, seed=17)
    second = assign_grouped_folds(list(reversed(units)), "leave_device", n_splits=4, seed=17)
    assert first == second
    manifest = fold_manifest(first, scheme="leave_device", seed=17)
    assert manifest[0]["response_unit_key"] == "0"
    assert manifest[0]["split_scheme"] == "leave_device"


def test_manual_leakage_is_detected():
    units = [unit("a", device="d1"), unit("b", device="d1")]
    assignments = [FoldAssignment("a", 0, "x"), FoldAssignment("b", 1, "y")]
    with pytest.raises(ValueError, match="protected groups cross folds"):
        assert_no_group_leakage(units, assignments, "leave_device")


def test_baseline_is_fitted_only_from_training_rows_and_falls_back_by_target():
    train = [
        unit("a", response=1.0, device_type="A"),
        unit("b", response=3.0, device_type="A"),
        unit("c", response=9.0, device_type="B"),
    ]
    model = BaselinePredictor("device_median").fit(train)
    assert model.predict(unit("test-a", device_type="A")) == 2.0
    assert model.predict(unit("test-unseen", device_type="unseen")) == 3.0


def test_zero_baseline_requires_fit_but_always_predicts_zero():
    model = BaselinePredictor("zero")
    with pytest.raises(RuntimeError):
        model.predict(unit("x"))
    assert model.fit([unit("train", response=100.0)]).predict(unit("x")) == 0.0


def test_metrics_report_support_baseline_gain_coverage_bias_and_tail_error():
    records = [
        PredictionRecord("a", 1.0, 1.0, 0.0, 0.5, 1.5),
        PredictionRecord("b", 2.0, 3.0, 0.0, 2.5, 3.5),
        PredictionRecord("c", 4.0, None, 0.0, supported=False),
    ]
    result = evaluate_predictions(records, catastrophic_error_limit=0.5)
    assert result.total_units == 3
    assert result.supported_units == 2
    assert result.supported_fraction == pytest.approx(2 / 3)
    assert result.mae == 0.5
    assert result.median_absolute_error == 0.5
    assert result.p90_absolute_error == pytest.approx(0.9)
    assert result.bias == 0.5
    assert result.baseline_mae == 1.5
    assert result.baseline_improvement == pytest.approx(2 / 3)
    assert result.interval_coverage == 0.5
    assert result.mean_interval_width == 1.0
    assert result.catastrophic_error_rate == 0.5


def test_metrics_reject_pseudo_replication_and_invalid_intervals():
    duplicate = [
        PredictionRecord("same", 1, 1, 0),
        PredictionRecord("same", 1, 1, 0),
    ]
    with pytest.raises(ValueError, match="unique"):
        evaluate_predictions(duplicate)
    with pytest.raises(ValueError, match="lower bound"):
        evaluate_predictions([PredictionRecord("a", 1, 1, 0, 2, 0)])


def test_metrics_do_not_emit_nan_when_every_prediction_abstains():
    result = evaluate_predictions(
        [PredictionRecord("a", 1, None, 0, supported=False)]
    )
    assert result.supported_fraction == 0
    assert result.mae is None
    assert not any(
        isinstance(value, float) and math.isnan(value)
        for value in result.__dict__.values()
    )
