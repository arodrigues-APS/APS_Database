import pytest

from aps.enrich.iv_parameters.contracts import (
    ExtractionConfig, MetricResult, SweepPoint, aggregate_replicates,
)
from aps.enrich.iv_parameters.on_resistance import extract_rdson
from aps.enrich.iv_parameters.threshold_voltage import extract_vth


def point(index, vg, vd, current, compliance=False):
    return SweepPoint(index, vg, vd, current, compliance)


def vth_config(**overrides):
    values = {
        "config_version": "vth-test-v1", "target_type": "delta_vth_v",
        "target_current_a": 0.01, "required_vds_v": 1.0,
        "vds_tolerance_v": 0.02, "sweep_direction": "ascending",
        "valid_min": -5.0, "valid_max": 10.0,
    }
    values.update(overrides)
    return ExtractionConfig(**values)


def rdson_config(**overrides):
    values = {
        "config_version": "rdson-test-v1", "target_type": "log_rdson_ratio",
        "required_vgs_v": 15.0, "vgs_tolerance_v": 0.05,
        "linear_vds_min_v": 0.01, "linear_vds_max_v": 1.0,
        "minimum_points": 3, "fit_intercept": True,
        "valid_min": 1.0, "valid_max": 1000.0,
    }
    values.update(overrides)
    return ExtractionConfig(**values)


def test_fixed_current_vth_uses_log_interpolation():
    result = extract_vth(
        [point(0, 1.0, 1.0, 0.001), point(1, 2.0, 1.0, 0.1)],
        vth_config(),
    )
    assert result.usable
    assert result.value == pytest.approx(1.5)


def test_vth_does_not_change_when_unrelated_peak_current_changes():
    base = [point(0, 1.0, 1.0, 0.001), point(1, 2.0, 1.0, 0.1)]
    first = extract_vth(base + [point(2, 3.0, 1.0, 1.0)], vth_config())
    second = extract_vth(base + [point(2, 3.0, 1.0, 100.0)], vth_config())
    assert first.value == pytest.approx(second.value)


def test_vth_filters_wrong_bias_and_compliance_points():
    rows = [
        point(0, 0.0, 2.0, 0.01), point(1, 0.5, 1.0, 0.01, True),
        point(2, 1.0, 1.0, 0.001), point(3, 2.0, 1.0, 0.1),
    ]
    assert extract_vth(rows, vth_config()).value == pytest.approx(1.5)


def test_descending_vth_sweep_uses_acquisition_order():
    rows = [point(0, 2.0, 1.0, 0.1), point(1, 1.0, 1.0, 0.001)]
    result = extract_vth(rows, vth_config(sweep_direction="descending"))
    assert result.value == pytest.approx(1.5)


def test_vth_requires_a_bracket():
    result = extract_vth(
        [point(0, 1.0, 1.0, 0.001), point(1, 2.0, 1.0, 0.002)],
        vth_config(),
    )
    assert not result.usable
    assert result.quality_reasons == ("target_current_not_bracketed",)


def test_rdson_robust_fit_recovers_slope_and_removes_outlier():
    # V = 20 mOhm * I + 5 mV fixture offset.
    rows = [
        point(i, 15.0, 0.005 + 0.020 * current, current)
        for i, current in enumerate([1, 2, 3, 4, 5])
    ]
    rows.append(point(6, 15.0, 0.70, 3.5))
    result = extract_rdson(rows, rdson_config())
    assert result.usable
    assert result.value == pytest.approx(20.0, rel=1e-6)
    assert result.diagnostics["retained_point_count"] == 5
    assert result.diagnostics["intercept_v"] == pytest.approx(0.005)


def test_rdson_rejects_bias_mismatch():
    rows = [point(i, 10.0, 0.02 * i, i) for i in range(1, 6)]
    result = extract_rdson(rows, rdson_config())
    assert not result.usable
    assert result.quality_reasons == ("insufficient_protocol_matched_points",)


def test_replicate_aggregate_reports_median_and_uncertainty():
    rows = [
        MetricResult(
            "vth_v", value, "V", "iv-parameters-v3.0", "cfg", "usable",
            uncertainty=0.01,
        )
        for value in (2.0, 2.1, 1.9)
    ]
    result = aggregate_replicates(rows)
    assert result.quality_status == "usable"
    assert result.value == 2.0
    assert result.replicate_count == 3
    assert result.uncertainty > 0.01


def test_single_replicate_is_screening_only_not_independent_evidence():
    row = MetricResult(
        "rdson_mohm", 20.0, "mohm", "iv-parameters-v3.0", "cfg", "usable"
    )
    result = aggregate_replicates([row])
    assert result.quality_status == "screening_only"
    assert result.quality_reasons == ("insufficient_replicates",)


def test_incompatible_replicates_fail_closed():
    rows = [
        MetricResult("vth_v", 2.0, "V", "iv-parameters-v3.0", "a", "usable"),
        MetricResult(
            "rdson_mohm", 20.0, "mohm", "iv-parameters-v3.0", "b", "usable"
        ),
    ]
    result = aggregate_replicates(rows)
    assert result.quality_status == "invalid"
    assert result.quality_reasons == ("incompatible_replicate_contracts",)


def test_configs_reject_invalid_contracts():
    with pytest.raises(ValueError):
        vth_config(sweep_direction="sideways")
    with pytest.raises(ValueError):
        ExtractionConfig("bad", "unknown")
