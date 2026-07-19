from aps.superset import create_iv_damage_prediction_dashboard_v3 as complete


def test_complete_dashboard_separates_scalar_projection_and_functional_claims():
    definitions = complete.dashboard.definitions()
    by_dataset = {row["ds"] for row in definitions}
    assert "scalar_gate" in by_dataset
    assert "curve_projection_gate" in by_dataset
    assert "curve_projection" in by_dataset
    assert "curve_gate" in by_dataset
    assert "curve_prediction" in by_dataset
    assert complete.dashboard.DATASETS["curve_projection"] == "iv_damage_curve_projection_view"
    assert complete.dashboard.DATASETS["curve_prediction"] == "iv_damage_curve_prediction_view"


def test_target_specific_units_and_curve_denominators_are_explicit():
    definitions = complete.dashboard.definitions()
    names = {row["name"] for row in definitions}
    assert "V3 Scalar — ΔVth Validation Error (V)" in names
    assert "V3 Scalar — log-RDS(on) Validation Error (ln ratio)" in names
    assert "V3 Curve — Held-Out Error (A)" in names
    denominator = next(row for row in definitions if "Independent Validation Denominators" in row["name"])
    assert {"independent_curves", "physical_devices"} <= set(denominator["params"]["all_columns"])


def test_scalar_dashboard_exposes_policy_monitoring_and_deployment_mode():
    definitions = complete.dashboard.definitions()
    gate = next(row for row in definitions if row["name"] == "V3 Scalar — Release Gate Matrix")
    assert {
        "acceptance_requirements", "latest_monitoring_passed",
        "latest_monitoring_at", "latest_monitoring_checks",
        "latest_monitoring_metrics",
    } <= set(gate["params"]["all_columns"])
    temporal = next(
        row for row in definitions
        if row["name"] == "V3 Scalar — Weekly Prospective Monitoring"
    )
    assert "deployment_mode" in temporal["params"]["all_columns"]


def test_curve_overlay_has_pre_prediction_and_simultaneous_band_in_amperes():
    definitions = complete.dashboard.definitions()
    learned = next(row for row in definitions if "Pre vs Predicted Post" in row["name"])
    labels = {metric["label"] for metric in learned["params"]["metrics"]}
    assert labels == {
        "pre current (A)", "predicted post (A)",
        "lower band (A)", "upper band (A)",
    }
    assert learned["params"]["x_axis"] == "x_value_v"
