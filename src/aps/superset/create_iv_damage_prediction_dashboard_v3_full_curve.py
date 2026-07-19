#!/usr/bin/env python3
"""Deploy the certified scalar + projected/functional full-curve V3 dashboard."""

from __future__ import annotations

from aps.db_config import SUPERSET_URL, get_connection
from aps.superset.nonproxy_dashboard_support import build_tabbed_layout
from aps.superset.superset_api import (
    build_json_metadata,
    create_chart,
    create_or_update_dashboard,
    find_database,
    find_or_create_dataset,
    get_session,
    refresh_dataset_columns,
)


DASHBOARD_TITLE = "IV Damage Predictor V3 — Certified Scalar & Full Curves"
DASHBOARD_SLUG = "iv-damage-predictor-v3"

DATASETS = {
    "scalar_gate": "iv_damage_release_gate_check_view",
    "scalar_validation": "iv_damage_validation_summary_view",
    "scalar_time": "iv_damage_temporal_monitoring_view",
    "backlog": "iv_damage_prediction_backlog_view",
    "curve_gate": "iv_damage_curve_release_gate_view",
    "curve_validation": "iv_damage_curve_validation_summary_view",
    "curve_prediction": "iv_damage_curve_prediction_view",
    "curve_time": "iv_damage_curve_temporal_monitoring_view",
}

TABS = {
    "scalar_gate": ("Scalar Release Gates", "TAB-v3-scalar-gates"),
    "scalar_validation": ("Scalar Validation", "TAB-v3-scalar-validation"),
    "scalar_monitoring": ("Scalar Prospective", "TAB-v3-scalar-monitoring"),
    "curve_gate": ("Full-Curve Release Gates", "TAB-v3-curve-gates"),
    "curve_validation": ("Full-Curve Validation", "TAB-v3-curve-validation"),
    "curve_prediction": ("Full-Curve Predictions", "TAB-v3-curve-predictions"),
    "curve_monitoring": ("Full-Curve Prospective", "TAB-v3-curve-monitoring"),
}

GUIDANCE = {
    TABS["scalar_gate"][1]: (
        "### Scalar claim boundary\n\nDevelopment, selection, one-time external certification, "
        "shadow monitoring, and decision release are independent gates. A green development gate alone "
        "does not authorize use. ΔVth is shown only in volts; log-RDS(on) response is shown only in ln(ratio)."
    ),
    TABS["scalar_validation"][1]: (
        "### Grouped diagnostics are not external certification\n\n`grouped_test` means each row was held "
        "out in that grouped diagnostic fold. `external_test` appears only for the one-time sealed certification."
    ),
    TABS["scalar_monitoring"][1]: (
        "### Prospective scalar evidence\n\nWeekly accuracy uses outcomes measured after prediction. "
        "Unmatched outcomes and abstentions remain in the operational denominator."
    ),
    TABS["curve_gate"][1]: (
        "### Full curves are a separate claim\n\nA scalar release and a deterministic shift/scale projection "
        "do not certify curve shape. Only a functional-curve model with its own external and prospective gates "
        "can become a decision release. Voltage is V and drain current is A."
    ),
    TABS["curve_validation"][1]: (
        "### One curve/device is the statistical unit\n\nErrors aggregate complete held-out curves; points are "
        "never counted as independent samples. The band metric requires every point of a curve to be covered."
    ),
    TABS["curve_prediction"][1]: (
        "### True shape-changing prediction\n\nThe functional model predicts the post-stress current vector "
        "from the pre-curve and stress covariates. Filter to one request before interpreting the overlay. "
        "Shadow curves are screening-only."
    ),
    TABS["curve_monitoring"][1]: (
        "### Prospective full-curve monitoring\n\nPromotion requires enough independently acquired post-curves, "
        "acceptable ampere error, simultaneous-band coverage, and abstention rate."
    ),
}


def metric(label: str, expression: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def table(columns: list[str]) -> dict:
    return {
        "query_mode": "raw", "all_columns": columns, "adhoc_filters": [],
        "row_limit": 5000, "include_search": True,
        "table_timestamp_format": "smart_date",
    }


def bar(x_axis: str, metrics: list[dict], groupby: list[str], *, sql_filter: str | None = None) -> dict:
    filters = [] if sql_filter is None else [{
        "expressionType": "SQL", "sqlExpression": sql_filter, "clause": "WHERE",
    }]
    return {
        "x_axis": x_axis, "metrics": metrics, "groupby": groupby,
        "adhoc_filters": filters, "row_limit": 10000, "show_legend": True,
        "rich_tooltip": True, "stack": False, "x_axis_sort_asc": True,
        "y_axis_format": ".3g",
    }


def line(x_axis: str, metrics: list[dict], groupby: list[str]) -> dict:
    return {
        "x_axis": x_axis, "metrics": metrics, "groupby": groupby,
        "adhoc_filters": [], "row_limit": 50000, "show_legend": True,
        "rich_tooltip": True, "x_axis_sort_asc": True,
        "y_axis_format": ".4g", "markerEnabled": False,
    }


def definitions() -> list[dict]:
    return [
        dict(name="V3 Scalar — Release Gate Matrix", ds="scalar_gate", tab="scalar_gate", viz="table", width=12, height=46,
             params=table(["model_version", "stress_type", "target_type", "response_unit", "release_status", "policy_version", "policy_approved", "candidate_selected", "external_certification_present", "external_certification_passed", "active_shadow", "active_decision_release", "certified_at", "acceptance_requirements", "latest_monitoring_passed", "latest_monitoring_at", "latest_monitoring_checks", "latest_monitoring_metrics"])),
        dict(name="V3 Scalar — ΔVth Validation Error (V)", ds="scalar_validation", tab="scalar_validation", viz="echarts_timeseries_bar", width=6, height=42,
             params=bar("split_scheme", [metric("median |error| (V)", "MAX(median_abs_error)"), metric("P90 |error| (V)", "MAX(p90_abs_error)")], ["model_version", "evaluation_kind", "split_role"], sql_filter="target_type = 'delta_vth_v'")),
        dict(name="V3 Scalar — log-RDS(on) Validation Error (ln ratio)", ds="scalar_validation", tab="scalar_validation", viz="echarts_timeseries_bar", width=6, height=42,
             params=bar("split_scheme", [metric("median |error| (ln ratio)", "MAX(median_abs_error)"), metric("P90 |error| (ln ratio)", "MAX(p90_abs_error)")], ["model_version", "evaluation_kind", "split_role"], sql_filter="target_type = 'log_rdson_ratio'")),
        dict(name="V3 Scalar — Weekly Prospective Monitoring", ds="scalar_time", tab="scalar_monitoring", viz="table", width=12, height=42,
             params=table(["monitoring_week", "model_version", "stress_type", "target_type", "response_unit", "predictions", "matched_outcomes", "abstentions", "mae", "bias", "interval_coverage", "deployment_mode"])),
        dict(name="V3 Scalar — Pending Request Backlog", ds="backlog", tab="scalar_monitoring", viz="table", width=12, height=34,
             params=table(["request_key", "physical_device_key", "stress_type", "target_type", "measurement_protocol_id", "request_status", "request_age", "created_at"])),
        dict(name="V3 Curve — Release Gate Matrix", ds="curve_gate", tab="curve_gate", viz="table", width=12, height=48,
             params=table(["model_version", "stress_type", "curve_family", "measurement_protocol_id", "x_unit", "current_unit", "release_status", "selected", "external_certification_passed", "active_shadow", "active_decision", "latest_monitoring_passed", "latest_monitoring_at"])),
        dict(name="V3 Curve — Held-Out Error (A)", ds="curve_validation", tab="curve_validation", viz="echarts_timeseries_bar", width=6, height=42,
             params=bar("split_scheme", [metric("mean curve MAE (A)", "MAX(mean_curve_mae_a)"), metric("P90 max point error (A)", "MAX(p90_max_abs_error_a)")], ["model_version", "evaluation_kind", "curve_family", "support_status"])),
        dict(name="V3 Curve — Simultaneous Band Coverage", ds="curve_validation", tab="curve_validation", viz="echarts_timeseries_bar", width=6, height=42,
             params=bar("split_scheme", [metric("whole-curve coverage", "MAX(simultaneous_band_coverage)")], ["model_version", "evaluation_kind", "curve_family", "support_status"])),
        dict(name="V3 Curve — Independent Validation Denominators", ds="curve_validation", tab="curve_validation", viz="table", width=12, height=36,
             params=table(["model_version", "split_scheme", "split_role", "evaluation_kind", "curve_family", "measurement_protocol_id", "support_status", "independent_curves", "physical_devices", "mean_curve_mae_a", "median_max_abs_error_a", "p90_max_abs_error_a", "mean_normalized_rmse", "simultaneous_band_coverage"])),
        dict(name="V3 Curve — Pre vs Predicted Post with Simultaneous Band", ds="curve_prediction", tab="curve_prediction", viz="echarts_timeseries_line", width=12, height=62,
             params=line("x_value_v", [metric("pre current (A)", "MAX(pre_i_drain_a)"), metric("predicted post (A)", "MAX(predicted_i_drain_a)"), metric("lower band (A)", "MAX(predicted_lower_a)"), metric("upper band (A)", "MAX(predicted_upper_a)")], ["request_key", "model_version", "deployment_mode", "curve_family"])),
        dict(name="V3 Curve — Prediction Provenance", ds="curve_prediction", tab="curve_prediction", viz="table", width=12, height=34,
             params=table(["request_key", "model_version", "stress_type", "curve_family", "measurement_protocol_id", "deployment_mode", "support_status", "evidence_status", "decision_eligible", "ood_score", "ood_threshold", "created_at"])),
        dict(name="V3 Curve — Weekly Prospective Monitoring", ds="curve_time", tab="curve_monitoring", viz="table", width=12, height=42,
             params=table(["monitoring_week", "model_version", "stress_type", "curve_family", "measurement_protocol_id", "deployment_mode", "curve_predictions", "matched_outcomes", "abstentions", "mean_curve_mae_a", "p90_max_abs_error_a", "simultaneous_band_coverage"])),
    ]


def _native_filter(fid: str, name: str, targets: list[tuple[int, str]], charts: list[int], all_charts: list[int], *, multi: bool = True) -> dict:
    return {
        "id": fid,
        "controlValues": {"enableEmptyFilter": False, "multiSelect": multi, "searchAllOptions": True, "inverseSelection": False},
        "name": name, "filterType": "filter_select",
        "targets": [{"datasetId": dataset, "column": {"name": column}} for dataset, column in targets],
        "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
        "scope": {"rootPath": ["ROOT_ID"], "excluded": [chart for chart in all_charts if chart not in charts]},
        "type": "NATIVE_FILTER", "description": name, "chartsInScope": charts,
        "tabsInScope": [], "cascadeParentIds": [],
    }


def verify_views() -> None:
    with get_connection() as connection, connection.cursor() as cursor:
        missing = []
        for view in DATASETS.values():
            cursor.execute("SELECT to_regclass(%s)", (f"public.{view}",))
            if cursor.fetchone()[0] is None:
                missing.append(view)
    if missing:
        raise RuntimeError(
            "certified scalar/full-curve schema is incomplete (missing: "
            + ", ".join(missing)
            + "). Apply forward migrations through schema/044."
        )


def create_dashboard() -> int | None:
    session = get_session()
    database_id = find_database(session)
    dataset_ids = {}
    for key, relation in DATASETS.items():
        dataset = find_or_create_dataset(session, database_id, relation)
        if dataset is None:
            raise RuntimeError(f"could not register {relation}")
        refresh_dataset_columns(session, dataset)
        dataset_ids[key] = dataset
    tabs = {key: [] for key in TABS}
    catalog = []
    for definition in definitions():
        chart_id, chart_uuid = create_chart(
            session, definition["name"], dataset_ids[definition["ds"]],
            definition["viz"], definition["params"],
            description=GUIDANCE[TABS[definition["tab"]][1]].split("\n\n", 1)[1],
        )
        tabs[definition["tab"]].append((chart_id, chart_uuid, definition["name"], definition["width"], definition["height"]))
        if chart_id:
            catalog.append({**definition, "chart_id": chart_id})
    layout = build_tabbed_layout(
        DASHBOARD_TITLE, "ivdamagev3",
        [(TABS[key][0], TABS[key][1], tabs[key]) for key in TABS], GUIDANCE,
    )
    all_charts = [item["chart_id"] for item in catalog]
    scalar_charts = [item["chart_id"] for item in catalog if item["ds"].startswith("scalar") or item["ds"] == "backlog"]
    curve_charts = [item["chart_id"] for item in catalog if item["ds"].startswith("curve")]
    curve_prediction_charts = [item["chart_id"] for item in catalog if item["ds"] == "curve_prediction"]
    curve_family_datasets = [
        key for key in dataset_ids if key.startswith("curve_")
    ]
    filters = [
        _native_filter("FILTER-v3-stress", "Stress Type", [(dataset_ids[key], "stress_type") for key in DATASETS], all_charts, all_charts),
        _native_filter("FILTER-v3-target", "Scalar Target (single)", [(dataset_ids[key], "target_type") for key in ("scalar_gate", "scalar_validation", "scalar_time", "backlog")], scalar_charts, all_charts, multi=False),
        _native_filter("FILTER-v3-family", "Curve Family (single)", [(dataset_ids[key], "curve_family") for key in curve_family_datasets], curve_charts, all_charts, multi=False),
        _native_filter("FILTER-v3-request", "Curve Request (single)", [(dataset_ids["curve_prediction"], "request_key")], curve_prediction_charts, all_charts, multi=False),
    ]
    metadata = build_json_metadata(all_charts, filters)
    metadata["cross_filters_enabled"] = False
    dashboard_id = create_or_update_dashboard(session, DASHBOARD_TITLE, layout, metadata, slug=DASHBOARD_SLUG)
    if dashboard_id:
        for chart_id in all_charts:
            response = session.put(f"{SUPERSET_URL}/api/v1/chart/{chart_id}", json={"dashboards": [dashboard_id]})
            if not response.ok:
                raise RuntimeError(f"could not associate chart {chart_id}: {response.status_code}")
    return dashboard_id


def main() -> None:
    verify_views()
    dashboard_id = create_dashboard()
    print(f"Dashboard ready: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/ (id={dashboard_id})")


if __name__ == "__main__":
    main()
