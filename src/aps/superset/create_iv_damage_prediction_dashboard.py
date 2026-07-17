#!/usr/bin/env python3
"""Create the prospective IV Damage Predictor V3 dashboard.

The dashboard separates release governance, grouped validation, operational
scoring/abstention, and outcomes.  It never registers the legacy V2 donor
prediction tables as an operational datasource.
"""

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


DASHBOARD_TITLE = "IV Damage Predictor — Release & Prospective Monitoring"
DASHBOARD_SLUG = "iv-damage-predictor-v3"

DATASETS = {
    "models": "iv_damage_model_card_view",
    "validation": "iv_damage_validation_summary_view",
    "monitoring": "iv_damage_prediction_monitoring_view",
    "eligible": "iv_damage_decision_eligible_prediction_view",
}

DATASET_COLUMNS = {
    "models": {
        "model_version", "model_name", "stress_type", "target_type", "algorithm",
        "release_status", "created_at", "validated_at", "released_at",
        "policy_version", "acceptance_policy_approved", "snapshot_version",
        "snapshot_hash", "snapshot_rows", "independent_group_count",
        "is_active_release", "validation_metrics", "released_domain",
    },
    "validation": {
        "model_run_id", "split_scheme", "split_role", "stress_type", "target_type",
        "device_type", "ion_species", "support_status", "independent_units",
        "physical_devices", "campaigns", "mean_abs_error", "median_abs_error",
        "p90_abs_error", "mean_bias", "interval_coverage",
    },
    "monitoring": {
        "prediction_id", "model_run_id", "request_id", "request_key", "stress_type",
        "target_type", "device_type", "support_status", "evidence_status", "in_domain",
        "decision_eligible", "ood_score", "ood_threshold", "predicted_response",
        "predicted_response_lower", "predicted_response_upper", "observed_response",
        "residual", "abs_residual", "interval_hit", "created_at", "matched_at",
    },
    "eligible": {
        "id", "model_run_id", "request_key", "physical_device_key", "device_type",
        "stress_type", "target_type", "pre_value", "predicted_response",
        "predicted_response_lower", "predicted_response_upper", "predicted_post_value",
        "predicted_post_lower", "predicted_post_upper", "support_status",
        "evidence_status", "in_domain", "validation_gate_passed", "decision_eligible",
        "ood_score", "ood_threshold", "model_version", "algorithm", "activated_at",
        "created_at", "reference_policy",
    },
}

TABS = {
    "release": ("Release Governance", "TAB-ivdamage-release"),
    "validation": ("Grouped Validation", "TAB-ivdamage-validation"),
    "operations": ("Scoring & Abstention", "TAB-ivdamage-operations"),
    "outcomes": ("Prospective Outcomes", "TAB-ivdamage-outcomes"),
}

GUIDANCE = {
    TABS["release"][1]: (
        "### Start here: release state is the claim boundary\n\n"
        "A candidate, validated, or shadow model is **not released**. Decision use requires "
        "an approved acceptance policy, an active released model for the exact stress/target "
        "domain, same-device pre-stress reference, complete features, in-domain support, and "
        "a passed validation gate. Empty tables mean no claim is currently available."
    ),
    TABS["validation"][1]: (
        "### Independent grouped evidence\n\n"
        "Counts are response units and distinct physical devices—not curve points. Read "
        "leave-device, leave-condition, leave-run/campaign, and external-test results separately. "
        "ΔVth errors are volts; log RDS(on) ratio errors are natural-log units, so filter to one "
        "target before interpreting magnitudes. Unsupported rows remain visible."
    ),
    TABS["operations"][1]: (
        "### Predictions and abstentions\n\n"
        "The status chart includes out-of-domain, insufficient-evidence, and invalid requests. "
        "The eligible table is sourced from the canonical database view and therefore contains "
        "only active-release, validation-gated, same-device, in-domain predictions."
    ),
    TABS["outcomes"][1]: (
        "### Prospective validation only\n\n"
        "Outcomes are measurements matched after prediction creation. Residual and interval-hit "
        "statistics exclude unmatched requests but the unmatched count remains visible in the "
        "coverage table. This tab is the evidence used to detect drift after release."
    ),
}

DESCRIPTIONS = {
    "IV Damage V3 — Model Release Registry": "One row per immutable model run. Active release and policy approval are separate fields; only a released and active model can reach the canonical decision view.",
    "IV Damage V3 — Models by Release State": "Model-run count by stress/target and lifecycle state. This is governance inventory, not prediction accuracy.",
    "IV Damage V3 — Grouped Validation Summary": "One aggregate per model, split, domain subgroup, and support status. independent_units is the denominator and physical_devices exposes residual clustering.",
    "IV Damage V3 — Validation Error": "Grouped-validation median and P90 absolute response error. Filter to one target because ΔVth uses V while log RDS(on) ratio is dimensionless natural log.",
    "IV Damage V3 — Validation Support": "Independent held-out response units by in-domain or abstention status. Unsupported units remain in the displayed denominator.",
    "IV Damage V3 — Interval Coverage": "Empirical held-out interval-hit fraction by split and domain subgroup. It is marginal validation coverage, not a posterior probability for one prediction.",
    "IV Damage V3 — Scoring Status": "Prospective request results by support and evidence status, including abstentions. Counts are prediction requests, not independent outcome observations.",
    "IV Damage V3 — Decision-Eligible Predictions": "Canonical active-release, same-device, in-domain predictions only. Response and post-value intervals retain target-specific units and model/release provenance.",
    "IV Damage V3 — Outcome Coverage": "Prospective predictions split by outcome matched/unmatched. Accuracy statistics use matched outcomes only; this chart keeps the missing-outcome denominator visible.",
    "IV Damage V3 — Prospective Residuals": "One matched prospective outcome per prediction request, with response residual, interval hit, OOD diagnostics, and timestamps. It is not retrospective training-fold evidence.",
}


def metric(label: str, expression: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def table(columns: list[str], *, metrics=None, groupby=None, row_limit=5000) -> dict:
    if metrics:
        return {
            "query_mode": "aggregate", "groupby": list(groupby or []),
            "metrics": list(metrics), "adhoc_filters": [], "row_limit": row_limit,
            "include_search": True, "table_timestamp_format": "smart_date",
        }
    return {
        "query_mode": "raw", "all_columns": columns, "adhoc_filters": [],
        "row_limit": row_limit, "include_search": True,
        "table_timestamp_format": "smart_date",
    }


def bar(x_axis: str, metrics: list[dict], *, groupby: list[str]) -> dict:
    return {
        "x_axis": x_axis, "metrics": metrics, "groupby": groupby,
        "adhoc_filters": [], "row_limit": 10000, "show_legend": True,
        "rich_tooltip": True, "stack": False, "x_axis_sort_asc": True,
        "y_axis_format": ".3g",
    }


def chart_definitions(ids: dict[str, int]) -> list[dict]:
    return [
        dict(name="IV Damage V3 — Model Release Registry", ds="models", tab="release", viz="table", width=12, height=42,
             params=table(["model_version", "stress_type", "target_type", "algorithm", "release_status", "is_active_release", "policy_version", "acceptance_policy_approved", "snapshot_version", "independent_group_count", "created_at", "validated_at", "released_at"])),
        dict(name="IV Damage V3 — Models by Release State", ds="models", tab="release", viz="echarts_timeseries_bar", width=12, height=38,
             params=bar("release_status", [metric("model runs", "COUNT(*)")], groupby=["stress_type", "target_type"])),
        dict(name="IV Damage V3 — Grouped Validation Summary", ds="validation", tab="validation", viz="table", width=12, height=42,
             params=table(["model_run_id", "split_scheme", "split_role", "stress_type", "target_type", "device_type", "ion_species", "support_status", "independent_units", "physical_devices", "campaigns", "mean_abs_error", "median_abs_error", "p90_abs_error", "mean_bias", "interval_coverage"])),
        dict(name="IV Damage V3 — Validation Error", ds="validation", tab="validation", viz="echarts_timeseries_bar", width=6, height=42,
             params=bar("split_scheme", [metric("median absolute error", "MAX(median_abs_error)"), metric("P90 absolute error", "MAX(p90_abs_error)")], groupby=["split_role", "target_type"])),
        dict(name="IV Damage V3 — Validation Support", ds="validation", tab="validation", viz="echarts_timeseries_bar", width=6, height=42,
             params={**bar("split_scheme", [metric("independent units", "SUM(independent_units)")], groupby=["support_status", "split_role"]), "stack": True}),
        dict(name="IV Damage V3 — Interval Coverage", ds="validation", tab="validation", viz="echarts_timeseries_bar", width=12, height=38,
             params=bar("split_scheme", [metric("interval coverage", "AVG(interval_coverage)")], groupby=["split_role", "target_type", "device_type"])),
        dict(name="IV Damage V3 — Scoring Status", ds="monitoring", tab="operations", viz="echarts_timeseries_bar", width=12, height=38,
             params={**bar("support_status", [metric("prediction requests", "COUNT(DISTINCT request_id)")], groupby=["evidence_status", "stress_type", "target_type"]), "stack": True}),
        dict(name="IV Damage V3 — Decision-Eligible Predictions", ds="eligible", tab="operations", viz="table", width=12, height=52,
             params=table(["request_key", "model_version", "stress_type", "target_type", "device_type", "physical_device_key", "pre_value", "predicted_response", "predicted_response_lower", "predicted_response_upper", "predicted_post_value", "predicted_post_lower", "predicted_post_upper", "ood_score", "ood_threshold", "activated_at", "created_at"])),
        dict(name="IV Damage V3 — Outcome Coverage", ds="monitoring", tab="outcomes", viz="table", width=12, height=34,
             params=table([], metrics=[metric("predictions", "COUNT(DISTINCT prediction_id)"), metric("matched outcomes", "COUNT(DISTINCT request_id) FILTER (WHERE observed_response IS NOT NULL)"), metric("unmatched outcomes", "COUNT(DISTINCT request_id) FILTER (WHERE observed_response IS NULL)")], groupby=["stress_type", "target_type", "device_type"])),
        dict(name="IV Damage V3 — Prospective Residuals", ds="monitoring", tab="outcomes", viz="table", width=12, height=54,
             params={**table(["request_key", "model_run_id", "stress_type", "target_type", "device_type", "predicted_response", "predicted_response_lower", "predicted_response_upper", "observed_response", "residual", "abs_residual", "interval_hit", "ood_score", "created_at", "matched_at"]), "adhoc_filters": [{"expressionType": "SQL", "sqlExpression": "observed_response IS NOT NULL", "clause": "WHERE"}]}),
    ]


def _filter(fid: str, name: str, targets: list[tuple[int, str]], scoped: list[int], all_ids: list[int], parents=None) -> dict:
    return {
        "id": fid, "controlValues": {"enableEmptyFilter": False, "multiSelect": True,
        "searchAllOptions": True, "inverseSelection": False}, "name": name,
        "filterType": "filter_select",
        "targets": [{"datasetId": dataset, "column": {"name": column}} for dataset, column in targets],
        "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
        "cascadeParentIds": list(parents or []),
        "scope": {"rootPath": ["ROOT_ID"], "excluded": [cid for cid in all_ids if cid not in scoped]},
        "type": "NATIVE_FILTER", "description": name, "chartsInScope": scoped,
        "tabsInScope": [],
    }


def native_filters(catalog: list[dict], ids: dict[str, int]) -> list[dict]:
    all_ids = [row["chart_id"] for row in catalog]
    def scope(keys):
        return [row["chart_id"] for row in catalog if row["ds"] in keys]
    stress = "NATIVE_FILTER-ivdamage-stress"
    target = "NATIVE_FILTER-ivdamage-target"
    shared = list(ids)
    return [
        _filter(stress, "Stress Type", [(ids[key], "stress_type") for key in shared], all_ids, all_ids),
        _filter(target, "Target", [(ids[key], "target_type") for key in shared], all_ids, all_ids, [stress]),
        _filter("NATIVE_FILTER-ivdamage-device", "Device Type", [(ids[key], "device_type") for key in ("validation", "monitoring", "eligible")], scope({"validation", "monitoring", "eligible"}), all_ids, [stress, target]),
        _filter("NATIVE_FILTER-ivdamage-split", "Validation Split", [(ids["validation"], "split_scheme")], scope({"validation"}), all_ids, [stress, target]),
        _filter("NATIVE_FILTER-ivdamage-release", "Release Status", [(ids["models"], "release_status")], scope({"models"}), all_ids, [stress, target]),
    ]


def verify_views() -> None:
    with get_connection() as connection, connection.cursor() as cursor:
        missing = []
        for view in DATASETS.values():
            cursor.execute("SELECT to_regclass(%s)", (f"public.{view}",))
            if cursor.fetchone()[0] is None:
                missing.append(view)
        if missing:
            raise RuntimeError(
                "V3 damage-prediction database model is not prepared (missing: "
                + ", ".join(missing)
                + "). Apply forward migrations through schema/033 before deploying the dashboard."
            )


def create_dashboard() -> int | None:
    session = get_session()
    database_id = find_database(session)
    ids: dict[str, int] = {}
    for key, view in DATASETS.items():
        dataset_id = find_or_create_dataset(session, database_id, view)
        if dataset_id is None:
            raise RuntimeError(f"Could not register {view}")
        refresh_dataset_columns(session, dataset_id)
        ids[key] = dataset_id

    deployed: list[dict] = []
    tabs = {key: [] for key in TABS}
    for definition in chart_definitions(ids):
        chart_id, chart_uuid = create_chart(
            session, definition["name"], ids[definition["ds"]], definition["viz"],
            definition["params"], description=DESCRIPTIONS[definition["name"]],
        )
        tabs[definition["tab"]].append((chart_id, chart_uuid, definition["name"], definition["width"], definition["height"]))
        if chart_id:
            deployed.append({**definition, "chart_id": chart_id})

    layout = build_tabbed_layout(
        DASHBOARD_TITLE, "ivdamage",
        [(TABS[key][0], TABS[key][1], tabs[key]) for key in TABS], GUIDANCE,
    )
    chart_ids = [row["chart_id"] for row in deployed]
    metadata = build_json_metadata(chart_ids, native_filters(deployed, ids))
    metadata["cross_filters_enabled"] = False
    metadata["chart_configuration"] = {}
    dashboard_id = create_or_update_dashboard(
        session, DASHBOARD_TITLE, layout, metadata, slug=DASHBOARD_SLUG
    )
    if dashboard_id:
        for chart_id in chart_ids:
            response = session.put(
                f"{SUPERSET_URL}/api/v1/chart/{chart_id}",
                json={"dashboards": [dashboard_id]},
            )
            if not response.ok:
                raise RuntimeError(f"Could not associate chart {chart_id}: {response.status_code}")
    return dashboard_id


def main() -> None:
    verify_views()
    dashboard_id = create_dashboard()
    print(f"Dashboard ready: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/ (id={dashboard_id})")


if __name__ == "__main__":
    main()
