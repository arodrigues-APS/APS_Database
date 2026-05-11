#!/usr/bin/env python3
"""
Create the "Post-IV Prediction V2 - Physical Response Diagnostics" dashboard
in Apache Superset.

The dashboard visualizes the V2 physical prediction workflow:
  * model-run validation gates by stress type and physical target,
  * held-out observed vs predicted parameter responses,
  * residuals vs donor distance,
  * donor support / unsupported reason counts,
  * feature and strict-pair coverage,
  * exploratory confidence-labeled parameter and curve outputs.

Prerequisites:
  * V2 tables/views exist. This script applies schema/024_iv_physical_prediction.sql
    before registering datasets.
  * At least one model run has been trained and validated:
      python3 data_processing_scripts/ml_post_iv_physical_prediction.py \
          --extract-features --build-pairs --train --validate
  * Superset is reachable at SUPERSET_URL with SUPERSET_USER/PASS.

Usage:
    python3 data_processing_scripts/create_iv_physical_prediction_dashboard.py
"""

import json
import sys
from pathlib import Path

from db_config import SUPERSET_URL, get_connection
from superset_api import (
    get_session,
    find_database,
    find_or_create_dataset,
    refresh_dataset_columns,
    create_chart,
    create_or_update_dashboard,
    build_json_metadata,
)


DASHBOARD_TITLE = "Post-IV Prediction V2 - Physical Response Diagnostics"
DASHBOARD_SLUG = "post-iv-physical-prediction"

MODEL_VIEW = "iv_physical_prediction_model_summary_view"
VALIDATION_VIEW = "iv_physical_prediction_validation_view"
SUPPORT_VIEW = "iv_physical_prediction_support_summary_view"
PAIR_VIEW = "iv_physical_prediction_pair_coverage_view"
FEATURE_VIEW = "iv_physical_prediction_feature_coverage_view"
FLAG_VIEW = "iv_physical_prediction_quality_flag_view"
PARAM_SUMMARY_VIEW = "iv_physical_parameter_prediction_summary_view"
CURVE_VIEW = "iv_physical_curve_prediction_view"
CURVE_SHAPE_VIEW = "iv_physical_curve_shape_plot_view"

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "schema" / "024_iv_physical_prediction.sql"

TARGET_COLORS = {
    "IdVg / delta Vth": "#1f77b4",
    "IdVd / log Rds(on) ratio": "#d55e00",
    "gate_pass": "#2ca02c",
    "gate_fail": "#d62728",
    "not_validated": "#7f7f7f",
    "ok": "#2ca02c",
    "unsupported": "#d62728",
    "strong": "#2ca02c",
    "weak": "#ffbf00",
    "source_reference": "#1f77b4",
    "predicted_post": "#d55e00",
    "within_median_gate": "#2ca02c",
    "within_p90_gate": "#ffbf00",
    "outside_gate": "#d62728",
    "Within-condition validation": "#1f77b4",
    "Leave-condition validation": "#9467bd",
    "sc": "#1f77b4",
    "irradiation": "#d55e00",
}


def apply_schema():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text())
        conn.commit()


def ensure_views_exist():
    required = [
        MODEL_VIEW,
        VALIDATION_VIEW,
        SUPPORT_VIEW,
        PAIR_VIEW,
        FEATURE_VIEW,
        FLAG_VIEW,
        PARAM_SUMMARY_VIEW,
        CURVE_VIEW,
        CURVE_SHAPE_VIEW,
    ]
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            (required,),
        )
        found = {row[0] for row in cur.fetchall()}
        missing = [name for name in required if name not in found]
        if missing:
            sys.exit(
                "ERROR: SQL view(s) missing: "
                + ", ".join(missing)
                + "\nRun the V2 schema rebuild first."
            )

        cur.execute("SELECT COUNT(*) FROM iv_physical_model_runs")
        run_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM iv_physical_validation_residuals")
        residual_count = cur.fetchone()[0]
        if run_count == 0 or residual_count == 0:
            print(
                "  WARNING: no validated model results found yet. "
                "The dashboard will be created, but charts may be empty."
            )


def table_params(columns, order_by=None, row_limit=10000, filters=None):
    return {
        "query_mode": "raw",
        "all_columns": columns,
        "metrics": [],
        "groupby": [],
        "order_by_cols": [json.dumps(col) for col in (order_by or [])],
        "adhoc_filters": filters or [],
        "row_limit": row_limit,
        "include_time": False,
        "table_timestamp_format": "smart_date",
    }


def latest_filter():
    return {
        "expressionType": "SQL",
        "sqlExpression": "is_latest_validated_model_run = true",
        "clause": "WHERE",
    }


def latest_model_filter():
    return {
        "expressionType": "SQL",
        "sqlExpression": "is_latest_model_run = true",
        "clause": "WHERE",
    }


def intended_stress_target_filter():
    return {
        "expressionType": "SQL",
        "sqlExpression": "is_intended_stress_target = true",
        "clause": "WHERE",
    }


def support_ok_filter():
    return {
        "expressionType": "SQL",
        "sqlExpression": "support_status = 'ok'",
        "clause": "WHERE",
    }


def nonnull_filter(sql):
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "clause": "WHERE",
    }


def target_type_filter(target_type):
    return {
        "expressionType": "SQL",
        "sqlExpression": f"target_type = '{target_type}'",
        "clause": "WHERE",
    }


def plot_pair_rank_filter(limit=25):
    return {
        "expressionType": "SQL",
        "sqlExpression": f"plot_pair_rank <= {int(limit)}",
        "clause": "WHERE",
    }


def gate_metric_bar_params():
    return {
        "x_axis": "target_label",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "MAX(median_abs_residual)",
                "label": "Median abs residual",
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "MAX(p90_abs_residual)",
                "label": "P90 abs residual",
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "MAX(gate_median_abs_residual_max)",
                "label": "Median gate",
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "MAX(gate_p90_abs_residual_max)",
                "label": "P90 gate",
            },
        ],
        "groupby": ["validation_label", "reference_tier", "stress_type"],
        "adhoc_filters": [latest_filter(), intended_stress_target_filter()],
        "row_limit": 100,
        "show_legend": True,
        "rich_tooltip": True,
        "x_axis_title": "Physical Target",
        "y_axis_title": "Residual",
        "y_axis_format": "SMART_NUMBER",
        "stack": False,
        "label_colors": TARGET_COLORS,
    }


def support_bar_params():
    return {
        "x_axis": "target_label",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "SUM(n_validation_pairs)",
                "label": "Validation pairs",
            },
        ],
        "groupby": ["validation_label", "reference_tier", "stress_type", "support_status"],
        "adhoc_filters": [latest_filter()],
        "row_limit": 1000,
        "show_legend": True,
        "rich_tooltip": True,
        "x_axis_title": "Physical Target",
        "y_axis_title": "Validation Pairs",
        "y_axis_format": "SMART_NUMBER",
        "stack": True,
        "label_colors": TARGET_COLORS,
    }


def feature_bar_params():
    return {
        "x_axis": "data_source",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "SUM(n_features)",
                "label": "Feature rows",
            },
        ],
        "groupby": ["quality_status"],
        "adhoc_filters": [],
        "row_limit": 1000,
        "show_legend": True,
        "rich_tooltip": True,
        "x_axis_title": "Source",
        "y_axis_title": "Feature Rows",
        "y_axis_format": "SMART_NUMBER",
        "stack": True,
        "label_colors": TARGET_COLORS,
    }


def predicted_observed_scatter_params():
    return {
        "x_axis": "observed_value",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(predicted_value)",
                "label": "Predicted value",
            },
        ],
        "groupby": [
            "validation_label", "reference_tier", "target_label", "stress_type",
            "device_type", "pair_key",
        ],
        "adhoc_filters": [
            latest_filter(),
            support_ok_filter(),
            nonnull_filter("observed_value IS NOT NULL AND predicted_value IS NOT NULL"),
        ],
        "row_limit": 10000,
        "truncate_metric": True,
        "show_legend": False,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": "Observed Response",
        "y_axis_title": "Predicted Response",
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "markerEnabled": True,
        "markerSize": 7,
        "zoomable": True,
        "label_colors": TARGET_COLORS,
    }


def residual_distance_scatter_params():
    return {
        "x_axis": "donor_distance",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(abs_residual)",
                "label": "Abs residual",
            },
        ],
        "groupby": [
            "validation_label", "reference_tier", "target_label", "stress_type",
            "device_type", "pair_key",
        ],
        "adhoc_filters": [
            latest_filter(),
            support_ok_filter(),
            nonnull_filter("donor_distance IS NOT NULL AND abs_residual IS NOT NULL"),
        ],
        "row_limit": 10000,
        "truncate_metric": True,
        "show_legend": False,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": "Mean Donor Distance",
        "y_axis_title": "Absolute Residual",
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "markerEnabled": True,
        "markerSize": 7,
        "zoomable": True,
        "label_colors": TARGET_COLORS,
    }


def curve_shape_line_params(target_type, x_title):
    return {
        "x_axis": "plot_x_value",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(plot_i_drain)",
                "label": "I_Drain (A)",
            },
        ],
        "groupby": ["plot_series_label"],
        "adhoc_filters": [
            latest_model_filter(),
            support_ok_filter(),
            target_type_filter(target_type),
            plot_pair_rank_filter(25),
            nonnull_filter("plot_x_value IS NOT NULL AND plot_i_drain IS NOT NULL"),
        ],
        "row_limit": 100000,
        "truncate_metric": True,
        "show_legend": True,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": x_title,
        "y_axis_title": "I_Drain (A)",
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "tooltipTimeFormat": "smart_date",
        "markerEnabled": False,
        "connectNulls": True,
        "zoomable": True,
        "sort_series_type": "max",
        "sort_series_ascending": False,
        "series_limit": 200,
        "series_limit_metric": {
            "expressionType": "SQL",
            "sqlExpression": "MAX(confidence_score)",
            "label": "_rank_by_confidence",
        },
        "label_colors": TARGET_COLORS,
    }


def build_dashboard_layout(charts):
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [
                "ROW-summary",
                "ROW-model",
                "ROW-scatter",
                "ROW-validation",
                "ROW-generated",
                "ROW-generated-shapes",
                "ROW-coverage",
                "ROW-flags",
            ],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
        "ROW-summary": {
            "type": "ROW",
            "id": "ROW-summary",
            "children": ["CHART-summary"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-model": {
            "type": "ROW",
            "id": "ROW-model",
            "children": ["CHART-gates", "CHART-support"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-scatter": {
            "type": "ROW",
            "id": "ROW-scatter",
            "children": ["CHART-predobs", "CHART-resdist"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-validation": {
            "type": "ROW",
            "id": "ROW-validation",
            "children": ["CHART-validation-table"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-generated": {
            "type": "ROW",
            "id": "ROW-generated",
            "children": ["CHART-param-summary", "CHART-curve-table"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-generated-shapes": {
            "type": "ROW",
            "id": "ROW-generated-shapes",
            "children": ["CHART-idvg-curve-shapes", "CHART-idvd-curve-shapes"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-coverage": {
            "type": "ROW",
            "id": "ROW-coverage",
            "children": ["CHART-feature-bar", "CHART-pair-table"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-flags": {
            "type": "ROW",
            "id": "ROW-flags",
            "children": ["CHART-feature-table", "CHART-flag-table"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }
    layout_map = {
        "summary": ("CHART-summary", "ROW-summary"),
        "gates": ("CHART-gates", "ROW-model"),
        "support": ("CHART-support", "ROW-model"),
        "predobs": ("CHART-predobs", "ROW-scatter"),
        "resdist": ("CHART-resdist", "ROW-scatter"),
        "validation_table": ("CHART-validation-table", "ROW-validation"),
        "param_summary": ("CHART-param-summary", "ROW-generated"),
        "curve_table": ("CHART-curve-table", "ROW-generated"),
        "idvg_curve_shapes": ("CHART-idvg-curve-shapes", "ROW-generated-shapes"),
        "idvd_curve_shapes": ("CHART-idvd-curve-shapes", "ROW-generated-shapes"),
        "feature_bar": ("CHART-feature-bar", "ROW-coverage"),
        "pair_table": ("CHART-pair-table", "ROW-coverage"),
        "feature_table": ("CHART-feature-table", "ROW-flags"),
        "flag_table": ("CHART-flag-table", "ROW-flags"),
    }
    for key, (cid, cuuid, cname, width, height) in charts.items():
        if cid is None:
            continue
        chart_id, row_id = layout_map[key]
        layout[chart_id] = {
            "type": "CHART",
            "id": chart_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": cid,
                "width": width,
                "height": height,
                "sliceName": cname,
                "uuid": cuuid,
            },
        }
    return layout


def filter_select(filter_id, name, targets, chart_ids, cascade=None):
    return {
        "id": filter_id,
        "controlValues": {
            "enableEmptyFilter": False,
            "defaultToFirstItem": False,
            "multiSelect": True,
            "searchAllOptions": True,
            "inverseSelection": False,
        },
        "name": name,
        "filterType": "filter_select",
        "targets": targets,
        "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
        "cascadeParentIds": cascade or [],
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "type": "NATIVE_FILTER",
        "description": "",
        "chartsInScope": chart_ids,
        "tabsInScope": [],
    }


def build_native_filters(chart_ids, datasets, chart_groups):
    mode_id = "NATIVE_FILTER-ivphys-validation-mode"
    reference_id = "NATIVE_FILTER-ivphys-reference-tier"
    target_id = "NATIVE_FILTER-ivphys-target"
    device_id = "NATIVE_FILTER-ivphys-device"
    stress_id = "NATIVE_FILTER-ivphys-stress"
    support_id = "NATIVE_FILTER-ivphys-support"
    confidence_id = "NATIVE_FILTER-ivphys-confidence"
    pair_id = "NATIVE_FILTER-ivphys-pair-key"
    role_id = "NATIVE_FILTER-ivphys-curve-role"
    quality_id = "NATIVE_FILTER-ivphys-quality"

    return [
        filter_select(
            mode_id,
            "Validation Mode",
            [
                {"datasetId": datasets["model"], "column": {"name": "validation_mode"}},
                {"datasetId": datasets["validation"], "column": {"name": "validation_mode"}},
                {"datasetId": datasets["support"], "column": {"name": "validation_mode"}},
                {"datasetId": datasets["param_summary"], "column": {"name": "validation_mode_used"}},
                {"datasetId": datasets["curve"], "column": {"name": "validation_mode_used"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "validation_mode_used"}},
            ],
            chart_groups["validation_mode"],
        ),
        filter_select(
            reference_id,
            "Reference Tier",
            [
                {"datasetId": datasets["model"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["validation"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["support"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["pair"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["flag"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["param_summary"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["curve"], "column": {"name": "reference_tier"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "reference_tier"}},
            ],
            chart_groups["reference_tier"],
            cascade=[mode_id],
        ),
        filter_select(
            target_id,
            "Target",
            [
                {"datasetId": datasets["model"], "column": {"name": "target_type"}},
                {"datasetId": datasets["validation"], "column": {"name": "target_type"}},
                {"datasetId": datasets["support"], "column": {"name": "target_type"}},
                {"datasetId": datasets["pair"], "column": {"name": "target_type"}},
                {"datasetId": datasets["feature"], "column": {"name": "target_type"}},
                {"datasetId": datasets["flag"], "column": {"name": "target_type"}},
                {"datasetId": datasets["param_summary"], "column": {"name": "target_type"}},
                {"datasetId": datasets["curve"], "column": {"name": "target_type"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "target_type"}},
            ],
            chart_ids,
            cascade=[mode_id, reference_id],
        ),
        filter_select(
            device_id,
            "Device Type",
            [
                {"datasetId": datasets["validation"], "column": {"name": "device_type"}},
                {"datasetId": datasets["support"], "column": {"name": "device_type"}},
                {"datasetId": datasets["pair"], "column": {"name": "device_type"}},
                {"datasetId": datasets["feature"], "column": {"name": "device_type"}},
                {"datasetId": datasets["flag"], "column": {"name": "device_type"}},
                {"datasetId": datasets["param_summary"], "column": {"name": "device_type"}},
                {"datasetId": datasets["curve"], "column": {"name": "device_type"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "device_type"}},
            ],
            chart_groups["device"],
            cascade=[mode_id, reference_id, target_id],
        ),
        filter_select(
            stress_id,
            "Stress Type",
            [
                {"datasetId": datasets["validation"], "column": {"name": "stress_type"}},
                {"datasetId": datasets["support"], "column": {"name": "stress_type"}},
                {"datasetId": datasets["pair"], "column": {"name": "stress_type"}},
                {"datasetId": datasets["param_summary"], "column": {"name": "stress_type"}},
                {"datasetId": datasets["curve"], "column": {"name": "stress_type"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "stress_type"}},
            ],
            chart_groups["stress"],
            cascade=[mode_id, reference_id, target_id, device_id],
        ),
        filter_select(
            support_id,
            "Support Status",
            [
                {"datasetId": datasets["validation"], "column": {"name": "support_status"}},
                {"datasetId": datasets["support"], "column": {"name": "support_status"}},
                {"datasetId": datasets["param_summary"], "column": {"name": "support_status"}},
                {"datasetId": datasets["curve"], "column": {"name": "support_status"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "support_status"}},
            ],
            chart_groups["support"],
            cascade=[mode_id, reference_id, target_id, device_id, stress_id],
        ),
        filter_select(
            confidence_id,
            "Confidence",
            [
                {"datasetId": datasets["param_summary"], "column": {"name": "confidence_level"}},
                {"datasetId": datasets["curve"], "column": {"name": "confidence_level"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "confidence_level"}},
            ],
            chart_groups["confidence"],
            cascade=[mode_id, reference_id, target_id, device_id, stress_id],
        ),
        filter_select(
            pair_id,
            "Pair Key",
            [
                {"datasetId": datasets["curve"], "column": {"name": "pair_key"}},
                {"datasetId": datasets["curve_shape"], "column": {"name": "pair_key"}},
            ],
            chart_groups["pair_key"],
            cascade=[mode_id, reference_id, target_id, device_id, stress_id, confidence_id],
        ),
        filter_select(
            role_id,
            "Curve Role",
            [
                {"datasetId": datasets["curve_shape"], "column": {"name": "curve_role"}},
            ],
            chart_groups["curve_role"],
            cascade=[
                mode_id,
                reference_id,
                target_id,
                device_id,
                stress_id,
                confidence_id,
                pair_id,
            ],
        ),
        filter_select(
            quality_id,
            "Quality Status",
            [
                {"datasetId": datasets["pair"], "column": {"name": "quality_status"}},
                {"datasetId": datasets["feature"], "column": {"name": "quality_status"}},
                {"datasetId": datasets["flag"], "column": {"name": "quality_status"}},
            ],
            chart_groups["quality"],
            cascade=[target_id, device_id],
        ),
    ]


def main():
    print(f"Creating {DASHBOARD_TITLE} dashboard\n" + "=" * 62)

    print("1. Applying V2 prediction schema and dashboard views ...")
    apply_schema()
    ensure_views_exist()
    print("   OK")

    print("\n2. Authenticating with Superset ...")
    try:
        session = get_session()
    except Exception as exc:
        sys.exit(f"   ERROR: could not authenticate ({exc})")
    print("   OK")

    print("\n3. Finding database ...")
    db_id = find_database(session)
    if not db_id:
        sys.exit("   ERROR: database not found")

    print("\n4. Registering datasets ...")
    datasets = {
        "model": find_or_create_dataset(session, db_id, MODEL_VIEW),
        "validation": find_or_create_dataset(session, db_id, VALIDATION_VIEW),
        "support": find_or_create_dataset(session, db_id, SUPPORT_VIEW),
        "pair": find_or_create_dataset(session, db_id, PAIR_VIEW),
        "feature": find_or_create_dataset(session, db_id, FEATURE_VIEW),
        "flag": find_or_create_dataset(session, db_id, FLAG_VIEW),
        "param_summary": find_or_create_dataset(session, db_id, PARAM_SUMMARY_VIEW),
        "curve": find_or_create_dataset(session, db_id, CURVE_VIEW),
        "curve_shape": find_or_create_dataset(session, db_id, CURVE_SHAPE_VIEW),
    }
    if not all(datasets.values()):
        sys.exit("   ERROR: one or more datasets could not be registered")
    for ds_id in datasets.values():
        refresh_dataset_columns(session, ds_id)

    print("\n5. Creating charts ...")
    charts = {}

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Model Gate Summary",
        datasets["model"],
        "table",
        table_params(
            [
                "model_run_id",
                "trained_at",
                "model_status",
                "validation_label",
                "reference_tier",
                "stress_type",
                "target_label",
                "target_gate_status",
                "is_intended_stress_target",
                "train_pairs",
                "target_validation_pairs",
                "target_supported_validation_pairs",
                "target_unsupported_validation_pairs",
                "median_abs_residual",
                "gate_median_abs_residual_max",
                "p90_abs_residual",
                "gate_p90_abs_residual_max",
                "gate_pass",
                "curve_reconstruction_enabled",
                "artifact_path",
            ],
            order_by=[
                ["model_run_id", False],
                ["validation_mode", True],
                ["reference_tier", True],
                ["stress_type", True],
                ["target_label", True],
            ],
            row_limit=1000,
        ),
    )
    charts["summary"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Model Gate Summary",
        12,
        34,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Latest Residual Gates",
        datasets["model"],
        "echarts_timeseries_bar",
        gate_metric_bar_params(),
    )
    charts["gates"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Latest Residual Gates",
        6,
        42,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Support Status",
        datasets["support"],
        "echarts_timeseries_bar",
        support_bar_params(),
    )
    charts["support"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Support Status",
        6,
        42,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Observed vs Predicted",
        datasets["validation"],
        "echarts_timeseries_scatter",
        predicted_observed_scatter_params(),
    )
    charts["predobs"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Observed vs Predicted",
        6,
        52,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Residual vs Donor Distance",
        datasets["validation"],
        "echarts_timeseries_scatter",
        residual_distance_scatter_params(),
    )
    charts["resdist"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Residual vs Donor Distance",
        6,
        52,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Validation Residual Details",
        datasets["validation"],
        "table",
        table_params(
            [
                "model_run_id",
                "validation_label",
                "reference_tier",
                "target_label",
                "stress_type",
                "device_type",
                "pair_key",
                "support_status",
                "support_reason",
                "observed_value",
                "predicted_value",
                "predicted_p10",
                "predicted_p90",
                "residual",
                "abs_residual",
                "residual_gate_band",
                "donor_count",
                "donor_distance",
                "physical_device_key",
                "sc_voltage_v",
                "sc_duration_us",
                "ion_species",
                "beam_energy_mev",
                "let_surface",
                "fluence_at_meas",
            ],
            order_by=[
                ["model_run_id", False],
                ["target_label", True],
                ["abs_residual", False],
            ],
            filters=[latest_filter()],
            row_limit=10000,
        ),
    )
    charts["validation_table"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Validation Residual Details",
        12,
        54,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Generated Parameter Summary",
        datasets["param_summary"],
        "table",
        table_params(
            [
                "model_run_id",
                "is_latest_model_run",
                "validation_mode_used",
                "reference_tier",
                "stress_type",
                "target_label",
                "curve_family",
                "device_type",
                "confidence_level",
                "support_status",
                "support_reason",
                "validation_gate_pass",
                "validation_supported_fraction",
                "validation_supported_pairs",
                "validation_total_pairs",
                "baseline_reference_method",
                "n_parameter_predictions",
                "n_numeric_predictions",
                "n_unsupported_predictions",
                "n_parameters_with_curves",
                "n_curve_points",
                "avg_confidence_score",
                "median_confidence_score",
                "avg_donor_count",
                "median_donor_distance",
                "median_baseline_reference_spread",
                "p90_baseline_reference_spread",
            ],
            order_by=[
                ["model_run_id", False],
                ["confidence_level", True],
                ["n_parameter_predictions", False],
            ],
            filters=[latest_model_filter()],
            row_limit=10000,
        ),
    )
    charts["param_summary"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Generated Parameter Summary",
        6,
        48,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Generated Curve Points",
        datasets["curve"],
        "table",
        table_params(
            [
                "model_run_id",
                "is_latest_model_run",
                "validation_mode_used",
                "reference_tier",
                "confidence_level",
                "confidence_score",
                "support_status",
                "support_reason",
                "target_label",
                "stress_type",
                "device_type",
                "pair_key",
                "x_axis_name",
                "x_value",
                "source_x_value",
                "predicted_x_value",
                "bias_axis_name",
                "bias_value",
                "point_index",
                "pristine_i_drain",
                "predicted_post_i_drain",
                "predicted_parameter_value",
                "donor_count",
                "donor_distance",
                "validation_gate_pass",
                "validation_supported_fraction",
                "baseline_reference_count",
                "baseline_reference_spread",
                "ion_species",
                "beam_energy_mev",
                "let_surface",
                "fluence_at_meas",
            ],
            order_by=[
                ["model_run_id", False],
                ["confidence_level", True],
                ["pair_key", True],
                ["point_index", True],
            ],
            filters=[latest_model_filter()],
            row_limit=10000,
        ),
    )
    charts["curve_table"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Generated Curve Points",
        6,
        48,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - IdVg Curve Shapes",
        datasets["curve_shape"],
        "echarts_timeseries_line",
        curve_shape_line_params("delta_vth_v", "V_Gate (V)"),
    )
    charts["idvg_curve_shapes"] = (
        cid,
        cuuid,
        "IV Physical Prediction - IdVg Curve Shapes",
        6,
        58,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - IdVd Curve Shapes",
        datasets["curve_shape"],
        "echarts_timeseries_line",
        curve_shape_line_params("log_rdson_ratio", "V_Drain (V)"),
    )
    charts["idvd_curve_shapes"] = (
        cid,
        cuuid,
        "IV Physical Prediction - IdVd Curve Shapes",
        6,
        58,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Feature Coverage by Source",
        datasets["feature"],
        "echarts_timeseries_bar",
        feature_bar_params(),
    )
    charts["feature_bar"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Feature Coverage by Source",
        6,
        44,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Pair Coverage",
        datasets["pair"],
        "table",
        table_params(
            [
                "reference_tier",
                "pairing_method",
                "target_label",
                "stress_type",
                "device_type",
                "manufacturer",
                "quality_status",
                "same_physical_device",
                "n_pairs",
                "n_split_groups",
                "n_physical_devices",
                "n_pre_files",
                "n_post_files",
                "min_sc_voltage_v",
                "max_sc_voltage_v",
                "min_sc_duration_us",
                "max_sc_duration_us",
                "n_irrad_runs",
                "n_ion_species",
                "mean_baseline_reference_count",
                "median_baseline_reference_spread",
                "p90_baseline_reference_spread",
                "median_delta_vth_v",
                "median_log_rdson_ratio",
            ],
            order_by=[["n_pairs", False], ["target_label", True]],
            row_limit=10000,
        ),
    )
    charts["pair_table"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Pair Coverage",
        6,
        44,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Feature Coverage Details",
        datasets["feature"],
        "table",
        table_params(
            [
                "data_source",
                "stress_condition",
                "measurement_category",
                "target_label",
                "device_type",
                "manufacturer",
                "quality_status",
                "n_features",
                "n_files",
                "n_physical_devices",
                "n_with_vth",
                "n_with_rdson",
                "n_usable",
            ],
            order_by=[["n_features", False], ["data_source", True]],
            row_limit=10000,
        ),
    )
    charts["feature_table"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Feature Coverage Details",
        6,
        50,
    )

    cid, cuuid = create_chart(
        session,
        "IV Physical Prediction - Quality and Support Reasons",
        datasets["flag"],
        "table",
        table_params(
            [
                "record_type",
                "model_run_id",
                "validation_label",
                "reference_tier",
                "target_label",
                "curve_family",
                "data_source",
                "stress_condition",
                "device_type",
                "quality_status",
                "reason",
                "n_records",
            ],
            order_by=[["n_records", False], ["reason", True]],
            row_limit=10000,
        ),
    )
    charts["flag_table"] = (
        cid,
        cuuid,
        "IV Physical Prediction - Quality and Support Reasons",
        6,
        50,
    )

    chart_ids = [item[0] for item in charts.values() if item[0] is not None]
    if not chart_ids:
        sys.exit("   ERROR: no charts were created")

    print("\n6. Building dashboard ...")
    position_json = build_dashboard_layout(charts)
    validation_charts = [
        charts["predobs"][0],
        charts["resdist"][0],
        charts["validation_table"][0],
    ]
    support_charts = [charts["support"][0]]
    pair_charts = [charts["pair_table"][0]]
    curve_shape_charts = [
        charts["idvg_curve_shapes"][0],
        charts["idvd_curve_shapes"][0],
    ]
    prediction_charts = [
        charts["param_summary"][0],
        charts["curve_table"][0],
        charts["idvg_curve_shapes"][0],
        charts["idvd_curve_shapes"][0],
    ]
    feature_charts = [
        charts["feature_bar"][0],
        charts["feature_table"][0],
        charts["flag_table"][0],
    ]
    model_charts = [charts["summary"][0], charts["gates"][0]]
    chart_groups = {
        "validation_mode": [
            cid for cid in model_charts + validation_charts + support_charts + prediction_charts
            if cid
        ],
        "reference_tier": [
            cid
            for cid in (
                model_charts + validation_charts + support_charts
                + pair_charts + prediction_charts + [charts["flag_table"][0]]
            )
            if cid
        ],
        "device": [
            cid
            for cid in validation_charts + support_charts + pair_charts + prediction_charts + feature_charts
            if cid
        ],
        "stress": [
            cid
            for cid in validation_charts + support_charts + pair_charts + prediction_charts
            if cid
        ],
        "support": [cid for cid in validation_charts + support_charts + prediction_charts if cid],
        "confidence": [cid for cid in prediction_charts if cid],
        "pair_key": [cid for cid in [charts["curve_table"][0]] + curve_shape_charts if cid],
        "curve_role": [cid for cid in curve_shape_charts if cid],
        "quality": [cid for cid in pair_charts + feature_charts if cid],
    }
    native_filters = build_native_filters(
        chart_ids, datasets, chart_groups
    )
    json_metadata = build_json_metadata(chart_ids, native_filters)
    json_metadata["label_colors"] = TARGET_COLORS
    json_metadata["shared_label_colors"] = TARGET_COLORS

    dash_id = create_or_update_dashboard(
        session,
        DASHBOARD_TITLE,
        position_json,
        json_metadata,
        slug=DASHBOARD_SLUG,
    )
    if not dash_id:
        sys.exit("   ERROR: dashboard creation failed")

    print("\n7. Associating charts with dashboard ...")
    for cid in chart_ids:
        resp = session.put(
            f"{SUPERSET_URL}/api/v1/chart/{cid}",
            json={"dashboards": [dash_id]},
        )
        status = "OK" if resp.ok else f"FAIL ({resp.status_code})"
        print(f"   chart {cid} -> dashboard {dash_id}: {status}")

    print("\n" + "=" * 62)
    print("Dashboard ready!")
    print(f"  URL: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")
    print(f"  Charts: {len(chart_ids)}")
    print("  Filters: Validation Mode, Reference Tier, Target, Device Type, Stress Type, Support Status, Confidence, Pair Key, Curve Role, Quality Status")
    print("  Generated output charts: parameter summary, curve shape overlays, and curve point sample")


if __name__ == "__main__":
    main()
