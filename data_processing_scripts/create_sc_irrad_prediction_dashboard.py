#!/usr/bin/env python3
"""
Create the "Predicted Irradiation Damage Equivalence" dashboard in Apache
Superset.

Uses the exploratory V2 prediction views built by ml_sc_irrad_equivalence.py:
  * damage_equivalence_prediction_fingerprint_view
  * damage_equivalence_prediction_match_view
  * damage_equivalence_prediction_coverage_view
  * damage_equivalence_prediction_match_segment_view

The measured damage-equivalence dashboard is intentionally left untouched.
This dashboard compares measured SC fingerprints against V2 predicted
irradiation fingerprints, with measured irradiation shown only as context.

Prerequisites:
  * Run: python3 data_processing_scripts/ml_sc_irrad_equivalence.py --rebuild
  * Superset is reachable at SUPERSET_URL with SUPERSET_USER/PASS.

Usage:
    python3 data_processing_scripts/create_sc_irrad_prediction_dashboard.py
"""

import json
import sys

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


DASHBOARD_TITLE = "Predicted Irradiation Damage Equivalence"
DASHBOARD_SLUG = "predicted-irrad-damage-equivalence"
FINGERPRINT_VIEW = "damage_equivalence_prediction_fingerprint_view"
MATCH_VIEW = "damage_equivalence_prediction_match_view"
COVERAGE_VIEW = "damage_equivalence_prediction_coverage_view"
SEGMENT_VIEW = "damage_equivalence_prediction_match_segment_view"

SOURCE_COLORS = {
    "sc": "#1f77b4",
    "irrad": "#7f7f7f",
    "predicted_irrad": "#9467bd",
}


def ensure_view_exists():
    required = [FINGERPRINT_VIEW, MATCH_VIEW, COVERAGE_VIEW, SEGMENT_VIEW]
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
                + "\nRun: python3 data_processing_scripts/ml_sc_irrad_equivalence.py --rebuild"
            )


def build_dashboard_layout(charts):
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [
                "ROW-coverage",
                "ROW-scatter",
                "ROW-links",
                "ROW-matches",
                "ROW-fingerprints",
            ],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
        "ROW-coverage": {
            "type": "ROW",
            "id": "ROW-coverage",
            "children": ["CHART-coverage"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-scatter": {
            "type": "ROW",
            "id": "ROW-scatter",
            "children": ["CHART-scatter-rds"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-links": {
            "type": "ROW",
            "id": "ROW-links",
            "children": ["CHART-link-rds"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-matches": {
            "type": "ROW",
            "id": "ROW-matches",
            "children": ["CHART-matches"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-fingerprints": {
            "type": "ROW",
            "id": "ROW-fingerprints",
            "children": ["CHART-fingerprints"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }
    layout_map = {
        "coverage": ("CHART-coverage", "ROW-coverage"),
        "scatter_rds": ("CHART-scatter-rds", "ROW-scatter"),
        "link_rds": ("CHART-link-rds", "ROW-links"),
        "matches": ("CHART-matches", "ROW-matches"),
        "fingerprints": ("CHART-fingerprints", "ROW-fingerprints"),
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


def filter_select(filter_id, name, targets, chart_ids, cascade=None,
                  default_value=None):
    default_mask = {"extraFormData": {}, "filterState": {"value": None}}
    if default_value is not None and targets:
        default_mask = {
            "extraFormData": {
                "filters": [{
                    "col": targets[0]["column"]["name"],
                    "op": "IN",
                    "val": [default_value],
                }],
            },
            "filterState": {"value": [default_value]},
        }
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
        "defaultDataMask": default_mask,
        "cascadeParentIds": cascade or [],
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "type": "NATIVE_FILTER",
        "description": "",
        "chartsInScope": chart_ids,
        "tabsInScope": [],
    }


def build_native_filters(chart_ids, fp_ds_id, match_ds_id, coverage_ds_id,
                         segment_ds_id, source_chart_ids, match_chart_ids,
                         coverage_chart_ids):
    latest_id = "NATIVE_FILTER-pred-latest-model"
    model_id = "NATIVE_FILTER-pred-model-run"
    device_id = "NATIVE_FILTER-pred-device-type"
    ref_id = "NATIVE_FILTER-pred-reference-tier"
    validation_id = "NATIVE_FILTER-pred-validation-mode"
    conf_id = "NATIVE_FILTER-pred-confidence"
    source_id = "NATIVE_FILTER-pred-source"
    ion_id = "NATIVE_FILTER-pred-ion"
    axes_id = "NATIVE_FILTER-pred-axes"
    status_id = "NATIVE_FILTER-pred-status"

    all_targets = lambda col: [
        {"datasetId": fp_ds_id, "column": {"name": col}},
        {"datasetId": match_ds_id, "column": {"name": col}},
        {"datasetId": coverage_ds_id, "column": {"name": col}},
        {"datasetId": segment_ds_id, "column": {"name": col}},
    ]

    return [
        filter_select(
            latest_id,
            "Latest Model",
            all_targets("is_latest_model_run"),
            chart_ids,
            default_value=True,
        ),
        filter_select(
            model_id,
            "Model Run",
            all_targets("model_run_id"),
            chart_ids,
            cascade=[latest_id],
        ),
        filter_select(
            device_id,
            "Device Type",
            all_targets("device_type"),
            chart_ids,
            cascade=[latest_id, model_id],
        ),
        filter_select(
            ref_id,
            "Reference Tier",
            all_targets("reference_tier"),
            chart_ids,
            cascade=[latest_id, model_id, device_id],
        ),
        filter_select(
            validation_id,
            "Validation Mode",
            all_targets("validation_mode_used"),
            chart_ids,
            cascade=[latest_id, model_id, device_id, ref_id],
        ),
        filter_select(
            conf_id,
            "Fingerprint Confidence",
            [
                {"datasetId": fp_ds_id, "column": {"name": "fingerprint_confidence"}},
                {"datasetId": match_ds_id, "column": {"name": "right_fingerprint_confidence"}},
                {"datasetId": segment_ds_id, "column": {"name": "fingerprint_confidence"}},
            ],
            source_chart_ids + match_chart_ids,
            cascade=[latest_id, model_id, device_id, ref_id, validation_id],
        ),
        filter_select(
            source_id,
            "Source",
            [{"datasetId": fp_ds_id, "column": {"name": "source"}}],
            source_chart_ids,
            cascade=[latest_id, model_id, device_id, ref_id, validation_id],
        ),
        filter_select(
            ion_id,
            "Ion Species",
            [
                {"datasetId": fp_ds_id, "column": {"name": "ion_species"}},
                {"datasetId": match_ds_id, "column": {"name": "right_ion_species"}},
                {"datasetId": segment_ds_id, "column": {"name": "ion_species"}},
            ],
            source_chart_ids + match_chart_ids,
            cascade=[latest_id, model_id, device_id, ref_id, validation_id],
        ),
        filter_select(
            axes_id,
            "Comparable Axes",
            [
                {"datasetId": match_ds_id, "column": {"name": "comparable_axis_labels"}},
                {"datasetId": coverage_ds_id, "column": {"name": "comparable_axis_labels"}},
                {"datasetId": segment_ds_id, "column": {"name": "comparable_axis_labels"}},
            ],
            match_chart_ids + coverage_chart_ids,
            cascade=[latest_id, model_id, device_id, ref_id, validation_id],
        ),
        filter_select(
            status_id,
            "Comparability Status",
            [
                {"datasetId": match_ds_id, "column": {"name": "comparability_status"}},
                {"datasetId": coverage_ds_id, "column": {"name": "comparability_status"}},
                {"datasetId": segment_ds_id, "column": {"name": "comparability_status"}},
            ],
            match_chart_ids + coverage_chart_ids,
            cascade=[latest_id, model_id, device_id, ref_id, validation_id],
        ),
    ]


def source_series_label(source, label, device_type):
    return ", ".join(str(part) for part in (source, label, device_type))


def load_source_label_colors():
    colors = dict(SOURCE_COLORS)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT source, label, device_type
            FROM {FINGERPRINT_VIEW}
            ORDER BY source, label, device_type
            """
        )
        for source, label, device_type in cur.fetchall():
            color = SOURCE_COLORS.get(source)
            if color:
                colors[source_series_label(source, label, device_type)] = color
    return colors


def scatter_params(x_col, y_col, x_label, y_label, label_colors):
    return {
        "x_axis": x_col,
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": f"AVG({y_col})",
            "label": y_label,
        }],
        "groupby": ["source", "label", "device_type"],
        "row_limit": 20000,
        "truncate_metric": True,
        "show_legend": False,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": x_label,
        "y_axis_title": y_label,
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "markerEnabled": True,
        "markerSize": 8,
        "zoomable": True,
        "label_colors": label_colors,
        "adhoc_filters": [{
            "expressionType": "SQL",
            "sqlExpression": f"{x_col} IS NOT NULL AND {y_col} IS NOT NULL",
            "clause": "WHERE",
        }],
    }


def match_link_params(x_col, y_col, x_label, y_label):
    return {
        "x_axis": x_col,
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": f"AVG({y_col})",
            "label": y_label,
        }],
        "groupby": ["match_label"],
        "row_limit": 20000,
        "series_limit": 300,
        "truncate_metric": True,
        "show_legend": False,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": x_label,
        "y_axis_title": y_label,
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "markerEnabled": True,
        "markerSize": 6,
        "connectNulls": False,
        "zoomable": True,
        "adhoc_filters": [{
            "expressionType": "SQL",
            "sqlExpression": f"{x_col} IS NOT NULL AND {y_col} IS NOT NULL",
            "clause": "WHERE",
        }],
    }


def coverage_table_params():
    return {
        "query_mode": "raw",
        "all_columns": [
            "model_run_id", "model_version", "is_latest_model_run",
            "reference_tier", "validation_mode_used",
            "pair_type", "comparability_status", "device_type",
            "n_left_fingerprints", "n_right_fingerprints",
            "comparable_pair_count", "comparable_right_count",
            "best_distance", "comparable_axis_labels",
            "left_dvth_fingerprints", "right_dvth_fingerprints",
            "left_drds_fingerprints", "right_drds_fingerprints",
            "prediction_count", "strong_prediction_count",
            "weak_prediction_count",
        ],
        "metrics": [],
        "groupby": [],
        "order_by_cols": [
            json.dumps(["is_latest_model_run", False]),
            json.dumps(["model_run_id", False]),
            json.dumps(["device_type", True]),
        ],
        "row_limit": 10000,
        "include_time": False,
        "table_timestamp_format": "smart_date",
    }


def match_table_params():
    return {
        "query_mode": "raw",
        "all_columns": [
            "model_run_id", "model_version", "is_latest_model_run",
            "reference_tier", "validation_mode_used",
            "pair_type", "comparability_status", "device_type",
            "right_source", "right_label", "right_irrad_run_id",
            "right_ion_species", "right_beam_energy_mev", "right_let_surface",
            "right_fingerprint_confidence", "right_prediction_count",
            "right_strong_prediction_count", "right_weak_prediction_count",
            "right_dvth_prediction_count", "right_drds_prediction_count",
            "right_median_confidence_score", "right_median_donor_count",
            "right_median_donor_distance",
            "right_median_validation_supported_fraction",
            "left_source", "left_label", "left_sc_voltage_v",
            "left_sc_duration_us", "match_rank", "nearest_distance",
            "comparable_axes", "comparable_axis_labels",
            "abs_delta_dvth", "abs_delta_drds", "abs_delta_dbv",
            "right_dvth", "left_dvth", "right_drds", "left_drds",
            "right_dbv", "left_dbv", "left_n_samples",
        ],
        "metrics": [],
        "groupby": [],
        "order_by_cols": [
            json.dumps(["is_latest_model_run", False]),
            json.dumps(["model_run_id", False]),
            json.dumps(["device_type", True]),
            json.dumps(["right_label", True]),
            json.dumps(["match_rank", True]),
        ],
        "row_limit": 10000,
        "include_time": False,
        "table_timestamp_format": "smart_date",
        "adhoc_filters": [{
            "expressionType": "SQL",
            "sqlExpression": "match_rank <= 3",
            "clause": "WHERE",
        }],
    }


def fingerprint_table_params():
    return {
        "query_mode": "raw",
        "all_columns": [
            "source", "is_predicted", "prediction_source",
            "model_run_id", "model_version", "is_latest_model_run",
            "reference_tier", "validation_mode_used",
            "fingerprint_confidence", "device_type", "label",
            "device_pair_status", "sc_voltage_v", "sc_duration_us",
            "ion_species", "beam_energy_mev", "let_surface",
            "let_bragg_peak", "range_um", "fluence_at_meas",
            "irrad_run_id", "dvth", "dvth_iqr", "dvth_n",
            "drds", "drds_iqr", "drds_n", "dbv", "dbv_iqr", "dbv_n",
            "n_samples", "measured_sample_count", "prediction_count",
            "strong_prediction_count", "weak_prediction_count",
            "dvth_prediction_count", "drds_prediction_count",
            "median_confidence_score", "median_donor_count",
            "median_donor_distance", "median_validation_supported_fraction",
            "median_validation_supported_pairs",
            "median_validation_total_pairs", "validation_gate_pass_all",
            "validation_gate_pass_count", "median_baseline_reference_count",
            "median_baseline_reference_spread", "baseline_reference_method",
            "median_predicted_post_vth", "median_predicted_post_rds",
        ],
        "metrics": [],
        "groupby": [],
        "order_by_cols": [
            json.dumps(["is_latest_model_run", False]),
            json.dumps(["model_run_id", False]),
            json.dumps(["source", True]),
            json.dumps(["device_type", True]),
            json.dumps(["label", True]),
        ],
        "row_limit": 20000,
        "include_time": False,
        "table_timestamp_format": "smart_date",
    }


def main():
    print("Creating Predicted Irradiation Damage Equivalence dashboard\n" + "=" * 70)

    print("1. Verifying prediction views exist ...")
    ensure_view_exists()
    print("   prediction damage-equivalence views found")

    print("\n2. Authenticating with Superset ...")
    try:
        session = get_session()
    except Exception as e:
        sys.exit(f"   ERROR: could not authenticate ({e})")
    print("   OK")

    print("\n3. Finding database ...")
    db_id = find_database(session)
    if not db_id:
        sys.exit("   ERROR: database not found")

    print("\n4. Registering datasets ...")
    fp_ds_id = find_or_create_dataset(session, db_id, FINGERPRINT_VIEW)
    match_ds_id = find_or_create_dataset(session, db_id, MATCH_VIEW)
    coverage_ds_id = find_or_create_dataset(session, db_id, COVERAGE_VIEW)
    segment_ds_id = find_or_create_dataset(session, db_id, SEGMENT_VIEW)
    if not all([fp_ds_id, match_ds_id, coverage_ds_id, segment_ds_id]):
        sys.exit("   ERROR: dataset registration failed")
    for ds_id in (fp_ds_id, match_ds_id, coverage_ds_id, segment_ds_id):
        refresh_dataset_columns(session, ds_id)

    print("\n5. Creating charts ...")
    charts = {}
    source_label_colors = load_source_label_colors()

    cid, cuuid = create_chart(
        session,
        "Predicted Damage Coverage",
        coverage_ds_id,
        "table",
        coverage_table_params(),
    )
    charts["coverage"] = (cid, cuuid, "Predicted Damage Coverage", 12, 28)

    cid, cuuid = create_chart(
        session,
        "Predicted Damage: ΔVth vs ΔRds(on)",
        fp_ds_id,
        "echarts_timeseries_scatter",
        scatter_params(
            "dvth",
            "drds",
            "ΔVth (V)",
            "ΔRds(on) (mΩ)",
            source_label_colors,
        ),
    )
    charts["scatter_rds"] = (
        cid,
        cuuid,
        "Predicted Damage: ΔVth vs ΔRds(on)",
        12,
        55,
    )

    cid, cuuid = create_chart(
        session,
        "Predicted Irrad Match Links: ΔVth vs ΔRds(on)",
        segment_ds_id,
        "echarts_timeseries_line",
        match_link_params("dvth", "drds", "ΔVth (V)", "ΔRds(on) (mΩ)"),
    )
    charts["link_rds"] = (
        cid,
        cuuid,
        "Predicted Irrad Match Links: ΔVth vs ΔRds(on)",
        12,
        45,
    )

    cid, cuuid = create_chart(
        session,
        "Predicted Irrad Nearest SC Equivalents",
        match_ds_id,
        "table",
        match_table_params(),
    )
    charts["matches"] = (
        cid,
        cuuid,
        "Predicted Irrad Nearest SC Equivalents",
        12,
        55,
    )

    cid, cuuid = create_chart(
        session,
        "Predicted Damage Fingerprints",
        fp_ds_id,
        "table",
        fingerprint_table_params(),
    )
    charts["fingerprints"] = (
        cid,
        cuuid,
        "Predicted Damage Fingerprints",
        12,
        45,
    )

    chart_ids = [c[0] for c in charts.values() if c[0] is not None]
    if not chart_ids:
        sys.exit("   ERROR: no charts were created")

    print("\n6. Building dashboard ...")
    position_json = build_dashboard_layout(charts)
    source_chart_ids = [
        charts["scatter_rds"][0],
        charts["fingerprints"][0],
    ]
    source_chart_ids = [cid for cid in source_chart_ids if cid is not None]
    match_chart_ids = [
        charts["link_rds"][0],
        charts["matches"][0],
    ]
    match_chart_ids = [cid for cid in match_chart_ids if cid is not None]
    coverage_chart_ids = [
        charts["coverage"][0],
    ] if charts["coverage"][0] is not None else []
    native_filters = build_native_filters(
        chart_ids,
        fp_ds_id,
        match_ds_id,
        coverage_ds_id,
        segment_ds_id,
        source_chart_ids,
        match_chart_ids,
        coverage_chart_ids,
    )
    json_metadata = build_json_metadata(chart_ids, native_filters)
    json_metadata["label_colors"] = source_label_colors
    json_metadata["shared_label_colors"] = source_label_colors

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

    print("\n" + "=" * 70)
    print("Dashboard ready!")
    print(f"  URL: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")
    print(f"  Charts: {len(chart_ids)}")
    print(
        "  Filters: Latest Model, Model Run, Device Type, Reference Tier, "
        "Validation Mode, Confidence, Source, Ion, Axes, Status"
    )


if __name__ == "__main__":
    main()
