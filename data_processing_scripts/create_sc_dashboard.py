#!/usr/bin/env python3
"""
Create the "Short Circuit" dashboard in Apache Superset via its REST API.

This dashboard visualises Short-Circuit ruggedness test data:
  - Pre-SC (pristine) vs post-SC IV curve overlays per device sample
  - SC event waveforms (oscilloscope time-domain captures)
  - Degradation tracking (Vth shift, Rdson change vs SC stress)

Datasets (SQL views created by ingestion_sc.py):
  1. sc_ruggedness_view       – all SC IV curves
  2. sc_waveform_view         – time-domain SC event captures
  3. sc_degradation_summary   – pre-aggregated per condition

Tabs:
  1. Pre/Post SC Comparison   – overlay pristine vs post-SC curves
  2. SC Waveform Viewer       – Vds / Id / Vgs vs time
  3. Individual Runs          – per-file curves with full metadata
  4. Degradation Tracking     – Vth shift & Rdson change vs SC duration

Filters (cascading):
  1. Manufacturer             – optional multi-select
  2. Device Type              – cascades from Manufacturer
  3. Sample Group             – cascades from Device Type
  4. Test Condition           – pristine / post_sc
  5. SC Condition Label       – optional multi-select
  6. Measurement Category     – optional multi-select
  7. SC Degraded              – boolean filter

Usage:
    source /tmp/aps_venv/bin/activate
    python3 create_sc_dashboard.py
"""

import json
import sys

from superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)
from db_config import SUPERSET_URL

DASHBOARD_TITLE = "Short Circuit"
DASHBOARD_SLUG = "short-circuit"


# ── Dashboard Layout ─────────────────────────────────────────────────────────

def build_dashboard_layout(tab_defs):
    """Build position_json from a list of (tab_name, tab_id, chart_tuples).

    chart_tuples: list of (chart_id, chart_uuid, chart_name, width, height).
    """
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": [], "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER", "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
    }

    tabs_id = "TABS-sc"
    tab_children = [td[1] for td in tab_defs]
    layout["GRID_ID"]["children"] = [tabs_id]
    layout[tabs_id] = {
        "type": "TABS", "id": tabs_id,
        "children": tab_children,
        "parents": ["ROOT_ID", "GRID_ID"],
    }

    for tab_name, tab_id, chart_list in tab_defs:
        tab_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_id]
        row_ids = []
        for i, (cid, cuuid, cname, width, height) in enumerate(chart_list):
            if cid is None:
                continue
            row_id = f"ROW-{tab_id}-{i}"
            chart_key = f"CHART-{tab_id}-{i}"
            layout[row_id] = {
                "type": "ROW", "id": row_id,
                "children": [chart_key],
                "parents": tab_parents,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            layout[chart_key] = {
                "type": "CHART", "id": chart_key, "children": [],
                "parents": tab_parents + [row_id],
                "meta": {
                    "chartId": cid, "width": width, "height": height,
                    "sliceName": cname, "uuid": cuuid,
                },
            }
            row_ids.append(row_id)

        layout[tab_id] = {
            "type": "TAB", "id": tab_id,
            "children": row_ids,
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": tab_name},
        }

    return layout


# ── Native Filters ───────────────────────────────────────────────────────────

def build_native_filters(all_chart_ids, main_ds_id, waveform_ds_id=None,
                         degradation_ds_id=None):
    """Build 7 cascading native filters for the SC dashboard."""
    mfr_fid = "NATIVE_FILTER-sc-manufacturer"
    dev_fid = "NATIVE_FILTER-sc-device-type"
    sg_fid  = "NATIVE_FILTER-sc-sample-group"
    tc_fid  = "NATIVE_FILTER-sc-test-condition"
    scl_fid = "NATIVE_FILTER-sc-condition-label"
    cat_fid = "NATIVE_FILTER-sc-meas-category"
    deg_fid = "NATIVE_FILTER-sc-degraded"

    # Build targets for filters that span multiple datasets
    def multi_targets(col):
        targets = [{"datasetId": main_ds_id, "column": {"name": col}}]
        if waveform_ds_id:
            targets.append({"datasetId": waveform_ds_id,
                            "column": {"name": col}})
        if degradation_ds_id:
            targets.append({"datasetId": degradation_ds_id,
                            "column": {"name": col}})
        return targets

    filters = [
        # 1. Manufacturer
        {
            "id": mfr_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Manufacturer",
            "filterType": "filter_select",
            "targets": multi_targets("manufacturer"),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filter by device manufacturer",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
        # 2. Device Type (cascades from Manufacturer)
        {
            "id": dev_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Device Type",
            "filterType": "filter_select",
            "targets": multi_targets("device_type"),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [mfr_fid],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Select device type(s)",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
        # 3. Sample Group (cascades from Device Type)
        {
            "id": sg_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Sample Group",
            "filterType": "filter_select",
            "targets": multi_targets("sample_group"),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Select sample group(s) — groups pre/post files "
                           "for the same physical device",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
        # 4. Test Condition (pristine / post_sc)
        {
            "id": tc_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "name": "Test Condition",
            "filterType": "filter_select",
            "targets": multi_targets("test_condition"),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filter by test condition "
                           "(pristine = before SC, post_sc = after SC)",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
        # 5. SC Condition Label (optional multi-select)
        {
            "id": scl_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "SC Condition",
            "filterType": "filter_select",
            "targets": multi_targets("sc_condition_label"),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filter by SC stress condition "
                           "(e.g. 600V_3us_Vgs15_minus4V)",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
        # 6. Measurement Category
        {
            "id": cat_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "name": "Measurement Category",
            "filterType": "filter_select",
            "targets": [{"datasetId": main_ds_id,
                         "column": {"name": "measurement_category"}}]
                       + ([{"datasetId": degradation_ds_id,
                            "column": {"name": "measurement_category"}}]
                          if degradation_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filter by measurement type "
                           "(IdVg, IdVd, Blocking, etc.)",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
        # 7. SC Degraded (boolean)
        {
            "id": deg_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "name": "SC Degraded",
            "filterType": "filter_select",
            "targets": [{"datasetId": main_ds_id,
                         "column": {"name": "is_sc_degraded"}}],
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filter by degradation status "
                           "(true = SC-degraded device, false = normal)",
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        },
    ]
    return filters


# build_json_metadata() and create_or_update_dashboard() are imported from superset_api.


# ── Chart Helpers ────────────────────────────────────────────────────────────

def cat_filter(cat):
    """Adhoc WHERE filter for a measurement category."""
    return {
        "expressionType": "SQL",
        "sqlExpression": f"measurement_category = '{cat}'",
        "clause": "WHERE",
    }


def sc_curve_params(x_axis, cat, x_title, y_title,
                    metric_expr="AVG(i_drain)",
                    metric_label="I_Drain (A)",
                    log_y=False, series_limit=50,
                    extra_groupby=None, extra_filters=None,
                    bias_col=None):
    """Line-chart params for SC IV curves.

    Groups by the SC condition plus metadata_id / step_index so separate
    physical sweeps are not stitched into one line.  If bias_col is given,
    adds it as an additional groupby for multi-step sweeps.
    """
    groupby = [
        "device_type", "sample_group", "test_condition",
        "sc_condition_label", "metadata_id", "step_index",
    ]
    if bias_col:
        groupby.append({
            "expressionType": "SQL",
            "sqlExpression": f"ROUND({bias_col}::numeric)",
            "label": bias_col.replace("_bin", "").replace("v_", "V_") + " (V)",
        })
    if extra_groupby:
        groupby.extend(extra_groupby)

    adhoc_filters = [cat_filter(cat)]
    if extra_filters:
        adhoc_filters.extend(extra_filters)

    params = {
        "x_axis": x_axis,
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": metric_expr,
            "label": metric_label,
        }],
        "groupby": groupby,
        "adhoc_filters": adhoc_filters,
        "row_limit": 100000,
        "truncate_metric": True,
        "show_legend": True,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": x_title,
        "y_axis_title": y_title,
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "tooltipTimeFormat": "smart_date",
        "markerEnabled": False,
        "connectNulls": True,
        "zoomable": True,
        "sort_series_type": "max",
        "sort_series_ascending": False,
    }
    if log_y:
        params["logAxis"] = "y"
    if series_limit:
        params["series_limit"] = series_limit
        params["series_limit_metric"] = {
            "expressionType": "SQL",
            "sqlExpression": f"COUNT(DISTINCT {x_axis})",
            "label": "_rank_by_sweep_range",
        }
    return params


def waveform_params(y_col, y_label, y_title):
    """Line-chart params for SC waveform time-domain plots."""
    return {
        "x_axis": "time_us",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": f"AVG({y_col})",
            "label": y_label,
        }],
        "groupby": [
            "device_type", "sample_group", "metadata_id",
            "sc_condition_label",
        ],
        "adhoc_filters": [],
        "row_limit": 100000,
        "truncate_metric": True,
        "show_legend": True,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": "Time (\u00b5s)",
        "y_axis_title": y_title,
        "y_axis_format": "SMART_NUMBER",
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "tooltipTimeFormat": "smart_date",
        "markerEnabled": False,
        "connectNulls": True,
        "zoomable": True,
        "sort_series_type": "max",
        "sort_series_ascending": False,
        "series_limit": 30,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print(f"Creating {DASHBOARD_TITLE} Dashboard")
    print("=" * 70)

    # 1. Authenticate
    print("\n1. Authenticating...")
    session = get_session()
    print("   OK")

    # 2. Find database
    print("\n2. Finding database...")
    db_id = find_database(session)
    if not db_id:
        print("  Please add the mosfets database connection first.")
        sys.exit(1)

    # 3. Create datasets
    print("\n3. Creating datasets...")
    main_ds = find_or_create_dataset(session, db_id, "sc_ruggedness_view")
    waveform_ds = find_or_create_dataset(session, db_id, "sc_waveform_view")
    degrad_ds = find_or_create_dataset(session, db_id,
                                        "sc_degradation_summary")
    if not main_ds:
        print("  FATAL: Could not create sc_ruggedness_view dataset.")
        print("  Run ingestion_sc.py first to create the views.")
        sys.exit(1)

    for ds_id in [main_ds, waveform_ds, degrad_ds]:
        if ds_id:
            refresh_dataset_columns(session, ds_id)

    # 4. Create charts
    print("\n4. Creating charts...")

    # ── Tab 1: Pre/Post SC Comparison ────────────────────────────────────
    print("\n   Tab 1: Pre/Post SC Comparison...")

    tab1_chart_defs = [
        # 0 – Data Summary table
        (
            "SC – Data Summary",
            main_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["device_type", "manufacturer",
                            "test_condition", "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "COUNT(DISTINCT sample_group)",
                     "label": "Samples"},
                    {"expressionType": "SQL",
                     "sqlExpression": "COUNT(DISTINCT metadata_id)",
                     "label": "Files"},
                    {"expressionType": "SQL",
                     "sqlExpression": "COUNT(*)",
                     "label": "Data Points"},
                ],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 10000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),

        # 1 – IdVg Transfer Curves (pristine vs post-SC overlay)
        (
            "SC – IdVg Transfer Curves",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_gate_plot_bin",
                cat="IdVg",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
                bias_col="v_drain_plot_bin",
            ),
            12, 60,
        ),

        # 2 – IdVd Output Curves
        (
            "SC – IdVd Output Curves",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_drain_plot_bin",
                cat="IdVd",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                bias_col="v_gate_plot_bin",
            ),
            12, 60,
        ),

        # 3 – Blocking Characteristics
        (
            "SC – Blocking Characteristics",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_drain_plot_bin",
                cat="Blocking",
                x_title="V_Drain (V)",
                y_title="|I_Drain| (A)",
                metric_expr="AVG(ABS(i_drain))",
                metric_label="|I_Drain| (A)",
                log_y=True,
            ),
            12, 60,
        ),

        # 4 – 3rd Quadrant
        (
            "SC – 3rd Quadrant (Body Diode)",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_drain_plot_bin",
                cat="3rd_Quadrant",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                bias_col="v_gate_plot_bin",
            ),
            12, 60,
        ),

        # 5 – Gate Leakage (Igss) — log Y
        (
            "SC – Gate Leakage (Igss)",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_gate_plot_bin",
                cat="Igss",
                x_title="V_Gate (V)",
                y_title="|I_Gate| (A)",
                metric_expr="AVG(ABS(i_gate))",
                metric_label="|I_Gate| (A)",
                log_y=True,
            ),
            12, 60,
        ),

        # 6 – Subthreshold Curves (log Y)
        (
            "SC – Subthreshold Curves",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_gate_plot_bin",
                cat="Subthreshold",
                x_title="V_Gate (V)",
                y_title="|I_Drain| (A)",
                metric_expr="AVG(ABS(i_drain))",
                metric_label="|I_Drain| (A)",
                log_y=True,
                bias_col="v_drain_plot_bin",
            ),
            12, 60,
        ),

        # 7 – Body Diode Curves
        (
            "SC – Body Diode Curves",
            main_ds,
            "echarts_timeseries_line",
            sc_curve_params(
                x_axis="v_drain_plot_bin",
                cat="Bodydiode",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                bias_col="v_gate_plot_bin",
            ),
            12, 60,
        ),
    ]

    # ── Tab 2: SC Waveform Viewer ────────────────────────────────────────
    print("   Tab 2: SC Waveform Viewer...")

    tab2_chart_defs = []
    if waveform_ds:
        tab2_chart_defs = [
            # 0 – Vds vs Time
            (
                "SC – Waveform: Vds vs Time",
                waveform_ds,
                "echarts_timeseries_line",
                waveform_params("vds", "Vds (V)", "V_DS (V)"),
                12, 60,
            ),
            # 1 – Id vs Time
            (
                "SC – Waveform: Id vs Time",
                waveform_ds,
                "echarts_timeseries_line",
                waveform_params("id_drain", "Id (A)", "I_D (A)"),
                12, 60,
            ),
            # 2 – Vgs vs Time
            (
                "SC – Waveform: Vgs vs Time",
                waveform_ds,
                "echarts_timeseries_line",
                waveform_params("vgs", "Vgs (V)", "V_GS (V)"),
                12, 60,
            ),
        ]

    # ── Tab 3: Individual Runs ───────────────────────────────────────────
    print("   Tab 3: Individual Runs...")

    tab3_chart_defs = [
        # 0 – Run Summary table
        (
            "SC – Run Summary",
            main_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["device_type", "device_id", "sample_group",
                            "experiment", "measurement_type",
                            "measurement_category", "test_condition",
                            "sc_condition_label", "is_sc_degraded"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "COUNT(*)",
                     "label": "Data Points"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MIN(v_gate)",
                     "label": "Vg Min"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(v_gate)",
                     "label": "Vg Max"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(ABS(i_drain))",
                     "label": "Max |Id|"},
                ],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 10000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),

        # 1 – Individual IdVg curves (per file)
        (
            "SC – IdVg (Individual Runs)",
            main_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_gate_plot_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(i_drain)",
                    "label": "I_Drain (A)",
                }],
                "groupby": ["device_id", "measurement_type", "metadata_id",
                            "step_index", "test_condition",
                            "sc_condition_label"],
                "adhoc_filters": [cat_filter("IdVg")],
                "row_limit": 100000,
                "truncate_metric": True,
                "show_legend": True,
                "legendType": "scroll",
                "rich_tooltip": True,
                "x_axis_title": "V_Gate (V)",
                "y_axis_title": "I_Drain (A)",
                "y_axis_format": "SMART_NUMBER",
                "truncateYAxis": False,
                "y_axis_bounds": [None, None],
                "tooltipTimeFormat": "smart_date",
                "markerEnabled": False,
                "connectNulls": True,
                "zoomable": True,
                "series_limit": 50,
                "series_limit_metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT v_gate_plot_bin)",
                    "label": "_rank_by_sweep_range",
                },
            },
            12, 60,
        ),

        # 2 – Individual IdVd curves
        (
            "SC – IdVd (Individual Runs)",
            main_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_drain_plot_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(i_drain)",
                    "label": "I_Drain (A)",
                }],
                "groupby": ["device_id", "measurement_type", "metadata_id",
                            "step_index", "test_condition",
                            "sc_condition_label"],
                "adhoc_filters": [cat_filter("IdVd")],
                "row_limit": 100000,
                "truncate_metric": True,
                "show_legend": True,
                "legendType": "scroll",
                "rich_tooltip": True,
                "x_axis_title": "V_Drain (V)",
                "y_axis_title": "I_Drain (A)",
                "y_axis_format": "SMART_NUMBER",
                "truncateYAxis": False,
                "y_axis_bounds": [None, None],
                "tooltipTimeFormat": "smart_date",
                "markerEnabled": False,
                "connectNulls": True,
                "zoomable": True,
                "series_limit": 50,
                "series_limit_metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT v_drain_plot_bin)",
                    "label": "_rank_by_sweep_range",
                },
            },
            12, 60,
        ),

        # 3 – Individual Blocking curves
        (
            "SC – Blocking (Individual Runs)",
            main_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_drain_plot_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(ABS(i_drain))",
                    "label": "|I_Drain| (A)",
                }],
                "groupby": ["device_id", "measurement_type", "metadata_id",
                            "step_index", "test_condition",
                            "sc_condition_label"],
                "adhoc_filters": [cat_filter("Blocking")],
                "row_limit": 100000,
                "truncate_metric": True,
                "show_legend": True,
                "legendType": "scroll",
                "rich_tooltip": True,
                "x_axis_title": "V_Drain (V)",
                "y_axis_title": "|I_Drain| (A)",
                "y_axis_format": "SMART_NUMBER",
                "truncateYAxis": False,
                "y_axis_bounds": [None, None],
                "tooltipTimeFormat": "smart_date",
                "markerEnabled": False,
                "connectNulls": True,
                "zoomable": True,
                "logAxis": "y",
                "series_limit": 50,
                "series_limit_metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT v_drain_plot_bin)",
                    "label": "_rank_by_sweep_range",
                },
            },
            12, 60,
        ),
    ]

    # ── Tab 4: Degradation Tracking ──────────────────────────────────────
    print("   Tab 4: Degradation Tracking...")

    tab4_chart_defs = []
    if degrad_ds:
        tab4_chart_defs = [
            # 0 – Vth Shift vs SC Duration scatter
            #     Uses degradation_summary: compare avg_abs_i_drain at a
            #     gate voltage near Vth between pristine and post_sc
            (
                "SC – Avg |Id| by SC Condition (IdVg)",
                degrad_ds,
                "echarts_timeseries_line",
                {
                    "x_axis": "v_gate_plot_bin",
                    "time_grain_sqla": None,
                    "x_axis_sort_asc": True,
                    "metrics": [{
                        "expressionType": "SQL",
                        "sqlExpression": (
                            "SUM(avg_abs_i_drain * n_points) "
                            "/ NULLIF(SUM(n_points), 0)"
                        ),
                        "label": "Avg |I_Drain| (A)",
                    }],
                    "groupby": ["device_type", "sample_group",
                                "test_condition", "sc_condition_label"],
                    "adhoc_filters": [
                        {
                            "expressionType": "SQL",
                            "sqlExpression":
                                "measurement_category IN ('IdVg', 'Vth', "
                                "'Subthreshold')",
                            "clause": "WHERE",
                        },
                    ],
                    "row_limit": 50000,
                    "truncate_metric": True,
                    "show_legend": True,
                    "legendType": "scroll",
                    "rich_tooltip": True,
                    "x_axis_title": "V_Gate (V)",
                    "y_axis_title": "Avg |I_Drain| (A)",
                    "y_axis_format": "SMART_NUMBER",
                    "truncateYAxis": False,
                    "y_axis_bounds": [None, None],
                    "tooltipTimeFormat": "smart_date",
                    "markerEnabled": False,
                    "connectNulls": True,
                    "zoomable": True,
                    "logAxis": "y",
                    "series_limit": 50,
                },
                12, 60,
            ),

            # 1 – IdVd degradation: compare output curves pre vs post
            (
                "SC – Avg Id by SC Condition (IdVd)",
                degrad_ds,
                "echarts_timeseries_line",
                {
                    "x_axis": "v_drain_plot_bin",
                    "time_grain_sqla": None,
                    "x_axis_sort_asc": True,
                    "metrics": [{
                        "expressionType": "SQL",
                        "sqlExpression": (
                            "SUM(avg_i_drain * n_points) "
                            "/ NULLIF(SUM(n_points), 0)"
                        ),
                        "label": "Avg I_Drain (A)",
                    }],
                    "groupby": ["device_type", "sample_group",
                                "test_condition", "sc_condition_label"],
                    "adhoc_filters": [
                        {
                            "expressionType": "SQL",
                            "sqlExpression":
                                "measurement_category = 'IdVd'",
                            "clause": "WHERE",
                        },
                    ],
                    "row_limit": 50000,
                    "truncate_metric": True,
                    "show_legend": True,
                    "legendType": "scroll",
                    "rich_tooltip": True,
                    "x_axis_title": "V_Drain (V)",
                    "y_axis_title": "Avg I_Drain (A)",
                    "y_axis_format": "SMART_NUMBER",
                    "truncateYAxis": False,
                    "y_axis_bounds": [None, None],
                    "tooltipTimeFormat": "smart_date",
                    "markerEnabled": False,
                    "connectNulls": True,
                    "zoomable": True,
                    "series_limit": 50,
                },
                12, 60,
            ),

            # 2 – Degradation flag summary table
            (
                "SC – Degradation Summary Table",
                degrad_ds,
                "table",
                {
                    "query_mode": "aggregate",
                    "groupby": ["device_type", "sample_group",
                                "test_condition", "sc_condition_label",
                                "measurement_category"],
                    "metrics": [
                        {"expressionType": "SQL",
                         "sqlExpression": "SUM(n_points)",
                         "label": "Total Points"},
                        {"expressionType": "SQL",
                         "sqlExpression": "AVG(avg_abs_i_drain)",
                         "label": "Avg |Id| (A)"},
                        {"expressionType": "SQL",
                         "sqlExpression": "AVG(avg_i_gate)",
                         "label": "Avg Ig (A)"},
                    ],
                    "all_columns": [],
                    "order_by_cols": [],
                    "row_limit": 10000,
                    "include_time": False,
                    "table_timestamp_format": "smart_date",
                },
                12, 50,
            ),
        ]

    # ── Create all charts and build tabs ─────────────────────────────────
    print("\n   Creating all charts...")

    all_chart_ids = []

    def create_tab_charts(chart_defs):
        """Create charts for a tab, return list of (id, uuid, name, w, h)."""
        info = []
        for name, ds_id, viz_type, params, width, height in chart_defs:
            cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
            info.append((cid, cuuid, name, width, height))
            if cid:
                all_chart_ids.append(cid)
        return info

    tab1_info = create_tab_charts(tab1_chart_defs)
    tab2_info = create_tab_charts(tab2_chart_defs)
    tab3_info = create_tab_charts(tab3_chart_defs)
    tab4_info = create_tab_charts(tab4_chart_defs)

    # 5. Build dashboard
    print("\n5. Building dashboard layout...")

    tab_defs = [
        ("Pre/Post SC Comparison", "TAB-prepost", tab1_info),
        ("SC Waveform Viewer", "TAB-waveform", tab2_info),
        ("Individual Runs", "TAB-individual", tab3_info),
        ("Degradation Tracking", "TAB-degradation", tab4_info),
    ]
    # Skip empty tabs
    tab_defs = [td for td in tab_defs if td[2]]

    position_json = build_dashboard_layout(tab_defs)

    native_filters = build_native_filters(
        all_chart_ids, main_ds,
        waveform_ds_id=waveform_ds,
        degradation_ds_id=degrad_ds,
    )
    json_metadata = build_json_metadata(all_chart_ids, native_filters)

    print("\n6. Creating dashboard...")
    dash_id = create_or_update_dashboard(
        session, DASHBOARD_TITLE, position_json, json_metadata,
        slug=DASHBOARD_SLUG,
    )

    # 7. Associate charts with dashboard
    print("\n7. Associating charts with dashboard...")
    if dash_id:
        for cid in all_chart_ids:
            resp = session.put(
                f"{SUPERSET_URL}/api/v1/chart/{cid}",
                json={"dashboards": [dash_id]},
            )
            status = "OK" if resp.ok else f"FAIL ({resp.status_code})"
            print(f"  Chart {cid} -> dashboard {dash_id}: {status}")

    print("\n" + "=" * 70)
    if dash_id:
        print("Dashboard ready!")
        print(f"  URL: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")
        print(f"  Charts: {len(all_chart_ids)}")
        tab_summary = ", ".join(
            f"{td[0]} ({len([c for c in td[2] if c[0]])})"
            for td in tab_defs
        )
        print(f"  Tabs: {tab_summary}")
        print("  Filters:")
        print("    1. Manufacturer          (optional)")
        print("    2. Device Type           (cascades from Manufacturer)")
        print("    3. Sample Group          (cascades from Device Type)")
        print("    4. Test Condition        (pristine / post_sc)")
        print("    5. SC Condition          (e.g. 600V_3us_Vgs15_minus4V)")
        print("    6. Measurement Category  (IdVg, IdVd, Blocking, etc.)")
        print("    7. SC Degraded           (boolean flag)")
    else:
        print("Dashboard creation failed — see errors above.")
    print("=" * 70)


if __name__ == "__main__":
    main()
