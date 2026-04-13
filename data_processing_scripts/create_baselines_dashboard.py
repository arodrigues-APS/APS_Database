#!/usr/bin/env python3
"""
Create the "Baselines" dashboard in Apache Superset via its REST API.

Dashboard design
================
Filters (cascading, top-down):
  1. Experiment  – required, multi-select  (8 options)
  2. Device      – required, multi-select  (cascades from Experiment)
                   Excluded from the overview table so researchers can
                   see available devices before picking.
  3. Measurement Category – required  (cascades from Experiment)
                   7 clean groups: IdVg, IdVd, 3rd_Quadrant, Blocking,
                   Igss, Vth, Other.  Charts stay empty until a
                   category is chosen (prevents 502 from loading
                   all data at once).  Overview table is excluded
                   so it always shows available data.
  4. V_Gate Range – numerical range slider on v_gate column.
                   Scoped to IdVg Transfer and Subthreshold charts.
  5. V_Drain Range – numerical range slider on v_drain column.
                   Scoped to IdVd, 3rd Quadrant, Blocking, and Igss charts.

  Y-axis auto-scales to the filtered data range in all line charts.

Charts:
  1. Available Data      – pivot table: device × measurement_category instance
                          counts (curves, not files — multi-step sweeps count
                          as multiple instances; not filtered by Device)
  2. IdVg Transfer Curves
  3. IdVd Output Curves  (grouped by step_index for multi-step)
  4. 3rd Quadrant Curves
  5. Blocking / BVDSS Curves  (log-scale Y for leakage)
  6. Igss Gate Leakage        (log-scale Y, x-axis = V_Gate)
  7. IdVg Subthreshold        (log |I_Drain| vs V_Gate)
  8. Vth Curves               (I_Drain vs V_Gate for Vth sweeps)
  9. TSP Parameters      – raw table of instrument settings

Workflow:
  Open dashboard → empty (no experiment selected)
  Pick experiment → overview populates, curves still empty
  Pick devices → curves still empty (need category)
  Pick measurement category → relevant curves populate
  Use V_Gate/V_Drain sliders to zoom into ranges

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 create_baselines_dashboard.py
"""

import json
import sys

from superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)
from db_config import SUPERSET_URL


# ── Dashboard Layout ─────────────────────────────────────────────────────────

def build_dashboard_layout(charts):
    """Build position_json from (chart_id, uuid, name, width, height) tuples."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": [], "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER", "id": "HEADER_ID",
            "meta": {"text": "Baselines"},
        },
    }
    row_children = []
    for i, (cid, cuuid, cname, width, height) in enumerate(charts):
        if cid is None:
            continue
        row_id = f"ROW-bl-{i}"
        chart_key = f"CHART-bl-{i}"
        layout[row_id] = {
            "type": "ROW", "id": row_id,
            "children": [chart_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        layout[chart_key] = {
            "type": "CHART", "id": chart_key, "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": cid, "width": width, "height": height,
                "sliceName": cname, "uuid": cuuid,
            },
        }
        row_children.append(row_id)
    layout["GRID_ID"]["children"] = row_children
    return layout


# ── Native Filters ───────────────────────────────────────────────────────────

def build_native_filters(chart_ids, overview_chart_id, meta_ds_id,
                         view_ds_id=None,
                         vgate_chart_ids=None, vdrain_chart_ids=None):
    """
    Five native filters:

    1. Experiment   – required (enableEmptyFilter=True), scopes ALL charts.
    2. Device       – required, cascades from Experiment.
                      Scopes all charts EXCEPT the overview table so
                      researchers can see available devices before picking.
    3. Measurement Category – required (enableEmptyFilter=True),
                      cascades from Experiment, scopes all charts
                      EXCEPT the overview table.
    4. V_Gate Range – numerical range slider, scoped to V_Gate line charts.
    5. V_Drain Range – numerical range slider, scoped to V_Drain line charts.
    """
    exp_fid = "NATIVE_FILTER-experiment"
    dev_fid = "NATIVE_FILTER-device"
    cat_fid = "NATIVE_FILTER-category"
    vgate_fid = "NATIVE_FILTER-vgate-range"
    vdrain_fid = "NATIVE_FILTER-vdrain-range"

    vgate_chart_ids = vgate_chart_ids or []
    vdrain_chart_ids = vdrain_chart_ids or []

    # Charts that the device filter applies to (exclude overview)
    device_scoped = [c for c in chart_ids if c != overview_chart_id]

    filters = [
        {
            "id": exp_fid,
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Experiment",
            "filterType": "filter_select",
            "targets": [{
                "datasetId": meta_ds_id,
                "column": {"name": "experiment"},
            }],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {
                    "value": None,
                },
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Select one or more experiments",
            "chartsInScope": list(chart_ids),
            "tabsInScope": [],
        },
        {
            "id": dev_fid,
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Device",
            "filterType": "filter_select",
            "targets": [{
                "datasetId": meta_ds_id,
                "column": {"name": "device_id"},
            }],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {
                    "value": None,
                },
            },
            "cascadeParentIds": [exp_fid],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": [overview_chart_id] if overview_chart_id else [],
            },
            "type": "NATIVE_FILTER",
            "description": "Select devices to compare (shows after picking experiment)",
            "chartsInScope": device_scoped,
            "tabsInScope": [],
        },
        {
            "id": cat_fid,
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "name": "Measurement Category",
            "filterType": "filter_select",
            "targets": [{
                "datasetId": meta_ds_id,
                "column": {"name": "measurement_category"},
            }],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {
                    "value": None,
                },
            },
            "cascadeParentIds": [exp_fid],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": [overview_chart_id] if overview_chart_id else [],
            },
            "type": "NATIVE_FILTER",
            "description": "Pick a measurement category to load curves (required to avoid overloading)",
            "chartsInScope": [c for c in chart_ids if c != overview_chart_id],
            "tabsInScope": [],
        },
    ]

    # ── Range filters (numerical sliders) ────────────────────────────
    # Cascade from both Experiment AND Device so the slider bounds
    # dynamically narrow to the data that matches the current selection.
    if view_ds_id and vgate_chart_ids:
        # Exclude every chart NOT in vgate_chart_ids
        vgate_excluded = [c for c in chart_ids if c not in vgate_chart_ids]
        filters.append({
            "id": vgate_fid,
            "controlValues": {
                "enableEmptyFilter": False,
            },
            "name": "V_Gate Range",
            "filterType": "filter_range",
            "targets": [{
                "datasetId": view_ds_id,
                "column": {"name": "v_gate_bin"},
            }],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {"value": None},
            },
            "cascadeParentIds": [exp_fid, dev_fid],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": vgate_excluded,
            },
            "type": "NATIVE_FILTER",
            "description": "Adjust V_Gate range – slider bounds update with Experiment & Device selection",
            "chartsInScope": list(vgate_chart_ids),
            "tabsInScope": [],
        })

    if view_ds_id and vdrain_chart_ids:
        vdrain_excluded = [c for c in chart_ids if c not in vdrain_chart_ids]
        filters.append({
            "id": vdrain_fid,
            "controlValues": {
                "enableEmptyFilter": False,
            },
            "name": "V_Drain Range",
            "filterType": "filter_range",
            "targets": [{
                "datasetId": view_ds_id,
                "column": {"name": "v_drain_bin"},
            }],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {"value": None},
            },
            "cascadeParentIds": [exp_fid, dev_fid],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": vdrain_excluded,
            },
            "type": "NATIVE_FILTER",
            "description": "Adjust V_Drain range – slider bounds update with Experiment & Device selection",
            "chartsInScope": list(vdrain_chart_ids),
            "tabsInScope": [],
        })

    return filters


# build_json_metadata() and create_or_update_dashboard() are imported from superset_api.


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Creating Baselines Dashboard in Apache Superset")
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
    view_ds = find_or_create_dataset(session, db_id, "baselines_view")
    meta_ds = find_or_create_dataset(session, db_id, "baselines_metadata")
    meas_ds = find_or_create_dataset(session, db_id, "baselines_measurements")
    if not view_ds:
        print("  FATAL: Could not create baselines_view dataset")
        sys.exit(1)
    for ds_id in [view_ds, meta_ds, meas_ds]:
        if ds_id:
            refresh_dataset_columns(session, ds_id)

    # 4. Create charts
    print("\n4. Creating charts...")

    chart_defs = [
        # 0 – Overview table (NOT filtered by Device → shows what exists)
        #     "Instances" = number of distinct curves (measurement_type × step_index)
        #     so multi-step files count as multiple instances.
        (
            "Baselines – Available Data",
            view_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["experiment", "device_id", "measurement_category"],
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": (
                        "COUNT(DISTINCT measurement_type "
                        "|| '_' || COALESCE(step_index::TEXT, '0'))"
                    ),
                    "label": "Instances",
                }],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 10000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),
        # 1 – IdVg transfer curves
        (
            "Baselines – IdVg Transfer Curves",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_gate_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(i_drain)",
                    "label": "I_Drain (A)",
                }],
                "groupby": ["device_id", "measurement_type"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = 'IdVg'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
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
            },
            12, 60,
        ),
        # 2 – IdVd output curves
        (
            "Baselines – IdVd Output Curves",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_drain_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(i_drain)",
                    "label": "I_Drain (A)",
                }],
                "groupby": ["device_id", "measurement_type", "step_index"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = 'IdVd'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
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
            },
            12, 60,
        ),
        # 3 – 3rd Quadrant
        (
            "Baselines – 3rd Quadrant Curves",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_drain_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(i_drain)",
                    "label": "I_Drain (A)",
                }],
                "groupby": ["device_id", "measurement_type"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = '3rd_Quadrant'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
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
            },
            12, 60,
        ),
        # 4 – Blocking / BVDSS (log scale for leakage currents)
        (
            "Baselines – Blocking Curves",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_drain_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(ABS(i_drain))",
                    "label": "|I_Drain| (A)",
                }],
                "groupby": ["device_id", "measurement_type"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = 'Blocking'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
                "rich_tooltip": True,
                "x_axis_title": "V_Drain (V)",
                "y_axis_title": "|I_Drain| (A)",
                "logAxis": "y",
                "y_axis_format": "SMART_NUMBER",
                "truncateYAxis": False,
                "y_axis_bounds": [None, None],
                "tooltipTimeFormat": "smart_date",
                "markerEnabled": False,
                "connectNulls": True,
                "zoomable": True,
            },
            12, 60,
        ),
        # 5 – Igss gate leakage (x-axis = v_gate; Igss has no v_drain)
        (
            "Baselines – Igss Gate Leakage",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_gate_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(ABS(i_gate))",
                    "label": "|I_Gate| (A)",
                }],
                "groupby": ["device_id", "measurement_type"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = 'Igss'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
                "rich_tooltip": True,
                "x_axis_title": "V_Gate (V)",
                "y_axis_title": "|I_Gate| (A)",
                "logAxis": "y",
                "y_axis_format": "SMART_NUMBER",
                "truncateYAxis": False,
                "y_axis_bounds": [None, None],
                "tooltipTimeFormat": "smart_date",
                "markerEnabled": False,
                "connectNulls": True,
                "zoomable": True,
            },
            12, 60,
        ),
        # 6 – IdVg subthreshold (log scale)
        (
            "Baselines – IdVg Subthreshold",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_gate_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(ABS(i_drain))",
                    "label": "|I_Drain| (A)",
                }],
                "groupby": ["device_id", "measurement_type"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = 'IdVg'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
                "rich_tooltip": True,
                "x_axis_title": "V_Gate (V)",
                "y_axis_title": "|I_Drain| (A)  [log]",
                "logAxis": "y",
                "y_axis_format": "SMART_NUMBER",
                "truncateYAxis": False,
                "y_axis_bounds": [None, None],
                "tooltipTimeFormat": "smart_date",
                "markerEnabled": False,
                "connectNulls": True,
                "zoomable": True,
            },
            12, 60,
        ),
        # 7 – Vth threshold voltage curves
        (
            "Baselines – Vth Curves",
            view_ds,
            "echarts_timeseries_line",
            {
                "x_axis": "v_gate_bin",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(i_drain)",
                    "label": "I_Drain (A)",
                }],
                "groupby": ["device_id", "measurement_type"],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression":
                        "measurement_category = 'Vth'",
                    "clause": "WHERE",
                }],
                "row_limit": 50000,
                "truncate_metric": True,
                "show_legend": True,
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
            },
            12, 60,
        ),
        # 8 – TSP parameters table
        (
            "Baselines – TSP Parameters",
            meta_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "experiment", "device_id",
                    "measurement_type", "measurement_category",
                    "sweep_start", "sweep_stop", "sweep_points",
                    "bias_value", "compliance_ch1", "compliance_ch2",
                    "meas_time", "hold_time", "plc", "step_num",
                ],
                "adhoc_filters": [{
                    "expressionType": "SQL",
                    "sqlExpression": "tsp_path IS NOT NULL",
                    "clause": "WHERE",
                }],
                "row_limit": 5000,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),
    ]

    charts_info = []   # (id, uuid, name, width, height)
    chart_ids_only = []

    for name, ds_id, viz_type, params, width, height in chart_defs:
        cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
        charts_info.append((cid, cuuid, name, width, height))
        if cid:
            chart_ids_only.append(cid)

    overview_chart_id = charts_info[0][0]  # first chart is the overview

    # Charts using v_gate as x-axis: IdVg(1), Igss(5), Subthreshold(6), Vth(7)
    vgate_chart_ids = [charts_info[i][0] for i in (1, 5, 6, 7)
                       if charts_info[i][0] is not None]
    # Charts using v_drain as x-axis: IdVd (2), 3rd Quad (3), Blocking (4)
    vdrain_chart_ids = [charts_info[i][0] for i in (2, 3, 4)
                        if charts_info[i][0] is not None]

    # 5. Build dashboard with native filters
    print("\n5. Creating dashboard with native filters...")
    position_json = build_dashboard_layout(charts_info)
    native_filters = build_native_filters(
        chart_ids_only, overview_chart_id, meta_ds,
        view_ds_id=view_ds,
        vgate_chart_ids=vgate_chart_ids,
        vdrain_chart_ids=vdrain_chart_ids,
    )
    json_metadata = build_json_metadata(chart_ids_only, native_filters)
    dash_id = create_or_update_dashboard(
        session, "Baselines", position_json, json_metadata,
        slug="baselines",
    )

    # 6. Associate charts with dashboard
    print("\n6. Associating charts with dashboard...")
    if dash_id:
        for cid in chart_ids_only:
            resp = session.put(
                f"{SUPERSET_URL}/api/v1/chart/{cid}",
                json={"dashboards": [dash_id]},
            )
            status = "OK" if resp.ok else f"FAIL ({resp.status_code})"
            print(f"  Chart {cid} -> dashboard {dash_id}: {status}")

    print("\n" + "=" * 70)
    if dash_id:
        print("Dashboard ready!")
        print(f"  URL: {SUPERSET_URL}/superset/dashboard/baselines/")
        print(f"  Charts: {len(chart_ids_only)}")
        print("  Filters:")
        print("    1. Experiment     (required)")
        print("    2. Device         (required, cascades from Experiment)")
        print("    3. Meas. Category (required, cascades from Experiment)")
        print("    4. V_Gate Range   (slider, scoped to IdVg/Igss/Vth charts)")
        print("    5. V_Drain Range  (slider, scoped to IdVd/3rdQ/Blocking)")
        print("  Workflow:")
        print("    Pick experiment → overview shows available devices")
        print("    Pick device(s)  → still need category")
        print("    Pick category   → curves populate")
    else:
        print("Dashboard creation failed — see errors above.")
    print("=" * 70)


if __name__ == "__main__":
    main()
