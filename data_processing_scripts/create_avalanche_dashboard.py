#!/usr/bin/env python3
"""
Create the "Avalanche" dashboard in Apache Superset via its REST API.

This dashboard visualises avalanche ruggedness waveform data (UIS / UID / RT):
  - Overview: captures per family/mode/device with outcome breakdown
  - Waveform Viewer: Vds, Id, Vgs vs time (µs) per shot
  - Individual Shots: per-capture peak stats and energy scaling charts

Datasets (SQL views created / refreshed by ingestion_avalanche.py):
  1. avalanche_waveform_view   – point-level time-domain waveforms
  2. avalanche_summary_view    – one row per capture with peak stats

Tabs:
  1. Overview         – summary tables and capture-count bar charts
  2. Waveform Viewer  – Vds / Id / Vgs vs time (µs)
  3. Individual Shots – per-shot detail table and energy-scaling charts

Filters (cascading):
  1. Manufacturer       – optional multi-select
  2. Device Type        – cascades from Manufacturer
  3. Avalanche Family   – top-level folder group (UIS_2018_botnk, Selam, …)
  4. Mode               – UIS / UID / RT / Avalanche
  5. Outcome            – survived / failed / unknown
  6. Capture            – Waveform Viewer only, defaults to one capture

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 create_avalanche_dashboard.py
"""

import sys

from superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)
from db_config import SUPERSET_URL

DASHBOARD_TITLE = "Avalanche"
DASHBOARD_SLUG  = "avalanche"


# ── Dashboard Layout ─────────────────────────────────────────────────────────

def build_dashboard_layout(tab_defs):
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

    tabs_id = "TABS-avl"
    layout["GRID_ID"]["children"] = [tabs_id]
    layout[tabs_id] = {
        "type": "TABS", "id": tabs_id,
        "children": [td[1] for td in tab_defs],
        "parents": ["ROOT_ID", "GRID_ID"],
    }

    for tab_name, tab_id, chart_list in tab_defs:
        tab_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_id]
        row_ids = []
        for i, (cid, cuuid, cname, width, height) in enumerate(chart_list):
            if cid is None:
                continue
            row_id   = f"ROW-{tab_id}-{i}"
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

def build_native_filters(all_chart_ids, waveform_ds_id, summary_ds_id,
                         waveform_chart_ids=None):
    mfr_fid  = "NATIVE_FILTER-avl-manufacturer"
    dev_fid  = "NATIVE_FILTER-avl-device"
    fam_fid  = "NATIVE_FILTER-avl-family"
    mode_fid = "NATIVE_FILTER-avl-mode"
    out_fid  = "NATIVE_FILTER-avl-outcome"
    cap_fid  = "NATIVE_FILTER-avl-capture"

    def multi_targets(col):
        targets = []
        for ds_id in (waveform_ds_id, summary_ds_id):
            if ds_id:
                targets.append({"datasetId": ds_id, "column": {"name": col}})
        return targets

    def make_filter(fid, name, col, cascade_from=None, description="",
                    targets=None, chart_scope=None, tab_scope=None,
                    default_to_first_item=False, multi_select=True):
        if cascade_from is None:
            cascade_parent_ids = []
        elif isinstance(cascade_from, (list, tuple)):
            cascade_parent_ids = list(cascade_from)
        else:
            cascade_parent_ids = [cascade_from]
        return {
            "id": fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": default_to_first_item,
                "multiSelect": multi_select,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": name,
            "filterType": "filter_select",
            "targets": targets if targets is not None else multi_targets(col),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": cascade_parent_ids,
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": description,
            "chartsInScope": list(chart_scope or all_chart_ids),
            "tabsInScope": list(tab_scope or []),
        }

    filters = [
        make_filter(mfr_fid,  "Manufacturer",     "manufacturer_label",
                    description="Filter by device manufacturer"),
        make_filter(dev_fid,  "Device",            "device_label",
                    cascade_from=mfr_fid,
                    description="Filter by part number or capture device ID"),
        make_filter(fam_fid,  "Avalanche Family",  "avalanche_family",
                    description="Top-level capture folder group"),
        make_filter(mode_fid, "Mode",              "avalanche_mode",
                    description="UIS / UID / RT / Avalanche"),
        make_filter(out_fid,  "Outcome",           "avalanche_outcome",
                    description="survived / failed / unknown"),
    ]

    if waveform_ds_id:
        waveform_scope = waveform_chart_ids or all_chart_ids
        capture_targets = []
        if summary_ds_id:
            capture_targets.append({"datasetId": summary_ds_id,
                                    "column": {"name": "capture_label"}})
        capture_targets.append({"datasetId": waveform_ds_id,
                                "column": {"name": "capture_label"}})
        filters.append(
            make_filter(
                cap_fid, "Capture", "capture_label",
                cascade_from=[dev_fid, fam_fid, mode_fid, out_fid],
                description="Single capture for waveform plots",
                targets=capture_targets,
                chart_scope=waveform_scope,
                tab_scope=["TAB-waveform"],
                default_to_first_item=True,
                multi_select=False,
            )
        )

    return filters


# ── Chart Helpers ────────────────────────────────────────────────────────────

def waveform_params(y_col, y_label, y_title):
    """Line-chart params for oscilloscope time-domain waveforms (µs axis)."""
    return {
        "x_axis": "time_us",
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": f"AVG({y_col})",
            "label": y_label,
        }],
        "groupby": ["capture_label"],
        "adhoc_filters": [
            {
                "expressionType": "SQL",
                "sqlExpression": f"{y_col} IS NOT NULL",
                "clause": "WHERE",
            },
        ],
        "row_limit": 10000,
        "truncate_metric": True,
        "show_legend": True,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": "Time (µs)",
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
        "series_limit": 10,
    }


def energy_scaling_params(x_col, x_title, y_col, y_label, y_title):
    """Line-chart params for energy / inductance scaling charts."""
    return {
        "x_axis": x_col,
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": f"AVG({y_col})",
            "label": y_label,
        }],
        "groupby": ["device_label", "avalanche_outcome"],
        "adhoc_filters": [
            {
                "expressionType": "SQL",
                "sqlExpression": f"{x_col} IS NOT NULL AND {y_col} IS NOT NULL",
                "clause": "WHERE",
            },
        ],
        "row_limit": 50000,
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
        "markerEnabled": True,
        "connectNulls": True,
        "zoomable": True,
        "sort_series_type": "max",
        "sort_series_ascending": True,
        "series_limit": 100,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print(f"Creating {DASHBOARD_TITLE} Dashboard")
    print("=" * 70)

    print("\n1. Authenticating...")
    session = get_session()
    print("   OK")

    print("\n2. Finding database...")
    db_id = find_database(session)
    if not db_id:
        print("  Please add the mosfets database connection first.")
        sys.exit(1)

    print("\n3. Creating datasets...")
    waveform_ds = find_or_create_dataset(session, db_id,
                                          "avalanche_waveform_view")
    summary_ds  = find_or_create_dataset(session, db_id,
                                          "avalanche_summary_view")
    if not waveform_ds:
        print("  FATAL: Could not create avalanche_waveform_view dataset.")
        print("  Run ingestion_avalanche.py first to create the views.")
        sys.exit(1)

    for ds_id in (waveform_ds, summary_ds):
        if ds_id:
            refresh_dataset_columns(session, ds_id)

    print("\n4. Creating charts...")

    # ── Tab 1: Overview ──────────────────────────────────────────────────
    print("\n   Tab 1: Overview...")

    tab1_chart_defs = []
    if summary_ds:
        tab1_chart_defs = [
            # 0 – Capture Summary table
            (
                "Avl – Capture Summary",
                summary_ds,
                "table",
                {
                    "query_mode": "aggregate",
                    "groupby": ["avalanche_family", "avalanche_mode",
                                "device_label", "manufacturer_label",
                                "avalanche_outcome"],
                    "metrics": [
                        {"expressionType": "SQL",
                         "sqlExpression": "COUNT(*)",
                         "label": "Captures"},
                        {"expressionType": "SQL",
                         "sqlExpression": "AVG(avalanche_energy_j)",
                         "label": "Avg Energy (J)"},
                        {"expressionType": "SQL",
                         "sqlExpression": "MAX(peak_id)",
                         "label": "Max Peak Id (A)"},
                        {"expressionType": "SQL",
                         "sqlExpression": "MAX(max_vds)",
                         "label": "Max Vds (V)"},
                    ],
                    "all_columns": [],
                    "order_by_cols": [],
                    "row_limit": 10000,
                    "include_time": False,
                    "table_timestamp_format": "smart_date",
                },
                12, 50,
            ),

            # 1 – Captures per Family (stacked by mode)
            (
                "Avl – Captures per Family",
                summary_ds,
                "echarts_timeseries_bar",
                {
                    "x_axis": "avalanche_family",
                    "time_grain_sqla": None,
                    "x_axis_sort_asc": True,
                    "metrics": [{"expressionType": "SQL",
                                 "sqlExpression": "COUNT(*)",
                                 "label": "Captures"}],
                    "groupby": ["avalanche_mode"],
                    "adhoc_filters": [],
                    "row_limit": 1000,
                    "show_legend": True,
                    "rich_tooltip": True,
                    "x_axis_title": "Avalanche Family",
                    "y_axis_title": "Number of Captures",
                    "y_axis_format": "SMART_NUMBER",
                    "stack": True,
                },
                6, 50,
            ),

            # 2 – Outcome Distribution per Device
            (
                "Avl – Outcome per Device",
                summary_ds,
                "echarts_timeseries_bar",
                {
                    "x_axis": "device_label",
                    "time_grain_sqla": None,
                    "x_axis_sort_asc": True,
                    "metrics": [{"expressionType": "SQL",
                                 "sqlExpression": "COUNT(*)",
                                 "label": "Captures"}],
                    "groupby": ["avalanche_outcome"],
                    "adhoc_filters": [],
                    "row_limit": 1000,
                    "show_legend": True,
                    "rich_tooltip": True,
                    "x_axis_title": "Device Type",
                    "y_axis_title": "Number of Captures",
                    "y_axis_format": "SMART_NUMBER",
                    "stack": True,
                },
                6, 50,
            ),
        ]

    # ── Tab 2: Waveform Viewer ───────────────────────────────────────────
    print("   Tab 2: Waveform Viewer...")

    tab2_chart_defs = [
        (
            "Avl – Waveform: Vds vs Time",
            waveform_ds,
            "echarts_timeseries_line",
            waveform_params("vds", "Vds (V)", "V_DS (V)"),
            12, 60,
        ),
        (
            "Avl – Waveform: Id vs Time",
            waveform_ds,
            "echarts_timeseries_line",
            waveform_params("id_drain", "Id (A)", "I_D (A)"),
            12, 60,
        ),
        (
            "Avl – Waveform: Vgs vs Time",
            waveform_ds,
            "echarts_timeseries_line",
            waveform_params("vgs", "Vgs (V)", "V_GS (V)"),
            12, 60,
        ),
    ]

    # ── Tab 3: Individual Shots ──────────────────────────────────────────
    print("   Tab 3: Individual Shots...")

    tab3_chart_defs = []
    if summary_ds:
        tab3_chart_defs = [
            # 0 – Per-shot detail table
            (
                "Avl – Shot Details",
                summary_ds,
                "table",
                {
                    "query_mode": "aggregate",
                    "groupby": [
                        "device_id", "device_type", "manufacturer",
                        "device_label", "manufacturer_label",
                        "avalanche_family", "avalanche_mode",
                        "avalanche_energy_j", "avalanche_peak_current_a",
                        "avalanche_inductance_mh", "avalanche_gate_bias_v",
                        "avalanche_shot_index", "avalanche_condition_label",
                        "avalanche_temperature_c", "avalanche_outcome",
                        "avalanche_measured_at",
                    ],
                    "metrics": [
                        {"expressionType": "SQL",
                         "sqlExpression": "MAX(peak_id)",
                         "label": "Peak Id (A)"},
                        {"expressionType": "SQL",
                         "sqlExpression": "MAX(max_vds)",
                         "label": "Max Vds (V)"},
                        {"expressionType": "SQL",
                         "sqlExpression": "MAX(pulse_duration_s) * 1e6",
                         "label": "Pulse Duration (µs)"},
                        {"expressionType": "SQL",
                         "sqlExpression": "SUM(n_points)",
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

            # 1 – Peak Id vs Energy (canonical robustness plot)
            (
                "Avl – Peak Id vs Energy",
                summary_ds,
                "echarts_timeseries_scatter",
                energy_scaling_params(
                    x_col="avalanche_energy_j",
                    x_title="Energy (J)",
                    y_col="peak_id",
                    y_label="Avg Peak Id (A)",
                    y_title="Peak I_D (A)",
                ),
                12, 60,
            ),

            # 2 – Max Vds vs Inductance (overvoltage scaling)
            (
                "Avl – Max Vds vs Inductance",
                summary_ds,
                "echarts_timeseries_scatter",
                energy_scaling_params(
                    x_col="avalanche_inductance_mh",
                    x_title="Inductance (mH)",
                    y_col="max_vds",
                    y_label="Avg Max Vds (V)",
                    y_title="Max V_DS (V)",
                ),
                12, 60,
            ),
        ]

    # ── Create all charts and build tabs ─────────────────────────────────
    print("\n   Creating all charts...")

    all_chart_ids = []

    def create_tab_charts(chart_defs):
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
    waveform_chart_ids = [cid for cid, _, _, _, _ in tab2_info if cid]

    print("\n5. Building dashboard layout...")

    tab_defs = [
        ("Overview",        "TAB-overview",   tab1_info),
        ("Waveform Viewer", "TAB-waveform",   tab2_info),
        ("Individual Shots","TAB-shots",      tab3_info),
    ]
    tab_defs = [td for td in tab_defs if td[2]]

    position_json = build_dashboard_layout(tab_defs)
    native_filters = build_native_filters(
        all_chart_ids, waveform_ds, summary_ds,
        waveform_chart_ids=waveform_chart_ids,
    )
    json_metadata  = build_json_metadata(all_chart_ids, native_filters)

    print("\n6. Creating dashboard...")
    dash_id = create_or_update_dashboard(
        session, DASHBOARD_TITLE, position_json, json_metadata,
        slug=DASHBOARD_SLUG,
    )

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
        print("    1. Manufacturer        (optional)")
        print("    2. Device              (part number or capture ID)")
        print("    3. Avalanche Family    (UIS_2018_botnk, Selam, …)")
        print("    4. Mode                (UIS / UID / RT / Avalanche)")
        print("    5. Outcome             (survived / failed / unknown)")
        print("    6. Capture             (Waveform Viewer, defaults to one)")
    else:
        print("Dashboard creation failed — see errors above.")
    print("=" * 70)


if __name__ == "__main__":
    main()
