#!/usr/bin/env python3
"""
Create the "Irradiation" dashboard in Apache Superset via its REST API.

This dashboard visualises irradiation characterisation data:
  - Pre- vs post-irradiation IV curve overlays per device
  - Cross-campaign comparison across ion species and beam energies
  - Per-file individual runs with full metadata
  - Campaign overview summary tables and charts

Datasets (SQL views created by seed_irradiation_campaigns.py):
  1. irradiation_view               – all IV curves linked to campaigns
  2. irradiation_degradation_summary – pre-aggregated per voltage bin
  3. irradiation_campaign_overview   – summary counts per campaign

Tabs:
  1. Campaign Overview       – summary tables and bar charts
  2. Pre/Post Comparison     – overlay pre_irrad vs post_irrad IV curves
  3. Cross-Campaign          – compare degradation across ion species
  4. Individual Runs         – per-file curves with full metadata

Filters (8 cascading):
  1. Ion Species             – proton / Au / Ca / etc.
  2. Beam Energy (MeV)       – cascades from Ion Species
  3. Beam Type               – broad_beam / micro_beam
  4. Campaign                – cascades from Ion Species
  5. Manufacturer            – device manufacturer
  6. Device Type             – cascades from Manufacturer
  7. Test Condition          – pre_irrad / post_irrad
  8. Measurement Category    – IdVg, IdVd, Blocking, etc.

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 create_irradiation_dashboard.py
"""

import json
import sys

from superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)
from db_config import SUPERSET_URL

DASHBOARD_TITLE = "Irradiation"
DASHBOARD_SLUG = "irradiation"


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

    tabs_id = "TABS-irrad"
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

def build_native_filters(all_chart_ids, main_ds_id, degrad_ds_id=None,
                         overview_ds_id=None):
    """Build 8 cascading native filters for the Irradiation dashboard."""
    ion_fid  = "NATIVE_FILTER-irrad-ion-species"
    nrg_fid  = "NATIVE_FILTER-irrad-beam-energy"
    bt_fid   = "NATIVE_FILTER-irrad-beam-type"
    camp_fid = "NATIVE_FILTER-irrad-campaign"
    mfr_fid  = "NATIVE_FILTER-irrad-manufacturer"
    dev_fid  = "NATIVE_FILTER-irrad-device-type"
    tc_fid   = "NATIVE_FILTER-irrad-test-condition"
    cat_fid  = "NATIVE_FILTER-irrad-meas-category"

    def multi_targets(col):
        targets = [{"datasetId": main_ds_id, "column": {"name": col}}]
        if degrad_ds_id:
            targets.append({"datasetId": degrad_ds_id,
                            "column": {"name": col}})
        if overview_ds_id:
            targets.append({"datasetId": overview_ds_id,
                            "column": {"name": col}})
        return targets

    def make_filter(fid, name, col, cascade_from=None, description="",
                    targets=None):
        return {
            "id": fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": name,
            "filterType": "filter_select",
            "targets": targets if targets is not None else multi_targets(col),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [cascade_from] if cascade_from else [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": description,
            "chartsInScope": list(all_chart_ids),
            "tabsInScope": [],
        }

    filters = [
        # 1. Ion Species
        make_filter(ion_fid, "Ion Species", "ion_species",
                    description="Filter by radiation particle type"),

        # 2. Beam Energy (cascades from Ion Species)
        make_filter(nrg_fid, "Beam Energy (MeV)", "beam_energy_mev",
                    cascade_from=ion_fid,
                    description="Filter by beam energy in MeV"),

        # 3. Beam Type
        make_filter(bt_fid, "Beam Type", "beam_type",
                    description="broad_beam or micro_beam"),

        # 4. Campaign (cascades from Ion Species)
        make_filter(camp_fid, "Campaign", "campaign_name",
                    cascade_from=ion_fid,
                    description="Filter by irradiation campaign"),

        # 5. Manufacturer
        make_filter(mfr_fid, "Manufacturer", "manufacturer",
                    description="Filter by device manufacturer"),

        # 6. Device Type (cascades from Manufacturer)
        make_filter(dev_fid, "Device Type", "device_type",
                    cascade_from=mfr_fid,
                    description="Filter by commercial part number"),

        # 7. Test Condition (pre_irrad / post_irrad)
        make_filter(tc_fid, "Test Condition", "test_condition",
                    description="pre_irrad = baseline before irradiation, "
                                "post_irrad = after irradiation"),

        # 8. Measurement Category — excludes overview dataset (already
        #    grouped by category, so filtering would be confusing)
        make_filter(cat_fid, "Measurement Category", "measurement_category",
                    description="IdVg, IdVd, Blocking, Igss, etc.",
                    targets=(
                        [{"datasetId": main_ds_id,
                          "column": {"name": "measurement_category"}}]
                        + ([{"datasetId": degrad_ds_id,
                             "column": {"name": "measurement_category"}}]
                           if degrad_ds_id else [])
                    )),
    ]
    return filters


# ── Chart Helpers ────────────────────────────────────────────────────────────

def cat_filter(cat):
    """Adhoc WHERE filter for a measurement category."""
    return {
        "expressionType": "SQL",
        "sqlExpression": f"measurement_category = '{cat}'",
        "clause": "WHERE",
    }


def irrad_curve_params(x_axis, cat, x_title, y_title,
                       metric_expr="AVG(i_drain)",
                       metric_label="I_Drain (A)",
                       log_y=False, series_limit=50,
                       bias_col=None):
    """Line-chart params for irradiation IV curves.

    Groups by (device_type, test_condition, irrad_condition_label) so
    pre-irrad and post-irrad lines from different ion species are
    distinguishable.  If bias_col is given, adds it as an additional
    groupby for multi-step sweeps.
    """
    groupby = ["device_type", "test_condition", "irrad_condition_label"]
    if bias_col:
        groupby.append({
            "expressionType": "SQL",
            "sqlExpression": f"ROUND({bias_col}::numeric)",
            "label": bias_col.replace("_bin", "").replace("v_", "V_") + " (V)",
        })

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
        "adhoc_filters": [cat_filter(cat)],
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
    main_ds = find_or_create_dataset(session, db_id, "irradiation_view")
    degrad_ds = find_or_create_dataset(session, db_id,
                                        "irradiation_degradation_summary")
    overview_ds = find_or_create_dataset(session, db_id,
                                          "irradiation_campaign_overview")
    if not main_ds:
        print("  FATAL: Could not create irradiation_view dataset.")
        print("  Run seed_irradiation_campaigns.py first to create the views.")
        sys.exit(1)

    for ds_id in [main_ds, degrad_ds, overview_ds]:
        if ds_id:
            refresh_dataset_columns(session, ds_id)

    # 4. Create charts
    print("\n4. Creating charts...")

    # ── Tab 1: Campaign Overview ─────────────────────────────────────────
    print("\n   Tab 1: Campaign Overview...")

    tab1_chart_defs = [
        # 0 – Campaign Summary table
        (
            "Irrad – Campaign Summary",
            overview_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["campaign_name", "ion_species", "beam_energy_mev",
                            "beam_type", "facility", "device_type",
                            "manufacturer", "test_condition",
                            "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "SUM(n_devices)",
                     "label": "Devices"},
                    {"expressionType": "SQL",
                     "sqlExpression": "SUM(n_files)",
                     "label": "Files"},
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

        # 1 – Devices per Campaign (stacked bar by test condition)
        (
            "Irrad – Devices per Campaign",
            overview_ds,
            "echarts_timeseries_bar",
            {
                "x_axis": "campaign_name",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{"expressionType": "SQL",
                             "sqlExpression": "SUM(n_devices)",
                             "label": "Devices"}],
                "groupby": ["test_condition"],
                "adhoc_filters": [],
                "row_limit": 1000,
                "show_legend": True,
                "rich_tooltip": True,
                "x_axis_title": "Campaign",
                "y_axis_title": "Number of Devices",
                "y_axis_format": "SMART_NUMBER",
                "stack": True,
            },
            6, 50,
        ),

        # 2 – Data Points per Ion Species (stacked bar by measurement category)
        (
            "Irrad – Points per Ion Species",
            overview_ds,
            "echarts_timeseries_bar",
            {
                "x_axis": "ion_species",
                "time_grain_sqla": None,
                "x_axis_sort_asc": True,
                "metrics": [{"expressionType": "SQL",
                             "sqlExpression": "SUM(n_points)",
                             "label": "Data Points"}],
                "groupby": ["measurement_category"],
                "adhoc_filters": [],
                "row_limit": 1000,
                "show_legend": True,
                "rich_tooltip": True,
                "x_axis_title": "Ion Species",
                "y_axis_title": "Data Points",
                "y_axis_format": "SMART_NUMBER",
                "stack": True,
            },
            6, 50,
        ),
    ]

    # ── Tab 2: Pre/Post Irradiation Comparison ───────────────────────────
    print("   Tab 2: Pre/Post Irradiation Comparison...")

    tab2_chart_defs = [
        # 0 – Data Summary table
        (
            "Irrad – Data Summary",
            main_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["device_type", "manufacturer",
                            "test_condition", "campaign_name",
                            "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "COUNT(DISTINCT device_id)",
                     "label": "Devices"},
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

        # 1 – IdVg Transfer Curves (pre vs post overlay)
        (
            "Irrad – IdVg Transfer Curves",
            main_ds,
            "echarts_timeseries_line",
            irrad_curve_params(
                x_axis="v_gate_bin",
                cat="IdVg",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
                bias_col="v_drain_bin",
            ),
            12, 60,
        ),

        # 2 – IdVd Output Curves
        (
            "Irrad – IdVd Output Curves",
            main_ds,
            "echarts_timeseries_line",
            irrad_curve_params(
                x_axis="v_drain_bin",
                cat="IdVd",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                bias_col="v_gate_bin",
            ),
            12, 60,
        ),

        # 3 – Blocking Characteristics (log Y)
        (
            "Irrad – Blocking Characteristics",
            main_ds,
            "echarts_timeseries_line",
            irrad_curve_params(
                x_axis="v_drain_bin",
                cat="Blocking",
                x_title="V_Drain (V)",
                y_title="|I_Drain| (A)",
                metric_expr="AVG(ABS(i_drain))",
                metric_label="|I_Drain| (A)",
                log_y=True,
            ),
            12, 60,
        ),

        # 4 – Gate Leakage Igss (log Y)
        (
            "Irrad – Gate Leakage (Igss)",
            main_ds,
            "echarts_timeseries_line",
            irrad_curve_params(
                x_axis="v_gate_bin",
                cat="Igss",
                x_title="V_Gate (V)",
                y_title="|I_Gate| (A)",
                metric_expr="AVG(ABS(i_gate))",
                metric_label="|I_Gate| (A)",
                log_y=True,
            ),
            12, 60,
        ),

        # 5 – Subthreshold Curves (log Y)
        (
            "Irrad – Subthreshold Curves",
            main_ds,
            "echarts_timeseries_line",
            irrad_curve_params(
                x_axis="v_gate_bin",
                cat="Subthreshold",
                x_title="V_Gate (V)",
                y_title="|I_Drain| (A)",
                metric_expr="AVG(ABS(i_drain))",
                metric_label="|I_Drain| (A)",
                log_y=True,
                bias_col="v_drain_bin",
            ),
            12, 60,
        ),
    ]

    # ── Tab 3: Cross-Campaign Comparison ────────────────────────────────
    print("   Tab 3: Cross-Campaign Comparison...")

    tab3_chart_defs = []
    if degrad_ds:
        tab3_chart_defs = [
            # 0 – IdVg shift comparison by ion species (log Y)
            (
                "Irrad – IdVg Shift by Ion Species",
                degrad_ds,
                "echarts_timeseries_line",
                {
                    "x_axis": "v_gate_bin",
                    "time_grain_sqla": None,
                    "x_axis_sort_asc": True,
                    "metrics": [{
                        "expressionType": "SQL",
                        "sqlExpression": "AVG(avg_abs_i_drain)",
                        "label": "Avg |I_Drain| (A)",
                    }],
                    "groupby": ["device_type", "ion_species", "test_condition"],
                    "adhoc_filters": [{
                        "expressionType": "SQL",
                        "sqlExpression": (
                            "measurement_category IN "
                            "('IdVg', 'Vth', 'Subthreshold')"
                        ),
                        "clause": "WHERE",
                    }],
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

            # 1 – Blocking leakage comparison by ion species (log Y)
            (
                "Irrad – Blocking by Ion Species",
                degrad_ds,
                "echarts_timeseries_line",
                {
                    "x_axis": "v_gate_bin",
                    "time_grain_sqla": None,
                    "x_axis_sort_asc": True,
                    "metrics": [{
                        "expressionType": "SQL",
                        "sqlExpression": "AVG(avg_abs_i_drain)",
                        "label": "Avg |I_Drain| (A)",
                    }],
                    "groupby": ["device_type", "ion_species", "test_condition"],
                    "adhoc_filters": [cat_filter("Blocking")],
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

            # 2 – Degradation Summary table
            (
                "Irrad – Degradation Summary",
                degrad_ds,
                "table",
                {
                    "query_mode": "aggregate",
                    "groupby": ["device_type", "manufacturer", "ion_species",
                                "beam_energy_mev", "test_condition",
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

    # ── Tab 4: Individual Runs ───────────────────────────────────────────
    print("   Tab 4: Individual Runs...")

    tab4_chart_defs = [
        # 0 – Run Summary table
        (
            "Irrad – Run Summary",
            main_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["device_type", "device_id", "experiment",
                            "measurement_type", "measurement_category",
                            "test_condition", "campaign_name",
                            "irrad_condition_label"],
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

        # 1 – Individual IdVg curves (per device_id)
        (
            "Irrad – IdVg (Individual Runs)",
            main_ds,
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
                "groupby": ["device_id", "measurement_type",
                            "test_condition", "irrad_condition_label"],
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
                    "sqlExpression": "COUNT(DISTINCT v_gate_bin)",
                    "label": "_rank_by_sweep_range",
                },
            },
            12, 60,
        ),

        # 2 – Individual IdVd curves
        (
            "Irrad – IdVd (Individual Runs)",
            main_ds,
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
                "groupby": ["device_id", "measurement_type",
                            "test_condition", "irrad_condition_label"],
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
                    "sqlExpression": "COUNT(DISTINCT v_drain_bin)",
                    "label": "_rank_by_sweep_range",
                },
            },
            12, 60,
        ),

        # 3 – Individual Blocking curves (log Y)
        (
            "Irrad – Blocking (Individual Runs)",
            main_ds,
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
                "groupby": ["device_id", "measurement_type",
                            "test_condition", "irrad_condition_label"],
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
                    "sqlExpression": "COUNT(DISTINCT v_drain_bin)",
                    "label": "_rank_by_sweep_range",
                },
            },
            12, 60,
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
        ("Campaign Overview",         "TAB-overview",    tab1_info),
        ("Pre/Post Comparison",        "TAB-prepost",     tab2_info),
        ("Cross-Campaign Comparison",  "TAB-crosscamp",   tab3_info),
        ("Individual Runs",            "TAB-individual",  tab4_info),
    ]
    # Skip empty tabs
    tab_defs = [td for td in tab_defs if td[2]]

    position_json = build_dashboard_layout(tab_defs)

    native_filters = build_native_filters(
        all_chart_ids, main_ds,
        degrad_ds_id=degrad_ds,
        overview_ds_id=overview_ds,
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
        print("    1. Ion Species           (top-level radiation type)")
        print("    2. Beam Energy (MeV)     (cascades from Ion Species)")
        print("    3. Beam Type             (broad_beam / micro_beam)")
        print("    4. Campaign              (cascades from Ion Species)")
        print("    5. Manufacturer          (optional)")
        print("    6. Device Type           (cascades from Manufacturer)")
        print("    7. Test Condition        (pre_irrad / post_irrad)")
        print("    8. Measurement Category  (IdVg, IdVd, Blocking, etc.)")
    else:
        print("Dashboard creation failed — see errors above.")
    print("=" * 70)


if __name__ == "__main__":
    main()
