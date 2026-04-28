#!/usr/bin/env python3
"""
Create the "SC ↔ Irradiation Damage Equivalence" dashboard in Apache Superset.

Uses the views built by ml_sc_irrad_equivalence.py:
  * `damage_equivalence_view` for raw SC/irradiation fingerprints
  * `damage_equivalence_match_view` for ranked SC equivalents
  * `damage_equivalence_coverage_view` for device-level comparability
Each row of the fingerprint view is one short-circuit test condition
(device_type, sc_voltage_v, sc_duration_us) or one irradiation run
(device_type, ion_species, beam_energy_mev, let_surface), with median
ΔVth / ΔRds(on) / ΔV(BR)DSS, their IQR, and sample counts.

Dashboard contents:
  1. Table   — Device comparability coverage
  2. Scatter — ΔVth vs ΔV(BR)DSS  (comparable device types only)
  3. Scatter — ΔVth vs ΔRds(on)   (comparable device types only)
  4. Table   — Ranked nearest SC equivalents with distance/axis overlap
  5. Table   — Raw fingerprint summary with full columns (sortable)

Filter:
  - Device Type (cascades across all charts).
  - Comparability Status (coverage and nearest-match tables).
  - Ion Species (nearest-match table).

Prerequisites:
  * PostgreSQL damage-equivalence views exist.  If not, run:
      python3 ml_sc_irrad_equivalence.py --rebuild
  * Superset is reachable at SUPERSET_URL with SUPERSET_USER/PASS.

Usage:
    python3 create_sc_irrad_dashboard.py
"""

import json
import sys

from db_config import SUPERSET_URL, get_connection
from superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)


DASHBOARD_TITLE = "SC ↔ Irradiation Damage Equivalence"
DASHBOARD_SLUG = "sc-irrad-equivalence"
VIEW_NAME = "damage_equivalence_view"
MATCH_VIEW_NAME = "damage_equivalence_match_view"
COVERAGE_VIEW_NAME = "damage_equivalence_coverage_view"


def ensure_view_exists():
    required = [VIEW_NAME, MATCH_VIEW_NAME, COVERAGE_VIEW_NAME]
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
                + "\n"
                "Run:  python3 ml_sc_irrad_equivalence.py --rebuild\n"
                "first to create the damage-equivalence views."
            )


def build_dashboard_layout(chart_tuples):
    """Coverage first, comparable scatters, then ranked matches and raw data."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": [
                "ROW-coverage", "ROW-scatter", "ROW-matches",
                "ROW-fingerprints",
            ],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER", "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
        "ROW-coverage": {
            "type": "ROW", "id": "ROW-coverage",
            "children": ["CHART-coverage"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-scatter": {
            "type": "ROW", "id": "ROW-scatter",
            "children": ["CHART-scatter-bv", "CHART-scatter-rds"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-matches": {
            "type": "ROW", "id": "ROW-matches",
            "children": ["CHART-matches"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-fingerprints": {
            "type": "ROW", "id": "ROW-fingerprints",
            "children": ["CHART-fingerprints"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }
    layout_map = {
        "coverage":     ("CHART-coverage", "ROW-coverage"),
        "bv":           ("CHART-scatter-bv", "ROW-scatter"),
        "rds":          ("CHART-scatter-rds", "ROW-scatter"),
        "matches":      ("CHART-matches", "ROW-matches"),
        "fingerprints": ("CHART-fingerprints", "ROW-fingerprints"),
    }
    for key, (cid, cuuid, cname, width, height) in chart_tuples.items():
        if cid is None:
            continue
        chart_id, row_id = layout_map[key]
        layout[chart_id] = {
            "type": "CHART", "id": chart_id, "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"chartId": cid, "width": width, "height": height,
                     "sliceName": cname, "uuid": cuuid},
        }
    return layout


def build_native_filters(chart_ids, fp_ds_id, match_ds_id, coverage_ds_id,
                         match_chart_ids, coverage_chart_ids):
    dev_fid = "NATIVE_FILTER-scirrad-device-type"
    status_fid = "NATIVE_FILTER-scirrad-comparability-status"
    ion_fid = "NATIVE_FILTER-scirrad-ion-species"
    return [
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
            "targets": [
                {"datasetId": fp_ds_id, "column": {"name": "device_type"}},
                {"datasetId": match_ds_id, "column": {"name": "device_type"}},
                {"datasetId": coverage_ds_id, "column": {"name": "device_type"}},
            ],
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "",
            "chartsInScope": chart_ids,
            "tabsInScope": [],
        },
        {
            "id": status_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Comparability Status",
            "filterType": "filter_select",
            "targets": [
                {
                    "datasetId": match_ds_id,
                    "column": {"name": "comparability_status"},
                },
                {
                    "datasetId": coverage_ds_id,
                    "column": {"name": "comparability_status"},
                },
            ],
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "",
            "chartsInScope": match_chart_ids + coverage_chart_ids,
            "tabsInScope": [],
        },
        {
            "id": ion_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Ion Species",
            "filterType": "filter_select",
            "targets": [
                {"datasetId": match_ds_id, "column": {"name": "ion_species"}},
            ],
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "",
            "chartsInScope": match_chart_ids,
            "tabsInScope": [],
        },
    ]


def scatter_params(x_col, y_col, x_label, y_label):
    """Scatter-chart params for `echarts_timeseries_scatter`.

    The x-axis is a numeric column; groupby=['source', 'label'] makes
    each fingerprint a distinct series, which echarts renders as dots.
    """
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
        "row_limit": 10000,
        "truncate_metric": True,
        "show_legend": True,
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
        "adhoc_filters": [
            {
                "expressionType": "SQL",
                "sqlExpression": f"{x_col} IS NOT NULL AND {y_col} IS NOT NULL",
                "clause": "WHERE",
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "device_pair_status = 'SC + irradiation'",
                "clause": "WHERE",
            },
        ],
    }


def fingerprint_table_params():
    return {
        "query_mode": "raw",
        "all_columns": [
            "source", "device_type", "label",
            "device_pair_status", "device_sc_count", "device_irrad_count",
            "sc_voltage_v", "sc_duration_us",
            "ion_species", "beam_energy_mev", "let_surface",
            "dvth", "dvth_iqr", "dvth_n",
            "drds", "drds_iqr", "drds_n",
            "dbv",  "dbv_iqr",  "dbv_n",
            "n_samples",
        ],
        "metrics": [],
        "groupby": [],
        "order_by_cols": [json.dumps(["source", True]),
                          json.dumps(["device_type", True]),
                          json.dumps(["label", True])],
        "row_limit": 10000,
        "include_time": False,
        "table_timestamp_format": "smart_date",
    }


def coverage_table_params():
    return {
        "query_mode": "raw",
        "all_columns": [
            "comparability_status", "device_type",
            "n_sc_fingerprints", "n_irrad_fingerprints",
            "comparable_pair_count", "comparable_irrad_count",
            "best_distance", "comparable_axis_labels",
            "sc_dvth_fingerprints", "irrad_dvth_fingerprints",
            "sc_drds_fingerprints", "irrad_drds_fingerprints",
            "sc_dbv_fingerprints", "irrad_dbv_fingerprints",
        ],
        "metrics": [],
        "groupby": [],
        "order_by_cols": [
            json.dumps(["comparability_status", True]),
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
            "comparability_status", "device_type", "irrad_label",
            "ion_species", "beam_energy_mev", "let_surface",
            "sc_label", "sc_voltage_v", "sc_duration_us",
            "match_rank", "nearest_distance", "comparable_axes",
            "comparable_axis_labels", "sc_candidate_count",
            "abs_delta_dvth", "abs_delta_drds", "abs_delta_dbv",
            "irrad_n_samples", "sc_n_samples",
            "irrad_dvth", "sc_dvth",
            "irrad_drds", "sc_drds",
            "irrad_dbv", "sc_dbv",
            "irrad_dvth_iqr", "sc_dvth_iqr",
            "irrad_drds_iqr", "sc_drds_iqr",
            "irrad_dbv_iqr", "sc_dbv_iqr",
        ],
        "metrics": [],
        "groupby": [],
        "order_by_cols": [
            json.dumps(["device_type", True]),
            json.dumps(["irrad_label", True]),
            json.dumps(["match_rank", True]),
        ],
        "row_limit": 10000,
        "include_time": False,
        "table_timestamp_format": "smart_date",
        "adhoc_filters": [
            {
                "expressionType": "SQL",
                "sqlExpression": "match_rank <= 3",
                "clause": "WHERE",
            },
        ],
    }


def main():
    print("Creating SC ↔ Irradiation Damage Equivalence dashboard\n" + "=" * 70)

    print("1. Verifying view exists …")
    ensure_view_exists()
    print("   damage-equivalence views found")

    print("\n2. Authenticating with Superset …")
    try:
        session = get_session()
    except Exception as e:
        sys.exit(f"   ERROR: could not authenticate ({e})")
    print("   OK")

    print("\n3. Finding database …")
    db_id = find_database(session)
    if not db_id:
        sys.exit("   ERROR: database not found")

    print("\n4. Registering datasets …")
    fp_ds_id = find_or_create_dataset(session, db_id, VIEW_NAME)
    match_ds_id = find_or_create_dataset(session, db_id, MATCH_VIEW_NAME)
    coverage_ds_id = find_or_create_dataset(session, db_id, COVERAGE_VIEW_NAME)
    if not all([fp_ds_id, match_ds_id, coverage_ds_id]):
        sys.exit("   ERROR: dataset registration failed")
    for ds_id in (fp_ds_id, match_ds_id, coverage_ds_id):
        refresh_dataset_columns(session, ds_id)

    print("\n5. Creating charts …")
    charts = {}

    cid, cuuid = create_chart(
        session, "Comparable Device Coverage", coverage_ds_id,
        "table",
        coverage_table_params(),
    )
    charts["coverage"] = (cid, cuuid, "Comparable Device Coverage", 12, 28)

    cid, cuuid = create_chart(
        session, "Damage: ΔVth vs ΔV(BR)DSS", fp_ds_id,
        "echarts_timeseries_scatter",
        scatter_params("dvth", "dbv", "ΔVth (V)", "ΔV(BR)DSS (V)"),
    )
    charts["bv"] = (cid, cuuid, "Damage: ΔVth vs ΔV(BR)DSS", 6, 50)

    cid, cuuid = create_chart(
        session, "Damage: ΔVth vs ΔRds(on)", fp_ds_id,
        "echarts_timeseries_scatter",
        scatter_params("dvth", "drds", "ΔVth (V)", "ΔRds(on) (mΩ)"),
    )
    charts["rds"] = (cid, cuuid, "Damage: ΔVth vs ΔRds(on)", 6, 50)

    cid, cuuid = create_chart(
        session, "Nearest SC Equivalents", match_ds_id,
        "table",
        match_table_params(),
    )
    charts["matches"] = (cid, cuuid, "Nearest SC Equivalents", 12, 55)

    cid, cuuid = create_chart(
        session, "Damage Fingerprints Table", fp_ds_id,
        "table",
        fingerprint_table_params(),
    )
    charts["fingerprints"] = (cid, cuuid, "Damage Fingerprints Table", 12, 45)

    chart_ids = [c[0] for c in charts.values() if c[0] is not None]
    if not chart_ids:
        sys.exit("   ERROR: no charts were created")

    print("\n6. Building dashboard …")
    position_json = build_dashboard_layout(charts)
    match_chart_ids = [
        charts["matches"][0],
    ] if charts["matches"][0] is not None else []
    coverage_chart_ids = [
        charts["coverage"][0],
    ] if charts["coverage"][0] is not None else []
    native_filters = build_native_filters(
        chart_ids, fp_ds_id, match_ds_id, coverage_ds_id,
        match_chart_ids, coverage_chart_ids,
    )
    json_metadata = build_json_metadata(chart_ids, native_filters)

    dash_id = create_or_update_dashboard(
        session, DASHBOARD_TITLE, position_json, json_metadata,
        slug=DASHBOARD_SLUG,
    )
    if not dash_id:
        sys.exit("   ERROR: dashboard creation failed")

    print("\n7. Associating charts with dashboard …")
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
    print("  Filters: Device Type, Comparability Status, Ion Species")


if __name__ == "__main__":
    main()
