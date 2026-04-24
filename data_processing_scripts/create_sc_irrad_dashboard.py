#!/usr/bin/env python3
"""
Create the "SC ↔ Irradiation Damage Equivalence" dashboard in Apache Superset.

Uses the `damage_equivalence_view` built by ml_sc_irrad_equivalence.py.
Each row of that view is one *fingerprint* — either a short-circuit test
condition (device_type, sc_voltage_v, sc_duration_us) or an irradiation
run (device_type, ion_species, beam_energy_mev, let_surface) — with
median ΔVth / ΔRds(on) / ΔV(BR)DSS, their IQR, and sample counts.

Dashboard contents:
  1. Scatter — ΔVth vs ΔV(BR)DSS  (SC circles vs Irradiation triangles)
  2. Scatter — ΔVth vs ΔRds(on)
  3. Table   — Fingerprint summary with full columns (sortable)

Filter:
  - Device Type (cascades across all three charts).

Prerequisites:
  * PostgreSQL view `damage_equivalence_view` exists.  If not, run:
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


def ensure_view_exists():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.views WHERE table_name = %s",
            (VIEW_NAME,),
        )
        if cur.fetchone() is None:
            sys.exit(
                f"ERROR: SQL view '{VIEW_NAME}' does not exist.\n"
                "Run:  python3 ml_sc_irrad_equivalence.py --rebuild\n"
                "first to create it."
            )


def build_dashboard_layout(chart_tuples):
    """Two scatters side-by-side, one wide table below.  Minimal layout."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": ["ROW-top", "ROW-bottom"],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER", "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
        "ROW-top": {
            "type": "ROW", "id": "ROW-top",
            "children": ["CHART-scatter-bv", "CHART-scatter-rds"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "ROW-bottom": {
            "type": "ROW", "id": "ROW-bottom",
            "children": ["CHART-table"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }
    layout_map = {
        "bv":    "CHART-scatter-bv",
        "rds":   "CHART-scatter-rds",
        "table": "CHART-table",
    }
    for key, (cid, cuuid, cname, width, height) in chart_tuples.items():
        if cid is None:
            continue
        layout[layout_map[key]] = {
            "type": "CHART", "id": layout_map[key], "children": [],
            "parents": ["ROOT_ID", "GRID_ID",
                        "ROW-top" if key != "table" else "ROW-bottom"],
            "meta": {"chartId": cid, "width": width, "height": height,
                     "sliceName": cname, "uuid": cuuid},
        }
    return layout


def build_native_filters(chart_ids, ds_id):
    dev_fid = "NATIVE_FILTER-scirrad-device-type"
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
            "targets": [{"datasetId": ds_id, "column": {"name": "device_type"}}],
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "",
            "chartsInScope": chart_ids,
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
        ],
    }


def table_params():
    return {
        "query_mode": "raw",
        "all_columns": [
            "source", "device_type", "label",
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


def main():
    print("Creating SC ↔ Irradiation Damage Equivalence dashboard\n" + "=" * 70)

    print("1. Verifying view exists …")
    ensure_view_exists()
    print(f"   {VIEW_NAME} found")

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

    print("\n4. Registering dataset …")
    ds_id = find_or_create_dataset(session, db_id, VIEW_NAME)
    if not ds_id:
        sys.exit("   ERROR: dataset registration failed")
    refresh_dataset_columns(session, ds_id)

    print("\n5. Creating charts …")
    charts = {}

    cid, cuuid = create_chart(
        session, "Damage: ΔVth vs ΔV(BR)DSS", ds_id,
        "echarts_timeseries_scatter",
        scatter_params("dvth", "dbv", "ΔVth (V)", "ΔV(BR)DSS (V)"),
    )
    charts["bv"] = (cid, cuuid, "Damage: ΔVth vs ΔV(BR)DSS", 6, 50)

    cid, cuuid = create_chart(
        session, "Damage: ΔVth vs ΔRds(on)", ds_id,
        "echarts_timeseries_scatter",
        scatter_params("dvth", "drds", "ΔVth (V)", "ΔRds(on) (mΩ)"),
    )
    charts["rds"] = (cid, cuuid, "Damage: ΔVth vs ΔRds(on)", 6, 50)

    cid, cuuid = create_chart(
        session, "Damage Fingerprints Table", ds_id,
        "table",
        table_params(),
    )
    charts["table"] = (cid, cuuid, "Damage Fingerprints Table", 12, 50)

    chart_ids = [c[0] for c in charts.values() if c[0] is not None]
    if not chart_ids:
        sys.exit("   ERROR: no charts were created")

    print("\n6. Building dashboard …")
    position_json = build_dashboard_layout(charts)
    native_filters = build_native_filters(chart_ids, ds_id)
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
    print("  Filter: Device Type (cascades across all charts)")


if __name__ == "__main__":
    main()
