#!/usr/bin/env python3
"""
Create the "Baselines Device Library" dashboard in Apache Superset via its REST API.

This dashboard shows **averaged device performance** across all device-library
eligible runs of the same device type, using the ``baselines_device_averages``
SQL view.  Instead of plotting every individual experiment curve, it computes
mean ± standard deviation per voltage bin, giving an overview of typical
behaviour and run-to-run consistency.

Dashboard design
================
Filters (cascading):
  1. Manufacturer         – optional, multi-select
  2. Device Type          – required, multi-select (cascades from Manufacturer)
  3. Measurement Category – required, multi-select
  6. Likely Irradiated    – optional, defaults to false (pristine-only);
                            select true for irradiated-only or clear for all data

Tabs:
  1. Mean Curves          – averaged device performance (default)
  2. ±1σ Bands            – upper/lower standard-deviation bounds
  3. Individual Runs      – every run that contributes to the mean

Mean Curves tab charts:
  1. Data Summary         – table: device_type × category → n_devices, n_runs
  2. IdVg Transfer Curves – one line per device × V_drain bias (integer V)
  3. IdVd Output Curves   – one line per device × V_gate bias (5 V steps)
  4. 3rd Quadrant         – one line per device × V_gate bias (integer V)
  5. Igss Mean            – avg |I_Gate| vs V_Gate (log-Y)
  6. Vth Mean Curves      – one line per device × V_drain bias (integer V)
  7. Consistency Overview  – CV and std-dev per device × category
  8. Registered Devices   – reference table from device_library
  9. Measured Datasheet    – minimal companion to the box plots: one row per
                             device type, one "median ± σ" cell per key
                             parameter (Vth, Rds(on), V(BR)DSS, Vsd)
                             (virtual dataset: device_datasheet)
  10-13. Box & Whisker Plots – RDS(on), Vth, Vf, VBR distributions
                             per device type (virtual dataset:
                             device_params_per_device)

Individual Runs tab charts:
  1. Run Summary          – table: device_id × experiment × measurement_type
  2. IdVg Transfer Curves – one line per device_id × measurement_type
  3. IdVd Output Curves   – one line per device_id × measurement_type
  4. 3rd Quadrant         – one line per device_id × measurement_type
  5. Igss Gate Leakage    – one line per device_id × measurement_type (log-Y)
  6. Vth Curves           – one line per device_id × measurement_type

Workflow:
  Pick manufacturer → narrows device type list
  Pick device type  → summary + curve charts populate
  Pick category     → relevant charts populate
  Bias conditions are shown as separate lines (auto-grouped),
  no manual bias selection needed.
  Switch to "Individual Runs" tab to see every run behind the mean.

Usage:
    source /tmp/aps_venv/bin/activate
    python3 create_baselines_dashboard_device_library.py
"""

import json
import sys

from aps.superset.superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)
from aps.db_config import SUPERSET_URL


BOXPLOT_PARAMS_SQL = """WITH
/* Per-device parameter values for box-and-whisker plots.
   Reuses the same extraction logic as device_calculated_params but stops
   at the per-device level so each row represents one physical device.
   Only pristine (non-irradiated) devices are included.                    */

/* ── Test-condition discovery ──────────────────────────────────────────── */
idvd_dev_bias AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_gate_bin)  AS max_vgs,
           MAX(v_drain_bin) AS max_vds
    FROM pristine_per_device
    WHERE measurement_category = 'IdVd'
      AND dev_avg_i_drain > 0    GROUP BY device_id, device_type, manufacturer
),
q3_dev_anchor AS (
    -- Body-diode VGS anchor: least-negative bin where Id<0.  See
    -- device_calculated_params for full reasoning.
    SELECT device_id, device_type, manufacturer,
           MAX(v_gate_bin) AS anchor_vgs
    FROM pristine_per_device
    WHERE measurement_category = '3rd_Quadrant'
      AND dev_avg_i_drain < 0    GROUP BY device_id, device_type, manufacturer
),

/* ── Vth per device ────────────────────────────────────────────────────── */
vth_dev_peak AS (
    SELECT device_id, device_type, manufacturer,
           GREATEST(0.005, MAX(dev_avg_i_drain) * 0.01) AS i_thresh
    FROM pristine_per_device
    WHERE measurement_category = 'Vth'
      AND dev_avg_i_drain > 0    GROUP BY device_id, device_type, manufacturer
),
vth_dev_crossing AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           b.v_drain_bin,
           MIN(b.v_gate_bin) AS vth_v
    FROM pristine_per_device b
    JOIN vth_dev_peak t USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'Vth'
      AND b.dev_avg_i_drain >= t.i_thresh    GROUP BY b.device_id, b.device_type, b.manufacturer, b.v_drain_bin
),
vth_dev_min_vds AS (
    SELECT device_id, device_type, manufacturer,
           MIN(ABS(v_drain_bin)) AS min_abs_vds
    FROM vth_dev_crossing
    GROUP BY device_id, device_type, manufacturer
),
vth_per_device AS (
    SELECT c.device_id, c.device_type, c.manufacturer,
           c.vth_v
    FROM vth_dev_crossing c
    JOIN vth_dev_min_vds m USING (device_id, device_type, manufacturer)
    WHERE ABS(c.v_drain_bin) = m.min_abs_vds
),

/* ── Rds(on) per device ────────────────────────────────────────────────── */
rdson_per_device AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           SUM(b.v_drain_bin * b.v_drain_bin) /
               NULLIF(SUM(b.v_drain_bin * b.dev_avg_i_drain), 0)
               * 1000.0 AS rdson_mohm
    FROM pristine_per_device b
    JOIN idvd_dev_bias m USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'IdVd'
      AND b.v_gate_bin  BETWEEN m.max_vgs - 1.0 AND m.max_vgs + 1.0
      AND b.v_drain_bin >  0.0
      AND b.v_drain_bin <= LEAST(m.max_vds * 0.15, 2.0)
      AND b.dev_avg_i_drain > 0.0    GROUP BY b.device_id, b.device_type, b.manufacturer
),

/* ── Vsd (body diode forward voltage) per device ───────────────────────── */
vsd_dev_target AS (
    -- Reference current WITHIN the anchor slice, with a 10 mA
    -- conduction floor.  See device_calculated_params for reasoning.
    SELECT b.device_id, b.device_type, b.manufacturer,
           a.anchor_vgs,
           MIN(b.dev_avg_i_drain) * 0.1 AS target_id
    FROM pristine_per_device b
    JOIN q3_dev_anchor a USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = '3rd_Quadrant'
      AND b.dev_avg_i_drain < 0      AND b.v_gate_bin BETWEEN a.anchor_vgs - 0.5 AND a.anchor_vgs + 0.5
    GROUP BY b.device_id, b.device_type, b.manufacturer, a.anchor_vgs
    HAVING ABS(MIN(b.dev_avg_i_drain)) >= 0.010
),
vsd_dev_ranked AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           ABS(b.v_drain_bin) AS vsd_v,
           ROW_NUMBER() OVER (
               PARTITION BY b.device_id, b.device_type, b.manufacturer
               ORDER BY ABS(b.dev_avg_i_drain - t.target_id) ASC
           ) AS rn
    FROM pristine_per_device b
    JOIN vsd_dev_target t USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = '3rd_Quadrant'
      AND b.v_gate_bin BETWEEN t.anchor_vgs - 0.5 AND t.anchor_vgs + 0.5
      AND b.dev_avg_i_drain < 0),
vsd_per_device AS (
    SELECT device_id, device_type, manufacturer, vsd_v
    FROM vsd_dev_ranked WHERE rn = 1
),

/* ── V(BR)DSS per device ───────────────────────────────────────────────── */
bvdss_dev_crossed AS (
    SELECT device_id, device_type, manufacturer,
           MIN(v_drain_bin) AS bvdss_v
    FROM pristine_per_device
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND dev_avg_abs_i_drain >= 100e-6    GROUP BY device_id, device_type, manufacturer
),
bvdss_dev_held AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_drain_bin) AS bvdss_v
    FROM pristine_per_device
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0      AND (device_id, device_type, manufacturer) NOT IN (
          SELECT device_id, device_type, manufacturer
          FROM bvdss_dev_crossed
      )
    GROUP BY device_id, device_type, manufacturer
),
bvdss_per_device AS (
    SELECT * FROM bvdss_dev_crossed
    UNION ALL
    SELECT * FROM bvdss_dev_held
),

/* ── Combine all per-device values into one wide row ───────────────────── */
all_devices AS (
    SELECT DISTINCT device_id, device_type, manufacturer
    FROM (
        SELECT device_id, device_type, manufacturer FROM vth_per_device
        UNION
        SELECT device_id, device_type, manufacturer FROM rdson_per_device
        UNION
        SELECT device_id, device_type, manufacturer FROM vsd_per_device
        UNION
        SELECT device_id, device_type, manufacturer FROM bvdss_per_device
    ) u
)

SELECT
    a.device_id,
    a.device_type,
    a.manufacturer,
    ROUND(v.vth_v::numeric,       3) AS vth_v,
    ROUND(r.rdson_mohm::numeric,  2) AS rdson_mohm,
    ROUND(s.vsd_v::numeric,       3) AS vsd_v,
    ROUND(bv.bvdss_v::numeric,    1) AS bvdss_v
FROM all_devices a
LEFT JOIN vth_per_device    v  USING (device_id, device_type, manufacturer)
LEFT JOIN rdson_per_device  r  USING (device_id, device_type, manufacturer)
LEFT JOIN vsd_per_device    s  USING (device_id, device_type, manufacturer)
LEFT JOIN bvdss_per_device  bv USING (device_id, device_type, manufacturer)
WHERE r.rdson_mohm IS NULL OR (r.rdson_mohm > 0 AND r.rdson_mohm < 1e6)
ORDER BY a.device_type, a.device_id
"""


# ── Measured Datasheet SQL ────────────────────────────────────────────────────
# Minimal "measured datasheet": one row per device_type with the median ± σ of
# the four key parameters, aggregated across the individual-device values that
# feed the box-and-whisker plots (device_params_per_device / BOXPLOT_PARAMS_SQL).
# Sourcing from the same per-device query guarantees the table mirrors the plots
# (the box median line == the table median); σ is the spread across devices.
# Cells are formatted "median ± σ" so the honesty signal (population spread)
# travels with the number; a device with n=1 shows the bare median, since σ is
# undefined.  Units live in the column headers, not the cells, to stay minimal.
# Device types with no extractable parameter are dropped by the HAVING clause.
DEVICE_DATASHEET_SQL = """SELECT
    pd.device_type,
    MAX(pd.manufacturer) AS manufacturer,
    CASE
        WHEN COUNT(pd.vth_v) = 0 THEN NULL
        WHEN COUNT(pd.vth_v) = 1
            THEN to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.vth_v), 'FM9990.00')
        ELSE to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.vth_v), 'FM9990.00')
             || ' ± ' || to_char(stddev_samp(pd.vth_v), 'FM9990.00')
    END AS vth_disp,
    CASE
        WHEN COUNT(pd.rdson_mohm) = 0 THEN NULL
        WHEN COUNT(pd.rdson_mohm) = 1
            THEN to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.rdson_mohm), 'FM9990.0')
        ELSE to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.rdson_mohm), 'FM9990.0')
             || ' ± ' || to_char(stddev_samp(pd.rdson_mohm), 'FM9990.0')
    END AS rdson_disp,
    CASE
        WHEN COUNT(pd.bvdss_v) = 0 THEN NULL
        WHEN COUNT(pd.bvdss_v) = 1
            THEN to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.bvdss_v), 'FM99990')
        ELSE to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.bvdss_v), 'FM99990')
             || ' ± ' || to_char(stddev_samp(pd.bvdss_v), 'FM99990')
    END AS bvdss_disp,
    CASE
        WHEN COUNT(pd.vsd_v) = 0 THEN NULL
        WHEN COUNT(pd.vsd_v) = 1
            THEN to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.vsd_v), 'FM9990.00')
        ELSE to_char(percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.vsd_v), 'FM9990.00')
             || ' ± ' || to_char(stddev_samp(pd.vsd_v), 'FM9990.00')
    END AS vsd_disp
FROM (
""" + BOXPLOT_PARAMS_SQL + """
) pd
GROUP BY pd.device_type
HAVING COUNT(pd.vth_v) + COUNT(pd.rdson_mohm)
     + COUNT(pd.bvdss_v) + COUNT(pd.vsd_v) > 0
ORDER BY pd.device_type
"""


def find_or_create_virtual_dataset(session, db_id, name, sql_query,
                                   schema="public"):
    """Find or create a SQL-based (virtual) dataset; update SQL if it exists."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/dataset/",
        params={"q": json.dumps({
            "filters": [{"col": "table_name", "opr": "eq", "value": name}],
            "page_size": 100,
        })},
    )
    resp.raise_for_status()
    for ds in resp.json()["result"]:
        if ds.get("table_name") == name:
            ds_id = ds["id"]
            session.put(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
                        json={"sql": sql_query})
            print(f"  Virtual dataset '{name}' exists (id={ds_id})")
            return ds_id

    resp = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json={
        "database": db_id,
        "table_name": name,
        "schema": schema,
        "sql": sql_query,
    })
    if resp.ok:
        ds_id = resp.json()["id"]
        print(f"  Created virtual dataset '{name}' (id={ds_id})")
        return ds_id
    print(f"  ERROR creating virtual dataset '{name}': "
          f"{resp.status_code} {resp.text[:200]}")
    return None


# refresh_dataset_columns() and create_chart() are imported from superset_api.


# ── Dashboard Layout ─────────────────────────────────────────────────────────

def build_dashboard_layout(charts, sigma_charts=None, individual_charts=None):
    """Build position_json from (chart_id, uuid, name, width, height) tuples.

    If *sigma_charts* is provided, creates a tabbed layout:
    'Mean Curves' (first tab, shown by default) and '±1σ Bands' (second tab).
    If *individual_charts* is also provided, adds a third 'Individual Runs' tab.
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
            "meta": {"text": "Baselines Device Library"},
        },
    }
    def _add_chart_rows(chart_list, prefix, parents):
        """Add ROW+CHART entries for a list of charts. Returns row IDs."""
        row_ids = []
        for i, (cid, cuuid, cname, width, height) in enumerate(chart_list):
            if cid is None:
                continue
            row_id = f"ROW-{prefix}-{i}"
            chart_key = f"CHART-{prefix}-{i}"
            layout[row_id] = {
                "type": "ROW", "id": row_id,
                "children": [chart_key],
                "parents": parents,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            layout[chart_key] = {
                "type": "CHART", "id": chart_key, "children": [],
                "parents": parents + [row_id],
                "meta": {
                    "chartId": cid, "width": width, "height": height,
                    "sliceName": cname, "uuid": cuuid,
                },
            }
            row_ids.append(row_id)
        return row_ids

    if sigma_charts:
        # Tabbed layout: Mean Curves + ±1σ Bands + (optionally) Individual Runs
        tabs_id = "TABS-bl"
        tab_mean_id = "TAB-mean"
        tab_sigma_id = "TAB-sigma"
        tab_indiv_id = "TAB-individual"

        tab_children = [tab_mean_id, tab_sigma_id]
        if individual_charts:
            tab_children.append(tab_indiv_id)

        layout["GRID_ID"]["children"] = [tabs_id]
        layout[tabs_id] = {
            "type": "TABS", "id": tabs_id,
            "children": tab_children,
            "parents": ["ROOT_ID", "GRID_ID"],
        }

        mean_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_mean_id]
        mean_rows = _add_chart_rows(charts, "mean", mean_parents)
        layout[tab_mean_id] = {
            "type": "TAB", "id": tab_mean_id,
            "children": mean_rows,
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": "Mean Curves"},
        }

        sigma_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_sigma_id]
        sigma_rows = _add_chart_rows(sigma_charts, "sigma", sigma_parents)
        layout[tab_sigma_id] = {
            "type": "TAB", "id": tab_sigma_id,
            "children": sigma_rows,
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": "\u00b11\u03c3 Bands"},
        }

        if individual_charts:
            indiv_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_indiv_id]
            indiv_rows = _add_chart_rows(individual_charts, "indiv",
                                         indiv_parents)
            layout[tab_indiv_id] = {
                "type": "TAB", "id": tab_indiv_id,
                "children": indiv_rows,
                "parents": ["ROOT_ID", "GRID_ID", tabs_id],
                "meta": {"text": "Individual Runs"},
            }
    else:
        # Flat layout (no sigma charts)
        row_children = _add_chart_rows(charts, "bl", ["ROOT_ID", "GRID_ID"])
        layout["GRID_ID"]["children"] = row_children

    return layout


# ── Native Filters ───────────────────────────────────────────────────────────

def build_native_filters(chart_ids, avg_ds_id, always_excluded=None,
                         irrad_excluded=None,
                         v_drain_chart_ids=None, v_gate_chart_ids=None,
                         indiv_ds_id=None, calc_ds_id=None,
                         meta_ds_id=None, boxplot_ds_id=None):
    """
    Five native filters for the averaged device-performance dashboard:

    1. Likely Irradiated    – boolean, defaults to false (pristine only)
    2. Manufacturer         – optional, multi-select
    3. Device Type          – required, cascades from Manufacturer
    4. V_Drain Bias (V)     – optional range, scoped to IdVg/Vth charts
    5. V_Gate Bias (V)      – optional range, scoped to IdVd/3rdQ charts

    *always_excluded* — charts excluded from Manufacturer / Device Type
    filters (e.g. device_library table, calc params virtual dataset).
    *irrad_excluded* — charts excluded from the Likely Irradiated filter
    (superset of always_excluded; also includes box plots whose virtual
    dataset has no is_likely_irradiated column).

    If *indiv_ds_id* is provided, the bias filters also target the
    individual-runs dataset (same column names: v_drain_bias, v_gate_bias).
    If *meta_ds_id* is provided, Manufacturer and Device Type filters also
    target baselines_metadata so the TSP Parameters table is filtered.
    If *boxplot_ds_id* is provided, Manufacturer and Device Type filters
    also target the box-plot virtual dataset.
    *calc_ds_id* is accepted but not used as a filter target — the calculated-
    parameters chart is excluded from all filters (always-excluded) to avoid
    a failing virtual-dataset SQL from breaking the filter dropdowns.
    """
    irr_fid = "NATIVE_FILTER-likely-irradiated"
    mfr_fid = "NATIVE_FILTER-manufacturer"
    dev_fid = "NATIVE_FILTER-device-type"
    vd_fid  = "NATIVE_FILTER-v-drain-bias"
    vg_fid  = "NATIVE_FILTER-v-gate-bias"

    always_excluded = always_excluded or []
    irrad_excluded = irrad_excluded or always_excluded
    filtered = [c for c in chart_ids if c not in always_excluded]
    irrad_filtered = [c for c in chart_ids if c not in irrad_excluded]
    v_drain_chart_ids = v_drain_chart_ids or []
    v_gate_chart_ids = v_gate_chart_ids or []

    # Charts NOT in a bias filter's scope must be excluded for that filter
    vd_excluded = [c for c in chart_ids if c not in v_drain_chart_ids]
    vg_excluded = [c for c in chart_ids if c not in v_gate_chart_ids]

    filters = [
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
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "manufacturer"}}]
                       + ([{"datasetId": meta_ds_id,
                            "column": {"name": "manufacturer"}}]
                          if meta_ds_id else [])
                       + ([{"datasetId": boxplot_ds_id,
                            "column": {"name": "manufacturer"}}]
                          if boxplot_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": list(always_excluded)},
            "type": "NATIVE_FILTER",
            "description": "Filter by device manufacturer",
            "chartsInScope": filtered,
            "tabsInScope": [],
        },
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
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "device_type"}}]
                       + ([{"datasetId": meta_ds_id,
                            "column": {"name": "device_type"}}]
                          if meta_ds_id else [])
                       + ([{"datasetId": boxplot_ds_id,
                            "column": {"name": "device_type"}}]
                          if boxplot_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [mfr_fid],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": list(always_excluded)},
            "type": "NATIVE_FILTER",
            "description": "Select device type(s) from the device library",
            "chartsInScope": filtered,
            "tabsInScope": [],
        },
        {
            "id": vd_fid,
            "controlValues": {
                "enableEmptyFilter": False,
            },
            "name": "V_Drain Bias (V) → IdVg, Vth",
            "filterType": "filter_range",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "v_drain_bias"}}]
                       + ([{"datasetId": indiv_ds_id,
                            "column": {"name": "v_drain_bias"}}]
                          if indiv_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": vd_excluded},
            "type": "NATIVE_FILTER",
            "description": "Select V_Drain bias values (IdVg / Vth charts)",
            "chartsInScope": v_drain_chart_ids,
            "tabsInScope": [],
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression":
                    "measurement_category IN ('IdVg', 'Vth')",
                "clause": "WHERE",
            }],
        },
        {
            "id": vg_fid,
            "controlValues": {
                "enableEmptyFilter": False,
            },
            "name": "V_Gate Bias (V) → IdVd, 3rd Quadrant",
            "filterType": "filter_range",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "v_gate_bias"}}]
                       + ([{"datasetId": indiv_ds_id,
                            "column": {"name": "v_gate_bias"}}]
                          if indiv_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": vg_excluded},
            "type": "NATIVE_FILTER",
            "description": "Select V_Gate bias values (IdVd / 3rd Quadrant charts)",
            "chartsInScope": v_gate_chart_ids,
            "tabsInScope": [],
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression":
                    "measurement_category IN ('IdVd', '3rd_Quadrant')",
                "clause": "WHERE",
            }],
        },
        {
            "id": irr_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "name": "Likely Irradiated",
            "filterType": "filter_select",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "is_likely_irradiated"}}]
                       + ([{"datasetId": indiv_ds_id,
                          "column": {"name": "is_likely_irradiated"}}]
                        if indiv_ds_id else [])
                       + ([{"datasetId": meta_ds_id,
                            "column": {"name": "is_likely_irradiated"}}]
                          if meta_ds_id else []),
            "defaultDataMask": {
                "extraFormData": {
                    "filters": [{
                        "col": "is_likely_irradiated",
                        "op": "IN",
                        "val": [False],
                    }],
                },
                "filterState": {
                    "label": "false",
                    "value": [False],
                },
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": list(irrad_excluded)},
            "type": "NATIVE_FILTER",
            "description": "Filter by irradiation status "
                           "(defaults to false for pristine-only; select true "
                           "for irradiated-only, or clear for all data)",
            "chartsInScope": irrad_filtered,
            "tabsInScope": [],
        },
    ]
    return filters


# build_json_metadata() and create_or_update_dashboard() are imported from superset_api.


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Creating Baselines Device Library Dashboard (Averaged Performance)")
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
    avg_ds = find_or_create_dataset(session, db_id, "baselines_device_averages")
    devlib_ds = find_or_create_dataset(session, db_id, "device_library")
    indiv_ds = find_or_create_dataset(session, db_id,
                                      "baselines_view_device_library")
    if not avg_ds:
        print("  FATAL: Could not create baselines_device_averages dataset.")
        print("  Run ingestion_baselines.py first to create the view.")
        sys.exit(1)
    if not indiv_ds:
        print("  WARNING: Could not create baselines_view_device_library dataset.")
        print("  Individual Runs tab will be skipped.")
    meta_ds = find_or_create_dataset(session, db_id, "baselines_metadata")
    if not meta_ds:
        print("  WARNING: Could not create baselines_metadata dataset.")
        print("  TSP Parameters table will be skipped.")
    datasheet_ds = find_or_create_virtual_dataset(
        session, db_id, "device_datasheet", DEVICE_DATASHEET_SQL
    )
    if not datasheet_ds:
        print("  WARNING: Could not create device_datasheet dataset.")
        print("  Measured Datasheet chart will be skipped.")
    boxplot_ds = find_or_create_virtual_dataset(
        session, db_id, "device_params_per_device", BOXPLOT_PARAMS_SQL
    )
    if not boxplot_ds:
        print("  WARNING: Could not create device_params_per_device dataset.")
        print("  Box plot charts will be skipped.")
    for ds_id in [avg_ds, devlib_ds, indiv_ds, meta_ds, datasheet_ds, boxplot_ds]:
        if ds_id:
            refresh_dataset_columns(session, ds_id)

    # 4. Create charts
    print("\n4. Creating charts...")

    # Helper to build a category filter
    def cat_filter(cat):
        return {
            "expressionType": "SQL",
            "sqlExpression": f"measurement_category = '{cat}'",
            "clause": "WHERE",
        }

    # Simple line-chart params for averaged curve charts.
    # Each chart groups by (device_type, coarsened_bias) so that every
    # bias condition appears as a separate line — matching the clean
    # per-experiment look of the Baselines dashboard.
    def curve_params(x_axis, bias_col, bias_round, cat, x_title, y_title,
                     metric_expr=("SUM(avg_i_drain * n_devices) "
                                  "/ NULLIF(SUM(n_devices), 0)"),
                     metric_label="Mean I_Drain (A)",
                     log_y=False, series_limit=0):
        """
        Parameters
        ----------
        x_axis     : str       – column for the x-axis (swept voltage)
        bias_col   : str|None  – column to group bias conditions by
        bias_round : int       – rounding divisor for the bias column
                                 (1 = integer, 5 = 5 V steps, etc.)
        cat        : str       – measurement_category value
        """
        groupby = ["device_type"]
        if bias_col:
            if bias_round == 1:
                sql = f"ROUND({bias_col})"
            else:
                sql = f"ROUND({bias_col} / {bias_round}) * {bias_round}"
            groupby.append({
                "expressionType": "SQL",
                "sqlExpression": sql,
                "label": bias_col.replace("_bin", " (V)"),
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
                "sqlExpression": "SUM(n_points)",
                "label": "_rank_by_frequency",
            }
        return params

    def sigma_curve_params(x_axis, bias_col, bias_round, cat, x_title, y_title,
                          upper_expr=None,
                          lower_expr=None,
                          upper_label="+1\u03c3 I_Drain (A)",
                          lower_label="\u22121\u03c3 I_Drain (A)",
                          log_y=False, series_limit=0):
        """Like curve_params but with two metrics: upper and lower ±1σ bounds."""
        upper_expr = (upper_expr if upper_expr is not None else
                      "SUM(upper_i_drain * n_devices) "
                      "/ NULLIF(SUM(n_devices), 0)")
        lower_expr = (lower_expr if lower_expr is not None else
                      "SUM(lower_i_drain * n_devices) "
                      "/ NULLIF(SUM(n_devices), 0)")
        groupby = ["device_type"]
        if bias_col:
            if bias_round == 1:
                sql = f"ROUND({bias_col})"
            else:
                sql = f"ROUND({bias_col} / {bias_round}) * {bias_round}"
            groupby.append({
                "expressionType": "SQL",
                "sqlExpression": sql,
                "label": bias_col.replace("_bin", " (V)"),
            })

        params = {
            "x_axis": x_axis,
            "time_grain_sqla": None,
            "x_axis_sort_asc": True,
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": upper_expr,
                    "label": upper_label,
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": lower_expr,
                    "label": lower_label,
                },
            ],
            "groupby": groupby,
            "adhoc_filters": [cat_filter(cat)],
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
                "sqlExpression": "SUM(n_points)",
                "label": "_rank_by_frequency",
            }
        return params

    chart_defs = [
        # 0 – Data Summary: how many devices / runs per category
        (
            "Device Library – Data Summary",
            avg_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["manufacturer", "device_type",
                            "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_devices)",
                     "label": "Devices Averaged"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_runs)",
                     "label": "Total Runs"},
                    {"expressionType": "SQL",
                     "sqlExpression": "SUM(n_points)",
                     "label": "Total Points"},
                ],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 10000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),

        # 1 – IdVg Transfer Curves
        #     Sweep V_gate, one line per (device, V_drain bias)
        #     V_drain has ~6 integer values → readable
        (
            "Device Library – IdVg Transfer Curves",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_gate_plot_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="IdVg",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 2 – Vth Curves
        #     Sweep V_gate, one line per (device, V_drain bias)
        #     V_drain has ~3 values → very clean
        (
            "Device Library – Vth Curves (Mean)",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_gate_plot_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="Vth",
                x_title="V_Gate (V)",
                y_title="Mean I_Drain (A)",
            ),
            12, 60,
        ),

        # 3 – IdVd Output Curves
        #     Sweep V_drain, one line per (device, V_gate bias)
        #     Use integer V_gate rounding; series_limit keeps the
        #     most common bias values to avoid cluttered legends.
        (
            "Device Library – IdVd Output Curves",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_drain_plot_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="IdVd",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                series_limit=10,
            ),
            12, 60,
        ),

        # 4 – 3rd Quadrant
        #     Sweep V_drain, one line per (device, V_gate bias)
        #     V_gate has ~8 integer values → readable
        (
            "Device Library – 3rd Quadrant",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_drain_plot_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="3rd_Quadrant",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 5 – Igss Mean Gate Leakage (log Y, no bias dimension)
        (
            "Device Library – Igss Gate Leakage (Mean)",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_gate_plot_bin",
                bias_col=None, bias_round=1,
                cat="Igss",
                x_title="V_Gate (V)",
                y_title="Mean |I_Gate| (A)",
                metric_expr=("SUM(avg_abs_i_gate * n_devices) "
                             "/ NULLIF(SUM(n_devices), 0)"),
                metric_label="Mean |I_Gate| (A)",
                log_y=True,
            ),
            12, 60,
        ),

        # 6 – Consistency: coefficient of variation (std/|mean|)
        (
            "Device Library – Run-to-Run Consistency",
            avg_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["device_type", "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_devices)",
                     "label": "Max Devices"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_runs)",
                     "label": "Max Runs"},
                    {"expressionType": "SQL",
                     "sqlExpression": (
                         "AVG(CASE WHEN ABS(avg_i_drain) > 1e-10 "
                         "THEN std_i_drain / ABS(avg_i_drain) END)"
                     ),
                     "label": "Avg CV (σ/μ)"},
                    {"expressionType": "SQL",
                     "sqlExpression": (
                         "AVG(CASE WHEN ABS(avg_i_drain) > 1e-10 "
                         "THEN std_i_drain END)"
                     ),
                     "label": "Avg Std Dev"},
                ],
                "adhoc_filters": [],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 1000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),

        # 7 – Registered Devices (unfiltered reference table)
        (
            "Device Library – Registered Devices",
            devlib_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "part_number", "device_category", "manufacturer",
                    "voltage_rating", "rdson_mohm", "current_rating_a",
                    "package_type", "notes",
                ],
                "row_limit": 500,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),
    ]

    # 8 – Measured Datasheet (virtual dataset device_datasheet)
    # Minimal companion to the four box plots below: one row per device_type,
    # one column per key parameter, each cell the population "median ± σ" across
    # devices (same per-device source as the box plots, so the numbers match the
    # median lines).  Units are in the headers, not the cells; device types with
    # no extractable parameter are dropped by the dataset's HAVING clause.
    if datasheet_ds:
        chart_defs.append((
            "Device Library – Measured Datasheet",
            datasheet_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "device_type", "manufacturer",
                    "vth_disp", "rdson_disp", "bvdss_disp", "vsd_disp",
                ],
                "column_config": {
                    "device_type":  {"label": "Device"},
                    "manufacturer": {"label": "Manufacturer"},
                    "vth_disp":     {"label": "V_th (V)"},
                    "rdson_disp":   {"label": "R_ds(on) (mΩ)"},
                    "bvdss_disp":   {"label": "V(BR)DSS (V)"},
                    "vsd_disp":     {"label": "V_sd (V)"},
                },
                "row_limit": 500,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 40,
        ))

    # 9–12 – Box & Whisker plots for key parameters (virtual dataset)
    # Each box plot shows the distribution of a calculated parameter across
    # individual devices, grouped by device_type on the x-axis.
    boxplot_chart_defs = []
    if boxplot_ds:
        boxplot_params_list = [
            ("Device Library – RDS(on) Distribution",
             "rdson_mohm", "RDS(on) (mΩ)"),
            ("Device Library – Vth Distribution",
             "vth_v", "Vth (V)"),
            ("Device Library – Vf (Vsd) Distribution",
             "vsd_v", "Vf / Vsd (V)"),
            ("Device Library – VBR Distribution",
             "bvdss_v", "V(BR)DSS (V)"),
        ]
        for bp_name, bp_col, bp_label in boxplot_params_list:
            boxplot_chart_defs.append((
                bp_name,
                boxplot_ds,
                "box_plot",
                {
                    "columns": ["device_id"],
                    "metrics": [{
                        "expressionType": "SIMPLE",
                        "column": {"column_name": bp_col},
                        "aggregate": "AVG",
                        "label": bp_label,
                    }],
                    "groupby": ["device_type"],
                    "adhoc_filters": [{
                        "expressionType": "SQL",
                        "sqlExpression": f"{bp_col} IS NOT NULL",
                        "clause": "WHERE",
                    }],
                    "whiskerOptions": "Tukey",
                    "x_ticks_layout": "45°",
                    "color_scheme": "supersetColors",
                    "row_limit": 10000,
                },
                6, 50,
            ))

    # ── ±1σ chart definitions (one per curve chart) ────────────────────────
    sigma_chart_defs = [
        # 0 – IdVg ±1σ
        (
            "Device Library – IdVg Transfer Curves (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_gate_plot_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="IdVg",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 1 – Vth ±1σ
        (
            "Device Library – Vth Curves (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_gate_plot_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="Vth",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 2 – IdVd ±1σ
        (
            "Device Library – IdVd Output Curves (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_drain_plot_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="IdVd",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                series_limit=10,
            ),
            12, 60,
        ),

        # 3 – 3rd Quadrant ±1σ
        (
            "Device Library – 3rd Quadrant (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_drain_plot_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="3rd_Quadrant",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 4 – Igss ±1σ
        (
            "Device Library – Igss Gate Leakage (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_gate_plot_bin",
                bias_col=None, bias_round=1,
                cat="Igss",
                x_title="V_Gate (V)",
                y_title="|I_Gate| (A)",
                upper_expr=("SUM((avg_abs_i_gate + COALESCE(std_i_gate, 0)) "
                            "* n_devices) / NULLIF(SUM(n_devices), 0)"),
                lower_expr=("SUM(GREATEST(avg_abs_i_gate - COALESCE(std_i_gate, 0), 0) "
                            "* n_devices) / NULLIF(SUM(n_devices), 0)"),
                upper_label="+1σ |I_Gate| (A)",
                lower_label="−1σ |I_Gate| (A)",
                log_y=True,
            ),
            12, 60,
        ),
    ]

    # ── Individual Runs chart definitions (one per curve chart) ──────────
    # These use baselines_view_device_library and group by device_id so
    # each physical run appears as its own line.

    def indiv_curve_params(x_axis, cat, x_title, y_title,
                           metric_expr="AVG(i_drain)",
                           metric_label="I_Drain (A)",
                           log_y=False, series_limit=0,
                           bias_col=None):
        """Line-chart params for individual-run curves.

        Groups by (device_id, measurement_type, bias_col) so every run
        at each bias condition is a separate line.  A series_limit keeps
        the chart readable.
        """
        groupby = ["device_id", "measurement_type", "metadata_id",
                   "step_index"]
        if bias_col:
            groupby.append(bias_col)
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
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression": f"measurement_category = '{cat}'",
                "clause": "WHERE",
            }],
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
            # Rank by number of *distinct* x-axis values so actual sweeps
            # (many voltage points) outrank single-point measurements
            # (e.g. Rdson at one fixed v_drain).
            params["series_limit_metric"] = {
                "expressionType": "SQL",
                "sqlExpression": f"COUNT(DISTINCT {x_axis})",
                "label": "_rank_by_sweep_range",
            }
        return params

    individual_chart_defs = []
    if indiv_ds:
        individual_chart_defs = [
            # 0 – Run Summary table
            (
                "Device Library – Run Summary (Individual)",
                indiv_ds,
                "table",
                {
                    "query_mode": "aggregate",
                    "groupby": ["device_type", "device_id", "experiment",
                                "measurement_type", "measurement_category"],
                    "metrics": [
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

            # 1 – IdVg Transfer Curves (Individual Runs)
            #     bias = V_drain (integer-rounded)
            (
                "Device Library – IdVg Transfer (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_gate_plot_bin",
                    cat="IdVg",
                    x_title="V_Gate (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_drain_bias",
                ),
                12, 60,
            ),

            # 2 – Vth Curves (Individual Runs)
            #     bias = V_drain (integer-rounded)
            (
                "Device Library – Vth Curves (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_gate_plot_bin",
                    cat="Vth",
                    x_title="V_Gate (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_drain_bias",
                ),
                12, 60,
            ),

            # 3 – IdVd Output Curves (Individual Runs)
            #     bias = V_gate (integer-rounded)
            (
                "Device Library – IdVd Output (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_drain_plot_bin",
                    cat="IdVd",
                    x_title="V_Drain (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_gate_bias",
                ),
                12, 60,
            ),

            # 4 – 3rd Quadrant (Individual Runs)
            #     bias = V_gate (integer-rounded)
            (
                "Device Library – 3rd Quadrant (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_drain_plot_bin",
                    cat="3rd_Quadrant",
                    x_title="V_Drain (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_gate_bias",
                ),
                12, 60,
            ),

            # 5 – Igss Gate Leakage (Individual Runs)
            #     no bias dimension (single-variable sweep)
            (
                "Device Library – Igss Gate Leakage (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
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
        ]

    # 6 – TSP Parameters table (on Individual Runs tab)
    #     Uses baselines_metadata directly; shows instrument settings
    #     per measurement run so users can diagnose disjointed curves.
    if meta_ds:
        individual_chart_defs.append((
            "Device Library – TSP Parameters",
            meta_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "device_type", "manufacturer",
                    "experiment", "device_id",
                    "measurement_type", "measurement_category",
                    "sweep_start", "sweep_stop", "sweep_points",
                    "bias_value", "bias_channel", "drain_bias_value",
                    "compliance_ch1", "compliance_ch2",
                    "meas_time", "hold_time", "plc",
                    "step_num", "step_start", "step_stop",
                    "delay_time",
                ],
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "tsp_path IS NOT NULL",
                        "clause": "WHERE",
                    },
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "device_type IS NOT NULL",
                        "clause": "WHERE",
                    },
                ],
                "row_limit": 5000,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ))

    # ── Create all charts ─────────────────────────────────────────────────
    charts_info = []  # (id, uuid, name, width, height)
    chart_ids_only = []

    for name, ds_id, viz_type, params, width, height in chart_defs:
        cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
        charts_info.append((cid, cuuid, name, width, height))
        if cid:
            chart_ids_only.append(cid)

    # Box plot charts (appended to Mean Curves tab after other charts)
    boxplot_charts_info = []
    boxplot_chart_ids = []
    if boxplot_chart_defs:
        print("\n   Creating box plot charts...")
        for name, ds_id, viz_type, params, width, height in boxplot_chart_defs:
            cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
            charts_info.append((cid, cuuid, name, width, height))
            boxplot_charts_info.append((cid, cuuid, name, width, height))
            if cid:
                chart_ids_only.append(cid)
                boxplot_chart_ids.append(cid)

    sigma_charts_info = []
    sigma_chart_ids = []

    print("\n   Creating ±1σ charts...")
    for name, ds_id, viz_type, params, width, height in sigma_chart_defs:
        cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
        sigma_charts_info.append((cid, cuuid, name, width, height))
        if cid:
            sigma_chart_ids.append(cid)

    indiv_charts_info = []
    indiv_chart_ids = []

    if individual_chart_defs:
        print("\n   Creating Individual Runs charts...")
        for name, ds_id, viz_type, params, width, height in individual_chart_defs:
            cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
            indiv_charts_info.append((cid, cuuid, name, width, height))
            if cid:
                indiv_chart_ids.append(cid)

    all_chart_ids = chart_ids_only + sigma_chart_ids + indiv_chart_ids

    devlib_chart_id = charts_info[7][0]  # registered devices table
    # Calculated params chart is at index 8 if it was created; exclude it
    # from all filters to prevent a failing virtual-dataset SQL from breaking
    # the Manufacturer / Device Type filter dropdowns.
    calc_chart_id = charts_info[8][0] if len(charts_info) > 8 else None
    # always_excluded: charts with no device_type/manufacturer columns
    # (excluded from Manufacturer + Device Type filters)
    always_excluded = [c for c in [devlib_chart_id, calc_chart_id] if c]
    # irrad_excluded: additionally includes box plots whose virtual datasets
    # have no is_likely_irradiated column.
    irrad_excluded = [c for c in [devlib_chart_id, calc_chart_id]
                      + boxplot_chart_ids if c]

    # Collect chart IDs for bias-filter scoping
    # V_Drain bias → IdVg Transfer + Vth (mean, sigma, individual)
    # Individual view now has v_drain_bias column matching the averages view.
    v_drain_ids = [
        charts_info[1][0], charts_info[2][0],           # mean IdVg, Vth
        sigma_charts_info[0][0], sigma_charts_info[1][0], # sigma IdVg, Vth
    ]
    if len(indiv_charts_info) > 5:
        v_drain_ids += [indiv_charts_info[1][0],         # indiv IdVg
                        indiv_charts_info[2][0]]         # indiv Vth
    v_drain_chart_ids = [c for c in v_drain_ids if c]

    # V_Gate bias → IdVd Output + 3rd Quadrant (mean, sigma, individual)
    v_gate_ids = [
        charts_info[3][0], charts_info[4][0],           # mean IdVd, 3rdQ
        sigma_charts_info[2][0], sigma_charts_info[3][0], # sigma IdVd, 3rdQ
    ]
    if len(indiv_charts_info) > 4:
        v_gate_ids += [indiv_charts_info[3][0],          # indiv IdVd
                       indiv_charts_info[4][0]]          # indiv 3rdQ
    v_gate_chart_ids = [c for c in v_gate_ids if c]

    # 5. Build dashboard with native filters (tabbed layout)
    print("\n5. Creating dashboard with native filters (tabbed layout)...")
    position_json = build_dashboard_layout(
        charts_info,
        sigma_charts=sigma_charts_info,
        individual_charts=indiv_charts_info or None,
    )
    native_filters = build_native_filters(
        all_chart_ids, avg_ds,
        always_excluded=always_excluded,
        irrad_excluded=irrad_excluded,
        v_drain_chart_ids=v_drain_chart_ids,
        v_gate_chart_ids=v_gate_chart_ids,
        indiv_ds_id=indiv_ds,
        calc_ds_id=datasheet_ds,
        meta_ds_id=meta_ds,
        boxplot_ds_id=boxplot_ds,
    )
    json_metadata = build_json_metadata(all_chart_ids, native_filters)
    dash_id = create_or_update_dashboard(
        session, "Baselines Device Library", position_json, json_metadata,
        slug="baselines-device-library",
    )

    # 6. Associate charts with dashboard
    print("\n6. Associating charts with dashboard...")
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
        print(f"  URL: {SUPERSET_URL}/superset/dashboard/baselines-device-library/")
        datasheet_chart_count = 1 if datasheet_ds else 0
        bp_count = len(boxplot_chart_ids)
        print(f"  Charts: {len(all_chart_ids)} "
              f"({len(chart_ids_only)} mean"
              f" [{datasheet_chart_count} datasheet, {bp_count} box plots]"
              f" + {len(sigma_chart_ids)} ±1σ"
              f" + {len(indiv_chart_ids)} individual)")
        print("  Tabs:")
        print("    1. Mean Curves      (default, shown on load)")
        print("    2. ±1σ Bands        (click tab to view)")
        print("    3. Individual Runs  (click tab to see every run + TSP params)")
        print("  Filters:")
        print("    1. Manufacturer         (optional, narrows device list)")
        print("    2. Device Type          (required, from device library)")
        print("    3. Measurement Category (required, cascades)")
        print("    4. V_Drain Bias (V)     (toggle biases for IdVg/Vth)")
        print("    5. V_Gate Bias (V)      (toggle biases for IdVd/3rdQ)")
        print("    6. Likely Irradiated    (defaults to false; clear for all data)")
    else:
        print("Dashboard creation failed — see errors above.")
    print("=" * 70)


if __name__ == "__main__":
    main()
