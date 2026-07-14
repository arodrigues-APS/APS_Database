#!/usr/bin/env python3
"""Create the validation-oriented CV and double-pulse characterization dashboard.

The legacy CV and DPT dashboards exposed raw tables but did not state units,
denominators, or the provenance of derived energy.  This generator uses the
audited views in ``schema/030_dynamic_characterization.sql`` and deliberately
keeps capacitance families and waveform quantities on separate physical axes.
"""

from __future__ import annotations

import argparse

from aps.config import get_settings
from aps.db_config import SUPERSET_URL, get_connection
from aps.paths import REPO_ROOT
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


SCHEMA_PATH = REPO_ROOT / "schema" / "030_dynamic_characterization.sql"
DASHBOARD_TITLE = "CV & Double-Pulse Characterization"
DASHBOARD_SLUG = "cv-dpt-characterization"
LEGACY_SOURCE_TABLES = ("public.cpvd", "public.dptgraphs", "public.dptslopes")

DATASETS = {
    "cv": "cv_characterization_view",
    "dpt": "dpt_characterization_view",
    "metrics": "dpt_switching_metric_view",
}

TABS = {
    "cv": ("CV Characteristics", "TAB-cvdpt-cv"),
    "waveforms": ("DPT Waveforms & Energy", "TAB-cvdpt-waveforms"),
    "metrics": ("DPT Switching Metrics", "TAB-cvdpt-metrics"),
    "qa": ("Raw / QA", "TAB-cvdpt-qa"),
}

GUIDANCE = {
    TABS["cv"][1]: (
        "### Capacitance–voltage evidence\n\n"
        "Values are measured parallel capacitances in **farads**. The source "
        "labels `cds`, `cgd`, `cgg`, and `cgs` are preserved; they are not "
        "silently relabeled as datasheet Ciss/Coss/Crss. Select a device and "
        "sample before comparing curve shape."
    ),
    TABS["waveforms"][1]: (
        "### Waveform and energy contract\n\n"
        "Time is **µs**, recovered from the original ingestion ×1e6 conversion. "
        "Power is instantaneous drain-terminal $V_{DS}I_D$. Cumulative energy "
        "is a trapezoidal integral of positive power over the imported capture "
        "window; it is a diagnostic and is **not** curated Eon or Eoff. Select "
        "one device, sample, and capture before interpreting a transient."
    ),
    TABS["metrics"][1]: (
        "### Legacy switching metrics\n\n"
        "Slew values use the historical 10–90% extraction and are displayed "
        "as magnitudes in V/µs and A/µs. Signed values remain in Raw / QA. "
        "Each point is one capture, not an independent device population."
    ),
    TABS["qa"][1]: (
        "### Provenance and QA\n\n"
        "These tables retain capture keys, nominal conditions, signed metrics, "
        "and derivation labels. Missing data remains missing; no Eon/Eoff windows "
        "or datasheet capacitance identities are inferred."
    ),
}

DESCRIPTIONS = {
    "CV/DPT – CV Coverage": "One row per device/sample/capacitance family. Counts are stored measured points and distinct source tables.",
    "CV/DPT – Capacitance vs Drain Bias": "Measured parallel capacitance in F versus drain bias in V. Log-y preserves orders of magnitude; traces do not bridge nulls.",
    "CV/DPT – Capacitance Range": "Per capacitance family min/median/max measured capacitance in F and supporting point count.",
    "CV/DPT – DPT Condition Coverage": "One row per nominal device/sample/temperature/bus/current condition with distinct capture and waveform-point counts.",
    "CV/DPT – Drain Voltage Waveform": "Measured VDS in V versus relative capture time in µs. Select one capture; aggregation across unrelated captures is not physically meaningful.",
    "CV/DPT – Drain Current Waveform": "Measured ID in A versus relative capture time in µs. Select one capture; aggregation across unrelated captures is not physically meaningful.",
    "CV/DPT – Gate Voltage Waveform": "Measured VGS in V versus relative capture time in µs. Select one capture; aggregation across unrelated captures is not physically meaningful.",
    "CV/DPT – Positive Power Waveform": "Positive part of measured VDS×ID in W versus relative time. This is a terminal-power diagnostic, not switching loss by itself.",
    "CV/DPT – Cumulative Positive Energy": "Trapezoidal positive VDS×ID integral in J over the full imported window. It is not Eon/Eoff because switching windows are not curated.",
    "CV/DPT – dv/dt vs Temperature": "Legacy 10–90% |dv/dt| in V/µs per capture versus temperature; turn-on and turn-off are separate series.",
    "CV/DPT – di/dt vs Temperature": "Legacy 10–90% |di/dt| in A/µs per capture versus temperature; turn-on and turn-off are separate series.",
    "CV/DPT – Switching Metric Detail": "One row per legacy slope record with signed and magnitude metrics, nominal conditions, and extraction method.",
    "CV/DPT – CV Raw Detail": "Forensic CV rows with measured farads, source capacitance label, device/sample provenance, and drain bias.",
    "CV/DPT – DPT Capture Detail": "Forensic DPT capture summary retaining source key, sample, nominal condition, point count, time span, and final diagnostic energy.",
}


def assert_legacy_source_tables(cur) -> None:
    """Fail closed when the historical CV/DPT snapshot is unavailable.

    The views in 030 are temporary compatibility views over aggregate tables
    created by a removed, destructive ingestion path. Keeping this preflight
    adjacent to the builder prevents a fresh database from receiving a
    misleading dashboard or an opaque PostgreSQL relation-not-found error.
    """
    missing = []
    for table_name in LEGACY_SOURCE_TABLES:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        if cur.fetchone()[0] is None:
            missing.append(table_name)
    if missing:
        raise RuntimeError(
            "CV/DPT dashboard is backed by a legacy database snapshot and "
            "cannot run because these source tables are absent: "
            + ", ".join(missing)
            + ". Do not recreate them with DatabaseScript.py; restore the "
            "documented legacy snapshot or use the forthcoming canonical importer."
        )


def verify_legacy_source_tables() -> None:
    """Verify legacy inputs before dashboard presentation uses their views."""
    get_settings().require_legacy_cv_dpt_enabled()
    with get_connection() as conn, conn.cursor() as cur:
        assert_legacy_source_tables(cur)


def verify_legacy_model_ready() -> None:
    """Require both the frozen inputs and the explicitly prepared model views."""
    get_settings().require_legacy_cv_dpt_enabled()
    with get_connection() as conn, conn.cursor() as cur:
        assert_legacy_source_tables(cur)
        missing = []
        for view_name in DATASETS.values():
            cur.execute("SELECT to_regclass(%s)", (view_name,))
            if cur.fetchone()[0] is None:
                missing.append(view_name)
        if missing:
            raise RuntimeError(
                "CV/DPT presentation model is not prepared (missing: "
                + ", ".join(missing)
                + "). Run aps models build legacy-cv-dpt first."
            )


def metric(label: str, expression: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def table(columns: list[str], *, row_limit: int = 1000, metrics=None,
          groupby=None) -> dict:
    if metrics:
        return {
            "query_mode": "aggregate", "groupby": list(groupby or []),
            "metrics": metrics, "adhoc_filters": [], "row_limit": row_limit,
            "include_search": True, "table_timestamp_format": "smart_date",
        }
    return {
        "query_mode": "raw", "all_columns": columns, "adhoc_filters": [],
        "row_limit": row_limit, "include_search": True,
        "table_timestamp_format": "smart_date",
    }


def line(x: str, y: str, y_label: str, *, groupby: list[str], log_y=False,
         row_limit=50000) -> dict:
    return {
        "x_axis": x,
        "metrics": [metric(y_label, f"AVG({y})")],
        "groupby": groupby,
        "adhoc_filters": [{
            "expressionType": "SQL",
            "sqlExpression": f"{x} IS NOT NULL AND {y} IS NOT NULL",
            "clause": "WHERE",
        }],
        "row_limit": row_limit,
        "show_legend": True,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": x.replace("_", " "),
        "y_axis_title": y_label,
        "x_axis_title_margin": 30,
        "y_axis_title_margin": 30,
        "y_axis_format": ".3g",
        "logAxis": bool(log_y),
        "connectNulls": False,
        "markerEnabled": False,
        "zoomable": True,
    }


def chart_definitions(ids: dict[str, int]) -> list[dict]:
    cv_group = ["device_type", "sample_id", "capacitance_type", "source_table"]
    wave_group = ["capture_key"]
    slope_group = ["device_type", "sample_id", "nominal_bus_voltage_v", "nominal_drain_current_a"]
    return [
        dict(name="CV/DPT – CV Coverage", ds="cv", tab="cv", viz="table", width=12, height=34,
             params=table([], metrics=[metric("points", "COUNT(*)"), metric("source tables", "COUNT(DISTINCT source_table)")], groupby=["device_type", "sample_id", "capacitance_type"])),
        dict(name="CV/DPT – Capacitance vs Drain Bias", ds="cv", tab="cv", viz="echarts_timeseries_line", width=8, height=54,
             params=line("drain_bias_v", "capacitance_f", "capacitance (F)", groupby=cv_group, log_y=True)),
        dict(name="CV/DPT – Capacitance Range", ds="cv", tab="cv", viz="table", width=4, height=54,
             params=table([], metrics=[metric("minimum F", "MIN(capacitance_f)"), metric("median F", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacitance_f)"), metric("maximum F", "MAX(capacitance_f)"), metric("points", "COUNT(*)")], groupby=["device_type", "capacitance_type"])),
        dict(name="CV/DPT – DPT Condition Coverage", ds="dpt", tab="waveforms", viz="table", width=12, height=34,
             params=table([], metrics=[metric("captures", "COUNT(DISTINCT capture_key)"), metric("waveform points", "COUNT(*)")], groupby=["device_type", "sample_id", "temperature_c", "nominal_bus_voltage_v", "nominal_drain_current_a"])),
        dict(name="CV/DPT – Drain Voltage Waveform", ds="dpt", tab="waveforms", viz="echarts_timeseries_line", width=6, height=45,
             params=line("time_relative_us", "drain_voltage_v", "VDS (V)", groupby=wave_group)),
        dict(name="CV/DPT – Drain Current Waveform", ds="dpt", tab="waveforms", viz="echarts_timeseries_line", width=6, height=45,
             params=line("time_relative_us", "drain_current_a", "ID (A)", groupby=wave_group)),
        dict(name="CV/DPT – Gate Voltage Waveform", ds="dpt", tab="waveforms", viz="echarts_timeseries_line", width=6, height=45,
             params=line("time_relative_us", "gate_voltage_v", "VGS (V)", groupby=wave_group)),
        dict(name="CV/DPT – Positive Power Waveform", ds="dpt", tab="waveforms", viz="echarts_timeseries_line", width=6, height=45,
             params=line("time_relative_us", "positive_power_w", "positive power (W)", groupby=wave_group)),
        dict(name="CV/DPT – Cumulative Positive Energy", ds="dpt", tab="waveforms", viz="echarts_timeseries_line", width=12, height=45,
             params=line("time_relative_us", "cumulative_positive_energy_j", "cumulative positive energy (J)", groupby=wave_group)),
        dict(name="CV/DPT – dv/dt vs Temperature", ds="metrics", tab="metrics", viz="echarts_timeseries_line", width=6, height=48,
             params={**line("temperature_c", "turn_off_dv_dt_magnitude_v_per_us", "|dv/dt| (V/µs)", groupby=slope_group), "metrics": [metric("turn-off |dv/dt|", "AVG(turn_off_dv_dt_magnitude_v_per_us)"), metric("turn-on |dv/dt|", "AVG(turn_on_dv_dt_magnitude_v_per_us)")]}),
        dict(name="CV/DPT – di/dt vs Temperature", ds="metrics", tab="metrics", viz="echarts_timeseries_line", width=6, height=48,
             params={**line("temperature_c", "turn_off_di_dt_magnitude_a_per_us", "|di/dt| (A/µs)", groupby=slope_group), "metrics": [metric("turn-off |di/dt|", "AVG(turn_off_di_dt_magnitude_a_per_us)"), metric("turn-on |di/dt|", "AVG(turn_on_di_dt_magnitude_a_per_us)")]}),
        dict(name="CV/DPT – Switching Metric Detail", ds="metrics", tab="qa", viz="table", width=12, height=48,
             params=table(["device_type", "sample_id", "capture_key", "temperature_c", "nominal_bus_voltage_v", "nominal_drain_current_a", "turn_off_dv_dt_v_per_us", "turn_on_dv_dt_v_per_us", "turn_off_di_dt_a_per_us", "turn_on_di_dt_a_per_us", "extraction_method"])),
        dict(name="CV/DPT – CV Raw Detail", ds="cv", tab="qa", viz="table", width=6, height=48,
             params=table(["device_type", "sample_id", "source_table", "temperature_c", "capacitance_type", "drain_bias_v", "capacitance_f"])),
        dict(name="CV/DPT – DPT Capture Detail", ds="dpt", tab="qa", viz="table", width=6, height=48,
             params=table([], metrics=[metric("points", "COUNT(*)"), metric("window µs", "MAX(time_relative_us)"), metric("final positive energy J", "MAX(cumulative_positive_energy_j)")], groupby=["device_type", "sample_id", "capture_key", "temperature_c", "nominal_bus_voltage_v", "nominal_drain_current_a", "energy_window_basis", "time_unit_provenance"])),
    ]


def _select_filter(fid: str, name: str, targets: list[tuple[int, str]],
                   scoped: list[int], all_ids: list[int], parents=None,
                   *, default_first=False) -> dict:
    return {
        "id": fid,
        "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": default_first,
                          "multiSelect": not default_first, "searchAllOptions": True,
                          "inverseSelection": False},
        "name": name, "filterType": "filter_select",
        "targets": [{"datasetId": ds, "column": {"name": col}} for ds, col in targets],
        "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
        "cascadeParentIds": list(parents or []),
        "scope": {"rootPath": ["ROOT_ID"], "excluded": [cid for cid in all_ids if cid not in scoped]},
        "type": "NATIVE_FILTER", "description": name,
        "chartsInScope": scoped, "tabsInScope": [],
    }


def native_filters(catalog: list[dict], ids: dict[str, int]) -> list[dict]:
    all_ids = [row["chart_id"] for row in catalog]

    def scope(keys):
        return [row["chart_id"] for row in catalog if row["ds"] in keys]

    device = "NATIVE_FILTER-cvdpt-device"
    sample = "NATIVE_FILTER-cvdpt-sample"
    result = [
        _select_filter(device, "Device Type", [(ids[k], "device_type") for k in ids], all_ids, all_ids, default_first=True),
        _select_filter(sample, "Sample", [(ids[k], "sample_id") for k in ids], all_ids, all_ids, [device], default_first=True),
        _select_filter("NATIVE_FILTER-cvdpt-temperature", "Temperature (°C)", [(ids[k], "temperature_c") for k in ids], all_ids, all_ids, [device, sample]),
        _select_filter("NATIVE_FILTER-cvdpt-capacitance", "Capacitance Family", [(ids["cv"], "capacitance_type")], scope({"cv"}), all_ids, [device, sample]),
        _select_filter("NATIVE_FILTER-cvdpt-capture", "DPT Capture", [(ids["dpt"], "capture_key"), (ids["metrics"], "capture_key")], scope({"dpt", "metrics"}), all_ids, [device, sample], default_first=True),
        _select_filter("NATIVE_FILTER-cvdpt-bus", "Nominal Bus Voltage (V)", [(ids["dpt"], "nominal_bus_voltage_v"), (ids["metrics"], "nominal_bus_voltage_v")], scope({"dpt", "metrics"}), all_ids, [device, sample]),
        _select_filter("NATIVE_FILTER-cvdpt-current", "Nominal Drain Current (A)", [(ids["dpt"], "nominal_drain_current_a"), (ids["metrics"], "nominal_drain_current_a")], scope({"dpt", "metrics"}), all_ids, [device, sample]),
    ]
    return result


def create_dashboard() -> int | None:
    session = get_session()
    db_id = find_database(session)
    if db_id is None:
        raise RuntimeError("Could not locate the Superset database")
    ids = {}
    for key, table_name in DATASETS.items():
        ds_id = find_or_create_dataset(session, db_id, table_name)
        if ds_id is None:
            raise RuntimeError(f"Could not register {table_name}")
        refresh_dataset_columns(session, ds_id)
        ids[key] = ds_id

    deployed = []
    tab_charts = {key: [] for key in TABS}
    for definition in chart_definitions(ids):
        cid, cuuid = create_chart(
            session, definition["name"], ids[definition["ds"]], definition["viz"],
            definition["params"], description=DESCRIPTIONS[definition["name"]],
        )
        tab_charts[definition["tab"]].append((cid, cuuid, definition["name"], definition["width"], definition["height"]))
        if cid:
            deployed.append({**definition, "chart_id": cid})

    tab_defs = [(TABS[key][0], TABS[key][1], tab_charts[key]) for key in TABS]
    layout = build_tabbed_layout(DASHBOARD_TITLE, "cvdpt", tab_defs, GUIDANCE)
    chart_ids = [row["chart_id"] for row in deployed]
    metadata = build_json_metadata(chart_ids, native_filters(deployed, ids))
    metadata["cross_filters_enabled"] = False
    metadata["chart_configuration"] = {}
    dash_id = create_or_update_dashboard(session, DASHBOARD_TITLE, layout, metadata, slug=DASHBOARD_SLUG)
    if dash_id:
        for cid in chart_ids:
            response = session.put(f"{SUPERSET_URL}/api/v1/chart/{cid}", json={"dashboards": [dash_id]})
            if not response.ok:
                raise RuntimeError(f"Could not associate chart {cid}: {response.status_code}")
    return dash_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema-only", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--skip-schema", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    verify_legacy_model_ready()
    if args.schema_only:
        parser.error(
            "database model builds no longer belong to dashboards; run "
            "aps models build legacy-cv-dpt"
        )
    if args.skip_schema:
        print("--skip-schema is obsolete; dashboards now always consume prepared models.")
    dash_id = create_dashboard()
    print(f"Dashboard ready: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/ (id={dash_id})")


if __name__ == "__main__":
    main()
