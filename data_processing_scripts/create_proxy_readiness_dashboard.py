#!/usr/bin/env python3
"""Create the Proxy Readiness dashboard.

The dashboard is intentionally conservative: it ranks short-circuit and
avalanche stress events as proxy candidates for irradiation events, then shows
why each candidate is supported, weak, or blocked.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import apply_schema as apply_common_schema
from db_config import SUPERSET_URL, get_connection
from superset_api import (
    build_json_metadata,
    create_chart,
    create_or_update_dashboard,
    find_database,
    find_or_create_dataset,
    get_session,
    refresh_dataset_columns,
)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SCHEMA_PATH = REPO_ROOT / "schema" / "025_proxy_readiness_waveforms.sql"
SINGLE_EVENT_SCHEMA = {"022_irradiation_single_events.sql"}
DASHBOARD_TITLE = "Proxy Readiness - Waveform Failure Phenotypes"
DASHBOARD_SLUG = "proxy-readiness-waveforms"

DATASET_TABLES = {
    "gate_zero": "stress_proxy_gate_zero_view",
    "readiness": "stress_proxy_readiness_view",
    "file_features": "stress_waveform_file_features",
    "event_features": "stress_waveform_event_features",
    "basis_features": "stress_waveform_basis_feature_view",
    "context": "stress_test_context_view",
    "candidates": "stress_proxy_candidate_view",
    "candidate_summary": "stress_proxy_candidate_summary_view",
}

CANDIDATE_COLORS = {
    "measured_damage_candidate": "#1f77b4",
    "predicted_damage_candidate": "#2ca02c",
    "device_run_measured_candidate": "#17becf",
    "weak_measured_candidate": "#bcbd22",
    "waveform_only_candidate": "#ff7f0e",
    "inspect_manually": "#9467bd",
    "missing_damage_context": "#8c564b",
    "phenotype_mismatch": "#d62728",
    "energy_out_of_range": "#7f7f7f",
    "sc": "#4c78a8",
    "avalanche": "#f58518",
}


def apply_proxy_schema() -> None:
    """Rebuild the single-event dependency and the proxy-readiness views."""
    with get_connection() as conn:
        apply_common_schema(conn, include_pipeline=SINGLE_EVENT_SCHEMA)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text())
        conn.commit()


def sql_filter(expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "clause": "WHERE",
    }


def table_params(columns, row_limit=1000, order_by=None, filters=None) -> dict:
    return {
        "query_mode": "raw",
        "all_columns": list(columns),
        "adhoc_filters": list(filters or []),
        "row_limit": row_limit,
        "include_search": True,
        "order_by_cols": [json.dumps(col) for col in (order_by or [])],
        "table_timestamp_format": "smart_date",
        "show_cell_bars": True,
        "color_pn": True,
    }


def metric(label: str, sql_expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expression,
        "label": label,
    }


def scatter_params(x_col: str, y_col: str, x_label: str, y_label: str,
                   groupby=None, filters=None) -> dict:
    return {
        "x_axis": x_col,
        "time_grain_sqla": None,
        "x_axis_sort_asc": True,
        "metrics": [metric(y_label, f"AVG({y_col})")],
        "groupby": list(groupby or [
            "candidate_source",
            "candidate_status",
            "damage_evidence_tier",
            "target_stress_record_key",
            "candidate_stress_record_key",
        ]),
        "adhoc_filters": [
            sql_filter(f"{x_col} IS NOT NULL AND {y_col} IS NOT NULL"),
            *(filters or []),
        ],
        "row_limit": 10000,
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
        "markerSize": 7,
        "zoomable": True,
        "label_colors": CANDIDATE_COLORS,
    }


def build_dashboard_layout(charts):
    """Build a simple full-width dashboard layout."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
    }
    row_children = []
    for i, (cid, cuuid, cname, width, height) in enumerate(charts):
        if cid is None:
            continue
        row_id = f"ROW-proxy-{i}"
        chart_key = f"CHART-proxy-{i}"
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        layout[chart_key] = {
            "type": "CHART",
            "id": chart_key,
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
        row_children.append(row_id)
    layout["GRID_ID"]["children"] = row_children
    return layout


def select_filter(filter_id: str, name: str, dataset_id: int, column: str,
                  scoped_chart_ids, all_chart_ids, parent_ids=None) -> dict:
    scoped_chart_ids = list(scoped_chart_ids)
    all_chart_ids = list(all_chart_ids)
    return {
        "id": filter_id,
        "controlValues": {
            "enableEmptyFilter": True,
            "defaultToFirstItem": False,
            "multiSelect": True,
            "searchAllOptions": True,
            "inverseSelection": False,
        },
        "name": name,
        "filterType": "filter_select",
        "targets": [{"datasetId": dataset_id, "column": {"name": column}}],
        "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
        "cascadeParentIds": list(parent_ids or []),
        "scope": {
            "rootPath": ["ROOT_ID"],
            "excluded": [cid for cid in all_chart_ids if cid not in scoped_chart_ids],
        },
        "type": "NATIVE_FILTER",
        "description": name,
        "chartsInScope": scoped_chart_ids,
        "tabsInScope": [],
    }


def build_native_filters(all_chart_ids, dataset_ids, chart_groups):
    candidate_ids = chart_groups["candidate"]
    context_ids = chart_groups["context"]
    all_ids = list(all_chart_ids)

    device_filter_id = "NATIVE_FILTER-proxy-device"
    return [
        select_filter(
            device_filter_id,
            "Device Type",
            dataset_ids["candidates"],
            "device_type",
            candidate_ids + context_ids + chart_groups["readiness"],
            all_ids,
        ),
        select_filter(
            "NATIVE_FILTER-proxy-target-event",
            "Target Event",
            dataset_ids["candidates"],
            "target_event_type",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-candidate-source",
            "Candidate Source",
            dataset_ids["candidates"],
            "candidate_source",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-status",
            "Candidate Status",
            dataset_ids["candidates"],
            "candidate_status",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-confidence",
            "Replacement Confidence",
            dataset_ids["candidates"],
            "replacement_confidence",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-evidence-tier",
            "Evidence Tier",
            dataset_ids["candidates"],
            "damage_evidence_tier",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-target-regime",
            "Target Regime",
            dataset_ids["candidates"],
            "target_stress_regime",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-context-source",
            "Context Source",
            dataset_ids["context"],
            "source",
            context_ids,
            all_ids,
        ),
        select_filter(
            "NATIVE_FILTER-proxy-context-regime",
            "Context Regime",
            dataset_ids["context"],
            "stress_regime",
            context_ids,
            all_ids,
        ),
    ]


def register_datasets(session, db_id: int) -> dict:
    dataset_ids = {}
    for key, table_name in DATASET_TABLES.items():
        ds_id = find_or_create_dataset(session, db_id, table_name)
        if ds_id is None:
            raise RuntimeError(f"Could not create or find Superset dataset {table_name}")
        refresh_dataset_columns(session, ds_id)
        dataset_ids[key] = ds_id
    return dataset_ids


def build_chart_defs(dataset_ids):
    gate_cols = [
        "gate_zero_status",
        "gate_zero_pass",
        "candidate_device_families",
        "device_families_with_sc_waveforms",
        "device_families_with_uid_uis_waveforms",
        "device_families_with_irradiation_waveforms_or_events",
        "device_families_with_electrical_proxy_post_iv_overlap",
        "device_families_with_irradiation_post_iv_overlap",
        "sc_waveform_files",
        "uid_uis_waveform_files",
        "irradiation_events",
        "post_iv_damage_fingerprints",
        "candidate_device_types",
    ]
    readiness_cols = [
        "device_type_label",
        "proxy_readiness_status",
        "gate_zero_candidate",
        "sc_waveform_files",
        "avalanche_waveform_files",
        "uid_uis_waveform_files",
        "irradiation_waveform_files",
        "irradiation_events",
        "seb_events",
        "selc_i_events",
        "selc_ii_events",
        "electrical_proxy_waveform_plus_post_iv_files",
        "irradiation_events_with_waveform_plus_post_iv",
        "comparable_damage_axis_count",
    ]
    summary_cols = [
        "candidate_source",
        "target_event_type",
        "target_path_type",
        "candidate_status",
        "replacement_confidence",
        "top_target_events",
        "device_type_count",
        "measured_damage_top_events",
        "predicted_damage_top_events",
        "waveform_only_top_events",
        "median_combined_screening_distance",
        "median_waveform_distance",
        "median_damage_distance",
        "device_types",
    ]
    candidate_cols = [
        "candidate_rank",
        "device_type",
        "target_event_type",
        "target_path_type",
        "target_irrad_run_id",
        "target_ion_species",
        "target_beam_energy_mev",
        "target_let_surface",
        "candidate_source",
        "candidate_stress_condition_label",
        "candidate_event_type",
        "candidate_sc_voltage_v",
        "candidate_sc_duration_us",
        "candidate_avalanche_mode",
        "candidate_avalanche_outcome",
        "target_energy_j",
        "candidate_energy_j",
        "log_energy_delta",
        "phenotype_distance",
        "waveform_distance",
        "best_damage_distance",
        "combined_screening_distance",
        "damage_evidence_tier",
        "measured_comparability_status",
        "measured_match_scope",
        "prediction_comparability_status",
        "candidate_status",
        "replacement_confidence",
        "candidate_blockers",
        "target_stress_record_key",
        "candidate_stress_record_key",
    ]
    evidence_cols = [
        "candidate_rank",
        "device_type",
        "target_stress_record_key",
        "candidate_stress_record_key",
        "target_stress_regime",
        "target_radiation_mechanism_class",
        "target_application_likeness",
        "candidate_stress_regime",
        "candidate_application_likeness",
        "target_vds_collapse_fraction",
        "candidate_vds_collapse_fraction",
        "collapse_delta",
        "target_gate_delta_fraction",
        "candidate_gate_delta_fraction",
        "gate_delta",
        "duration_log_delta",
        "path_penalty",
        "measured_comparable_axes",
        "measured_comparable_axis_labels",
        "measured_match_scope",
        "prediction_model_version",
        "prediction_reference_tier",
        "prediction_validation_mode",
        "prediction_comparable_axes",
        "prediction_fingerprint_confidence",
        "prediction_validation_gate_pass_all",
    ]
    context_cols = [
        "source",
        "stress_record_key",
        "device_type",
        "filename",
        "event_type",
        "path_type",
        "stress_regime",
        "soa_relation",
        "test_method_class",
        "test_timescale_class",
        "radiation_mechanism_class",
        "response_reversibility",
        "application_likeness",
        "stress_energy_j",
        "stress_energy_basis",
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "normalized_vds",
        "normalized_current",
        "post_iv_axis_count",
        "context_flags",
    ]
    event_cols = [
        "source",
        "event_record_type",
        "device_type",
        "filename",
        "event_type",
        "path_type",
        "event_energy_vds_id_j",
        "event_energy_proxy_j",
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "post_iv_axis_count",
        "match_basis_class",
        "readiness_status",
        "quality_flags",
    ]

    top_rank_filter = sql_filter("candidate_rank = 1")
    top_ten_filter = sql_filter("candidate_rank <= 10")

    return [
        (
            "Proxy Readiness - Gate Zero Status",
            dataset_ids["gate_zero"],
            "table",
            table_params(gate_cols, row_limit=10),
            12,
            26,
        ),
        (
            "Proxy Readiness - Device Coverage",
            dataset_ids["readiness"],
            "table",
            table_params(
                readiness_cols,
                row_limit=200,
                order_by=[["gate_zero_candidate", False], ["device_type_label", True]],
            ),
            12,
            42,
        ),
        (
            "Proxy Readiness - Candidate Summary",
            dataset_ids["candidate_summary"],
            "table",
            table_params(
                summary_cols,
                row_limit=200,
                order_by=[["top_target_events", False]],
            ),
            12,
            42,
        ),
        (
            "Proxy Readiness - Best Proxy Candidates",
            dataset_ids["candidates"],
            "table",
            table_params(
                candidate_cols,
                row_limit=1000,
                order_by=[
                    ["candidate_status_priority", True],
                    ["combined_screening_distance", True],
                ],
                filters=[top_rank_filter],
            ),
            12,
            70,
        ),
        (
            "Proxy Readiness - Candidate Evidence Detail",
            dataset_ids["candidates"],
            "table",
            table_params(
                evidence_cols,
                row_limit=2500,
                order_by=[
                    ["target_stress_record_key", True],
                    ["candidate_rank", True],
                ],
                filters=[top_ten_filter],
            ),
            12,
            60,
        ),
        (
            "Proxy Readiness - Energy vs Phenotype Distance",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "log_energy_delta",
                "phenotype_distance",
                "|log(candidate energy / irradiation energy)|",
                "Phenotype distance",
                filters=[top_ten_filter],
            ),
            12,
            52,
        ),
        (
            "Proxy Readiness - Waveform vs Damage Distance",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "waveform_distance",
                "best_damage_distance",
                "Waveform distance",
                "Best damage distance",
                filters=[top_ten_filter],
            ),
            12,
            52,
        ),
        (
            "Proxy Readiness - Stress Test Context",
            dataset_ids["context"],
            "table",
            table_params(
                context_cols,
                row_limit=2500,
                order_by=[["source", True], ["device_type", True]],
            ),
            12,
            64,
        ),
        (
            "Proxy Readiness - Event Feature Coverage",
            dataset_ids["event_features"],
            "table",
            table_params(
                event_cols,
                row_limit=2500,
                order_by=[["source", True], ["device_type", True]],
            ),
            12,
            54,
        ),
    ]


def create_dashboard() -> int | None:
    session = get_session()
    db_id = find_database(session)
    if db_id is None:
        raise RuntimeError("Could not locate the Superset database")

    print("\nRegistering proxy-readiness datasets...")
    dataset_ids = register_datasets(session, db_id)

    print("\nCreating proxy-readiness charts...")
    charts_info = []
    chart_ids = []
    chart_id_by_name = {}
    for name, ds_id, viz_type, params, width, height in build_chart_defs(dataset_ids):
        cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
        charts_info.append((cid, cuuid, name, width, height))
        if cid:
            chart_ids.append(cid)
            chart_id_by_name[name] = cid

    candidate_chart_names = {
        "Proxy Readiness - Best Proxy Candidates",
        "Proxy Readiness - Candidate Evidence Detail",
        "Proxy Readiness - Energy vs Phenotype Distance",
        "Proxy Readiness - Waveform vs Damage Distance",
    }
    context_chart_names = {"Proxy Readiness - Stress Test Context"}
    readiness_chart_names = {"Proxy Readiness - Device Coverage"}
    chart_groups = {
        "candidate": [chart_id_by_name[n] for n in candidate_chart_names if n in chart_id_by_name],
        "context": [chart_id_by_name[n] for n in context_chart_names if n in chart_id_by_name],
        "readiness": [chart_id_by_name[n] for n in readiness_chart_names if n in chart_id_by_name],
    }

    print("\nBuilding proxy-readiness dashboard layout...")
    position_json = build_dashboard_layout(charts_info)
    native_filters = build_native_filters(chart_ids, dataset_ids, chart_groups)
    json_metadata = build_json_metadata(chart_ids, native_filters)
    json_metadata["label_colors"] = CANDIDATE_COLORS
    json_metadata["shared_label_colors"] = CANDIDATE_COLORS

    dash_id = create_or_update_dashboard(
        session,
        DASHBOARD_TITLE,
        position_json,
        json_metadata,
        slug=DASHBOARD_SLUG,
    )

    if dash_id:
        print("\nAssociating charts with dashboard...")
        for cid in chart_ids:
            resp = session.put(
                f"{SUPERSET_URL}/api/v1/chart/{cid}",
                json={"dashboards": [dash_id]},
            )
            status = "OK" if resp.ok else f"FAIL ({resp.status_code})"
            print(f"  Chart {cid} -> dashboard {dash_id}: {status}")

    print("\n" + "=" * 70)
    if dash_id:
        print("Dashboard ready")
        print(f"  URL: {SUPERSET_URL}/superset/dashboard/{DASHBOARD_SLUG}/")
        print(f"  Charts: {len(chart_ids)}")
        print("  Primary views: stress_test_context_view, stress_proxy_candidate_view")
    else:
        print("Dashboard update did not return an id")
    return dash_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Rebuild database views and exit before touching Superset.",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Use existing database views and only update Superset metadata.",
    )
    args = parser.parse_args()

    if not args.skip_schema:
        print("Rebuilding proxy-readiness SQL views...")
        apply_proxy_schema()
        print("Proxy-readiness SQL views rebuilt")

    if args.schema_only:
        return

    create_dashboard()


if __name__ == "__main__":
    main()
