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
PIPELINE_SCHEMAS = {
    "022_irradiation_single_events.sql",
    "027_radiation_stress_dose.sql",
}
DASHBOARD_TITLE = "Proxy Readiness - Waveform Failure Phenotypes"
DASHBOARD_SLUG = "proxy-readiness-waveforms"

DATASET_TABLES = {
    "gate_zero": "stress_proxy_gate_zero_view",
    "readiness": "stress_proxy_readiness_view",
    "file_features": "stress_waveform_file_features",
    "event_features": "stress_waveform_event_features",
    "basis_features": "stress_waveform_basis_feature_view",
    "context": "stress_test_context_view",
    "destruction_boundary": "stress_destruction_boundary_view",
    "candidates": "stress_proxy_candidate_view",
    "candidate_summary": "stress_proxy_candidate_summary_view",
    "experiment_plan": "stress_proxy_experiment_plan_view",
}

CANDIDATE_COLORS = {
    "measured_damage_candidate": "#1f77b4",
    "predicted_damage_candidate": "#2ca02c",
    "device_run_measured_candidate": "#17becf",
    "weak_measured_candidate": "#bcbd22",
    "analog_questionable": "#8c6d31",
    "waveform_only_candidate": "#ff7f0e",
    "cross_device_screening_only": "#9edae5",
    "inspect_manually": "#9467bd",
    "missing_damage_context": "#8c564b",
    "missing_phenotype_overlap": "#6b6ecf",
    "phenotype_mismatch": "#d62728",
    "energy_out_of_range": "#7f7f7f",
    "sc": "#4c78a8",
    "avalanche": "#f58518",
    "irradiation": "#54a24b",
    "robustness": "#d62728",
    "reliability": "#1f77b4",
    "radiation": "#54a24b",
    "unknown": "#9d755d",
    "SEB": "#54a24b",
    "SELCI": "#e45756",
    "SELCII": "#72b7b2",
    "MIXED": "#b279a2",
    "UNKNOWN": "#9d755d",
    "energy_comparable": "#1f77b4",
    "energy_censored_phenotype_only": "#9467bd",
    "thermal_runaway_pair": "#2ca02c",
    "thermal_runaway_pair_secondary": "#17becf",
    "gate_oxide_pair_repetitive_only": "#bcbd22",
    "cumulative_defect_no_electrical_analog": "#8c6d31",
    # Fixed LET bands (MeV*cm2/mg) on a heat ramp: hotter = higher LET.
    # New beams fall into an existing band, so this list never grows.
    "LET 00-05": "#fec44f",
    "LET 05-15": "#fe9929",
    "LET 15-25": "#ec7014",
    "LET 25-50": "#cc4c02",
    "LET 50-80": "#993404",
    "LET 80+": "#662506",
    "LET n/a": "#969696",
}

FIGURE1B_LANDSCAPE_DESCRIPTION = (
    "Recreates Kozak et al. IEEE TPEL 2023 Figure 1(b) with database "
    "stress records: normalized VDS is the stimulus-severity axis and "
    "stress/measurement window duration is the log timescale axis. "
    "Irradiation durations are detected event or file measurement windows, "
    "not physical ion-strike durations. Avalanche normalized_vds > 1.60 "
    "rows are excluded by quality flag; the empty region above 1000 h is "
    "the intended reliability-coverage gap."
)
FIGURE1B_DESTRUCTIVE_DESCRIPTION = (
    "Empirical destruction-limit markers for Figure 1(b). Current plottable "
    "destructive rows are irradiation SEB records only; SC and avalanche rows "
    "are not relabeled destructive without explicit outcome data."
)
FIGURE1B_BOUNDARY_DESCRIPTION = (
    "Per-device empirical destruction-boundary rollup for Figure 1(b). "
    "Survived means not classified destructive, so unknown-outcome rows are "
    "included and the boundary is a lower-bound estimate."
)

# Superset's annotation-layer schema requires showMarkers/hideLine even for
# FORMULA layers; the remaining keys mirror what the explore UI saves.
FIGURE1B_REFERENCE_LINES = [
    {
        "annotationType": "FORMULA",
        "sourceType": "",
        "name": "Acceptable test time: 1000 h",
        "value": "3.6e6",
        "style": "dashed",
        "color": "#7f7f7f",
        "opacity": "",
        "width": 1,
        "show": True,
        "showLabel": True,
        "showMarkers": False,
        "hideLine": False,
        "overrides": {"time_range": None},
    },
    {
        "annotationType": "FORMULA",
        "sourceType": "",
        "name": "Specified lifetime: 15 y",
        "value": "4.73e8",
        "style": "dashed",
        "color": "#4c78a8",
        "opacity": "",
        "width": 1,
        "show": True,
        "showLabel": True,
        "showMarkers": False,
        "hideLine": False,
        "overrides": {"time_range": None},
    },
]
# Lower bound sits just below the fastest recorded event (0.25 us) so the
# data keeps half the plot; upper bound keeps the 15 y reference visible.
FIGURE1B_Y_BOUNDS = [1e-7, 1e9]
FIGURE1B_DESTRUCTIVE_Y_BOUNDS = [1e-2, 1e3]
FIGURE1B_X_BOUNDS = [0.0, 1.7]
# d3 trimmed scientific notation; SMART_NUMBER renders wide log ranges as
# "100p"/"1B", which is unreadable on time and ratio axes.
SCI_AXIS_FORMAT = "~e"

# Known probe/unit-scaling artifact family (676 rows); excluded from display,
# never rescaled. Matches the context_flags quality limit.
AVALANCHE_NVDS_ARTIFACT_EXCLUSION = (
    "NOT (source = 'avalanche' AND normalized_vds > 1.60)"
)

AMPLIFICATION_DESCRIPTION = (
    "Energy amplification of irradiation single events: terminal electrical "
    "energy released in the event window divided by the ion's deposited "
    "energy (electronic component). Ratios of 1e6-1e10 show the ion acts "
    "only as a trigger; the destructive energy is supplied by the blocking "
    "bias circuit and device output capacitance. Rows without a positive "
    "deposited-energy estimate are excluded."
)


def apply_proxy_schema() -> None:
    """Rebuild the single-event dependency and the proxy-readiness views."""
    with get_connection() as conn:
        apply_common_schema(conn, include_pipeline=PIPELINE_SCHEMAS)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text())
        conn.commit()


def sql_filter(expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "clause": "WHERE",
    }


def table_params(columns, row_limit=1000, order_by=None, filters=None,
                 description=None) -> dict:
    params = {
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
    if description is not None:
        params["_description"] = description
    return params


def big_number_params(label: str, sql_expression: str, subheader: str,
                      number_format: str = "SMART_NUMBER") -> dict:
    return {
        "metric": metric(label, sql_expression),
        "adhoc_filters": [],
        "time_range": "No filter",
        "header_font_size": 0.42,
        "subheader_font_size": 0.16,
        "y_axis_format": number_format,
        "time_format": "smart_date",
        "subheader": subheader,
        "show_trend_line": False,
        "start_y_axis_at_zero": True,
        "conditional_formatting": [],
    }


def metric(label: str, sql_expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expression,
        "label": label,
    }


def scatter_params(x_col: str, y_col: str, x_label: str, y_label: str,
                   groupby=None, filters=None, show_legend=False,
                   log_x=False, log_y=False, annotation_layers=None,
                   x_axis_bounds=None, y_axis_bounds=None,
                   description=None, y_axis_format="SMART_NUMBER",
                   zoomable=True) -> dict:
    params = {
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
        "show_legend": show_legend,
        "legendType": "scroll",
        "rich_tooltip": True,
        "x_axis_title": x_label,
        "y_axis_title": y_label,
        # Default margins overlap the tick row on numeric axes.
        "x_axis_title_margin": 30,
        "y_axis_title_margin": 30,
        "y_axis_format": y_axis_format,
        "truncateYAxis": False,
        "y_axis_bounds": [None, None],
        "markerEnabled": True,
        "markerSize": 7,
        "zoomable": zoomable,
        "label_colors": CANDIDATE_COLORS,
    }
    if annotation_layers:
        params["annotation_layers"] = list(annotation_layers)
    # NOTE: echarts_timeseries_scatter only supports a log *y* axis; the
    # "logAxis" values "x"/"both" below are truthy strings that Superset
    # treats as the y-axis checkbox, so log_x has never produced a log x
    # axis. Kept for compatibility with existing charts; avoid log_x for
    # new charts and reshape the query instead (e.g., plot a ratio).
    if log_x and log_y:
        params["logAxis"] = "both"
        params["x_axis_bounds"] = [1e-18, None]
        params["y_axis_bounds"] = [1e-18, None]
    elif log_x:
        params["logAxis"] = "x"
        params["x_axis_bounds"] = [1e-18, None]
    elif log_y:
        params["logAxis"] = "y"
        params["y_axis_bounds"] = [1e-12, None]
    if x_axis_bounds is not None:
        params["x_axis_bounds"] = list(x_axis_bounds)
        params["truncateXAxis"] = True
    if y_axis_bounds is not None:
        # Superset ignores y_axis_bounds unless the axis is truncated.
        params["y_axis_bounds"] = list(y_axis_bounds)
        params["truncateYAxis"] = True
    if description is not None:
        params["_description"] = description
    return params


def build_dashboard_layout(charts):
    """Build a simple dashboard layout, packing narrow KPI cards into rows."""
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

    def flush_row(row_index, row_items):
        row_id = f"ROW-proxy-{row_index}"
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart_index, cid, cuuid, cname, width, height in row_items:
            chart_key = f"CHART-proxy-{chart_index}"
            layout[row_id]["children"].append(chart_key)
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

    current_row = []
    current_width = 0
    row_index = 0
    for chart_index, (cid, cuuid, cname, width, height) in enumerate(charts):
        if cid is None:
            continue
        width = max(1, min(int(width), 12))
        if current_row and current_width + width > 12:
            flush_row(row_index, current_row)
            row_index += 1
            current_row = []
            current_width = 0
        current_row.append((chart_index, cid, cuuid, cname, width, height))
        current_width += width
        if current_width >= 12:
            flush_row(row_index, current_row)
            row_index += 1
            current_row = []
            current_width = 0

    if current_row:
        flush_row(row_index, current_row)

    layout["GRID_ID"]["children"] = row_children
    return layout


def select_filter(filter_id: str, name: str, dataset_id: int, column: str,
                  scoped_chart_ids, all_chart_ids, parent_ids=None) -> dict:
    scoped_chart_ids = list(scoped_chart_ids)
    all_chart_ids = list(all_chart_ids)
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
            "NATIVE_FILTER-proxy-target-tier",
            "Target Tier",
            dataset_ids["candidates"],
            "target_match_tier",
            candidate_ids,
            all_ids,
            parent_ids=[device_filter_id],
        ),
        select_filter(
            "NATIVE_FILTER-proxy-target-let",
            "Target LET (MeV cm2/mg)",
            dataset_ids["candidates"],
            "target_let_surface",
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
            "NATIVE_FILTER-proxy-mechanism",
            "Mechanism Class",
            dataset_ids["candidates"],
            "mechanism_match_class",
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
        select_filter(
            "NATIVE_FILTER-proxy-context-let",
            "Irradiation LET",
            dataset_ids["context"],
            "let_label",
            context_ids,
            all_ids,
        ),
        select_filter(
            "NATIVE_FILTER-proxy-context-event",
            "Context Event Type",
            dataset_ids["context"],
            "event_type",
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
    experiment_plan_cols = [
        "planning_rank",
        "planning_priority_tier",
        "plan_action_type",
        "primary_blocker",
        "measurement_device_type",
        "measurement_plan",
        "measurement_recipe_key",
        "candidate_source",
        "candidate_device_type",
        "target_device_type",
        "candidate_sc_voltage_v",
        "candidate_sc_duration_us",
        "candidate_avalanche_mode",
        "candidate_sample_group",
        "representative_candidate_condition",
        "pair_count",
        "affected_target_count",
        "affected_target_device_type_count",
        "affected_target_device_types",
        "affected_event_types",
        "affected_ion_species",
        "candidate_statuses",
        "mechanism_match_classes",
        "cross_device_pair_count",
        "potential_proxy_record_count",
        "expected_unlock",
        "planning_rationale",
    ]
    summary_cols = [
        "target_match_tier",
        "match_scope",
        "candidate_source",
        "target_event_type",
        "target_path_type",
        "mechanism_match_class",
        "candidate_status",
        "replacement_confidence",
        "top_target_events",
        "device_type_count",
        "candidate_device_type_count",
        "measured_damage_top_events",
        "predicted_damage_top_events",
        "waveform_only_top_events",
        "median_combined_screening_distance",
        "median_waveform_distance",
        "median_damage_distance",
        "device_types",
        "candidate_device_types",
    ]
    candidate_cols = [
        "candidate_rank",
        "match_scope",
        "distance_setting_name",
        "device_type",
        "target_voltage_class",
        "target_technology_class",
        "target_event_type",
        "target_match_tier",
        "target_path_type",
        "target_irrad_run_id",
        "target_ion_species",
        "target_beam_energy_mev",
        "target_let_surface",
        "target_fluence_at_meas",
        "target_repetition_fluence_cm2",
        "target_repetition_dose_gy",
        "target_radiation_dose_scope",
        "target_radiation_fluence_basis",
        "target_radiation_energy_basis",
        "target_radiation_deposited_energy_j",
        "target_radiation_deposited_energy_electronic_j",
        "target_radiation_deposited_energy_nuclear_j",
        "target_radiation_deposited_energy_total_j",
        "target_radiation_dose_electronic_gy",
        "target_radiation_dose_nuclear_gy",
        "target_radiation_dose_total_gy",
        "target_radiation_dose_gy",
        "target_radiation_total_dose_gy",
        "target_radiation_layer_count",
        "target_radiation_min_energy_in_mev",
        "target_radiation_min_energy_out_mev",
        "target_radiation_stopped_in_any_layer",
        "target_radiation_min_range_margin_um",
        "candidate_source",
        "candidate_device_type",
        "candidate_device_label",
        "candidate_manufacturer",
        "candidate_voltage_class",
        "candidate_technology_class",
        "candidate_stress_condition_label",
        "candidate_event_type",
        "candidate_sc_voltage_v",
        "candidate_sc_duration_us",
        "candidate_avalanche_mode",
        "candidate_avalanche_outcome",
        "target_energy_j",
        "target_energy_floor_j",
        "target_energy_basis",
        "target_stress_energy_density_j_cm3",
        "target_energy_density_basis",
        "target_energy_localization_class",
        "target_energy_density_geometry_confidence",
        "target_energy_window_basis",
        "target_energy_censored_reason",
        "target_active_window_confidence",
        "target_energy_is_comparable",
        "target_energy_level",
        "candidate_energy_j",
        "candidate_energy_basis",
        "candidate_stress_energy_density_j_cm3",
        "candidate_energy_density_basis",
        "candidate_energy_localization_class",
        "candidate_energy_density_geometry_confidence",
        "candidate_energy_window_basis",
        "candidate_energy_censored_reason",
        "candidate_active_window_confidence",
        "candidate_energy_is_comparable",
        "candidate_energy_level",
        "candidate_stress_pulse_index",
        "candidate_pulse_count_in_sequence",
        "candidate_prior_pulse_count",
        "candidate_pulse_sequence_key",
        "candidate_cumulative_pulse_energy_j",
        "candidate_cumulative_prior_energy_j",
        "candidate_pulse_history_basis",
        "candidate_repetition_pulse_count",
        "candidate_repetition_single_pulse_energy_j",
        "candidate_repetition_cumulative_energy_j",
        "dose_context_available",
        "repetition_context_available",
        "energy_density_ratio",
        "log_energy_delta",
        "normalized_vds_delta",
        "phenotype_axes_used",
        "phenotype_distance",
        "waveform_distance",
        "best_damage_distance",
        "combined_screening_distance",
        "damage_evidence_tier",
        "measured_comparability_status",
        "measured_match_scope",
        "measured_sign_mismatch_axes",
        "prediction_comparability_status",
        "prediction_sign_mismatch_axes",
        "candidate_status",
        "replacement_confidence",
        "uncapped_candidate_status",
        "candidate_rank_penalty",
        "candidate_blockers",
        "target_stress_record_key",
        "candidate_stress_record_key",
    ]
    evidence_cols = [
        "candidate_rank",
        "match_scope",
        "distance_setting_name",
        "device_type",
        "target_voltage_class",
        "target_technology_class",
        "candidate_device_type",
        "candidate_voltage_class",
        "candidate_technology_class",
        "target_stress_record_key",
        "candidate_stress_record_key",
        "target_stress_regime",
        "target_match_tier",
        "target_radiation_mechanism_class",
        "target_application_likeness",
        "target_radiation_fluence_basis",
        "target_radiation_energy_basis",
        "target_radiation_deposited_energy_j",
        "target_radiation_dose_electronic_gy",
        "target_radiation_dose_nuclear_gy",
        "target_radiation_dose_total_gy",
        "target_radiation_dose_gy",
        "target_repetition_fluence_cm2",
        "target_repetition_dose_gy",
        "target_radiation_stopped_in_any_layer",
        "target_radiation_min_range_margin_um",
        "candidate_stress_regime",
        "candidate_application_likeness",
        "target_energy_window_basis",
        "target_energy_floor_j",
        "target_stress_energy_density_j_cm3",
        "target_energy_density_basis",
        "target_energy_localization_class",
        "target_energy_density_geometry_confidence",
        "target_energy_censored_reason",
        "target_energy_is_comparable",
        "target_energy_level",
        "candidate_energy_window_basis",
        "candidate_energy_censored_reason",
        "candidate_energy_is_comparable",
        "candidate_energy_level",
        "candidate_pulse_count_in_sequence",
        "candidate_cumulative_pulse_energy_j",
        "candidate_pulse_history_basis",
        "candidate_repetition_cumulative_energy_j",
        "repetition_context_available",
        "target_vds_collapse_fraction",
        "candidate_vds_collapse_fraction",
        "collapse_delta",
        "target_gate_delta_fraction",
        "candidate_gate_delta_fraction",
        "gate_delta",
        "normalized_vds_delta",
        "duration_log_delta",
        "dose_context_available",
        "energy_density_ratio",
        "mechanism_match_class",
        "mechanism_status_ceiling",
        "mechanism_rationale",
        "path_penalty",
        "phenotype_axes_used",
        "measured_comparable_axes",
        "measured_comparable_axis_labels",
        "measured_sign_mismatch_axis_count",
        "measured_sign_mismatch_axes",
        "measured_match_scope",
        "prediction_model_version",
        "prediction_reference_tier",
        "prediction_validation_mode",
        "prediction_comparable_axes",
        "prediction_sign_mismatch_axis_count",
        "prediction_sign_mismatch_axes",
        "prediction_fingerprint_confidence",
        "prediction_validation_gate_pass_all",
    ]
    context_cols = [
        "source",
        "stress_record_key",
        "device_type",
        "voltage_class",
        "technology_class",
        "filename",
        "event_type",
        "path_type",
        "ion_species",
        "beam_energy_mev",
        "let_surface",
        "let_bin",
        "let_label",
        "fluence_at_meas",
        "stress_regime",
        "figure1_panel_label",
        "figure1_regime_family",
        "soa_relation",
        "test_method_class",
        "test_timescale_class",
        "radiation_mechanism_class",
        "response_reversibility",
        "application_likeness",
        "electrical_terminal_energy_j",
        "electrical_terminal_energy_basis",
        "stress_energy_j",
        "stress_energy_basis",
        "stress_duration_s",
        "effective_stress_time_s",
        "figure1b_time_basis",
        "stress_pulse_index",
        "pulse_count_in_sequence",
        "prior_pulse_count",
        "pulse_sequence_key",
        "cumulative_pulse_energy_j",
        "cumulative_prior_energy_j",
        "pulse_history_basis",
        "pulse_history_provenance",
        "average_terminal_power_w",
        "stress_energy_density_j_cm3",
        "energy_density_basis",
        "energy_density_active_volume_cm3",
        "energy_density_geometry_confidence",
        "energy_density_geometry_provenance",
        "energy_localization_class",
        "peak_abs_power_w",
        "radiation_dose_scope",
        "radiation_fluence_basis",
        "radiation_energy_basis",
        "radiation_deposited_energy_j",
        "radiation_deposited_energy_electronic_j",
        "radiation_deposited_energy_nuclear_j",
        "radiation_deposited_energy_total_j",
        "radiation_dose_electronic_gy",
        "radiation_dose_nuclear_gy",
        "radiation_dose_total_gy",
        "radiation_dose_gy",
        "radiation_total_dose_gy",
        "radiation_layer_count",
        "radiation_calculated_layer_count",
        "radiation_modeled_mass_kg",
        "radiation_min_energy_in_mev",
        "radiation_min_energy_out_mev",
        "radiation_stopped_in_any_layer",
        "radiation_min_range_margin_um",
        "energy_window_basis",
        "energy_censored_reason",
        "active_window_confidence",
        "energy_is_comparable",
        "energy_level",
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "normalized_vds",
        "normalized_current",
        "post_iv_axis_count",
        "context_flags",
    ]
    destruction_boundary_cols = [
        "device_type",
        "voltage_class",
        "destructive_count",
        "min_destructive_normalized_vds",
        "max_survived_normalized_vds",
        "record_count",
        "boundary_interpretation",
    ]
    event_cols = [
        "source",
        "event_record_type",
        "device_type",
        "filename",
        "event_type",
        "path_type",
        "ion_species",
        "let_surface",
        "event_energy_vds_id_j",
        "event_electrical_terminal_energy_j",
        "event_energy_proxy_j",
        "energy_window_basis",
        "energy_censored_reason",
        "active_window_confidence",
        "energy_is_comparable",
        "energy_level",
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
            "Proxy Readiness - Gate Zero Pass KPI",
            dataset_ids["gate_zero"],
            "big_number_total",
            big_number_params(
                "Gate-Zero Pass",
                "MAX(CASE WHEN gate_zero_pass THEN 1 ELSE 0 END)",
                "1 means at least three candidate device families pass coverage",
                number_format=",d",
            ),
            3,
            20,
        ),
        (
            "Proxy Readiness - Gate Zero Candidate Families KPI",
            dataset_ids["gate_zero"],
            "big_number_total",
            big_number_params(
                "Candidate Families",
                "MAX(candidate_device_families)",
                "Device families with proxy and irradiation waveform-plus-post-IV overlap",
                number_format=",d",
            ),
            3,
            20,
        ),
        (
            "Proxy Readiness - Gate Zero Electrical Proxy Post-IV KPI",
            dataset_ids["gate_zero"],
            "big_number_total",
            big_number_params(
                "Electrical Proxy + Post-IV",
                "MAX(device_families_with_electrical_proxy_post_iv_overlap)",
                "Families where SC or UID/UIS waveforms overlap post-IV damage",
                number_format=",d",
            ),
            3,
            20,
        ),
        (
            "Proxy Readiness - Gate Zero Irradiation Post-IV KPI",
            dataset_ids["gate_zero"],
            "big_number_total",
            big_number_params(
                "Irradiation + Post-IV",
                "MAX(device_families_with_irradiation_post_iv_overlap)",
                "Families where irradiation waveform/event coverage overlaps post-IV damage",
                number_format=",d",
            ),
            3,
            20,
        ),
        (
            "Proxy Readiness - Device Coverage / Blocker Matrix",
            dataset_ids["readiness"],
            "table",
            table_params(
                readiness_cols,
                row_limit=200,
                order_by=[
                    ["gate_zero_candidate", False],
                    ["proxy_readiness_status", True],
                    ["device_type_label", True],
                ],
            ),
            12,
            46,
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
            44,
        ),
        (
            "Proxy Readiness - Censored SEB Candidate Coverage",
            dataset_ids["candidate_summary"],
            "table",
            table_params(
                summary_cols,
                row_limit=200,
                order_by=[["top_target_events", False]],
                filters=[
                    sql_filter("target_match_tier = 'energy_censored_phenotype_only'"),
                    sql_filter("target_event_type = 'SEB'"),
                ],
            ),
            12,
            36,
        ),
        (
            "Proxy Readiness - Experiment Planning Queue",
            dataset_ids["experiment_plan"],
            "table",
            table_params(
                experiment_plan_cols,
                row_limit=250,
                order_by=[
                    ["planning_rank", True],
                ],
            ),
            12,
            58,
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
            76,
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
            70,
        ),
        (
            "Proxy Readiness - Candidate Pairs: Target vs Best Proxy Terminal Energy",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "target_energy_j",
                "candidate_energy_j",
                "Target irradiation terminal electrical energy (J)",
                "Selected proxy terminal electrical energy (J)",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "damage_evidence_tier",
                ],
                filters=[
                    top_rank_filter,
                    sql_filter("target_energy_j > 0.0"),
                    sql_filter("candidate_energy_j > 0.0"),
                ],
                show_legend=True,
                log_x=True,
                log_y=True,
            ),
            12,
            68,
        ),
        (
            "Proxy Readiness - Candidate Pairs: Energy Mismatch vs Phenotype Mismatch",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "log_energy_delta",
                "phenotype_distance",
                "|log(selected proxy energy / target irradiation energy)|",
                "Phenotype mismatch distance",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "damage_evidence_tier",
                ],
                filters=[top_rank_filter],
                show_legend=True,
            ),
            12,
            68,
        ),
        (
            "Proxy Readiness - Candidate Pairs: Waveform vs Damage Distance",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "waveform_distance",
                "best_damage_distance",
                "Best proxy waveform distance",
                "Best damage distance",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "damage_evidence_tier",
                ],
                filters=[top_rank_filter],
                show_legend=True,
            ),
            12,
            68,
        ),
        (
            "Proxy Readiness - Candidate Pairs: Energy Density Ratio vs Phenotype Mismatch",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "energy_density_ratio",
                "phenotype_distance",
                "Proxy/target local energy-density ratio",
                "Phenotype mismatch distance",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "target_match_tier",
                    "mechanism_match_class",
                ],
                filters=[top_rank_filter],
                show_legend=True,
                log_x=True,
            ),
            12,
            68,
        ),
        (
            "Proxy Readiness - Normalized Observed V/I Stress Scatter by Test Type",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "normalized_current",
                "Observed |VDS| / device voltage rating",
                "Measured |ID| / current rating (SC/avalanche peak; irradiation event)",
                groupby=["source"],
                filters=[sql_filter(AVALANCHE_NVDS_ARTIFACT_EXCLUSION)],
                show_legend=True,
                x_axis_bounds=FIGURE1B_X_BOUNDS,
            ),
            12,
            48,
        ),
        (
            "Proxy Readiness - Normalized Blocking Bias vs Terminal Electrical Energy by Test Type",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "stress_energy_j",
                "Observed |VDS| / device voltage rating",
                "Terminal electrical energy dissipated (J; VDS*ID event/window integration)",
                groupby=["source"],
                filters=[
                    sql_filter("stress_energy_j > 0.0"),
                    sql_filter(AVALANCHE_NVDS_ARTIFACT_EXCLUSION),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=FIGURE1B_X_BOUNDS,
            ),
            12,
            48,
        ),
        (
            "Proxy Readiness - Normalized Blocking Bias vs Average Terminal Power by Test Type",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "average_terminal_power_w",
                "Observed |VDS| / device voltage rating",
                "Average terminal power over energy window (W)",
                groupby=["source"],
                filters=[
                    sql_filter("average_terminal_power_w > 0.0"),
                    sql_filter(AVALANCHE_NVDS_ARTIFACT_EXCLUSION),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=FIGURE1B_X_BOUNDS,
            ),
            12,
            48,
        ),
        (
            "Proxy Readiness - Irradiation Radiation Deposited Energy vs Blocking Bias",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "radiation_deposited_energy_j",
                "Observed |VDS| / device voltage rating",
                "Radiation deposited energy (J; electronic component)",
                groupby=["let_bin"],
                filters=[
                    sql_filter("source = 'irradiation'"),
                    sql_filter("event_record_type = 'detected_single_event'"),
                    sql_filter("radiation_deposited_energy_j > 0.0"),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=[0.0, 1.0],
            ),
            12,
            44,
        ),
        (
            "Proxy Readiness - Irradiation Energy Amplification (Terminal / Deposited) vs Blocking Bias",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "electrical_terminal_energy_j"
                " / NULLIF(radiation_deposited_energy_j, 0.0)",
                "Observed |VDS| / device voltage rating",
                "Terminal energy / ion deposited energy (amplification, log)",
                groupby=["let_bin"],
                filters=[
                    sql_filter("source = 'irradiation'"),
                    sql_filter("event_record_type = 'detected_single_event'"),
                    sql_filter("radiation_deposited_energy_j > 0.0"),
                    sql_filter("electrical_terminal_energy_j > 0.0"),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=[0.0, 1.0],
                y_axis_format=SCI_AXIS_FORMAT,
                description=AMPLIFICATION_DESCRIPTION,
            ),
            12,
            44,
        ),
        (
            "Proxy Readiness - Figure 1(b): Stress vs Timescale Landscape",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "stress_duration_s",
                "Observed |VDS| / device voltage rating (1.0 = rating)",
                "Stress/measurement window duration (s, log)",
                groupby=["figure1_regime_family"],
                filters=[
                    sql_filter(AVALANCHE_NVDS_ARTIFACT_EXCLUSION),
                ],
                show_legend=True,
                log_y=True,
                annotation_layers=FIGURE1B_REFERENCE_LINES,
                x_axis_bounds=FIGURE1B_X_BOUNDS,
                y_axis_bounds=FIGURE1B_Y_BOUNDS,
                y_axis_format=SCI_AXIS_FORMAT,
                zoomable=False,
                description=FIGURE1B_LANDSCAPE_DESCRIPTION,
            ),
            12,
            68,
        ),
        (
            "Proxy Readiness - Figure 1(b): Effective Stress-Time Landscape",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "effective_stress_time_s",
                "Observed |VDS| / device voltage rating (1.0 = rating)",
                "Effective cumulative stress time (s, log; repetitive sequences scaled by pulse count)",
                groupby=["figure1_regime_family"],
                filters=[
                    sql_filter(AVALANCHE_NVDS_ARTIFACT_EXCLUSION),
                ],
                show_legend=True,
                log_y=True,
                annotation_layers=FIGURE1B_REFERENCE_LINES,
                x_axis_bounds=FIGURE1B_X_BOUNDS,
                y_axis_bounds=FIGURE1B_Y_BOUNDS,
                y_axis_format=SCI_AXIS_FORMAT,
                zoomable=False,
                description=FIGURE1B_LANDSCAPE_DESCRIPTION,
            ),
            12,
            68,
        ),
        (
            "Proxy Readiness - Figure 1(b): Destructive Outcomes (Destruction Limit Markers)",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "stress_duration_s",
                "Observed |VDS| / device voltage rating (1.0 = rating)",
                "Stress/measurement window duration (s, log)",
                groupby=["event_type"],
                filters=[
                    sql_filter(
                        "response_reversibility = 'destructive_or_catastrophic'"
                    ),
                    sql_filter(AVALANCHE_NVDS_ARTIFACT_EXCLUSION),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=FIGURE1B_X_BOUNDS,
                y_axis_bounds=FIGURE1B_DESTRUCTIVE_Y_BOUNDS,
                y_axis_format=SCI_AXIS_FORMAT,
                zoomable=False,
                description=FIGURE1B_DESTRUCTIVE_DESCRIPTION,
            ),
            12,
            44,
        ),
        (
            "Proxy Readiness - Figure 1(b): Destruction Boundary by Device",
            dataset_ids["destruction_boundary"],
            "table",
            table_params(
                destruction_boundary_cols,
                row_limit=500,
                order_by=[["device_type", True], ["voltage_class", True]],
                description=FIGURE1B_BOUNDARY_DESCRIPTION,
            ),
            12,
            38,
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
            58,
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
            48,
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
        description = params.get("_description")
        if description is not None:
            params = {key: value for key, value in params.items()
                      if key != "_description"}
        cid, cuuid = create_chart(
            session, name, ds_id, viz_type, params, description=description
        )
        charts_info.append((cid, cuuid, name, width, height))
        if cid:
            chart_ids.append(cid)
            chart_id_by_name[name] = cid

    candidate_chart_names = {
        "Proxy Readiness - Candidate Summary",
        "Proxy Readiness - Censored SEB Candidate Coverage",
        "Proxy Readiness - Best Proxy Candidates",
        "Proxy Readiness - Candidate Evidence Detail",
        "Proxy Readiness - Candidate Pairs: Target vs Best Proxy Terminal Energy",
        "Proxy Readiness - Candidate Pairs: Energy Mismatch vs Phenotype Mismatch",
        "Proxy Readiness - Candidate Pairs: Waveform vs Damage Distance",
    }
    context_chart_names = {
        "Proxy Readiness - Stress Test Context",
        "Proxy Readiness - Normalized Observed V/I Stress Scatter by Test Type",
        "Proxy Readiness - Normalized Blocking Bias vs Terminal Electrical Energy by Test Type",
        "Proxy Readiness - Normalized Blocking Bias vs Average Terminal Power by Test Type",
        "Proxy Readiness - Irradiation Radiation Deposited Energy vs Blocking Bias",
        "Proxy Readiness - Irradiation Energy Amplification (Terminal / Deposited) vs Blocking Bias",
        "Proxy Readiness - Figure 1(b): Stress vs Timescale Landscape",
        "Proxy Readiness - Figure 1(b): Effective Stress-Time Landscape",
        "Proxy Readiness - Figure 1(b): Destructive Outcomes (Destruction Limit Markers)",
    }
    readiness_chart_names = {"Proxy Readiness - Device Coverage / Blocker Matrix"}
    planning_chart_names = {"Proxy Readiness - Experiment Planning Queue"}
    chart_groups = {
        "candidate": [chart_id_by_name[n] for n in candidate_chart_names if n in chart_id_by_name],
        "context": [chart_id_by_name[n] for n in context_chart_names if n in chart_id_by_name],
        "readiness": [chart_id_by_name[n] for n in readiness_chart_names if n in chart_id_by_name],
        "planning": [chart_id_by_name[n] for n in planning_chart_names if n in chart_id_by_name],
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
