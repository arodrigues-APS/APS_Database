#!/usr/bin/env python3
"""Create the Proxy Readiness dashboard.

The dashboard is intentionally conservative: it ranks short-circuit and
avalanche stress events as proxy candidates for irradiation events, then shows
why each candidate is supported, weak, or blocked.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
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
# Applied right after 025 because it depends on stress_test_context_view.
MECH_ENERGY_SCHEMA_PATH = REPO_ROOT / "schema" / "028_mechanistic_energy_proxy.sql"
PIPELINE_SCHEMAS = {
    "022_irradiation_single_events.sql",
    "027_radiation_stress_dose.sql",
}
DASHBOARD_TITLE = "Proxy Readiness - Waveform Failure Features"
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
    "candidates_v2": "stress_proxy_candidate_energy_v2",
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
    "missing_damage_signature_overlap": "#6b6ecf",
    "damage_signature_mismatch": "#d62728",
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
    "energy_censored_damage_signature_only": "#9467bd",
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
    "validated": "#2ca02c",
    "validation_candidate": "#1f77b4",
    "curation_candidate": "#ff7f0e",
    "screening_only": "#8c959f",
    "blocked": "#d62728",
    "no_curated_truth": "#8c959f",
    "validated_by_curated_measured_post_iv": "#2ca02c",
    "curated_equivalent_non_measured": "#17becf",
    "curated_not_equivalent": "#d62728",
    "curated_uncertain": "#ff7f0e",
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
    "energy released in the event window divided by the ion's LET-based "
    "ionizing/electronic deposited energy. This is intentionally the "
    "electronic stopping channel because the single-event burnout picture is "
    "triggered by ionization, while the destructive energy is supplied by the "
    "blocking-bias circuit and device output capacitance. Rows without a "
    "positive deposited-energy estimate are excluded."
)
IONIZING_DEPOSITED_ENERGY_DESCRIPTION = (
    "This plot uses the LET-based ionizing/electronic deposited-energy "
    "estimate carried as radiation_deposited_energy_j. In the current seed, "
    "heavy-ion rows copy irradiation_runs.let_surface into electronic and "
    "total stopping, and set nuclear stopping to 0 pending SRIM or equivalent "
    "material-specific tables. Nuclear stopping should be treated as a "
    "separate displacement-damage diagnostic, not as a co-equal SEB/SELC "
    "burnout-energy series."
)

# Stored depletion energy is the pre-strike electrostatic field energy per area
# in the reverse-biased depletion region (Kosier model). It is intentionally a
# different quantity from terminal electrical energy and radiation deposited
# energy: do not merge them into one scalar.
DEPLETION_STORED_ENERGY_DESCRIPTION = (
    "Pre-strike stored electrostatic field energy per area in the reverse-"
    "biased depletion region (Kosier model), in uJ/cm2, against observed "
    "normalized blocking bias. Dashed lines mark the Kosier SELC (60 uJ/cm2) "
    "and SEB (207 uJ/cm2) critical areal-energy thresholds. This is "
    "intentionally separate from terminal electrical energy and radiation "
    "deposited energy. Rows use Kosier Table I measured epi doping where a "
    "voltage class is covered; out-of-table classes fall back to the rated-"
    "voltage reach-through estimate when possible. Missing depletion inputs "
    "are excluded from this chart."
)
DEPLETION_RATIO_DESCRIPTION = (
    "A ratio of 1.0 means the modeled stored-depletion-energy threshold is "
    "reached. Net doping uses seeded Kosier Table I values when available "
    "and otherwise falls back to the rated-voltage reach-through estimate. Grouped by "
    "irradiation event_type so SEB and SELC populations can be compared "
    "against the threshold line."
)
DEPLETION_TERMINAL_VS_SEB_DESCRIPTION = (
    "Connects modeled stored-field susceptibility (SEB ratio, x) to the "
    "terminal electrical energy actually released in the detected event "
    "window (y, log). The vertical line marks SEB ratio = 1.0. These are "
    "separate quantities: a high SEB ratio describes pre-strike field "
    "susceptibility, terminal energy measures the electrical release. Only "
    "detected single events with a positive terminal energy and a depletion "
    "model are shown; SEB-typed rows currently lack positive terminal energy "
    "in the seed, so the cloud is dominated by SELCII/SELCI events."
)
ENERGY_CHAIN_TABLE_DESCRIPTION = (
    "Row-level alignment of the irradiation energy chain for each stress "
    "record: ionizing deposited energy (trigger), stored depletion energy and "
    "SEB/SELC ratios (modeled susceptibility), and terminal electrical energy "
    "(measured release). These domains use different units and localization "
    "assumptions and must not be read as interchangeable joules. "
    "se_depletion_model_quality flags whether the depletion inputs are "
    "measured from Kosier Table I or estimated."
)

V2_MECHANISTIC_TABLE_DESCRIPTION = (
    "v2 mechanistic-energy candidate ranking (stress_proxy_candidate_energy_v2). "
    "This is a staged, per-axis-overlap screening ranking, NOT a fitted score. "
    "Read every overlap class and ratio together with "
    "mechanistic_energy_candidate_status and energy_v2_blockers: a numeric "
    "overlap is a retrieval hint, never an equivalence claim. Target severity "
    "(stored depletion ratio) and candidate severity (bulk terminal ratio) are "
    "different physical quantities kept in separate columns. Localization "
    "mismatch is a context note, never a blocker."
)

V2_PARITY_SCATTER_DESCRIPTION = (
    "Energy-equivalence parity: each rank-1 candidate's target severity ratio "
    "(X = stored depletion energy / its own SEB·SELC critical) vs candidate "
    "severity ratio (Y = bulk terminal areal energy / Kosier U_crit, log scale). "
    "Both axes are energy normalized to their OWN failure threshold, so points "
    "near Y=X are a screening equivalence — comparable multiples of each "
    "threshold — never a claim the raw joules are equal. Points high above the "
    "line are candidates that over-deposit relative to the irradiation "
    "susceptibility. Colored by critical_severity_overlap_class. (For the full "
    "log-log parity with the diagonal and ±dex bands, see the interactive "
    "viewer's 'v2 energy equivalence' tab — Superset cannot log the X axis.)"
)

V2_OVERLAP_BAR_DESCRIPTION = (
    "Where we got: rank-1 candidate counts per overlap class, split by match "
    "scope (same-device vs cross-device). The headline equivalence read is the "
    "contrast between axes — terminal-energy overlap is mostly strong while "
    "critical-severity overlap is mostly far-miss: candidates release comparable "
    "raw energy but sit at very different multiples of the failure threshold. "
    "Strong overlap concentrated in same-device is the trustworthy signal."
)

# Superset's annotation-layer schema requires showMarkers/hideLine even for
# FORMULA layers (see FIGURE1B_REFERENCE_LINES). Threshold lines are in the
# same axis units as the plotted metric: uJ/cm2 for stored energy, unitless
# ratio for the SEB/SELC ratio charts.
DEPLETION_STORED_ENERGY_REFERENCE_LINES = [
    {
        "annotationType": "FORMULA",
        "sourceType": "",
        "name": "SELC threshold: 60 uJ/cm2",
        "value": "60",
        "style": "dashed",
        "color": "#9467bd",
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
        "name": "SEB threshold: 207 uJ/cm2",
        "value": "207",
        "style": "dashed",
        "color": "#d62728",
        "opacity": "",
        "width": 1,
        "show": True,
        "showLabel": True,
        "showMarkers": False,
        "hideLine": False,
        "overrides": {"time_range": None},
    },
]
DEPLETION_RATIO_REFERENCE_LINE = [
    {
        "annotationType": "FORMULA",
        "sourceType": "",
        "name": "Threshold: 1.0",
        "value": "1.0",
        "style": "dashed",
        "color": "#d62728",
        "opacity": "",
        "width": 1,
        "show": True,
        "showLabel": True,
        "showMarkers": False,
        "hideLine": False,
        "overrides": {"time_range": None},
    },
]
# SEB ratio spans ~0.04-3.5 and SELC ratio ~0.15-12 in the current seed, both
# under two orders, so the ratio charts and the stored-energy chart use linear
# y axes; the threshold lines stay legible without a log transform.
DEPLETION_X_BOUNDS = [0.0, 1.0]


TAB_READINESS = "Readiness & Actions"
TAB_CANDIDATE = "Candidate Triage"
TAB_MECHANISTIC = "v2 / Mechanistic"
TAB_DIAGNOSTICS = "Method Diagnostics"
TAB_RAW = "Raw / QA"
TAB_ORDER = [TAB_READINESS, TAB_CANDIDATE, TAB_MECHANISTIC, TAB_DIAGNOSTICS, TAB_RAW]
TAB_IDS = {
    TAB_READINESS: "TAB-proxy-readiness",
    TAB_CANDIDATE: "TAB-proxy-candidate",
    TAB_MECHANISTIC: "TAB-proxy-mechanistic",
    TAB_DIAGNOSTICS: "TAB-proxy-diagnostics",
    TAB_RAW: "TAB-proxy-raw",
}
MARKDOWN_PANELS = [
    {
        "tab": TAB_CANDIDATE,
        "code": (
            "### Proxy claim status\n\n"
            "`proxy_claim_status` is the conservative interpretation layer. "
            "Use `validation_candidate` and `curation_candidate` as a queue for "
            "manual truth labeling; do not read distance, waveform similarity, "
            "or energy overlap as validation by itself. A row is `validated` "
            "only in v2 after a curated `proxy_truth_labels` entry marks the pair "
            "`equivalent` with `label_basis = measured_post_iv`."
        ),
        "width": 12,
        "height": 5,
    },
    {
        "tab": TAB_CANDIDATE,
        "code": (
            "### Read distance *with* evidence coverage\n\n"
            "`damage_signature_distance` is a **screening distance**, not a "
            "proxy-equivalence score. It is only comparable **within the same "
            "evidence class**. Rows based on collapse only "
            "(`collapse_only_signature`, the current avalanche cohort) must "
            "not be treated as equivalent to rows with collapse plus "
            "normalized-bias overlap (`collapse_bias_signature`, the current "
            "SC cohort). Gate overlap is absent for every proxy row today, so "
            "no comparison reaches `full_signature`. Always read the evidence "
            "class and missing axes alongside the distance. "
            "`coverage_adjusted_damage_signature_distance` is an experimental "
            "triage diagnostic with uncalibrated penalties and is **not** used "
            "to rank candidates."
        ),
        "width": 12,
        "height": 6,
    },
    {
        "tab": TAB_DIAGNOSTICS,
        "code": (
            "### Irradiation energy chain\n\n"
            "Ionizing deposited energy estimates the radiation **trigger**. "
            "Stored depletion energy estimates whether the reverse-biased "
            "device had enough field energy to cross the Kosier **SEB/SELC "
            "thresholds**. Terminal electrical energy measures the energy "
            "**released** during the observed waveform window. These are "
            "separate quantities with different units and should not be merged "
            "into one scalar."
        ),
        "width": 12,
        "height": 6,
    },
    {
        "tab": TAB_DIAGNOSTICS,
        "code": (
            "[Open the interactive damage-signature and energy viewer]"
            "(https://rawdata.aps.ee.ethz.ch/data/www/tools/"
            "phenotype-3d/index.html)"
        ),
        "width": 12,
        "height": 3,
    },
]
DECISION_STATUS_SQL = (
    "candidate_status IN ("
    "'measured_damage_candidate', 'predicted_damage_candidate', "
    "'device_run_measured_candidate', 'weak_measured_candidate', "
    "'analog_questionable', 'inspect_manually', 'damage_signature_mismatch', "
    "'energy_out_of_range', 'missing_damage_context')"
)
ENERGY_DAMAGE_SIGNATURE_DECISION_DESCRIPTION = (
    "Top-ranked proxy per energy-comparable target, restricted to "
    "decision-driving statuses and genuine failure modes (measured / "
    "predicted / device-run / weak damage, analog-questionable, "
    "inspect-manually, damage-signature-mismatch, energy-out-of-range, "
    "missing-damage-context). Only the cross-device and waveform-only "
    "screening cloud is excluded; see the all-status diagnostic on Method "
    "Diagnostics. Reference thresholds: damage signature mismatch cut-off = 2.50 "
    "(y); energy out-of-range cut-off |log10(proxy/target energy)| ~= 1.74 dex "
    "(x; the underlying scoring threshold is 4.0 nats) — points past it are "
    "energy failures."
)
WAVEFORM_DAMAGE_DESCRIPTION = (
    "Only candidates with measured or predicted post-IV damage evidence "
    "appear here (best_damage_distance is null otherwise). A sparse or "
    "empty plot means damage evidence is missing for the top candidates, "
    "not that no candidates exist."
)
ENERGY_DAMAGE_SIGNATURE_ALL_DESCRIPTION = (
    "All top-ranked candidates including cross-device and waveform-only "
    "screening rows. The dense low band is cross-device avalanche "
    "screening, which is capped at screening confidence by design. "
    "Diagnostic only; use the filtered version on the Candidate Triage tab "
    "for decisions. Reference thresholds: damage signature mismatch = 2.50; "
    "energy out-of-range |log10| ~= 1.74 dex (scoring threshold 4.0 nats)."
)
EVIDENCE_CLASS_DISTANCE_DESCRIPTION = (
    "Damage-signature distance separated by evidence tier so distances are "
    "not read across classes. Tier 2 = collapse_bias_signature (current SC "
    "cohort, collapse + normalized Vds); tier 4 = collapse_only_signature "
    "(current avalanche cohort, collapse only; normalized Vds excluded by "
    "design and gate unavailable). A small tier-4 distance rests on one axis "
    "and is not equivalent to a small tier-2 distance. No proxy row reaches "
    "tier 1 (full signature) today because gate overlap is absent everywhere."
)
CLAIM_STATUS_DESCRIPTION = (
    "Fail-closed proxy interpretation for rank-1 candidates. "
    "validation_candidate means same-device measured post-IV support is strong "
    "enough for review, curation_candidate means a human truth label is still "
    "needed, screening_only means visual discovery only, and blocked means a "
    "required evidence axis rejected the row."
)
DECISION_SAFE_TABLE_DESCRIPTION = (
    "Rows eligible for human proxy-truth curation, ordered by "
    "decision_safe_rank rather than raw visual rank. These are not validated "
    "claims until a curated measured post-IV truth label exists."
)
V2_CLAIM_STATUS_DESCRIPTION = (
    "v2 claim status adds mechanistic-energy blockers and curated truth labels "
    "on top of the v1 damage-signature screen. Only proxy_truth_labels rows "
    "with label='equivalent' and label_basis='measured_post_iv' become "
    "validated."
)

def apply_proxy_schema() -> None:
    """Rebuild the single-event dependency and the proxy-readiness views."""
    with get_connection() as conn:
        apply_common_schema(conn, include_pipeline=PIPELINE_SCHEMAS)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text())
            cur.execute(MECH_ENERGY_SCHEMA_PATH.read_text())
        conn.commit()


def sql_filter(expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "clause": "WHERE",
    }


def table_params(columns, row_limit=1000, order_by=None, filters=None,
                 description=None, show_cell_bars=False) -> dict:
    params = {
        "query_mode": "raw",
        "all_columns": list(columns),
        "adhoc_filters": list(filters or []),
        "row_limit": row_limit,
        "include_search": True,
        "order_by_cols": [json.dumps(col) for col in (order_by or [])],
        "table_timestamp_format": "smart_date",
        "show_cell_bars": show_cell_bars,
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


def bar_params(x_col: str, x_label: str, y_label: str, groupby=None,
               filters=None, metric_label="pairs", metric_sql="COUNT(*)",
               stack=True, description=None) -> dict:
    """Categorical distribution bar (echarts_timeseries_bar over a text x_axis)."""
    params = {
        "x_axis": x_col,
        "metrics": [metric(metric_label, metric_sql)],
        "groupby": list(groupby or []),
        "adhoc_filters": [sql_filter(f"{x_col} IS NOT NULL"), *(filters or [])],
        "row_limit": 1000,
        "x_axis_title": x_label,
        "x_axis_title_margin": 30,
        "y_axis_title": y_label,
        "y_axis_title_margin": 30,
        "y_axis_format": ",d",
        "show_legend": True,
        "legendType": "scroll",
        "rich_tooltip": True,
        "stack": "Stack" if stack else None,
        "order_desc": True,
        "sort_series_type": "sum",
    }
    if description is not None:
        params["_description"] = description
    return params


def build_dashboard_layout(charts, markdown_panels=None):
    """Build a tabbed dashboard layout.

    `charts` is a list of (chart_id, uuid, name, width, height, tab) tuples.
    `markdown_panels` is a list of {tab, code, width, height} dicts placed at the
    top of their tab. Items are grouped by tab (TAB_ORDER) and packed into rows
    of width <= 12. Tabs with no items are omitted.
    """
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
    tabs_id = "TABS-proxy"
    layout[tabs_id] = {
        "type": "TABS",
        "id": tabs_id,
        "children": [],
        "parents": ["ROOT_ID", "GRID_ID"],
        "meta": {},
    }

    items_by_tab = {tab: [] for tab in TAB_ORDER}
    for panel in markdown_panels or []:
        tab = panel.get("tab", TAB_ORDER[-1])
        items_by_tab.setdefault(tab, []).append(
            {
                "kind": "markdown",
                "code": panel.get("code", ""),
                "width": panel.get("width", 12),
                "height": panel.get("height", 6),
            }
        )

    for cid, cuuid, cname, width, height, tab in charts:
        if cid is None:
            continue
        tab = tab or TAB_RAW
        items_by_tab.setdefault(tab, []).append(
            {
                "kind": "chart",
                "cid": cid,
                "uuid": cuuid,
                "name": cname,
                "width": width,
                "height": height,
            }
        )

    counters = {"row": 0, "node": 0}

    def emit_row(base_parents, tab_id, row_items):
        counters["row"] += 1
        row_id = f"ROW-proxy-{counters['row']}"
        row_parents = list(base_parents) + [tab_id]
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": row_parents,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for item in row_items:
            counters["node"] += 1
            if item["kind"] == "markdown":
                node_id = f"MARKDOWN-proxy-{counters['node']}"
                layout[node_id] = {
                    "type": "MARKDOWN",
                    "id": node_id,
                    "children": [],
                    "parents": row_parents + [row_id],
                    "meta": {
                        "width": item["width"],
                        "height": item["height"],
                        "code": item["code"],
                    },
                }
            else:
                node_id = f"CHART-proxy-{counters['node']}"
                layout[node_id] = {
                    "type": "CHART",
                    "id": node_id,
                    "children": [],
                    "parents": row_parents + [row_id],
                    "meta": {
                        "chartId": item["cid"],
                        "width": item["width"],
                        "height": item["height"],
                        "sliceName": item["name"],
                        "uuid": item["uuid"],
                    },
                }
            layout[row_id]["children"].append(node_id)
        layout[tab_id]["children"].append(row_id)

    base_parents = ["ROOT_ID", "GRID_ID", tabs_id]
    for tab in TAB_ORDER:
        tab_items = items_by_tab.get(tab, [])
        if not tab_items:
            continue
        tab_id = TAB_IDS[tab]
        layout[tabs_id]["children"].append(tab_id)
        layout[tab_id] = {
            "type": "TAB",
            "id": tab_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": tab},
        }

        current_row = []
        current_width = 0
        for item in tab_items:
            width = max(1, min(int(item["width"]), 12))
            item = dict(item, width=width)
            if current_row and current_width + width > 12:
                emit_row(base_parents, tab_id, current_row)
                current_row = []
                current_width = 0
            current_row.append(item)
            current_width += width
            if current_width >= 12:
                emit_row(base_parents, tab_id, current_row)
                current_row = []
                current_width = 0
        if current_row:
            emit_row(base_parents, tab_id, current_row)

    layout["GRID_ID"]["children"] = [tabs_id]
    return layout


def select_filter(filter_id: str, name: str, targets, scoped_chart_ids,
                  all_chart_ids, parent_ids=None, tabs_in_scope=None) -> dict:
    """Build one native filter.

    `targets` is a list of (dataset_id, column) tuples. Multiple targets let a
    single filter apply across datasets that name the same concept differently
    (e.g. ``device_type`` on most views but ``measurement_device_type`` on the
    planning view), so the Device filter can also scope the planning queue.
    `tabs_in_scope` pins the filter to the tabs that actually hold its charts.
    """
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
        "targets": [
            {"datasetId": ds_id, "column": {"name": column}}
            for ds_id, column in targets
        ],
        "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
        "cascadeParentIds": list(parent_ids or []),
        "scope": {
            "rootPath": ["ROOT_ID"],
            "excluded": [cid for cid in all_chart_ids if cid not in scoped_chart_ids],
        },
        "type": "NATIVE_FILTER",
        "description": name,
        "chartsInScope": scoped_chart_ids,
        "tabsInScope": list(tabs_in_scope or []),
    }


def build_native_filters(all_chart_ids, dataset_ids, chart_groups):
    candidate_ids = chart_groups["candidate"]
    candidate_v2_ids = chart_groups.get("candidate_v2", [])
    context_ids = chart_groups["context"]
    readiness_ids = chart_groups["readiness"]
    planning_ids = chart_groups["planning"]
    device_only_ids = chart_groups.get("device_only", [])
    all_ids = list(all_chart_ids)

    cand = dataset_ids["candidates"]
    cand_v2 = dataset_ids["candidates_v2"]
    ctx = dataset_ids["context"]
    tab_readiness = TAB_IDS[TAB_READINESS]
    tab_candidate = TAB_IDS[TAB_CANDIDATE]
    tab_mechanistic = TAB_IDS[TAB_MECHANISTIC]
    tab_diag = TAB_IDS[TAB_DIAGNOSTICS]

    device_filter_id = "NATIVE_FILTER-proxy-device"

    def candidate_filter(fid, label, column):
        return select_filter(
            fid, label, [(cand, column)], candidate_ids, all_ids,
            parent_ids=[device_filter_id],
            tabs_in_scope=[tab_candidate, tab_diag],
        )

    def candidate_v2_filter(fid, label, column):
        return select_filter(
            fid, label, [(cand_v2, column)], candidate_v2_ids, all_ids,
            parent_ids=[device_filter_id],
            tabs_in_scope=[tab_mechanistic],
        )

    def context_filter(fid, label, column):
        return select_filter(
            fid, label, [(ctx, column)], context_ids, all_ids,
            tabs_in_scope=[tab_diag],
        )

    return [
        select_filter(
            device_filter_id,
            "Device Type",
            [
                (cand, "device_type"),
                (cand_v2, "device_type"),
                (ctx, "device_type"),
                (dataset_ids["readiness"], "device_type"),
                (dataset_ids["experiment_plan"], "measurement_device_type"),
                (dataset_ids["destruction_boundary"], "device_type"),
            ],
            candidate_ids + candidate_v2_ids + context_ids + readiness_ids
            + planning_ids + device_only_ids,
            all_ids,
            tabs_in_scope=[tab_readiness, tab_candidate, tab_mechanistic, tab_diag],
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-target-event", "Target Event", "target_event_type"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-target-tier", "Target Tier", "target_match_tier"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-target-let", "Target LET (MeV cm2/mg)",
            "target_let_surface"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-candidate-source", "Candidate Source",
            "candidate_source"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-mechanism", "Mechanism Class",
            "mechanism_match_class"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-status", "Candidate Status", "candidate_status"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-claim-status", "Proxy Claim Status",
            "proxy_claim_status"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-confidence", "Replacement Confidence",
            "replacement_confidence"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-evidence-tier", "Evidence Tier",
            "damage_evidence_tier"
        ),
        candidate_filter(
            "NATIVE_FILTER-proxy-target-regime", "Target Regime",
            "target_stress_regime"
        ),
        candidate_v2_filter(
            "NATIVE_FILTER-proxy-v2-regime", "v2 Target Regime",
            "target_mechanistic_regime"
        ),
        candidate_v2_filter(
            "NATIVE_FILTER-proxy-v2-status", "v2 Candidate Status",
            "mechanistic_energy_candidate_status"
        ),
        candidate_v2_filter(
            "NATIVE_FILTER-proxy-v2-claim-status", "v2 Proxy Claim Status",
            "proxy_claim_status"
        ),
        candidate_v2_filter(
            "NATIVE_FILTER-proxy-v2-scope", "v2 Match Scope", "match_scope"
        ),
        candidate_v2_filter(
            "NATIVE_FILTER-proxy-v2-overlap", "v2 Severity Overlap",
            "critical_severity_overlap_class"
        ),
        candidate_v2_filter(
            "NATIVE_FILTER-proxy-v2-source", "v2 Candidate Source",
            "candidate_source"
        ),
        context_filter("NATIVE_FILTER-proxy-context-source", "Context Source", "source"),
        context_filter(
            "NATIVE_FILTER-proxy-context-regime", "Context Regime", "stress_regime"
        ),
        context_filter("NATIVE_FILTER-proxy-context-let", "Irradiation LET", "let_label"),
        context_filter(
            "NATIVE_FILTER-proxy-context-event", "Context Event Type", "event_type"
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
        "candidate_device_families",
        "device_families_with_electrical_proxy_post_iv_overlap",
        "device_families_with_irradiation_post_iv_overlap",
        "candidate_device_types",
    ]
    readiness_cols = [
        "device_type_label",
        "proxy_readiness_status",
        "gate_zero_candidate",
        "sc_waveform_files",
        "uid_uis_waveform_files",
        "irradiation_events",
        "electrical_proxy_waveform_plus_post_iv_files",
        "irradiation_events_with_waveform_plus_post_iv",
        "comparable_damage_axis_count",
    ]
    next_measurement_cols = [
        "planning_rank",
        "plan_action_type",
        "measurement_device_type",
        "measurement_plan",
        "affected_target_count",
        "expected_unlock",
    ]
    experiment_plan_cols = [
        "planning_rank",
        "planning_priority_tier",
        "plan_action_type",
        "measurement_device_type",
        "measurement_plan",
        "affected_target_count",
        "expected_unlock",
        "planning_rationale",
    ]
    summary_cols = [
        "target_match_tier",
        "match_scope",
        "candidate_source",
        "target_event_type",
        "mechanism_match_class",
        "candidate_status",
        "replacement_confidence",
        "proxy_claim_status",
        "proxy_claim_basis",
        "top_target_events",
        "candidate_device_type_count",
        "validation_candidate_top_events",
        "curation_candidate_top_events",
        "screening_only_top_events",
        "blocked_top_events",
        "collapse_only_signature_top_events",
        "collapse_bias_signature_top_events",
        "full_signature_top_events",
        "median_combined_screening_distance",
        "median_waveform_distance",
        "median_damage_distance",
        "median_damage_signature_distance",
    ]
    censored_cols = [
        "match_scope",
        "candidate_source",
        "candidate_status",
        "replacement_confidence",
        "top_target_events",
        "candidate_device_type_count",
    ]
    candidate_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "candidate_rank",
        "decision_safe_rank",
        "device_type",
        "target_event_type",
        "target_match_tier",
        "target_energy_j",
        "target_energy_comparability_class",
        "target_energy_censored_reason",
        "candidate_source",
        "candidate_device_label",
        "candidate_stress_condition_label",
        "candidate_energy_j",
        "candidate_energy_comparability_class",
        "candidate_status",
        "replacement_confidence",
        "proxy_claim_status",
        "proxy_claim_basis",
        "match_scope",
        "damage_evidence_tier",
        "damage_signature_evidence_class",
        "signature_claim_quality",
        "damage_signature_coverage_score",
        "damage_signature_missing_axes",
        "damage_signature_distance",
        "coverage_adjusted_damage_signature_distance",
        "measured_comparability_status",
        "measured_match_scope",
        "measured_sign_mismatch_axis_count",
        "prediction_comparability_status",
        "prediction_sign_mismatch_axis_count",
        "waveform_distance",
        "best_damage_distance",
        "combined_screening_distance",
        "candidate_blockers",
        "proxy_claim_blockers",
        "proxy_claim_summary",
    ]
    decision_safe_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "decision_safe_rank",
        "candidate_rank",
        "device_type",
        "target_event_type",
        "target_match_tier",
        "candidate_source",
        "candidate_device_label",
        "candidate_stress_condition_label",
        "match_scope",
        "candidate_status",
        "proxy_claim_status",
        "proxy_claim_basis",
        "damage_evidence_tier",
        "signature_claim_quality",
        "measured_comparability_status",
        "measured_match_scope",
        "measured_sign_mismatch_axis_count",
        "target_energy_comparability_class",
        "candidate_energy_comparability_class",
        "combined_screening_distance",
        "proxy_claim_blockers",
        "proxy_claim_summary",
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
        "target_energy_comparability_class",
        "target_stress_energy_density_j_cm3",
        "target_energy_density_basis",
        "target_energy_localization_class",
        "target_energy_density_geometry_confidence",
        "target_se_depletion_model_basis",
        "target_se_depletion_model_quality",
        "target_se_depletion_critical_seb_j_cm2",
        "target_se_depletion_critical_selc_j_cm2",
        "target_se_depletion_voltage_v",
        "target_se_depletion_active_thickness_um",
        "target_se_depletion_net_doping_cm3",
        "target_se_depletion_net_doping_basis",
        "target_se_depletion_width_um",
        "target_se_depletion_peak_field_v_cm",
        "target_se_depletion_stored_energy_j_cm2",
        "target_se_depletion_ratio_to_seb",
        "target_se_depletion_ratio_to_selc",
        "target_se_depletion_predicted_seb_voltage_v",
        "target_se_depletion_predicted_selc_voltage_v",
        "target_energy_censored_reason",
        "target_energy_is_comparable",
        "target_energy_level",
        "candidate_energy_window_basis",
        "candidate_energy_censored_reason",
        "candidate_energy_comparability_class",
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
        "damage_signature_axes_used",
        "has_collapse_overlap",
        "has_gate_overlap",
        "has_normalized_vds_overlap",
        "damage_signature_available_axes",
        "damage_signature_missing_axes",
        "damage_signature_axis_mask",
        "damage_signature_coverage_score",
        "damage_signature_evidence_class",
        "damage_signature_evidence_tier",
        "signature_claim_quality",
        "damage_signature_distance",
        "coverage_adjusted_damage_signature_distance",
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
        "candidate_status",
        "proxy_claim_status",
        "proxy_claim_basis",
        "decision_safe_rank",
        "candidate_blockers",
        "proxy_claim_blockers",
        "proxy_claim_summary",
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
        "se_depletion_model_basis",
        "se_depletion_model_quality",
        "se_depletion_critical_seb_j_cm2",
        "se_depletion_critical_selc_j_cm2",
        "se_depletion_voltage_v",
        "se_depletion_active_thickness_um",
        "se_depletion_net_doping_cm3",
        "se_depletion_net_doping_basis",
        "se_depletion_width_um",
        "se_depletion_peak_field_v_cm",
        "se_depletion_stored_energy_j_cm2",
        "se_depletion_ratio_to_seb",
        "se_depletion_ratio_to_selc",
        "se_depletion_predicted_seb_voltage_v",
        "se_depletion_predicted_selc_voltage_v",
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
    energy_chain_cols = [
        "stress_record_key",
        "device_type",
        "event_type",
        "filename",
        "normalized_vds",
        "se_depletion_model_quality",
        "se_depletion_stored_energy_j_cm2",
        "se_depletion_ratio_to_selc",
        "se_depletion_ratio_to_seb",
        "radiation_deposited_energy_j",
        "radiation_deposited_energy_total_j",
        "electrical_terminal_energy_j",
        "electrical_terminal_energy_basis",
        "energy_is_comparable",
        "energy_censored_reason",
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "context_flags",
    ]

    top_rank_filter = sql_filter("candidate_rank = 1")
    top_ten_filter = sql_filter("candidate_rank <= 10")
    decision_status_filter = sql_filter(DECISION_STATUS_SQL)

    # v2 mechanistic-energy candidate columns.  Every table keeps the status and
    # blockers beside the numeric ratios/classes (Phase-5 acceptance: no scalar
    # without its evidence class + blockers).  Target vs candidate severity stay
    # in separate columns (separation invariant #1).
    v2_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "device_type",
        "target_event_type",
        "target_ion_species",
        "match_scope",
        "target_mechanistic_regime",
        "candidate_source",
        "candidate_mechanistic_regime",
        "regime_match_class",
        "candidate_rank_v1",
        "candidate_status_v1",
        "decision_safe_rank_v1",
        "mechanistic_energy_candidate_rank",
        "mechanistic_energy_candidate_status",
        "proxy_claim_status",
        "proxy_claim_basis",
        "truth_validation_status",
        "truth_label",
        "truth_label_basis",
        "proxy_claim_status_v1",
        "proxy_claim_basis_v1",
        "signature_claim_quality_v1",
        "target_energy_comparability_class",
        "candidate_energy_comparability_class",
        "critical_severity_overlap_class",
        "terminal_energy_overlap_class",
        "cumulative_exposure_overlap_class",
        "power_rate_overlap_class",
        "localization_mismatch_log10",
        "target_severity_point_ratio",
        "candidate_severity_point_ratio",
        "damage_evidence_class",
        "measured_sign_mismatch_axis_count",
        "prediction_sign_mismatch_axis_count",
        "energy_v2_blockers",
        "proxy_claim_blockers",
        "proxy_claim_summary",
        "energy_v2_notes",
    ]
    v2_rank1_filter = sql_filter("mechanistic_energy_candidate_rank = 1")

    return [
        (
            "Proxy Readiness - Gate Zero Candidate Families KPI",
            dataset_ids["gate_zero"],
            "big_number_total",
            big_number_params(
                "Candidate Families",
                "MAX(candidate_device_families)",
                "of 3 required to pass gate-zero",
                number_format=",d",
            ),
            4,
            16,
            TAB_READINESS,
            None,
        ),
        (
            "Proxy Readiness - Gate Zero Status",
            dataset_ids["gate_zero"],
            "table",
            table_params(gate_cols, row_limit=1),
            8,
            16,
            TAB_READINESS,
            None,
        ),
        (
            "Proxy Readiness - Next Measurements (Top 3)",
            dataset_ids["experiment_plan"],
            "table",
            table_params(
                next_measurement_cols,
                row_limit=3,
                order_by=[["planning_rank", True]],
                filters=[sql_filter("planning_rank <= 3")],
            ),
            12,
            22,
            TAB_READINESS,
            "planning",
        ),
        (
            "Proxy Readiness - Experiment Planning Queue",
            dataset_ids["experiment_plan"],
            "table",
            table_params(
                experiment_plan_cols,
                row_limit=250,
                order_by=[["planning_rank", True]],
            ),
            12,
            46,
            TAB_READINESS,
            "planning",
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
            40,
            TAB_READINESS,
            "readiness",
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
            34,
            TAB_CANDIDATE,
            "candidate",
        ),
        (
            "Proxy Readiness - Censored SEB Candidate Coverage",
            dataset_ids["candidate_summary"],
            "table",
            table_params(
                censored_cols,
                row_limit=50,
                order_by=[["top_target_events", False]],
                filters=[
                    sql_filter("target_match_tier = 'energy_censored_damage_signature_only'"),
                    sql_filter("target_event_type = 'SEB'"),
                ],
            ),
            12,
            22,
            TAB_CANDIDATE,
            "candidate",
        ),
        (
            "Proxy Readiness - Claim Status by Scope",
            dataset_ids["candidates"],
            "echarts_timeseries_bar",
            bar_params(
                "proxy_claim_status",
                "Proxy claim status",
                "rank-1 target events",
                groupby=["match_scope", "candidate_source"],
                filters=[top_rank_filter],
                description=CLAIM_STATUS_DESCRIPTION,
            ),
            12,
            34,
            TAB_CANDIDATE,
            "candidate",
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
            52,
            TAB_CANDIDATE,
            "candidate",
        ),
        (
            "Proxy Readiness - Decision-Safe Curation Queue",
            dataset_ids["candidates"],
            "table",
            table_params(
                decision_safe_cols,
                row_limit=1000,
                order_by=[
                    ["target_stress_record_key", True],
                    ["decision_safe_rank", True],
                ],
                filters=[sql_filter("decision_safe_rank IS NOT NULL")],
                description=DECISION_SAFE_TABLE_DESCRIPTION,
            ),
            12,
            48,
            TAB_CANDIDATE,
            "candidate",
        ),
        (
            "Proxy Readiness - Candidate Pairs: Energy Mismatch vs Damage Signature Mismatch",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "log_energy_delta_dex",
                "damage_signature_distance",
                "|log10(selected proxy energy / target irradiation energy)|",
                "Damage signature mismatch distance",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "damage_evidence_tier",
                ],
                filters=[top_rank_filter, decision_status_filter],
                show_legend=True,
                description=ENERGY_DAMAGE_SIGNATURE_DECISION_DESCRIPTION,
            ),
            12,
            54,
            TAB_CANDIDATE,
            "candidate",
        ),
        (
            "Proxy Readiness - Damage Signature Distance by Evidence Class",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "damage_signature_evidence_tier",
                "damage_signature_distance",
                "Evidence tier (1=full ... 4=collapse-only; lower is richer)",
                "Damage signature mismatch distance",
                groupby=[
                    "candidate_source",
                    "damage_signature_evidence_class",
                    "target_stress_record_key",
                    "candidate_stress_record_key",
                ],
                filters=[top_rank_filter],
                show_legend=True,
                description=EVIDENCE_CLASS_DISTANCE_DESCRIPTION,
            ),
            12,
            54,
            TAB_CANDIDATE,
            "candidate",
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
                description=WAVEFORM_DAMAGE_DESCRIPTION,
            ),
            12,
            54,
            TAB_CANDIDATE,
            "candidate",
        ),
        (
            "Proxy Readiness - Candidate Pairs: Energy vs Damage Signature (All Statuses, Diagnostic)",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "log_energy_delta_dex",
                "damage_signature_distance",
                "|log10(selected proxy energy / target irradiation energy)|",
                "Damage signature mismatch distance",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "damage_evidence_tier",
                ],
                filters=[top_rank_filter],
                show_legend=True,
                description=ENERGY_DAMAGE_SIGNATURE_ALL_DESCRIPTION,
            ),
            12,
            54,
            TAB_DIAGNOSTICS,
            "candidate",
        ),
        (
            "Proxy Readiness - Candidate Pairs: Energy Density Ratio vs Damage Signature Mismatch",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            scatter_params(
                "damage_signature_distance",
                "energy_density_ratio",
                "Damage signature mismatch distance",
                "Proxy/target local energy-density ratio (log)",
                groupby=[
                    "candidate_source",
                    "candidate_status",
                    "target_match_tier",
                    "mechanism_match_class",
                ],
                filters=[top_rank_filter],
                show_legend=True,
                log_y=True,
            ),
            12,
            54,
            TAB_DIAGNOSTICS,
            "candidate",
        ),
        (
            "Proxy Readiness - Irradiation Depletion Stored Energy vs Blocking Bias",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "se_depletion_stored_energy_j_cm2 * 1000000.0",
                "Observed |VDS| / device voltage rating",
                "Stored depletion energy (uJ/cm2; modeled)",
                groupby=["event_type"],
                filters=[
                    sql_filter("source = 'irradiation'"),
                    sql_filter("se_depletion_stored_energy_j_cm2 IS NOT NULL"),
                ],
                show_legend=True,
                annotation_layers=DEPLETION_STORED_ENERGY_REFERENCE_LINES,
                x_axis_bounds=DEPLETION_X_BOUNDS,
                y_axis_bounds=[0.0, None],
                description=DEPLETION_STORED_ENERGY_DESCRIPTION,
            ),
            12,
            90,
            TAB_DIAGNOSTICS,
            "context",
        ),
        (
            "Proxy Readiness - Irradiation Depletion Ratio to SEB vs Blocking Bias",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "se_depletion_ratio_to_seb",
                "Observed |VDS| / device voltage rating",
                "Stored depletion energy / SEB threshold (1.0 = threshold)",
                groupby=["event_type"],
                filters=[sql_filter("source = 'irradiation'")],
                show_legend=True,
                annotation_layers=DEPLETION_RATIO_REFERENCE_LINE,
                x_axis_bounds=DEPLETION_X_BOUNDS,
                y_axis_bounds=[0.0, None],
                description=DEPLETION_RATIO_DESCRIPTION,
            ),
            6,
            58,
            TAB_DIAGNOSTICS,
            "context",
        ),
        (
            "Proxy Readiness - Irradiation Depletion Ratio to SELC vs Blocking Bias",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "se_depletion_ratio_to_selc",
                "Observed |VDS| / device voltage rating",
                "Stored depletion energy / SELC threshold (1.0 = threshold)",
                groupby=["event_type"],
                filters=[sql_filter("source = 'irradiation'")],
                show_legend=True,
                annotation_layers=DEPLETION_RATIO_REFERENCE_LINE,
                x_axis_bounds=DEPLETION_X_BOUNDS,
                y_axis_bounds=[0.0, None],
                description=DEPLETION_RATIO_DESCRIPTION,
            ),
            6,
            58,
            TAB_DIAGNOSTICS,
            "context",
        ),
        (
            "Proxy Readiness - Irradiation Terminal Energy vs Depletion SEB Ratio",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "se_depletion_ratio_to_seb",
                "electrical_terminal_energy_j",
                "Stored depletion energy / SEB threshold (1.0 = SEB threshold)",
                "Terminal electrical energy released (J, log)",
                groupby=["event_type"],
                filters=[
                    sql_filter("source = 'irradiation'"),
                    sql_filter("event_record_type = 'detected_single_event'"),
                    sql_filter("electrical_terminal_energy_j > 0.0"),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=[0.0, None],
                description=DEPLETION_TERMINAL_VS_SEB_DESCRIPTION,
            ),
            12,
            46,
            TAB_DIAGNOSTICS,
            "context",
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
            46,
            TAB_DIAGNOSTICS,
            "context",
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
            46,
            TAB_DIAGNOSTICS,
            "context",
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
            46,
            TAB_DIAGNOSTICS,
            "context",
        ),
        (
            "Proxy Readiness - Irradiation LET-Based Ionizing Deposited Energy vs Blocking Bias",
            dataset_ids["context"],
            "echarts_timeseries_scatter",
            scatter_params(
                "normalized_vds",
                "radiation_deposited_energy_j",
                "Observed |VDS| / device voltage rating",
                "LET-based ionizing deposited energy (J)",
                groupby=["let_bin"],
                filters=[
                    sql_filter("source = 'irradiation'"),
                    sql_filter("event_record_type = 'detected_single_event'"),
                    sql_filter("radiation_deposited_energy_j > 0.0"),
                ],
                show_legend=True,
                log_y=True,
                x_axis_bounds=[0.0, 1.0],
                description=IONIZING_DEPOSITED_ENERGY_DESCRIPTION,
            ),
            12,
            44,
            TAB_DIAGNOSTICS,
            "context",
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
                "Terminal energy / ionizing deposited energy (amplification, log)",
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
            TAB_DIAGNOSTICS,
            "context",
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
            60,
            TAB_DIAGNOSTICS,
            "context",
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
            60,
            TAB_DIAGNOSTICS,
            "context",
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
            TAB_DIAGNOSTICS,
            "context",
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
            TAB_DIAGNOSTICS,
            "device_only",
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
            TAB_RAW,
            None,
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
            TAB_RAW,
            None,
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
            TAB_RAW,
            None,
        ),
        (
            "Proxy Readiness - Irradiation Energy Chain Detail",
            dataset_ids["context"],
            "table",
            table_params(
                energy_chain_cols,
                row_limit=2500,
                order_by=[
                    ["se_depletion_ratio_to_seb", False],
                    ["normalized_vds", False],
                ],
                filters=[sql_filter("source = 'irradiation'")],
                description=ENERGY_CHAIN_TABLE_DESCRIPTION,
            ),
            12,
            48,
            TAB_RAW,
            None,
        ),
        (
            "Proxy Readiness - v2 Energy Equivalence Parity (Severity)",
            dataset_ids["candidates_v2"],
            "echarts_timeseries_scatter",
            scatter_params(
                "target_severity_point_ratio",
                "candidate_severity_point_ratio",
                "Target severity ratio (÷ own SEB/SELC critical)",
                "Candidate severity ratio (÷ Kosier U_crit, log)",
                groupby=[
                    "critical_severity_overlap_class",
                    "target_stress_record_key",
                    "candidate_stress_record_key",
                ],
                filters=[v2_rank1_filter],
                log_y=True,
                show_legend=False,
                description=V2_PARITY_SCATTER_DESCRIPTION,
            ),
            12,
            50,
            TAB_MECHANISTIC,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Critical-Severity Overlap by Scope",
            dataset_ids["candidates_v2"],
            "echarts_timeseries_bar",
            bar_params(
                "critical_severity_overlap_class",
                "Critical-severity overlap class",
                "rank-1 candidates",
                groupby=["match_scope"],
                filters=[v2_rank1_filter],
                description=V2_OVERLAP_BAR_DESCRIPTION,
            ),
            6,
            40,
            TAB_MECHANISTIC,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Terminal-Energy Overlap by Scope",
            dataset_ids["candidates_v2"],
            "echarts_timeseries_bar",
            bar_params(
                "terminal_energy_overlap_class",
                "Terminal-energy overlap class",
                "rank-1 candidates",
                groupby=["match_scope"],
                filters=[v2_rank1_filter],
                description=V2_OVERLAP_BAR_DESCRIPTION,
            ),
            6,
            40,
            TAB_MECHANISTIC,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Proxy Claim Status by Scope",
            dataset_ids["candidates_v2"],
            "echarts_timeseries_bar",
            bar_params(
                "proxy_claim_status",
                "v2 proxy claim status",
                "rank-1 target events",
                groupby=["match_scope", "truth_validation_status"],
                filters=[v2_rank1_filter],
                description=V2_CLAIM_STATUS_DESCRIPTION,
            ),
            12,
            34,
            TAB_MECHANISTIC,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Rank-1 Mechanistic Candidate",
            dataset_ids["candidates_v2"],
            "table",
            table_params(
                v2_cols,
                row_limit=500,
                order_by=[
                    ["mechanistic_energy_status_priority", True],
                    ["mechanistic_energy_candidate_rank", True],
                ],
                filters=[v2_rank1_filter],
                description=V2_MECHANISTIC_TABLE_DESCRIPTION,
            ),
            12,
            52,
            TAB_MECHANISTIC,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Claim Review Queue",
            dataset_ids["candidates_v2"],
            "table",
            table_params(
                v2_cols,
                row_limit=1000,
                order_by=[
                    ["target_stress_record_key", True],
                    ["mechanistic_energy_candidate_rank", True],
                ],
                filters=[sql_filter(
                    "proxy_claim_status IN ('validated', 'validation_candidate', "
                    "'curation_candidate', 'blocked')"
                )],
                description=V2_CLAIM_STATUS_DESCRIPTION,
            ),
            12,
            52,
            TAB_MECHANISTIC,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Candidate Pool (Top 10)",
            dataset_ids["candidates_v2"],
            "table",
            table_params(
                v2_cols,
                row_limit=2000,
                order_by=[
                    ["target_stress_record_key", True],
                    ["mechanistic_energy_candidate_rank", True],
                ],
            ),
            12,
            56,
            TAB_MECHANISTIC,
            "candidate_v2",
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
    groups = defaultdict(list)
    for name, ds_id, viz_type, params, width, height, tab, group in build_chart_defs(dataset_ids):
        description = params.get("_description")
        if description is not None:
            params = {key: value for key, value in params.items()
                      if key != "_description"}
        cid, cuuid = create_chart(
            session, name, ds_id, viz_type, params, description=description
        )
        charts_info.append((cid, cuuid, name, width, height, tab))
        if not cid:
            continue
        chart_ids.append(cid)
        if group:
            groups[group].append(cid)

    chart_groups = {
        "candidate": groups.get("candidate", []),
        "candidate_v2": groups.get("candidate_v2", []),
        "context": groups.get("context", []),
        "readiness": groups.get("readiness", []),
        "planning": groups.get("planning", []),
        "device_only": groups.get("device_only", []),
    }

    print("\nBuilding proxy-readiness dashboard layout...")
    position_json = build_dashboard_layout(charts_info, MARKDOWN_PANELS)
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
