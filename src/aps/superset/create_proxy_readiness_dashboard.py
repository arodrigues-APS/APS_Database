#!/usr/bin/env python3
"""Create the Proxy Readiness dashboard.

The dashboard is intentionally conservative: it ranks short-circuit and
avalanche stress events as proxy candidates for irradiation events, then shows
why each candidate is supported, weak, or blocked.
"""

from __future__ import annotations

import argparse
import json

from aps.db_config import SUPERSET_URL
from aps.superset.superset_api import (
    build_json_metadata,
    create_chart,
    create_or_update_dashboard,
    find_database,
    find_or_create_dataset,
    get_session,
    refresh_dataset_columns,
)
from aps.viewers.proxy_viz_palette import CANDIDATE_COLORS

# Keep the established slug so deploying this definition updates the existing
# dashboard instead of creating a second copy.  The title now reflects the
# actual scope: three screening methods, their evidence gates, and concordance.
DASHBOARD_TITLE = "Proxy Method Readiness & Concordance"
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
    "candidates_v3": "stress_proxy_candidate_combined_v3",
    "combined_settings": "stress_proxy_combined_ranker_settings",
    "concordance_enrichment": "stress_proxy_concordance_enrichment_view",
    "candidate_boundary": "stress_candidate_destruction_boundary_energy_view",
}

# Native-filter targets are declared centrally so definition tests can prove
# that every target column exists in the corresponding SQL view.  The mapping
# also prevents a filter from being scoped to a chart merely because it shared
# a broad legacy group label.
FILTER_TARGET_COLUMNS = {
    "readiness": {"device_type"},
    "experiment_plan": {"measurement_device_type"},
    "candidates": {
        "device_type",
        "target_event_type",
        "candidate_source",
        "match_scope",
        "proxy_claim_status",
        "damage_signature_evidence_class",
    },
    "candidate_summary": {
        "target_event_type",
        "candidate_source",
        "match_scope",
        "proxy_claim_status",
    },
    "candidates_v2": {
        "device_type",
        "target_event_type",
        "candidate_source",
        "match_scope",
        "proxy_claim_status",
    },
    "candidates_v3": {
        "device_type",
        "target_event_type",
        "candidate_source",
        "match_scope",
    },
    "concordance_enrichment": {
        "device_type",
        "target_event_type",
        "v2_pick_source",
        "v2_match_scope",
        "v2_proxy_claim_status",
    },
    "context": {"device_type", "source", "stress_regime", "event_type"},
    "destruction_boundary": {"device_type"},
    "candidate_boundary": {"device_type", "source"},
    "event_features": {"device_type"},
}

# Unit-of-analysis and evidence provenance is appended to every saved chart
# description.  This makes measured, modeled, aggregate, and screening views
# distinguishable from Superset itself rather than only from external docs.
DATASET_PROVENANCE = {
    "gate_zero": (
        "one portfolio-wide evidence-gate row",
        "derived coverage gate from measured waveform and post-IV availability",
    ),
    "readiness": (
        "one physical device family",
        "derived coverage counts from measured waveform and post-IV records",
    ),
    "file_features": (
        "one waveform file",
        "measured waveform metadata with derived readiness features",
    ),
    "event_features": (
        "one detected or file-level stress event",
        "measured waveform features with derived event/readiness classifications",
    ),
    "basis_features": (
        "one stress-feature basis row",
        "derived waveform feature basis",
    ),
    "context": (
        "one stress record or detected irradiation event",
        "measured stress context plus explicitly labeled modeled energy fields",
    ),
    "destruction_boundary": (
        "one device and voltage-class boundary rollup",
        "empirical outcome rollup; unknown outcomes are not failure evidence",
    ),
    "candidates": (
        "one target/candidate pair",
        "v1 screening output; distances are not calibrated equivalence probabilities",
    ),
    "candidate_summary": (
        "one grouped cohort of v1 rank-1 targets",
        "aggregate of v1 screening output",
    ),
    "experiment_plan": (
        "one proposed measurement action",
        "derived planning queue ranked by evidence expected to be unlocked",
    ),
    "candidates_v2": (
        "one target/candidate pair",
        "v2 staged mechanistic-energy screening output with fail-closed blockers",
    ),
    "candidates_v3": (
        "one target/candidate pair from the v2 top-10 shortlist",
        "v3 uncalibrated combined-vector reranking output",
    ),
    "combined_settings": (
        "one ranker-settings version",
        "declared, uncalibrated v3 weights and configuration",
    ),
    "concordance_enrichment": (
        "one energy-rankable target",
        "derived v1/v2 concordance and curated-truth coverage diagnostics",
    ),
    "candidate_boundary": (
        "one candidate device/source/timescale boundary cell",
        "empirical survival/failure rollup with explicit unknown outcomes",
    ),
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
    "(stored depletion ratio) and candidate failure fraction (own electrical threshold) are "
    "different physical quantities kept in separate columns. Localization "
    "mismatch is a context note, never a blocker."
)

V2_PARITY_SCATTER_DESCRIPTION = (
    "Energy-equivalence parity: each rank-1 candidate's target severity ratio "
    "(X = stored depletion energy / SEB or SELC critical) vs candidate failure "
    "fraction (Y = terminal energy / measured electrical destruction boundary, "
    "log scale). Points near Y=X are a screening hint — comparable multiples of "
    "different thresholds — never a claim the raw joules are equal. Points high "
    "above the line are candidates that over-stress relative to the irradiation "
    "susceptibility. Colored by candidate_failure_fraction_overlap_class. (For "
    "the full log-log parity with diagonal and dex bands, see the interactive "
    "viewer's v2 energy-equivalence tab — Superset cannot log the X axis.)"
)

V2_OVERLAP_BAR_DESCRIPTION = (
    "Where we got: rank-1 candidate counts per overlap class, split by match "
    "scope (same-device vs cross-device). The headline equivalence read is the "
    "contrast between axes — terminal-energy overlap is mostly strong while "
    "candidate failure-fraction overlap shows whether candidates sit at comparable "
    "multiples of their own electrical destruction thresholds. "
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


TAB_READINESS = "Verdict & Next Actions"
TAB_CANDIDATE = "v1 · Waveform ranker"
TAB_MECHANISTIC = "v2 · Energy ranker"
TAB_V3 = "v3 · Combined vector"
TAB_CONCORDANCE = "Concordance & Curation"
# Raw/QA folded in here (2026-07-22 redesign): one forensic tab instead of two.
TAB_PHYSICS = "Physics & Raw"
TAB_ORDER = [
    TAB_READINESS,
    TAB_CANDIDATE,
    TAB_MECHANISTIC,
    TAB_V3,
    TAB_CONCORDANCE,
    TAB_PHYSICS,
]
TAB_IDS = {
    TAB_READINESS: "TAB-proxy-readiness",
    TAB_CANDIDATE: "TAB-proxy-candidate",
    TAB_MECHANISTIC: "TAB-proxy-mechanistic",
    TAB_V3: "TAB-proxy-v3",
    TAB_CONCORDANCE: "TAB-proxy-concordance",
    TAB_PHYSICS: "TAB-proxy-physics",
}
MARKDOWN_PANELS = [
    {
        "tab": TAB_READINESS,
        "code": (
            "### Decision contract and provenance\n\n"
            "This dashboard asks whether SC/avalanche records are sufficiently "
            "supported to act as **screening candidates** for irradiation "
            "targets, why v1/v2/v3 disagree, and which measurement removes the "
            "next blocker. Rows represent device families, target/candidate "
            "pairs, or stress records as stated in each chart description. "
            "Measured, modeled, and derived fields are never interchangeable. "
            "A distance, overlap, or rank is not validation: only curated "
            "measured post-IV truth can produce a validated claim. Source views "
            "are rebuilt from schemas 025, 028, and 029; the Superset dashboard "
            "modified timestamp identifies the deployed build."
        ),
        "width": 12,
        "height": 7,
    },
    {
        "tab": TAB_MECHANISTIC,
        "code": (
            "### v2 is a fail-closed screening ranker\n\n"
            "Read target severity, candidate failure-fraction support, terminal "
            "energy, match scope, claim status, and blockers together. Missing "
            "own-device destruction boundaries remain **missing evidence**; "
            "Kosier or cross-device fallbacks are context and must not be read "
            "as measured candidate thresholds."
        ),
        "width": 12,
        "height": 5,
    },
    {
        "tab": TAB_CONCORDANCE,
        "code": (
            "### Agreement and human review\n\n"
            "Read the **Method-Agreement Map** below first: bottom-left points "
            "are strong candidates (both methods rank them highly), far-right "
            "points are method disagreements to curate. Exact rank-1 agreement "
            "(the KPI) is intentionally strict; enrichment asks the softer "
            "question of where the v2 winner lies in the energy-free signature "
            "ordering. Review queues remain unvalidated until a curated "
            "measured post-IV label supplies reviewer, basis, and date.\n\n"
            "[Open the interactive damage-signature viewer for per-record "
            "identity and 3-D concordance]"
            "(https://rawdata.aps.ee.ethz.ch/data/www/tools/"
            "damage-signature-3d/index.html)"
        ),
        "width": 12,
        "height": 6,
    },
    {
        "tab": TAB_CANDIDATE,
        "code": (
            "### Proxy claim status\n\n"
            "`proxy_claim_status` is the conservative interpretation layer. "
            "Use `validation_candidate` and `curation_candidate` as a queue for "
            "manual truth labeling; do not read distance, waveform similarity, "
            "or energy overlap as validation by itself. A row is `validated` "
            "only in v2 after a curated `proxy_truth_labels` entry marks the pair "
            "`equivalent` with `label_basis = measured_post_iv`.\n\n"
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
        "height": 10,
    },
    {
        "tab": TAB_PHYSICS,
        "code": (
            "### Irradiation energy chain\n\n"
            "Ionizing deposited energy estimates the radiation **trigger**. "
            "Stored depletion energy estimates whether the reverse-biased "
            "device had enough field energy to cross the Kosier **SEB/SELC "
            "thresholds**. Terminal electrical energy measures the energy "
            "**released** during the observed waveform window. These are "
            "separate quantities with different units and should not be merged "
            "into one scalar.\n\n"
            "[Open the interactive damage-signature and energy viewer]"
            "(https://rawdata.aps.ee.ethz.ch/data/www/tools/"
            "damage-signature-3d/index.html)\n\n"
            "### Forensic QA and export\n\n"
            "The wide tables below preserve evidence and provenance for "
            "drill-through and export. They are not the primary decision "
            "surface. Row limits apply to the on-screen table; narrow filters "
            "before interpreting absence or downloading a cohort."
        ),
        "width": 12,
        "height": 10,
    },
    {
        "tab": TAB_V3,
        "code": (
            "### v3 vector explorer\n\n"
            "The dashboard keeps the rank-1 table and uncalibrated weights here. "
            "Use the interactive viewer's v3 vector explorer for the stacked "
            "component-share breakdown before judging the screening weights."
        ),
        "width": 12,
        "height": 4,
    },
]
DECISION_STATUS_SQL = (
    "candidate_status IN ("
    "'measured_damage_candidate', 'predicted_damage_candidate', "
    "'device_run_measured_candidate', 'weak_measured_candidate', "
    "'analog_questionable', 'inspect_manually', 'damage_signature_mismatch', "
    "'missing_damage_context')"
)
ENERGY_DAMAGE_SIGNATURE_DECISION_DESCRIPTION = (
    "Top-ranked proxy per energy-comparable target, restricted to "
    "decision-driving statuses and genuine failure modes (measured / "
    "predicted / device-run / weak damage, analog-questionable, "
    "inspect-manually, damage-signature-mismatch, "
    "missing-damage-context). Only the cross-device and waveform-only "
    "screening cloud is excluded; see the all-status diagnostic on Method "
    "Diagnostics. Reference thresholds: damage signature mismatch cut-off = 2.50 "
    "(y); terminal-energy out-of-range cut-off |log10(proxy terminal / target terminal energy)| ~= 1.74 dex "
    "(x; the underlying scoring threshold is 4.0 nats) — points past it are "
    "terminal-energy failures."
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
    "terminal-energy out-of-range |log10(proxy terminal / target terminal energy)| ~= 1.74 dex (scoring threshold 4.0 nats)."
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
        tab = tab or TAB_PHYSICS
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


def build_chart_catalog(chart_defs, chart_ids):
    """Return the deployed chart contract used by filters and reconciliation.

    The positional chart-definition tuple remains compatible with the other
    dashboard builders, while this catalog records the fields that Superset
    lifecycle checks need. Failed chart creations (chart_id is None) are
    omitted from filter scopes but remain visible in the caller's error output.
    """
    if len(chart_defs) != len(chart_ids):
        raise ValueError("chart definitions and chart ids must have equal length")

    catalog = []
    for definition, chart_id in zip(chart_defs, chart_ids):
        if chart_id is None:
            continue
        name, dataset_id, _viz_type, _params, _width, _height, tab, group = definition
        catalog.append(
            {
                "chart_id": int(chart_id),
                "name": name,
                "dataset_id": dataset_id,
                "tab": tab,
                "tab_id": TAB_IDS[tab],
                "group": group,
            }
        )
    return catalog


def build_native_filters(all_chart_ids, dataset_ids, chart_catalog):
    """Build a small, dashboard-wide native-filter set from actual chart inputs.

    2026-07-22 redesign: Target Event / Candidate Source / Match Scope are
    merged into one filter per concept covering every method tab, replacing a
    private Event/Source/Scope/Claim quadruplet per v1/v2/v3/Review tab. The
    pre-redesign filters already co-targeted `candidates` + `candidates_v2` +
    `concordance_enrichment` for these three concepts (and `candidates_v2` +
    `candidate_boundary.source` for source), so those pairings sharing one
    value domain is evidenced by the prior deployment. `candidates_v3` and
    `candidate_summary` joining the merge is new. Candidate Source (the one
    also reaching `candidate_boundary.source`, so the most likely of the
    three to disagree) was checked live at 2026-07-22 first deploy: a
    GROUP BY across all six target columns found exactly two values,
    `avalanche` and `sc`, both present with identical spelling in every one
    of the six views (row counts differ a lot by view, e.g. candidate_boundary
    has as few as 4-16 rows for a value vs thousands elsewhere, but zero is
    the only count that would actually blank a chart, and none were zero) --
    confirmed safe. Target Event and Match Scope were not separately checked;
    if either misbehaves, split it back out using this same query pattern
    (GROUP BY value, COUNT(*) per target dataset/column, UNION ALL, diff the
    value sets) to find which view disagrees.
    Claim Status stays split: v2 and the curated enrichment view share
    statuses including `validated`, but v1's `proxy_claim_status` never
    reaches `validated` (that requires a v2 truth label), so merging it would
    apply a `validated` selection to v1 charts and blank them. v1's claim
    status remains readable via its column and the existing "Claim Status by
    Scope" bar instead of a dedicated filter. The v1-only Evidence Class
    filter is dropped for the same reason every other single-tab filter was
    cut: the column and its dedicated evidence-class chart already carry that
    read without spending rail space on a filter used nowhere else.
    """
    all_ids = [int(chart_id) for chart_id in all_chart_ids if chart_id is not None]
    catalog_ids = [row["chart_id"] for row in chart_catalog]
    if set(all_ids) != set(catalog_ids):
        raise ValueError("chart catalog must describe every deployed chart id")

    dataset_key_by_id = {dataset_id: key for key, dataset_id in dataset_ids.items()}
    if len(dataset_key_by_id) != len(dataset_ids):
        raise ValueError("dataset ids must be unique when building filter scopes")

    def target(dataset_key, column):
        valid_columns = FILTER_TARGET_COLUMNS.get(dataset_key, set())
        if column not in valid_columns:
            raise ValueError(
                f"unsupported filter target {dataset_key}.{column}; "
                "update FILTER_TARGET_COLUMNS only after the SQL view exposes it"
            )
        return dataset_ids[dataset_key], column

    def scoped(*, tabs=None, dataset_keys=None, groups=None, names=None):
        tab_set = set(tabs or [])
        dataset_set = set(dataset_keys or [])
        dataset_id_set = {dataset_ids[key] for key in dataset_set}
        group_set = set(groups or [])
        name_set = set(names or [])
        return [
            row["chart_id"]
            for row in chart_catalog
            if (not tab_set or row["tab"] in tab_set)
            and (not dataset_id_set or row["dataset_id"] in dataset_id_set)
            and (not group_set or row["group"] in group_set)
            and (not name_set or row["name"] in name_set)
        ]

    def tab_ids_for(chart_ids):
        selected = set(chart_ids)
        tabs = {
            row["tab"]
            for row in chart_catalog
            if row["chart_id"] in selected
        }
        return [TAB_IDS[tab] for tab in TAB_ORDER if tab in tabs]

    device_filter_id = "NATIVE_FILTER-proxy-device"

    def make_filter(fid, label, targets, chart_ids):
        if not chart_ids:
            raise ValueError(f"native filter {fid} has no charts in scope")
        return select_filter(
            fid,
            label,
            targets,
            chart_ids,
            all_ids,
            parent_ids=[] if fid == device_filter_id else [device_filter_id],
            tabs_in_scope=tab_ids_for(chart_ids),
        )

    device_target_map = {
        "readiness": "device_type",
        "experiment_plan": "measurement_device_type",
        "candidates": "device_type",
        "candidates_v2": "device_type",
        "candidates_v3": "device_type",
        "concordance_enrichment": "device_type",
        "context": "device_type",
        "destruction_boundary": "device_type",
        "candidate_boundary": "device_type",
        "event_features": "device_type",
    }
    device_scope = scoped(dataset_keys=device_target_map)

    # Shared across every method + curation tab. candidate_summary carries its
    # own copy of these three columns (a cohort-level rollup of `candidates`,
    # not a join), so it needs its own targets alongside `candidates` itself.
    method_tabs = {TAB_CANDIDATE, TAB_MECHANISTIC, TAB_V3, TAB_CONCORDANCE}
    rank_dataset_keys = {
        "candidates", "candidate_summary", "candidates_v2", "candidates_v3",
        "concordance_enrichment",
    }
    event_scope = scoped(tabs=method_tabs, dataset_keys=rank_dataset_keys)
    source_scope = scoped(
        tabs=method_tabs, dataset_keys=rank_dataset_keys | {"candidate_boundary"}
    )
    match_scope_scope = scoped(tabs=method_tabs, dataset_keys=rank_dataset_keys)

    # Claim Status stays scoped to v2 + Concordance only; see docstring.
    claim_scope = scoped(
        tabs={TAB_MECHANISTIC, TAB_CONCORDANCE},
        dataset_keys={"candidates_v2", "concordance_enrichment"},
    )

    physics_scope = scoped(tabs={TAB_PHYSICS}, dataset_keys={"context"})

    return [
        make_filter(
            device_filter_id,
            "Device Type",
            [
                target(dataset_key, column)
                for dataset_key, column in device_target_map.items()
            ],
            device_scope,
        ),
        make_filter(
            "NATIVE_FILTER-proxy-target-event",
            "Target Event",
            [
                target("candidates", "target_event_type"),
                target("candidate_summary", "target_event_type"),
                target("candidates_v2", "target_event_type"),
                target("candidates_v3", "target_event_type"),
                target("concordance_enrichment", "target_event_type"),
            ],
            event_scope,
        ),
        make_filter(
            "NATIVE_FILTER-proxy-candidate-source",
            "Candidate Source",
            [
                target("candidates", "candidate_source"),
                target("candidate_summary", "candidate_source"),
                target("candidates_v2", "candidate_source"),
                target("candidate_boundary", "source"),
                target("candidates_v3", "candidate_source"),
                target("concordance_enrichment", "v2_pick_source"),
            ],
            source_scope,
        ),
        make_filter(
            "NATIVE_FILTER-proxy-match-scope",
            "Match Scope",
            [
                target("candidates", "match_scope"),
                target("candidate_summary", "match_scope"),
                target("candidates_v2", "match_scope"),
                target("candidates_v3", "match_scope"),
                target("concordance_enrichment", "v2_match_scope"),
            ],
            match_scope_scope,
        ),
        make_filter(
            "NATIVE_FILTER-proxy-claim-status",
            "Claim Status",
            [
                target("candidates_v2", "proxy_claim_status"),
                target("concordance_enrichment", "v2_proxy_claim_status"),
            ],
            claim_scope,
        ),
        # Physics & Raw: three controls, scoped to the `context` dataset (the
        # merged-in Raw tables that read `context` pick these up for free).
        make_filter(
            "NATIVE_FILTER-proxy-context-source",
            "Context Source",
            [target("context", "source")],
            physics_scope,
        ),
        make_filter(
            "NATIVE_FILTER-proxy-context-regime",
            "Context Regime",
            [target("context", "stress_regime")],
            physics_scope,
        ),
        make_filter(
            "NATIVE_FILTER-proxy-context-event",
            "Context Event Type",
            [target("context", "event_type")],
            physics_scope,
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
    # 2026-07-22 redesign: trimmed to the columns that carry a decision or
    # verdict; the dropped fields (uid_uis_waveform_files count,
    # irradiation-events-with-overlap subtotal) remain queryable from the
    # view directly and are not needed to read the gate/blocker verdict itself.
    readiness_cols = [
        "device_type_label",
        "proxy_readiness_status",
        "gate_zero_candidate",
        "sc_waveform_files",
        "irradiation_events",
        "electrical_proxy_waveform_plus_post_iv_files",
        "comparable_damage_axis_count",
    ]
    experiment_plan_cols = [
        "planning_rank",
        "planning_priority_tier",
        "plan_action_type",
        "measurement_device_type",
        "affected_target_count",
        "expected_unlock",
    ]
    # Trimmed from 22 to the cohort identity + outcome columns; the per-status
    # top-event subtotals and the three other median-distance variants remain
    # in the view for drill-through and the CSV export.
    summary_cols = [
        "target_match_tier",
        "match_scope",
        "candidate_source",
        "target_event_type",
        "candidate_status",
        "proxy_claim_status",
        "top_target_events",
        "candidate_device_type_count",
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
    # Trimmed from 15 to the identity + verdict columns; every row here is
    # already rank 1 (see the waveform_rank filter below), so the rank column
    # itself is redundant on screen. candidate_device_label,
    # candidate_stress_condition_label, best_damage_distance, and
    # proxy_claim_blockers remain in the view for drill-through.
    candidate_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "device_type",
        "target_event_type",
        "candidate_source",
        "match_scope",
        "proxy_claim_status",
        "damage_signature_evidence_class",
        "signature_axis_distance",
    ]
    decision_safe_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "decision_safe_rank",
        "device_type",
        "target_event_type",
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
        "measured_sign_mismatch_axis_count",
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
    # Full 40-column detail, kept only for the forensic "Candidate Pool"
    # dump (row_limit 2000, Physics & Raw tab).
    v2_cols_full = [
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
        "candidate_failure_fraction_overlap_class",
        "terminal_energy_overlap_class",
        "cumulative_exposure_overlap_class",
        "timescale_overlap_class",
        "power_rate_overlap_class",
        "localization_mismatch_log10",
        "target_severity_point_ratio",
        "candidate_failure_fraction_point",
        "damage_evidence_class",
        "measured_sign_mismatch_axis_count",
        "prediction_sign_mismatch_axis_count",
        "energy_v2_blockers",
        "proxy_claim_blockers",
        "proxy_claim_summary",
        "energy_v2_notes",
    ]
    # Trimmed to 11 for the two decision/curation surfaces (v2 Rank-1
    # Mechanistic Candidate, v2 Claim Review Queue): identity, verdict,
    # the two overlap-class axes, truth status, and blockers. Every other
    # v2_cols_full field stays queryable via the Candidate Pool table and the
    # v2 CSV export (export_proxy_candidate_energy_v2_csv.py).
    v2_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "device_type",
        "target_event_type",
        "match_scope",
        "mechanistic_energy_candidate_status",
        "proxy_claim_status",
        "candidate_failure_fraction_overlap_class",
        "terminal_energy_overlap_class",
        "truth_validation_status",
        "energy_v2_blockers",
    ]
    v2_rank1_filter = sql_filter("mechanistic_energy_candidate_rank = 1")
    v3_rank1_filter = sql_filter("combined_rank = 1")

    # Full 26-column detail, kept only for the forensic "Candidate Pool"
    # dump (Physics & Raw tab).
    v3_cols_full = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "device_type",
        "target_event_type",
        "candidate_source",
        "match_scope",
        "waveform_rank",
        "energy_rank",
        "combined_rank",
        "combined_vector_distance",
        "signature_axis_distance",
        "duration_log_delta",
        "log_energy_delta_dex",
        "candidate_failure_fraction_overlap_class",
        "terminal_energy_overlap_class",
        "failure_fraction_log_delta",
        "best_damage_distance",
        "damage_signature_coverage_gap",
        "regime_match_class",
        "path_penalty",
        "proxy_claim_status",
        "proxy_claim_basis",
        "mechanistic_energy_candidate_status",
        "truth_validation_status",
        "energy_v2_blockers",
        "energy_v2_notes",
    ]
    # Trimmed to 10 for the v3 Rank-1 Combined Candidate decision table;
    # the component-share/delta breakdown lives in the interactive viewer's
    # v3 vector explorer (see the Physics & Raw tab markdown link) and the
    # full column set stays available via the Candidate Pool table.
    v3_cols = [
        "target_stress_record_key",
        "candidate_stress_record_key",
        "device_type",
        "target_event_type",
        "candidate_source",
        "match_scope",
        "waveform_rank",
        "energy_rank",
        "combined_rank",
        "combined_vector_distance",
    ]
    combined_setting_cols = [
        "setting_name", "description", "signature_axis_weight",
        "duration_weight", "log_energy_weight", "failure_fraction_weight",
        "post_iv_damage_weight", "regime_path_weight", "coverage_gap_weight",
    ]
    # Trimmed from 23 to the fields needed to triage a conflict row: which
    # target, which conflict, each method's pick, and enrichment/truth
    # status. same_device_source_conflict, c2m0080120d_avalanche_vs_sc_conflict,
    # the raw pick keys, and the two overlap-class columns stay in the view
    # for drill-through and the concordance CSV export.
    concordance_cols = [
        "conflict_priority",
        "target_stress_record_key",
        "device_type",
        "target_event_type",
        "v2_match_scope",
        "v2_pick_source",
        "v1_signature_pick_source",
        "v2_pick_dssig_percentile",
        "enrichment_band",
        "truth_validation_status",
    ]
    candidate_boundary_cols = [
        "boundary_scope", "device_type", "voltage_class", "source",
        "test_timescale_class", "survived_count", "destructive_count",
        "unknown_outcome_count", "record_count", "boundary_low_j",
        "boundary_high_j", "boundary_inverted", "boundary_energy_basis",
        "boundary_energy_basis_family", "boundary_blockers", "boundary_notes",
        "boundary_usable",
    ]

    defs = [
        # 2026-07-22 redesign: one equal-width, equal-height KPI row (6 tiles
        # x width 2 = 12) instead of a 4-KPI row followed by a 5th tile
        # orphaned alone in a mostly-empty row.
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
            2,
            16,
            TAB_READINESS,
            None,
        ),
        (
            "Proxy Readiness - v1 Targets Ranked",
            dataset_ids["candidates"],
            "big_number_total",
            big_number_params(
                "v1 ranked targets",
                "COUNT(DISTINCT CASE WHEN candidate_rank = 1 THEN target_stress_record_key END)",
                "waveform_rank population; includes energy-censored targets",
                number_format=",d",
            ),
            2, 16, TAB_READINESS, None,
        ),
        (
            "Proxy Readiness - v2 Targets Ranked",
            dataset_ids["candidates_v2"],
            "big_number_total",
            big_number_params(
                "v2 ranked targets",
                "COUNT(DISTINCT CASE WHEN mechanistic_energy_candidate_rank = 1 THEN target_stress_record_key END)",
                "energy-rankable targets; censored targets are v1-only",
                number_format=",d",
            ),
            2, 16, TAB_READINESS, None,
        ),
        (
            "Proxy Readiness - v3 Targets Ranked",
            dataset_ids["candidates_v3"],
            "big_number_total",
            big_number_params(
                "v3 ranked targets",
                "COUNT(DISTINCT CASE WHEN combined_rank = 1 THEN target_stress_record_key END)",
                "combined vector over v2 top-10 pool",
                number_format=",d",
            ),
            2, 16, TAB_READINESS, None,
        ),
        (
            "Proxy Readiness - Concordance Median Enrichment Percentile",
            dataset_ids["concordance_enrichment"],
            "big_number_total",
            big_number_params(
                "median enrichment percentile",
                "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v2_pick_dssig_percentile)",
                "v2 pick location in v1 signature ordering; lower is better",
                number_format=".1f",
            ),
            2, 16, TAB_READINESS, "concordance",
        ),
        (
            "Proxy Readiness - Curation Queue Depth",
            dataset_ids["concordance_enrichment"],
            "big_number_total",
            big_number_params(
                "curation queue depth",
                "COUNT(*) FILTER (WHERE source_conflict OR v2_proxy_claim_status IN ('validation_candidate', 'curation_candidate'))",
                "targets needing truth-label adjudication or conflict review",
                number_format=",d",
            ),
            2, 16, TAB_READINESS, "concordance",
        ),
        (
            "Proxy Readiness - Gate Zero Status",
            dataset_ids["gate_zero"],
            "table",
            table_params(gate_cols, row_limit=1),
            12,
            16,
            TAB_READINESS,
            None,
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
            "Proxy Readiness - v1 Waveform Candidate Summary",
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
            "Proxy Readiness - Best v1 Waveform Candidates",
            dataset_ids["candidates"],
            "table",
            table_params(
                candidate_cols,
                row_limit=1000,
                order_by=[["signature_axis_distance", True]],
                filters=[sql_filter("waveform_rank = 1")],
            ),
            12,
            40,
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
            # 2026-07-23: demoted off Concordance to keep that tab visual-first;
            # the wide curation queues live here as forensic drill-through.
            TAB_PHYSICS,
            "candidate",
        ),
        (
            "Proxy Readiness - Signature Axis Distance by Evidence Class",
            dataset_ids["candidates"],
            "echarts_timeseries_scatter",
            {
                **scatter_params(
                    "damage_signature_evidence_tier",
                    "signature_axis_distance",
                    "Evidence tier (1=full ... 4=collapse-only; lower is richer)",
                    "signature_axis_distance",
                    groupby=[
                        "candidate_source",
                        "damage_signature_evidence_class",
                        "target_stress_record_key",
                        "candidate_stress_record_key",
                    ],
                    filters=[top_rank_filter],
                    show_legend=True,
                ),
                "y_axis_title": "signature_axis_distance (energy-free)",
                "_description": (
                    "Distribution by evidence tier using signature_axis_distance, "
                    "the prior-free and energy-free v1 comparator. Superset renders "
                    "this as a jittered scatter surrogate for the planned strip/box "
                    "form."
                ),
            },
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
                "waveform_only_distance",
                "best_damage_distance",
                "waveform_only_distance",
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
            "Proxy Readiness - Irradiation Depletion Threshold Ratio vs Blocking Bias",
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
            50,
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
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
            TAB_PHYSICS,
            None,
        ),
        (
            "Proxy Readiness - v2 Energy Equivalence Parity (Severity)",
            dataset_ids["candidates_v2"],
            "echarts_timeseries_scatter",
            scatter_params(
                "target_severity_point_ratio",
                "candidate_failure_fraction_point",
                "Target severity ratio (÷ SEB/SELC critical)",
                "Candidate failure fraction (÷ own electrical threshold, log)",
                groupby=[
                    "candidate_failure_fraction_overlap_class",
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
            "Proxy Readiness - v2 Failure-Fraction Overlap by Scope",
            dataset_ids["candidates_v2"],
            "echarts_timeseries_bar",
            bar_params(
                "candidate_failure_fraction_overlap_class",
                "Candidate failure-fraction overlap class",
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
            # 2026-07-23: demoted off Concordance (visual-first) to Raw.
            TAB_PHYSICS,
            "candidate_v2",
        ),
        (
            "Proxy Readiness - v2 Candidate Pool (Top 10)",
            dataset_ids["candidates_v2"],
            "table",
            table_params(
                v2_cols_full,
                row_limit=2000,
                order_by=[
                    ["target_stress_record_key", True],
                    ["mechanistic_energy_candidate_rank", True],
                ],
            ),
            12,
            56,
            TAB_PHYSICS,
            "candidate_v2",
        ),
    ]

    # 2026-07-22 redesign: every chart above already carries its deployed
    # name/tab/columns directly (no post-hoc kill/move/rename pass) so this
    # list is the actual deployed set, not an intermediate draft of it.
    out = list(defs)

    out.extend([
        (
            "Proxy Readiness - v2 Boundary Coverage",
            dataset_ids["candidates_v2"],
            "big_number_total",
            big_number_params(
                "usable own-boundary coverage",
                "100.0 * COUNT(*) FILTER (WHERE mechanistic_energy_candidate_rank = 1 AND candidate_failure_fraction_gate_usable) / NULLIF(COUNT(*) FILTER (WHERE mechanistic_energy_candidate_rank = 1), 0)",
                "failure-fraction axis is a data-gap tracker until this rises",
                number_format=".1f",
            ),
            4, 16, TAB_MECHANISTIC, "candidate_v2",
        ),
        (
            "Proxy Readiness - Candidate Destruction Boundary Data Gaps",
            dataset_ids["candidate_boundary"],
            "table",
            table_params(
                candidate_boundary_cols,
                row_limit=500,
                order_by=[["boundary_usable", False], ["unknown_outcome_count", False]],
                description="Candidate-side destruction-boundary rollup that decides when the v2 failure-fraction axis is usable.",
            ),
            12, 44, TAB_MECHANISTIC, None,
        ),
        (
            "Proxy Readiness - v3 Rank-1 Combined Candidate",
            dataset_ids["candidates_v3"],
            "table",
            table_params(
                v3_cols, row_limit=819,
                order_by=[["combined_vector_distance", True]],
                filters=[v3_rank1_filter],
                description="v3 combined-vector rank-1 picks. Screening-only and uncalibrated; weights are visible in the settings panel.",
            ),
            12, 52, TAB_V3, "candidate_v3",
        ),
        (
            "Proxy Readiness - v3 Combined Ranker Weights",
            dataset_ids["combined_settings"],
            "table",
            table_params(
                combined_setting_cols, row_limit=10,
                description="UNCALIBRATED / screening-only weights used by stress_proxy_candidate_combined_v3.",
            ),
            12, 22, TAB_V3, None,
        ),
        (
            "Proxy Readiness - v3 Candidate Pool (Top 10)",
            dataset_ids["candidates_v3"],
            "table",
            table_params(
                v3_cols_full, row_limit=2500,
                order_by=[["target_stress_record_key", True], ["combined_rank", True]],
            ),
            12, 56, TAB_PHYSICS, None,
        ),
        (
            "Proxy Readiness - Concordance Best-Decile Share",
            dataset_ids["concordance_enrichment"],
            "big_number_total",
            big_number_params(
                "best decile share",
                "100.0 * COUNT(*) FILTER (WHERE v2_pick_dssig_percentile <= 10.0) / NULLIF(COUNT(*), 0)",
                "v2 picks in top 10% of v1 prior-free signature ordering",
                number_format=".1f",
            ),
            3, 16, TAB_CONCORDANCE, "concordance",
        ),
        (
            "Proxy Readiness - Concordance Strict Rank-1 Agreement",
            dataset_ids["concordance_enrichment"],
            "big_number_total",
            big_number_params(
                "strict rank-1 agreement",
                "100.0 * COUNT(*) FILTER (WHERE prior_free_signature_rank1_agreement) / NULLIF(COUNT(*), 0)",
                "expected lower bound after v1/v2 separation",
                number_format=".1f",
            ),
            3, 16, TAB_CONCORDANCE, "concordance",
        ),
        (
            "Proxy Readiness - Concordance Energy-Blended Control Agreement",
            dataset_ids["concordance_enrichment"],
            "big_number_total",
            big_number_params(
                "energy-blended control",
                "100.0 * COUNT(*) FILTER (WHERE energy_blended_control_agreement) / NULLIF(COUNT(*), 0)",
                "circularity gauge; compare against enrichment",
                number_format=".1f",
            ),
            3, 16, TAB_CONCORDANCE, "concordance",
        ),
        (
            "Proxy Readiness - Truth-Label Coverage",
            dataset_ids["concordance_enrichment"],
            "big_number_total",
            big_number_params(
                "curated truth labels",
                "COUNT(*) FILTER (WHERE truth_validation_status IS DISTINCT FROM 'no_curated_truth')",
                "fail-closed coverage for v1/v2/v3 curation",
                number_format=",d",
            ),
            3, 16, TAB_CONCORDANCE, "concordance",
        ),
        (
            # 2026-07-23: visual-first centerpiece. One point per target's v2
            # energy-pick, both axes 819/819 populated (SQL-verified; the
            # union-view v1%xv2% map was rejected because v2_rank_percentile is
            # only ~53% populated and silently drops the conflicts). Single
            # groupby => clean palette colours by source (sc/avalanche); the
            # trade-off is per-point record identity, which the Method Conflict
            # Browser below and the interactive viewer carry instead.
            "Proxy Readiness - Method-Agreement Map",
            dataset_ids["concordance_enrichment"],
            "echarts_timeseries_scatter",
            scatter_params(
                "v2_pick_dssig_percentile",
                "v2_pick_signature_axis_distance",
                "v2 pick's percentile in v1 waveform ordering (left = methods agree)",
                "v2 pick signature distance (low = close waveform match)",
                groupby=["v2_pick_source"],
                show_legend=True,
                x_axis_bounds=[0.0, 100.0],
                y_axis_bounds=[0.0, None],
                description=(
                    "Each point is one target's v2 energy-ranked pick (819 "
                    "targets, both axes fully populated). X: where that pick "
                    "sits in v1's full energy-free waveform ordering -- far "
                    "left means the two independent methods agree it is a top "
                    "match. Y: the pick's own signature distance -- low means a "
                    "close waveform match. BOTTOM-LEFT = strong candidates "
                    "(both methods rank it high). FAR RIGHT = the methods "
                    "disagree; those are the curation targets, listed by record "
                    "in the Method Conflict Browser below. "
                    "READING THE Y-AXIS: distance is evidence-quantized, not "
                    "continuous. Colour is the proxy source, and it doubles as "
                    "an evidence-richness read: sc picks carry collapse+bias "
                    "signatures (richer) and form the genuine low-distance "
                    "cluster bottom-left; avalanche picks carry collapse-only "
                    "signatures (fewer axes), so their distance snaps to a few "
                    "discrete levels -- the horizontal stripe near ~3.9 is those "
                    "evidence-poor picks saturating, NOT 'moderately far "
                    "matches'. Only 19 of 819 v2 picks are sc; the rest are "
                    "avalanche. Hover shows source and the two values. For "
                    "per-record identity of any point, use the interactive "
                    "damage-signature viewer linked above."
                ),
            ),
            12,
            56,
            TAB_CONCORDANCE,
            "concordance",
        ),
        (
            "Proxy Readiness - Enrichment Distribution by Decile",
            dataset_ids["concordance_enrichment"],
            "echarts_timeseries_bar",
            bar_params(
                "v2_pick_dssig_decile",
                "v2-pick decile in v1 signature ordering",
                "rank-1 targets",
                groupby=["v2_match_scope"],
                metric_label="targets",
                description="Post-separation enrichment histogram: low deciles mean v2 energy picks concentrate near v1's energy-free signature-best ordering.",
            ),
            12, 38, TAB_CONCORDANCE, "concordance",
        ),
        (
            "Proxy Readiness - Method Conflict Browser",
            dataset_ids["concordance_enrichment"],
            "table",
            table_params(
                concordance_cols, row_limit=1000,
                order_by=[["conflict_priority", True], ["target_stress_record_key", True]],
                filters=[sql_filter("source_conflict")],
                description="Targets where v1 signature-best source differs from the v2 pick source; C2M0080120D avalanche-vs-SC rows sort first.",
            ),
            12, 56, TAB_CONCORDANCE, "concordance",
        ),
    ])
    return out


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
    deployed_defs = []
    chart_defs = build_chart_defs(dataset_ids)
    dataset_key_by_id = {dataset_id: key for key, dataset_id in dataset_ids.items()}
    for definition in chart_defs:
        name, ds_id, viz_type, params, width, height, tab, group = definition
        description = params.get("_description")
        if description is not None:
            params = {key: value for key, value in params.items()
                      if key != "_description"}
        dataset_key = dataset_key_by_id[ds_id]
        unit, evidence = DATASET_PROVENANCE[dataset_key]
        provenance = f"Unit of analysis: {unit}. Evidence basis: {evidence}."
        description = f"{description} {provenance}" if description else provenance
        cid, cuuid = create_chart(
            session, name, ds_id, viz_type, params, description=description
        )
        charts_info.append((cid, cuuid, name, width, height, tab))
        if not cid:
            continue
        chart_ids.append(cid)
        deployed_defs.append(definition)

    print("\nBuilding proxy-readiness dashboard layout...")
    position_json = build_dashboard_layout(charts_info, MARKDOWN_PANELS)
    chart_catalog = build_chart_catalog(deployed_defs, chart_ids)
    native_filters = build_native_filters(chart_ids, dataset_ids, chart_catalog)
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
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.schema_only:
        parser.error(
            "database model builds no longer belong to dashboards; run "
            "aps models build proxy-analytics"
        )
    if args.skip_schema:
        print("--skip-schema is obsolete; dashboards now always consume prepared models.")

    create_dashboard()


if __name__ == "__main__":
    main()
