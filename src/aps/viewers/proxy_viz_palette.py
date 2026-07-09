"""Shared proxy-readiness visualization palettes.

The dashboard builder and the interactive viewer both encode source, claim
status, overlap, and concordance meaning with color.  Keeping the mappings in
one module prevents drift between Superset and the static Plotly viewer.
"""

from __future__ import annotations

SOURCE_ORDER = ("irradiation", "sc", "avalanche")
SOURCE_COLORS = {
    "irradiation": "#0067a5",
    "sc": "#c45100",
    "avalanche": "#007c72",
}
SOURCE_STYLES = {
    "irradiation": {
        "name": "Irradiation",
        "color": SOURCE_COLORS["irradiation"],
        "symbol": "circle",
        "size": 3,
        "opacity": 0.42,
    },
    "avalanche": {
        "name": "Avalanche",
        "color": SOURCE_COLORS["avalanche"],
        "symbol": "diamond",
        "size": 3,
        "opacity": 0.45,
    },
    "sc": {
        "name": "Short circuit",
        "color": SOURCE_COLORS["sc"],
        "symbol": "square",
        "size": 6,
        "opacity": 0.95,
    },
}

EVENT_TYPE_COLORS = {
    "SEB": "#0067a5",
    "SELCI": "#b54600",
    "SELCII": "#6b4c9a",
    "MIXED": "#008571",
    "UNKNOWN": "#5f6670",
}
EVENT_TYPE_FALLBACK = "#5f6670"

CLAIM_STATUS_COLORS = {
    "validated": "#007a3d",
    "validation_candidate": "#005fcc",
    "curation_candidate": "#c45100",
    "screening_only": "#5f6670",
    "blocked": "#b00020",
    "no_curated_truth": "#4b5563",
    "validated_by_curated_measured_post_iv": "#007a3d",
    "curated_equivalent_non_measured": "#007c72",
    "curated_not_equivalent": "#b00020",
    "curated_uncertain": "#c45100",
}

OVERLAP_COLORS = {
    "strong_overlap": "#123f5a",
    "partial_overlap": "#2b5870",
    "near_miss": "#477089",
    "far_miss": "#66879c",
    "missing_interval": "#4b5563",
}

CONFLICT_ACCENT = "#b00020"
DEEMPHASIS_GRAY = "#6b7280"

CONCORDANCE_STYLE = {
    "consensus": ("#007a3d", "diamond"),
    "v2_pick": ("#c45100", "circle"),
    "v1_pick": ("#005fcc", "square"),
    "strong_disagree": (CONFLICT_ACCENT, "circle"),
    "conflict_focus": (CONFLICT_ACCENT, "diamond"),
}

V3_COMPONENT_COLORS = {
    "signature": "#005f9e",
    "duration": "#9b5d00",
    "failure fraction": "#a63d00",
    "regime/path": "#6b4c9a",
    "terminal energy": "#007a5e",
    "post-IV damage": "#0077a7",
    "coverage gap": "#4b5563",
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
    "robustness": "#d62728",
    "reliability": "#1f77b4",
    "radiation": "#54a24b",
    "unknown": "#9d755d",
    "energy_comparable": "#1f77b4",
    "energy_censored_damage_signature_only": "#9467bd",
    "thermal_runaway_pair": "#2ca02c",
    "thermal_runaway_pair_secondary": "#17becf",
    "gate_oxide_pair_repetitive_only": "#bcbd22",
    "cumulative_defect_no_electrical_analog": "#8c6d31",
    "LET 00-05": "#fec44f",
    "LET 05-15": "#fe9929",
    "LET 15-25": "#ec7014",
    "LET 25-50": "#cc4c02",
    "LET 50-80": "#993404",
    "LET 80+": "#662506",
    "LET n/a": "#969696",
    **SOURCE_COLORS,
    **EVENT_TYPE_COLORS,
    **CLAIM_STATUS_COLORS,
    **OVERLAP_COLORS,
}
