#!/usr/bin/env python3
"""
Build one interactive HTML viewer for both APS damage-signature-space 3D plots.

The HTML uses the already-exported source-record and pairwise-delta CSV files.
When the downloaded Plotly browser asset is present beside those files, the
runtime is embedded into the HTML and the viewer works offline.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


OUT_DIR = Path("out/avalanche_irrad_pilot")
SOURCE_CSV = OUT_DIR / "damage_signature_sources_3d.csv"
DELTA_CSV = OUT_DIR / "damage_signature_delta_3d.csv"
PLOTLY_ASSET = OUT_DIR / "plotly-2.35.2.min.js"
PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"
OUTPUT_HTML = OUT_DIR / "damage_signature_3d_interactive.html"


SOURCE_STYLES = {
    "irradiation": {
        "name": "Irradiation",
        "color": "#377eb8",
        "symbol": "circle",
        "size": 3,
        "opacity": 0.42,
    },
    "avalanche": {
        "name": "Avalanche",
        "color": "#1b9e77",
        "symbol": "diamond",
        "size": 3,
        "opacity": 0.45,
    },
    "sc": {
        "name": "Short circuit",
        "color": "#e66101",
        "symbol": "square",
        "size": 6,
        "opacity": 0.95,
    },
}

DELTA_STYLES = {
    "avalanche": {
        "name": "Irradiation vs avalanche",
        "color": "#1b9e77",
        "symbol": "diamond",
        "size": 3,
        "opacity": 0.38,
    },
    "sc": {
        "name": "Irradiation vs SC",
        "color": "#d95f02",
        "symbol": "circle",
        "size": 3,
        "opacity": 0.42,
    },
}


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def display_value(value: Any, digits: int = 5) -> str:
    if value is None or pd.isna(value):
        return "not recorded"
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        value = float(value)
        if value.is_integer():
            return str(int(value))
        return f"{value:.{digits}g}"
    text = str(value)
    return text if text else "not recorded"


def display_joules(value: Any) -> str:
    """Format a joule value in scientific notation, 3 significant digits."""
    if value is None or pd.isna(value):
        return "not recorded"
    value = float(value)
    if value == 0.0:
        return "0 J"
    return f"{value:.3g} J"


def display_stored_energy(value: Any) -> str:
    """Format a stored depletion areal energy (J/cm2) as uJ/cm2."""
    if value is None or pd.isna(value):
        return "not recorded"
    return f"{float(value) * 1e6:.3g} uJ/cm2"


def display_ratio(value: Any) -> str:
    """Format a unitless ratio, 3 significant digits."""
    return display_value(value, digits=3)


def cell(row: Any, name: str, formatter=display_value) -> str:
    """Format an optional named field from an itertuples row.

    Returns ``not recorded`` when the column is absent (older CSV exports) or
    null, so the viewer never crashes on a partially regenerated CSV.
    """
    return formatter(getattr(row, name, None))


def log10_or_na(series: pd.Series, na_value: float) -> np.ndarray:
    """log10 of positive values; ``na_value`` sentinel elsewhere (no imputation)."""
    values = numeric(series)
    return np.where(values.gt(0.0), np.log10(values.where(values.gt(0.0))), na_value)


def scaled_marker_size(series: pd.Series, minimum: float = 3.0,
                       maximum: float = 12.0) -> list[float]:
    """Map a non-negative magnitude to a marker-size range; missing -> minimum."""
    values = numeric(series).clip(lower=0.0)
    if values.notna().sum() == 0:
        return [minimum] * len(values)
    vmax = float(values.max()) or 1.0
    return (minimum + (maximum - minimum) * values.fillna(0.0) / vmax).tolist()


# Irradiation event-type palette, shared with the dashboard CANDIDATE_COLORS.
EVENT_TYPE_COLORS = {
    "SEB": "#54a24b",
    "SELCI": "#e45756",
    "SELCII": "#72b7b2",
    "MIXED": "#b279a2",
    "UNKNOWN": "#9d755d",
}
EVENT_TYPE_FALLBACK = "#9d755d"


def json_for_html(value: Any) -> str:
    text = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return text.replace("</", "<\\/")


def mesh_plane(
    *,
    x: list[float],
    y: list[float],
    z: list[float],
    color: str = "#777777",
    opacity: float = 0.045,
) -> dict[str, Any]:
    return {
        "type": "mesh3d",
        "x": x,
        "y": y,
        "z": z,
        "i": [0, 0],
        "j": [1, 2],
        "k": [2, 3],
        "color": color,
        "opacity": opacity,
        "hoverinfo": "skip",
        "showlegend": False,
    }


def log_decade_ticks(minimum: float, maximum: float) -> tuple[list[float], list[str]]:
    """Return (log10 positions, decade labels) spanning [minimum, maximum]."""
    lo = math.floor(math.log10(minimum))
    hi = math.ceil(math.log10(maximum))
    positions = list(range(lo, hi + 1))
    return [float(p) for p in positions], [f"1e{p}" for p in positions]


def common_layout(title: str, scene: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": {
            "text": title,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 20},
        },
        "template": "plotly_white",
        "paper_bgcolor": "#ffffff",
        "plot_bgcolor": "#ffffff",
        "margin": {"l": 0, "r": 0, "t": 72, "b": 0},
        "legend": {
            "x": 0.01,
            "y": 0.99,
            "bgcolor": "rgba(255,255,255,0.82)",
            "bordercolor": "#d0d7de",
            "borderwidth": 1,
            "itemsizing": "constant",
        },
        "hoverlabel": {
            "bgcolor": "#ffffff",
            "font": {"color": "#17202a", "size": 12},
            "bordercolor": "#8c959f",
        },
        "scene": scene,
        "uirevision": title,
    }


def source_plot_payload(records: pd.DataFrame) -> dict[str, Any]:
    data = records.copy()
    for column in (
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "normalized_vds",
    ):
        data[column] = numeric(data[column])
    data = data[data["vds_collapse_fraction"].notna()].copy()

    positive_normalized = data.loc[
        data["normalized_vds"].gt(0.0),
        "normalized_vds",
    ]
    normalized_min = float(positive_normalized.min())
    normalized_max = float(positive_normalized.max())
    collapse_upper = max(
        1.0,
        float(data["vds_collapse_fraction"].max()) * 1.03,
    )
    gate_max = float(data["gate_delta_fraction"].max())
    gate_upper = max(0.85, gate_max * 1.08)
    gate_na = -0.12
    normalized_na = math.log10(normalized_min) - 0.35
    normalized_upper = math.log10(normalized_max) + 0.05

    data["plot_gate"] = data["gate_delta_fraction"].fillna(gate_na)
    data["plot_normalized_vds"] = np.where(
        data["normalized_vds"].gt(0.0),
        np.log10(data["normalized_vds"]),
        normalized_na,
    )

    traces: list[dict[str, Any]] = [
        mesh_plane(
            x=[0.0, collapse_upper, collapse_upper, 0.0],
            y=[gate_na, gate_na, gate_na, gate_na],
            z=[
                normalized_na,
                normalized_na,
                normalized_upper,
                normalized_upper,
            ],
        ),
        mesh_plane(
            x=[0.0, collapse_upper, collapse_upper, 0.0],
            y=[gate_na, gate_na, gate_upper, gate_upper],
            z=[
                normalized_na,
                normalized_na,
                normalized_na,
                normalized_na,
            ],
        ),
    ]

    hover_template = (
        "<b>%{customdata[0]}</b><br>"
        "Device: %{customdata[1]}<br>"
        "Event/type: %{customdata[2]}<br>"
        "Condition: %{customdata[3]}<br>"
        "File: %{customdata[4]}<br>"
        "Stress key: %{customdata[5]}<br>"
        "<br>Vds collapse fraction: %{x:.5g}<br>"
        "Gate-current fraction: %{customdata[6]}<br>"
        "Normalized Vds: %{customdata[7]}<br>"
        "<br><b>energy chain</b><br>"
        "Terminal energy: %{customdata[8]} (%{customdata[9]})<br>"
        "Radiation deposited (ionizing): %{customdata[10]}<br>"
        "Radiation deposited (total): %{customdata[11]}<br>"
        "Stored depletion energy: %{customdata[12]}<br>"
        "SEB ratio: %{customdata[13]} | SELC ratio: %{customdata[14]}<br>"
        "Depletion model: %{customdata[15]}<br>"
        "Predicted SEB / SELC V: %{customdata[16]} / %{customdata[17]}<br>"
        "Energy window basis: %{customdata[18]}"
        "<extra></extra>"
    )

    for source in ("irradiation", "avalanche", "sc"):
        group = data[data["source"].eq(source)]
        if group.empty:
            continue
        style = SOURCE_STYLES[source]
        customdata = [
            [
                style["name"],
                display_value(row.device_label),
                display_value(row.event_type),
                display_value(row.stress_condition_label),
                display_value(row.filename),
                display_value(row.stress_record_key),
                display_value(row.gate_delta_fraction),
                display_value(row.normalized_vds),
                cell(row, "electrical_terminal_energy_j", display_joules),
                cell(row, "electrical_terminal_energy_basis"),
                cell(row, "radiation_deposited_energy_j", display_joules),
                cell(row, "radiation_deposited_energy_total_j", display_joules),
                cell(row, "se_depletion_stored_energy_j_cm2", display_stored_energy),
                cell(row, "se_depletion_ratio_to_seb", display_ratio),
                cell(row, "se_depletion_ratio_to_selc", display_ratio),
                cell(row, "se_depletion_model_quality"),
                cell(row, "se_depletion_predicted_seb_voltage_v"),
                cell(row, "se_depletion_predicted_selc_voltage_v"),
                cell(row, "energy_window_basis"),
            ]
            for row in group.itertuples(index=False)
        ]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{style['name']} (n={len(group):,})",
                "x": group["vds_collapse_fraction"].astype(float).tolist(),
                "y": group["plot_gate"].astype(float).tolist(),
                "z": group["plot_normalized_vds"].astype(float).tolist(),
                "customdata": customdata,
                "hovertemplate": hover_template,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": style["size"],
                    "opacity": style["opacity"],
                    "line": {
                        "color": "#202124" if source == "sc" else style["color"],
                        "width": 1 if source == "sc" else 0,
                    },
                },
            }
        )

    normalized_ticks = [
        value
        for value in (0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 20.0, 30.0)
        if normalized_min * 0.75 <= value <= normalized_max * 1.25
    ]
    scene = {
        "dragmode": "orbit",
        "aspectmode": "manual",
        "aspectratio": {"x": 1.30, "y": 1.0, "z": 1.0},
        "camera": {"eye": {"x": 1.55, "y": 1.50, "z": 1.05}},
        "xaxis": {
            "title": {
                "text": "Vds collapse fraction<br>0 = none, 1 = full collapse"
            },
            "range": [0.0, collapse_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "yaxis": {
            "title": {"text": "Gate-current fraction<br>Ig / (Ig + Id)"},
            "range": [gate_na - 0.02, gate_upper],
            "tickvals": [gate_na, 0.0, 0.2, 0.4, 0.6, 0.8],
            "ticktext": ["not recorded", "0", "0.2", "0.4", "0.6", "0.8"],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "zaxis": {
            "title": {
                "text": "Normalized blocking voltage<br>|Vds| / device rating (log display)"
            },
            "range": [normalized_na, normalized_upper],
            "tickvals": [
                normalized_na,
                *(math.log10(value) for value in normalized_ticks),
            ],
            "ticktext": [
                "not recorded",
                *(display_value(value, digits=2) for value in normalized_ticks),
            ],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
    }
    return {
        "traces": traces,
        "layout": common_layout(
            "Individual irradiation, short-circuit, and avalanche records",
            scene,
        ),
        "note": (
            f"Each marker is one independent stress record: "
            f"{len(data[data['source'].eq('irradiation')]):,} irradiation, "
            f"{len(data[data['source'].eq('sc')]):,} SC, and "
            f"{len(data[data['source'].eq('avalanche')]):,} avalanche. "
            "SC and avalanche gate current was not recorded, so those points "
            "use the labelled not-recorded plane. Avalanche normalized-Vds "
            "values are shown as stored but have a known scaling artifact."
        ),
    }


def delta_plot_payload(comparisons: pd.DataFrame) -> dict[str, Any]:
    data = comparisons.copy()
    for column in (
        "collapse_delta",
        "gate_delta",
        "normalized_vds_delta",
    ):
        data[column] = numeric(data[column])
    data = data[data["collapse_delta"].notna()].copy()

    collapse_upper = max(1.0, float(data["collapse_delta"].max()) * 1.05)
    gate_values = data["gate_delta"].dropna()
    gate_upper = (
        max(1.0, float(gate_values.max()) * 1.05)
        if not gate_values.empty
        else 1.0
    )
    normalized_values = data["normalized_vds_delta"].dropna()
    normalized_upper = (
        max(0.5, float(normalized_values.max()) * 1.08)
        if not normalized_values.empty
        else 1.0
    )
    gate_na = -0.08 * gate_upper
    normalized_na = -0.08 * normalized_upper

    data["plot_gate"] = data["gate_delta"].fillna(gate_na)
    data["plot_normalized_vds"] = data["normalized_vds_delta"].fillna(
        normalized_na
    )

    traces: list[dict[str, Any]] = [
        mesh_plane(
            x=[0.0, collapse_upper, collapse_upper, 0.0],
            y=[gate_na, gate_na, gate_na, gate_na],
            z=[
                normalized_na,
                normalized_na,
                normalized_upper,
                normalized_upper,
            ],
        ),
        mesh_plane(
            x=[0.0, collapse_upper, collapse_upper, 0.0],
            y=[gate_na, gate_na, gate_upper, gate_upper],
            z=[
                normalized_na,
                normalized_na,
                normalized_na,
                normalized_na,
            ],
        ),
    ]

    hover_template = (
        "<b>%{customdata[0]}</b><br>"
        "Irradiation device: %{customdata[1]}<br>"
        "Irradiation event: %{customdata[2]}<br>"
        "Ion: %{customdata[3]}<br>"
        "Irradiation file: %{customdata[4]}<br>"
        "<br>Proxy device: %{customdata[5]}<br>"
        "Proxy event/type: %{customdata[6]}<br>"
        "Proxy condition: %{customdata[7]}<br>"
        "Proxy file: %{customdata[8]}<br>"
        "Scope / rank: %{customdata[9]} / %{customdata[10]}<br>"
        "Status: %{customdata[11]}<br>"
        "<br>Collapse delta: %{x:.5g}<br>"
        "Gate delta: %{customdata[12]}<br>"
        "Normalized-Vds delta: %{customdata[13]}<br>"
        "<br><b>energy context</b><br>"
        "Target SEB / SELC ratio: %{customdata[14]} / %{customdata[15]}<br>"
        "Target deposited (ionizing): %{customdata[16]}<br>"
        "Target terminal energy: %{customdata[17]} (%{customdata[18]})<br>"
        "Proxy terminal energy: %{customdata[19]} (%{customdata[20]})<br>"
        "Energy-density ratio: %{customdata[21]}<br>"
        "Log energy delta: %{customdata[22]}<br>"
        "Damage-signature distance: %{customdata[23]}<br>"
        "<br><b>evidence coverage</b><br>"
        "Evidence class: %{customdata[26]}<br>"
        "Available axes: %{customdata[27]}<br>"
        "Missing axes: %{customdata[28]}<br>"
        "Coverage score: %{customdata[29]}<br>"
        "Coverage-adjusted distance (diag.): %{customdata[30]}<br>"
        "Mechanism match: %{customdata[24]}<br>"
        "Blockers: %{customdata[25]}"
        "<extra></extra>"
    )

    for source in ("avalanche", "sc"):
        group = data[data["candidate_source"].eq(source)]
        if group.empty:
            continue
        style = DELTA_STYLES[source]
        customdata = [
            [
                style["name"],
                display_value(row.target_device_label),
                display_value(row.target_event_type),
                display_value(row.target_ion_species),
                display_value(row.target_filename),
                display_value(row.candidate_device_label),
                display_value(row.candidate_event_type),
                display_value(row.candidate_stress_condition_label),
                display_value(row.candidate_filename),
                display_value(row.match_scope),
                display_value(row.candidate_rank),
                display_value(row.candidate_status),
                display_value(row.gate_delta),
                display_value(row.normalized_vds_delta),
                cell(row, "target_se_depletion_ratio_to_seb", display_ratio),
                cell(row, "target_se_depletion_ratio_to_selc", display_ratio),
                cell(row, "target_radiation_deposited_energy_j", display_joules),
                cell(row, "target_energy_j", display_joules),
                cell(row, "target_energy_basis"),
                cell(row, "candidate_energy_j", display_joules),
                cell(row, "candidate_energy_basis"),
                cell(row, "energy_density_ratio", display_ratio),
                cell(row, "log_energy_delta", display_ratio),
                cell(row, "damage_signature_distance", display_ratio),
                cell(row, "mechanism_match_class"),
                cell(row, "candidate_blockers"),
                cell(row, "damage_signature_evidence_class"),
                cell(row, "damage_signature_available_axes"),
                cell(row, "damage_signature_missing_axes"),
                cell(row, "damage_signature_coverage_score", display_ratio),
                cell(
                    row,
                    "coverage_adjusted_damage_signature_distance",
                    display_ratio,
                ),
            ]
            for row in group.itertuples(index=False)
        ]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{style['name']} (n={len(group):,})",
                "x": group["collapse_delta"].astype(float).tolist(),
                "y": group["plot_gate"].astype(float).tolist(),
                "z": group["plot_normalized_vds"].astype(float).tolist(),
                "customdata": customdata,
                "hovertemplate": hover_template,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": style["size"],
                    "opacity": style["opacity"],
                    "line": {"color": style["color"], "width": 0},
                },
            }
        )

    normalized_ticks = np.linspace(0.0, normalized_upper, 6)
    scene = {
        "dragmode": "orbit",
        "aspectmode": "manual",
        "aspectratio": {"x": 1.30, "y": 1.0, "z": 1.0},
        "camera": {"eye": {"x": 1.55, "y": 1.50, "z": 1.05}},
        "xaxis": {
            "title": {
                "text": "collapse_delta<br>|candidate - irradiation|"
            },
            "range": [0.0, collapse_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "yaxis": {
            "title": {"text": "gate_delta<br>|candidate - irradiation|"},
            "range": [gate_na, gate_upper],
            "tickvals": [gate_na, 0.0, 0.25, 0.5, 0.75, 1.0],
            "ticktext": ["not recorded", "0", "0.25", "0.5", "0.75", "1"],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "zaxis": {
            "title": {
                "text": "normalized_vds_delta<br>|candidate - irradiation|"
            },
            "range": [normalized_na, normalized_upper],
            "tickvals": [normalized_na, *normalized_ticks.tolist()],
            "ticktext": [
                "not recorded",
                *(display_value(value, digits=2) for value in normalized_ticks),
            ],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
    }
    return {
        "traces": traces,
        "layout": common_layout(
            "Ranked irradiation-to-proxy comparisons in delta space",
            scene,
        ),
        "note": (
            f"Each marker is one ranked irradiation-to-proxy comparison: "
            f"{len(data[data['candidate_source'].eq('avalanche')]):,} "
            "irradiation-to-avalanche and "
            f"{len(data[data['candidate_source'].eq('sc')]):,} "
            "irradiation-to-SC. Distances are not equally evidenced: avalanche "
            "comparisons are collapse-only (normalized-Vds delta excluded by "
            "design, gate delta unavailable), while SC comparisons are "
            "collapse + normalized-Vds. Read the per-point evidence class and "
            "blockers before comparing ranks across the two cohorts."
        ),
    }


def energy_context_plot_payload(records: pd.DataFrame) -> dict[str, Any]:
    """Irradiation depletion susceptibility vs released terminal energy.

    x = normalized blocking bias, y = stored depletion energy / SEB threshold,
    z = terminal electrical energy (log10 display). Marker size scales with the
    Vds collapse fraction. Only irradiation rows carry a depletion model, so
    rows without an SEB ratio are excluded rather than imputed.
    """
    data = records.copy()
    for column in (
        "normalized_vds",
        "se_depletion_ratio_to_seb",
        "se_depletion_ratio_to_selc",
        "electrical_terminal_energy_j",
        "vds_collapse_fraction",
    ):
        data[column] = numeric(data[column]) if column in data else np.nan
    data = data[
        data["normalized_vds"].notna()
        & data["se_depletion_ratio_to_seb"].notna()
    ].copy()

    empty_note = (
        "No rows carry both a normalized blocking bias and a modeled SEB "
        "stored-energy ratio, so the energy-context view has no comparable "
        "points. This depends on the depletion model being populated for "
        "irradiation rows."
    )
    if data.empty:
        return {
            "traces": [],
            "layout": common_layout(
                "Irradiation depletion susceptibility vs terminal energy",
                {"dragmode": "orbit"},
            ),
            "note": empty_note,
        }

    terminal = data["electrical_terminal_energy_j"]
    positive = terminal[terminal.gt(0.0)]
    if not positive.empty:
        t_min = float(positive.min())
        t_max = float(positive.max())
        z_na = math.log10(t_min) - 0.6
        z_top = math.log10(t_max) + 0.1
    else:
        z_na, z_top = -1.0, 1.0
    data["plot_z"] = log10_or_na(terminal, z_na)

    x_upper = max(1.0, float(data["normalized_vds"].max()) * 1.03)
    y_upper = max(1.2, float(data["se_depletion_ratio_to_seb"].max()) * 1.05)

    traces: list[dict[str, Any]] = [
        # Terminal-energy not-recorded floor (display only, never zero).
        mesh_plane(
            x=[0.0, x_upper, x_upper, 0.0],
            y=[0.0, 0.0, y_upper, y_upper],
            z=[z_na, z_na, z_na, z_na],
        ),
        # SEB threshold plane at ratio = 1.0.
        mesh_plane(
            x=[0.0, x_upper, x_upper, 0.0],
            y=[1.0, 1.0, 1.0, 1.0],
            z=[z_na, z_na, z_top, z_top],
            color="#d62728",
            opacity=0.07,
        ),
    ]

    hover_template = (
        "<b>%{customdata[0]}</b><br>"
        "Device: %{customdata[1]}<br>"
        "File: %{customdata[2]}<br>"
        "Stress key: %{customdata[3]}<br>"
        "<br>Normalized Vds: %{x:.4g}<br>"
        "SEB ratio: %{y:.3g}<br>"
        "SELC ratio: %{customdata[4]}<br>"
        "Stored depletion energy: %{customdata[5]}<br>"
        "Terminal energy: %{customdata[6]} (%{customdata[7]})<br>"
        "Radiation deposited (ionizing): %{customdata[8]}<br>"
        "Vds collapse fraction: %{customdata[9]}<br>"
        "Depletion model: %{customdata[10]}"
        "<extra></extra>"
    )

    for event_type, group in data.groupby("event_type", sort=True):
        if group.empty:
            continue
        color = EVENT_TYPE_COLORS.get(str(event_type), EVENT_TYPE_FALLBACK)
        customdata = [
            [
                display_value(event_type),
                display_value(row.device_label),
                display_value(row.filename),
                display_value(row.stress_record_key),
                cell(row, "se_depletion_ratio_to_selc", display_ratio),
                cell(row, "se_depletion_stored_energy_j_cm2", display_stored_energy),
                cell(row, "electrical_terminal_energy_j", display_joules),
                cell(row, "electrical_terminal_energy_basis"),
                cell(row, "radiation_deposited_energy_j", display_joules),
                cell(row, "vds_collapse_fraction", display_ratio),
                cell(row, "se_depletion_model_quality"),
            ]
            for row in group.itertuples(index=False)
        ]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{event_type} (n={len(group):,})",
                "x": group["normalized_vds"].astype(float).tolist(),
                "y": group["se_depletion_ratio_to_seb"].astype(float).tolist(),
                "z": group["plot_z"].astype(float).tolist(),
                "customdata": customdata,
                "hovertemplate": hover_template,
                "marker": {
                    "color": color,
                    "symbol": "circle",
                    "size": scaled_marker_size(group["vds_collapse_fraction"]),
                    "opacity": 0.6,
                    "line": {"color": color, "width": 0},
                },
            }
        )

    z_tickvals, z_ticktext = ([], [])
    if not positive.empty:
        z_tickvals, z_ticktext = log_decade_ticks(t_min, t_max)
    scene = {
        "dragmode": "orbit",
        "aspectmode": "manual",
        "aspectratio": {"x": 1.25, "y": 1.0, "z": 1.0},
        "camera": {"eye": {"x": 1.6, "y": 1.5, "z": 1.05}},
        "xaxis": {
            "title": {"text": "Normalized blocking voltage<br>|Vds| / device rating"},
            "range": [0.0, x_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "yaxis": {
            "title": {
                "text": "Stored depletion energy / SEB threshold<br>1.0 = threshold (red plane)"
            },
            "range": [0.0, y_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "zaxis": {
            "title": {"text": "Terminal electrical energy<br>(J, log display)"},
            "range": [z_na, z_top],
            "tickvals": [z_na, *z_tickvals],
            "ticktext": ["not recorded", *z_ticktext],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
    }
    return {
        "traces": traces,
        "layout": common_layout(
            "Irradiation depletion susceptibility vs terminal energy",
            scene,
        ),
        "note": (
            "Each marker is one irradiation record with a modeled depletion "
            "ratio. x is normalized blocking bias, y is stored depletion "
            "energy over the Kosier SEB threshold (the red plane marks ratio "
            "1.0), and z is terminal electrical energy on a log display. "
            "Marker size grows with Vds collapse fraction. Terminal energy is "
            "modeled as not recorded on the floor plane, never zero. The "
            "depletion model is estimated from rated voltage and active SiC "
            "thickness."
        ),
    }


def energy_delta_plot_payload(comparisons: pd.DataFrame) -> dict[str, Any]:
    """Proxy energy context: damage-signature vs energy mismatch in 3D.

    x = damage-signature distance, y = log energy delta, z = log10 of the
    proxy/target active-volume energy-density ratio. Rows whose energy-density
    ratio is missing or non-positive are placed on an explicit not-comparable
    plane instead of being imputed as zero.
    """
    data = comparisons.copy()
    for column in ("damage_signature_distance", "log_energy_delta", "energy_density_ratio"):
        data[column] = numeric(data[column]) if column in data else np.nan
    data = data[
        data["damage_signature_distance"].notna()
        & data["log_energy_delta"].notna()
    ].copy()

    empty_note = (
        "No ranked comparisons carry both a damage-signature distance and a "
        "log energy delta, so the proxy-energy-context view has no comparable "
        "points."
    )
    if data.empty:
        return {
            "traces": [],
            "layout": common_layout(
                "Proxy energy context: damage signature vs energy mismatch",
                {"dragmode": "orbit"},
            ),
            "note": empty_note,
        }

    ratio = data["energy_density_ratio"]
    positive = ratio[ratio.gt(0.0)]
    if not positive.empty:
        r_min = float(positive.min())
        r_max = float(positive.max())
        z_na = math.log10(r_min) - 0.6
        z_top = math.log10(r_max) + 0.1
    else:
        z_na, z_top = -1.0, 1.0
    data["plot_z"] = log10_or_na(ratio, z_na)

    x_upper = max(0.5, float(data["damage_signature_distance"].max()) * 1.05)
    y_values = data["log_energy_delta"]
    y_lower = min(0.0, float(y_values.min()) * 1.05)
    y_upper = max(0.5, float(y_values.max()) * 1.05)

    traces: list[dict[str, Any]] = [
        # Energy-density "not comparable" floor plane (display only).
        mesh_plane(
            x=[0.0, x_upper, x_upper, 0.0],
            y=[y_lower, y_lower, y_upper, y_upper],
            z=[z_na, z_na, z_na, z_na],
        ),
    ]

    hover_template = (
        "<b>%{customdata[0]}</b><br>"
        "Target device: %{customdata[1]}<br>"
        "Target event: %{customdata[2]}<br>"
        "Target file: %{customdata[3]}<br>"
        "<br>Proxy device: %{customdata[4]}<br>"
        "Proxy condition: %{customdata[5]}<br>"
        "Rank / status: %{customdata[6]} / %{customdata[7]}<br>"
        "<br>Target SEB / SELC ratio: %{customdata[8]} / %{customdata[9]}<br>"
        "Target deposited (ionizing): %{customdata[10]}<br>"
        "Target terminal energy: %{customdata[11]} (%{customdata[12]})<br>"
        "Proxy terminal energy: %{customdata[13]} (%{customdata[14]})<br>"
        "<br>Damage-signature distance: %{x:.4g}<br>"
        "Log energy delta: %{y:.4g}<br>"
        "Energy-density ratio: %{customdata[15]}<br>"
        "<br><b>evidence coverage</b><br>"
        "Evidence class: %{customdata[18]}<br>"
        "Available axes: %{customdata[19]}<br>"
        "Missing axes: %{customdata[20]}<br>"
        "Coverage score: %{customdata[21]}<br>"
        "Coverage-adjusted distance (diag.): %{customdata[22]}<br>"
        "Mechanism match: %{customdata[16]}<br>"
        "Blockers: %{customdata[17]}"
        "<extra></extra>"
    )

    for source in ("avalanche", "sc"):
        group = data[data["candidate_source"].eq(source)]
        if group.empty:
            continue
        style = DELTA_STYLES[source]
        customdata = [
            [
                style["name"],
                display_value(row.target_device_label),
                display_value(row.target_event_type),
                display_value(row.target_filename),
                display_value(row.candidate_device_label),
                cell(row, "candidate_stress_condition_label"),
                display_value(row.candidate_rank),
                display_value(row.candidate_status),
                cell(row, "target_se_depletion_ratio_to_seb", display_ratio),
                cell(row, "target_se_depletion_ratio_to_selc", display_ratio),
                cell(row, "target_radiation_deposited_energy_j", display_joules),
                cell(row, "target_energy_j", display_joules),
                cell(row, "target_energy_basis"),
                cell(row, "candidate_energy_j", display_joules),
                cell(row, "candidate_energy_basis"),
                cell(row, "energy_density_ratio", display_ratio),
                cell(row, "mechanism_match_class"),
                cell(row, "candidate_blockers"),
                cell(row, "damage_signature_evidence_class"),
                cell(row, "damage_signature_available_axes"),
                cell(row, "damage_signature_missing_axes"),
                cell(row, "damage_signature_coverage_score", display_ratio),
                cell(
                    row,
                    "coverage_adjusted_damage_signature_distance",
                    display_ratio,
                ),
            ]
            for row in group.itertuples(index=False)
        ]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{style['name']} (n={len(group):,})",
                "x": group["damage_signature_distance"].astype(float).tolist(),
                "y": group["log_energy_delta"].astype(float).tolist(),
                "z": group["plot_z"].astype(float).tolist(),
                "customdata": customdata,
                "hovertemplate": hover_template,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": style["size"],
                    "opacity": style["opacity"],
                    "line": {"color": style["color"], "width": 0},
                },
            }
        )

    z_tickvals, z_ticktext = ([], [])
    if not positive.empty:
        z_tickvals, z_ticktext = log_decade_ticks(r_min, r_max)
    n_not_comparable = int((~ratio.gt(0.0)).sum())
    scene = {
        "dragmode": "orbit",
        "aspectmode": "manual",
        "aspectratio": {"x": 1.25, "y": 1.0, "z": 1.0},
        "camera": {"eye": {"x": 1.6, "y": 1.5, "z": 1.05}},
        "xaxis": {
            "title": {"text": "Damage-signature distance<br>0 = identical signature"},
            "range": [0.0, x_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "yaxis": {
            "title": {"text": "Log energy delta<br>|log(proxy / target energy)|"},
            "range": [y_lower, y_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "zaxis": {
            "title": {"text": "Proxy / target energy-density ratio<br>(log10 display)"},
            "range": [z_na, z_top],
            "tickvals": [z_na, *z_tickvals],
            "ticktext": ["not comparable", *z_ticktext],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
    }
    return {
        "traces": traces,
        "layout": common_layout(
            "Proxy energy context: damage signature vs energy mismatch",
            scene,
        ),
        "note": (
            "Each marker is one ranked irradiation-to-proxy comparison. Low x "
            "and low y means the proxy is close in both damage signature and "
            "terminal energy; high x is a damage-signature mismatch and high y "
            "is an energy mismatch. z is the active-volume energy-density "
            "ratio on a log10 display; irradiation is ion-track localized "
            "while SC/avalanche are bulk approximations, so extreme z is a "
            "localization mismatch to review manually. "
            f"{n_not_comparable:,} rows lack a positive energy-density ratio "
            "and sit on the not-comparable floor plane."
        ),
    }


def plotly_script_tag() -> str:
    if PLOTLY_ASSET.exists():
        runtime = PLOTLY_ASSET.read_text()
        return f"<script>{runtime}</script>"
    return (
        f'<script src="{PLOTLY_CDN}"></script>'
        "<!-- Network access is required because the local Plotly asset "
        "was not found when this file was generated. -->"
    )


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>APS interactive damage-signature and energy viewer</title>
<style>
:root {
  color-scheme: light;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
    "Segoe UI", sans-serif;
  color: #17202a;
  background: #f6f8fa;
}
* { box-sizing: border-box; }
body { margin: 0; min-height: 100vh; }
header {
  padding: 18px 24px 12px;
  background: #ffffff;
  border-bottom: 1px solid #d0d7de;
}
h1 { margin: 0 0 6px; font-size: 22px; }
header p { margin: 0; color: #57606a; font-size: 14px; }
.controls {
  display: flex;
  gap: 8px;
  padding: 12px 24px 0;
  background: #f6f8fa;
}
.tab {
  border: 1px solid #afb8c1;
  border-bottom: 0;
  border-radius: 8px 8px 0 0;
  padding: 9px 14px;
  background: #eaeef2;
  color: #24292f;
  cursor: pointer;
  font-weight: 600;
}
.tab.active {
  background: #ffffff;
  color: #0969da;
  border-color: #8c959f;
}
.panel {
  margin: 0 16px 16px;
  background: #ffffff;
  border: 1px solid #d0d7de;
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(31,35,40,0.08);
  overflow: hidden;
}
.plot {
  width: 100%;
  height: min(78vh, 900px);
  min-height: 620px;
}
.note {
  padding: 10px 16px;
  border-top: 1px solid #d8dee4;
  background: #f6f8fa;
  color: #57606a;
  font-size: 13px;
  line-height: 1.45;
}
.help {
  padding: 0 24px 12px;
  color: #57606a;
  font-size: 13px;
}
.error {
  margin: 24px;
  padding: 16px;
  border: 1px solid #cf222e;
  background: #ffebe9;
  color: #82071e;
  border-radius: 6px;
}
[hidden] { display: none !important; }
@media (max-width: 800px) {
  .plot { min-height: 520px; height: 72vh; }
  .controls { padding-left: 12px; }
  .panel { margin: 0 6px 6px; }
}
</style>
__PLOTLY_SCRIPT__
</head>
<body>
<header>
  <h1>APS interactive damage-signature and energy viewer</h1>
  <p>Four views of the same stress data: independent source records, ranked
  pairwise deltas, and two energy-context scenes (depletion susceptibility and
  proxy energy mismatch).</p>
</header>
<div class="controls" role="tablist" aria-label="3D plot views">
  <button id="source-tab" class="tab active" role="tab" aria-selected="true"
    data-view="source">Individual source records</button>
  <button id="delta-tab" class="tab" role="tab" aria-selected="false"
    data-view="delta">Delta comparisons</button>
  <button id="energy-tab" class="tab" role="tab" aria-selected="false"
    data-view="energy">Energy context</button>
  <button id="energyDelta-tab" class="tab" role="tab" aria-selected="false"
    data-view="energyDelta">Proxy energy context</button>
</div>
<div class="help">
  Drag to rotate, use the wheel or pinch to zoom, hover for record metadata and
  the energy chain, and click a legend item to hide or isolate a cohort. The
  camera icon exports the current view; the home icon resets the camera.
</div>
<main class="panel">
  <div id="source-plot" class="plot" role="tabpanel"></div>
  <div id="delta-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energy-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energyDelta-plot" class="plot" role="tabpanel" hidden></div>
  <div id="plot-note" class="note"></div>
</main>
<script id="source-payload" type="application/json">__SOURCE_PAYLOAD__</script>
<script id="delta-payload" type="application/json">__DELTA_PAYLOAD__</script>
<script id="energy-payload" type="application/json">__ENERGY_PAYLOAD__</script>
<script id="energy-delta-payload" type="application/json">__ENERGY_DELTA_PAYLOAD__</script>
<script>
(function () {
  if (!window.Plotly) {
    document.querySelector("main").innerHTML =
      '<div class="error"><b>Interactive runtime failed to load.</b> ' +
      "Regenerate this page with the local Plotly asset available.</div>";
    return;
  }

  const VIEWS = ["source", "delta", "energy", "energyDelta"];
  const payloads = {
    source: JSON.parse(document.getElementById("source-payload").textContent),
    delta: JSON.parse(document.getElementById("delta-payload").textContent),
    energy: JSON.parse(document.getElementById("energy-payload").textContent),
    energyDelta: JSON.parse(
      document.getElementById("energy-delta-payload").textContent
    )
  };
  const rendered = {
    source: false, delta: false, energy: false, energyDelta: false
  };
  const config = {
    responsive: true,
    scrollZoom: true,
    displaylogo: false,
    toImageButtonOptions: {
      format: "png",
      filename: "aps_damage_signature_3d",
      width: 1800,
      height: 1200,
      scale: 1
    }
  };

  function render(view) {
    const node = document.getElementById(view + "-plot");
    if (rendered[view]) {
      Plotly.Plots.resize(node);
      return;
    }
    const payload = payloads[view];
    if (!payload.traces || payload.traces.length === 0) {
      node.innerHTML =
        '<div class="error" style="border-color:#9a6700;background:#fff8c5;' +
        'color:#7a5c00">No comparable rows for this view yet.</div>';
    } else {
      Plotly.newPlot(node, payload.traces, payload.layout, config);
    }
    rendered[view] = true;
  }

  function viewFromHash() {
    const key = (window.location.hash || "").replace("#", "");
    return VIEWS.indexOf(key) >= 0 ? key : "source";
  }

  function show(view) {
    document.querySelectorAll(".tab").forEach(function (button) {
      const active = button.dataset.view === view;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    VIEWS.forEach(function (name) {
      document.getElementById(name + "-plot").hidden = name !== view;
    });
    document.getElementById("plot-note").textContent = payloads[view].note;
    render(view);
  }

  document.querySelectorAll(".tab").forEach(function (button) {
    button.addEventListener("click", function () {
      const view = button.dataset.view;
      if (window.location.hash !== "#" + view) {
        window.location.hash = view;
      } else {
        show(view);
      }
    });
  });

  window.addEventListener("hashchange", function () {
    show(viewFromHash());
  });

  show(viewFromHash());
})();
</script>
</body>
</html>
"""


def main() -> None:
    missing = [path for path in (SOURCE_CSV, DELTA_CSV) if not path.exists()]
    if missing:
        names = ", ".join(str(path) for path in missing)
        raise SystemExit(f"Missing prerequisite damage signature CSV files: {names}")

    source_records = pd.read_csv(SOURCE_CSV)
    delta_comparisons = pd.read_csv(DELTA_CSV)
    source_payload = source_plot_payload(source_records)
    delta_payload = delta_plot_payload(delta_comparisons)
    energy_payload = energy_context_plot_payload(source_records)
    energy_delta_payload = energy_delta_plot_payload(delta_comparisons)

    html = (
        HTML_TEMPLATE.replace("__PLOTLY_SCRIPT__", plotly_script_tag())
        .replace("__SOURCE_PAYLOAD__", json_for_html(source_payload))
        .replace("__DELTA_PAYLOAD__", json_for_html(delta_payload))
        .replace("__ENERGY_PAYLOAD__", json_for_html(energy_payload))
        .replace("__ENERGY_DELTA_PAYLOAD__", json_for_html(energy_delta_payload))
    )
    OUTPUT_HTML.write_text(html)
    size_mb = OUTPUT_HTML.stat().st_size / (1024 * 1024)

    def count_points(payload: dict[str, Any]) -> int:
        return sum(
            len(trace.get("x", []))
            for trace in payload["traces"]
            if trace.get("type") == "scatter3d"
        )

    print(f"Wrote {OUTPUT_HTML} ({size_mb:.2f} MiB)")
    print(
        "Views: "
        f"{count_points(source_payload):,} source; "
        f"{count_points(delta_payload):,} delta; "
        f"{count_points(energy_payload):,} energy-context; "
        f"{count_points(energy_delta_payload):,} proxy-energy-context points"
    )


if __name__ == "__main__":
    main()
