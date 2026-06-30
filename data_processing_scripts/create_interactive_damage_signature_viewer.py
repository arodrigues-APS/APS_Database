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

try:
    from data_processing_scripts.depletion_threshold_model import (
        KOSIER_2026_SEB_CRITICAL_J_CM2,
        KOSIER_2026_SELC_CRITICAL_J_CM2,
    )
except ImportError:  # Allows running from inside data_processing_scripts/.
    from depletion_threshold_model import (
        KOSIER_2026_SEB_CRITICAL_J_CM2,
        KOSIER_2026_SELC_CRITICAL_J_CM2,
    )


OUT_DIR = Path("out/avalanche_irrad_pilot")
SOURCE_CSV = OUT_DIR / "damage_signature_sources_3d.csv"
DELTA_CSV = OUT_DIR / "damage_signature_delta_3d.csv"
# Optional: written by export_proxy_candidate_energy_v2_csv.py.  When absent the
# v2 tab renders its empty-state note instead of failing the whole build.
V2_CSV = OUT_DIR / "proxy_candidate_energy_v2.csv"
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


def display_area(value: Any) -> str:
    """Format an active-area estimate in cm2."""
    if value is None or pd.isna(value):
        return "not recorded"
    return f"{float(value):.3g} cm2"


def display_comparison_ratio(value: Any) -> str:
    """Format a ratio against a Kosier threshold energy."""
    if value is None or pd.isna(value):
        return "not comparable"
    return f"{float(value):.3g}x"


def numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return a numeric column or an all-NaN series for older CSV exports."""
    if column in frame:
        return numeric(frame[column])
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def positive_ratio(numerator: Any, denominator: Any) -> float | None:
    numerator = finite_number(numerator)
    denominator = finite_number(denominator)
    if numerator is None or denominator is None or denominator <= 0.0:
        return None
    return numerator / denominator


def finite_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def comparability_label(ratio: Any) -> str:
    """Classify whether two positive energies are within one decade."""
    ratio = finite_number(ratio)
    if ratio is None:
        return "not comparable"
    if ratio < 0.1:
        return "far below"
    if ratio > 10.0:
        return "far above"
    return "same order of magnitude"


def recorded_sum(series: pd.Series) -> float:
    values = numeric(series).dropna()
    if values.empty:
        return 0.0
    return float(values.sum())


def positive_mean(series: pd.Series) -> float:
    """Mean over strictly positive values; 0.0 when none are present."""
    values = numeric(series)
    positive = values[values.gt(0.0)]
    if positive.empty:
        return 0.0
    return float(positive.mean())


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


def common_cartesian_layout(title: str) -> dict[str, Any]:
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
        "margin": {"l": 72, "r": 24, "t": 88, "b": 96},
        "hoverlabel": {
            "bgcolor": "#ffffff",
            "font": {"color": "#17202a", "size": 12},
            "bordercolor": "#8c959f",
        },
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

    planes: list[dict[str, Any]] = [
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

    # Split markers per (device, source) so the global device filter can
    # isolate a single device; legend proxies keep one stable entry per source.
    device_label = data["device_label"].fillna("unlabeled device")
    device_order = [str(d) for d in device_label.value_counts().index]

    legend_proxies: list[dict[str, Any]] = []
    for source in ("irradiation", "avalanche", "sc"):
        group = data[data["source"].eq(source)]
        if group.empty:
            continue
        style = SOURCE_STYLES[source]
        legend_proxies.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{style['name']} (n={len(group):,})",
                "x": [None],
                "y": [None],
                "z": [None],
                "legendgroup": source,
                "hoverinfo": "skip",
                "showlegend": True,
                "visible": True,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": style["size"],
                    "opacity": style["opacity"],
                },
            }
        )

    data_traces: list[dict[str, Any]] = []
    trace_device: list[str] = []
    for dev_name in device_order:
        dev_mask = device_label.eq(dev_name)
        for source in ("irradiation", "avalanche", "sc"):
            group = data[dev_mask & data["source"].eq(source)]
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
            data_traces.append(
                {
                    "type": "scatter3d",
                    "mode": "markers",
                    "name": style["name"],
                    "legendgroup": source,
                    "showlegend": False,
                    "visible": True,
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
            trace_device.append(dev_name)

    traces = [*planes, *legend_proxies, *data_traces]
    n_fixed = len(planes) + len(legend_proxies)

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
    base_title = "Individual irradiation, short-circuit, and avalanche records"
    titles = {
        dev: f"{base_title}<br>{dev} (n={int(device_label.eq(dev).sum()):,})"
        for dev in device_order
    }
    return {
        "traces": traces,
        "layout": common_layout(base_title, scene),
        "note": (
            f"Each marker is one independent stress record: "
            f"{len(data[data['source'].eq('irradiation')]):,} irradiation, "
            f"{len(data[data['source'].eq('sc')]):,} SC, and "
            f"{len(data[data['source'].eq('avalanche')]):,} avalanche. "
            "SC and avalanche gate current was not recorded, so those points "
            "use the labelled not-recorded plane. Avalanche normalized-Vds "
            "values are shown as stored but have a known scaling artifact. Use "
            "the device filter at the top of the page to isolate one device."
        ),
        "filter": {
            "devices": device_order,
            "traceDevices": [None] * n_fixed + trace_device,
            "titleAll": base_title,
            "titles": titles,
            "allShowsOnly": None,
        },
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

    # gate_delta is dropped as a plotted axis: it is NULL for every proxy
    # candidate (avalanche and SC never record a gate waveform), so it added a
    # dead dimension where every point sat on the gate=0 face.  The two axes
    # that actually carry signal are collapse_delta (always present) and
    # normalized_vds_delta (present for SC; NULL by design for avalanche).
    collapse_upper = max(1.0, float(data["collapse_delta"].max()) * 1.05)
    normalized_values = data["normalized_vds_delta"].dropna()
    normalized_upper = (
        max(0.5, float(normalized_values.max()) * 1.08)
        if not normalized_values.empty
        else 1.0
    )
    normalized_na = -0.08 * normalized_upper

    data["plot_normalized_vds"] = data["normalized_vds_delta"].fillna(
        normalized_na
    )

    traces: list[dict[str, Any]] = []

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

    # Split markers per (target device, candidate source); the global device
    # filter keys on the irradiation (target) device. Legend proxies keep one
    # stable entry per candidate source.
    target_device = data["target_device_label"].fillna("unlabeled device")
    device_order = [str(d) for d in target_device.value_counts().index]

    for source in ("avalanche", "sc"):
        group = data[data["candidate_source"].eq(source)]
        if group.empty:
            continue
        style = DELTA_STYLES[source]
        traces.append(
            {
                "type": "scatter",
                "mode": "markers",
                "name": f"{style['name']} (n={len(group):,})",
                "x": [None],
                "y": [None],
                "legendgroup": source,
                "hoverinfo": "skip",
                "showlegend": True,
                "visible": True,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": max(5, style["size"]),
                    "opacity": style["opacity"],
                },
            }
        )

    n_fixed = len(traces)
    trace_device: list[str] = []
    for dev_name in device_order:
        dev_mask = target_device.eq(dev_name)
        for source in ("avalanche", "sc"):
            group = data[dev_mask & data["candidate_source"].eq(source)]
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
                    "type": "scatter",
                    "mode": "markers",
                    "name": style["name"],
                    "legendgroup": source,
                    "showlegend": False,
                    "visible": True,
                    "x": group["collapse_delta"].astype(float).tolist(),
                    "y": group["plot_normalized_vds"].astype(float).tolist(),
                    "customdata": customdata,
                    "hovertemplate": hover_template,
                    "marker": {
                        "color": style["color"],
                        "symbol": style["symbol"],
                        "size": max(5, style["size"]),
                        "opacity": style["opacity"],
                        "line": {"color": style["color"], "width": 0},
                    },
                }
            )
            trace_device.append(dev_name)

    normalized_ticks = np.linspace(0.0, normalized_upper, 6)
    floor_half = 0.45 * abs(normalized_na)
    layout = common_cartesian_layout(
        "Ranked irradiation-to-proxy comparisons in delta space"
    )
    layout.update(
        {
            "legend": {
                "x": 0.99,
                "y": 0.99,
                "xanchor": "right",
                "bgcolor": "rgba(255,255,255,0.82)",
                "bordercolor": "#d0d7de",
                "borderwidth": 1,
                "itemsizing": "constant",
            },
            "shapes": [
                # Labelled not-recorded band for comparisons without a
                # normalized-Vds delta (every avalanche pair sits here).
                {
                    "type": "rect",
                    "layer": "below",
                    "xref": "x",
                    "yref": "y",
                    "x0": 0.0,
                    "x1": collapse_upper,
                    "y0": normalized_na - floor_half,
                    "y1": normalized_na + floor_half,
                    "fillcolor": "#777777",
                    "opacity": 0.06,
                    "line": {"width": 0},
                }
            ],
            "xaxis": {
                "title": {"text": "collapse_delta |candidate - irradiation|"},
                "range": [0.0, collapse_upper],
                "gridcolor": "#d8dee4",
                "zerolinecolor": "#8c959f",
            },
            "yaxis": {
                "title": {
                    "text": "normalized_vds_delta |candidate - irradiation|"
                },
                "range": [normalized_na - floor_half, normalized_upper],
                "tickvals": [normalized_na, *normalized_ticks.tolist()],
                "ticktext": [
                    "not recorded",
                    *(display_value(value, digits=2) for value in normalized_ticks),
                ],
                "gridcolor": "#d8dee4",
                "zerolinecolor": "#8c959f",
            },
        }
    )
    base_title = "Ranked irradiation-to-proxy comparisons in delta space"
    titles = {
        dev: f"{base_title}<br>target {dev} "
        f"(n={int(target_device.eq(dev).sum()):,} pairs)"
        for dev in device_order
    }
    return {
        "traces": traces,
        "layout": layout,
        "note": (
            f"Each marker is one ranked irradiation-to-proxy comparison: "
            f"{len(data[data['candidate_source'].eq('avalanche')]):,} "
            "irradiation-to-avalanche and "
            f"{len(data[data['candidate_source'].eq('sc')]):,} "
            "irradiation-to-SC, plotted in the two delta axes that carry "
            "signal. gate_delta is omitted because no proxy candidate records "
            "a gate waveform, so it is NULL for every comparison. Avalanche "
            "comparisons are collapse-only (normalized-Vds delta excluded by "
            "design) and sit on the labelled not-recorded band; SC comparisons "
            "are collapse + normalized-Vds. Read the per-point evidence class "
            "and blockers before comparing ranks across the two cohorts. Use "
            "the device filter at the top of the page to isolate pairs by "
            "irradiation (target) device."
        ),
        "filter": {
            "devices": device_order,
            "traceDevices": [None] * n_fixed + trace_device,
            "titleAll": base_title,
            "titles": titles,
            "allShowsOnly": None,
        },
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

    # Two reference planes, always visible regardless of the device filter.
    planes: list[dict[str, Any]] = [
        # Terminal-energy not-recorded floor (display only, never zero).
        mesh_plane(
            x=[0.0, x_upper, x_upper, 0.0],
            y=[0.0, 0.0, y_upper, y_upper],
            z=[z_na, z_na, z_na, z_na],
        ),
        # SEB threshold plane at ratio = 1.0. The y-axis is stored energy / SEB
        # critical energy, so ratio 1.0 IS each device's own threshold; this one
        # plane stays correct under any device filter.
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

    # Per-device absolute SEB threshold energy (J) = 207 uJ/cm2 * active area.
    # The areal threshold is a fixed Kosier constant, so the only per-device
    # quantity worth surfacing is the active-area-scaled absolute energy, shown
    # in the title when a single device is selected.
    active_area = derived_active_area_cm2(data)
    device_label = (
        data["device_label"].fillna("unlabeled device")
        if "device_label" in data
        else pd.Series("all devices", index=data.index)
    )
    device_order = list(device_label.value_counts().index)

    def seb_threshold_energy_j(idx: pd.Index) -> float | None:
        area = active_area.reindex(idx).dropna()
        if area.empty:
            return None
        return KOSIER_2026_SEB_CRITICAL_J_CM2 * float(area.median())

    base_title = "Irradiation depletion susceptibility vs terminal energy"
    all_title = (
        f"{base_title}<br>All devices (n={len(data):,}) \u2014 red plane = "
        "each device's own SEB threshold (ratio 1.0)"
    )

    def device_title(name: str, idx: pd.Index) -> str:
        energy = seb_threshold_energy_j(idx)
        if energy is None:
            tail = "SEB threshold area not modeled"
        else:
            tail = (
                f"SEB threshold \u2248 {display_joules(energy)} "
                "(207\u00b5J/cm\u00b2 \u00d7 active area)"
            )
        return f"{base_title}<br>{name} (n={len(idx):,}) \u2014 {tail}"

    # Legend proxies: one always-visible entry per event type so the per-device
    # data traces can stay off the legend (no duplicate rows when filtering).
    event_types = [et for et in sorted(data["event_type"].dropna().unique())]
    legend_proxies: list[dict[str, Any]] = []
    for event_type in event_types:
        total = int(data["event_type"].eq(event_type).sum())
        color = EVENT_TYPE_COLORS.get(str(event_type), EVENT_TYPE_FALLBACK)
        legend_proxies.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{event_type} (n={total:,})",
                "x": [None],
                "y": [None],
                "z": [None],
                "legendgroup": str(event_type),
                "hoverinfo": "skip",
                "showlegend": True,
                "visible": True,
                "marker": {"color": color, "size": 8, "opacity": 0.85},
            }
        )

    # One data trace per (device, event type) so the dropdown can isolate a
    # single device by toggling trace visibility.
    data_traces: list[dict[str, Any]] = []
    trace_device: list[str] = []
    for dev_name in device_order:
        dev_rows = data[device_label.eq(dev_name)]
        for event_type, group in dev_rows.groupby("event_type", sort=True):
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
            data_traces.append(
                {
                    "type": "scatter3d",
                    "mode": "markers",
                    "name": f"{event_type}",
                    "legendgroup": str(event_type),
                    "showlegend": False,
                    "visible": True,
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
            trace_device.append(str(dev_name))

    traces = [*planes, *legend_proxies, *data_traces]
    n_fixed = len(planes) + len(legend_proxies)

    z_tickvals, z_ticktext = ([], [])
    if not positive.empty:
        z_tickvals, z_ticktext = log_decade_ticks(t_min, t_max)
    scene = {
        "dragmode": "orbit",
        "aspectmode": "manual",
        "aspectratio": {"x": 1.25, "y": 1.0, "z": 1.0},
        # Home angle captured from the rendered viewer: rotates the blocking-bias
        # (x) axis to vertical so the SEB-ratio climb reads top-to-bottom.
        "camera": {
            "up": {"x": 0.3716, "y": 0.8955, "z": 0.2450},
            "center": {"x": 0.2046, "y": -0.3792, "z": 0.1495},
            "eye": {"x": -1.8604, "y": 0.8397, "z": -1.1735},
            "projection": {"type": "perspective"},
        },
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
    layout = common_layout(base_title, scene)
    layout["title"]["text"] = all_title
    titles = {
        str(dev): device_title(str(dev), data.index[device_label.eq(dev)])
        for dev in device_order
    }
    return {
        "traces": traces,
        "layout": layout,
        "note": (
            "Each marker is one irradiation record with a modeled depletion "
            "ratio. x is normalized blocking bias, y is stored depletion "
            "energy over the Kosier SEB threshold (the red plane marks ratio "
            "1.0), and z is terminal electrical energy on a log display. "
            "Marker size grows with Vds collapse fraction. Use the device "
            "filter at the top of the page to isolate one device; because y is "
            "already normalized to each device's SEB critical energy, the red "
            "plane stays the correct threshold for every device, and the "
            "selected device's absolute SEB threshold energy (207 uJ/cm2 times "
            "its active area) is shown in the title. Terminal energy is modeled "
            "as not recorded on the floor plane, never zero. The depletion "
            "model is estimated from rated voltage and active SiC thickness."
        ),
        "filter": {
            "devices": [str(dev) for dev in device_order],
            "traceDevices": [None] * n_fixed + trace_device,
            "titleAll": all_title,
            "titles": titles,
            "allShowsOnly": None,
        },
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

    # Split markers per (target device, candidate source); the global device
    # filter keys on the irradiation (target) device.
    target_device = data["target_device_label"].fillna("unlabeled device")
    device_order = [str(d) for d in target_device.value_counts().index]

    for source in ("avalanche", "sc"):
        group = data[data["candidate_source"].eq(source)]
        if group.empty:
            continue
        style = DELTA_STYLES[source]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{style['name']} (n={len(group):,})",
                "x": [None],
                "y": [None],
                "z": [None],
                "legendgroup": source,
                "hoverinfo": "skip",
                "showlegend": True,
                "visible": True,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": style["size"],
                    "opacity": style["opacity"],
                },
            }
        )

    n_fixed = len(traces)
    trace_device: list[str] = []
    for dev_name in device_order:
        dev_mask = target_device.eq(dev_name)
        for source in ("avalanche", "sc"):
            group = data[dev_mask & data["candidate_source"].eq(source)]
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
                    "name": style["name"],
                    "legendgroup": source,
                    "showlegend": False,
                    "visible": True,
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
            trace_device.append(dev_name)

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
    base_title = "Proxy energy context: damage signature vs energy mismatch"
    titles = {
        dev: f"{base_title}<br>target {dev} "
        f"(n={int(target_device.eq(dev).sum()):,} pairs)"
        for dev in device_order
    }
    return {
        "traces": traces,
        "layout": common_layout(base_title, scene),
        "note": (
            "Each marker is one ranked irradiation-to-proxy comparison. Low x "
            "and low y means the proxy is close in both damage signature and "
            "terminal energy; high x is a damage-signature mismatch and high y "
            "is an energy mismatch. z is the active-volume energy-density "
            "ratio on a log10 display; irradiation is ion-track localized "
            "while SC/avalanche are bulk approximations, so extreme z is a "
            "localization mismatch to review manually. "
            f"{n_not_comparable:,} rows lack a positive energy-density ratio "
            "and sit on the not-comparable floor plane. Use the device filter "
            "at the top of the page to isolate pairs by irradiation (target) "
            "device."
        ),
        "filter": {
            "devices": device_order,
            "traceDevices": [None] * n_fixed + trace_device,
            "titleAll": base_title,
            "titles": titles,
            "allShowsOnly": None,
        },
    }


def derived_active_area_cm2(records: pd.DataFrame) -> pd.Series:
    """Derive active area from exported active volume and active SiC thickness."""
    volume_cm3 = numeric_column(records, "energy_density_active_volume_cm3")
    thickness_cm = numeric_column(records, "se_depletion_active_thickness_um") * 1e-4
    valid = volume_cm3.gt(0.0) & thickness_cm.gt(0.0)
    return (volume_cm3 / thickness_cm).where(valid)


# Minimum irradiation records for a device to get its own per-event energy view.
MIN_DEVICE_RECORDS = 5


def irradiation_energy_summary(records: pd.DataFrame) -> dict[str, Any]:
    """Summarize irradiation energy reservoirs against Kosier thresholds.

    Deposited and terminal energies are absolute Joules. Kosier SEB/SELC
    thresholds are areal J/cm2, so this converts them to Joules using the
    active area derived from the exported active volume and active thickness.
    """
    if "source" not in records:
        data = records.iloc[0:0].copy()
    else:
        data = records[records["source"].eq("irradiation")].copy()

    active_area = derived_active_area_cm2(data)
    selc_areal = numeric_column(
        data,
        "se_depletion_critical_selc_j_cm2",
    ).fillna(KOSIER_2026_SELC_CRITICAL_J_CM2)
    seb_areal = numeric_column(
        data,
        "se_depletion_critical_seb_j_cm2",
    ).fillna(KOSIER_2026_SEB_CRITICAL_J_CM2)
    stored_areal = numeric_column(data, "se_depletion_stored_energy_j_cm2")

    computed = {
        "ionizing_deposited": numeric_column(data, "radiation_deposited_energy_j"),
        "total_deposited": numeric_column(data, "radiation_deposited_energy_total_j"),
        "terminal": numeric_column(data, "electrical_terminal_energy_j"),
        "stored_field": stored_areal * active_area,
        "kosier_selc_needed": selc_areal * active_area,
        "kosier_seb_needed": seb_areal * active_area,
    }
    metric_defs = [
        (
            "Ionizing deposited",
            "radiation_deposited_energy_j; electronic/ionizing channel",
            computed["ionizing_deposited"],
            "#377eb8",
        ),
        (
            "Total deposited",
            "radiation_deposited_energy_total_j; electronic plus nuclear channel",
            computed["total_deposited"],
            "#4c78a8",
        ),
        (
            "Terminal electrical",
            "electrical_terminal_energy_j; waveform-integrated Vds * Id",
            computed["terminal"],
            "#f58518",
        ),
        (
            "Modeled stored field",
            "se_depletion_stored_energy_j_cm2 * derived active area",
            computed["stored_field"],
            "#b279a2",
        ),
        (
            "Kosier SELC needed",
            "60 uJ/cm2 Kosier SELC threshold * derived active area",
            computed["kosier_selc_needed"],
            "#e45756",
        ),
        (
            "Kosier SEB needed",
            "207 uJ/cm2 Kosier SEB threshold * derived active area",
            computed["kosier_seb_needed"],
            "#54a24b",
        ),
    ]

    if "device_label" in data and data["device_label"].notna().any():
        device = data["device_label"].fillna("unlabeled device")
    elif "device_type" in data and data["device_type"].notna().any():
        device = data["device_type"].fillna("unlabeled device")
    else:
        device = pd.Series("all devices", index=data.index)

    devices = []
    for dev_name in device.value_counts().index:
        idx = data.index[device.eq(dev_name)]
        if len(idx) < MIN_DEVICE_RECORDS:
            continue
        selc_mean = positive_mean(computed["kosier_selc_needed"].reindex(idx))
        seb_mean = positive_mean(computed["kosier_seb_needed"].reindex(idx))
        metrics = []
        any_positive = False
        for label, basis, series, color in metric_defs:
            positive = numeric(series).reindex(idx)
            positive = positive[positive.gt(0.0)]
            count = int(positive.shape[0])
            if count:
                any_positive = True
            mean_j = float(positive.mean()) if count else 0.0
            median_j = float(positive.median()) if count else 0.0
            ratio_to_selc = positive_ratio(mean_j, selc_mean)
            ratio_to_seb = positive_ratio(mean_j, seb_mean)
            metrics.append(
                {
                    "label": label,
                    "basis": basis,
                    "mean_j": mean_j,
                    "median_j": median_j,
                    "recorded_count": count,
                    "ratio_to_selc": ratio_to_selc,
                    "ratio_to_seb": ratio_to_seb,
                    "selc_comparison": comparability_label(ratio_to_selc),
                    "seb_comparison": comparability_label(ratio_to_seb),
                    "color": color,
                }
            )
        if not any_positive:
            continue
        area_dev = active_area.reindex(idx)
        devices.append(
            {
                "name": str(dev_name),
                "n_records": int(len(idx)),
                "metrics": metrics,
                "n_active_area_records": int(area_dev.notna().sum()),
                "active_area_median_cm2": (
                    float(area_dev.dropna().median())
                    if area_dev.notna().any()
                    else None
                ),
            }
        )

    return {
        "devices": devices,
        "n_irradiation_records": int(len(data)),
        "n_active_area_records": int(active_area.notna().sum()),
        "active_area_median_cm2": (
            float(active_area.dropna().median()) if active_area.notna().any() else None
        ),
    }


def energy_balance_plot_payload(records: pd.DataFrame) -> dict[str, Any]:
    """Per-event mean/median energy reservoirs by device vs Kosier thresholds."""
    summary = irradiation_energy_summary(records)
    devices = summary["devices"]
    base_title = "Per-event energy by reservoir vs Kosier SELC/SEB energy"
    empty_note = (
        "No per-device irradiation energy is available. This view needs "
        f"irradiation rows with a device label, at least {MIN_DEVICE_RECORDS} "
        "records per device, and deposited, terminal, or active-area energy."
    )
    if not devices:
        return {
            "traces": [],
            "layout": common_cartesian_layout(base_title),
            "note": empty_note,
        }

    def device_title(dev: dict[str, Any]) -> str:
        return f"{base_title}<br>{dev['name']} (n={dev['n_records']:,} records)"

    mean_color = "#377eb8"
    median_color = "#f58518"
    mean_hover = (
        "<b>%{x}</b><br>"
        "Mean per event: %{customdata[0]}<br>"
        "Median per event: %{customdata[1]}<br>"
        "Events with value: %{customdata[2]}<br>"
        "Basis: %{customdata[3]}<br>"
        "<br>Mean vs Kosier SELC needed: %{customdata[4]} (%{customdata[5]})<br>"
        "Mean vs Kosier SEB needed: %{customdata[6]} (%{customdata[7]})"
        "<extra>Mean</extra>"
    )
    median_hover = (
        "<b>%{x}</b><br>"
        "Median per event: %{customdata[1]}<br>"
        "Mean per event: %{customdata[0]}<br>"
        "Events with value: %{customdata[2]}<br>"
        "Basis: %{customdata[3]}"
        "<extra>Median</extra>"
    )

    traces = []
    trace_device: list[str] = []
    for dev in devices:
        metrics = dev["metrics"]
        labels = [m["label"] for m in metrics]
        customdata = [
            [
                display_joules(m["mean_j"]),
                display_joules(m["median_j"]),
                display_value(m["recorded_count"]),
                m["basis"],
                display_comparison_ratio(m["ratio_to_selc"]),
                m["selc_comparison"],
                display_comparison_ratio(m["ratio_to_seb"]),
                m["seb_comparison"],
            ]
            for m in metrics
        ]
        traces.append(
            {
                "type": "bar",
                "name": "Mean",
                "legendgroup": "Mean",
                "x": labels,
                "y": [m["mean_j"] for m in metrics],
                "customdata": customdata,
                "hovertemplate": mean_hover,
                "visible": True,
                "marker": {
                    "color": mean_color,
                    "line": {"color": "#24292f", "width": 0.5},
                },
            }
        )
        traces.append(
            {
                "type": "bar",
                "name": "Median",
                "legendgroup": "Median",
                "x": labels,
                "y": [m["median_j"] for m in metrics],
                "customdata": customdata,
                "hovertemplate": median_hover,
                "visible": True,
                "marker": {
                    "color": median_color,
                    "line": {"color": "#24292f", "width": 0.5},
                },
            }
        )
        trace_device.extend([dev["name"], dev["name"]])

    layout = common_cartesian_layout(base_title)
    layout["title"]["text"] = device_title(devices[0])
    layout.update(
        {
            "barmode": "group",
            "bargap": 0.28,
            "showlegend": True,
            "legend": {
                "orientation": "h",
                "x": 0.5,
                "xanchor": "center",
                "y": 1.02,
                "yanchor": "bottom",
            },
            "xaxis": {
                "title": {"text": "Energy reservoir"},
                "tickangle": -18,
                "gridcolor": "#d8dee4",
            },
            "yaxis": {
                "title": {"text": "Per-event energy (J, log scale)"},
                "type": "log",
                "gridcolor": "#d8dee4",
                "zerolinecolor": "#8c959f",
            },
        }
    )

    default = devices[0]
    terminal = next(m for m in default["metrics"] if m["label"] == "Terminal electrical")
    deposited = next(m for m in default["metrics"] if m["label"] == "Ionizing deposited")
    note = (
        f"Bars are per-event mean and median energy for one device; use the "
        f"device filter at the top of the page to switch device. Because active "
        f"area is fixed within a device, the Kosier SELC/SEB needed bars are a "
        f"true per-device reference (critical areal energy times derived area), "
        f"not a record-count-inflated sum. For {default['name']} "
        f"(n={default['n_records']:,}), mean terminal electrical energy is "
        f"{display_comparison_ratio(terminal['ratio_to_selc'])} the SELC "
        f"threshold and {display_comparison_ratio(terminal['ratio_to_seb'])} the "
        f"SEB threshold; mean ionizing deposited energy is "
        f"{display_comparison_ratio(deposited['ratio_to_selc'])} SELC and "
        f"{display_comparison_ratio(deposited['ratio_to_seb'])} SEB. Treat this "
        f"as an order-of-magnitude check: ion-track deposited energy, terminal "
        f"electrical release, and stored depletion-field energy are different "
        f"reservoirs. With the filter on All devices this shows the most common "
        f"device; a device with no per-event energy shows an empty chart."
    )

    return {
        "traces": traces,
        "layout": layout,
        "note": note,
        "filter": {
            "devices": [d["name"] for d in devices],
            "traceDevices": trace_device,
            "titleAll": base_title,
            "titles": {d["name"]: device_title(d) for d in devices},
            "allShowsOnly": devices[0]["name"],
        },
    }


CRITICAL_OVERLAP_COLORS = {
    "strong_overlap": "#1a9850",
    "partial_overlap": "#a6d96a",
    "near_miss": "#fdae61",
    "far_miss": "#d73027",
    "missing_interval": "#999999",
}
V2_TARGET_BAND_COLOR = "#4575b4"
V2_NEEDED_BAND_COLUMNS = [
    "target_severity_low",
    "target_severity_high",
    "target_severity_point_ratio",
    "candidate_severity_low",
    "candidate_severity_high",
    "candidate_severity_point_ratio",
]


def _v2_clean(value: Any) -> str:
    """CSV/array cell -> display string ('' for NULL/NaN)."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value)


def _v2_target_label(rec: dict[str, Any]) -> str:
    key = _v2_clean(rec.get("target_stress_record_key"))
    event = _v2_clean(rec.get("target_event_type")) or "target"
    return f"{event} · {key[-18:]}" if key else event


def v2_interval_overlap_plot_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """Per-target severity-interval overlap for v2's rank-1 candidate.

    For each target, draw the target stored-field severity band and the rank-1
    candidate's terminal-critical severity band as horizontal intervals (error
    bars around the nominal point ratio) on a shared log severity-ratio axis,
    colored by the critical-severity overlap class.  Target and candidate stay
    semantically separate (separation invariant #1): this is a *screening
    descriptor*, never an equivalence claim, so every hover carries the v2
    status and blockers next to the numbers (Phase-5 acceptance).
    """
    base_title = "v2 severity-interval overlap (rank-1 candidate per target)"
    empty_note = (
        "No v2 rank-1 candidate rows with severity bands are available. Export "
        "stress_proxy_candidate_energy_v2 with "
        "export_proxy_candidate_energy_v2_csv.py after applying schema/028."
    )

    def empty() -> dict[str, Any]:
        return {
            "traces": [],
            "layout": common_cartesian_layout(base_title),
            "note": empty_note,
        }

    if rows is None or rows.empty:
        return empty()
    df = rows.copy()
    if "mechanistic_energy_candidate_rank" in df.columns:
        df = df[df["mechanistic_energy_candidate_rank"] == 1]
    if any(col not in df.columns for col in V2_NEEDED_BAND_COLUMNS):
        return empty()
    df = df.dropna(subset=V2_NEEDED_BAND_COLUMNS)
    if df.empty:
        return empty()

    has_device = "device_type" in df.columns
    devices = sorted(df["device_type"].dropna().unique()) if has_device else [None]

    target_hover = (
        "<b>Target %{y}</b><br>"
        "Stored-field severity ratio: %{x:.3g}<br>"
        "Band: [%{customdata[0]}, %{customdata[1]}]<br>"
        "Event %{customdata[2]} · regime %{customdata[3]}"
        "<extra>Target (stored depletion ratio)</extra>"
    )
    candidate_hover = (
        "<b>Candidate %{customdata[0]}</b><br>"
        "Terminal-critical severity ratio: %{x:.3g}<br>"
        "Band: [%{customdata[1]}, %{customdata[2]}]<br>"
        "Severity overlap: %{customdata[3]}<br>"
        "v1 rank %{customdata[4]} → v2 rank %{customdata[5]}<br>"
        "Status: %{customdata[6]}<br>"
        "Regime match: %{customdata[7]} · localization log10: %{customdata[8]}<br>"
        "Blockers: %{customdata[9]}<br>"
        "Notes: %{customdata[10]}"
        "<extra>Candidate (bulk terminal ratio)</extra>"
    )

    traces: list[dict[str, Any]] = []
    trace_device: list[Any] = []
    titles: dict[str, str] = {}
    for dev in devices:
        sub = df if dev is None else df[df["device_type"] == dev]
        recs = sub.to_dict("records")
        y_labels = [_v2_target_label(r) for r in recs]

        t_x = [float(r["target_severity_point_ratio"]) for r in recs]
        t_plus = [float(r["target_severity_high"]) - x for r, x in zip(recs, t_x)]
        t_minus = [x - float(r["target_severity_low"]) for r, x in zip(recs, t_x)]
        traces.append({
            "type": "scatter",
            "mode": "markers",
            "name": "Target severity band",
            "legendgroup": "target",
            "showlegend": dev == devices[0],
            "x": t_x,
            "y": y_labels,
            "error_x": {
                "type": "data", "symmetric": False,
                "array": t_plus, "arrayminus": t_minus,
                "color": V2_TARGET_BAND_COLOR, "thickness": 2, "width": 6,
            },
            "marker": {"color": V2_TARGET_BAND_COLOR, "symbol": "line-ns-open", "size": 9},
            "customdata": [[
                display_value(r["target_severity_low"]),
                display_value(r["target_severity_high"]),
                _v2_clean(r.get("target_event_type")),
                _v2_clean(r.get("target_mechanistic_regime")),
            ] for r in recs],
            "hovertemplate": target_hover,
            "visible": True,
        })

        c_x = [float(r["candidate_severity_point_ratio"]) for r in recs]
        c_plus = [float(r["candidate_severity_high"]) - x for r, x in zip(recs, c_x)]
        c_minus = [x - float(r["candidate_severity_low"]) for r, x in zip(recs, c_x)]
        marker_colors = [
            CRITICAL_OVERLAP_COLORS.get(
                _v2_clean(r.get("critical_severity_overlap_class")), "#999999")
            for r in recs
        ]
        traces.append({
            "type": "scatter",
            "mode": "markers",
            "name": "Candidate severity band",
            "legendgroup": "candidate",
            "showlegend": dev == devices[0],
            "x": c_x,
            "y": y_labels,
            "error_x": {
                "type": "data", "symmetric": False,
                "array": c_plus, "arrayminus": c_minus,
                "color": "#8c959f", "thickness": 2, "width": 6,
            },
            "marker": {"color": marker_colors, "symbol": "diamond", "size": 10,
                       "line": {"color": "#24292f", "width": 0.5}},
            "customdata": [[
                _v2_clean(r.get("candidate_source")),
                display_value(r["candidate_severity_low"]),
                display_value(r["candidate_severity_high"]),
                _v2_clean(r.get("critical_severity_overlap_class")),
                _v2_clean(r.get("candidate_rank_v1")),
                _v2_clean(r.get("mechanistic_energy_candidate_rank")),
                _v2_clean(r.get("mechanistic_energy_candidate_status")),
                _v2_clean(r.get("regime_match_class")),
                _v2_clean(r.get("localization_mismatch_log10")),
                _v2_clean(r.get("energy_v2_blockers")) or "(none)",
                _v2_clean(r.get("energy_v2_notes")) or "(none)",
            ] for r in recs],
            "hovertemplate": candidate_hover,
            "visible": True,
        })
        trace_device.extend([dev, dev])
        if dev is not None:
            titles[dev] = f"{base_title}<br>{dev}"

    layout = common_cartesian_layout(base_title)
    if devices and devices[0] is not None:
        layout["title"]["text"] = titles[devices[0]]
    layout.update({
        "showlegend": True,
        "legend": {"orientation": "h", "x": 0.5, "xanchor": "center",
                   "y": 1.04, "yanchor": "bottom"},
        "xaxis": {"title": {"text": "Severity ratio to threshold (log; 1.0 = threshold)"},
                  "type": "log", "gridcolor": "#d8dee4", "zerolinecolor": "#8c959f"},
        "yaxis": {"title": {"text": "Target"}, "automargin": True,
                  "gridcolor": "#eef1f4"},
        "shapes": [{"type": "line", "x0": 1.0, "x1": 1.0, "y0": 0, "y1": 1,
                    "yref": "paper", "line": {"color": "#8c959f", "dash": "dot", "width": 1}}],
    })

    note = (
        "Each row is one target's rank-1 v2 candidate. The blue band is the "
        "target stored-field severity interval (depletion ratio to its SEB/SELC "
        "threshold); the diamond band is the candidate's bulk terminal-critical "
        "ratio, colored by the critical-severity overlap class (green strong → "
        "red far-miss). These are different physical quantities on a shared "
        "screening axis — overlap is a retrieval hint, not an equivalence claim. "
        "Every candidate hover shows the v2 status and blockers next to the "
        "numbers. Use the device filter at the top to switch device."
    )
    return {
        "traces": traces,
        "layout": layout,
        "note": note,
        "filter": {
            "devices": [d for d in devices if d is not None],
            "traceDevices": trace_device,
            "titleAll": base_title,
            "titles": titles,
            "allShowsOnly": devices[0] if devices and devices[0] is not None else None,
        },
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
  flex-wrap: wrap;
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
.filterbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  padding: 0 24px 14px;
  background: #f6f8fa;
  color: #57606a;
  font-size: 13px;
}
.filterbar label { font-weight: 600; color: #24292f; }
.filterbar select {
  font: inherit;
  padding: 4px 8px;
  border: 1px solid #afb8c1;
  border-radius: 6px;
  background: #ffffff;
  color: #24292f;
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
  <p>Six views of the same stress data: independent source records, ranked
  pairwise deltas, two energy-context scenes, a per-device per-event energy
  comparison against Kosier SELC/SEB thresholds, and the v2 mechanistic
  severity-interval overlap for each target's rank-1 candidate.</p>
</header>
<div class="controls" role="tablist" aria-label="Plot views">
  <button id="source-tab" class="tab active" role="tab" aria-selected="true"
    data-view="source">Individual source records</button>
  <button id="delta-tab" class="tab" role="tab" aria-selected="false"
    data-view="delta">Delta comparisons</button>
  <button id="energy-tab" class="tab" role="tab" aria-selected="false"
    data-view="energy">Energy context</button>
  <button id="energyDelta-tab" class="tab" role="tab" aria-selected="false"
    data-view="energyDelta">Proxy energy context</button>
  <button id="energySums-tab" class="tab" role="tab" aria-selected="false"
    data-view="energySums">Energy by device</button>
  <button id="v2overlap-tab" class="tab" role="tab" aria-selected="false"
    data-view="v2overlap">v2 severity overlap</button>
</div>
<div class="help">
  For 3D scenes, drag to rotate and use the wheel or pinch to zoom. Hover for
  record metadata, energy-chain values, and Kosier comparison ratios. The camera
  icon exports the current view; the home icon resets the camera where available.
</div>
<div class="filterbar">
  <label for="device-filter">Device filter:</label>
  <select id="device-filter" aria-label="Filter all views by device">
    <option value="__all__">All devices</option>
  </select>
  <span>applies to every tab; the two pair tabs key on the irradiation (target)
  device.</span>
</div>
<main class="panel">
  <div id="source-plot" class="plot" role="tabpanel"></div>
  <div id="delta-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energy-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energyDelta-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energySums-plot" class="plot" role="tabpanel" hidden></div>
  <div id="v2overlap-plot" class="plot" role="tabpanel" hidden></div>
  <div id="plot-note" class="note"></div>
</main>
<script id="source-payload" type="application/json">__SOURCE_PAYLOAD__</script>
<script id="delta-payload" type="application/json">__DELTA_PAYLOAD__</script>
<script id="energy-payload" type="application/json">__ENERGY_PAYLOAD__</script>
<script id="energy-delta-payload" type="application/json">__ENERGY_DELTA_PAYLOAD__</script>
<script id="energy-sums-payload" type="application/json">__ENERGY_SUMS_PAYLOAD__</script>
<script id="v2overlap-payload" type="application/json">__V2_PAYLOAD__</script>
<script id="device-options" type="application/json">__DEVICE_OPTIONS__</script>
<script>
(function () {
  if (!window.Plotly) {
    document.querySelector("main").innerHTML =
      '<div class="error"><b>Interactive runtime failed to load.</b> ' +
      "Regenerate this page with the local Plotly asset available.</div>";
    return;
  }

  const VIEWS = ["source", "delta", "energy", "energyDelta", "energySums",
    "v2overlap"];
  const payloads = {
    source: JSON.parse(document.getElementById("source-payload").textContent),
    delta: JSON.parse(document.getElementById("delta-payload").textContent),
    energy: JSON.parse(document.getElementById("energy-payload").textContent),
    energyDelta: JSON.parse(
      document.getElementById("energy-delta-payload").textContent
    ),
    energySums: JSON.parse(
      document.getElementById("energy-sums-payload").textContent
    ),
    v2overlap: JSON.parse(
      document.getElementById("v2overlap-payload").textContent
    )
  };
  const rendered = {
    source: false, delta: false, energy: false, energyDelta: false,
    energySums: false, v2overlap: false
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

  const DEVICE_ALL = "__all__";
  let currentDevice = DEVICE_ALL;
  const deviceOptions = JSON.parse(
    document.getElementById("device-options").textContent
  );
  const deviceSelect = document.getElementById("device-filter");
  deviceOptions.forEach(function (name) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    deviceSelect.appendChild(opt);
  });

  function applyFilter(view) {
    const node = document.getElementById(view + "-plot");
    const payload = payloads[view];
    if (!payload || !payload.filter || !node || !node.data) {
      return;
    }
    const f = payload.filter;
    const td = f.traceDevices || [];
    if (td.length === 0) {
      return;
    }
    let eff = currentDevice;
    if (eff === DEVICE_ALL) {
      eff = f.allShowsOnly || null;
    }
    const visible = td.map(function (d) {
      return !eff || d === null || d === eff;
    });
    const indices = td.map(function (_unused, i) { return i; });
    Plotly.restyle(node, { visible: visible }, indices);
    let title = f.titleAll;
    if (eff && f.titles && f.titles[eff]) {
      title = f.titles[eff];
    }
    if (title) {
      Plotly.relayout(node, { "title.text": title });
    }
  }

  deviceSelect.addEventListener("change", function () {
    currentDevice = deviceSelect.value;
    VIEWS.forEach(function (name) {
      if (rendered[name]) {
        applyFilter(name);
      }
    });
  });

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
      applyFilter(view);
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
    # v2 export is optional: absent ⇒ the tab shows its empty-state note.
    v2_records = pd.read_csv(V2_CSV) if V2_CSV.exists() else pd.DataFrame()
    source_payload = source_plot_payload(source_records)
    delta_payload = delta_plot_payload(delta_comparisons)
    energy_payload = energy_context_plot_payload(source_records)
    energy_delta_payload = energy_delta_plot_payload(delta_comparisons)
    energy_sums_payload = energy_balance_plot_payload(source_records)
    v2_payload = v2_interval_overlap_plot_payload(v2_records)

    # Union of per-view device options, in tab order, deduped, for the one
    # global device filter that drives every tab.
    device_options: list[str] = []
    for payload in (
        source_payload,
        energy_payload,
        delta_payload,
        energy_delta_payload,
        energy_sums_payload,
        v2_payload,
    ):
        for dev in payload.get("filter", {}).get("devices", []):
            if dev not in device_options:
                device_options.append(dev)

    html = (
        HTML_TEMPLATE.replace("__PLOTLY_SCRIPT__", plotly_script_tag())
        .replace("__SOURCE_PAYLOAD__", json_for_html(source_payload))
        .replace("__DELTA_PAYLOAD__", json_for_html(delta_payload))
        .replace("__ENERGY_PAYLOAD__", json_for_html(energy_payload))
        .replace("__ENERGY_DELTA_PAYLOAD__", json_for_html(energy_delta_payload))
        .replace("__ENERGY_SUMS_PAYLOAD__", json_for_html(energy_sums_payload))
        .replace("__V2_PAYLOAD__", json_for_html(v2_payload))
        .replace("__DEVICE_OPTIONS__", json_for_html(device_options))
    )
    OUTPUT_HTML.write_text(html)
    size_mb = OUTPUT_HTML.stat().st_size / (1024 * 1024)

    def count_points(payload: dict[str, Any]) -> int:
        return sum(
            len(trace.get("x", []))
            for trace in payload["traces"]
            if trace.get("type") in {"scatter3d", "scatter", "bar"}
        )

    print(f"Wrote {OUTPUT_HTML} ({size_mb:.2f} MiB)")
    print(
        "Views: "
        f"{count_points(source_payload):,} source; "
        f"{count_points(delta_payload):,} delta; "
        f"{count_points(energy_payload):,} energy-context; "
        f"{count_points(energy_delta_payload):,} proxy-energy-context; "
        f"{count_points(energy_sums_payload):,} per-device energy bars; "
        f"{count_points(v2_payload):,} v2 severity-interval points"
    )


if __name__ == "__main__":
    main()
