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
    from data_processing_scripts.proxy_viz_palette import (
        CONCORDANCE_STYLE,
        DEEMPHASIS_GRAY,
        EVENT_TYPE_COLORS,
        EVENT_TYPE_FALLBACK,
        OVERLAP_COLORS as CRITICAL_OVERLAP_COLORS,
        SOURCE_STYLES,
        V3_COMPONENT_COLORS,
    )
except ImportError:  # Allows running from inside data_processing_scripts/.
    from proxy_viz_palette import (
        CONCORDANCE_STYLE,
        DEEMPHASIS_GRAY,
        EVENT_TYPE_COLORS,
        EVENT_TYPE_FALLBACK,
        OVERLAP_COLORS as CRITICAL_OVERLAP_COLORS,
        SOURCE_STYLES,
        V3_COMPONENT_COLORS,
    )

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
# Optional: written by export_proxy_method_concordance_csv.py (v1×v2 join).
CONCORDANCE_CSV = OUT_DIR / "proxy_method_concordance.csv"
# Optional: written by export_proxy_candidate_combined_v3_csv.py.
V3_CSV = OUT_DIR / "proxy_candidate_combined_v3.csv"
PLOTLY_ASSET = OUT_DIR / "plotly-2.35.2.min.js"
PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"
OUTPUT_HTML = OUT_DIR / "damage_signature_3d_interactive.html"



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


def dex_series(frame: pd.DataFrame, dex_column: str, nats_column: str) -> pd.Series:
    """Natural-log energy delta, converted to log10/dex units for display.

    Prefers a precomputed ``dex_column`` (schema/025's ``log_energy_delta_dex``)
    and falls back to converting the natural-log ``nats_column``
    (``log_energy_delta``) by dividing once by ``ln(10)``, so older CSVs
    exported before the dex column existed still render. This is the single
    conversion point: a value already in dex is never divided by ln(10) again.
    """
    converted = numeric_column(frame, nats_column) / math.log(10)
    if dex_column in frame:
        dex = numeric(frame[dex_column])
        return dex.where(dex.notna(), converted)
    return converted


def scaled_marker_size(series: pd.Series, minimum: float = 3.0,
                       maximum: float = 12.0) -> list[float]:
    """Map a non-negative magnitude to a marker-size range; missing -> minimum."""
    values = numeric(series).clip(lower=0.0)
    if values.notna().sum() == 0:
        return [minimum] * len(values)
    vmax = float(values.max()) or 1.0
    return (minimum + (maximum - minimum) * values.fillna(0.0) / vmax).tolist()



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


def cartesian_legend_row_layout(title: str) -> dict[str, Any]:
    """Cartesian layout for tabs that stack a two-line title (base + device)
    AND a horizontal legend row above the plot area.  The shared 88px top
    margin cannot hold both, so give them an explicit vertical order inside a
    taller margin: title first, legend below, plot area last."""
    layout = common_cartesian_layout(title)
    layout["margin"] = {**layout["margin"], "t": 230}
    layout["title"] = {**layout["title"], "y": 0.97, "yanchor": "top",
                       "pad": {"t": 0, "b": 12}}
    layout["showlegend"] = True
    layout["legend"] = {"orientation": "h", "x": 0.5, "xanchor": "center",
                        "y": 0.885, "yanchor": "top",
                        "font": {"size": 11},
                        "bgcolor": "rgba(255,255,255,0.86)",
                        "itemsizing": "constant"}
    return layout


def _empty_payload(title: str, note: str, *, disabled: bool = True) -> dict[str, Any]:
    payload = {
        "traces": [],
        "layout": common_cartesian_layout(title),
        "note": note,
    }
    if disabled:
        payload["disabledReason"] = note
    return payload


def _rank1(frame: pd.DataFrame, rank_column: str) -> pd.DataFrame:
    if frame is None or frame.empty or rank_column not in frame.columns:
        return pd.DataFrame()
    out = frame.copy()
    out[rank_column] = numeric(out[rank_column])
    return out[out[rank_column] == 1].copy()


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame is None or frame.empty or column not in frame.columns:
        return {}
    counts = frame[column].fillna("not recorded").astype(str).value_counts()
    return {str(k): int(v) for k, v in counts.items()}


def _truth_status_is_curated(series: pd.Series) -> pd.Series:
    values = series.fillna("no_curated_truth").astype(str)
    return values.ne("no_curated_truth") & values.ne("")


def _table_payload(
    title: str,
    columns: list[tuple[str, list[Any]]],
    note: str,
    *,
    height: int = 720,
    disabled_reason: str | None = None,
) -> dict[str, Any]:
    layout = common_cartesian_layout(title)
    layout.update({"height": height, "margin": {"l": 20, "r": 20, "t": 82, "b": 20}})
    payload = {
        "traces": [{
            "type": "table",
            "header": {
                "values": [label for label, _values in columns],
                "fill": {"color": "#eaeef2"},
                "align": "left",
                "font": {"color": "#24292f", "size": 12},
            },
            "cells": {
                "values": [values for _label, values in columns],
                "align": "left",
                "height": 24,
                "fill": {"color": "#ffffff"},
                "font": {"color": "#24292f", "size": 11},
            },
        }],
        "layout": layout,
        "note": note,
    }
    if disabled_reason:
        payload["disabledReason"] = disabled_reason
    return payload


def _format_count(value: Any) -> str:
    number = finite_number(value)
    if number is None:
        return "not exported"
    return f"{int(number):,}"


def _pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "not available"
    return f"{numerator / denominator:.1%}"


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
    data["log_energy_delta_dex"] = dex_series(
        data, "log_energy_delta_dex", "log_energy_delta"
    )

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
        "Claim status: %{customdata[33]}<br>"
        "Claim basis: %{customdata[34]}<br>"
        "Decision-safe rank: %{customdata[36]}<br>"
        "<br>Collapse delta: %{x:.5g}<br>"
        "Gate delta: %{customdata[12]}<br>"
        "Normalized-Vds delta: %{customdata[13]}<br>"
        "<br><b>energy context</b><br>"
        "Target SEB / SELC ratio: %{customdata[14]} / %{customdata[15]}<br>"
        "Target deposited (ionizing): %{customdata[16]}<br>"
        "Target terminal energy: %{customdata[17]} (%{customdata[18]})<br>"
        "Proxy terminal energy: %{customdata[19]} (%{customdata[20]})<br>"
        "Energy comparability: %{customdata[39]} / %{customdata[40]}<br>"
        "Proxy terminal-density / irradiation deposited-density: %{customdata[21]}<br>"
        "Terminal-energy mismatch (dex): %{customdata[22]}<br>"
        "Signature-axis distance: %{customdata[23]}<br>"
        "Waveform-only distance: %{customdata[24]}<br>"
        "Legacy damage-signature distance: %{customdata[25]}<br>"
        "<br><b>evidence coverage</b><br>"
        "Evidence class: %{customdata[28]}<br>"
        "Signature quality: %{customdata[35]}<br>"
        "Available axes: %{customdata[29]}<br>"
        "Missing axes: %{customdata[30]}<br>"
        "Coverage score: %{customdata[31]}<br>"
        "Coverage-adjusted distance (diag.): %{customdata[32]}<br>"
        "Mechanism match: %{customdata[26]}<br>"
        "Candidate blockers: %{customdata[27]}<br>"
        "Claim blockers: %{customdata[37]}<br>"
        "Claim summary: %{customdata[38]}"
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
                    cell(row, "log_energy_delta_dex", display_ratio),
                    cell(row, "signature_axis_distance", display_ratio)
                    or cell(row, "damage_signature_distance", display_ratio),
                    cell(row, "waveform_only_distance", display_ratio),
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
                    cell(row, "proxy_claim_status"),
                    cell(row, "proxy_claim_basis"),
                    cell(row, "signature_claim_quality"),
                    cell(row, "decision_safe_rank"),
                    cell(row, "proxy_claim_blockers"),
                    cell(row, "proxy_claim_summary"),
                    cell(row, "target_energy_comparability_class"),
                    cell(row, "candidate_energy_comparability_class"),
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

    x = damage-signature distance, y = terminal-energy mismatch (dex/log10),
    z = log10 of the electrical-proxy terminal-energy density divided by the
    irradiation deposited-energy density. Rows whose energy-density ratio is
    missing or non-positive are placed on an explicit not-comparable plane
    instead of being imputed as zero.
    """
    data = comparisons.copy()
    for column in ("damage_signature_distance", "log_energy_delta", "energy_density_ratio"):
        data[column] = numeric(data[column]) if column in data else np.nan
    data = data[
        data["damage_signature_distance"].notna()
        & data["log_energy_delta"].notna()
    ].copy()
    data["log_energy_delta_dex"] = dex_series(
        data, "log_energy_delta_dex", "log_energy_delta"
    )

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
    y_values = data["log_energy_delta_dex"]
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
        "Terminal-energy mismatch (dex): %{y:.4g}<br>"
        "Proxy terminal-density / irradiation deposited-density: %{customdata[15]}<br>"
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
                    "y": group["log_energy_delta_dex"].astype(float).tolist(),
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
            "title": {
                "text": "Terminal-energy mismatch (dex)<br>"
                "|log10(proxy terminal / target terminal energy)|"
            },
            "range": [y_lower, y_upper],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "zaxis": {
            "title": {
                "text": "Proxy terminal-density / irradiation deposited-density<br>"
                "(log10 display)"
            },
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
            "is a terminal-energy mismatch. z compares electrical-proxy "
            "terminal-energy density against irradiation deposited-energy "
            "density on a log10 display; irradiation is ion-track localized "
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

    layout = cartesian_legend_row_layout(base_title)
    layout["title"]["text"] = device_title(devices[0])
    layout.update(
        {
            "barmode": "group",
            "bargap": 0.28,
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



V2_TARGET_BAND_COLUMNS = [
    "target_severity_low",
    "target_severity_high",
    "target_severity_point_ratio",
]
V2_TARGET_BAND_COLOR = "#2f6f9f"


def _v2_clean(value: Any) -> str:
    """CSV/array cell -> display string ('' for NULL/NaN)."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value)


def _v2_key_tail(key: str, max_len: int = 28) -> str:
    """Rightmost portion of a record key for axis/hover labels.

    Typical keys ('irradiation:10843:11297') fit whole.  Longer keys are cut
    at a ':' token boundary and marked with an ellipsis — a plain slice
    produced mid-token fragments like 'iation:10843:11297'."""
    if len(key) <= max_len:
        return key
    tail = key[-max_len:]
    cut = tail.find(":")
    if cut != -1:
        tail = tail[cut + 1:]
    return "…" + tail


def _v2_target_label(rec: dict[str, Any]) -> str:
    key = _v2_clean(rec.get("target_stress_record_key"))
    event = _v2_clean(rec.get("target_event_type")) or "target"
    return f"{event} · {_v2_key_tail(key)}" if key else event


def _v2_numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _with_v2_candidate_axis(df: pd.DataFrame) -> pd.DataFrame:
    """Attach candidate severity columns used by the v2 charts.

    Own-threshold failure fraction is the preferred Phase-5 axis. Some live
    exports legitimately lack a two-sided destruction boundary for every
    candidate, leaving that axis empty. In that case, fall back row-by-row to
    the exported Kosier-context severity interval so the viewer can still show
    the available screening comparison without pretending it is own-threshold.
    """
    out = df.copy()
    failure_low = _v2_numeric_column(out, "candidate_failure_fraction_low")
    failure_high = _v2_numeric_column(out, "candidate_failure_fraction_high")
    failure_point = _v2_numeric_column(out, "candidate_failure_fraction_point")
    kosier_low = _v2_numeric_column(out, "candidate_severity_low_kosier_context")
    kosier_high = _v2_numeric_column(out, "candidate_severity_high_kosier_context")
    kosier_point = _v2_numeric_column(out, "candidate_severity_point_ratio_kosier_context")

    use_failure = failure_low.notna() & failure_high.notna() & failure_point.notna()
    out["_v2_candidate_low"] = failure_low.where(use_failure, kosier_low)
    out["_v2_candidate_high"] = failure_high.where(use_failure, kosier_high)
    out["_v2_candidate_point"] = failure_point.where(use_failure, kosier_point)
    out["_v2_candidate_axis_basis"] = np.where(
        use_failure,
        "own-threshold failure fraction",
        "Kosier-context severity fallback",
    )
    failure_overlap = out.get("candidate_failure_fraction_overlap_class")
    if failure_overlap is None:
        failure_overlap = pd.Series("", index=out.index, dtype="object")
    kosier_overlap = out.get("critical_severity_overlap_class_kosier_context")
    if kosier_overlap is None:
        kosier_overlap = pd.Series("", index=out.index, dtype="object")
    out["_v2_candidate_overlap_class"] = failure_overlap.where(use_failure, kosier_overlap)
    return out


def v2_interval_overlap_plot_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """Per-target severity-interval overlap for v2's rank-1 candidate.

    For each target, draw the target stored-field severity band and the rank-1
    candidate's own-threshold failure-fraction band as horizontal intervals (error
    bars around the nominal point) on a shared log threshold-ratio axis,
    colored by the candidate failure-fraction overlap class.  Target and candidate stay
    semantically separate (separation invariant #1): this is a *screening
    descriptor*, never an equivalence claim, so every hover carries the v2
    status and blockers next to the numbers (Phase-5 acceptance).
    """
    base_title = "v2 target severity vs candidate severity proxy (rank-1 candidate per target)"
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
    if any(col not in df.columns for col in V2_TARGET_BAND_COLUMNS):
        return empty()
    df = _with_v2_candidate_axis(df)
    needed = V2_TARGET_BAND_COLUMNS + [
        "_v2_candidate_low", "_v2_candidate_high", "_v2_candidate_point",
    ]
    df = df.dropna(subset=needed)
    df = df[(df["target_severity_low"] > 0)
            & (df["target_severity_high"] > 0)
            & (df["target_severity_point_ratio"] > 0)
            & (df["_v2_candidate_low"] > 0)
            & (df["_v2_candidate_high"] > 0)
            & (df["_v2_candidate_point"] > 0)]
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
        "Candidate severity ratio: %{x:.3g}<br>"
        "Band: [%{customdata[1]}, %{customdata[2]}]<br>"
        "Basis: %{customdata[21]}<br>"
        "Overlap: %{customdata[3]}<br>"
        "v1 rank %{customdata[4]} → v2 rank %{customdata[5]}<br>"
        "Status: %{customdata[6]}<br>"
        "Proxy claim: %{customdata[11]} (%{customdata[12]})<br>"
        "Truth: %{customdata[13]} · %{customdata[14]} / %{customdata[15]}<br>"
        "v1 claim: %{customdata[18]} · rank %{customdata[19]} · signature %{customdata[20]}<br>"
        "Regime match: %{customdata[7]} · localization log10: %{customdata[8]}<br>"
        "Blockers: %{customdata[9]}<br>"
        "Claim blockers: %{customdata[16]}<br>"
        "Claim summary: %{customdata[17]}<br>"
        "Notes: %{customdata[10]}"
        "<extra>Candidate severity</extra>"
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

        c_x = [float(r["_v2_candidate_point"]) for r in recs]
        c_plus = [float(r["_v2_candidate_high"]) - x for r, x in zip(recs, c_x)]
        c_minus = [x - float(r["_v2_candidate_low"]) for r, x in zip(recs, c_x)]
        marker_colors = [
            CRITICAL_OVERLAP_COLORS.get(
                _v2_clean(r.get("_v2_candidate_overlap_class")), "#999999")
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
                display_value(r["_v2_candidate_low"]),
                display_value(r["_v2_candidate_high"]),
                _v2_clean(r.get("_v2_candidate_overlap_class")),
                _v2_clean(r.get("candidate_rank_v1")),
                _v2_clean(r.get("mechanistic_energy_candidate_rank")),
                _v2_clean(r.get("mechanistic_energy_candidate_status")),
                _v2_clean(r.get("regime_match_class")),
                _v2_clean(r.get("localization_mismatch_log10")),
                _v2_clean(r.get("energy_v2_blockers")) or "(none)",
                _v2_clean(r.get("energy_v2_notes")) or "(none)",
                _v2_clean(r.get("proxy_claim_status")) or "screening_only",
                _v2_clean(r.get("proxy_claim_basis")) or "not recorded",
                _v2_clean(r.get("truth_validation_status")) or "no_curated_truth",
                _v2_clean(r.get("truth_label")) or "unlabeled",
                _v2_clean(r.get("truth_label_basis")) or "unlabeled",
                _v2_clean(r.get("proxy_claim_blockers")) or "(none)",
                _v2_clean(r.get("proxy_claim_summary")) or "not recorded",
                _v2_clean(r.get("proxy_claim_status_v1")) or "not recorded",
                _v2_clean(r.get("decision_safe_rank_v1")) or "not ranked",
                _v2_clean(r.get("signature_claim_quality_v1")) or "not recorded",
                _v2_clean(r.get("_v2_candidate_axis_basis")),
            ] for r in recs],
            "hovertemplate": candidate_hover,
            "visible": True,
        })
        trace_device.extend([dev, dev])
        if dev is not None:
            titles[dev] = f"{base_title}<br>{dev}"

    layout = cartesian_legend_row_layout(base_title)
    if devices and devices[0] is not None:
        layout["title"]["text"] = titles[devices[0]]
    layout.update({
        "xaxis": {"title": {"text": "Ratio to threshold (log; 1.0 = threshold)"},
                  "type": "log", "gridcolor": "#d8dee4", "zerolinecolor": "#8c959f"},
        "yaxis": {"title": {"text": "Target"}, "automargin": True,
                  "gridcolor": "#eef1f4"},
        "shapes": [{"type": "line", "x0": 1.0, "x1": 1.0, "y0": 0, "y1": 1,
                    "yref": "paper", "line": {"color": "#8c959f", "dash": "dot", "width": 1}}],
    })

    own_boundary_usable = bool(
        "candidate_failure_fraction_gate_usable" in df.columns
        and df["candidate_failure_fraction_gate_usable"].fillna(False).astype(bool).any()
    )
    disabled_reason = None
    if not own_boundary_usable:
        disabled_reason = (
            "Own-threshold candidate failure-fraction coverage is 0%; interval "
            "bands are hidden until candidate destruction-boundary data exists."
        )
    note = (
        "Each row is one target's rank-1 v2 candidate. The blue band is the "
        "target stored-field severity interval (depletion ratio to its SEB/SELC "
        "threshold); the diamond band is the candidate severity interval. It "
        "uses the own electrical failure-threshold fraction when available and "
        "falls back to the Kosier-context severity interval when the candidate "
        "has no two-sided destruction boundary. Colors follow the plotted "
        "overlap class (green strong → red far-miss). These are different "
        "physical quantities on a shared "
        "screening axis — overlap is a retrieval hint, not an equivalence claim. "
        "Every candidate hover shows the v2 status and blockers next to the "
        "numbers. Use the device filter at the top to switch device."
    )
    payload = {
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
    if disabled_reason:
        payload["disabledReason"] = disabled_reason
    return payload


SEVERITY_CLASS_ORDER = [
    "strong_overlap", "partial_overlap", "near_miss", "far_miss", "missing_interval",
]
SEVERITY_CLASS_LABELS = {
    "strong_overlap": "strong overlap (equivalent)",
    "partial_overlap": "partial overlap",
    "near_miss": "near miss",
    "far_miss": "far miss",
    "missing_interval": "missing interval",
}
# The three comparable energy-overlap axes (shared strong→far vocabulary).
# cumulative-exposure uses a different present/missing vocabulary and is
# summarized separately.
V2_OVERLAP_SUMMARY_AXES = [
    ("candidate_failure_fraction_overlap_class", "Own-threshold severity"),
    ("critical_severity_overlap_class_kosier_context", "Kosier-context severity"),
    ("terminal_energy_overlap_class", "Terminal energy"),
    ("timescale_overlap_class", "Timescale"),
]


def _parity_log_range(lo: float, hi: float) -> list[float]:
    """Decade-aligned shared [x, y] log10 range covering ratios lo..hi.

    Shared between both axes so the y=x identity stays the visual diagonal;
    widened to at least one decade so a single-point device still renders."""
    lo_log = float(math.floor(math.log10(lo)))
    hi_log = float(math.ceil(math.log10(hi)))
    if hi_log <= lo_log:
        hi_log = lo_log + 1.0
    return [lo_log, hi_log]


def v2_severity_parity_plot_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """Target vs candidate severity-ratio equivalence parity scatter.

    Each point is one target's rank-1 candidate. X is the target stored-field
    severity ratio (depletion energy / its own SEB·SELC critical); Y is the
    candidate own-threshold failure fraction (terminal energy divided by its
    measured electrical destruction-boundary energy). Both axes are normalized to their own relevant threshold, so
    proximity to the y=x diagonal is a SCREENING equivalence (comparable
    multiples of each threshold) — never a claim the raw joules are equal
    (separation invariant #1). The ±0.5 / ±1.5 dex guides are the strong /
    partial overlap-class boundaries; the x=1 / y=1 crosshairs mark each side's
    critical threshold.
    """
    base_title = "v2 target severity vs candidate severity proxy (rank-1; log-log parity)"
    empty_note = (
        "No v2 rank-1 rows with positive target and candidate failure fractions. "
        "Export stress_proxy_candidate_energy_v2 with "
        "export_proxy_candidate_energy_v2_csv.py after applying schema/028."
    )

    def empty() -> dict[str, Any]:
        return {"traces": [], "layout": common_cartesian_layout(base_title),
                "note": empty_note}

    if rows is None or rows.empty:
        return empty()
    df = rows.copy()
    if "mechanistic_energy_candidate_rank" in df.columns:
        df = df[df["mechanistic_energy_candidate_rank"] == 1]
    need = ["target_severity_point_ratio"]
    if any(c not in df.columns for c in need):
        return empty()
    df = _with_v2_candidate_axis(df)
    df = df.dropna(subset=need + ["_v2_candidate_point"])
    df = df[(df["target_severity_point_ratio"] > 0)
            & (df["_v2_candidate_point"] > 0)]
    if df.empty:
        return empty()

    has_device = "device_type" in df.columns
    devices = sorted(df["device_type"].dropna().unique()) if has_device else [None]
    x_range_all = _parity_log_range(
        float(df["target_severity_point_ratio"].min()),
        float(df["target_severity_point_ratio"].max()),
    )
    y_range_all = _parity_log_range(
        float(df["_v2_candidate_point"].min()),
        float(df["_v2_candidate_point"].max()),
    )
    range_all = {"x": x_range_all, "y": y_range_all}
    shape_lo_log = min(x_range_all[0], y_range_all[0])
    shape_hi_log = max(x_range_all[1], y_range_all[1])

    hover = (
        "<b>%{customdata[0]}</b><br>"
        "Target severity ratio: %{x:.3g}<br>"
        "Candidate severity ratio: %{y:.3g}<br>"
        "Basis: %{customdata[8]}<br>"
        "Overlap: %{customdata[1]}<br>"
        "Candidate %{customdata[2]} · status %{customdata[3]}<br>"
        "Proxy claim: %{customdata[5]} · truth %{customdata[6]}<br>"
        "Blockers: %{customdata[4]}<br>"
        "Claim blockers: %{customdata[7]}"
        "<extra></extra>"
    )

    traces: list[dict[str, Any]] = []
    trace_device: list[Any] = []
    titles: dict[str, str] = {}
    ranges: dict[str, dict[str, list[float]]] = {}
    for dev in devices:
        sub = df if dev is None else df[df["device_type"] == dev]
        recs = sub.to_dict("records")
        if dev is not None:
            # Per-device axis window: the global range spans every device's
            # extremes (~11 decades live), which crushed a single device's
            # points into one corner of an empty canvas.
            ranges[dev] = {
                "x": _parity_log_range(
                    float(sub["target_severity_point_ratio"].min()),
                    float(sub["target_severity_point_ratio"].max()),
                ),
                "y": _parity_log_range(
                    float(sub["_v2_candidate_point"].min()),
                    float(sub["_v2_candidate_point"].max()),
                ),
            }
        colors = [
            CRITICAL_OVERLAP_COLORS.get(
                _v2_clean(r.get("_v2_candidate_overlap_class")), "#999999")
            for r in recs
        ]
        traces.append({
            "type": "scatter", "mode": "markers",
            "name": _v2_clean(dev) or "pairs",
            "showlegend": False,
            "x": [float(r["target_severity_point_ratio"]) for r in recs],
            "y": [float(r["_v2_candidate_point"]) for r in recs],
            "marker": {"color": colors, "size": 7, "opacity": 0.82,
                       "line": {"color": "#24292f", "width": 0.4}},
            "customdata": [[
                (_v2_clean(r.get("target_event_type")) + " · "
                 + _v2_key_tail(_v2_clean(r.get("target_stress_record_key")))),
                _v2_clean(r.get("_v2_candidate_overlap_class")),
                _v2_clean(r.get("candidate_source")),
                _v2_clean(r.get("mechanistic_energy_candidate_status")),
                _v2_clean(r.get("energy_v2_blockers")) or "(none)",
                _v2_clean(r.get("proxy_claim_status")) or "screening_only",
                _v2_clean(r.get("truth_validation_status")) or "no_curated_truth",
                _v2_clean(r.get("proxy_claim_blockers")) or "(none)",
                _v2_clean(r.get("_v2_candidate_axis_basis")),
            ] for r in recs],
            "hovertemplate": hover,
            "visible": True,
        })
        trace_device.append(dev)
        if dev is not None:
            titles[dev] = f"{base_title}<br>{dev}"

    # Stable color key (always visible, device-independent) so the legend does
    # not vanish when the device filter hides a device's data trace.
    for cls in SEVERITY_CLASS_ORDER:
        traces.append({
            "type": "scatter", "mode": "markers",
            "name": SEVERITY_CLASS_LABELS[cls],
            "x": [None], "y": [None],
            "marker": {"color": CRITICAL_OVERLAP_COLORS[cls], "size": 10},
            "showlegend": True, "hoverinfo": "skip", "visible": True,
        })
        trace_device.append(None)

    # Identity, ±dex overlap bands, and ratio=1 crosshairs. Plotly shape
    # coordinates on a log axis are in log10 units, so a y=x diagonal is the
    # line log10(y)=log10(x) and the ±dex bands are parallel offsets.
    def diag(offset: float, dash: str, color: str, width: float) -> dict[str, Any]:
        return {"type": "line", "xref": "x", "yref": "y",
                "x0": shape_lo_log, "y0": shape_lo_log + offset,
                "x1": shape_hi_log, "y1": shape_hi_log + offset,
                "line": {"color": color, "dash": dash, "width": width}}
    shapes = [
        diag(0.0, "solid", "#24292f", 1.4),
        diag(0.5, "dash", "#1a9850", 1.0), diag(-0.5, "dash", "#1a9850", 1.0),
        diag(1.5, "dot", "#fdae61", 1.0), diag(-1.5, "dot", "#fdae61", 1.0),
        {"type": "line", "xref": "x", "yref": "y", "x0": 0.0, "y0": y_range_all[0],
         "x1": 0.0, "y1": y_range_all[1], "line": {"color": "#8c959f", "dash": "dot", "width": 1}},
        {"type": "line", "xref": "x", "yref": "y", "x0": x_range_all[0], "y0": 0.0,
         "x1": x_range_all[1], "y1": 0.0, "line": {"color": "#8c959f", "dash": "dot", "width": 1}},
    ]

    layout = cartesian_legend_row_layout(base_title)
    layout.update({
        "xaxis": {"title": {"text": "Target severity ratio "
                            "(stored depletion ÷ its SEB·SELC critical; log)"},
                  "type": "log", "range": x_range_all, "gridcolor": "#d8dee4"},
        "yaxis": {"title": {"text": "Candidate severity proxy "
                            "(own threshold when available; Kosier fallback; log)"},
                  "type": "log", "range": y_range_all, "gridcolor": "#d8dee4"},
        "shapes": shapes,
    })
    note = (
        "Each point is one target's rank-1 candidate, all devices together "
        "(pick a device above to focus each axis on that device's own X/Y "
        "range). Points ON the black "
        "diagonal are severity-equivalent; green dashed = ±0.5 dex strong-overlap "
        "band, orange dotted = ±1.5 dex partial band. Gray crosshairs at ratio=1 "
        "mark each side's critical threshold. Most rank-1 candidates sit far "
        "ABOVE the diagonal — they reach a far higher multiple of their own "
        "threshold than the irradiation target does, which is why critical "
        "severity overlap is mostly far-miss. Candidate points use the own "
        "threshold when a two-sided destruction boundary exists; otherwise they "
        "use the exported Kosier-context severity fallback. This is a screening "
        "comparison, not a claim the raw joules are equal. For SELC targets, "
        "the target axis is a leakage-onset fraction while the candidate axis "
        "is a destruction-threshold fraction; they are like-for-like only as "
        "fractions of each side's own threshold."
    )
    return {
        "traces": traces, "layout": layout, "note": note,
        "filter": {
            "devices": [d for d in devices if d is not None],
            "traceDevices": trace_device,
            "titleAll": base_title, "titles": titles,
            # "All devices" really shows every device here (the scatter reads
            # fine overlaid); per-device selection also swaps the axis window.
            "allShowsOnly": None,
            "ranges": ranges,
            "rangeAll": range_all,
        },
    }


def v2_overlap_summary_plot_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """Stacked bar: rank-1 candidates per overlap class, for each energy axis.

    The one-glance "where we got": how many rank-1 candidates fall in
    strong/partial/near/far/missing overlap on each comparable energy axis
    (critical severity, terminal energy, power/rate). Cumulative-exposure is an
    evidence-availability axis (present/missing), not an overlap axis, so it is
    summarized in the note instead of the shared color legend. Global summary —
    no device filter (use the parity tab for per-device detail).
    """
    base_title = "v2 energy-equivalence overlap summary (rank-1)"
    empty_note = (
        "No v2 rank-1 rows to summarize. Export "
        "stress_proxy_candidate_energy_v2 with "
        "export_proxy_candidate_energy_v2_csv.py after applying schema/028."
    )
    if rows is None or rows.empty:
        return {"traces": [], "layout": common_cartesian_layout(base_title),
                "note": empty_note}
    df = rows.copy()
    if "mechanistic_energy_candidate_rank" in df.columns:
        df = df[df["mechanistic_energy_candidate_rank"] == 1]
    present_axes = [(c, l) for c, l in V2_OVERLAP_SUMMARY_AXES if c in df.columns]
    if df.empty or not present_axes:
        return {"traces": [], "layout": common_cartesian_layout(base_title),
                "note": empty_note}

    axis_labels = [lbl for _, lbl in present_axes]
    total = len(df)
    traces: list[dict[str, Any]] = []
    for cls in SEVERITY_CLASS_ORDER:
        counts = [int((df[col] == cls).sum()) for col, _ in present_axes]
        if sum(counts) == 0:
            continue
        traces.append({
            "type": "bar", "orientation": "h",
            "name": SEVERITY_CLASS_LABELS[cls],
            "y": axis_labels, "x": counts,
            "marker": {"color": CRITICAL_OVERLAP_COLORS[cls],
                       "line": {"color": "#24292f", "width": 0.4}},
            "customdata": [[f"{(c / total * 100):.0f}%" if total else "0%"]
                           for c in counts],
            "hovertemplate": ("<b>%{y}</b><br>" + SEVERITY_CLASS_LABELS[cls]
                              + ": %{x} (%{customdata[0]})<extra></extra>"),
        })

    layout = cartesian_legend_row_layout(base_title)
    layout.update({
        "barmode": "stack",
        "xaxis": {"title": {"text": f"Rank-1 candidates (n={total})"},
                  "gridcolor": "#d8dee4"},
        "yaxis": {"title": {"text": "Energy axis"}, "automargin": True},
    })
    cum_note = ""
    if "cumulative_exposure_overlap_class" in df.columns:
        cc = df["cumulative_exposure_overlap_class"].value_counts().to_dict()
        cum_note = (" Cumulative-exposure (evidence availability, not overlap): "
                    + ", ".join(f"{k}={v}" for k, v in cc.items()) + ".")
    modal_bits = []
    for col, label in present_axes:
        counts = df[col].fillna("missing_interval").astype(str).value_counts()
        if counts.empty:
            continue
        modal_bits.append(f"{label}: {counts.index[0]} ({int(counts.iloc[0])}/{total})")
    modal_sentence = "; ".join(modal_bits) if modal_bits else "no overlap classes recorded"
    note = (
        f"Rank-1 candidates (n={total}) by overlap class on each comparable "
        f"energy axis. Current modal state: {modal_sentence}. The own-threshold "
        "severity row is the decision-quality axis; when it is missing, the "
        "Kosier-context row is an explicit fallback diagnostic rather than an "
        "equivalence claim. Green=strong overlap, orange=near/partial miss, "
        "red=far miss, gray=missing interval." + cum_note
        + " Global summary; use the parity tab for per-device, per-pair detail."
    )
    return {"traces": traces, "layout": layout, "note": note}


# v1 (damage-signature) vs v2 (energy) concordance categories.

CONCORDANCE_LABELS = {
    "consensus": "Consensus (both rank-1)",
    "v2_pick": "v2 (energy) pick",
    "v1_pick": "v1 (damage-sig) pick",
    "strong_disagree": "v2 pick · v1 demoted",
    "conflict_focus": "C2M0080120D avalanche-vs-SC conflict",
}


def _concordance_coords(rec: dict[str, Any]) -> tuple[float, float, float] | None:
    """(signature-axis distance, failure-fraction distance, terminal distance).

    X is v1's prior-free signature_axis_distance.  Y is the v2 target-severity
    vs candidate-failure-fraction log-distance when the own-threshold boundary
    is usable; otherwise it falls back to the staged overlap-class score
    (strong=0, partial=1, near=2, far=3, missing=4).  Z is |log10 terminal
    energy delta|.
    """
    try:
        x = float(rec.get("signature_axis_distance", rec.get("damage_signature_distance")))
        led = float(rec.get("log_energy_delta_dex"))
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(x) and math.isfinite(led)):
        return None

    y = finite_number(rec.get("v2_pick_dssig_percentile"))
    if y is None:
        try:
            tsr = float(rec.get("target_severity_point_ratio"))
            csr = float(rec.get("candidate_failure_fraction_point"))
            if math.isfinite(tsr) and math.isfinite(csr) and tsr > 0 and csr > 0:
                y = abs(math.log10(csr) - math.log10(tsr))
        except (TypeError, ValueError):
            y = None
    if y is None:
        y = {
            "strong_overlap": 0.0,
            "partial_overlap": 1.0,
            "near_miss": 2.0,
            "far_miss": 3.0,
            "missing_interval": 4.0,
        }.get(_v2_clean(rec.get("candidate_failure_fraction_overlap_class")), 4.0)
    return (x, y, abs(led))


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "t", "1", "yes"}


def _concordance_customdata(rec: dict[str, Any], category: str) -> list[str]:
    return [
        CONCORDANCE_LABELS[category],
        (_v2_clean(rec.get("target_event_type")) + " · "
         + _v2_key_tail(_v2_clean(rec.get("target_stress_record_key")))),
        _v2_clean(rec.get("candidate_source")),
        _v2_clean(rec.get("v1_rank")),
        _v2_clean(rec.get("v2_rank")),
        _v2_clean(rec.get("mechanistic_energy_candidate_status")),
        _v2_clean(rec.get("candidate_failure_fraction_overlap_class")),
        _v2_clean(rec.get("energy_v2_blockers")) or "(none)",
        _v2_clean(rec.get("proxy_claim_status")) or "screening_only",
        _v2_clean(rec.get("proxy_claim_basis")) or "not recorded",
        _v2_clean(rec.get("truth_validation_status")) or "no_curated_truth",
        _v2_clean(rec.get("proxy_claim_status_v1"))
        or _v2_clean(rec.get("v1_proxy_claim_status"))
        or "not recorded",
        _v2_clean(rec.get("signature_claim_quality_v1"))
        or _v2_clean(rec.get("v1_signature_claim_quality"))
        or "not recorded",
        "C2M0080120D avalanche-vs-SC conflict"
        if _truthy_flag(rec.get("c2m0080120d_avalanche_vs_sc_conflict"))
        else ("source conflict" if _truthy_flag(rec.get("source_conflict")) else "no source conflict"),
    ]


def concordance_3d_plot_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """3D distance space comparing the two proxy methods' rank-1 picks.

    Per target, the v1 (damage-signature) pick and v2 (energy) pick are placed
    in a shared distance space — X = v1 prior-free signature-axis distance,
    Y = v2 failure-fraction distance (class fallback), Z = terminal-energy distance (dex) —
    and joined by a line whose length is literally how far apart the two methods'
    chosen proxies are. Consensus picks coincide (zero-length); disagreements
    pull toward different axes. Colored by agreement; v1 picks demoted out of
    v2's top-10 are flagged as strong disagreement.
    """
    base_title = "v1 damage-signature vs v2 energy — proxy concordance (3D)"
    empty_note = (
        "No concordance rows. Export the v1×v2 join with "
        "export_proxy_method_concordance_csv.py after applying schema/028."
    )
    scene = {
        "dragmode": "orbit",
        "xaxis": {"title": {"text": "v1 signature-axis distance (prior-free)"}},
        "yaxis": {"title": {"text": "v2 pick percentile in v1 ordering / fallback"}},
        "zaxis": {"title": {"text": "terminal-energy distance (dex)"}},
    }

    def empty() -> dict[str, Any]:
        return {"traces": [], "layout": common_layout(base_title, scene),
                "note": empty_note}

    if rows is None or rows.empty:
        return empty()
    df = rows.copy()
    required = ["target_stress_record_key", "v1_rank", "v2_rank",
                "signature_axis_distance", "target_severity_point_ratio",
                "candidate_failure_fraction_point", "log_energy_delta"]
    if any(c not in df.columns for c in required):
        return empty()
    for col in ("v1_rank", "v2_rank", "signature_axis_distance",
                "target_severity_point_ratio", "candidate_failure_fraction_point",
                "log_energy_delta"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["log_energy_delta_dex"] = dex_series(
        df, "log_energy_delta_dex", "log_energy_delta"
    )

    # device -> parallel point arrays; device -> connector segments.
    dev_x: dict[Any, list] = {}
    dev_pts: dict[Any, dict[str, list]] = {}
    dev_conn: dict[Any, dict[str, list]] = {}
    counts = {"consensus": 0, "mild": 0, "strong": 0, "unplottable": 0, "conflict_focus": 0}

    def _add_point(device, coord, category, rec):
        p = dev_pts.setdefault(device, {"x": [], "y": [], "z": [],
                                        "color": [], "symbol": [], "cdat": []})
        color, symbol = CONCORDANCE_STYLE[category]
        if _truthy_flag(rec.get("c2m0080120d_avalanche_vs_sc_conflict")):
            color, symbol = CONCORDANCE_STYLE["conflict_focus"]
        p["x"].append(coord[0]); p["y"].append(coord[1]); p["z"].append(coord[2])
        p["color"].append(color); p["symbol"].append(symbol)
        p["cdat"].append(_concordance_customdata(rec, category))

    for _tkey, group in df.groupby("target_stress_record_key"):
        recs = group.to_dict("records")
        exported_v2_ranks = [finite_number(r.get("v2_rank")) for r in recs]
        exported_v2_ranks = [r for r in exported_v2_ranks if r is not None and r > 0]
        exported_top_n = int(max(exported_v2_ranks)) if exported_v2_ranks else len(recs)
        v2_pick = next((r for r in recs if r.get("v2_rank") == 1), None)
        if v2_pick is None:
            continue
        device = _v2_clean(v2_pick.get("device_type")) or None
        v2c = _concordance_coords(v2_pick)
        if _truthy_flag(v2_pick.get("c2m0080120d_avalanche_vs_sc_conflict")):
            counts["conflict_focus"] += 1
        v1_pick = next((r for r in recs if r.get("v1_rank") == 1), None)
        if v1_pick is not None and v2_pick.get("v1_rank") == 1:
            if v2c is None:
                counts["unplottable"] += 1
                continue
            _add_point(device, v2c, "consensus", v2_pick)
            counts["consensus"] += 1
        elif v1_pick is not None:
            v1c = _concordance_coords(v1_pick)
            if v2c is None or v1c is None:
                counts["unplottable"] += 1
                continue
            _add_point(device, v2c, "v2_pick", v2_pick)
            _add_point(device, v1c, "v1_pick", v1_pick)
            conn = dev_conn.setdefault(device, {"x": [], "y": [], "z": []})
            conn["x"] += [v1c[0], v2c[0], None]
            conn["y"] += [v1c[1], v2c[1], None]
            conn["z"] += [v1c[2], v2c[2], None]
            counts["mild"] += 1
        else:
            if v2c is None:
                counts["unplottable"] += 1
                continue
            _add_point(device, v2c, "strong_disagree", v2_pick)
            counts["strong"] += 1

    if not dev_pts:
        return empty()
    devices = sorted(d for d in dev_pts if d is not None)
    ordered = devices or [None]

    hover = (
        "<b>%{customdata[0]}</b><br>"
        "%{customdata[1]} · cand %{customdata[2]}<br>"
        "v1 rank %{customdata[3]} → v2 rank %{customdata[4]}<br>"
        "signature %{x:.3g} · enrichment/fallback Y %{y:.2f} · terminal %{z:.2f} dex<br>"
        "v2 %{customdata[5]} · overlap %{customdata[6]}<br>"
        "proxy claim: %{customdata[8]} (%{customdata[9]}) · truth %{customdata[10]}<br>"
        "v1 claim: %{customdata[11]} · signature %{customdata[12]}<br>"
        "conflict: %{customdata[13]}<br>"
        "blockers: %{customdata[7]}<extra></extra>"
    )

    traces: list[dict[str, Any]] = []
    trace_device: list[Any] = []
    titles: dict[str, str] = {}
    for dev in ordered:
        p = dev_pts.get(dev)
        if p and p["x"]:
            traces.append({
                "type": "scatter3d", "mode": "markers",
                "name": _v2_clean(dev) or "picks", "showlegend": False,
                "x": p["x"], "y": p["y"], "z": p["z"],
                "marker": {"size": 4, "color": p["color"], "symbol": p["symbol"],
                           "line": {"color": "#24292f", "width": 0.3}, "opacity": 0.9},
                "customdata": p["cdat"], "hovertemplate": hover, "visible": True,
            })
            trace_device.append(dev)
        conn = dev_conn.get(dev)
        if conn and conn["x"]:
            traces.append({
                "type": "scatter3d", "mode": "lines",
                "name": "v1↔v2 distance", "showlegend": False,
                "x": conn["x"], "y": conn["y"], "z": conn["z"],
                "line": {"color": "#8c959f", "width": 2},
                "hoverinfo": "skip", "visible": True,
            })
            trace_device.append(dev)
        if dev is not None:
            titles[dev] = f"{base_title}<br>{dev}"

    # Legend proxies (always visible) for the agreement categories + connector.
    for cat in ("consensus", "v2_pick", "v1_pick", "strong_disagree", "conflict_focus"):
        color, symbol = CONCORDANCE_STYLE[cat]
        traces.append({
            "type": "scatter3d", "mode": "markers",
            "name": CONCORDANCE_LABELS[cat], "x": [None], "y": [None], "z": [None],
            "marker": {"size": 6, "color": color, "symbol": symbol},
            "showlegend": True, "hoverinfo": "skip", "visible": True,
        })
        trace_device.append(None)
    traces.append({
        "type": "scatter3d", "mode": "lines", "name": "v1↔v2 distance",
        "x": [None], "y": [None], "z": [None],
        "line": {"color": "#8c959f", "width": 2},
        "showlegend": True, "hoverinfo": "skip", "visible": True,
    })
    trace_device.append(None)

    layout = common_layout(base_title, scene)
    plotted = counts["consensus"] + counts["mild"] + counts["strong"]
    note = (
        f"Each target places its v1 (damage-signature) and v2 (energy) rank-1 "
        f"pick in a shared distance space; the gray line is the distance between "
        f"the two methods' choices. Near the origin = strong proxy on all axes. "
        f"Of {plotted} plotted targets: {counts['consensus']} consensus (both "
        f"rank-1 the same candidate — points coincide), {counts['mild']} mild "
        f"disagreement (different pick inside v2's top-10, connected by a line), "
        f"{counts['strong']} strong disagreement (v1's pick demoted out of v2's "
        f"top-10 — only v2's pick shown, in red). "
        f"{counts['conflict_focus']} C2M0080120D avalanche-vs-SC conflict targets "
        f"are highlighted with the conflict accent. {counts['unplottable']} more "
        f"targets can't be placed in 3D (signature or terminal-energy axis is "
        f"missing), itself a data-coverage signal. X is energy-FREE "
        f"(signature_axis_distance), so agreement here is an independent "
        f"cross-method check, not circular. All devices shown together by "
        f"default; drag to orbit, or pick a device above to isolate it."
    )
    return {
        "traces": traces, "layout": layout, "note": note,
        "filter": {
            "devices": [d for d in ordered if d is not None],
            "traceDevices": trace_device,
            "titleAll": base_title, "titles": titles,
            # "All devices" really shows the whole cloud — the note's global
            # counts then match what is on screen.
            "allShowsOnly": None,
        },
    }


V3_COMPONENTS = [
    ("signature", "signature_component_share", "signature_component_weighted_sq"),
    ("duration", "duration_component_share", "duration_component_weighted_sq"),
    ("failure fraction", "failure_fraction_component_share", "failure_fraction_component_weighted_sq"),
    ("regime/path", "regime_path_component_share", "regime_path_component_weighted_sq"),
    ("terminal energy", "log_energy_component_share", "log_energy_component_weighted_sq"),
    ("post-IV damage", "post_iv_damage_component_share", "post_iv_damage_component_weighted_sq"),
    ("coverage gap", "coverage_gap_component_share", "coverage_gap_component_weighted_sq"),
]


def concordance_enrichment_ecdf_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """2D enrichment ECDF for v2 picks inside v1's signature ordering."""
    base_title = "v2 picks inside v1 prior-free signature ordering — enrichment ECDF"
    layout = common_cartesian_layout(base_title)
    layout.update({
        "xaxis": {"title": "v2-pick percentile in v1 signature ordering (lower is better)",
                  "range": [0, 100]},
        "yaxis": {"title": "Cumulative share of v2 rank-1 targets", "range": [0, 1]},
        "shapes": [
            {"type": "line", "xref": "x", "yref": "paper", "x0": 10, "x1": 10,
             "y0": 0, "y1": 1, "line": {"color": "#8c959f", "dash": "dash"}},
            {"type": "line", "xref": "x", "yref": "paper", "x0": 50, "x1": 50,
             "y0": 0, "y1": 1, "line": {"color": "#d0d7de", "dash": "dot"}},
        ],
    })
    if rows is None or rows.empty or "v2_pick_dssig_percentile" not in rows:
        return {"traces": [], "layout": layout,
                "note": "No enrichment columns found. Re-export proxy_method_concordance.csv after applying schema/029."}
    df = rows.copy()
    df = df[pd.to_numeric(df.get("v2_rank"), errors="coerce") == 1]
    df["percentile"] = numeric(df["v2_pick_dssig_percentile"])
    df = df[df["percentile"].notna()]
    if df.empty:
        return {"traces": [], "layout": layout,
                "note": "No v2 rank-1 rows have a signature-ordering percentile."}
    traces = []
    trace_device = []
    titles = {}
    for scope, sub in df.groupby(df.get("match_scope", pd.Series("all", index=df.index)).fillna("all")):
        vals = sorted(float(v) for v in sub["percentile"] if math.isfinite(float(v)))
        if not vals:
            continue
        y = [(i + 1) / len(vals) for i in range(len(vals))]
        color = "#24292f" if scope == "cross_device" else DEEMPHASIS_GRAY
        traces.append({
            "type": "scatter", "mode": "lines+markers", "name": str(scope),
            "x": vals, "y": y,
            "line": {"color": color, "width": 3 if scope == "cross_device" else 2},
            "marker": {"size": 5, "color": color},
            "hovertemplate": "percentile %{x:.1f}<br>ECDF %{y:.1%}<extra>" + str(scope) + "</extra>",
            "visible": True,
        })
        trace_device.append(None)
    median = float(df["percentile"].median())
    best_decile = int((df["percentile"] <= 10.0).sum())
    note = (
        f"Post-separation enrichment check: exact rank-1 concordance is a lower-bound diagnostic, "
        f"while this ECDF asks where v2 rank-1 picks fall in v1's energy-free signature ordering. "
        f"Median percentile is {median:.1f}; {best_decile}/{len(df)} ({best_decile / len(df):.1%}) "
        f"fall in the best decile. Cross-device enrichment is the independence-evidence series; "
        f"same-device rows are context."
    )
    return {"traces": traces, "layout": layout, "note": note,
            "filter": {"devices": [], "traceDevices": trace_device,
                       "titleAll": base_title, "titles": {}, "allShowsOnly": None}}


def v3_vector_explorer_payload(rows: pd.DataFrame) -> dict[str, Any]:
    """Horizontal stacked bars of v3 weighted component shares for rank-1 picks."""
    base_title = "v3 combined vector — rank-1 weighted component shares"
    layout = common_cartesian_layout(base_title)
    layout.update({
        "barmode": "stack",
        "xaxis": {"title": "Share of weighted squared vector distance", "range": [0, 1], "tickformat": ".0%"},
        "yaxis": {"title": "target -> v3 rank-1 candidate", "automargin": True},
        "legend": {"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        "margin": {"l": 260, "r": 30, "t": 90, "b": 70},
        "height": 900,
    })
    if rows is None or rows.empty:
        return {"traces": [], "layout": layout,
                "note": "No v3 CSV found. Run export_proxy_candidate_combined_v3_csv.py after applying schema/028."}
    df = rows.copy()
    if "combined_rank" not in df:
        return {"traces": [], "layout": layout,
                "note": "v3 CSV is missing combined_rank; regenerate the v3 export."}
    df["combined_rank"] = numeric(df["combined_rank"])
    df = df[df["combined_rank"] == 1].copy()
    if df.empty:
        return {"traces": [], "layout": layout,
                "note": "No v3 rank-1 rows in the export."}
    df["combined_vector_distance"] = numeric_column(df, "combined_vector_distance")
    df = df.sort_values(["combined_vector_distance", "target_stress_record_key"], na_position="last").head(80)
    labels = [
        (_v2_clean(r.get("target_event_type")) + " · " + _v2_key_tail(_v2_clean(r.get("target_stress_record_key")))
         + " -> " + _v2_clean(r.get("candidate_source")))
        for r in df.to_dict("records")
    ]
    traces = []
    trace_device = []
    for label, share_col, sq_col in V3_COMPONENTS:
        shares = numeric_column(df, share_col).fillna(0.0).clip(lower=0.0, upper=1.0)
        sq = numeric_column(df, sq_col).fillna(0.0)
        traces.append({
            "type": "bar", "orientation": "h", "name": label,
            "x": shares.tolist(), "y": labels,
            "marker": {"color": V3_COMPONENT_COLORS[label]},
            "customdata": np.stack([
                sq.astype(float).to_numpy(),
                numeric_column(df, "combined_vector_distance").fillna(np.nan).to_numpy(),
                df.get("proxy_claim_status", pd.Series("", index=df.index)).fillna("").astype(str).to_numpy(),
            ], axis=-1).tolist(),
            "hovertemplate": (
                "%{y}<br>" + label + " share %{x:.1%}<br>weighted sq %{customdata[0]:.3g}"
                "<br>combined distance %{customdata[1]:.3g}<br>proxy claim %{customdata[2]}<extra></extra>"
            ),
            "visible": True,
        })
        trace_device.append(None)
    note = (
        "v3 is an uncalibrated, screening-only weighted vector over v2's top-10 pool. "
        "Each bar sums the seven weighted squared terms for the rank-1 combined pick; "
        "large segments show which axis drove the choice and should be inspected before trusting weights."
    )
    return {"traces": traces, "layout": layout, "note": note,
            "filter": {"devices": [], "traceDevices": trace_device,
                       "titleAll": base_title, "titles": {}, "allShowsOnly": None}}



def overview_payload(
    source_records: pd.DataFrame,
    delta_rows: pd.DataFrame,
    v2_rows: pd.DataFrame,
    concordance_rows: pd.DataFrame,
    v3_rows: pd.DataFrame,
) -> dict[str, Any]:
    """Landing view: build-time readiness KPIs plus candidate funnel."""
    title = "Proxy readiness overview"
    v2_rank1 = _rank1(v2_rows, "mechanistic_energy_candidate_rank")
    v3_rank1 = _rank1(v3_rows, "combined_rank")
    conc_rank1 = _rank1(concordance_rows, "v2_rank")

    pool_total = None
    pool_source = "not exported"
    if delta_rows is not None and not delta_rows.empty and "dssig_pool_size" in delta_rows.columns:
        pool_sizes = delta_rows.copy()
        pool_sizes["_dssig_pool_size"] = numeric(pool_sizes["dssig_pool_size"])
        pool_sizes = pool_sizes.dropna(subset=["_dssig_pool_size"])
        if "target_stress_record_key" in pool_sizes.columns:
            pool_total = pool_sizes.groupby("target_stress_record_key")["_dssig_pool_size"].max().sum()
            pool_source = "delta-export target universe"
        elif "target_record_key" in pool_sizes.columns:
            pool_total = pool_sizes.groupby("target_record_key")["_dssig_pool_size"].max().sum()
            pool_source = "delta-export target universe"
        else:
            pool_total = pool_sizes["_dssig_pool_size"].sum()
            pool_source = "delta export rows"
    elif not conc_rank1.empty and "dssig_pool_size" in conc_rank1.columns:
        pool_total = numeric(conc_rank1["dssig_pool_size"]).dropna().sum()
        pool_source = "v2-covered concordance targets"
    top_export = len(delta_rows) if delta_rows is not None and not delta_rows.empty else len(concordance_rows)

    def decision_safe_count(frame: pd.DataFrame) -> tuple[int, int]:
        if frame is None or frame.empty:
            return (0, 0)
        rank_col = "decision_safe_rank" if "decision_safe_rank" in frame.columns else "v1_decision_safe_rank"
        status_col = "proxy_claim_status" if "proxy_claim_status" in frame.columns else "v1_proxy_claim_status"
        if rank_col in frame.columns:
            safe = numeric(frame[rank_col]).notna()
            rank1 = numeric(frame[rank_col]).eq(1)
            return (int(safe.sum()), int(rank1.sum()))
        if status_col in frame.columns:
            status = frame[status_col].fillna("").astype(str)
            safe = status.isin(["validation_candidate", "curation_candidate"])
            return (int(safe.sum()), 0)
        return (0, 0)

    decision_safe, decision_rank1 = decision_safe_count(delta_rows)
    funnel_labels = ["Ranked pool", "Exported top candidates", "Decision-safe", "Decision-safe rank-1"]
    funnel_values = [
        int(pool_total) if pool_total and math.isfinite(float(pool_total)) else None,
        int(top_export),
        int(decision_safe),
        int(decision_rank1),
    ]
    plotted_labels = [label for label, value in zip(funnel_labels, funnel_values) if value is not None]
    plotted_values = [value for value in funnel_values if value is not None]

    traces = [{
        "type": "bar",
        "orientation": "h",
        "y": plotted_labels,
        "x": plotted_values,
        "marker": {"color": ["#6f7782", "#2f6f9f", "#1a9850", "#54a24b"][:len(plotted_values)]},
        "hovertemplate": "%{y}: %{x:,}<extra></extra>",
    }]
    layout = common_cartesian_layout(title)
    layout.update({
        "height": 700,
        "xaxis": {"title": {"text": "Candidate rows"}, "gridcolor": "#d8dee4"},
        "yaxis": {"title": {"text": "Funnel stage"}, "automargin": True},
        "margin": {"l": 190, "r": 36, "t": 88, "b": 70},
    })

    terminal_counts = _value_counts(v2_rank1, "terminal_energy_overlap_class")
    kosier_counts = _value_counts(v2_rank1, "critical_severity_overlap_class_kosier_context")
    source_counts = _value_counts(v2_rank1, "candidate_source")
    boundary_usable = int(
        v2_rank1.get("candidate_failure_fraction_gate_usable", pd.Series(False, index=v2_rank1.index))
        .fillna(False).astype(bool).sum()
    ) if not v2_rank1.empty else 0
    truth_curated = int(_truth_status_is_curated(
        v2_rank1.get("truth_validation_status", pd.Series(index=v2_rank1.index, dtype="object"))
    ).sum()) if not v2_rank1.empty else 0
    median_enrichment = None
    best_decile = None
    if not conc_rank1.empty and "v2_pick_dssig_percentile" in conc_rank1.columns:
        pct = numeric(conc_rank1["v2_pick_dssig_percentile"]).dropna()
        if not pct.empty:
            median_enrichment = float(pct.median())
            best_decile = int((pct <= 10.0).sum())

    note_bits = [
        f"v2 rank-1 targets: {len(v2_rank1):,}",
        f"v3 rank-1 targets exported: {len(v3_rank1):,}",
        f"own-boundary coverage: {_pct(boundary_usable, len(v2_rank1))}",
        f"curated truth labels on v2 rank-1: {truth_curated:,}",
        f"ranked-pool denominator: {pool_source}",
        "terminal overlap: " + ", ".join(f"{k}={v}" for k, v in terminal_counts.items()) if terminal_counts else "terminal overlap: not exported",
        "Kosier-context severity: " + ", ".join(f"{k}={v}" for k, v in kosier_counts.items()) if kosier_counts else "Kosier-context severity: not exported",
        "rank-1 source mix: " + ", ".join(f"{k}={v}" for k, v in source_counts.items()) if source_counts else "rank-1 source mix: not exported",
    ]
    if median_enrichment is None:
        note_bits.append("enrichment percentile: not exported; apply schema/029 and re-export concordance")
    else:
        note_bits.append(
            f"enrichment: median {median_enrichment:.1f}th percentile; "
            f"best-decile share {_pct(best_decile or 0, len(numeric(conc_rank1['v2_pick_dssig_percentile']).dropna()))}"
        )
    note = "Overview computed from the CSVs at build time. " + "; ".join(note_bits) + "."
    return {"traces": traces, "layout": layout, "note": note}


def boundary_coverage_payload(v2_rows: pd.DataFrame) -> dict[str, Any]:
    title = "Candidate destruction-boundary coverage by device"
    rank1 = _rank1(v2_rows, "mechanistic_energy_candidate_rank")
    if rank1.empty or "candidate_failure_fraction_gate_usable" not in rank1.columns:
        return _empty_payload(title, "No v2 rank-1 boundary-coverage columns are exported.")
    device = rank1.get("device_type", pd.Series("unknown", index=rank1.index)).fillna("unknown").astype(str)
    usable = rank1["candidate_failure_fraction_gate_usable"].fillna(False).astype(bool)
    table = pd.DataFrame({"device": device, "usable": usable})
    grouped = table.groupby("device")["usable"].agg(["sum", "count"]).sort_values("count", ascending=False)
    devices = grouped.index.tolist()
    usable_counts = grouped["sum"].astype(int).tolist()
    missing_counts = (grouped["count"] - grouped["sum"]).astype(int).tolist()
    traces = [
        {"type": "bar", "orientation": "h", "name": "usable own-boundary", "y": devices, "x": usable_counts,
         "marker": {"color": "#1a9850"}},
        {"type": "bar", "orientation": "h", "name": "missing own-boundary", "y": devices, "x": missing_counts,
         "marker": {"color": "#d73027"}},
    ]
    layout = cartesian_legend_row_layout(title)
    layout.update({
        "barmode": "stack",
        "height": max(560, 34 * len(devices) + 220),
        "xaxis": {"title": {"text": "v2 rank-1 targets"}, "gridcolor": "#d8dee4"},
        "yaxis": {"title": {"text": "Device"}, "automargin": True},
    })
    total = int(grouped["count"].sum())
    usable_total = int(grouped["sum"].sum())
    note = (
        f"Own-threshold candidate failure-fraction coverage is {usable_total}/{total} "
        f"({_pct(usable_total, total)}). Missing rows fall back to Kosier-context severity in the parity view; "
        "this chart is the measurement-priority view for collecting candidate destruction boundaries."
    )
    return {"traces": traces, "layout": layout, "note": note}


def energy_context_2d_payload(records: pd.DataFrame) -> dict[str, Any]:
    title = "Irradiation depletion threshold ratio vs blocking bias"
    if records is None or records.empty:
        return _empty_payload(title, "No source-record CSV rows are available.")
    df = records.copy()
    if "source" in df.columns:
        df = df[df["source"].eq("irradiation")].copy()
    for col in ("normalized_vds", "se_depletion_ratio_to_seb", "se_depletion_ratio_to_selc", "electrical_terminal_energy_j"):
        df[col] = numeric_column(df, col)
    df = df[df["normalized_vds"].notna() & df["se_depletion_ratio_to_seb"].gt(0.0)].copy()
    if df.empty:
        return _empty_payload(title, "No irradiation rows carry normalized blocking bias and depletion threshold ratios.")
    traces = []
    for event_type, group in df.groupby(df.get("event_type", pd.Series("UNKNOWN", index=df.index)).fillna("UNKNOWN")):
        color = EVENT_TYPE_COLORS.get(str(event_type), EVENT_TYPE_FALLBACK)
        customdata = [[
            display_value(row.device_label),
            display_value(row.filename),
            display_ratio(row.se_depletion_ratio_to_selc),
            display_joules(row.electrical_terminal_energy_j),
        ] for row in group.itertuples(index=False)]
        traces.append({
            "type": "scatter", "mode": "markers", "name": str(event_type),
            "x": group["normalized_vds"].astype(float).tolist(),
            "y": group["se_depletion_ratio_to_seb"].astype(float).tolist(),
            "customdata": customdata,
            "hovertemplate": (
                "Device %{customdata[0]}<br>File %{customdata[1]}<br>"
                "Normalized Vds %{x:.3g}<br>SEB ratio %{y:.3g}<br>"
                "SELC ratio %{customdata[2]}<br>Terminal energy %{customdata[3]}<extra></extra>"
            ),
            "marker": {"color": color, "size": 6, "opacity": 0.72,
                       "line": {"color": "#24292f", "width": 0.25}},
        })
    layout = cartesian_legend_row_layout(title)
    layout.update({
        "xaxis": {"title": {"text": "Normalized blocking voltage |Vds| / rating"}, "gridcolor": "#d8dee4"},
        "yaxis": {"title": {"text": "Stored depletion energy / SEB threshold (log)"}, "type": "log", "gridcolor": "#d8dee4"},
        "shapes": [{"type": "line", "xref": "paper", "yref": "y", "x0": 0, "x1": 1, "y0": 1.0, "y1": 1.0,
                    "line": {"color": "#d73027", "dash": "dash", "width": 1}}],
    })
    return {"traces": traces, "layout": layout,
            "note": "2D replacement for the old irradiation-only 3D energy scene. The dashed line marks the SEB threshold ratio of 1.0; terminal energy remains in hover context."}


def evidence_quality_summary_payload(delta_rows: pd.DataFrame) -> dict[str, Any]:
    title = "v1 signature evidence quality by proxy source"
    if delta_rows is None or delta_rows.empty or "damage_signature_evidence_class" not in delta_rows.columns:
        return _empty_payload(title, "No damage-signature evidence-class columns are exported.")
    df = delta_rows.copy()
    source = df.get("candidate_source", pd.Series("unknown", index=df.index)).fillna("unknown").astype(str)
    evidence = df["damage_signature_evidence_class"].fillna("not recorded").astype(str)
    grouped = pd.crosstab(evidence, source)
    traces = []
    for col in grouped.columns:
        traces.append({"type": "bar", "name": col, "x": grouped.index.tolist(), "y": grouped[col].astype(int).tolist()})
    layout = cartesian_legend_row_layout(title)
    layout.update({
        "barmode": "stack",
        "xaxis": {"title": {"text": "Evidence class"}, "tickangle": -12},
        "yaxis": {"title": {"text": "Candidate pairs"}, "gridcolor": "#d8dee4"},
    })
    return {"traces": traces, "layout": layout,
            "note": "Archive companion for the demoted signature-space views. Distances should be compared within, not across, evidence classes."}


def _concordance_rank1_records(rows: pd.DataFrame) -> list[dict[str, Any]]:
    if rows is None or rows.empty or "target_stress_record_key" not in rows.columns:
        return []
    df = rows.copy()
    for col in ("v1_rank", "v2_rank", "waveform_rank"):
        if col in df.columns:
            df[col] = numeric(df[col])
    records = []
    for target, group in df.groupby("target_stress_record_key"):
        recs = group.to_dict("records")
        exported_v2_ranks = [finite_number(r.get("v2_rank")) for r in recs]
        exported_v2_ranks = [r for r in exported_v2_ranks if r is not None and r > 0]
        exported_top_n = int(max(exported_v2_ranks)) if exported_v2_ranks else len(recs)
        v2_pick = next((r for r in recs if r.get("v2_rank") == 1), None)
        if v2_pick is None:
            continue
        v1_pick = next((r for r in recs if r.get("v1_rank") == 1), None)
        if v2_pick.get("v1_rank") == 1:
            category = "consensus"
        elif v1_pick is not None:
            category = "mild_v1_in_v2_top10"
        else:
            category = "demoted_v1_outside_v2_top10"
        records.append({"target": target, "v2": v2_pick, "v1": v1_pick, "category": category, "exported_top_n": exported_top_n})
    return records


def agreement_matrix_payload(rows: pd.DataFrame) -> dict[str, Any]:
    title = "v1/v2 rank-1 agreement matrix"
    recs = _concordance_rank1_records(rows)
    if not recs:
        return _empty_payload(title, "No concordance rows are available for the agreement matrix.")
    labels = {
        "consensus": "Consensus",
        "mild_v1_in_v2_top10": "v1 pick in v2 top-10",
        "demoted_v1_outside_v2_top10": "v1 pick demoted",
    }
    devices = sorted({str(r["v2"].get("device_type") or "unknown") for r in recs})
    traces = []
    for category, label in labels.items():
        counts = []
        for dev in devices:
            counts.append(sum(1 for r in recs if r["category"] == category and str(r["v2"].get("device_type") or "unknown") == dev))
        traces.append({"type": "bar", "name": label, "x": devices, "y": counts,
                       "marker": {"color": CONCORDANCE_STYLE.get("consensus" if category == "consensus" else ("v2_pick" if category.startswith("mild") else "strong_disagree"), ("#999", "circle"))[0]}})
    layout = cartesian_legend_row_layout(title)
    layout.update({
        "barmode": "stack",
        "xaxis": {"title": {"text": "Device"}, "tickangle": -18},
        "yaxis": {"title": {"text": "Targets"}, "gridcolor": "#d8dee4"},
    })
    counts = {key: sum(1 for r in recs if r["category"] == key) for key in labels}
    note = (
        "Agreement definition: consensus means v2's rank-1 candidate is also v1's rank-1; "
        "mild means v1's rank-1 survives inside v2's exported top-10; demoted means it does not. "
        "Raw rank-1 agreement is chance-level after ranker separation, so use enrichment percentile as the headline independence metric. "
        + ", ".join(f"{labels[k]}={v}" for k, v in counts.items()) + "."
    )
    return {"traces": traces, "layout": layout, "note": note}


def conflict_browser_payload(rows: pd.DataFrame) -> dict[str, Any]:
    title = "Method conflict browser"
    recs = _concordance_rank1_records(rows)
    if not recs:
        return _empty_payload(title, "No concordance rows are available for the conflict browser.")
    filtered = []
    for r in recs:
        v2 = r["v2"]
        if r["category"] != "consensus" or _truthy_flag(v2.get("source_conflict")) or _truthy_flag(v2.get("c2m0080120d_avalanche_vs_sc_conflict")):
            filtered.append(r)
    filtered.sort(key=lambda r: (
        0 if _truthy_flag(r["v2"].get("c2m0080120d_avalanche_vs_sc_conflict")) else 1,
        0 if _truthy_flag(r["v2"].get("source_conflict")) else 1,
        str(r["v2"].get("device_type") or ""),
        str(r["target"]),
    ))
    total_filtered = len(filtered)
    filtered = filtered[:160]
    cols = {
        "Target": [], "Device": [], "Category": [], "v2 source": [], "v1 source": [],
        "v1 rank": [], "v2 rank": [], "Claim": [], "Truth": [], "Blockers": [], "Conflict": [],
    }
    for r in filtered:
        v2 = r["v2"]; v1 = r["v1"] or {}
        cols["Target"].append(_v2_key_tail(str(r["target"]), 34))
        cols["Device"].append(_v2_clean(v2.get("device_type")))
        cols["Category"].append(r["category"])
        cols["v2 source"].append(_v2_clean(v2.get("candidate_source")))
        cols["v1 source"].append(_v2_clean(v1.get("candidate_source")) or _v2_clean(v2.get("v1_signature_pick_source")))
        cols["v1 rank"].append(_v2_clean(v2.get("v1_rank")))
        cols["v2 rank"].append(_v2_clean(v2.get("v2_rank")))
        cols["Claim"].append(_v2_clean(v2.get("proxy_claim_status")))
        cols["Truth"].append(_v2_clean(v2.get("truth_validation_status")))
        cols["Blockers"].append(_v2_clean(v2.get("energy_v2_blockers")) or "(none)")
        cols["Conflict"].append(_v2_clean(v2.get("c2m0080120d_avalanche_vs_sc_conflict")) or _v2_clean(v2.get("source_conflict")))
    note = (
        f"Showing {len(filtered)} of {total_filtered} rows where the v1 signature-best and v2 energy-best methods disagree "
        "or flag a source conflict. C2M0080120D avalanche-vs-SC conflicts sort first, then other source conflicts."
    )
    return _table_payload(title, list(cols.items()), note,
                          height=max(620, 30 * len(filtered) + 150))


def curation_queue_payload(v2_rows: pd.DataFrame, concordance_rows: pd.DataFrame) -> dict[str, Any]:
    title = "Decision-safe curation queue"
    rank1 = _rank1(v2_rows, "mechanistic_energy_candidate_rank")
    if rank1.empty:
        return _empty_payload(title, "No v2 rank-1 rows are exported for the curation queue.")
    status = rank1.get("proxy_claim_status", pd.Series("", index=rank1.index)).fillna("").astype(str)
    truth = rank1.get("truth_validation_status", pd.Series("no_curated_truth", index=rank1.index)).fillna("no_curated_truth").astype(str)
    queue = rank1[status.isin(["validation_candidate", "curation_candidate"]) | truth.eq("no_curated_truth")].copy()
    if queue.empty:
        return _empty_payload(title, "No rank-1 rows currently require curation.", disabled=False)
    conflict_targets: set[str] = set()
    c2m_targets: set[str] = set()
    if concordance_rows is not None and not concordance_rows.empty and "target_stress_record_key" in concordance_rows.columns:
        if "source_conflict" in concordance_rows.columns:
            conflict_targets = set(concordance_rows.loc[
                concordance_rows["source_conflict"].map(_truthy_flag), "target_stress_record_key"
            ].dropna().astype(str))
        if "c2m0080120d_avalanche_vs_sc_conflict" in concordance_rows.columns:
            c2m_targets = set(concordance_rows.loc[
                concordance_rows["c2m0080120d_avalanche_vs_sc_conflict"].map(_truthy_flag), "target_stress_record_key"
            ].dropna().astype(str))
    target_keys = queue.get("target_stress_record_key", pd.Series(index=queue.index)).fillna("").astype(str)
    queue["_c2m_conflict"] = target_keys.isin(c2m_targets)
    queue["_source_conflict"] = target_keys.isin(conflict_targets)
    status_priority = {"validation_candidate": 0, "curation_candidate": 1, "blocked": 2, "screening_only": 3}
    queue["_status_priority"] = queue.get("proxy_claim_status", pd.Series("", index=queue.index)).fillna("").map(status_priority).fillna(4)
    queue = queue.sort_values(
        ["_c2m_conflict", "_source_conflict", "_status_priority", "device_type", "target_stress_record_key"],
        ascending=[False, False, True, True, True],
        na_position="last",
    ).head(200)
    cols = {
        "Target": [_v2_key_tail(_v2_clean(v), 34) for v in queue.get("target_stress_record_key", pd.Series(index=queue.index))],
        "Device": queue.get("device_type", pd.Series(index=queue.index)).fillna("").astype(str).tolist(),
        "Source": queue.get("candidate_source", pd.Series(index=queue.index)).fillna("").astype(str).tolist(),
        "Claim": queue.get("proxy_claim_status", pd.Series(index=queue.index)).fillna("").astype(str).tolist(),
        "Truth": queue.get("truth_validation_status", pd.Series(index=queue.index)).fillna("no_curated_truth").astype(str).tolist(),
        "Terminal overlap": queue.get("terminal_energy_overlap_class", pd.Series(index=queue.index)).fillna("").astype(str).tolist(),
        "Kosier severity": queue.get("critical_severity_overlap_class_kosier_context", pd.Series(index=queue.index)).fillna("").astype(str).tolist(),
        "Boundary usable": queue.get("candidate_failure_fraction_gate_usable", pd.Series(False, index=queue.index)).fillna(False).astype(str).tolist(),
        "Blockers": queue.get("energy_v2_blockers", pd.Series(index=queue.index)).fillna("(none)").astype(str).tolist(),
        "Conflict": [
            "C2M0080120D" if c2m else ("source_conflict" if conflict else "")
            for c2m, conflict in zip(queue["_c2m_conflict"].tolist(), queue["_source_conflict"].tolist())
        ],
    }
    return _table_payload(title, list(cols.items()),
                          f"Top {len(queue)} rank-1 rows needing human truth curation or fail-closed claim review. Truth-label overlay hooks use the same status fields.",
                          height=max(640, 26 * len(queue) + 150))


def reciprocal_enrichment_payload(rows: pd.DataFrame) -> dict[str, Any]:
    title = "Reciprocal method enrichment"
    recs = _concordance_rank1_records(rows)
    if not recs or "v2_pick_dssig_percentile" not in getattr(rows, "columns", []):
        return _empty_payload(title, "No enrichment columns are exported for reciprocal enrichment.")
    x = []; y = []; colors = []; labels = []
    for r in recs:
        v2 = r["v2"]; v1 = r["v1"]
        pct = finite_number(v2.get("v2_pick_dssig_percentile"))
        if pct is None or v1 is None:
            continue
        v1_v2_rank = finite_number(v1.get("v2_rank"))
        exported_top_n = finite_number(r.get("exported_top_n")) or 10.0
        if v1_v2_rank is None:
            continue
        x.append(pct)
        y.append(100.0 * v1_v2_rank / max(exported_top_n, 1.0))
        scope = _v2_clean(v2.get("match_scope")) or "unknown"
        colors.append("#24292f" if scope == "cross_device" else DEEMPHASIS_GRAY)
        labels.append(scope)
    if not x:
        return _empty_payload(title, "No reciprocal enrichment rows can be computed from the exported top-N concordance join.")
    trace = {"type": "scatter", "mode": "markers", "x": x, "y": y,
             "marker": {"color": colors, "size": 7, "opacity": 0.78},
             "customdata": labels,
             "hovertemplate": "v2 pick in v1 ordering %{x:.1f} percentile<br>v1 pick in exported v2 top-N %{y:.1f} percentile<br>%{customdata}<extra></extra>"}
    layout = common_cartesian_layout(title)
    layout.update({
        "xaxis": {"title": {"text": "v2 pick percentile in v1 signature ordering"}, "range": [0, 100]},
        "yaxis": {"title": {"text": "v1 pick percentile in exported v2 ordering"}, "range": [0, 100]},
        "shapes": [{"type": "line", "xref": "x", "yref": "y", "x0": 10, "x1": 10, "y0": 0, "y1": 100,
                    "line": {"color": "#8c959f", "dash": "dash"}},
                   {"type": "line", "xref": "x", "yref": "y", "x0": 0, "x1": 100, "y0": 10, "y1": 10,
                    "line": {"color": "#8c959f", "dash": "dash"}}],
    })
    return {"traces": [trace], "layout": layout,
            "note": f"Mirror enrichment computed from {len(x)} exported concordance top-N targets. X is schema/029's v2-pick percentile in v1 ordering; Y is the v1 pick's location inside the exported v2 ordering, normalized by the exported top-N depth for that target."}


def v3_agreement_payload(v3_rows: pd.DataFrame, concordance_rows: pd.DataFrame) -> dict[str, Any]:
    title = "v3 rank-1 agreement with v1 and v2"
    v3 = _rank1(v3_rows, "combined_rank")
    recs = _concordance_rank1_records(concordance_rows)
    if v3.empty or not recs:
        return _empty_payload(title, "v3 and concordance exports are both required for the v3 agreement bar.")
    by_target = {str(r["target"]): r for r in recs}
    counts = {"matches both": 0, "matches v2 only": 0, "matches v1 only": 0, "matches neither": 0}
    for row in v3.to_dict("records"):
        key = str(row.get("target_stress_record_key"))
        rec = by_target.get(key)
        if rec is None:
            continue
        v3_key = _v2_clean(row.get("candidate_stress_record_key"))
        v2_key = _v2_clean(rec["v2"].get("candidate_stress_record_key"))
        v1_key = _v2_clean((rec["v1"] or {}).get("candidate_stress_record_key"))
        match_v2 = v3_key and v3_key == v2_key
        match_v1 = v3_key and v1_key and v3_key == v1_key
        if match_v1 and match_v2:
            counts["matches both"] += 1
        elif match_v2:
            counts["matches v2 only"] += 1
        elif match_v1:
            counts["matches v1 only"] += 1
        else:
            counts["matches neither"] += 1
    labels = list(counts.keys())
    values = [counts[k] for k in labels]
    layout = common_cartesian_layout(title)
    layout.update({"xaxis": {"title": {"text": "Agreement class"}},
                   "yaxis": {"title": {"text": "v3 rank-1 targets"}, "gridcolor": "#d8dee4"}})
    return {"traces": [{"type": "bar", "x": labels, "y": values,
                         "marker": {"color": ["#1a9850", "#4575b4", "#e6731a", "#8c959f"]}}],
            "layout": layout,
            "note": "v3 is screening-only. This bar asks whether the combined vector reproduces v1, v2, both, or neither before inspecting component shares."}

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
.tab[disabled] {
  cursor: not-allowed;
  opacity: 0.46;
  color: #6e7781;
}
.diagnostics {
  padding: 8px 16px 12px;
  border-top: 1px solid #d8dee4;
  background: #ffffff;
  color: #6e7781;
  font-size: 12px;
  line-height: 1.4;
}
.diagnostics:empty { display: none; }
.subviews { padding-top: 8px; }
.subviews[hidden] { display: none !important; }
.subtab { border-bottom: 1px solid #afb8c1; border-radius: 6px; padding: 6px 10px; }
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
  <p>Post-separation readiness and curation viewer: overview funnel, v2 energy
  readiness, method enrichment, v3 explainability, curation worklist, archived
  signature-space geometry, and irradiation energy context.</p>
</header>
<div class="controls" role="tablist" aria-label="Viewer sections">
  <button id="overview-tab" class="tab active" role="tab" aria-selected="true"
    data-view="overview">Overview</button>
  <button id="v2-tab" class="tab" role="tab" aria-selected="false"
    data-view="v2summary">v2 readiness</button>
  <button id="method-tab" class="tab" role="tab" aria-selected="false"
    data-view="agreement">Method agreement</button>
  <button id="v3-tab" class="tab" role="tab" aria-selected="false"
    data-view="v3">v3 explainability</button>
  <button id="curation-tab" class="tab" role="tab" aria-selected="false"
    data-view="curation">Curation queue</button>
  <button id="signature-tab" class="tab" role="tab" aria-selected="false"
    data-view="source">Signature space</button>
  <button id="energy-tab" class="tab" role="tab" aria-selected="false"
    data-view="energy">Energy context</button>
</div>
<div class="controls subviews" aria-label="Subview controls">
  <button class="tab subtab" data-subview="v2summary">summary</button>
  <button class="tab subtab" data-subview="v2parity">parity</button>
  <button class="tab subtab" data-subview="boundary">boundary coverage</button>
  <button class="tab subtab" data-subview="v2overlap">intervals</button>
  <button class="tab subtab" data-subview="agreement">agreement matrix</button>
  <button class="tab subtab" data-subview="concordanceEcdf">enrichment ECDF</button>
  <button class="tab subtab" data-subview="reciprocal">reciprocal enrichment</button>
  <button class="tab subtab" data-subview="conflictBrowser">conflict browser</button>
  <button class="tab subtab" data-subview="concordance">3D diagnostics</button>
  <button class="tab subtab" data-subview="v3">component shares</button>
  <button class="tab subtab" data-subview="v3agreement">v3 agreement</button>
  <button class="tab subtab" data-subview="source">sources 3D</button>
  <button class="tab subtab" data-subview="delta">delta geometry</button>
  <button class="tab subtab" data-subview="evidenceSummary">evidence summary</button>
  <button class="tab subtab" data-subview="energy">depletion 2D</button>
  <button class="tab subtab" data-subview="energySums">energy by device</button>
</div>
<div class="help">
  Empty or data-starved panels are disabled with an explicit reason. For 3D
  archive/diagnostic scenes, drag to rotate and use the wheel or pinch to zoom.
</div>
<div class="filterbar">
  <label for="device-filter">Device filter:</label>
  <select id="device-filter" aria-label="Filter all views by device">
    <option value="__all__">All devices</option>
  </select>
  <span>applies to views that expose per-device traces; pair views key on the
  irradiation target device.</span>
</div>
<main class="panel">
  <div id="overview-plot" class="plot" role="tabpanel"></div>
  <div id="v2summary-plot" class="plot" role="tabpanel" hidden></div>
  <div id="v2parity-plot" class="plot" role="tabpanel" hidden></div>
  <div id="boundary-plot" class="plot" role="tabpanel" hidden></div>
  <div id="v2overlap-plot" class="plot" role="tabpanel" hidden></div>
  <div id="agreement-plot" class="plot" role="tabpanel" hidden></div>
  <div id="concordanceEcdf-plot" class="plot" role="tabpanel" hidden></div>
  <div id="reciprocal-plot" class="plot" role="tabpanel" hidden></div>
  <div id="conflictBrowser-plot" class="plot" role="tabpanel" hidden></div>
  <div id="concordance-plot" class="plot" role="tabpanel" hidden></div>
  <div id="v3-plot" class="plot" role="tabpanel" hidden></div>
  <div id="v3agreement-plot" class="plot" role="tabpanel" hidden></div>
  <div id="curation-plot" class="plot" role="tabpanel" hidden></div>
  <div id="source-plot" class="plot" role="tabpanel" hidden></div>
  <div id="delta-plot" class="plot" role="tabpanel" hidden></div>
  <div id="evidenceSummary-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energy-plot" class="plot" role="tabpanel" hidden></div>
  <div id="energySums-plot" class="plot" role="tabpanel" hidden></div>
  <div id="plot-note" class="note"></div>
  <div id="plot-diagnostics" class="diagnostics"></div>
</main>
<script id="overview-payload" type="application/json">__OVERVIEW_PAYLOAD__</script>
<script id="v2summary-payload" type="application/json">__V2_SUMMARY_PAYLOAD__</script>
<script id="v2parity-payload" type="application/json">__V2_PARITY_PAYLOAD__</script>
<script id="boundary-payload" type="application/json">__BOUNDARY_PAYLOAD__</script>
<script id="v2overlap-payload" type="application/json">__V2_PAYLOAD__</script>
<script id="agreement-payload" type="application/json">__AGREEMENT_PAYLOAD__</script>
<script id="concordanceEcdf-payload" type="application/json">__CONCORDANCE_ECDF_PAYLOAD__</script>
<script id="reciprocal-payload" type="application/json">__RECIPROCAL_PAYLOAD__</script>
<script id="conflictBrowser-payload" type="application/json">__CONFLICT_BROWSER_PAYLOAD__</script>
<script id="concordance-payload" type="application/json">__CONCORDANCE_PAYLOAD__</script>
<script id="v3-payload" type="application/json">__V3_PAYLOAD__</script>
<script id="v3agreement-payload" type="application/json">__V3_AGREEMENT_PAYLOAD__</script>
<script id="curation-payload" type="application/json">__CURATION_PAYLOAD__</script>
<script id="source-payload" type="application/json">__SOURCE_PAYLOAD__</script>
<script id="delta-payload" type="application/json">__DELTA_PAYLOAD__</script>
<script id="evidenceSummary-payload" type="application/json">__EVIDENCE_SUMMARY_PAYLOAD__</script>
<script id="energy-payload" type="application/json">__ENERGY_PAYLOAD__</script>
<script id="energy-sums-payload" type="application/json">__ENERGY_SUMS_PAYLOAD__</script>
<script id="device-options" type="application/json">__DEVICE_OPTIONS__</script>
<script>
(function () {
  if (!window.Plotly) {
    document.querySelector("main").innerHTML =
      '<div class="error"><b>Interactive runtime failed to load.</b> ' +
      "Regenerate this page with the local Plotly asset available.</div>";
    return;
  }

  const VIEWS = ["overview", "v2summary", "v2parity", "boundary", "v2overlap",
    "agreement", "concordanceEcdf", "reciprocal", "conflictBrowser", "concordance",
    "v3", "v3agreement", "curation", "source", "delta", "evidenceSummary",
    "energy", "energySums"];
  const MAIN_VIEWS = ["overview", "v2summary", "agreement", "v3", "curation",
    "source", "energy"];
  const SUBVIEW_PARENT = {
    v2summary: "v2summary", v2parity: "v2summary", boundary: "v2summary", v2overlap: "v2summary",
    agreement: "agreement", concordanceEcdf: "agreement", reciprocal: "agreement",
    conflictBrowser: "agreement", concordance: "agreement",
    v3: "v3", v3agreement: "v3",
    source: "source", delta: "source", evidenceSummary: "source",
    energy: "energy", energySums: "energy"
  };
  const SECTION_CHILDREN = {};
  VIEWS.forEach(function (name) {
    const parent = SUBVIEW_PARENT[name] || name;
    SECTION_CHILDREN[parent] = SECTION_CHILDREN[parent] || [];
    SECTION_CHILDREN[parent].push(name);
  });
  const payloads = {
    overview: JSON.parse(document.getElementById("overview-payload").textContent),
    v2summary: JSON.parse(document.getElementById("v2summary-payload").textContent),
    v2parity: JSON.parse(document.getElementById("v2parity-payload").textContent),
    boundary: JSON.parse(document.getElementById("boundary-payload").textContent),
    v2overlap: JSON.parse(document.getElementById("v2overlap-payload").textContent),
    agreement: JSON.parse(document.getElementById("agreement-payload").textContent),
    concordanceEcdf: JSON.parse(document.getElementById("concordanceEcdf-payload").textContent),
    reciprocal: JSON.parse(document.getElementById("reciprocal-payload").textContent),
    conflictBrowser: JSON.parse(document.getElementById("conflictBrowser-payload").textContent),
    concordance: JSON.parse(document.getElementById("concordance-payload").textContent),
    v3: JSON.parse(document.getElementById("v3-payload").textContent),
    v3agreement: JSON.parse(document.getElementById("v3agreement-payload").textContent),
    curation: JSON.parse(document.getElementById("curation-payload").textContent),
    source: JSON.parse(document.getElementById("source-payload").textContent),
    delta: JSON.parse(document.getElementById("delta-payload").textContent),
    evidenceSummary: JSON.parse(document.getElementById("evidenceSummary-payload").textContent),
    energy: JSON.parse(document.getElementById("energy-payload").textContent),
    energySums: JSON.parse(document.getElementById("energy-sums-payload").textContent)
  };
  const rendered = {};
  VIEWS.forEach(function (name) { rendered[name] = false; });
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
  let currentView = null;
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

  // Views that can only render one device at a time (categorical axes)
  // declare filter.allShowsOnly; "All devices" then falls back to the first
  // device. That fallback must be visible, not silent: the title and the
  // note both say so, otherwise the dropdown claims "All devices" while the
  // chart shows one device.
  function fallbackDevice(f) {
    if (currentDevice === DEVICE_ALL && f && f.allShowsOnly) {
      return f.allShowsOnly;
    }
    return null;
  }

  function disabledReason(view) {
    const payload = payloads[view];
    if (!payload) {
      return "payload missing";
    }
    if (payload.disabledReason) {
      return payload.disabledReason;
    }
    if (!payload.traces || payload.traces.length === 0) {
      return payload.note || "No comparable rows for this view yet.";
    }
    return "";
  }

  function isDisabled(view) {
    return Boolean(disabledReason(view));
  }

  function firstEnabled(parent) {
    const children = SECTION_CHILDREN[parent] || [parent];
    for (let i = 0; i < children.length; i += 1) {
      if (!isDisabled(children[i])) {
        return children[i];
      }
    }
    return parent === "overview" ? "overview" : firstEnabled("overview");
  }

  function noteFor(view) {
    const payload = payloads[view];
    if (!payload) {
      return "";
    }
    const reason = disabledReason(view);
    if (reason) {
      return reason;
    }
    const f = payload.filter || {};
    const fb = fallbackDevice(f);
    if (fb && f.devices && f.devices.length > 1) {
      return "per-device view: showing " + fb + " (first of " +
        f.devices.length + " devices) because this tab cannot overlay " +
        "devices; pick a device above to switch. " + (payload.note || "");
    }
    return payload.note || "";
  }

  function diagnosticsFor(parent) {
    const children = SECTION_CHILDREN[parent] || [];
    const disabled = children.filter(function (name) { return isDisabled(name); });
    if (disabled.length === 0) {
      return "";
    }
    return "Disabled views: " + disabled.map(function (name) {
      return name + " (" + disabledReason(name) + ")";
    }).join("; ");
  }

  function refreshDisabledButtons() {
    document.querySelectorAll(".tab[data-view]").forEach(function (button) {
      const parent = button.dataset.view;
      const disabled = (SECTION_CHILDREN[parent] || [parent]).every(function (name) {
        return isDisabled(name);
      });
      button.disabled = disabled;
      button.title = disabled ? diagnosticsFor(parent) : "";
    });
    document.querySelectorAll(".subtab").forEach(function (button) {
      const view = button.dataset.subview;
      const reason = disabledReason(view);
      button.disabled = Boolean(reason);
      button.title = reason || "";
    });
  }

  function effectiveDevice(f) {
    let eff = currentDevice;
    if (eff === DEVICE_ALL) {
      eff = f.allShowsOnly || null;
    }
    return eff;
  }

  function selectedAxisRange(f, eff) {
    return (eff && f.ranges && f.ranges[eff]) || f.rangeAll || null;
  }

  function axisRanges(range) {
    if (!range) {
      return null;
    }
    if (Array.isArray(range)) {
      return { x: range.slice(), y: range.slice() };
    }
    return {
      x: range.x ? range.x.slice() : null,
      y: range.y ? range.y.slice() : null
    };
  }

  function applyAxisRanges(update, range) {
    const pair = axisRanges(range);
    if (!pair) {
      return;
    }
    if (pair.x) {
      update["xaxis.range"] = pair.x;
    }
    if (pair.y) {
      update["yaxis.range"] = pair.y;
    }
  }

  function layoutWithCurrentRange(payload) {
    const layout = JSON.parse(JSON.stringify(payload.layout || {}));
    const f = payload.filter || {};
    const pair = axisRanges(selectedAxisRange(f, effectiveDevice(f)));
    if (pair && pair.x) {
      layout.xaxis = layout.xaxis || {};
      layout.xaxis.range = pair.x;
    }
    if (pair && pair.y) {
      layout.yaxis = layout.yaxis || {};
      layout.yaxis.range = pair.y;
    }
    return layout;
  }

  function installFocusedReset(node, view) {
    if (node._apsFocusedResetInstalled) {
      return;
    }
    node._apsFocusedResetInstalled = true;
    node.on("plotly_relayout", function (eventData) {
      if (eventData &&
          (eventData["xaxis.autorange"] || eventData["yaxis.autorange"])) {
        window.setTimeout(function () { applyFilter(view); }, 0);
      }
    });
  }

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
    const eff = effectiveDevice(f);
    const visible = td.map(function (d) {
      return !eff || d === null || d === eff;
    });
    const indices = td.map(function (_unused, i) { return i; });
    Plotly.restyle(node, { visible: visible }, indices);
    const update = {};
    let title = f.titleAll;
    if (eff && f.titles && f.titles[eff]) {
      title = f.titles[eff];
      if (fallbackDevice(f) && f.devices && f.devices.length > 1) {
        title += " — first of " + f.devices.length +
          " devices (per-device view)";
      }
    }
    if (title) {
      update["title.text"] = title;
    }
    if (f.ranges || f.rangeAll) {
      applyAxisRanges(update, selectedAxisRange(f, eff));
    }
    if (Object.keys(update).length > 0) {
      Plotly.relayout(node, update);
    }
  }

  deviceSelect.addEventListener("change", function () {
    currentDevice = deviceSelect.value;
    VIEWS.forEach(function (name) {
      if (rendered[name]) {
        applyFilter(name);
      }
    });
    if (currentView) {
      document.getElementById("plot-note").textContent = noteFor(currentView);
    }
  });

  function render(view) {
    const node = document.getElementById(view + "-plot");
    if (rendered[view]) {
      Plotly.Plots.resize(node);
      return;
    }
    const payload = payloads[view];
    const reason = disabledReason(view);
    if (reason) {
      node.innerHTML =
        '<div class="error" style="border-color:#9a6700;background:#fff8c5;' +
        'color:#7a5c00">' + reason + '</div>';
    } else {
      Plotly.newPlot(node, payload.traces, layoutWithCurrentRange(payload), config);
      installFocusedReset(node, view);
      applyFilter(view);
    }
    rendered[view] = true;
  }

  function viewFromHash() {
    const key = (window.location.hash || "").replace("#", "");
    return VIEWS.indexOf(key) >= 0 ? key : "overview";
  }

  function show(requestedView) {
    const requestedParent = SUBVIEW_PARENT[requestedView] || requestedView;
    let view = requestedView;
    if (isDisabled(view)) {
      view = firstEnabled(requestedParent);
    }
    const parent = SUBVIEW_PARENT[view] || view;
    document.querySelectorAll(".tab[data-view]").forEach(function (button) {
      const active = button.dataset.view === parent;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.querySelectorAll(".subtab").forEach(function (button) {
      const child = button.dataset.subview;
      const relevant = (SECTION_CHILDREN[parent] || []).indexOf(child) >= 0
        && parent !== "overview" && parent !== "curation";
      button.hidden = !relevant;
      button.classList.toggle("active", child === view);
    });
    const subviews = document.querySelector(".subviews");
    subviews.hidden = (SECTION_CHILDREN[parent] || []).length <= 1;
    VIEWS.forEach(function (name) {
      document.getElementById(name + "-plot").hidden = name !== view;
    });
    currentView = view;
    document.getElementById("plot-note").textContent = noteFor(view);
    document.getElementById("plot-diagnostics").textContent = diagnosticsFor(parent);
    render(view);
  }

  refreshDisabledButtons();

  document.querySelectorAll(".tab[data-view]").forEach(function (button) {
    button.addEventListener("click", function () {
      if (button.disabled) {
        return;
      }
      const view = firstEnabled(button.dataset.view);
      if (window.location.hash !== "#" + view) {
        window.location.hash = view;
      } else {
        show(view);
      }
    });
  });
  document.querySelectorAll(".subtab").forEach(function (button) {
    button.addEventListener("click", function () {
      if (button.disabled) {
        return;
      }
      const view = button.dataset.subview;
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
    energy_payload = energy_context_2d_payload(source_records)
    energy_sums_payload = energy_balance_plot_payload(source_records)
    concordance_records = (
        pd.read_csv(CONCORDANCE_CSV) if CONCORDANCE_CSV.exists() else pd.DataFrame()
    )
    v3_records = pd.read_csv(V3_CSV) if V3_CSV.exists() else pd.DataFrame()
    overview_payload_data = overview_payload(
        source_records, delta_comparisons, v2_records, concordance_records, v3_records
    )
    v2_payload = v2_interval_overlap_plot_payload(v2_records)
    v2_parity_payload = v2_severity_parity_plot_payload(v2_records)
    v2_summary_payload = v2_overlap_summary_plot_payload(v2_records)
    boundary_payload = boundary_coverage_payload(v2_records)
    agreement_payload = agreement_matrix_payload(concordance_records)
    concordance_payload = concordance_3d_plot_payload(concordance_records)
    concordance_ecdf_payload = concordance_enrichment_ecdf_payload(concordance_records)
    reciprocal_payload = reciprocal_enrichment_payload(concordance_records)
    conflict_payload = conflict_browser_payload(concordance_records)
    v3_payload = v3_vector_explorer_payload(v3_records)
    v3_agreement = v3_agreement_payload(v3_records, concordance_records)
    curation_payload_data = curation_queue_payload(v2_records, concordance_records)
    evidence_summary_payload = evidence_quality_summary_payload(delta_comparisons)

    # Union of per-view device options, in tab order, deduped, for the one
    # global device filter that drives every tab.
    all_payloads = (
        overview_payload_data,
        v2_summary_payload,
        v2_parity_payload,
        boundary_payload,
        v2_payload,
        agreement_payload,
        concordance_ecdf_payload,
        reciprocal_payload,
        conflict_payload,
        concordance_payload,
        v3_payload,
        v3_agreement,
        curation_payload_data,
        source_payload,
        delta_payload,
        evidence_summary_payload,
        energy_payload,
        energy_sums_payload,
    )
    device_options: list[str] = []
    for payload in all_payloads:
        for dev in payload.get("filter", {}).get("devices", []):
            if dev not in device_options:
                device_options.append(dev)

    html = (
        HTML_TEMPLATE.replace("__PLOTLY_SCRIPT__", plotly_script_tag())
        .replace("__OVERVIEW_PAYLOAD__", json_for_html(overview_payload_data))
        .replace("__V2_SUMMARY_PAYLOAD__", json_for_html(v2_summary_payload))
        .replace("__V2_PARITY_PAYLOAD__", json_for_html(v2_parity_payload))
        .replace("__BOUNDARY_PAYLOAD__", json_for_html(boundary_payload))
        .replace("__V2_PAYLOAD__", json_for_html(v2_payload))
        .replace("__AGREEMENT_PAYLOAD__", json_for_html(agreement_payload))
        .replace("__CONCORDANCE_ECDF_PAYLOAD__", json_for_html(concordance_ecdf_payload))
        .replace("__RECIPROCAL_PAYLOAD__", json_for_html(reciprocal_payload))
        .replace("__CONFLICT_BROWSER_PAYLOAD__", json_for_html(conflict_payload))
        .replace("__CONCORDANCE_PAYLOAD__", json_for_html(concordance_payload))
        .replace("__V3_PAYLOAD__", json_for_html(v3_payload))
        .replace("__V3_AGREEMENT_PAYLOAD__", json_for_html(v3_agreement))
        .replace("__CURATION_PAYLOAD__", json_for_html(curation_payload_data))
        .replace("__SOURCE_PAYLOAD__", json_for_html(source_payload))
        .replace("__DELTA_PAYLOAD__", json_for_html(delta_payload))
        .replace("__EVIDENCE_SUMMARY_PAYLOAD__", json_for_html(evidence_summary_payload))
        .replace("__ENERGY_PAYLOAD__", json_for_html(energy_payload))
        .replace("__ENERGY_SUMS_PAYLOAD__", json_for_html(energy_sums_payload))
        .replace("__DEVICE_OPTIONS__", json_for_html(device_options))
    )
    OUTPUT_HTML.write_text(html)
    size_mb = OUTPUT_HTML.stat().st_size / (1024 * 1024)

    def count_points(payload: dict[str, Any]) -> int:
        total = 0
        for trace in payload["traces"]:
            if trace.get("type") in {"scatter3d", "scatter", "bar"}:
                total += len(trace.get("x", []))
            elif trace.get("type") == "table":
                cells = trace.get("cells", {}).get("values", [])
                total += len(cells[0]) if cells else 0
        return total

    print(f"Wrote {OUTPUT_HTML} ({size_mb:.2f} MiB)")
    print(
        "Views: "
        f"{count_points(overview_payload_data):,} overview; "
        f"{count_points(v2_summary_payload):,} v2 summary; "
        f"{count_points(v2_parity_payload):,} v2 parity; "
        f"{count_points(boundary_payload):,} boundary; "
        f"{count_points(agreement_payload):,} agreement; "
        f"{count_points(concordance_ecdf_payload):,} ECDF; "
        f"{count_points(conflict_payload):,} conflicts; "
        f"{count_points(reciprocal_payload):,} reciprocal; "
        f"{count_points(v3_payload):,} v3 components; "
        f"{count_points(v3_agreement):,} v3 agreement; "
        f"{count_points(curation_payload_data):,} curation rows; "
        f"{count_points(source_payload):,} source; "
        f"{count_points(delta_payload):,} delta; "
        f"{count_points(evidence_summary_payload):,} evidence; "
        f"{count_points(energy_payload):,} energy; "
        f"{count_points(energy_sums_payload):,} energy bars"
    )


if __name__ == "__main__":
    main()
