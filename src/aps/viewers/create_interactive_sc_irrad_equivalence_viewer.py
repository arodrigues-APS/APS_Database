#!/usr/bin/env python3
"""
Build an interactive 3D viewer for measured SC / avalanche / irradiation
damage-equivalence fingerprints.

The source dashboard shows the same evidence in three 2D Superset projections.
This generated HTML keeps the native three-axis damage space together:

  * fingerprint coordinates: ΔVth, ΔRds(on), ΔV(BR)DSS
  * match links: rank-1 strong/usable nearest equivalents
  * match deltas: ranked pairwise absolute axis differences

Outputs:
  out/sc_irrad_equivalence/equivalence_3d/index.html

Prerequisites:
  python3 ml_sc_irrad_equivalence.py --rebuild
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from aps.db_config import get_connection  # noqa: E402


from aps.paths import OUT_ROOT

OUT_DIR = OUT_ROOT / "sc_irrad_equivalence" / "equivalence_3d"
OUTPUT_HTML = OUT_DIR / "index.html"
PLOTLY_ASSET = OUT_DIR / "plotly-2.35.2.min.js"
SHARED_PLOTLY_ASSET = OUT_ROOT / "avalanche_irrad_pilot" / "plotly-2.35.2.min.js"
PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"

FINGERPRINT_SQL = """
SELECT source,
       device_type,
       label,
       device_pair_status,
       sc_voltage_v,
       sc_duration_us,
       avalanche_sample_group,
       ion_species,
       beam_energy_mev,
       let_surface,
       irrad_run_id,
       dvth,
       dvth_iqr,
       dvth_n,
       drds,
       drds_iqr,
       drds_n,
       dbv,
       dbv_iqr,
       dbv_n,
       n_samples
FROM damage_equivalence_view
WHERE dvth IS NOT NULL
   OR drds IS NOT NULL
   OR dbv IS NOT NULL
ORDER BY source, device_type, label;
"""

MATCH_SQL = """
SELECT pair_type,
       left_source,
       right_source,
       device_type,
       right_fingerprint_key,
       right_label,
       left_label,
       left_sc_voltage_v,
       left_sc_duration_us,
       left_avalanche_sample_group,
       right_ion_species,
       right_beam_energy_mev,
       right_let_surface,
       right_irrad_run_id,
       right_avalanche_sample_group,
       match_rank,
       nearest_distance,
       comparable_axes,
       comparable_axis_labels,
       comparability_status,
       sign_mismatch_axis_count,
       sign_mismatch_axes,
       left_dvth,
       left_drds,
       left_dbv,
       left_n_samples,
       right_dvth,
       right_drds,
       right_dbv,
       right_n_samples,
       abs_delta_dvth,
       abs_delta_drds,
       abs_delta_dbv
FROM damage_equivalence_match_view
WHERE match_rank <= 3
ORDER BY pair_type, device_type, right_fingerprint_key, match_rank;
"""

SOURCE_STYLES = {
    "irrad": {
        "name": "Irradiation",
        "color": "#d55e00",
        "symbol": "circle",
        "opacity": 0.72,
    },
    "sc": {
        "name": "Short circuit",
        "color": "#1f77b4",
        "symbol": "square",
        "opacity": 0.86,
    },
    "avalanche": {
        "name": "Avalanche",
        "color": "#2ca02c",
        "symbol": "diamond",
        "opacity": 0.90,
    },
}

PAIR_STYLES = {
    "sc_vs_irradiation": {
        "name": "SC ↔ irradiation",
        "color": "#7b3294",
        "symbol": "circle",
    },
    "sc_vs_avalanche": {
        "name": "SC ↔ avalanche",
        "color": "#008b8b",
        "symbol": "diamond",
    },
    "avalanche_vs_irradiation": {
        "name": "Avalanche ↔ irradiation",
        "color": "#c44536",
        "symbol": "square",
    },
}

STATUS_OPACITY = {
    "strong": 0.92,
    "usable": 0.72,
    "weak": 0.46,
    "inspect manually": 0.34,
}


def load_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    with get_connection() as conn:
        fingerprints = pd.read_sql_query(FINGERPRINT_SQL, conn)
        matches = pd.read_sql_query(MATCH_SQL, conn)
    if fingerprints.empty:
        raise SystemExit("damage_equivalence_view returned no fingerprints")
    if matches.empty:
        raise SystemExit("damage_equivalence_match_view returned no matches")
    return fingerprints, matches


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def display_value(value: Any, digits: int = 5) -> str:
    if value is None or pd.isna(value):
        return "not recorded"
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        value = float(value)
        if not math.isfinite(value):
            return "not recorded"
        if value.is_integer():
            return str(int(value))
        return f"{value:.{digits}g}"
    text = str(value)
    return text if text else "not recorded"


def json_for_html(value: Any) -> str:
    text = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return text.replace("</", "<\\/")


def axis_spec(values: pd.Series, title: str) -> dict[str, Any]:
    finite = numeric(values).replace([np.inf, -np.inf], np.nan).dropna()
    if finite.empty:
        observed_min = 0.0
        observed_max = 1.0
    else:
        observed_min = float(finite.min())
        observed_max = float(finite.max())
    span = observed_max - observed_min
    magnitude = max(abs(observed_min), abs(observed_max), 1.0)
    pad = max(span * 0.10, magnitude * 0.04, 1e-6)
    if span <= 1e-12:
        observed_min -= pad
        observed_max += pad
        span = observed_max - observed_min
    sentinel = observed_min - (0.22 * span) - pad
    upper = observed_max + pad
    ticks = np.linspace(observed_min, observed_max, 5)
    return {
        "title": title,
        "sentinel": sentinel,
        "range": [sentinel - pad * 0.25, upper],
        "physical_range": [observed_min, upper],
        "tickvals": [sentinel, *ticks.tolist()],
        "ticktext": [
            "not recorded",
            *(display_value(value, digits=4) for value in ticks),
        ],
    }


def mesh_plane(
    *,
    x: list[float],
    y: list[float],
    z: list[float],
) -> dict[str, Any]:
    return {
        "type": "mesh3d",
        "x": x,
        "y": y,
        "z": z,
        "i": [0, 0],
        "j": [1, 2],
        "k": [2, 3],
        "color": "#6e7781",
        "opacity": 0.035,
        "hoverinfo": "skip",
        "showlegend": False,
    }


def na_planes(specs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    x = specs["dvth"]
    y = specs["drds"]
    z = specs["dbv"]
    x_low, x_high = x["physical_range"]
    y_low, y_high = y["physical_range"]
    z_low, z_high = z["physical_range"]
    return [
        mesh_plane(
            x=[x["sentinel"]] * 4,
            y=[y_low, y_high, y_high, y_low],
            z=[z_low, z_low, z_high, z_high],
        ),
        mesh_plane(
            x=[x_low, x_high, x_high, x_low],
            y=[y["sentinel"]] * 4,
            z=[z_low, z_low, z_high, z_high],
        ),
        mesh_plane(
            x=[x_low, x_high, x_high, x_low],
            y=[y_low, y_low, y_high, y_high],
            z=[z["sentinel"]] * 4,
        ),
    ]


def condition_label(row: Any, source_attr: str = "source") -> str:
    source = getattr(row, source_attr)
    if source == "sc":
        return (
            f"{display_value(getattr(row, 'sc_voltage_v', None))} V, "
            f"{display_value(getattr(row, 'sc_duration_us', None))} us"
        )
    if source == "irrad":
        return (
            f"{display_value(getattr(row, 'ion_species', None))} @ "
            f"{display_value(getattr(row, 'beam_energy_mev', None))} MeV, "
            f"LET {display_value(getattr(row, 'let_surface', None))}, "
            f"run {display_value(getattr(row, 'irrad_run_id', None))}"
        )
    if source == "avalanche":
        return display_value(getattr(row, "avalanche_sample_group", None))
    return "not recorded"


def match_condition(row: Any, side: str) -> str:
    source = getattr(row, f"{side}_source")
    if source == "sc":
        return (
            f"{display_value(getattr(row, f'{side}_sc_voltage_v', None))} V, "
            f"{display_value(getattr(row, f'{side}_sc_duration_us', None))} us"
        )
    if source == "irrad":
        return (
            f"{display_value(getattr(row, f'{side}_ion_species', None))} @ "
            f"{display_value(getattr(row, f'{side}_beam_energy_mev', None))} MeV, "
            f"LET {display_value(getattr(row, f'{side}_let_surface', None))}, "
            f"run {display_value(getattr(row, f'{side}_irrad_run_id', None))}"
        )
    if source == "avalanche":
        return display_value(getattr(row, f"{side}_avalanche_sample_group", None))
    return "not recorded"


def source_name(source: str) -> str:
    return SOURCE_STYLES.get(source, {}).get("name", source)


def pair_name(pair_type: str) -> str:
    return PAIR_STYLES.get(pair_type, {}).get("name", pair_type)


def common_scene(specs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "dragmode": "orbit",
        "aspectmode": "cube",
        "camera": {"eye": {"x": 1.55, "y": 1.45, "z": 1.05}},
        "xaxis": {
            "title": {"text": specs["dvth"]["title"]},
            "range": specs["dvth"]["range"],
            "tickvals": specs["dvth"]["tickvals"],
            "ticktext": specs["dvth"]["ticktext"],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "yaxis": {
            "title": {"text": specs["drds"]["title"]},
            "range": specs["drds"]["range"],
            "tickvals": specs["drds"]["tickvals"],
            "ticktext": specs["drds"]["ticktext"],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
        "zaxis": {
            "title": {"text": specs["dbv"]["title"]},
            "range": specs["dbv"]["range"],
            "tickvals": specs["dbv"]["tickvals"],
            "ticktext": specs["dbv"]["ticktext"],
            "gridcolor": "#d8dee4",
            "zerolinecolor": "#8c959f",
        },
    }


def common_layout(title: str, specs: dict[str, dict[str, Any]]) -> dict[str, Any]:
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
            "bgcolor": "rgba(255,255,255,0.84)",
            "bordercolor": "#d0d7de",
            "borderwidth": 1,
            "itemsizing": "constant",
        },
        "hoverlabel": {
            "bgcolor": "#ffffff",
            "font": {"color": "#17202a", "size": 12},
            "bordercolor": "#8c959f",
        },
        "scene": common_scene(specs),
        "uirevision": title,
    }


def fingerprint_payload(
    fingerprints: pd.DataFrame,
    matches: pd.DataFrame,
) -> dict[str, Any]:
    data = fingerprints.copy()
    for column in ("dvth", "drds", "dbv", "n_samples"):
        data[column] = numeric(data[column])

    specs = {
        "dvth": axis_spec(data["dvth"], "ΔVth (V)"),
        "drds": axis_spec(data["drds"], "ΔRds(on) (mΩ)"),
        "dbv": axis_spec(data["dbv"], "ΔV(BR)DSS (V)"),
    }
    data["plot_dvth"] = data["dvth"].fillna(specs["dvth"]["sentinel"])
    data["plot_drds"] = data["drds"].fillna(specs["drds"]["sentinel"])
    data["plot_dbv"] = data["dbv"].fillna(specs["dbv"]["sentinel"])
    data["plot_size"] = (
        5.0 + np.sqrt(data["n_samples"].fillna(1.0).clip(lower=1.0)) * 1.35
    ).clip(6.0, 15.0)

    traces: list[dict[str, Any]] = na_planes(specs)
    hover_template = (
        "<b>%{customdata[1]}</b><br>"
        "Source: %{customdata[0]}<br>"
        "Device: %{customdata[2]}<br>"
        "Condition: %{customdata[3]}<br>"
        "Device coverage: %{customdata[4]}<br>"
        "<br>ΔVth: %{customdata[5]}<br>"
        "ΔRds(on): %{customdata[6]}<br>"
        "ΔV(BR)DSS: %{customdata[7]}<br>"
        "Samples: %{customdata[8]}"
        "<extra></extra>"
    )

    for source in ("irrad", "sc", "avalanche"):
        group = data[data["source"].eq(source)]
        if group.empty:
            continue
        style = SOURCE_STYLES[source]
        customdata = [
            [
                style["name"],
                display_value(row.label),
                display_value(row.device_type),
                condition_label(row),
                display_value(row.device_pair_status),
                display_value(row.dvth),
                display_value(row.drds),
                display_value(row.dbv),
                display_value(row.n_samples),
            ]
            for row in group.itertuples(index=False)
        ]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": f"{style['name']} fingerprints (n={len(group):,})",
                "legendgroup": f"source-{source}",
                "x": group["plot_dvth"].astype(float).tolist(),
                "y": group["plot_drds"].astype(float).tolist(),
                "z": group["plot_dbv"].astype(float).tolist(),
                "customdata": customdata,
                "hovertemplate": hover_template,
                "marker": {
                    "color": style["color"],
                    "symbol": style["symbol"],
                    "size": group["plot_size"].astype(float).tolist(),
                    "opacity": style["opacity"],
                    "line": {"color": "#202124", "width": 0.5},
                },
            }
        )

    link_rows = matches[
        matches["match_rank"].eq(1)
        & matches["comparability_status"].isin(["strong", "usable"])
    ].copy()
    for column in (
        "left_dvth",
        "left_drds",
        "left_dbv",
        "right_dvth",
        "right_drds",
        "right_dbv",
    ):
        link_rows[column] = numeric(link_rows[column])

    link_hover = (
        "<b>%{customdata[0]}</b><br>"
        "Device: %{customdata[1]}<br>"
        "Status: %{customdata[2]}<br>"
        "Rank / distance: %{customdata[3]} / %{customdata[4]}<br>"
        "Comparable axes: %{customdata[5]} (%{customdata[6]})<br>"
        "Sign mismatch: %{customdata[7]}<br>"
        "<br>Right: %{customdata[8]} (%{customdata[9]})<br>"
        "Left: %{customdata[10]} (%{customdata[11]})"
        "<extra></extra>"
    )
    for (pair_type, status), group in link_rows.groupby(
        ["pair_type", "comparability_status"],
        sort=True,
    ):
        style = PAIR_STYLES.get(pair_type, {})
        xs: list[float | None] = []
        ys: list[float | None] = []
        zs: list[float | None] = []
        customdata: list[list[str]] = []
        for row in group.itertuples(index=False):
            right = [
                row.right_dvth
                if pd.notna(row.right_dvth)
                else specs["dvth"]["sentinel"],
                row.right_drds
                if pd.notna(row.right_drds)
                else specs["drds"]["sentinel"],
                row.right_dbv
                if pd.notna(row.right_dbv)
                else specs["dbv"]["sentinel"],
            ]
            left = [
                row.left_dvth
                if pd.notna(row.left_dvth)
                else specs["dvth"]["sentinel"],
                row.left_drds
                if pd.notna(row.left_drds)
                else specs["drds"]["sentinel"],
                row.left_dbv
                if pd.notna(row.left_dbv)
                else specs["dbv"]["sentinel"],
            ]
            hover = [
                pair_name(row.pair_type),
                display_value(row.device_type),
                display_value(row.comparability_status),
                display_value(row.match_rank),
                display_value(row.nearest_distance),
                display_value(row.comparable_axes),
                display_value(row.comparable_axis_labels),
                display_value(row.sign_mismatch_axes),
                f"{source_name(row.right_source)} {display_value(row.right_label)}",
                match_condition(row, "right"),
                f"{source_name(row.left_source)} {display_value(row.left_label)}",
                match_condition(row, "left"),
            ]
            xs.extend([float(right[0]), float(left[0]), None])
            ys.extend([float(right[1]), float(left[1]), None])
            zs.extend([float(right[2]), float(left[2]), None])
            customdata.extend([hover, hover, hover])
        traces.append(
            {
                "type": "scatter3d",
                "mode": "lines",
                "name": (
                    f"{style.get('name', pair_type)} {status} links "
                    f"(n={len(group):,})"
                ),
                "legendgroup": f"links-{pair_type}",
                "x": xs,
                "y": ys,
                "z": zs,
                "customdata": customdata,
                "hovertemplate": link_hover,
                "line": {
                    "color": style.get("color", "#6e7781"),
                    "width": 6 if status == "strong" else 4,
                },
                "opacity": STATUS_OPACITY.get(status, 0.6),
            }
        )

    return {
        "traces": traces,
        "layout": common_layout(
            "Measured damage fingerprints with rank-1 usable/strong links",
            specs,
        ),
        "note": (
            f"{len(data):,} measured damage fingerprints are shown in "
            "post-IV damage space. Link segments are rank-1 nearest "
            "equivalents whose dashboard status is strong or usable. "
            "Not-recorded planes are display locations only; they are not "
            "zero-valued measurements."
        ),
    }


def delta_payload(matches: pd.DataFrame) -> dict[str, Any]:
    data = matches.copy()
    for column in (
        "abs_delta_dvth",
        "abs_delta_drds",
        "abs_delta_dbv",
        "match_rank",
        "nearest_distance",
        "comparable_axes",
    ):
        data[column] = numeric(data[column])

    specs = {
        "dvth": axis_spec(data["abs_delta_dvth"], "|ΔVth difference| (V)"),
        "drds": axis_spec(data["abs_delta_drds"], "|ΔRds(on) difference| (mΩ)"),
        "dbv": axis_spec(data["abs_delta_dbv"], "|ΔV(BR)DSS difference| (V)"),
    }
    data["plot_dvth"] = data["abs_delta_dvth"].fillna(
        specs["dvth"]["sentinel"]
    )
    data["plot_drds"] = data["abs_delta_drds"].fillna(
        specs["drds"]["sentinel"]
    )
    data["plot_dbv"] = data["abs_delta_dbv"].fillna(specs["dbv"]["sentinel"])
    data["plot_size"] = (
        12.0 - data["match_rank"].fillna(3.0).clip(lower=1.0, upper=3.0) * 1.7
    ).clip(5.5, 10.5)

    traces: list[dict[str, Any]] = na_planes(specs)
    hover_template = (
        "<b>%{customdata[0]}</b><br>"
        "Device: %{customdata[1]}<br>"
        "Status: %{customdata[2]}<br>"
        "Rank / distance: %{customdata[3]} / %{customdata[4]}<br>"
        "Comparable axes: %{customdata[5]} (%{customdata[6]})<br>"
        "Sign mismatch: %{customdata[7]}<br>"
        "<br>Right: %{customdata[8]} (%{customdata[9]})<br>"
        "Left: %{customdata[10]} (%{customdata[11]})<br>"
        "<br>|ΔVth|: %{customdata[12]}<br>"
        "|ΔRds(on)|: %{customdata[13]}<br>"
        "|ΔV(BR)DSS|: %{customdata[14]}"
        "<extra></extra>"
    )

    for (pair_type, status), group in data.groupby(
        ["pair_type", "comparability_status"],
        sort=True,
    ):
        style = PAIR_STYLES.get(pair_type, {})
        customdata = [
            [
                pair_name(row.pair_type),
                display_value(row.device_type),
                display_value(row.comparability_status),
                display_value(row.match_rank),
                display_value(row.nearest_distance),
                display_value(row.comparable_axes),
                display_value(row.comparable_axis_labels),
                display_value(row.sign_mismatch_axes),
                f"{source_name(row.right_source)} {display_value(row.right_label)}",
                match_condition(row, "right"),
                f"{source_name(row.left_source)} {display_value(row.left_label)}",
                match_condition(row, "left"),
                display_value(row.abs_delta_dvth),
                display_value(row.abs_delta_drds),
                display_value(row.abs_delta_dbv),
            ]
            for row in group.itertuples(index=False)
        ]
        traces.append(
            {
                "type": "scatter3d",
                "mode": "markers",
                "name": (
                    f"{style.get('name', pair_type)} {status} "
                    f"(n={len(group):,})"
                ),
                "legendgroup": f"delta-{pair_type}",
                "x": group["plot_dvth"].astype(float).tolist(),
                "y": group["plot_drds"].astype(float).tolist(),
                "z": group["plot_dbv"].astype(float).tolist(),
                "customdata": customdata,
                "hovertemplate": hover_template,
                "marker": {
                    "color": style.get("color", "#6e7781"),
                    "symbol": style.get("symbol", "circle"),
                    "size": group["plot_size"].astype(float).tolist(),
                    "opacity": STATUS_OPACITY.get(status, 0.42),
                    "line": {"color": "#202124", "width": 0.35},
                },
            }
        )

    return {
        "traces": traces,
        "layout": common_layout(
            "Ranked nearest-equivalent links in absolute-delta space",
            specs,
        ),
        "note": (
            f"{len(data):,} ranked match rows are shown as absolute differences "
            "between the two endpoint fingerprints. Lower values are closer on "
            "that damage axis; the dashboard ranking also includes per-device "
            "axis scaling, sample counts, IQR, comparable-axis count, and sign "
            "mismatch penalties."
        ),
    }


def plotly_script_tag() -> str:
    for path in (PLOTLY_ASSET, SHARED_PLOTLY_ASSET):
        if path.exists():
            runtime = path.read_text()
            return f"<script>{runtime}</script>"
    return (
        f'<script src="{PLOTLY_CDN}"></script>'
        "<!-- Network access is required because no local Plotly asset was found. -->"
    )


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>APS interactive 3D damage-equivalence viewer</title>
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
h1 { margin: 0 0 6px; font-size: 22px; letter-spacing: 0; }
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
  .controls { padding-left: 12px; overflow-x: auto; }
  .panel { margin: 0 6px 6px; }
}
</style>
__PLOTLY_SCRIPT__
</head>
<body>
<header>
  <h1>APS interactive 3D damage-equivalence viewer</h1>
  <p>Measured post-IV damage fingerprints and ranked nearest-equivalent links.</p>
</header>
<div class="controls" role="tablist" aria-label="3D plot views">
  <button id="fingerprints-tab" class="tab active" role="tab"
    aria-selected="true" data-view="fingerprints">Fingerprints + links</button>
  <button id="deltas-tab" class="tab" role="tab" aria-selected="false"
    data-view="deltas">Match deltas</button>
</div>
<div class="help">
  Drag to rotate, use the wheel or pinch to zoom, hover for record metadata,
  and click legend items to hide or isolate cohorts.
</div>
<main class="panel">
  <div id="fingerprints-plot" class="plot" role="tabpanel"></div>
  <div id="deltas-plot" class="plot" role="tabpanel" hidden></div>
  <div id="plot-note" class="note"></div>
</main>
<script id="fingerprints-payload" type="application/json">__FINGERPRINT_PAYLOAD__</script>
<script id="deltas-payload" type="application/json">__DELTA_PAYLOAD__</script>
<script>
(function () {
  if (!window.Plotly) {
    document.querySelector("main").innerHTML =
      '<div class="error"><b>Interactive runtime failed to load.</b> ' +
      "Regenerate this page with the local Plotly asset available.</div>";
    return;
  }

  const payloads = {
    fingerprints: JSON.parse(document.getElementById("fingerprints-payload").textContent),
    deltas: JSON.parse(document.getElementById("deltas-payload").textContent)
  };
  const rendered = {fingerprints: false, deltas: false};
  const config = {
    responsive: true,
    scrollZoom: true,
    displaylogo: false,
    toImageButtonOptions: {
      format: "png",
      filename: "aps_damage_equivalence_3d",
      width: 1800,
      height: 1200,
      scale: 1
    }
  };

  function render(view) {
    if (rendered[view]) {
      Plotly.Plots.resize(document.getElementById(view + "-plot"));
      return;
    }
    const payload = payloads[view];
    Plotly.newPlot(view + "-plot", payload.traces, payload.layout, config);
    rendered[view] = true;
  }

  function show(view) {
    document.querySelectorAll(".tab").forEach(function (button) {
      const active = button.dataset.view === view;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.getElementById("fingerprints-plot").hidden = view !== "fingerprints";
    document.getElementById("deltas-plot").hidden = view !== "deltas";
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
    show(window.location.hash === "#deltas" ? "deltas" : "fingerprints");
  });

  show(window.location.hash === "#deltas" ? "deltas" : "fingerprints");
})();
</script>
</body>
</html>
"""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fingerprints, matches = load_frames()
    fp_payload = fingerprint_payload(fingerprints, matches)
    match_delta_payload = delta_payload(matches)
    html = (
        HTML_TEMPLATE.replace("__PLOTLY_SCRIPT__", plotly_script_tag())
        .replace("__FINGERPRINT_PAYLOAD__", json_for_html(fp_payload))
        .replace("__DELTA_PAYLOAD__", json_for_html(match_delta_payload))
    )
    OUTPUT_HTML.write_text(html)
    size_mb = OUTPUT_HTML.stat().st_size / (1024 * 1024)
    link_count = len(
        matches[
            matches["match_rank"].eq(1)
            & matches["comparability_status"].isin(["strong", "usable"])
        ]
    )
    print(f"Wrote {OUTPUT_HTML} ({size_mb:.2f} MiB)")
    print(
        "Views: "
        f"{len(fingerprints):,} fingerprints; "
        f"{link_count:,} rank-1 usable/strong links; "
        f"{len(matches):,} ranked match deltas"
    )


if __name__ == "__main__":
    main()
