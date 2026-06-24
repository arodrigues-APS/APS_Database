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
) -> dict[str, Any]:
    return {
        "type": "mesh3d",
        "x": x,
        "y": y,
        "z": z,
        "i": [0, 0],
        "j": [1, 2],
        "k": [2, 3],
        "color": "#777777",
        "opacity": 0.045,
        "hoverinfo": "skip",
        "showlegend": False,
    }


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
        "Normalized Vds: %{customdata[7]}"
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
        "Normalized-Vds delta: %{customdata[13]}"
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
            "irradiation-to-SC. Gate delta is unavailable for both groups; "
            "avalanche normalized-Vds delta is excluded by design."
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
<title>APS interactive 3D damage signature viewer</title>
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
  <h1>APS interactive 3D damage signature viewer</h1>
  <p>Two views of the same stress data: independent source records and ranked pairwise deltas.</p>
</header>
<div class="controls" role="tablist" aria-label="3D plot views">
  <button id="source-tab" class="tab active" role="tab" aria-selected="true"
    data-view="source">Individual source records</button>
  <button id="delta-tab" class="tab" role="tab" aria-selected="false"
    data-view="delta">Delta comparisons</button>
</div>
<div class="help">
  Drag to rotate, use the wheel or pinch to zoom, hover for record metadata,
  and click a legend item to hide or isolate a cohort. The camera icon exports
  the current view; the home icon resets the camera.
</div>
<main class="panel">
  <div id="source-plot" class="plot" role="tabpanel"></div>
  <div id="delta-plot" class="plot" role="tabpanel" hidden></div>
  <div id="plot-note" class="note"></div>
</main>
<script id="source-payload" type="application/json">__SOURCE_PAYLOAD__</script>
<script id="delta-payload" type="application/json">__DELTA_PAYLOAD__</script>
<script>
(function () {
  if (!window.Plotly) {
    document.querySelector("main").innerHTML =
      '<div class="error"><b>Interactive runtime failed to load.</b> ' +
      "Regenerate this page with the local Plotly asset available.</div>";
    return;
  }

  const payloads = {
    source: JSON.parse(document.getElementById("source-payload").textContent),
    delta: JSON.parse(document.getElementById("delta-payload").textContent)
  };
  const rendered = {source: false, delta: false};
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
    if (rendered[view]) {
      Plotly.Plots.resize(document.getElementById(view + "-plot"));
      return;
    }
    const payload = payloads[view];
    Plotly.newPlot(
      view + "-plot",
      payload.traces,
      payload.layout,
      config
    );
    rendered[view] = true;
  }

  function show(view) {
    document.querySelectorAll(".tab").forEach(function (button) {
      const active = button.dataset.view === view;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.getElementById("source-plot").hidden = view !== "source";
    document.getElementById("delta-plot").hidden = view !== "delta";
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
    show(window.location.hash === "#delta" ? "delta" : "source");
  });

  show(window.location.hash === "#delta" ? "delta" : "source");
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

    html = (
        HTML_TEMPLATE.replace("__PLOTLY_SCRIPT__", plotly_script_tag())
        .replace("__SOURCE_PAYLOAD__", json_for_html(source_payload))
        .replace("__DELTA_PAYLOAD__", json_for_html(delta_payload))
    )
    OUTPUT_HTML.write_text(html)
    size_mb = OUTPUT_HTML.stat().st_size / (1024 * 1024)
    print(f"Wrote {OUTPUT_HTML} ({size_mb:.2f} MiB)")
    print(
        "Views: "
        f"{sum(len(trace.get('x', [])) for trace in source_payload['traces'] if trace.get('type') == 'scatter3d'):,} "
        "source points; "
        f"{sum(len(trace.get('x', [])) for trace in delta_payload['traces'] if trace.get('type') == 'scatter3d'):,} "
        "delta points"
    )


if __name__ == "__main__":
    main()
