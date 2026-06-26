#!/usr/bin/env python3
"""
Plot individual irradiation, short-circuit, and avalanche stress records in 3D.

Unlike plot_damage_signature_delta_3d.py, this script does not plot candidate/target
differences. Each point is one record from stress_test_context_view, positioned
using that record's own Vds collapse fraction, gate-current fraction, and
normalized blocking voltage.

Outputs:
  out/avalanche_irrad_pilot/damage_signature_sources_3d.png
  out/avalanche_irrad_pilot/damage_signature_sources_3d.csv
  out/avalanche_irrad_pilot/damage_signature_sources_3d_coverage.csv
  out/avalanche_irrad_pilot/damage_signature_sources_3d_NOTES.md
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("matplotlib is required for the damage signature plot") from exc

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db_config import get_connection  # noqa: E402


OUT_DIR = Path("out/avalanche_irrad_pilot")

DAMAGE_SIGNATURE_SQL = """
SELECT source,
       stress_record_key,
       metadata_id,
       event_id,
       event_index,
       device_type,
       device_label,
       voltage_class,
       technology_class,
       filename,
       stress_condition_label,
       event_type,
       path_type,
       vds_collapse_fraction,
       gate_delta_fraction,
       normalized_vds,
       normalized_current,
       context_flags,
       event_record_type,
       electrical_terminal_energy_j,
       electrical_terminal_energy_basis,
       stress_energy_j,
       stress_energy_basis,
       average_terminal_power_w,
       stress_duration_s,
       energy_is_comparable,
       energy_window_basis,
       energy_censored_reason,
       energy_level,
       radiation_deposited_energy_j,
       radiation_deposited_energy_total_j,
       radiation_dose_gy,
       radiation_dose_total_gy,
       radiation_energy_basis,
       radiation_fluence_basis,
       se_depletion_model_quality,
       se_depletion_voltage_v,
       se_depletion_stored_energy_j_cm2,
       se_depletion_ratio_to_seb,
       se_depletion_ratio_to_selc,
       se_depletion_predicted_seb_voltage_v,
       se_depletion_predicted_selc_voltage_v,
       se_depletion_width_um,
       se_depletion_peak_field_v_cm,
       se_depletion_net_doping_cm3,
       energy_localization_class
FROM stress_test_context_view
WHERE source IN ('irradiation', 'sc', 'avalanche')
ORDER BY source, stress_record_key;
"""


def load_records() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(DAMAGE_SIGNATURE_SQL, conn)
    finally:
        conn.close()


def summarize_coverage(records: pd.DataFrame) -> pd.DataFrame:
    """Count records and available physical damage signature coordinates."""
    rows = []
    axes = [
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "normalized_vds",
    ]
    for source, group in records.groupby("source", sort=True):
        rows.append(
            {
                "source": source,
                "n_records": len(group),
                "n_files": group["metadata_id"].nunique(),
                "n_collapse_fraction": group[
                    "vds_collapse_fraction"
                ].notna().sum(),
                "n_gate_current_fraction": group[
                    "gate_delta_fraction"
                ].notna().sum(),
                "n_normalized_vds": group["normalized_vds"].notna().sum(),
                "n_complete_3d": group[axes].notna().all(axis=1).sum(),
            }
        )
    return pd.DataFrame(rows)


def choose_log_ticks(minimum: float, maximum: float) -> list[float]:
    candidates = [
        0.01,
        0.03,
        0.1,
        0.3,
        1.0,
        3.0,
        10.0,
        20.0,
        30.0,
    ]
    lower = minimum * 0.75
    upper = maximum * 1.25
    return [value for value in candidates if lower <= value <= upper]


def format_tick(value: float) -> str:
    if value >= 1.0:
        return f"{value:g}"
    return f"{value:.2g}"


def plot_source_damage_signatures(records: pd.DataFrame, out_path: Path) -> None:
    """
    Plot each source record; missing coordinates use labelled N/A planes.

    The normalized-Vds display coordinate is log10-transformed because the
    stored values span more than three orders of magnitude. Tick labels remain
    in the original normalized-Vds units.
    """
    data = records.copy()
    axes = [
        "vds_collapse_fraction",
        "gate_delta_fraction",
        "normalized_vds",
    ]
    for column in axes:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data[data["vds_collapse_fraction"].notna()].copy()
    if data.empty:
        raise ValueError("No individual collapse fractions are available")

    positive_normalized_vds = data.loc[
        data["normalized_vds"].gt(0.0),
        "normalized_vds",
    ]
    if positive_normalized_vds.empty:
        raise ValueError("No positive normalized-Vds values are available")

    collapse_max = float(data["vds_collapse_fraction"].max())
    gate_max = float(data["gate_delta_fraction"].max())
    normalized_vds_min = float(positive_normalized_vds.min())
    normalized_vds_max = float(positive_normalized_vds.max())

    gate_na = -0.12
    normalized_vds_na = math.log10(normalized_vds_min) - 0.35
    data["plot_gate_fraction"] = data["gate_delta_fraction"].fillna(gate_na)
    data["plot_normalized_vds"] = np.where(
        data["normalized_vds"].gt(0.0),
        np.log10(data["normalized_vds"]),
        normalized_vds_na,
    )

    fig = plt.figure(figsize=(11.5, 8.5))
    ax = fig.add_subplot(111, projection="3d")
    styles = {
        "irradiation": {
            "color": "#377eb8",
            "marker": "o",
            "label": "Irradiation",
            "size": 14,
            "alpha": 0.24,
            "edgecolor": "none",
            "linewidth": 0.0,
        },
        "avalanche": {
            "color": "#1b9e77",
            "marker": "^",
            "label": "Avalanche",
            "size": 18,
            "alpha": 0.26,
            "edgecolor": "none",
            "linewidth": 0.0,
        },
        "sc": {
            "color": "#e66101",
            "marker": "s",
            "label": "Short circuit",
            "size": 48,
            "alpha": 0.85,
            "edgecolor": "black",
            "linewidth": 0.35,
        },
    }

    # Draw the large cohorts first so the smaller SC cohort remains visible.
    for source in ("irradiation", "avalanche", "sc"):
        group = data[data["source"] == source]
        if group.empty:
            continue
        style = styles[source]
        ax.scatter(
            group["vds_collapse_fraction"],
            group["plot_gate_fraction"],
            group["plot_normalized_vds"],
            s=style["size"],
            alpha=style["alpha"],
            color=style["color"],
            marker=style["marker"],
            edgecolors=style["edgecolor"],
            linewidths=style["linewidth"],
            label=f"{style['label']} (n={len(group):,})",
            depthshade=False,
        )

    collapse_upper = max(1.0, collapse_max * 1.03)
    gate_upper = max(0.85, gate_max * 1.08)
    normalized_vds_upper = math.log10(normalized_vds_max) + 0.05

    # Gate-current N/A plane.
    plane_x, plane_z = np.meshgrid(
        [0.0, collapse_upper],
        [normalized_vds_na, normalized_vds_upper],
    )
    ax.plot_surface(
        plane_x,
        np.full_like(plane_x, gate_na),
        plane_z,
        color="#777777",
        alpha=0.04,
        shade=False,
    )

    # Normalized-Vds N/A plane.
    plane_x, plane_y = np.meshgrid(
        [0.0, collapse_upper],
        [gate_na, gate_upper],
    )
    ax.plot_surface(
        plane_x,
        plane_y,
        np.full_like(plane_x, normalized_vds_na),
        color="#777777",
        alpha=0.04,
        shade=False,
    )

    ax.set_xlim(0.0, collapse_upper)
    ax.set_ylim(gate_na - 0.02, gate_upper)
    ax.set_zlim(normalized_vds_na, normalized_vds_upper)
    ax.set_xlabel(
        "Vds collapse fraction\n0 = none, 1 = full collapse",
        labelpad=13,
    )
    ax.set_ylabel(
        "Gate-current fraction\nIg / (Ig + Id)",
        labelpad=13,
    )
    ax.set_zlabel(
        "Normalized blocking voltage\n|Vds| / device rating (log display)",
        labelpad=14,
    )

    gate_ticks = [gate_na, 0.0, 0.2, 0.4, 0.6, 0.8]
    ax.set_yticks(gate_ticks)
    ax.set_yticklabels(
        ["not recorded", "0", "0.2", "0.4", "0.6", "0.8"]
    )

    normalized_ticks = choose_log_ticks(
        normalized_vds_min,
        normalized_vds_max,
    )
    ax.set_zticks(
        [normalized_vds_na, *(math.log10(value) for value in normalized_ticks)]
    )
    ax.set_zticklabels(
        ["not recorded", *(format_tick(value) for value in normalized_ticks)]
    )

    ax.view_init(elev=24, azim=-58)
    ax.set_box_aspect((1.30, 1.0, 1.0))
    ax.set_title(
        "Individual irradiation, short-circuit, and avalanche records\n"
        "in damage signature space",
        pad=20,
    )
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(0.0, 0.94),
        fontsize=9,
    )
    ax.grid(True)
    fig.text(
        0.5,
        0.025,
        "Each marker is one stress record, not a pairwise comparison. "
        "Gate current was not recorded for SC or avalanche.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(
        left=0.01,
        right=0.88,
        bottom=0.10,
        top=0.90,
    )
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def markdown_table(frame: pd.DataFrame) -> str:
    header = "| " + " | ".join(frame.columns) + " |"
    separator = "| " + " | ".join("---" for _ in frame.columns) + " |"
    rows = [
        "| " + " | ".join(str(row[column]) for column in frame.columns) + " |"
        for _, row in frame.iterrows()
    ]
    return "\n".join([header, separator, *rows])


def write_notes(coverage: pd.DataFrame, records: pd.DataFrame, out_path: Path) -> None:
    normalized_vds = pd.to_numeric(
        records["normalized_vds"],
        errors="coerce",
    )
    avalanche_normalized = normalized_vds[records["source"].eq("avalanche")]
    text = f"""# Individual-source 3D Damage Signature Plot

This is the non-delta version of the damage signature plot. Every marker in
`damage_signature_sources_3d.png` is one record from
`stress_test_context_view`, colored by its own source: irradiation, short
circuit, or avalanche.

## Axes

- x is the record's own Vds collapse fraction. Zero means no collapse; one
  means a collapse equal to the pre-event blocking voltage.
- y is the record's own gate-current participation fraction,
  `Ig / (Ig + Id)`.
- z is the record's own blocking voltage divided by rated device voltage. The
  display uses a logarithmic scale because the stored values span more than
  three orders of magnitude.

## Coverage

{markdown_table(coverage)}

SC and avalanche waveforms do not contain a gate-current channel, so those
records are placed on the y-axis plane labelled `not recorded`. This is an
unknown value, not zero gate participation. Records without normalized Vds are
similarly placed on the z-axis `not recorded` plane.

The stored avalanche normalized-Vds values range from
{avalanche_normalized.min():.4g} to {avalanche_normalized.max():.4g}, including
values far above one. The database already flags this as a known avalanche
clamp/scaling artifact. Those raw values are plotted because this figure shows
individual stored damage signatures, but their z-position should not be interpreted
as directly physically comparable to irradiation or SC until that scaling is
corrected.

The exact plotted records, with missing values preserved, are exported in
`damage_signature_sources_3d.csv`.
"""
    out_path.write_text(text)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    records = load_records()
    if records.empty:
        raise SystemExit("No individual stress damage signature records found")

    coverage = summarize_coverage(records)
    records.to_csv(
        OUT_DIR / "damage_signature_sources_3d.csv",
        index=False,
    )
    coverage.to_csv(
        OUT_DIR / "damage_signature_sources_3d_coverage.csv",
        index=False,
    )
    plot_source_damage_signatures(
        records,
        OUT_DIR / "damage_signature_sources_3d.png",
    )
    write_notes(
        coverage,
        records,
        OUT_DIR / "damage_signature_sources_3d_NOTES.md",
    )

    print(f"Wrote individual-source damage signature outputs to {OUT_DIR}")
    print(coverage.to_string(index=False))


if __name__ == "__main__":
    main()
