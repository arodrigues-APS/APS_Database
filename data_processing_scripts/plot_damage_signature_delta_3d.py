#!/usr/bin/env python3
"""
Plot irradiation-to-SC and irradiation-to-avalanche damage-signature deltas in 3D.

The source relation is stress_proxy_candidate_view. Each row is a ranked
irradiation-target/proxy-candidate comparison; it is not a standalone sample.
Missing axes are placed on explicitly labelled N/A display planes and are never
written back as zero-valued measurements.

Outputs:
  out/avalanche_irrad_pilot/damage_signature_delta_3d.png
  out/avalanche_irrad_pilot/damage_signature_delta_3d.csv
  out/avalanche_irrad_pilot/damage_signature_delta_3d_coverage.csv
  out/avalanche_irrad_pilot/damage_signature_delta_3d_NOTES.md
"""

from __future__ import annotations

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
SELECT target_stress_record_key,
       target_metadata_id,
       target_event_id,
       target_filename,
       target_device_label,
       target_event_type,
       target_ion_species,
       candidate_stress_record_key,
       candidate_rank,
       match_scope,
       candidate_source,
       candidate_device_type,
       candidate_device_label,
       candidate_metadata_id,
       candidate_event_id,
       candidate_sample_group,
       candidate_filename,
       candidate_stress_condition_label,
       candidate_event_type,
       collapse_delta,
       gate_delta,
       normalized_vds_delta,
       damage_signature_axes_used,
       damage_signature_distance,
       has_collapse_overlap,
       has_gate_overlap,
       has_normalized_vds_overlap,
       damage_signature_available_axes,
       damage_signature_missing_axes,
       damage_signature_axis_mask,
       damage_signature_coverage_score,
       damage_signature_evidence_class,
       damage_signature_evidence_tier,
       coverage_adjusted_damage_signature_distance,
       candidate_status,
       waveform_distance,
       best_damage_distance,
       combined_screening_distance,
       candidate_blockers,
       mechanism_match_class,
       mechanism_rationale,
       target_energy_j,
       target_energy_basis,
       target_energy_is_comparable,
       target_energy_censored_reason,
       target_energy_level,
       target_radiation_deposited_energy_j,
       target_radiation_deposited_energy_total_j,
       target_radiation_dose_gy,
       target_se_depletion_model_quality,
       target_se_depletion_stored_energy_j_cm2,
       target_se_depletion_ratio_to_seb,
       target_se_depletion_ratio_to_selc,
       target_se_depletion_predicted_seb_voltage_v,
       target_se_depletion_predicted_selc_voltage_v,
       target_stress_energy_density_j_cm3,
       target_energy_density_basis,
       target_energy_localization_class,
       candidate_energy_j,
       candidate_energy_basis,
       candidate_energy_is_comparable,
       candidate_energy_level,
       candidate_stress_energy_density_j_cm3,
       candidate_energy_density_basis,
       candidate_energy_localization_class,
       energy_density_ratio,
       log_energy_delta
FROM stress_proxy_candidate_view
WHERE candidate_source IN ('sc', 'avalanche')
ORDER BY candidate_source, target_stress_record_key, candidate_rank;
"""


def load_comparisons() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(DAMAGE_SIGNATURE_SQL, conn)
    finally:
        conn.close()


def summarize_coverage(comparisons: pd.DataFrame) -> pd.DataFrame:
    """Count measured damage signature axes and distinct records by proxy source."""
    rows = []
    for source, group in comparisons.groupby("candidate_source", sort=True):
        complete = group[
            ["collapse_delta", "gate_delta", "normalized_vds_delta"]
        ].notna().all(axis=1)
        rows.append(
            {
                "comparison": f"irradiation_vs_{source}",
                "n_comparisons": len(group),
                "n_irradiation_targets": group[
                    "target_stress_record_key"
                ].nunique(),
                "n_proxy_samples": group["candidate_stress_record_key"].nunique(),
                "n_collapse_delta": group["collapse_delta"].notna().sum(),
                "n_gate_delta": group["gate_delta"].notna().sum(),
                "n_normalized_vds_delta": group[
                    "normalized_vds_delta"
                ].notna().sum(),
                "n_complete_3d": complete.sum(),
                "dominant_evidence_class": (
                    group["damage_signature_evidence_class"].mode().iat[0]
                    if "damage_signature_evidence_class" in group
                    and not group["damage_signature_evidence_class"].dropna().empty
                    else "unknown"
                ),
                "n_collapse_only_signature": (
                    (group.get("damage_signature_evidence_class")
                     == "collapse_only_signature").sum()
                    if "damage_signature_evidence_class" in group
                    else 0
                ),
                "n_collapse_bias_signature": (
                    (group.get("damage_signature_evidence_class")
                     == "collapse_bias_signature").sum()
                    if "damage_signature_evidence_class" in group
                    else 0
                ),
                "n_full_signature": (
                    (group.get("damage_signature_evidence_class")
                     == "full_signature").sum()
                    if "damage_signature_evidence_class" in group
                    else 0
                ),
            }
        )
    return pd.DataFrame(rows)


def plot_damage_signature_delta_3d(
    comparisons: pd.DataFrame,
    out_path: Path,
) -> None:
    """Plot all comparisons with unavailable values on labelled N/A planes."""
    axis_columns = ["collapse_delta", "gate_delta", "normalized_vds_delta"]
    plot_data = comparisons.copy()
    for column in axis_columns:
        plot_data[column] = pd.to_numeric(plot_data[column], errors="coerce")

    # A point needs the collapse coordinate to be locatable. The current
    # materialized view has collapse_delta for every ranked comparison.
    plot_data = plot_data[plot_data["collapse_delta"].notna()].copy()
    if plot_data.empty:
        raise ValueError("No collapse_delta values are available for plotting")

    collapse_values = plot_data["collapse_delta"].dropna()
    gate_values = plot_data["gate_delta"].dropna()
    normalized_vds_values = plot_data["normalized_vds_delta"].dropna()

    collapse_upper = max(1.0, float(collapse_values.max()) * 1.05)
    gate_upper = (
        max(1.0, float(gate_values.max()) * 1.05)
        if not gate_values.empty
        else 1.0
    )
    normalized_vds_upper = (
        max(0.5, float(normalized_vds_values.max()) * 1.08)
        if not normalized_vds_values.empty
        else 1.0
    )

    # The sentinels sit outside the physical non-negative delta range.
    gate_na = -0.08 * gate_upper
    normalized_vds_na = -0.08 * normalized_vds_upper
    plot_data["plot_gate_delta"] = plot_data["gate_delta"].fillna(gate_na)
    plot_data["plot_normalized_vds_delta"] = plot_data[
        "normalized_vds_delta"
    ].fillna(normalized_vds_na)

    fig = plt.figure(figsize=(11, 8))
    ax = fig.add_subplot(111, projection="3d")
    styles = {
        "sc": {
            "color": "#d95f02",
            "marker": "o",
            "label": "Irradiation vs SC",
            "size": 13,
            "alpha": 0.20,
        },
        "avalanche": {
            "color": "#1b9e77",
            "marker": "^",
            "label": "Irradiation vs avalanche",
            "size": 11,
            "alpha": 0.16,
        },
    }
    for source in ("avalanche", "sc"):
        group = plot_data[plot_data["candidate_source"] == source]
        if group.empty:
            continue
        style = styles[source]
        ax.scatter(
            group["collapse_delta"],
            group["plot_gate_delta"],
            group["plot_normalized_vds_delta"],
            s=style["size"],
            alpha=style["alpha"],
            color=style["color"],
            marker=style["marker"],
            label=f"{style['label']} (n={len(group):,})",
            depthshade=False,
            linewidths=0,
        )

    # Translucent planes make the display-only N/A locations visible.
    plane_x, plane_z = np.meshgrid(
        [0.0, collapse_upper],
        [normalized_vds_na, normalized_vds_upper],
    )
    ax.plot_surface(
        plane_x,
        np.full_like(plane_x, gate_na),
        plane_z,
        color="#777777",
        alpha=0.035,
        shade=False,
    )
    plane_x, plane_y = np.meshgrid(
        [0.0, collapse_upper],
        [gate_na, gate_upper],
    )
    ax.plot_surface(
        plane_x,
        plane_y,
        np.full_like(plane_x, normalized_vds_na),
        color="#777777",
        alpha=0.035,
        shade=False,
    )

    ax.set_xlim(0.0, collapse_upper)
    ax.set_ylim(gate_na, gate_upper)
    ax.set_zlim(normalized_vds_na, normalized_vds_upper)
    ax.set_xlabel(
        "collapse_delta\n|candidate - irradiation|",
        labelpad=12,
    )
    ax.set_ylabel(
        "gate_delta\n|candidate - irradiation|",
        labelpad=12,
    )
    ax.set_zlabel(
        "normalized_vds_delta\n|candidate - irradiation|",
        labelpad=12,
    )
    ax.set_yticks([gate_na, 0.0, 0.25, 0.5, 0.75, 1.0])
    ax.set_yticklabels(["N/A", "0", "0.25", "0.5", "0.75", "1.0"])
    z_ticks = np.linspace(0.0, normalized_vds_upper, 6)
    ax.set_zticks([normalized_vds_na, *z_ticks])
    ax.set_zticklabels(
        ["N/A", *(f"{value:.2g}" for value in z_ticks)]
    )
    ax.view_init(elev=24, azim=-57)
    ax.set_title(
        "All ranked proxy comparisons in damage-signature-delta space",
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
        0.02,
        "N/A is a display plane, not zero: proxy gate current is not "
        "captured; avalanche normalized Vds is excluded by design.",
        ha="center",
        fontsize=9,
    )
    fig.subplots_adjust(
        left=0.02,
        right=0.88,
        bottom=0.10,
        top=0.90,
    )
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def markdown_table(frame: pd.DataFrame) -> str:
    display = frame.copy()
    header = "| " + " | ".join(display.columns) + " |"
    separator = "| " + " | ".join("---" for _ in display.columns) + " |"
    rows = [
        "| " + " | ".join(str(row[column]) for column in display.columns) + " |"
        for _, row in display.iterrows()
    ]
    return "\n".join([header, separator, *rows])


def write_notes(coverage: pd.DataFrame, out_path: Path) -> None:
    text = f"""# 3D Damage Signature Delta Plot

Each point in `damage_signature_delta_3d.png` is one ranked comparison from
`stress_proxy_candidate_view`: an irradiation target event paired with either
an SC or avalanche proxy candidate. The axes are absolute candidate-to-target
differences:

- `collapse_delta`: Vds collapse-fraction mismatch.
- `gate_delta`: gate-current-fraction mismatch.
- `normalized_vds_delta`: blocking-voltage mismatch after normalization to
  device voltage rating.

## Axis coverage

{markdown_table(coverage)}

The source waveforms do not currently capture gate current for SC or avalanche,
so `gate_delta` is unavailable for both groups. Avalanche
`normalized_vds_delta` is also NULL by design because its normalized Vds has a
known clamp/scaling artifact. The plot places missing values on explicitly
labelled `N/A` planes outside the physical non-negative delta range. These
display coordinates are not zero-valued imputations.

There are therefore no fully measured three-axis comparisons in the current
materialized data. The plot still compares both proxy types on collapse delta
and compares irradiation-to-SC pairs on normalized-Vds delta. The exact source
rows, with NULLs preserved, are in `damage_signature_delta_3d.csv`.

## Evidence coverage

Each comparison now carries a `damage_signature_evidence_class` describing which
axes actually overlapped. Distances are only comparable *within* a class:

- `collapse_only_signature` (current avalanche cohort) rests on one axis.
- `collapse_bias_signature` (current SC cohort) rests on collapse + normalized
  Vds.

A lower-dimensional comparison can show a deceptively small distance, so the
`damage_signature_distance` is a screening distance within its evidence class,
not a cross-class equivalence score. The experimental
`coverage_adjusted_damage_signature_distance` adds an uncalibrated missing-axis
penalty for triage only; it is never used to rank candidates.
"""
    out_path.write_text(text)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    comparisons = load_comparisons()
    if comparisons.empty:
        raise SystemExit("No irradiation-to-proxy damage signature comparisons found")

    coverage = summarize_coverage(comparisons)
    comparisons.to_csv(OUT_DIR / "damage_signature_delta_3d.csv", index=False)
    coverage.to_csv(
        OUT_DIR / "damage_signature_delta_3d_coverage.csv",
        index=False,
    )
    plot_damage_signature_delta_3d(
        comparisons,
        OUT_DIR / "damage_signature_delta_3d.png",
    )
    write_notes(
        coverage,
        OUT_DIR / "damage_signature_delta_3d_NOTES.md",
    )

    print(f"Wrote damage-signature-delta outputs to {OUT_DIR}")
    print(coverage.to_string(index=False))


if __name__ == "__main__":
    main()
