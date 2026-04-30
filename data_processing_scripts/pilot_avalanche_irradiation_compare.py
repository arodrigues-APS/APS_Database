#!/usr/bin/env python3
"""
Pilot avalanche vs irradiation comparison for the overlapping C2M0080120D data.

This is deliberately a case-study script, not a production equivalence model.
It compares:
  * C2M0080120D / D3 UIS avalanche waveforms
  * C2M0080120D proton SELCII irradiation events
  * C2M0080120D heavy-ion SEB irradiation events as a hard-failure contrast

Outputs land in out/avalanche_irrad_pilot/.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("matplotlib is required for this pilot") from exc

from db_config import get_connection


OUT_DIR = Path("out/avalanche_irrad_pilot")


@dataclass(frozen=True)
class WindowConfig:
    edge_fraction: float = 0.05
    min_edge_points: int = 25


def finite_series(values: pd.Series) -> np.ndarray:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    return arr[np.isfinite(arr)]


def edge_median(arr: np.ndarray, *, start: bool, cfg: WindowConfig) -> float:
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return math.nan
    n_edge = max(cfg.min_edge_points, int(arr.size * cfg.edge_fraction))
    n_edge = min(n_edge, arr.size)
    chunk = arr[:n_edge] if start else arr[-n_edge:]
    return float(np.nanmedian(chunk))


def safe_ratio(num: float, den: float) -> float:
    if den is None or not np.isfinite(den) or abs(den) <= 0:
        return math.nan
    return float(num / den)


def load_sql_frame(conn, sql: str, params: tuple | None = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def avalanche_waveform_features(waveforms: pd.DataFrame) -> pd.DataFrame:
    cfg = WindowConfig()
    rows = []
    for metadata_id, group in waveforms.groupby("metadata_id", sort=True):
        g = group.sort_values(["time_us", "point_index"])
        time_s = finite_series(g["time_val"])
        time_us = finite_series(g["time_us"])
        id_abs = np.abs(finite_series(g["id_drain"]))
        ig_abs = np.abs(finite_series(g["igs"])) if "igs" in g else np.array([])
        vds = finite_series(g["vds"])

        if id_abs.size == 0 or vds.size == 0 or time_s.size == 0:
            continue

        first = g.iloc[0]
        id_initial = edge_median(id_abs, start=True, cfg=cfg)
        id_final = edge_median(id_abs, start=False, cfg=cfg)
        peak_idx = int(np.nanargmax(id_abs))
        vds_peak_idx = int(np.nanargmax(vds))
        vds_after_peak = vds[vds_peak_idx:]
        min_vds_after_peak = float(np.nanmin(vds_after_peak)) if vds_after_peak.size else math.nan
        vds_initial = edge_median(vds, start=True, cfg=cfg)
        vds_final = edge_median(vds, start=False, cfg=cfg)
        max_vds = float(np.nanmax(vds))
        peak_id = float(np.nanmax(id_abs))
        id_rise_from_initial = peak_id - id_initial
        vds_drop_peak_to_final = max_vds - vds_final
        vds_drop_peak_to_min_after = max_vds - min_vds_after_peak
        vds_collapse_fraction = safe_ratio(vds_drop_peak_to_min_after, max(abs(max_vds), 1.0))

        gate_fraction = math.nan
        if ig_abs.size:
            ig_initial = edge_median(ig_abs, start=True, cfg=cfg)
            peak_ig = float(np.nanmax(ig_abs))
            delta_ig = max(0.0, peak_ig - ig_initial)
            gate_fraction = safe_ratio(delta_ig, delta_ig + max(id_rise_from_initial, 0.0))

        energy_j = math.nan
        if time_s.size == id_abs.size == vds.size and time_s.size > 1:
            order = np.argsort(time_s)
            energy_j = float(np.trapz(np.maximum(vds[order], 0.0) * id_abs[order], time_s[order]))

        rows.append(
            {
                "cohort": "avalanche_d3_uis_waveform",
                "source": "avalanche",
                "metadata_id": int(metadata_id),
                "event_id": math.nan,
                "device_type": first["device_type"],
                "sample_group": first["sample_group"],
                "device_id": first["device_id"],
                "label": first["avalanche_condition_label"],
                "ion_species": None,
                "event_type": "UIS_waveform_descriptor",
                "path_type": "drain_source_power_pulse",
                "is_catastrophic": None,
                "peak_current_a": peak_id,
                "delta_id_abs_a": peak_id,
                "id_rise_from_initial_a": id_rise_from_initial,
                "id_before_a": id_initial,
                "id_after_a": id_final,
                "vds_before_v": vds_initial,
                "vds_after_v": vds_final,
                "vds_delta_v": vds_final - max_vds,
                "vds_collapse_fraction": vds_collapse_fraction,
                "gate_delta_fraction": gate_fraction,
                "duration_s": float(np.nanmax(time_s) - np.nanmin(time_s)),
                "time_to_peak_id_s": float(time_s[peak_idx] - np.nanmin(time_s)),
                "energy_like_j": energy_j,
                "n_points": int(len(g)),
                "filename": first["filename"],
            }
        )
    return pd.DataFrame(rows)


def irradiation_event_features(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in events.iterrows():
        before_v = row["vds_before_v"]
        after_v = row["vds_after_v"]
        vds_delta = row["vds_delta_v"]
        collapse_fraction = math.nan
        if pd.notna(before_v) and abs(float(before_v)) > 0:
            collapse_fraction = max(0.0, -float(vds_delta)) / abs(float(before_v))

        ion = str(row["ion_species"]).lower() if pd.notna(row["ion_species"]) else "unknown"
        if ion == "proton":
            cohort = "irradiation_proton_selcii"
        elif row["event_type"] == "SEB":
            cohort = "irradiation_heavy_ion_seb"
        else:
            cohort = "irradiation_other_c2m_event"

        rows.append(
            {
                "cohort": cohort,
                "source": "irradiation",
                "metadata_id": int(row["metadata_id"]),
                "event_id": int(row["event_id"]),
                "device_type": row["device_type"],
                "sample_group": None,
                "device_id": row["device_id"],
                "label": f"{row['ion_species']} {row['event_type']} event {row['event_index']}",
                "ion_species": row["ion_species"],
                "event_type": row["event_type"],
                "path_type": row["path_type"],
                "is_catastrophic": bool(row["is_catastrophic"]),
                "peak_current_a": row["id_after_a"],
                "delta_id_abs_a": row["delta_id_abs_a"],
                "id_rise_from_initial_a": row["delta_id_abs_a"],
                "id_before_a": row["id_before_a"],
                "id_after_a": row["id_after_a"],
                "vds_before_v": before_v,
                "vds_after_v": after_v,
                "vds_delta_v": vds_delta,
                "vds_collapse_fraction": collapse_fraction,
                "gate_delta_fraction": row["gate_delta_fraction"],
                "duration_s": row["time_end"] - row["time_start"]
                if pd.notna(row["time_end"]) and pd.notna(row["time_start"])
                else math.nan,
                "time_to_peak_id_s": row["time_peak"] - row["time_start"]
                if pd.notna(row["time_peak"]) and pd.notna(row["time_start"])
                else math.nan,
                "energy_like_j": math.nan,
                "n_points": row["cluster_width_points"],
                "filename": row["filename"],
            }
        )
    return pd.DataFrame(rows)


def cohort_summary(features: pd.DataFrame) -> pd.DataFrame:
    aggregations = {
        "metadata_id": pd.Series.nunique,
        "event_id": "count",
        "delta_id_abs_a": ["count", "median", "min", "max"],
        "vds_collapse_fraction": ["count", "median", "min", "max"],
        "gate_delta_fraction": ["count", "median", "min", "max"],
        "duration_s": ["median", "min", "max"],
        "energy_like_j": ["count", "median", "min", "max"],
    }
    summary = features.groupby("cohort").agg(aggregations)
    summary.columns = [
        "_".join(str(part) for part in col if part) for col in summary.columns.to_flat_index()
    ]
    return summary.reset_index().rename(
        columns={
            "metadata_id_nunique": "n_files",
            "event_id_count": "n_events",
            "delta_id_abs_a_count": "n_delta_id",
            "vds_collapse_fraction_count": "n_vds_collapse",
            "gate_delta_fraction_count": "n_gate_fraction",
            "energy_like_j_count": "n_energy_like",
        }
    )


def damage_case_study(conn) -> pd.DataFrame:
    sql = """
    WITH vals AS (
      SELECT test_condition,
             MAX((gate_params->>'vth_v')::double precision)
               FILTER (WHERE measurement_category='Vth') AS vth_from_vth,
             MAX((gate_params->>'vth_v')::double precision)
               FILTER (WHERE measurement_category='IdVg') AS vth_from_idvg,
             MAX((gate_params->>'rdson_mohm')::double precision)
               FILTER (WHERE measurement_category='IdVd') AS rds_mohm
      FROM baselines_metadata
      WHERE data_source='curve_tracer_avalanche_iv'
        AND device_type='C2M0080120D'
        AND lower(sample_group)='d3'
      GROUP BY test_condition
    ), pre AS (
      SELECT * FROM vals WHERE test_condition='pre_avalanche'
    ), post AS (
      SELECT * FROM vals WHERE test_condition='post_avalanche'
    )
    SELECT 'C2M0080120D' AS device_type,
           'D3/d3/IV/3 probable sample mapping' AS sample_mapping,
           pre.vth_from_vth AS pre_vth_from_vth,
           post.vth_from_vth AS post_vth_from_vth,
           post.vth_from_vth - pre.vth_from_vth AS delta_vth_from_vth,
           pre.vth_from_idvg AS pre_vth_from_idvg,
           post.vth_from_idvg AS post_vth_from_idvg,
           post.vth_from_idvg - pre.vth_from_idvg AS delta_vth_from_idvg,
           pre.rds_mohm AS pre_rds_mohm,
           post.rds_mohm AS post_rds_mohm,
           post.rds_mohm - pre.rds_mohm AS delta_rds_mohm
    FROM pre, post;
    """
    return load_sql_frame(conn, sql)


def plot_event_space(features: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    colors = {
        "avalanche_d3_uis_waveform": "#1f77b4",
        "irradiation_proton_selcii": "#d62728",
        "irradiation_heavy_ion_seb": "#2ca02c",
        "irradiation_other_c2m_event": "#7f7f7f",
    }
    markers = {
        "avalanche_d3_uis_waveform": "s",
        "irradiation_proton_selcii": "o",
        "irradiation_heavy_ion_seb": "^",
        "irradiation_other_c2m_event": ".",
    }
    for cohort, group in features.groupby("cohort"):
        x = pd.to_numeric(group["delta_id_abs_a"], errors="coerce")
        y = pd.to_numeric(group["vds_collapse_fraction"], errors="coerce")
        valid = x.gt(0) & y.notna()
        if not valid.any():
            continue
        ax.scatter(
            x[valid],
            y[valid],
            s=70 if "avalanche" in cohort or "seb" in cohort else 45,
            alpha=0.75,
            label=cohort,
            color=colors.get(cohort, "#7f7f7f"),
            marker=markers.get(cohort, "o"),
            edgecolor="black" if "avalanche" in cohort or "seb" in cohort else "none",
            linewidth=0.5,
        )
    ax.set_xscale("log")
    ax.set_xlabel("delta |Id| or pulse amplitude (A, log scale)")
    ax.set_ylabel("Vds collapse fraction")
    ax.set_title("C2M0080120D pilot: event/pulse phenotype space")
    ax.axhline(0.5, color="black", linestyle="--", linewidth=1, alpha=0.5)
    ax.grid(True, which="both", linewidth=0.4, alpha=0.35)
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_avalanche_waveforms(waveforms: pd.DataFrame, out_path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(9, 7), sharex=True)
    for metadata_id, group in waveforms.groupby("metadata_id", sort=True):
        g = group.sort_values(["time_us", "point_index"])
        label = str(g.iloc[0]["avalanche_condition_label"])
        axes[0].plot(g["time_us"], g["id_drain"].abs(), label=label, linewidth=1.2)
        axes[1].plot(g["time_us"], g["vds"], label=label, linewidth=1.2)
    axes[0].set_ylabel("|Id| (A)")
    axes[1].set_ylabel("Vds (V)")
    axes[1].set_xlabel("Time (us)")
    axes[0].set_title("C2M0080120D D3 UIS avalanche waveforms")
    for ax in axes:
        ax.grid(True, linewidth=0.4, alpha=0.35)
        ax.legend(loc="best", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def write_readme(summary: pd.DataFrame, damage: pd.DataFrame, out_path: Path) -> None:
    proton = summary[summary["cohort"] == "irradiation_proton_selcii"]
    avalanche = summary[summary["cohort"] == "avalanche_d3_uis_waveform"]
    seb = summary[summary["cohort"] == "irradiation_heavy_ion_seb"]

    def value(df: pd.DataFrame, col: str) -> str:
        if df.empty or col not in df:
            return "n/a"
        val = df.iloc[0][col]
        if pd.isna(val):
            return "n/a"
        if isinstance(val, (float, np.floating)):
            return f"{val:.6g}"
        return str(val)

    def markdown_table(df: pd.DataFrame) -> str:
        if df.empty:
            return "No damage row found."
        display = df.copy()
        for col in display.columns:
            display[col] = display[col].map(
                lambda x: "" if pd.isna(x) else f"{x:.6g}" if isinstance(x, (float, np.floating)) else str(x)
            )
        header = "| " + " | ".join(display.columns) + " |"
        sep = "| " + " | ".join("---" for _ in display.columns) + " |"
        rows = [
            "| " + " | ".join(str(row[col]) for col in display.columns) + " |"
            for _, row in display.iterrows()
        ]
        return "\n".join([header, sep, *rows])

    text = f"""# Avalanche vs Irradiation Pilot

Scope: C2M0080120D only.

This pilot intentionally does not claim UIS/proton failure equivalence. The
available proton C2M0080120D events are SELCII leakage events, not SEB.

## Cohorts

- Avalanche D3 UIS waveforms: {value(avalanche, 'n_files')} files.
- Proton irradiation SELCII events: {value(proton, 'n_events')} events across {value(proton, 'n_files')} files.
- Heavy-ion irradiation SEB contrast: {value(seb, 'n_events')} events across {value(seb, 'n_files')} files.

## Key Descriptors

- Avalanche D3 median pulse/current amplitude: {value(avalanche, 'delta_id_abs_a_median')} A.
- Avalanche D3 median Vds collapse fraction: {value(avalanche, 'vds_collapse_fraction_median')}.
- Proton SELCII median delta |Id|: {value(proton, 'delta_id_abs_a_median')} A.
- Proton SELCII median Vds collapse fraction: {value(proton, 'vds_collapse_fraction_median')}.
- Heavy-ion SEB median delta |Id|: {value(seb, 'delta_id_abs_a_median')} A.
- Heavy-ion SEB median Vds collapse fraction: {value(seb, 'vds_collapse_fraction_median')}.

## Damage Case Study

The probable C2M0080120D D3/d3 mapping has one pre/post avalanche IV pair:

{markdown_table(damage)}

## Interpretation

The proton subset does not show a UIS-burnout analogue in this pilot: it has
small drain-source leakage steps and essentially no Vds collapse. The heavy-ion
SEB contrast is the irradiation cohort that shares the hard-collapse descriptor
with the UIS waveforms, although its current scale is still instrument- and
stress-regime dependent.

Treat this as a capability check and case study, not population-level evidence.
"""
    out_path.write_text(text)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()

    avalanche_sql = """
    SELECT w.metadata_id, w.measurement_id, w.experiment, w.device_id,
           w.device_type, md.sample_group, w.filename,
           w.avalanche_condition_label, w.avalanche_peak_current_a,
           w.avalanche_energy_j, w.point_index, w.time_val, w.time_us,
           w.vds, w.id_drain, w.vgs, w.igs
    FROM avalanche_waveform_view w
    JOIN baselines_metadata md ON md.id = w.metadata_id
    WHERE w.device_type='C2M0080120D'
      AND lower(md.sample_group)='d3'
      AND w.avalanche_mode='UIS'
    ORDER BY w.metadata_id, w.point_index;
    """
    event_sql = """
    SELECT *
    FROM irradiation_single_event_view
    WHERE device_type='C2M0080120D'
      AND (
        lower(ion_species)='proton'
        OR event_type='SEB'
      )
    ORDER BY metadata_id, event_index;
    """

    avalanche_waveforms = load_sql_frame(conn, avalanche_sql)
    irradiation_events = load_sql_frame(conn, event_sql)
    damage = damage_case_study(conn)
    conn.close()

    if avalanche_waveforms.empty:
        raise SystemExit("No C2M0080120D D3 avalanche waveforms found")
    if irradiation_events.empty:
        raise SystemExit("No C2M0080120D irradiation events found")

    avalanche_features = avalanche_waveform_features(avalanche_waveforms)
    irrad_features = irradiation_event_features(irradiation_events)
    features = pd.concat([avalanche_features, irrad_features], ignore_index=True)
    summary = cohort_summary(features)

    avalanche_waveforms.to_csv(OUT_DIR / "avalanche_d3_waveforms.csv", index=False)
    irradiation_events.to_csv(OUT_DIR / "irradiation_c2m_events.csv", index=False)
    features.to_csv(OUT_DIR / "pilot_event_features.csv", index=False)
    summary.to_csv(OUT_DIR / "pilot_cohort_summary.csv", index=False)
    damage.to_csv(OUT_DIR / "avalanche_d3_damage_case_study.csv", index=False)

    plot_event_space(features, OUT_DIR / "phenotype_space.png")
    plot_avalanche_waveforms(avalanche_waveforms, OUT_DIR / "avalanche_d3_waveforms.png")
    write_readme(summary, damage, OUT_DIR / "README.md")

    print(f"Wrote pilot outputs to {OUT_DIR}")
    print(summary.to_string(index=False))
    if not damage.empty:
        print()
        print(damage.to_string(index=False))


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    main()
