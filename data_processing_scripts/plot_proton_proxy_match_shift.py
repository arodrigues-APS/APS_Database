#!/usr/bin/env python3
"""
Visualize how proton SEB proxy matches change when collapse is added.

This is a descriptive diagnostic, not a validated equivalence model.  It compares
200 MeV proton irradiation SEB events to SC and avalanche waveform candidates in
energy/collapse space using two nearest-neighbor metrics:

  1. energy_only: min |log(E_proxy) - log(E_proton)|
  2. energy_plus_collapse: min sqrt(log_energy_delta^2 + collapse_delta^2)

Outputs:
  out/proxy_matching_shift/proton_proxy_match_shift.png
  out/proxy_matching_shift/proton_proxy_match_shift.csv
  out/proxy_matching_shift/proton_proxy_match_shift_summary.md
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
except ImportError as exc:
    raise SystemExit("numpy, pandas, and matplotlib are required") from exc

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db_config import get_connection  # noqa: E402


OUT_DIR = Path("out/proxy_matching_shift")
EPS = 1e-12

PROTON_SQL = """
SELECT event_id, metadata_id, event_index, device_type, filename,
       COALESCE(event_energy_vds_id_j, event_energy_proxy_j) AS energy_j,
       COALESCE(vds_collapse_fraction, 0.0) AS collapse_fraction,
       peak_abs_id_a,
       beam_energy_mev,
       path_type,
       readiness_status,
       has_condition_post_iv
FROM stress_waveform_event_features
WHERE source = 'irradiation'
  AND ion_species = 'proton'
  AND event_type = 'SEB'
  AND COALESCE(event_energy_vds_id_j, event_energy_proxy_j) IS NOT NULL
ORDER BY event_id;
"""

PROXY_SQL = """
SELECT source, event_type, metadata_id, device_type, filename,
       event_energy_vds_id_j AS energy_j,
       COALESCE(vds_collapse_fraction, 0.0) AS collapse_fraction,
       peak_abs_id_a,
       readiness_status,
       has_condition_post_iv
FROM stress_waveform_event_features
WHERE source IN ('sc', 'avalanche')
  AND event_energy_vds_id_j IS NOT NULL
ORDER BY source, event_type, metadata_id;
"""


def label_proxy(row: pd.Series) -> str:
    if row["source"] == "sc":
        return "SC waveform"
    return f"avalanche {row['event_type']}"


def finite_energy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["energy_j"] = pd.to_numeric(out["energy_j"], errors="coerce")
    out["collapse_fraction"] = pd.to_numeric(out["collapse_fraction"], errors="coerce").fillna(0.0)
    return out[out["energy_j"].notna() & (out["energy_j"] > 0)].reset_index(drop=True)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = get_connection()
    try:
        proton = pd.read_sql_query(PROTON_SQL, conn)
        proxy = pd.read_sql_query(PROXY_SQL, conn)
    finally:
        conn.close()
    proton = finite_energy(proton)
    proxy = finite_energy(proxy)
    if proton.empty:
        raise SystemExit("No proton SEB events with energy were found")
    if proxy.empty:
        raise SystemExit("No SC/avalanche proxy waveforms with energy were found")
    proxy["proxy_label"] = proxy.apply(label_proxy, axis=1)
    return proton, proxy


def nearest_matches(proton: pd.DataFrame, proxy: pd.DataFrame,
                    collapse_scale: float) -> pd.DataFrame:
    rows = []
    proxy_energy_log = np.log(np.maximum(proxy["energy_j"].to_numpy(float), EPS))
    proxy_collapse = proxy["collapse_fraction"].to_numpy(float)

    for _, p in proton.iterrows():
        p_energy = float(p["energy_j"])
        p_collapse = float(p["collapse_fraction"])
        log_delta = np.abs(proxy_energy_log - math.log(max(p_energy, EPS)))
        collapse_delta = np.abs(proxy_collapse - p_collapse)
        ec_distance = np.sqrt(log_delta ** 2 + (collapse_delta / collapse_scale) ** 2)

        energy_idx = int(np.argmin(log_delta))
        ec_idx = int(np.argmin(ec_distance))
        energy_match = proxy.iloc[energy_idx]
        ec_match = proxy.iloc[ec_idx]

        rows.append({
            "event_id": int(p["event_id"]),
            "metadata_id": int(p["metadata_id"]),
            "event_index": int(p["event_index"]),
            "device_type": p["device_type"],
            "filename": p["filename"],
            "proton_energy_j": p_energy,
            "proton_collapse_fraction": p_collapse,
            "proton_peak_abs_id_a": p["peak_abs_id_a"],
            "proton_readiness_status": p["readiness_status"],
            "proton_has_condition_post_iv": bool(p["has_condition_post_iv"]),
            "energy_only_proxy_source": energy_match["source"],
            "energy_only_proxy_type": energy_match["event_type"],
            "energy_only_proxy_label": energy_match["proxy_label"],
            "energy_only_proxy_metadata_id": int(energy_match["metadata_id"]),
            "energy_only_proxy_device_type": energy_match["device_type"],
            "energy_only_proxy_energy_j": float(energy_match["energy_j"]),
            "energy_only_proxy_collapse_fraction": float(energy_match["collapse_fraction"]),
            "energy_only_proxy_has_condition_post_iv": bool(energy_match["has_condition_post_iv"]),
            "energy_only_log_energy_distance": float(log_delta[energy_idx]),
            "energy_plus_collapse_proxy_source": ec_match["source"],
            "energy_plus_collapse_proxy_type": ec_match["event_type"],
            "energy_plus_collapse_proxy_label": ec_match["proxy_label"],
            "energy_plus_collapse_proxy_metadata_id": int(ec_match["metadata_id"]),
            "energy_plus_collapse_proxy_device_type": ec_match["device_type"],
            "energy_plus_collapse_proxy_energy_j": float(ec_match["energy_j"]),
            "energy_plus_collapse_proxy_collapse_fraction": float(ec_match["collapse_fraction"]),
            "energy_plus_collapse_proxy_has_condition_post_iv": bool(ec_match["has_condition_post_iv"]),
            "energy_plus_collapse_distance": float(ec_distance[ec_idx]),
            "collapse_scale": collapse_scale,
        })
    return pd.DataFrame(rows)


def color_for_label(label: str) -> str:
    colors = {
        "SC waveform": "#1f77b4",
        "avalanche UID": "#ff7f0e",
        "avalanche UIS": "#d55e00",
        "avalanche RT": "#9467bd",
        "avalanche Test": "#8c564b",
        "avalanche Avalanche": "#e377c2",
    }
    return colors.get(label, "#7f7f7f")


def setup_scatter(ax, proton: pd.DataFrame, proxy: pd.DataFrame, title: str) -> None:
    for label, group in proxy.groupby("proxy_label"):
        ax.scatter(
            group["energy_j"],
            group["collapse_fraction"],
            s=24,
            alpha=0.30,
            color=color_for_label(label),
            label=label,
            linewidth=0,
        )
    ax.scatter(
        proton["energy_j"],
        proton["collapse_fraction"],
        s=52,
        marker="*",
        color="#111111",
        label="proton SEB",
        zorder=5,
    )
    ax.set_xscale("log")
    ax.set_ylim(-0.04, 1.08)
    ax.set_xlabel("Energy in waveform/event window (J, log)")
    ax.set_ylabel("Vds collapse fraction")
    ax.grid(True, which="both", linewidth=0.35, alpha=0.35)
    ax.set_title(title)


def draw_links(ax, matches: pd.DataFrame, metric: str) -> None:
    if metric == "energy_only":
        source_col = "energy_only_proxy_label"
        energy_col = "energy_only_proxy_energy_j"
        collapse_col = "energy_only_proxy_collapse_fraction"
    else:
        source_col = "energy_plus_collapse_proxy_label"
        energy_col = "energy_plus_collapse_proxy_energy_j"
        collapse_col = "energy_plus_collapse_proxy_collapse_fraction"

    for _, row in matches.iterrows():
        label = row[source_col]
        ax.plot(
            [row["proton_energy_j"], row[energy_col]],
            [row["proton_collapse_fraction"], row[collapse_col]],
            color=color_for_label(label),
            alpha=0.28,
            linewidth=0.8,
            zorder=2,
        )


def plot_counts(ax, matches: pd.DataFrame) -> None:
    energy_counts = Counter(matches["energy_only_proxy_label"])
    ec_counts = Counter(matches["energy_plus_collapse_proxy_label"])
    labels = sorted(set(energy_counts) | set(ec_counts))
    x = np.arange(len(labels))
    width = 0.38
    ax.bar(x - width / 2, [energy_counts.get(label, 0) for label in labels],
           width, label="energy only", color="#999999")
    ax.bar(x + width / 2, [ec_counts.get(label, 0) for label in labels],
           width, label="energy + collapse", color="#111111")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.set_ylabel("Nearest proton events")
    ax.set_title("Nearest proxy class counts")
    ax.grid(axis="y", linewidth=0.35, alpha=0.35)
    ax.legend(frameon=False)


def plot_switch_matrix(ax, matches: pd.DataFrame) -> None:
    left_labels = sorted(matches["energy_only_proxy_label"].unique())
    right_labels = sorted(matches["energy_plus_collapse_proxy_label"].unique())
    matrix = np.zeros((len(left_labels), len(right_labels)), dtype=int)
    left_index = {label: i for i, label in enumerate(left_labels)}
    right_index = {label: i for i, label in enumerate(right_labels)}
    for _, row in matches.iterrows():
        matrix[left_index[row["energy_only_proxy_label"]],
               right_index[row["energy_plus_collapse_proxy_label"]]] += 1

    image = ax.imshow(matrix, cmap="Blues", aspect="auto")
    ax.set_xticks(np.arange(len(right_labels)))
    ax.set_xticklabels(right_labels, rotation=35, ha="right")
    ax.set_yticks(np.arange(len(left_labels)))
    ax.set_yticklabels(left_labels)
    ax.set_xlabel("Energy + collapse nearest proxy")
    ax.set_ylabel("Energy-only nearest proxy")
    ax.set_title("Match shift matrix")
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            if matrix[i, j] > 0:
                ax.text(j, i, str(matrix[i, j]), ha="center", va="center", color="#111111")
    plt.colorbar(image, ax=ax, fraction=0.046, pad=0.04, label="events")


def write_summary(matches: pd.DataFrame, out_path: Path) -> None:
    energy_counts = Counter(matches["energy_only_proxy_label"])
    ec_counts = Counter(matches["energy_plus_collapse_proxy_label"])
    switched = int((matches["energy_only_proxy_label"] !=
                    matches["energy_plus_collapse_proxy_label"]).sum())
    n = len(matches)

    def format_counts(counter: Counter) -> str:
        return "\n".join(
            f"- {label}: {count}" for label, count in counter.most_common()
        )

    median_proton_energy = matches["proton_energy_j"].median()
    median_proton_collapse = matches["proton_collapse_fraction"].median()
    median_energy_only_collapse = matches["energy_only_proxy_collapse_fraction"].median()
    median_ec_collapse = matches["energy_plus_collapse_proxy_collapse_fraction"].median()

    text = f"""# Proton Proxy Match Shift

Rows: {n} proton SEB events.
Events that change nearest proxy class when collapse is added: {switched}.

## Energy-Only Nearest Proxy Counts

{format_counts(energy_counts)}

## Energy + Collapse Nearest Proxy Counts

{format_counts(ec_counts)}

## Median Context

- Proton event energy: {median_proton_energy:.6g} J
- Proton collapse fraction: {median_proton_collapse:.6g}
- Energy-only proxy collapse fraction: {median_energy_only_collapse:.6g}
- Energy + collapse proxy collapse fraction: {median_ec_collapse:.6g}

Interpretation: energy-only matching finds waveforms with similar Joules even
when the failure phenotype differs. Adding collapse penalizes high-collapse
avalanche waveforms and moves the nearest candidates toward SC waveforms, whose
collapse profile is closer to the proton SEB events.
"""
    out_path.write_text(text)


def write_csv(matches: pd.DataFrame, out_path: Path) -> None:
    matches.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)


def make_plot(proton: pd.DataFrame, proxy: pd.DataFrame, matches: pd.DataFrame,
              out_path: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(15, 11))
    ax1, ax2, ax3, ax4 = axes.flat

    setup_scatter(ax1, proton, proxy, "Energy-only nearest proxy")
    draw_links(ax1, matches, "energy_only")
    setup_scatter(ax2, proton, proxy, "Energy + collapse nearest proxy")
    draw_links(ax2, matches, "energy_plus_collapse")
    plot_counts(ax3, matches)
    plot_switch_matrix(ax4, matches)

    handles, labels = ax1.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    fig.legend(by_label.values(), by_label.keys(), loc="upper center",
               ncol=4, frameon=False, bbox_to_anchor=(0.5, 0.985))
    fig.suptitle("200 MeV proton SEB proxy matching: why collapse changes the answer",
                 y=1.02, fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collapse-scale", type=float, default=0.25,
                    help="Collapse fraction scale used in the energy+collapse distance")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = ap.parse_args()

    if args.collapse_scale <= 0:
        raise SystemExit("--collapse-scale must be positive")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    proton, proxy = load_data()
    matches = nearest_matches(proton, proxy, args.collapse_scale)

    csv_path = args.out_dir / "proton_proxy_match_shift.csv"
    png_path = args.out_dir / "proton_proxy_match_shift.png"
    summary_path = args.out_dir / "proton_proxy_match_shift_summary.md"
    write_csv(matches, csv_path)
    make_plot(proton, proxy, matches, png_path)
    write_summary(matches, summary_path)

    print(f"proton events: {len(proton)}")
    print(f"proxy candidates: {len(proxy)}")
    print(f"wrote {csv_path}")
    print(f"wrote {png_path}")
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    main()
