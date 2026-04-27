#!/usr/bin/env python3
"""
Short-Circuit ↔ Irradiation Damage-Equivalence Model
====================================================
Build a nearest-neighbor map that, given an irradiation run (ion,
beam_energy, LET), answers: *what short-circuit voltage and duration
produces the same device damage?*

Bridge metric: median (ΔVth, ΔRds(on), ΔBV) per stress event, where
pristine baseline is the device_type-wide median across all files
labeled pristine / pre_irrad.  Runs extract_damage_metrics.py first
if gate_params is not populated.

What it does:
  1. Creates/refreshes SQL view `damage_equivalence_view` containing one
     row per SC condition (device_type, sc_voltage_v, sc_duration_us)
     and one row per irradiation run (device_type, irrad_run_id), each
     with median ΔVth / ΔRds / ΔBV, IQR, and sample counts.
  2. In Python: builds a per-device-type damage-space nearest-neighbor
     retriever. The distance metric is a reliability-weighted Euclidean
     score over available axes (ΔVth, ΔRds, ΔBV), normalized by per-axis
     robust scale. Missing axes are dropped pairwise.
  3. Writes `out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv` and
     two scatter plots (ΔVth vs ΔRds, ΔVth vs ΔBV).

CLI prediction mode (uses the cached view):
    python3 ml_sc_irrad_equivalence.py --ion Au --energy 1162 --let 67.1 \
        --device-type C2M0080120D

Usage:
    python3 ml_sc_irrad_equivalence.py --rebuild       # build view + plots
    python3 ml_sc_irrad_equivalence.py --ion Au --energy 1162 --let 67.1
    python3 ml_sc_irrad_equivalence.py --device-type C2M0080120D --rebuild
"""

import argparse
import csv
import sys
from pathlib import Path

try:
    import numpy as np
except ImportError:
    sys.exit("numpy is required: pip install --break-system-packages numpy")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("matplotlib is required: "
             "pip install --break-system-packages matplotlib")

from db_config import get_connection


# ── SQL view ────────────────────────────────────────────────────────────────
# Pristine baseline: median Vth/Rds/BV per device_type, computed ONLY over
# files explicitly labeled pristine pre-stress:
#   * irrad_role = 'pre_irrad'     (irradiation pre-stress IV)
#   * test_condition = 'pristine'  (SC pre-stress IV)
#
# data_source='baselines' is deliberately excluded. That pool was built for
# the device-library workflow and contains mixed Idss/leakage measurements
# from pre-/post-irrad experiment pairs — its BV/Vth values don't represent
# clean device characterisation and previously dragged C2M0080120D BV median
# from 1089 V down to 811 V (~280 V bias).
#
# Verified: no device_type loses baseline coverage under this stricter filter
# — every device with post_sc or post_irrad data also has explicit pristine
# or pre_irrad rows.
DAMAGE_VIEW_SQL = """
DROP VIEW IF EXISTS damage_equivalence_view CASCADE;

CREATE VIEW damage_equivalence_view AS
WITH pristine_pool AS (
    SELECT device_type,
           (gate_params->>'vth_v')::double precision      AS vth,
           (gate_params->>'rdson_mohm')::double precision AS rds,
           (gate_params->>'bvdss_v')::double precision    AS bv
    FROM baselines_metadata
    WHERE device_type IS NOT NULL
      AND gate_params IS NOT NULL
      AND (irrad_role = 'pre_irrad' OR test_condition = 'pristine')
),
pristine_stats AS (
    SELECT device_type,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vth) AS pristine_vth,
           COUNT(vth)                                       AS pristine_vth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rds) AS pristine_rds,
           COUNT(rds)                                       AS pristine_rds_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bv)  AS pristine_bv,
           COUNT(bv)                                        AS pristine_bv_n
    FROM pristine_pool
    GROUP BY device_type
),

/* ── SC per-file deltas ──────────────────────────────────────────────────── */
/* Physical sanity clips: extractor occasionally blows up on broken devices
   (e.g. near-zero current → Rds(on) = 10^13 mΩ).  Filter those here so
   they don't poison the aggregate fingerprint. */
sc_per_file AS (
    SELECT md.device_type,
           md.sample_group,
           md.sc_voltage_v,
           md.sc_duration_us,
           CASE WHEN ABS((md.gate_params->>'vth_v')::double precision
                         - p.pristine_vth) <= 10.0
                THEN (md.gate_params->>'vth_v')::double precision - p.pristine_vth
                ELSE NULL END AS dvth,
           CASE WHEN ABS((md.gate_params->>'rdson_mohm')::double precision
                         - p.pristine_rds) <= 10000.0
                THEN (md.gate_params->>'rdson_mohm')::double precision - p.pristine_rds
                ELSE NULL END AS drds,
           CASE WHEN ABS((md.gate_params->>'bvdss_v')::double precision
                         - p.pristine_bv) <= 2000.0
                THEN (md.gate_params->>'bvdss_v')::double precision - p.pristine_bv
                ELSE NULL END AS dbv
    FROM baselines_metadata md
    JOIN pristine_stats p USING (device_type)
    WHERE md.data_source = 'sc_ruggedness'
      AND md.test_condition = 'post_sc'
      AND md.sc_voltage_v IS NOT NULL
      AND md.sc_duration_us IS NOT NULL
      AND md.gate_params IS NOT NULL
),
sc_fp AS (
    SELECT device_type, sc_voltage_v, sc_duration_us,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dvth)           AS dvth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth)      AS dvth_iqr,
           COUNT(dvth)                                                 AS dvth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY drds)           AS drds,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds)      AS drds_iqr,
           COUNT(drds)                                                 AS drds_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dbv)            AS dbv,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)       AS dbv_iqr,
           COUNT(dbv)                                                  AS dbv_n,
           COUNT(DISTINCT sample_group)                                AS n_samples
    FROM sc_per_file
    GROUP BY device_type, sc_voltage_v, sc_duration_us
),

/* ── Irradiation per-file deltas ─────────────────────────────────────────── */
/* Same sanity clips as SC side. */
irrad_per_file AS (
    SELECT md.device_type,
           md.device_id,
           md.irrad_run_id,
           CASE WHEN ABS((md.gate_params->>'vth_v')::double precision
                         - p.pristine_vth) <= 10.0
                THEN (md.gate_params->>'vth_v')::double precision - p.pristine_vth
                ELSE NULL END AS dvth,
           CASE WHEN ABS((md.gate_params->>'rdson_mohm')::double precision
                         - p.pristine_rds) <= 10000.0
                THEN (md.gate_params->>'rdson_mohm')::double precision - p.pristine_rds
                ELSE NULL END AS drds,
           CASE WHEN ABS((md.gate_params->>'bvdss_v')::double precision
                         - p.pristine_bv) <= 2000.0
                THEN (md.gate_params->>'bvdss_v')::double precision - p.pristine_bv
                ELSE NULL END AS dbv
    FROM baselines_metadata md
    JOIN pristine_stats p USING (device_type)
    WHERE md.irrad_role = 'post_irrad'
      AND md.irrad_run_id IS NOT NULL
      AND md.gate_params IS NOT NULL
),
irrad_fp AS (
    SELECT ipf.device_type, ipf.irrad_run_id,
           r.ion_species, r.beam_energy_mev, r.let_surface,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dvth)           AS dvth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth)      AS dvth_iqr,
           COUNT(dvth)                                                 AS dvth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY drds)           AS drds,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds)      AS drds_iqr,
           COUNT(drds)                                                 AS drds_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dbv)            AS dbv,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)       AS dbv_iqr,
           COUNT(dbv)                                                  AS dbv_n,
           COUNT(DISTINCT device_id)                                   AS n_samples
    FROM irrad_per_file ipf
    JOIN irradiation_runs r ON r.id = ipf.irrad_run_id
    GROUP BY ipf.device_type, ipf.irrad_run_id,
             r.ion_species, r.beam_energy_mev, r.let_surface
)

SELECT 'sc'::text                              AS source,
       device_type,
       sc_voltage_v, sc_duration_us,
       NULL::text                              AS ion_species,
       NULL::double precision                  AS beam_energy_mev,
       NULL::double precision                  AS let_surface,
       NULL::integer                           AS irrad_run_id,
       dvth, dvth_iqr, dvth_n,
       drds, drds_iqr, drds_n,
       dbv,  dbv_iqr,  dbv_n,
       n_samples,
       sc_voltage_v::text || 'V / '
         || sc_duration_us::text || 'us'       AS label
FROM sc_fp
UNION ALL
SELECT 'irrad'::text                           AS source,
       device_type,
       NULL::double precision                  AS sc_voltage_v,
       NULL::double precision                  AS sc_duration_us,
       ion_species, beam_energy_mev, let_surface,
       irrad_run_id,
       dvth, dvth_iqr, dvth_n,
       drds, drds_iqr, drds_n,
       dbv,  dbv_iqr,  dbv_n,
       n_samples,
       COALESCE(ion_species, '?')
         || ' @ ' || COALESCE(beam_energy_mev::text, '?') || ' MeV'
         || ' (LET ' || COALESCE(let_surface::text, '?') || ')'
                                               AS label
FROM irrad_fp;
"""


# ── Output paths ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "out" / "sc_irrad_equivalence"
DAMAGE_AXES = ("dvth", "drds", "dbv")


# ── Helpers ─────────────────────────────────────────────────────────────────
def ensure_gate_params_populated(conn):
    """If gate_params is empty everywhere, hint to run extract_damage_metrics.py first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM baselines_metadata "
            "WHERE gate_params->>'vth_v' IS NOT NULL"
        )
        n = cur.fetchone()[0]
    if n < 10:
        print("WARNING: gate_params looks empty across baselines_metadata "
              f"(only {n} rows with vth_v). Run:\n"
              "    python3 extract_damage_metrics.py\n"
              "first to populate damage metrics, or this view will be empty.",
              file=sys.stderr)


def rebuild_view(conn):
    print("Rebuilding damage_equivalence_view …", flush=True)
    with conn.cursor() as cur:
        cur.execute(DAMAGE_VIEW_SQL)
    conn.commit()


def load_fingerprints(conn, device_type=None):
    sql = """
        SELECT source, device_type, sc_voltage_v, sc_duration_us,
               ion_species, beam_energy_mev, let_surface, irrad_run_id,
               dvth, dvth_iqr, dvth_n,
               drds, drds_iqr, drds_n,
               dbv,  dbv_iqr,  dbv_n,
               n_samples, label
        FROM damage_equivalence_view
    """
    params = []
    if device_type:
        sql += " WHERE device_type = %s"
        params.append(device_type)
    sql += " ORDER BY device_type, source, label"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def _robust_axis_scale(values):
    """Robust per-axis scale used to normalize damage differences."""
    vals = np.array([float(v) for v in values if v is not None], dtype=float)
    if vals.size < 2:
        return 1.0
    q1, q3 = np.percentile(vals, [25.0, 75.0])
    iqr = float(q3 - q1)
    if iqr > 1e-12:
        # Convert IQR to Gaussian-equivalent sigma.
        return max(iqr / 1.349, 1e-6)
    std = float(np.std(vals))
    return max(std, 1.0)


def _fit_axis_scales(group):
    """Return per-axis robust scales for one device_type group."""
    return {
        axis: _robust_axis_scale([fp.get(axis) for fp in group])
        for axis in DAMAGE_AXES
    }


def _axis_reliability(fp, axis):
    """Reliability from axis sample count and spread (IQR)."""
    n = fp.get(f"{axis}_n")
    iqr = fp.get(f"{axis}_iqr")
    n_term = np.sqrt(max(float(n), 1.0)) if n is not None else 1.0
    iqr_term = 1.0 / (1.0 + abs(float(iqr))) if iqr is not None else 1.0
    return n_term * iqr_term


def _damage_space_distance(ir_fp, sc_fp, axis_scales):
    """Reliability-weighted distance between one irrad and one SC fingerprint."""
    weighted_sq = 0.0
    total_w = 0.0
    n_dims = 0
    for axis in DAMAGE_AXES:
        iv = ir_fp.get(axis)
        sv = sc_fp.get(axis)
        if iv is None or sv is None:
            continue
        scale = axis_scales.get(axis, 1.0)
        if scale <= 0:
            scale = 1.0
        delta = (float(iv) - float(sv)) / scale
        w = np.sqrt(_axis_reliability(ir_fp, axis) *
                    _axis_reliability(sc_fp, axis))
        weighted_sq += w * delta * delta
        total_w += w
        n_dims += 1

    if n_dims == 0 or total_w <= 0.0:
        return None, 0
    return float(np.sqrt(weighted_sq / total_w)), n_dims


def compute_matches(fps, k=3):
    """For each irrad fingerprint, retrieve k nearest SC fingerprints in the
    same device_type using reliability-weighted damage-space distance.

    Distance is computed over axis intersection (ΔVth/ΔRds/ΔBV) and each
    axis is normalized by a robust per-device_type scale.

    Returns list of {irrad_fp, matches: [(sc_fp, distance, n_dims)]}.
    """
    per_dev = {}
    for fp in fps:
        per_dev.setdefault(fp["device_type"], []).append(fp)

    results = []
    for dev, grp in per_dev.items():
        sc = [f for f in grp if f["source"] == "sc"]
        ir = [f for f in grp if f["source"] == "irrad"]
        if not sc or not ir:
            continue

        axis_scales = _fit_axis_scales(grp)

        for irfp in ir:
            dists = []
            for scfp in sc:
                d, n_dims = _damage_space_distance(irfp, scfp, axis_scales)
                if d is None:
                    continue
                dists.append((scfp, d, n_dims))
            dists.sort(key=lambda t: t[1])
            results.append({"irrad": irfp, "matches": dists[:k]})
    return results


def write_csv(results, path):
    fields = [
        "device_type", "ion_species", "beam_energy_mev", "let_surface",
        "irrad_run_id", "irrad_dvth", "irrad_drds", "irrad_dbv",
        "irrad_n_samples",
        "nearest_sc_voltage_v", "nearest_sc_duration_us",
        "nearest_distance", "nearest_n_dims",
        "sc_dvth", "sc_drds", "sc_dbv", "sc_n_samples",
        "k3_alternatives",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            ir = r["irrad"]
            if not r["matches"]:
                continue
            first = r["matches"][0]
            alts = "; ".join(
                f"{m[0]['sc_voltage_v']}V/{m[0]['sc_duration_us']}us (d={m[1]:.2f})"
                for m in r["matches"][1:]
            )
            w.writerow({
                "device_type":      ir["device_type"],
                "ion_species":      ir["ion_species"],
                "beam_energy_mev":  ir["beam_energy_mev"],
                "let_surface":      ir["let_surface"],
                "irrad_run_id":     ir["irrad_run_id"],
                "irrad_dvth":       ir["dvth"],
                "irrad_drds":       ir["drds"],
                "irrad_dbv":        ir["dbv"],
                "irrad_n_samples":  ir["n_samples"],
                "nearest_sc_voltage_v":  first[0]["sc_voltage_v"],
                "nearest_sc_duration_us":first[0]["sc_duration_us"],
                "nearest_distance":      round(first[1], 3),
                "nearest_n_dims":        first[2],
                "sc_dvth":          first[0]["dvth"],
                "sc_drds":          first[0]["drds"],
                "sc_dbv":           first[0]["dbv"],
                "sc_n_samples":     first[0]["n_samples"],
                "k3_alternatives":  alts,
            })


def plot_pair(fps, x_key, y_key, x_label, y_label, path):
    # Restrict to device_types that have BOTH SC and irradiation fingerprints —
    # those are the only points that matter for equivalence matching; others
    # would just be visual noise.
    dtypes_with_sc    = {f["device_type"] for f in fps if f["source"] == "sc"}
    dtypes_with_irrad = {f["device_type"] for f in fps if f["source"] == "irrad"}
    keep = dtypes_with_sc & dtypes_with_irrad
    fps = [f for f in fps if f["device_type"] in keep]

    sc = [f for f in fps if f["source"] == "sc"
          and f.get(x_key) is not None and f.get(y_key) is not None]
    ir = [f for f in fps if f["source"] == "irrad"
          and f.get(x_key) is not None and f.get(y_key) is not None]
    if not sc and not ir:
        print(f"  skipping {path.name}: no data on both axes")
        return

    fig, ax = plt.subplots(figsize=(11, 7))
    palette = ["tab:blue", "tab:orange", "tab:green", "tab:red"]
    dev_colors = {d: palette[i % len(palette)] for i, d in enumerate(sorted(keep))}

    for fp in sc:
        c = dev_colors[fp["device_type"]]
        ax.scatter(fp[x_key], fp[y_key], marker="o",
                   s=40 + 8 * (fp["n_samples"] or 1), c=c, alpha=0.55,
                   edgecolors="k", linewidths=0.4)
        ax.annotate(f"{fp['sc_voltage_v']:g}V/{fp['sc_duration_us']:g}us",
                    (fp[x_key], fp[y_key]), fontsize=6.5, alpha=0.75,
                    xytext=(5, 3), textcoords="offset points")
    for fp in ir:
        c = dev_colors[fp["device_type"]]
        ax.scatter(fp[x_key], fp[y_key], marker="^",
                   s=80 + 14 * (fp["n_samples"] or 1), c=c,
                   edgecolors="k", linewidths=0.7)
        ion = fp.get("ion_species") or "?"
        energy = fp.get("beam_energy_mev")
        lbl = f"{ion}/{energy:g}MeV" if energy is not None else ion
        ax.annotate(lbl, (fp[x_key], fp[y_key]), fontsize=8,
                    xytext=(5, 3), textcoords="offset points", weight="bold")

    ax.axhline(0, color="k", lw=0.5, alpha=0.3)
    ax.axvline(0, color="k", lw=0.5, alpha=0.3)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(f"{x_label} vs {y_label}: SC (•) vs Irradiation (▲)")
    for d, c in dev_colors.items():
        ax.scatter([], [], marker="o", c=c, label=f"SC {d}")
    for d, c in dev_colors.items():
        ax.scatter([], [], marker="^", c=c, label=f"Irrad {d}")
    ax.legend(fontsize=8, loc="best")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  wrote {path}")


def print_cli_prediction(conn, ion, energy, let, device_type):
    """Find the irrad fingerprint in the view closest to (ion, energy, LET)
    for the given device_type, then print its nearest SC match.
    """
    fps = load_fingerprints(conn, device_type=device_type)
    if not fps:
        print(f"No fingerprints for device_type={device_type}.", file=sys.stderr)
        sys.exit(1)

    irrad = [f for f in fps if f["source"] == "irrad"]
    if not irrad:
        print(f"No irradiation fingerprints for device_type={device_type}.",
              file=sys.stderr)
        sys.exit(1)

    def query_score(f):
        s = 0.0
        if ion and f.get("ion_species"):
            s += 0.0 if ion.lower() == f["ion_species"].lower() else 10.0
        if energy is not None and f.get("beam_energy_mev") is not None:
            s += abs(energy - f["beam_energy_mev"]) / max(abs(energy), 1.0)
        if let is not None and f.get("let_surface") is not None:
            s += abs(let - f["let_surface"]) / max(abs(let), 1.0)
        return s
    target = min(irrad, key=query_score)

    results = compute_matches(fps, k=3)
    match = next((r for r in results if r["irrad"] is target), None)
    if not match or not match["matches"]:
        print("No SC match could be computed (missing damage axes).",
              file=sys.stderr)
        sys.exit(1)

    first = match["matches"][0]
    scfp, dist, n_dims = first
    print(f"\nClosest irradiation fingerprint in the view:")
    print(f"  {target['label']}  (device_type={target['device_type']})")
    print(f"  ΔVth={target['dvth']}, ΔRds={target['drds']}, ΔBV={target['dbv']}, "
          f"n_samples={target['n_samples']}")
    print("\nNearest SC condition (reliability-weighted damage-space distance):")
    print(f"  {scfp['sc_voltage_v']:g} V × {scfp['sc_duration_us']:g} µs")
    print(f"  distance = {dist:.3f}  (over {n_dims} damage axes)")
    print(f"  SC ΔVth={scfp['dvth']}, ΔRds={scfp['drds']}, ΔBV={scfp['dbv']}, "
          f"n_samples={scfp['n_samples']}")
    if dist > 1.5:
        print("\n  WARNING: distance > 1.5 — this is a weak match. "
              f"Check IQR and consider extending the dataset.")
    if len(match["matches"]) > 1:
        print(f"\nAlternatives (k=3):")
        for scfp2, d2, nd2 in match["matches"][1:]:
            print(f"  {scfp2['sc_voltage_v']:g} V × {scfp2['sc_duration_us']:g} µs  "
                  f"(distance={d2:.3f}, {nd2} axes)")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--rebuild", action="store_true",
                    help="Recreate damage_equivalence_view, CSV, and plots")
    ap.add_argument("--device-type",
                    help="Restrict CLI prediction to one device_type")
    ap.add_argument("--ion", help="Ion species for CLI prediction (e.g. Au)")
    ap.add_argument("--energy", type=float,
                    help="Beam energy in MeV for CLI prediction")
    ap.add_argument("--let", type=float,
                    help="LET at surface (MeV·cm²/mg) for CLI prediction")
    args = ap.parse_args()

    conn = get_connection()
    try:
        ensure_gate_params_populated(conn)
        if args.rebuild:
            rebuild_view(conn)
            fps = load_fingerprints(conn, device_type=args.device_type)
            print(f"Loaded {len(fps)} fingerprints "
                  f"({sum(1 for f in fps if f['source']=='sc')} SC, "
                  f"{sum(1 for f in fps if f['source']=='irrad')} irrad)")

            results = compute_matches(fps, k=3)
            print(f"Computed nearest-SC matches for "
                  f"{len(results)} irradiation fingerprints.")

            csv_path = OUT_DIR / "irrad_to_sc_equivalents.csv"
            write_csv(results, csv_path)
            print(f"Wrote {csv_path}")

            plot_pair(fps, "dvth", "drds",
                      "ΔVth (V)", "ΔRds(on) (mΩ)",
                      OUT_DIR / "damage_scatter_dvth_drds.png")
            plot_pair(fps, "dvth", "dbv",
                      "ΔVth (V)", "ΔV(BR)DSS (V)",
                      OUT_DIR / "damage_scatter_dvth_dbv.png")

        if args.ion or args.energy or args.let:
            if not args.device_type:
                sys.exit("--device-type is required for CLI prediction")
            print_cli_prediction(conn, args.ion, args.energy, args.let,
                                 args.device_type)
        elif not args.rebuild:
            ap.print_help()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
