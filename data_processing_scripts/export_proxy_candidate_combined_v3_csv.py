#!/usr/bin/env python3
"""Export stress_proxy_candidate_combined_v3 to CSV for the interactive viewer.

The v3 ranker is a screening-only weighted vector over v2's top-10 pool.
This export keeps top-N rows per target and adds weighted squared-component
terms plus per-row shares so the viewer can show which axes drive each pick.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from psycopg2.extras import RealDictCursor

try:
    from db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - package import path
    from data_processing_scripts.db_config import get_connection

OUT_DIR = Path(__file__).resolve().parent.parent / "out" / "avalanche_irrad_pilot"
OUTPUT_CSV = OUT_DIR / "proxy_candidate_combined_v3.csv"

ARRAY_COLUMNS = (
    "energy_v2_blockers",
    "energy_v2_notes",
    "proxy_claim_blockers",
)

QUERY = """
    WITH components AS (
        SELECT
            v3.*,
            signature_axis_weight * POWER(COALESCE(signature_axis_distance, 3.0), 2)
                AS signature_component_weighted_sq,
            duration_weight * POWER(COALESCE(duration_log_delta, 1.0), 2)
                AS duration_component_weighted_sq,
            log_energy_weight * POWER(COALESCE(ABS(log_energy_delta), 5.0), 2)
                AS log_energy_component_weighted_sq,
            failure_fraction_weight * POWER(
                COALESCE(failure_fraction_log_delta, terminal_energy_overlap_score), 2
            ) AS failure_fraction_component_weighted_sq,
            post_iv_damage_weight * POWER(COALESCE(best_damage_distance, 2.50), 2)
                AS post_iv_damage_component_weighted_sq,
            regime_path_weight * POWER(COALESCE(path_penalty, 0.75), 2)
                AS regime_path_component_weighted_sq,
            coverage_gap_weight * POWER(COALESCE(damage_signature_coverage_gap, 1.0), 2)
                AS coverage_gap_component_weighted_sq
        FROM stress_proxy_candidate_combined_v3 v3
        WHERE combined_rank <= %s
    ), totals AS (
        SELECT
            *,
            signature_component_weighted_sq
          + duration_component_weighted_sq
          + log_energy_component_weighted_sq
          + failure_fraction_component_weighted_sq
          + post_iv_damage_component_weighted_sq
          + regime_path_component_weighted_sq
          + coverage_gap_component_weighted_sq AS component_weighted_sq_total
        FROM components
    )
    SELECT
        target_stress_record_key, candidate_stress_record_key, device_type,
        target_event_type, target_ion_species, candidate_source, match_scope,
        waveform_rank, energy_rank, combined_rank, combined_vector_distance,
        proxy_claim_status, proxy_claim_basis, proxy_claim_blockers, proxy_claim_summary,
        mechanistic_energy_candidate_status, truth_validation_status, truth_label,
        truth_label_basis, signature_axis_distance, duration_log_delta,
        log_energy_delta, log_energy_delta_dex, candidate_failure_fraction_overlap_class,
        terminal_energy_overlap_class, failure_fraction_log_delta, best_damage_distance,
        damage_signature_coverage_gap, regime_match_class, path_penalty,
        signature_axis_weight, duration_weight, log_energy_weight, failure_fraction_weight,
        post_iv_damage_weight, regime_path_weight, coverage_gap_weight,
        signature_component_weighted_sq, duration_component_weighted_sq,
        log_energy_component_weighted_sq, failure_fraction_component_weighted_sq,
        post_iv_damage_component_weighted_sq, regime_path_component_weighted_sq,
        coverage_gap_component_weighted_sq, component_weighted_sq_total,
        signature_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS signature_component_share,
        duration_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS duration_component_share,
        log_energy_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS log_energy_component_share,
        failure_fraction_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS failure_fraction_component_share,
        post_iv_damage_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS post_iv_damage_component_share,
        regime_path_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS regime_path_component_share,
        coverage_gap_component_weighted_sq / NULLIF(component_weighted_sq_total, 0.0)
            AS coverage_gap_component_share,
        energy_v2_blockers, energy_v2_notes
    FROM totals
    ORDER BY device_type, target_stress_record_key, combined_rank
"""


def _flatten_array(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def export(conn, out_path: Path = OUTPUT_CSV, top_n: int = 3) -> Path:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, (top_n,))
        rows = [dict(r) for r in cur.fetchall()]
    df = pd.DataFrame(rows)
    if not df.empty:
        total = pd.to_numeric(df.get("component_weighted_sq_total"), errors="coerce")
        distance = pd.to_numeric(df.get("combined_vector_distance"), errors="coerce")
        delta = (total - distance.pow(2)).abs()
        bad = delta[delta > 1e-9]
        if not bad.empty:
            raise RuntimeError(
                "v3 component invariant failed: sum(weighted components) "
                "does not match combined_vector_distance^2"
            )
    for col in ARRAY_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_flatten_array)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=OUTPUT_CSV)
    ap.add_argument("--top-n", type=int, default=3,
                    help="Keep top-N v3 candidates per target (default 3).")
    args = ap.parse_args()
    with get_connection() as conn:
        path = export(conn, out_path=args.out, top_n=args.top_n)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
