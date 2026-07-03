#!/usr/bin/env python3
"""Export v1 (damage-signature) vs v2 (energy) proxy concordance to CSV.

Feeds the interactive viewer's 3D "method concordance" tab. Each row is one
(target, candidate) pair in v2's top-10, enriched with v1's energy-free
and prior-free ``signature_axis_distance`` from the uncapped ranked view, so the
viewer can place each method's rank-1 pick in a shared distance space and draw
the distance between them.

Why the uncapped ranked view for the v1 join: v2 re-ranks the full pool, so a
v2 rank-1 pair can sit outside v1's top-10 wrapper; the ranked view (the same
pool v2 reads) carries ``signature_axis_distance`` for every pair.

``signature_axis_distance`` is the post-IV damage fingerprint distance using
only measured axes (collapse/gate/Vds), excluding both energy and path priors.
``damage_signature_distance`` and ``combined_screening_distance`` are kept as
contaminated controls because they include path and energy terms respectively.

Usage:
    python3 data_processing_scripts/export_proxy_method_concordance_csv.py
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
OUTPUT_CSV = OUT_DIR / "proxy_method_concordance.csv"

ARRAY_COLUMNS = (
    "energy_v2_blockers",
    "energy_v2_notes",
    "proxy_claim_blockers",
    "proxy_claim_blockers_v1",
    "v1_proxy_claim_blockers",
)

QUERY = """
    SELECT
        v2.target_stress_record_key,
        v2.candidate_stress_record_key,
        v2.device_type,
        v2.target_event_type,
        v2.candidate_source,
        v2.match_scope,
        v2.candidate_rank_v1                      AS v1_rank,
        v2.mechanistic_energy_candidate_rank      AS v2_rank,
        v2.mechanistic_energy_candidate_status,
        v2.proxy_claim_status,
        v2.proxy_claim_basis,
        v2.proxy_claim_blockers,
        v2.proxy_claim_summary,
        v2.truth_validation_status,
        v2.truth_label,
        v2.truth_label_basis,
        v2.proxy_claim_status_v1,
        v2.proxy_claim_basis_v1,
        v2.proxy_claim_blockers_v1,
        v2.proxy_claim_summary_v1,
        v2.decision_safe_rank_v1,
        v2.signature_claim_quality_v1,
        v2.candidate_failure_fraction_overlap_class,
        v2.critical_severity_overlap_class_kosier_context,
        v2.target_severity_point_ratio,
        v2.candidate_failure_fraction_point,
        v2.candidate_severity_point_ratio_kosier_context,
        v2.energy_v2_blockers,
        v2.energy_v2_notes,
        -- v1 side, from the uncapped pool v2 reads from:
        r.signature_axis_distance,                -- energy-free + prior-free comparator
        r.damage_signature_distance,              -- path-prior contaminated control
        r.combined_screening_distance,            -- energy-blended contaminated control
        r.log_energy_delta,                       -- terminal-energy mismatch (natural log / nats; NOT log10)
        r.log_energy_delta_dex,                   -- same mismatch, converted to log10/dex for display
        r.damage_signature_evidence_class,        -- v1 confidence (measured anchor?)
        r.signature_claim_quality                 AS v1_signature_claim_quality,
        r.target_energy_comparability_class       AS v1_target_energy_comparability_class,
        r.candidate_energy_comparability_class    AS v1_candidate_energy_comparability_class,
        r.decision_safe_rank                      AS v1_decision_safe_rank,
        r.proxy_claim_status                      AS v1_proxy_claim_status,
        r.proxy_claim_basis                       AS v1_proxy_claim_basis,
        r.proxy_claim_blockers                    AS v1_proxy_claim_blockers,
        r.proxy_claim_summary                     AS v1_proxy_claim_summary,
        r.candidate_status                        AS v1_candidate_status
    FROM stress_proxy_candidate_energy_v2 v2
    LEFT JOIN stress_proxy_candidate_ranked_view r
        ON r.target_stress_record_key = v2.target_stress_record_key
       AND r.candidate_stress_record_key = v2.candidate_stress_record_key
    WHERE v2.mechanistic_energy_candidate_rank <= %s
    ORDER BY v2.device_type, v2.target_stress_record_key,
             v2.mechanistic_energy_candidate_rank
"""


def _flatten_array(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def export(conn, out_path: Path = OUTPUT_CSV, top_n: int = 10) -> Path:
    """Write v2's top-``top_n`` candidates per target, enriched with v1 distance."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, (top_n,))
        rows = [dict(r) for r in cur.fetchall()]
    df = pd.DataFrame(rows)
    for col in ARRAY_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_flatten_array)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=OUTPUT_CSV)
    ap.add_argument("--top-n", type=int, default=10,
                    help="Keep v2's top-N candidates per target (default 10, so "
                         "the v1 rank-1 pick can be found within the pool).")
    args = ap.parse_args()
    with get_connection() as conn:
        path = export(conn, out_path=args.out, top_n=args.top_n)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
