#!/usr/bin/env python3
"""Export v1 (damage-signature) vs v2 (energy) proxy concordance to CSV.

Feeds the interactive viewer's 3D "method concordance" tab. Each row is one
(target, candidate) pair in v2's top-10, enriched with v1's *energy-free*
``damage_signature_distance`` from the uncapped ranked view, so the viewer can
place each method's rank-1 pick in a shared distance space and draw the distance
between them.

Why the uncapped ranked view for the v1 join: v2 re-ranks the full pool, so a
v2 rank-1 pair can sit outside v1's top-10 wrapper; the ranked view (the same
pool v2 reads) carries ``damage_signature_distance`` for every pair.

``damage_signature_distance`` is the post-IV damage fingerprint distance
(collapse/gate/Vds axes + path) and excludes energy — that is what makes it an
*independent* comparator to v2's energy ranking. ``combined_screening_distance``
is kept too, but it blends in an energy term, so it is the contaminated control.

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

ARRAY_COLUMNS = ("energy_v2_blockers",)

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
        v2.critical_severity_overlap_class,
        v2.target_severity_point_ratio,
        v2.candidate_severity_point_ratio,
        v2.energy_v2_blockers,
        -- v1 side, from the uncapped pool v2 reads from:
        r.damage_signature_distance,              -- energy-FREE (independent axis)
        r.combined_screening_distance,            -- energy-blended (contaminated control)
        r.log_energy_delta,                       -- terminal-energy mismatch (log10)
        r.damage_signature_evidence_class,        -- v1 confidence (measured anchor?)
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
