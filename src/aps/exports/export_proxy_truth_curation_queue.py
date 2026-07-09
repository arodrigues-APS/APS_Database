#!/usr/bin/env python3
"""Export a proxy-truth curation queue.

The queue is intentionally fail-closed: it does not declare new proxy truth.  It
selects candidate pairs that have enough evidence to deserve human review, adds
the current v1/v2 claim interpretation, and writes rows that can be adjudicated
into ``proxy_truth_labels``.

Usage:
    python3 -m aps.exports.export_proxy_truth_curation_queue
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from psycopg2.extras import RealDictCursor

try:
    from aps.db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - package import path
    from aps.db_config import get_connection

from aps.paths import OUT_ROOT

OUT_DIR = OUT_ROOT / "mechanistic_energy_proxy_calibration"
OUTPUT_CSV = OUT_DIR / "proxy_truth_curation_queue.csv"

ARRAY_COLUMNS = (
    "proxy_claim_blockers",
    "proxy_claim_blockers_v1",
    "energy_v2_blockers",
    "energy_v2_notes",
)

QUEUE_COLUMNS = [
    "curation_priority",
    "curation_priority_reason",
    "suggested_label_basis",
    "target_stress_record_key",
    "candidate_stress_record_key",
    "device_type",
    "target_event_type",
    "target_ion_species",
    "candidate_source",
    "match_scope",
    "candidate_rank_v1",
    "mechanistic_energy_candidate_rank",
    "candidate_status_v1",
    "mechanistic_energy_candidate_status",
    "proxy_claim_status_v1",
    "proxy_claim_status",
    "proxy_claim_basis",
    "decision_safe_rank_v1",
    "truth_validation_status",
    "truth_label",
    "truth_label_basis",
    "truth_reviewer",
    "truth_review_date",
    "signature_claim_quality_v1",
    "damage_evidence_class",
    "candidate_failure_fraction_overlap_class",
    "terminal_energy_overlap_class",
    "measured_sign_mismatch_axis_count",
    "prediction_sign_mismatch_axis_count",
    "target_energy_comparability_class",
    "candidate_energy_comparability_class",
    "proxy_claim_blockers_v1",
    "energy_v2_blockers",
    "energy_v2_notes",
    "proxy_claim_blockers",
    "proxy_claim_summary",
]

QUERY = """
WITH pool AS (
    SELECT
        v2.*,
        CASE
            WHEN v2.truth_label IS NULL
             AND EXISTS (
                SELECT 1
                FROM stress_proxy_candidate_energy_v2 v1_pick
                WHERE v1_pick.target_stress_record_key = v2.target_stress_record_key
                  AND v1_pick.candidate_rank_v1 = 1
                  AND v1_pick.mechanistic_energy_candidate_rank BETWEEN 2 AND 10
             )
                THEN 1
            WHEN v2.proxy_claim_status IN ('validation_candidate', 'curation_candidate')
             AND v2.truth_label IS NULL
                THEN 2
            WHEN v2.truth_validation_status = 'curated_uncertain'
                THEN 3
            WHEN v2.proxy_claim_status = 'validated'
                THEN 4
            WHEN v2.proxy_claim_status = 'blocked'
                THEN 5
            WHEN v2.match_scope = 'same_device'
             AND v2.damage_evidence_class = 'measured_damage'
                THEN 6
            WHEN v2.mechanistic_energy_candidate_rank = 1
                THEN 7
            ELSE 9
        END AS curation_priority,
        CASE
            WHEN v2.truth_label IS NULL
             AND EXISTS (
                SELECT 1
                FROM stress_proxy_candidate_energy_v2 v1_pick
                WHERE v1_pick.target_stress_record_key = v2.target_stress_record_key
                  AND v1_pick.candidate_rank_v1 = 1
                  AND v1_pick.mechanistic_energy_candidate_rank BETWEEN 2 AND 10
             )
                THEN 'mild_v1_v2_disagreement_v1_pick_inside_v2_top10'
            WHEN v2.proxy_claim_status IN ('validation_candidate', 'curation_candidate')
             AND v2.truth_label IS NULL
                THEN 'needs_truth_label_for_claim_candidate'
            WHEN v2.truth_validation_status = 'curated_uncertain'
                THEN 'resolve_uncertain_truth_label'
            WHEN v2.proxy_claim_status = 'validated'
                THEN 'validated_pair_audit'
            WHEN v2.proxy_claim_status = 'blocked'
                THEN 'blocked_candidate_audit'
            WHEN v2.match_scope = 'same_device'
             AND v2.damage_evidence_class = 'measured_damage'
                THEN 'same_device_measured_damage_anchor'
            WHEN v2.mechanistic_energy_candidate_rank = 1
                THEN 'rank1_screening_only_audit'
            ELSE 'screening_context'
        END AS curation_priority_reason,
        CASE
            WHEN v2.damage_evidence_class = 'measured_damage'
             AND v2.match_scope = 'same_device'
                THEN 'measured_post_iv'
            WHEN v2.damage_evidence_class = 'measured_damage'
                THEN 'measured_post_iv_cross_device_screening'
            WHEN v2.damage_evidence_class = 'predicted_damage'
                THEN 'predicted_post_iv_not_validation'
            ELSE 'manual_review_required'
        END AS suggested_label_basis
    FROM stress_proxy_candidate_energy_v2 v2
    WHERE v2.mechanistic_energy_candidate_rank <= %s
), queued AS (
    SELECT *
    FROM pool
    WHERE curation_priority < 9
       OR proxy_claim_status IN ('validation_candidate', 'curation_candidate', 'blocked', 'validated')
)
SELECT
    curation_priority,
    curation_priority_reason,
    suggested_label_basis,
    target_stress_record_key,
    candidate_stress_record_key,
    device_type,
    target_event_type,
    target_ion_species,
    candidate_source,
    match_scope,
    candidate_rank_v1,
    mechanistic_energy_candidate_rank,
    candidate_status_v1,
    mechanistic_energy_candidate_status,
    proxy_claim_status_v1,
    proxy_claim_status,
    proxy_claim_basis,
    decision_safe_rank_v1,
    truth_validation_status,
    truth_label,
    truth_label_basis,
    truth_reviewer,
    truth_review_date,
    signature_claim_quality_v1,
    damage_evidence_class,
    candidate_failure_fraction_overlap_class,
    terminal_energy_overlap_class,
    measured_sign_mismatch_axis_count,
    prediction_sign_mismatch_axis_count,
    target_energy_comparability_class,
    candidate_energy_comparability_class,
    proxy_claim_blockers_v1,
    energy_v2_blockers,
    energy_v2_notes,
    proxy_claim_blockers,
    proxy_claim_summary
FROM queued
ORDER BY
    curation_priority,
    device_type,
    target_stress_record_key,
    mechanistic_energy_candidate_rank,
    candidate_rank_v1
LIMIT %s
"""


def _flatten_array(value: object) -> str:
    """Postgres text[] (psycopg2 -> list) -> '; '-joined string ('' for NULL)."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def export(conn, out_path: Path = OUTPUT_CSV, top_n: int = 10,
           row_limit: int = 2000) -> Path:
    """Write candidate pairs that should be reviewed into ``proxy_truth_labels``."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, (top_n, row_limit))
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows, columns=QUEUE_COLUMNS)
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
                    help="Read v2's top-N candidates per target (default 10).")
    ap.add_argument("--row-limit", type=int, default=2000,
                    help="Maximum queue rows to export (default 2000).")
    args = ap.parse_args()
    with get_connection() as conn:
        path = export(conn, out_path=args.out, top_n=args.top_n,
                      row_limit=args.row_limit)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
