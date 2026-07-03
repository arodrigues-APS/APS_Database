#!/usr/bin/env python3
"""Auto-seed quarantined proxy truth labels from measured post-IV matches.

The generated labels are intentionally *not* human truth.  They use
``label_basis = 'measured_post_iv_auto'`` and ``reviewer = 'auto_seed'`` so the
v2 calibrator quarantines them from headline truth-hit metrics.  Existing rows
are never overwritten; a human-curated ``proxy_truth_labels`` row wins by
primary-key conflict.

Usage:
    python3 data_processing_scripts/seed_proxy_truth_labels_auto.py
    python3 data_processing_scripts/seed_proxy_truth_labels_auto.py --dry-run
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

from psycopg2.extras import RealDictCursor

try:
    from db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - package import path
    from data_processing_scripts.db_config import get_connection

AUTO_LABEL_BASIS = "measured_post_iv_auto"
AUTO_REVIEWER = "auto_seed"
AUTO_LABEL = "equivalent"
AUTO_NOTE = (
    "Auto-seeded from same-device exact-condition measured post-IV "
    "comparability; quarantined from headline calibration metrics."
)


@dataclass(frozen=True)
class SeedResult:
    eligible_rows: int
    inserted_rows: int


def build_seed_sql(dry_run: bool = False) -> str:
    """Return the idempotent auto-label seeding SQL."""
    insert_cte = """
    inserted AS (
        INSERT INTO proxy_truth_labels (
            target_stress_record_key,
            candidate_stress_record_key,
            label,
            label_basis,
            reviewer,
            review_date,
            notes
        )
        SELECT
            e.target_stress_record_key,
            e.candidate_stress_record_key,
            %(label)s,
            %(label_basis)s,
            %(reviewer)s,
            CURRENT_DATE,
            %(notes)s
        FROM eligible e
        ON CONFLICT (target_stress_record_key, candidate_stress_record_key)
            DO NOTHING
        RETURNING 1
    )
    """
    dry_cte = """
    inserted AS (
        SELECT NULL::integer WHERE FALSE
    )
    """
    return f"""
WITH eligible AS (
    SELECT DISTINCT
        r.target_stress_record_key,
        r.candidate_stress_record_key
    FROM stress_proxy_candidate_ranked_view r
    WHERE r.match_scope = 'same_device'
      AND r.damage_evidence_tier = 'measured_damage'
      AND r.measured_match_scope = 'exact_condition'
      AND r.measured_comparability_status IN ('strong', 'usable')
      AND COALESCE(r.measured_sign_mismatch_axis_count, 0) = 0
),
{dry_cte if dry_run else insert_cte}
SELECT
    (SELECT COUNT(*) FROM eligible) AS eligible_rows,
    (SELECT COUNT(*) FROM inserted) AS inserted_rows
"""


def seed_auto_labels(conn: Any, dry_run: bool = False) -> SeedResult:
    params = {
        "label": AUTO_LABEL,
        "label_basis": AUTO_LABEL_BASIS,
        "reviewer": AUTO_REVIEWER,
        "notes": AUTO_NOTE,
    }
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(build_seed_sql(dry_run=dry_run), params)
        row = dict(cur.fetchone())
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    return SeedResult(
        eligible_rows=int(row.get("eligible_rows") or 0),
        inserted_rows=int(row.get("inserted_rows") or 0),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count eligible rows without inserting labels.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with get_connection() as conn:
        result = seed_auto_labels(conn, dry_run=args.dry_run)
    action = "Would insert" if args.dry_run else "Inserted"
    print(
        f"Eligible auto truth labels: {result.eligible_rows}; "
        f"{action.lower()} {result.inserted_rows}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
