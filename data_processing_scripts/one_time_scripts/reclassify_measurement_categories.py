#!/usr/bin/env python3
"""
Reclassify mislabeled measurement_category values in baselines_metadata.

The string-based classifier in common.categorize_measurement() operates on
the filename/measurement_type alone, so sweeps whose name matches the "IdVd"
regex but describe a Blocking or 3rd-quadrant test end up with the wrong
category.  This backfill mirrors common.refine_category_by_sweep() but runs
as a single SQL pass against already-ingested rows.

The thresholds must be kept in sync with common.py:
  Blocking     : vd_max ≥ 30 V AND max(|i_drain|) < 1 A
  3rd_Quadrant : vd_max ≤ 0.5 V AND vd_min < -0.1 V AND id_min < -1e-7 A

By default the script runs dry: counts affected rows per device_type without
modifying the DB.  Pass --apply to commit the changes.  Any existing
promotion_decision on reclassified rows is cleared so the promotion gate
re-adjudicates them against the correct category's extraction logic.

Usage:
    python3 reclassify_measurement_categories.py            # dry-run
    python3 reclassify_measurement_categories.py --apply
    python3 reclassify_measurement_categories.py --apply --data-source irradiation
"""

import argparse
import sys
from time import perf_counter

try:
    import psycopg2
except ImportError:
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


# Keep these in sync with common._REFINE_* constants.
BLOCKING_VD_MIN = 30.0
BLOCKING_ID_ABS = 1.0
Q3_VD_MAX       = 0.5
Q3_VD_MIN       = -0.1
Q3_ID_MIN       = -1e-7


PREVIEW_SQL = """
WITH r AS (
    SELECT metadata_id,
           MAX(v_drain)      AS vd_max,
           MIN(v_drain)      AS vd_min,
           MIN(i_drain)      AS id_min,
           MAX(ABS(i_drain)) AS id_abs_max
    FROM baselines_measurements
    GROUP BY metadata_id
)
SELECT md.data_source,
       md.device_type,
       COUNT(*) FILTER (WHERE r.vd_max >= %(bv)s
                          AND (r.id_abs_max IS NULL OR r.id_abs_max < %(bi)s))
           AS to_blocking,
       COUNT(*) FILTER (WHERE r.vd_max <= %(q3v)s
                          AND r.vd_min < %(q3m)s
                          AND r.id_min < %(q3i)s)
           AS to_3rd_quadrant,
       COUNT(*) AS total_idvd
FROM baselines_metadata md
JOIN r ON r.metadata_id = md.id
WHERE md.measurement_category = 'IdVd'
  {source_filter}
GROUP BY md.data_source, md.device_type
ORDER BY md.data_source, md.device_type
"""


APPLY_BLOCKING_SQL = """
WITH r AS (
    SELECT metadata_id,
           MAX(v_drain)      AS vd_max,
           MAX(ABS(i_drain)) AS id_abs_max
    FROM baselines_measurements
    GROUP BY metadata_id
)
UPDATE baselines_metadata md
SET measurement_category = 'Blocking',
    promotion_decision   = NULL,
    promotion_reason     = NULL,
    promotion_ts         = NULL,
    gate_params          = NULL
FROM r
WHERE r.metadata_id = md.id
  AND md.measurement_category = 'IdVd'
  AND r.vd_max >= %(bv)s
  AND (r.id_abs_max IS NULL OR r.id_abs_max < %(bi)s)
  {source_filter}
RETURNING md.id
"""


APPLY_Q3_SQL = """
WITH r AS (
    SELECT metadata_id,
           MAX(v_drain) AS vd_max,
           MIN(v_drain) AS vd_min,
           MIN(i_drain) AS id_min
    FROM baselines_measurements
    GROUP BY metadata_id
)
UPDATE baselines_metadata md
SET measurement_category = '3rd_Quadrant',
    promotion_decision   = NULL,
    promotion_reason     = NULL,
    promotion_ts         = NULL,
    gate_params          = NULL
FROM r
WHERE r.metadata_id = md.id
  AND md.measurement_category = 'IdVd'
  AND r.vd_max <= %(q3v)s
  AND r.vd_min < %(q3m)s
  AND r.id_min < %(q3i)s
  {source_filter}
RETURNING md.id
"""


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--apply", action="store_true",
                    help="Commit the reclassifications (default: dry-run)")
    ap.add_argument("--data-source", type=str, default=None,
                    choices=['baselines', 'irradiation', 'sc_ruggedness'],
                    help="Limit reclassification to a single data_source")
    args = ap.parse_args()

    src_clause = ""
    src_params = {}
    if args.data_source:
        src_clause = "AND md.data_source = %(ds)s"
        src_params = {'ds': args.data_source}

    params = {
        'bv': BLOCKING_VD_MIN, 'bi': BLOCKING_ID_ABS,
        'q3v': Q3_VD_MAX,      'q3m': Q3_VD_MIN, 'q3i': Q3_ID_MIN,
        **src_params,
    }

    print("=" * 72)
    print("Measurement-category backfill (IdVd → Blocking / 3rd_Quadrant)")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Rules:  Blocking     vd_max ≥ {BLOCKING_VD_MIN:g} V "
          f"AND |Id|_max < {BLOCKING_ID_ABS:g} A")
    print(f"        3rd_Quadrant vd_max ≤ {Q3_VD_MAX:g} V "
          f"AND vd_min < {Q3_VD_MIN:g} V "
          f"AND id_min < {Q3_ID_MIN:g} A")
    if args.data_source:
        print(f"Scope:  data_source = {args.data_source!r} only")
    print(f"Mode:   {'APPLY (commit changes)' if args.apply else 'DRY RUN'}")
    print("=" * 72)

    t0 = perf_counter()
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASSWORD)
    conn.autocommit = False
    cur = conn.cursor()

    # Preview
    cur.execute(PREVIEW_SQL.format(source_filter=src_clause), params)
    rows = cur.fetchall()

    print(f"\n{'data_source':<15} {'device_type':<24} "
          f"{'→Blocking':>10} {'→3rd_Q':>8} {'total_idvd':>12}")
    print("-" * 72)
    totals = {'to_blocking': 0, 'to_3rd': 0, 'total': 0}
    for ds, dt, to_b, to_q, tot in rows:
        if to_b == 0 and to_q == 0:
            continue  # skip device_types with no changes
        print(f"{str(ds):<15} {str(dt):<24} {to_b:>10} {to_q:>8} {tot:>12}")
        totals['to_blocking'] += to_b
        totals['to_3rd']      += to_q
        totals['total']       += tot
    print("-" * 72)
    print(f"{'TOTAL':<15} {'':<24} {totals['to_blocking']:>10} "
          f"{totals['to_3rd']:>8} {totals['total']:>12}")

    if not args.apply:
        print(f"\nDRY RUN — no changes committed.  "
              f"Re-run with --apply to persist.")
        print(f"Elapsed: {perf_counter() - t0:.2f}s")
        cur.close()
        conn.close()
        return

    # Apply Blocking first (the Blocking and 3rd_Q predicates are disjoint
    # on vd_max, so order is inconsequential — we commit in two steps for
    # clearer per-category counts in the audit log).
    cur.execute(APPLY_BLOCKING_SQL.format(source_filter=src_clause), params)
    n_blocking = cur.rowcount
    cur.execute(APPLY_Q3_SQL.format(source_filter=src_clause), params)
    n_q3 = cur.rowcount
    conn.commit()

    print(f"\nCommitted:")
    print(f"  {n_blocking} row(s) IdVd → Blocking")
    print(f"  {n_q3} row(s) IdVd → 3rd_Quadrant")
    print(f"  promotion_decision/reason/ts/gate_params cleared on all "
          f"reclassified rows so the gate re-adjudicates them.")
    print(f"\nElapsed: {perf_counter() - t0:.2f}s")
    print("=" * 72)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
