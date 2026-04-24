#!/usr/bin/env python3
"""
Extract Per-File Damage Metrics (Vth, Rds(on), V(BR)DSS, Vsd)
==============================================================
Runs the EXTRACT_PER_FILE_SQL from `promote_to_baselines.py` over ALL
rows in `baselines_metadata` (not just pristine-promotion candidates)
and stores {vth_v, rdson_mohm, bvdss_v, vsd_v} into the JSONB column
`gate_params`.

The promote_to_baselines.py gate is restricted to pre_irrad / pristine
files because that is what its IQR-based promotion rule needs.  For the
SC ↔ irradiation damage-equivalence analysis we need the same four
parameters extracted for post_sc and post_irrad files too, so that
Δ = post − pristine_population_median can be computed downstream.

Behaviour:
  * Idempotent — by default skips any row whose gate_params already
    contains ANY of the four parameter keys.  Newly ingested files are
    picked up automatically on the next run.
  * Merge semantics — existing keys in gate_params (e.g. the
    promotion-audit fields like vth_v_median) are preserved.  Only the
    four raw extracted values are overwritten/added.
  * --rebuild overwrites every row regardless of prior state.
  * --device-type restricts to a single part number for faster
    iteration during development.

Usage:
    python3 extract_damage_metrics.py                       # idempotent
    python3 extract_damage_metrics.py --rebuild             # force re-extract
    python3 extract_damage_metrics.py --device-type C2M0080120D
    python3 extract_damage_metrics.py --dry-run             # print counts only
"""

import argparse
import json
import sys
from time import perf_counter

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import Json

from db_config import get_connection
from promote_to_baselines import EXTRACT_PER_FILE_SQL


PARAM_KEYS = ("vth_v", "rdson_mohm", "bvdss_v", "vsd_v")


def fetch_extracted(cur, device_type=None, rebuild=False):
    """Run EXTRACT_PER_FILE_SQL with predicates chosen to cover all rows.

    * source_predicate = 'TRUE'  — don't restrict by data_source
    * decision_filter  = 'TRUE'  — don't exclude already-promoted rows
    * device_type_filter — optional narrowing for dev iteration

    When rebuild is False we still ask the DB for everything but then
    filter client-side against already-populated gate_params, because
    EXTRACT_PER_FILE_SQL doesn't accept a JSONB predicate without
    restructuring it.
    """
    extra = ""
    params = []
    if device_type:
        extra = "AND device_type = %s"
        params = [device_type]
    sql = EXTRACT_PER_FILE_SQL.format(
        source_predicate="TRUE",
        decision_filter="TRUE",
        device_type_filter=extra,
    )
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_existing_gate_params(cur, device_type=None):
    """Return {metadata_id: set_of_param_keys_already_populated}."""
    sql = """
        SELECT id, gate_params
        FROM baselines_metadata
        WHERE gate_params IS NOT NULL
    """
    params = []
    if device_type:
        sql += " AND device_type = %s"
        params.append(device_type)
    cur.execute(sql, params)
    out = {}
    for mid, gp in cur.fetchall():
        if gp is None:
            continue
        out[mid] = {k for k in PARAM_KEYS if gp.get(k) is not None}
    return out


def apply_extraction(cur, metadata_id, extracted):
    """Merge the four extracted parameters into gate_params.

    Uses jsonb || so existing keys (promotion audit fields) are kept.
    """
    payload = {
        k: float(extracted[k]) if extracted.get(k) is not None else None
        for k in PARAM_KEYS
    }
    # Strip nulls so we don't overwrite real values with None on partial extracts
    payload = {k: v for k, v in payload.items() if v is not None}
    if not payload:
        return False
    cur.execute(
        """
        UPDATE baselines_metadata
        SET gate_params = COALESCE(gate_params, '{}'::jsonb) || %s::jsonb
        WHERE id = %s
        """,
        (Json(payload), metadata_id),
    )
    return True


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--device-type", help="Restrict to one device_type")
    ap.add_argument("--rebuild", action="store_true",
                    help="Re-extract every row, overwriting existing values")
    ap.add_argument("--dry-run", action="store_true",
                    help="Only report counts, don't write")
    args = ap.parse_args()

    t0 = perf_counter()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            print("Extracting per-file parameters …", flush=True)
            rows = fetch_extracted(cur, device_type=args.device_type,
                                   rebuild=args.rebuild)
            print(f"  candidate files returned by extractor: {len(rows)}")

            existing = {} if args.rebuild else fetch_existing_gate_params(
                cur, device_type=args.device_type)

            updated = 0
            skipped_already_done = 0
            skipped_no_params = 0
            by_source = {"updated": {}, "skipped": {}}

            # Need data_source + irrad_role + test_condition for summary
            # (not returned by EXTRACT_PER_FILE_SQL) — fetch in one shot.
            cur.execute("""
                SELECT id, data_source, test_condition, irrad_role
                FROM baselines_metadata
            """)
            src = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

            for row in rows:
                mid = row["metadata_id"]
                extracted = {k: row.get(k) for k in PARAM_KEYS}
                if not any(v is not None for v in extracted.values()):
                    skipped_no_params += 1
                    continue
                if not args.rebuild:
                    have = existing.get(mid, set())
                    # Skip only if every extracted key is already populated
                    to_add = {k for k, v in extracted.items()
                              if v is not None and k not in have}
                    if not to_add:
                        skipped_already_done += 1
                        continue
                bucket_key = src.get(mid, (None, None, None))
                if not args.dry_run:
                    if apply_extraction(cur, mid, extracted):
                        updated += 1
                        by_source["updated"][bucket_key] = \
                            by_source["updated"].get(bucket_key, 0) + 1
                else:
                    updated += 1
                    by_source["updated"][bucket_key] = \
                        by_source["updated"].get(bucket_key, 0) + 1

            if args.dry_run:
                conn.rollback()
                print("\n-- DRY RUN: no writes committed --")
            else:
                conn.commit()

            elapsed = perf_counter() - t0
            print(f"\nSummary ({'dry-run' if args.dry_run else 'applied'})")
            print(f"  updated:               {updated}")
            print(f"  skipped (no params):   {skipped_no_params}")
            print(f"  skipped (already done):{skipped_already_done}")
            print(f"  elapsed: {elapsed:.1f} s")

            if by_source["updated"]:
                print("\nUpdated counts by (data_source, test_condition, irrad_role):")
                for k, v in sorted(by_source["updated"].items(),
                                   key=lambda kv: (str(kv[0][0]),
                                                   str(kv[0][1]),
                                                   str(kv[0][2]))):
                    print(f"  {k}: {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
