#!/usr/bin/env python3
"""
Ingestion parity fingerprint.

Captures a deterministic JSON summary of the database state produced by
the ingestion pipeline so each step of the Flask-first migration can be
verified against a pre-migration baseline.

    python scripts/ingestion_fingerprint.py capture [--out PATH] [--label STR]
    python scripts/ingestion_fingerprint.py diff FILE1 FILE2

The capture groups baselines_metadata by (data_source, device_type,
manufacturer, measurement_category) plus per-bucket MD5 of the sorted
csv_path list.  Phase 1 (device matching) is caught by shifts in
device_type/manufacturer; Phase 2 (parameter extraction) is caught by
promotion_decision, is_likely_irradiated, and gate_params coverage;
Phase 3 (logbook parsing) is caught by irrad_run_id assignment counts
and the irradiation_runs catalog.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "data_processing_scripts"))

from db_config import get_connection  # noqa: E402


FINGERPRINT_VERSION = 1


def _git_sha():
    try:
        return subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return None


def _rows_as_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def capture(conn):
    cur = conn.cursor()

    totals = {}
    for t in (
        "baselines_metadata",
        "baselines_measurements",
        "device_library",
        "irradiation_campaigns",
        "irradiation_runs",
        "experiment_campaign_map",
    ):
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        totals[t] = cur.fetchone()[0]

    # Per-bucket counts + csv_path identity hash.
    # COALESCE on nullable dimensions so NULLs group together rather than
    # splitting across buckets.
    cur.execute("""
        SELECT
            COALESCE(data_source, '<null>')          AS data_source,
            COALESCE(device_type, '<null>')          AS device_type,
            COALESCE(manufacturer, '<null>')         AS manufacturer,
            COALESCE(measurement_category, '<null>') AS measurement_category,
            COUNT(*)                                 AS count,
            MD5(STRING_AGG(COALESCE(csv_path, ''), '|' ORDER BY csv_path)) AS csv_path_hash
        FROM baselines_metadata
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 3, 4
    """)
    by_bucket = _rows_as_dicts(cur)

    # Flag distribution — catches FLAG_IRRADIATED_SQL threshold shifts.
    cur.execute("""
        SELECT
            COALESCE(data_source, '<null>') AS data_source,
            is_likely_irradiated,
            COUNT(*) AS count
        FROM baselines_metadata
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    by_flag = _rows_as_dicts(cur)

    # Promotion outcome — catches promote_to_baselines.py changes.
    cur.execute("""
        SELECT
            COALESCE(data_source, '<null>')          AS data_source,
            COALESCE(measurement_category, '<null>') AS measurement_category,
            COALESCE(promotion_decision, '<null>')   AS promotion_decision,
            COUNT(*) AS count
        FROM baselines_metadata
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    promotion = _rows_as_dicts(cur)

    # Irrad run / role assignment — catches logbook parsing and
    # experiment_campaign_map changes.
    cur.execute("""
        SELECT
            COALESCE(ic.campaign_name, '<unassigned>') AS campaign_name,
            COALESCE(ir.ion_species, '<no_run>')       AS ion_species,
            COALESCE(md.irrad_role, '<null>')          AS irrad_role,
            COUNT(*) AS count
        FROM baselines_metadata md
        LEFT JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
        LEFT JOIN irradiation_runs ir      ON md.irrad_run_id = ir.id
        WHERE md.data_source = 'irradiation'
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    irrad_assignments = _rows_as_dicts(cur)

    # gate_params coverage per category — catches extract_damage_metrics.py
    # behaviour.
    cur.execute("""
        SELECT
            COALESCE(data_source, '<null>')          AS data_source,
            COALESCE(measurement_category, '<null>') AS measurement_category,
            (gate_params IS NOT NULL)                AS has_gate_params,
            COUNT(*) AS count
        FROM baselines_metadata
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    gate_params_coverage = _rows_as_dicts(cur)

    # irradiation_runs catalog — catches seed_irradiation_campaigns.py and
    # (later) logbook-driven run creation.
    cur.execute("""
        SELECT
            ic.campaign_name,
            ir.ion_species,
            ir.beam_energy_mev,
            ir.let_surface,
            ir.let_bragg_peak,
            ir.range_um
        FROM irradiation_runs ir
        JOIN irradiation_campaigns ic ON ir.campaign_id = ic.id
        ORDER BY ic.campaign_name, ir.ion_species, ir.beam_energy_mev
    """)
    irradiation_runs_catalog = _rows_as_dicts(cur)

    # device_library catalog — so Phase 1 changes that re-seed the library
    # are explicit in the diff.
    cur.execute("""
        SELECT part_number, device_category, manufacturer,
               voltage_rating, rdson_mohm, current_rating_a, package_type
        FROM device_library
        ORDER BY part_number
    """)
    device_library = _rows_as_dicts(cur)

    cur.close()
    return {
        "version": FINGERPRINT_VERSION,
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_sha": _git_sha(),
        "totals": totals,
        "baselines_by_bucket": by_bucket,
        "baselines_by_flag": by_flag,
        "promotion_decisions": promotion,
        "irrad_assignments": irrad_assignments,
        "gate_params_coverage": gate_params_coverage,
        "irradiation_runs_catalog": irradiation_runs_catalog,
        "device_library": device_library,
    }


def _key_for(section, row):
    """Composite key identifying a row within a section for diff alignment."""
    if section == "baselines_by_bucket":
        return (row["data_source"], row["device_type"],
                row["manufacturer"], row["measurement_category"])
    if section == "baselines_by_flag":
        return (row["data_source"], row["is_likely_irradiated"])
    if section == "promotion_decisions":
        return (row["data_source"], row["measurement_category"],
                row["promotion_decision"])
    if section == "irrad_assignments":
        return (row["campaign_name"], row["ion_species"], row["irrad_role"])
    if section == "gate_params_coverage":
        return (row["data_source"], row["measurement_category"],
                row["has_gate_params"])
    if section == "irradiation_runs_catalog":
        return (row["campaign_name"], row["ion_species"],
                row["beam_energy_mev"])
    if section == "device_library":
        return (row["part_number"],)
    return tuple(sorted(row.items()))


def diff(before, after):
    """Print a human-readable diff between two fingerprints."""
    out = []

    # Totals first — the fastest signal.
    btot = before.get("totals", {})
    atot = after.get("totals", {})
    total_keys = sorted(set(btot) | set(atot))
    total_rows = [(k, btot.get(k), atot.get(k))
                  for k in total_keys if btot.get(k) != atot.get(k)]
    if total_rows:
        out.append("Totals changed:")
        for k, b, a in total_rows:
            out.append(f"  {k}: {b} -> {a}")
    else:
        out.append("Totals: unchanged.")

    # Per-section row-level diff.
    sections = [
        "baselines_by_bucket",
        "baselines_by_flag",
        "promotion_decisions",
        "irrad_assignments",
        "gate_params_coverage",
        "irradiation_runs_catalog",
        "device_library",
    ]
    for section in sections:
        b_rows = {_key_for(section, r): r for r in before.get(section, [])}
        a_rows = {_key_for(section, r): r for r in after.get(section, [])}
        added = sorted(a_rows.keys() - b_rows.keys())
        removed = sorted(b_rows.keys() - a_rows.keys())
        shared = sorted(a_rows.keys() & b_rows.keys())
        changed = [k for k in shared if a_rows[k] != b_rows[k]]
        if not (added or removed or changed):
            continue
        out.append(f"\n{section}:")
        for k in added:
            out.append(f"  + {k}: {a_rows[k]}")
        for k in removed:
            out.append(f"  - {k}: {b_rows[k]}")
        for k in changed:
            out.append(f"  ~ {k}:")
            for col in sorted(set(a_rows[k]) | set(b_rows[k])):
                bv, av = b_rows[k].get(col), a_rows[k].get(col)
                if bv != av:
                    out.append(f"      {col}: {bv} -> {av}")

    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cap = sub.add_parser("capture", help="write fingerprint JSON")
    p_cap.add_argument("--out", default=None,
                       help="output path (default: out/fingerprints/<label>_<timestamp>.json)")
    p_cap.add_argument("--label", default="snapshot",
                       help="label embedded in default filename")

    p_diff = sub.add_parser("diff", help="diff two fingerprint files")
    p_diff.add_argument("before")
    p_diff.add_argument("after")

    args = parser.parse_args()

    if args.cmd == "capture":
        conn = get_connection()
        try:
            fp = capture(conn)
        finally:
            conn.close()

        if args.out:
            out_path = Path(args.out)
        else:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            out_path = REPO_ROOT / "out" / "fingerprints" / f"{args.label}_{ts}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(fp, indent=2, sort_keys=False, default=str))

        print(f"Wrote {out_path}")
        print(f"  baselines_metadata rows:  {fp['totals']['baselines_metadata']}")
        print(f"  bucket groups:            {len(fp['baselines_by_bucket'])}")
        print(f"  irrad assignment groups:  {len(fp['irrad_assignments'])}")
        print(f"  irradiation runs:         {fp['totals']['irradiation_runs']}")
        return

    if args.cmd == "diff":
        before = json.loads(Path(args.before).read_text())
        after = json.loads(Path(args.after).read_text())
        result = diff(before, after)
        print(result)
        # Non-zero exit if anything differs beyond the trivial header.
        if result.strip() == "Totals: unchanged.":
            sys.exit(0)
        sys.exit(1)


if __name__ == "__main__":
    main()
