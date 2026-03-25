#!/usr/bin/env python3
"""
GSI March 2025 Au Irradiation — Unified Device Mapping
Single use for ingesting Natalija's data/mapping
========================================================

Builds the mapping between:
  - Pristine pre-characterization files  (MOSFET_preCharact_GSI_March25/data/)
  - GSI irradiation data                 (GSIMarch2025Au/B*/DUT*/)
  - device_library part numbers           (PostgreSQL)

The pristine files use shorthand like "B3D1" but don't encode the chip ID.
The GSI folder embeds the chip ID in filenames (e.g. CPM312000075A_SN001_run119_irrad.txt).
This script bridges the two.

Outputs:
  1. gsi_march2025_mapping.csv       — human-readable mapping table
  2. Seeds any missing devices into device_library
  3. Updates baselines_metadata.device_type for already-ingested GSI March 2025 files

Usage:
    python3 gsi_march2025_mapping.py [--dry-run]
"""

import os
import re
import csv
import sys
import argparse
from pathlib import Path
from collections import defaultdict

try:
    import psycopg2
except ImportError:
    psycopg2 = None

# ── Paths ────────────────────────────────────────────────────────────────────
GSI_ROOT = "/home/arodrigues/APS_Database/GSIMarch2025Au"
PRISTINE_ROOT = "/home/arodrigues/APS_Database/Pristine measurements"
PRISTINE_PRECHARACT = os.path.join(PRISTINE_ROOT, "MOSFET_preCharact_GSI_March25", "data")
PRISTINE_BLOCKING = os.path.join(PRISTINE_ROOT, "MOSFET_Blocking_preCharact_GSI_March25", "data")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "gsi_march2025_mapping.csv")

# ── DB Connection ────────────────────────────────────────────────────────────
DB_HOST = "localhost"
DB_PORT = 5435
DB_NAME = "mosfets"
DB_USER = "postgres"
DB_PASSWORD = "APSLab"


# ── Chip ID Normalization ────────────────────────────────────────────────────
#
# The GSI folder uses raw chip IDs with inconsistent formatting.
# The device_library uses the official Wolfspeed/Infineon hyphenated format.
#
# Wolfspeed naming convention:
#   CPM{gen}-{voltage}-{rdson}{suffix}  (MOSFETs)
#   CPW{gen}-{voltage}-{spec}{suffix}   (Diodes)
#
# Examples:
#   CPM312000075A  ->  CPM3-1200-0075A   (Gen 3, 1200V, 75mΩ)
#   CPW51700Z050A  ->  CPW5-1700-Z050A   (Gen 5, 1700V, Z050 spec)

# Map from raw filename chip ID -> (normalized_part_number, device_category, manufacturer, voltage, rdson/current, package, notes)
RAW_TO_DEVICE = {
    # ── Wolfspeed Gen 3 MOSFETs ──
    "CPM312000075A":  ("CPM3-1200-0075A", "MOSFET", "Wolfspeed", "1200 V", "75", None, "bare_die", None),
    "CPM3120075A":    ("CPM3-1200-0075A", "MOSFET", "Wolfspeed", "1200 V", "75", None, "bare_die", None),
    "CM312000075A":   ("CPM3-1200-0075A", "MOSFET", "Wolfspeed", "1200 V", "75", None, "bare_die",
                       "Raw ID missing 'P'; confirmed CPM3-1200-0075A"),

    # ── Wolfspeed Gen 2 MOSFETs ──
    "CPM212000080A":  ("CPM2-1200-0080A", "MOSFET", "Wolfspeed", "1200 V", "80", None, "bare_die", None),

    # ── Wolfspeed Diodes (1700V) ──
    "CPW51700Z050A":  ("CPW5-1700-Z050A", "Diode", "Wolfspeed", "1700 V", None, "50", "bare_die", None),
    "CPW517000Z050B": ("CPW5-1700-Z050B", "Diode", "Wolfspeed", "1700 V", None, "50", "bare_die",
                       "Extra '0' in raw ID; confirmed CPW5-1700-Z050B"),

    # ── Wolfspeed Diodes (1200V) ──
    "CPW412000010B":  ("CPW4-1200-S010B", "Diode", "Wolfspeed", "1200 V", None, "33", "bare_die", None),
    "CPW41200010B":   ("CPW4-1200-S010B", "Diode", "Wolfspeed", "1200 V", None, "33", "bare_die", None),
    "CPW4-1200-010B": ("CPW4-1200-S010B", "Diode", "Wolfspeed", "1200 V", None, "33", "bare_die",
                       "Missing 'S' in raw ID"),
    "CPW412000020B":  ("CPW4-1200-S020B", "Diode", "Wolfspeed", "1200 V", None, "91", "bare_die", None),

    # ── Infineon Trench MOSFETs ──
    "IFX Trench":     ("IFX-Trench", "MOSFET", "Infineon", None, None, None, "bare_die",
                       "Infineon SiC trench MOSFET bare die"),
    "IFX Trnech":     ("IFX-Trench", "MOSFET", "Infineon", None, None, None, "bare_die",
                       "Typo in raw data ('Trnech' -> 'Trench')"),

    # ── Infineon Diodes ──
    "IFXDiode3x3":    ("IFX-Diode-3x3", "Diode", "Infineon", None, None, None, "bare_die",
                       "Infineon SiC diode, 3x3mm bare die"),
    "IFX Diode 3x3":  ("IFX-Diode-3x3", "Diode", "Infineon", None, None, None, "bare_die",
                       "Infineon SiC diode, 3x3mm bare die"),

    # ── VU MOSFET (reference) ──
    "VU_MOSFET_uncoated": ("VU-MOSFET-uncoated", "MOSFET", "VU", None, None, None, "bare_die",
                           "Uncoated reference MOSFET from VU (Vrije Universiteit)"),
}


def extract_chip_id_from_filename(filename):
    """
    Extract the raw chip ID from a GSI data filename.
    E.g. 'CPM312000075A_SN001_run119_irrad.txt' -> 'CPM312000075A'
         'IFX Trench_SN001_run030_irrad.txt'     -> 'IFX Trench'
    """
    stem = Path(filename).stem
    # Pattern: {ChipID}_SN{nnn}_run{nnn}_{type}
    m = re.match(r'^(.+?)_SN\d{3}_run\d+_.+$', stem)
    if m:
        return m.group(1)
    return None


def scan_gsi_folder():
    """
    Walk the GSI irradiation folder and build a Board/DUT -> chip ID mapping.
    Returns dict: {(board, dut): set_of_raw_chip_ids}
    Also returns list of all (board, dut, raw_chip_id, filename, run_number, meas_type) tuples.
    """
    mapping = defaultdict(set)
    all_files = []

    for board_dir in sorted(Path(GSI_ROOT).iterdir()):
        if not board_dir.is_dir():
            continue
        board_name = board_dir.name
        # Skip non-board directories
        if not re.match(r'^B\d', board_name):
            continue

        for dut_dir in sorted(board_dir.iterdir()):
            if not dut_dir.is_dir():
                continue
            dut_name = dut_dir.name
            if not re.match(r'^DUT\d', dut_name, re.IGNORECASE):
                # Handle special subdirs like "Irradiations without bias"
                # Scan recursively
                for txt_file in dut_dir.rglob("*.txt"):
                    chip_id = extract_chip_id_from_filename(txt_file.name)
                    if chip_id:
                        # Try to determine parent DUT from path
                        all_files.append((board_name, dut_name, chip_id, txt_file.name, None, None))
                continue

            dut_num = re.search(r'DUT(\d+)', dut_name, re.IGNORECASE).group(1)
            dut_key = f"DUT{dut_num}"

            for txt_file in sorted(dut_dir.glob("*.txt")):
                chip_id = extract_chip_id_from_filename(txt_file.name)
                if chip_id:
                    mapping[(board_name, dut_key)].add(chip_id)

                    # Extract run number and measurement type
                    stem = txt_file.stem
                    run_match = re.search(r'run(\d+)', stem)
                    run_num = int(run_match.group(1)) if run_match else None
                    meas_match = re.search(r'run\d+_(.+)$', stem)
                    meas_type = meas_match.group(1) if meas_match else None

                    all_files.append((board_name, dut_key, chip_id, txt_file.name, run_num, meas_type))

    return mapping, all_files


def scan_pristine_files():
    """
    Scan the pristine measurement CSV files and extract (board, dut) identifiers.
    Returns list of (device_id, board, dut, filename, measurement_type, experiment) tuples.
    """
    results = []

    for data_dir, experiment in [
        (PRISTINE_PRECHARACT, "MOSFET_preCharact_GSI_March25"),
        (PRISTINE_BLOCKING, "MOSFET_Blocking_preCharact_GSI_March25"),
    ]:
        if not os.path.isdir(data_dir):
            continue
        for f in sorted(os.listdir(data_dir)):
            if not f.lower().endswith('.csv'):
                continue
            stem = Path(f).stem

            # Parse B##D# pattern
            m = re.match(r'^(B\d+(?:_\d+)?)(D)(\d+)_(.+)$', stem)
            if m:
                board = m.group(1)
                dut_num = m.group(3)
                meas_type = m.group(4)
                device_id = f"{board}D{dut_num}"
                results.append((device_id, board, f"DUT{dut_num}", f, meas_type, experiment))
            elif stem.startswith("test_"):
                results.append(("test", "test", "test", f, stem[5:], experiment))

    return results


def normalize_chip_id(raw_id):
    """Normalize a raw chip ID to a device_library part number."""
    if raw_id in RAW_TO_DEVICE:
        return RAW_TO_DEVICE[raw_id][0]
    return raw_id


def get_device_info(raw_id):
    """Get full device info tuple for a raw chip ID."""
    if raw_id in RAW_TO_DEVICE:
        return RAW_TO_DEVICE[raw_id]
    return (raw_id, None, None, None, None, None, None, None)


def build_unified_mapping():
    """Build the complete mapping table."""
    print("=" * 80)
    print("GSI March 2025 Au — Unified Device Mapping")
    print("=" * 80)

    # Step 1: Scan GSI folder
    print("\n1. Scanning GSI irradiation folder...")
    gsi_mapping, gsi_files = scan_gsi_folder()
    print(f"   Found {len(gsi_mapping)} board/DUT combinations, {len(gsi_files)} files")

    # Step 2: Scan pristine files
    print("\n2. Scanning pristine measurement files...")
    pristine_files = scan_pristine_files()
    print(f"   Found {len(pristine_files)} pristine measurement files")

    # Step 3: Build the unified mapping
    print("\n3. Building unified mapping...")

    # Normalize board names: pristine uses "B3" but GSI might use "B3" or "B3point2"
    # Map pristine board names to GSI board names
    pristine_boards = set()
    for _, board, dut, _, _, _ in pristine_files:
        if board != "test":
            pristine_boards.add((board, dut))

    gsi_boards = set(gsi_mapping.keys())

    # Build the mapping rows
    rows = []
    unmapped_pristine = []

    for device_id, board, dut, filename, meas_type, experiment in pristine_files:
        if board == "test":
            rows.append({
                'device_id': device_id,
                'board': board,
                'dut': dut,
                'raw_chip_id': None,
                'part_number': None,
                'device_category': None,
                'manufacturer': None,
                'voltage_rating': None,
                'rdson_mohm': None,
                'current_rating_a': None,
                'package_type': None,
                'pristine_file': filename,
                'measurement_type': meas_type,
                'experiment': experiment,
                'n_irrad_runs': 0,
                'irrad_run_range': None,
                'notes': 'Test device — no chip ID mapping available',
            })
            continue

        key = (board, dut)
        if key in gsi_mapping:
            raw_ids = gsi_mapping[key]
            # Use the most common / primary chip ID (skip typos)
            primary_raw = sorted(raw_ids, key=lambda x: -len([
                f for f in gsi_files
                if f[0] == board and f[1] == dut and f[2] == x
            ]))[0]

            info = get_device_info(primary_raw)
            part_number = info[0]

            # Count irradiation runs for this board/DUT
            irrad_runs = [f for f in gsi_files
                          if f[0] == board and f[1] == dut and f[5] == 'irrad']
            run_nums = sorted([f[4] for f in irrad_runs if f[4] is not None])
            run_range = f"{min(run_nums)}-{max(run_nums)}" if run_nums else None

            rows.append({
                'device_id': device_id,
                'board': board,
                'dut': dut,
                'raw_chip_id': primary_raw,
                'part_number': part_number,
                'device_category': info[1],
                'manufacturer': info[2],
                'voltage_rating': info[3],
                'rdson_mohm': info[4],
                'current_rating_a': info[5],
                'package_type': info[6],
                'pristine_file': filename,
                'measurement_type': meas_type,
                'experiment': experiment,
                'n_irrad_runs': len(irrad_runs),
                'irrad_run_range': run_range,
                'notes': info[7],
            })
        else:
            unmapped_pristine.append((device_id, board, dut, filename, experiment))
            rows.append({
                'device_id': device_id,
                'board': board,
                'dut': dut,
                'raw_chip_id': None,
                'part_number': None,
                'device_category': None,
                'manufacturer': None,
                'voltage_rating': None,
                'rdson_mohm': None,
                'current_rating_a': None,
                'package_type': None,
                'pristine_file': filename,
                'measurement_type': meas_type,
                'experiment': experiment,
                'n_irrad_runs': 0,
                'irrad_run_range': None,
                'notes': f'Board {board} not found in GSI folder — chip ID unknown',
            })

    # Also include GSI board/DUT combinations that DON'T have pristine data
    pristine_keys = set((b, d) for _, b, d, _, _, _ in pristine_files if b != "test")
    for (board, dut), raw_ids in sorted(gsi_mapping.items()):
        if (board, dut) in pristine_keys:
            continue
        primary_raw = sorted(raw_ids, key=lambda x: -len([
            f for f in gsi_files
            if f[0] == board and f[1] == dut and f[2] == x
        ]))[0]
        info = get_device_info(primary_raw)

        irrad_runs = [f for f in gsi_files
                      if f[0] == board and f[1] == dut and f[5] == 'irrad']
        run_nums = sorted([f[4] for f in irrad_runs if f[4] is not None])
        run_range = f"{min(run_nums)}-{max(run_nums)}" if run_nums else None

        device_id = f"{board}{dut.replace('UT', '')}"  # e.g. B1D1

        rows.append({
            'device_id': device_id,
            'board': board,
            'dut': dut,
            'raw_chip_id': primary_raw,
            'part_number': info[0],
            'device_category': info[1],
            'manufacturer': info[2],
            'voltage_rating': info[3],
            'rdson_mohm': info[4],
            'current_rating_a': info[5],
            'package_type': info[6],
            'pristine_file': None,
            'measurement_type': None,
            'experiment': 'GSIMarch2025Au',
            'n_irrad_runs': len(irrad_runs),
            'irrad_run_range': run_range,
            'notes': f'Irradiation only — no pristine pre-characterization data; {info[7] or ""}',
        })

    return rows, unmapped_pristine, gsi_mapping, gsi_files


def write_csv(rows, output_path):
    """Write the mapping table to CSV."""
    fieldnames = [
        'device_id', 'board', 'dut', 'raw_chip_id', 'part_number',
        'device_category', 'manufacturer', 'voltage_rating',
        'rdson_mohm', 'current_rating_a', 'package_type',
        'pristine_file', 'measurement_type', 'experiment',
        'n_irrad_runs', 'irrad_run_range', 'notes',
    ]
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n   Written to: {output_path}")


def seed_missing_devices(rows, dry_run=False):
    """Add any new devices to device_library that aren't already there."""
    if psycopg2 is None:
        print("\n   psycopg2 not available — skipping DB operations")
        return

    # Collect unique part numbers and their info
    new_devices = {}
    for row in rows:
        pn = row['part_number']
        if pn and pn not in new_devices:
            new_devices[pn] = (
                pn,
                row['device_category'],
                row['manufacturer'],
                row['voltage_rating'],
                row['rdson_mohm'],
                row['current_rating_a'],
                row['package_type'],
                row['notes'],
            )

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
        conn.autocommit = False
        cur = conn.cursor()

        # Check which devices already exist
        cur.execute("SELECT part_number FROM device_library")
        existing = set(r[0] for r in cur.fetchall())

        to_insert = {pn: info for pn, info in new_devices.items() if pn not in existing}

        if not to_insert:
            print("   All devices already in device_library.")
        else:
            print(f"\n   New devices to add to device_library ({len(to_insert)}):")
            for pn, info in sorted(to_insert.items()):
                print(f"     {pn:<25s}  {info[1] or '?':>8s}  {info[2] or '?'}")

            if not dry_run:
                for pn, info in to_insert.items():
                    cur.execute("""
                        INSERT INTO device_library
                            (part_number, device_category, manufacturer,
                             voltage_rating, rdson_mohm, current_rating_a,
                             package_type, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (part_number) DO NOTHING
                    """, info)
                conn.commit()
                print(f"   Inserted {len(to_insert)} new devices.")
            else:
                print("   (dry run — no changes made)")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"   DB error: {e}")


def update_baselines_metadata(rows, dry_run=False):
    """
    Update device_type and manufacturer in baselines_metadata for
    GSI March 2025 experiment files that were previously ingested
    without device mapping.
    """
    if psycopg2 is None:
        print("\n   psycopg2 not available — skipping DB operations")
        return

    # Build device_id -> (part_number, manufacturer) lookup
    device_map = {}
    for row in rows:
        did = row['device_id']
        pn = row['part_number']
        mfr = row['manufacturer']
        if did and pn:
            device_map[did] = (pn, mfr)

    if not device_map:
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
        conn.autocommit = False
        cur = conn.cursor()

        # Find GSI March 2025 records missing device_type
        cur.execute("""
            SELECT id, device_id, experiment
            FROM baselines_metadata
            WHERE experiment IN (
                'MOSFET_preCharact_GSI_March25',
                'MOSFET_Blocking_preCharact_GSI_March25'
            )
            AND (device_type IS NULL OR device_type = '')
        """)
        records = cur.fetchall()

        updated = 0
        for rec_id, device_id, experiment in records:
            if device_id in device_map:
                pn, mfr = device_map[device_id]
                if not dry_run:
                    cur.execute("""
                        UPDATE baselines_metadata
                        SET device_type = %s, manufacturer = %s
                        WHERE id = %s
                    """, (pn, mfr, rec_id))
                updated += 1

        if updated:
            if not dry_run:
                conn.commit()
            print(f"   Updated {updated} baselines_metadata records with device_type.")
            if dry_run:
                print("   (dry run — no changes made)")
        else:
            print("   No baselines_metadata records to update.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"   DB error: {e}")


def print_summary(rows, unmapped, gsi_mapping, gsi_files):
    """Print a human-readable summary."""
    print("\n" + "=" * 80)
    print("UNIFIED DEVICE MAPPING — GSI March 2025 Au (1162 MeV)")
    print("=" * 80)

    # Group by board/DUT for summary
    board_dut_map = {}
    for row in rows:
        key = (row['board'], row['dut'])
        if key not in board_dut_map:
            board_dut_map[key] = row

    # Print compact summary table
    print(f"\n{'Board':<12s} {'DUT':<6s} {'Part Number':<25s} {'Category':<10s} "
          f"{'Manufacturer':<14s} {'Pristine':<10s} {'Irrad Runs':<12s}")
    print("-" * 95)

    last_board = None
    for key in sorted(board_dut_map.keys()):
        row = board_dut_map[key]
        board = row['board']
        if board != last_board and last_board is not None:
            print()
        last_board = board

        has_pristine = "Yes" if row['pristine_file'] else "No"
        irrad_info = row['irrad_run_range'] or '-'
        pn = row['part_number'] or '???'
        cat = row['device_category'] or '?'
        mfr = row['manufacturer'] or '?'

        print(f"{board:<12s} {row['dut']:<6s} {pn:<25s} {cat:<10s} "
              f"{mfr:<14s} {has_pristine:<10s} {irrad_info}")

    # Print unmapped warnings
    if unmapped:
        print(f"\n{'!'*60}")
        print(f"WARNING: {len(unmapped)} pristine files have no GSI board mapping:")
        for device_id, board, dut, filename, experiment in unmapped:
            print(f"  {device_id:<10s} ({filename})")
        print("  -> Ask the person who recorded the data for the chip IDs")
        print(f"{'!'*60}")

    # Print device library seed summary
    unique_parts = set()
    for row in rows:
        if row['part_number']:
            unique_parts.add(row['part_number'])
    print(f"\nUnique device part numbers: {len(unique_parts)}")
    for pn in sorted(unique_parts):
        print(f"  {pn}")

    # Print file count summary
    n_pristine = sum(1 for r in rows if r['pristine_file'])
    n_irrad_only = sum(1 for r in rows if not r['pristine_file'])
    print(f"\nPristine measurement files: {n_pristine}")
    print(f"Irradiation-only entries:   {n_irrad_only}")
    print(f"Total mapping rows:         {len(rows)}")


def main():
    parser = argparse.ArgumentParser(description="GSI March 2025 Au — Unified Device Mapping")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't modify the database, just show what would happen")
    args = parser.parse_args()

    rows, unmapped, gsi_mapping, gsi_files = build_unified_mapping()

    # Write CSV
    print("\n4. Writing CSV mapping table...")
    write_csv(rows, OUTPUT_CSV)

    # Seed device library
    print("\n5. Checking device_library for missing devices...")
    seed_missing_devices(rows, dry_run=args.dry_run)

    # Update baselines_metadata
    print("\n6. Updating baselines_metadata device types...")
    update_baselines_metadata(rows, dry_run=args.dry_run)

    # Print summary
    print_summary(rows, unmapped, gsi_mapping, gsi_files)

    print("\n" + "=" * 80)
    print("Done!")
    print(f"CSV mapping: {OUTPUT_CSV}")
    print("=" * 80)


if __name__ == "__main__":
    main()
