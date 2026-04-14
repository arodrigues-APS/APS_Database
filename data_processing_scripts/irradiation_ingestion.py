#!/usr/bin/env python3
"""
Irradiation Data Ingestion Script
===================================
Parses Keithley .txt measurement files from Measurements/Irradiation/
and loads them into the PostgreSQL database for the irradiation dashboard.

Each campaign folder under Irradiation/ is scanned for .txt files matching
the standard naming convention:
    {ChipID}_SN{nnn}_run{nnn}_{measurement_type}.txt

Supports three column formats produced by the Keithley instruments:
  - 4-col IV sweep:   Vg  Ig  Vd  Id   (or Vds Id Vgs Ig)
  - 3-col monitoring:  time  Vds  Ids
  - 7-col fluence:     time  Vds  Ids  Vgs  Igs  arduino_ms  fluence

Inserts into:
  - baselines_metadata     (data_source = 'irradiation')
  - baselines_measurements (FK to metadata)

Links each file to its irradiation_campaign via the campaign folder mapping.

Skipped campaigns (incompatible data formats):
  - 08_PADOVA 2022         — C-V impedance data (Agilent E4990A), not IV
  - 2022_21_11_Padova_Xrays — logbook only, no measurement files

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 irradiation_ingestion.py [--dry-run] [--rebuild]
"""

import os
import re
import sys
import argparse
from pathlib import Path
from time import perf_counter

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import execute_values

from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATA_ROOT
from common import (load_device_library, compute_file_hash, categorize_measurement)


# ── Irradiation root ────────────────────────────────────────────────────────
IRRADIATION_ROOT = os.path.join(DATA_ROOT, "Measurements", "Irradiation")

# ── Campaign folder configs ─────────────────────────────────────────────────
# Each entry maps a filesystem folder name to:
#   campaign_name:  must match irradiation_campaigns.campaign_name
#   data_subdirs:   list of relative subdirectory globs to scan for .txt files
#                   (None means scan the entire folder recursively)
#   facility, ion_species, beam_energy_mev, beam_type: campaign metadata
#   notes:          human-readable description

CAMPAIGN_CONFIGS = [
    {
        "folder": "GSIMarch2025Au",
        "campaign_name": "GSI_March_2025_Au",
        "facility": "GSI Darmstadt",
        "ion_species": "Au",
        "beam_energy_mev": 1162.0,
        "beam_type": "broad_beam",
        "notes": "Gold ions at 1162 MeV, March 2025 campaign at GSI",
    },
    {
        "folder": "2022_30_08_PSI",
        "campaign_name": "PSI_Proton_2022",
        "facility": "PSI Villigen",
        "ion_species": "proton",
        "beam_energy_mev": None,
        "beam_type": "broad_beam",
        "notes": "Proton irradiation, August 2022 at PSI",
    },
    {
        "folder": "2023_03_08_UCL_ions",
        "campaign_name": "UCL_Ions_2023",
        "facility": "UCL",
        "ion_species": "Fe",
        "beam_energy_mev": None,
        "beam_type": "broad_beam",
        "notes": "Heavy ion irradiation, March 2023 at UCL",
    },
    {
        "folder": "2022_01_06_GSI_Ca",
        "campaign_name": "GSI_Ca_2022",
        "facility": "GSI Darmstadt",
        "ion_species": "Ca",
        "beam_energy_mev": None,
        "beam_type": "broad_beam",
        "notes": "Calcium ions, January 2022 campaign at GSI",
    },
]


# ── Known device chip-ID normalization ──────────────────────────────────────
# Maps raw chip IDs found in filenames to (part_number, device_category,
# manufacturer).  Extends the mapping from gsi_march2025_mapping.py to cover
# all campaigns.

CHIP_ID_TO_DEVICE = {
    # Wolfspeed Gen 3 MOSFETs (1200V, 75mΩ)
    "CPM312000075A": ("CPM3-1200-0075A", "MOSFET", "Wolfspeed"),
    "CPM3120075A":   ("CPM3-1200-0075A", "MOSFET", "Wolfspeed"),
    "CM312000075A":  ("CPM3-1200-0075A", "MOSFET", "Wolfspeed"),

    # Wolfspeed Gen 2 MOSFETs (1200V, 80mΩ)
    "CPM212000080A": ("CPM2-1200-0080A", "MOSFET", "Wolfspeed"),

    # Wolfspeed packaged MOSFETs
    "C2M0080120D": ("C2M0080120D", "MOSFET", "Wolfspeed"),
    "C2M0025120D": ("C2M0025120D", "MOSFET", "Wolfspeed"),
    "C3M0075120D": ("C3M0075120D", "MOSFET", "Wolfspeed"),

    # Wolfspeed Diodes
    "CPW51700Z050A":  ("CPW5-1700-Z050A", "Diode", "Wolfspeed"),
    "CPW517000Z050B": ("CPW5-1700-Z050B", "Diode", "Wolfspeed"),
    "CPW412000010B":  ("CPW4-1200-S010B", "Diode", "Wolfspeed"),
    "CPW41200010B":   ("CPW4-1200-S010B", "Diode", "Wolfspeed"),
    "CPW4-1200-010B": ("CPW4-1200-S010B", "Diode", "Wolfspeed"),
    "CPW412000020B":  ("CPW4-1200-S020B", "Diode", "Wolfspeed"),
    "CPW41200S020B":  ("CPW4-1200-S020B", "Diode", "Wolfspeed"),

    # Infineon
    "IFX Trench":     ("IFX-Trench", "MOSFET", "Infineon"),
    "IFX Trnech":     ("IFX-Trench", "MOSFET", "Infineon"),
    "IFXDiode3x3":    ("IFX-Diode-3x3", "Diode", "Infineon"),
    "IFX Diode 3x3":  ("IFX-Diode-3x3", "Diode", "Infineon"),
    "12M1H090":       ("12M1H090", "MOSFET", "Infineon"),

    # Rohm
    "SCT3030KL": ("SCT3030KL", "MOSFET", "Rohm"),

    # Infineon Trench (UCL naming)
    "Trench": ("IFX-Trench", "MOSFET", "Infineon"),

    # VU reference
    "VU_MOSFET_uncoated": ("VU-MOSFET-uncoated", "MOSFET", "VU"),

    # Diode generic names (GSI Ca data)
    "diode":  (None, "Diode", None),
    "Diode":  (None, "Diode", None),
}


def extract_chip_id(filename):
    """
    Extract chip ID from a Keithley filename.

    Pattern: {ChipID}_SN{nnn}_run{nnn}_{type}.txt
    Examples:
        CPM312000075A_SN001_run119_irrad.txt  -> CPM312000075A
        IFX Trench_SN003_run050_IDVGfwd.txt   -> IFX Trench
        Trench_SN001_run039_irrad.txt         -> Trench
        C2M0080120D_SN004_run006_irrad.txt    -> C2M0080120D
    """
    stem = Path(filename).stem
    m = re.match(r'^(.+?)_SN\d{3}_run\d+.*$', stem)
    if m:
        return m.group(1)
    return None


def extract_measurement_type(filename):
    """
    Extract the measurement type suffix from a Keithley filename.

    Pattern: {ChipID}_SN{nnn}_run{nnn}_{type}.txt
    Examples:
        CPM312000075A_SN001_run000_IDVGfwd.txt -> IDVGfwd
        C2M0080120D_SN004_run006_irrad.txt     -> irrad
        CM312000075A_SN001_run002_irrad.txt    -> irrad
    """
    stem = Path(filename).stem
    m = re.search(r'_run\d+_(.+)$', stem)
    if m:
        return m.group(1)
    return None


def extract_device_id_from_path(filepath, campaign_folder):
    """
    Extract a device identifier (board/DUT or subfolder-based) from the
    file path relative to the campaign folder.

    For GSIMarch2025Au:   B1/DUT1/file.txt  -> B1D1
    For PSI:              Cree_80mOhm/file.txt -> Cree_80mOhm
    For UCL:              .../Cree/25mOhm/B8_D2/file.txt -> B8_D2
    For GSI Ca:           .../80_mOhm/B18_D1/file.txt -> B18_D1
    """
    p = Path(filepath)
    parts = p.parts

    # Try to find Board/DUT pattern in path
    for i, part in enumerate(parts):
        # B{n}/DUT{n} pattern (GSI March 2025)
        if re.match(r'^B\d', part) and i + 1 < len(parts):
            next_part = parts[i + 1]
            if re.match(r'^DUT\d', next_part, re.IGNORECASE):
                board = part
                dut_num = re.search(r'DUT(\d+)', next_part, re.IGNORECASE).group(1)
                return f"{board}D{dut_num}"

        # B{n}_D{n} pattern (UCL, GSI Ca)
        m = re.match(r'^B(\d+)_D(\d+)$', part)
        if m:
            return part

    # Fall back to parent directory name if it looks like a device grouping
    parent = p.parent.name
    if parent and parent not in ('all_data', 'All_data_Day1', 'DATA',
                                  campaign_folder, 'data'):
        return parent

    # Last resort: use chip ID from filename
    chip_id = extract_chip_id(p.name)
    if chip_id:
        # Try to get SN from filename for uniqueness
        m = re.search(r'_SN(\d{3})', p.stem)
        if m:
            return f"{chip_id}_SN{m.group(1)}"
    return chip_id or "unknown"


def normalize_chip_id(raw_chip_id):
    """Normalize a raw chip ID to a device_library part number."""
    if raw_chip_id in CHIP_ID_TO_DEVICE:
        return CHIP_ID_TO_DEVICE[raw_chip_id]
    return (raw_chip_id, None, None)


def parse_keithley_txt(filepath):
    """
    Parse a Keithley .txt measurement file.

    Returns:
        (headers, data_rows, header_metadata)
    where header_metadata is a dict with keys extracted from the file header:
        date, nplc_drain, nplc_gate, gate_voltage, drain_voltage
    """
    header_meta = {}
    headers = None
    data_rows = []

    try:
        with open(filepath, 'r', errors='replace') as f:
            lines = f.readlines()
    except Exception:
        return None, None, {}

    if not lines:
        return None, None, {}

    data_start = None
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # Parse header fields
        if i == 0 and not line_stripped[0].isdigit() and not line_stripped.startswith('-'):
            header_meta['date'] = line_stripped
            continue

        m_nplc = re.match(r'NPLC[_ ](?:ke\d+|DRAIN|GATE)\s*=\s*([\d.]+)', line_stripped, re.IGNORECASE)
        if m_nplc:
            if 'drain' in line_stripped.lower() or '2636' in line_stripped:
                header_meta['nplc_drain'] = float(m_nplc.group(1))
            if 'gate' in line_stripped.lower() or '2410' in line_stripped:
                header_meta['nplc_gate'] = float(m_nplc.group(1))
            continue

        m_gv = re.match(r'Gate voltage\s*=\s*([\d.eE+-]+)', line_stripped, re.IGNORECASE)
        if m_gv:
            header_meta['gate_voltage'] = float(m_gv.group(1))
            continue

        m_dv = re.match(r'Drain voltage\s*=\s*([\d.eE+-]+)', line_stripped, re.IGNORECASE)
        if m_dv:
            header_meta['drain_voltage'] = float(m_dv.group(1))
            continue

        # Skip separator lines
        if line_stripped.startswith('*'):
            continue

        # Column header line — contains letters and not just numbers
        if headers is None and re.search(r'[a-zA-Z]', line_stripped):
            # Check if this looks like a column header (tab-separated words)
            parts = re.split(r'\t+', line_stripped)
            if len(parts) >= 2 and any(re.search(r'[VvIi]', p) for p in parts):
                headers = [p.strip() for p in parts]
                data_start = i + 1
                continue

        # Data lines (tab-separated numbers)
        if headers is not None and i >= data_start:
            parts = re.split(r'\t+', line_stripped)
            if len(parts) >= 2:
                row = []
                valid = True
                for val_str in parts:
                    val_str = val_str.strip()
                    if not val_str:
                        row.append(None)
                        continue
                    try:
                        row.append(float(val_str))
                    except ValueError:
                        valid = False
                        break
                if valid and row:
                    data_rows.append(row)

    return headers, data_rows, header_meta


def map_irrad_columns(headers, row):
    """
    Map Keithley .txt columns to the standard measurement schema.

    Handles the column naming variants across campaigns:
      - Vg / Vgs / Vd / Vds  (gate/drain voltage)
      - Ig / Igs / Id / Ids  (gate/drain current)
      - time                 (time column)
      - fluence, arduino_ms  (extra columns, stored as metadata)

    Returns dict with keys: v_gate, i_gate, v_drain, i_drain, time_val
    """
    result = {
        'v_gate': None, 'i_gate': None,
        'v_drain': None, 'i_drain': None,
        'time_val': None,
    }

    for i, h in enumerate(headers):
        if i >= len(row):
            break
        val = row[i]
        hl = h.lower().strip()

        if hl in ('vg', 'vgs'):
            result['v_gate'] = val
        elif hl in ('ig', 'igs'):
            result['i_gate'] = val
        elif hl in ('vd', 'vds'):
            result['v_drain'] = val
        elif hl in ('id', 'ids'):
            result['i_drain'] = val
        elif hl in ('time', 'time_val', 't'):
            result['time_val'] = val

    return result


def find_txt_files(campaign_folder_path):
    """
    Recursively find all .txt files in a campaign folder that look like
    Keithley measurement data (match the naming convention).

    Skips non-measurement files (scripts, readmes, logbooks, etc.)
    """
    txt_files = []
    skip_dirs = {'Script', 'script', 'Scripts', 'png', 'Degradation_rates',
                 '__pycache__', '.cache', 'ProjectMedia',
                 'NI Project Data', 'Automation Examples',
                 'Getting Started Workbook', 'Tasks',
                 'CAMAC_CaMay22', 'CNAFS',
                 'Other_data_from_GSI', 'Corinna',
                 'Script_we_used_GSI_2024'}

    for root, dirs, files in os.walk(campaign_folder_path):
        # Prune skipped directories
        dirs[:] = [d for d in dirs if d not in skip_dirs]

        for f in sorted(files):
            if not f.lower().endswith('.txt'):
                continue
            # Must match Keithley naming convention
            if not re.match(r'.+_SN\d{3}_run\d+', f):
                continue
            # Skip known non-data files
            if f in ('How to run.txt',):
                continue
            txt_files.append(os.path.join(root, f))

    return txt_files


def ensure_campaign_exists(cur, config):
    """
    Ensure the irradiation campaign exists in the database.
    Returns the campaign id.
    """
    cur.execute(
        "SELECT id FROM irradiation_campaigns WHERE campaign_name = %s",
        (config["campaign_name"],)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO irradiation_campaigns
            (campaign_name, facility, ion_species, beam_energy_mev,
             beam_type, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        config["campaign_name"],
        config.get("facility"),
        config["ion_species"],
        config.get("beam_energy_mev"),
        config.get("beam_type"),
        config.get("notes"),
    ))
    return cur.fetchone()[0]


def ensure_data_source_column(cur):
    """Add data_source column to baselines_metadata if it doesn't exist."""
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE baselines_metadata
                ADD COLUMN data_source TEXT DEFAULT 'baselines';
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_baselines_meta_data_source
            ON baselines_metadata(data_source);
    """)


def match_device_from_library(chip_id, device_library):
    """
    Try to match a chip ID against the device library entries.
    Returns (device_type, manufacturer) or (None, None).
    """
    if not chip_id:
        return None, None

    # First try the hardcoded mapping
    part_number, category, manufacturer = normalize_chip_id(chip_id)
    if part_number and part_number != chip_id:
        return part_number, manufacturer

    # Then try substring match against device_library (longest match first)
    chip_upper = chip_id.upper()
    for dev in device_library:
        pn = dev['part_number']
        if pn and pn.upper() in chip_upper or chip_upper in pn.upper():
            return pn, dev.get('manufacturer')

    # Return the normalized chip ID even if not in library
    if part_number:
        return part_number, manufacturer
    return chip_id, None


def ingest_campaign(cur, conn, config, device_library, dry_run=False):
    """
    Ingest all measurement files from one irradiation campaign folder.

    Returns (files_loaded, files_skipped, files_error, total_points).
    """
    folder_path = os.path.join(IRRADIATION_ROOT, config["folder"])
    campaign_name = config["campaign_name"]

    if not os.path.isdir(folder_path):
        print(f"  WARNING: folder not found: {folder_path}")
        return 0, 0, 0, 0

    # Ensure campaign exists in DB
    if not dry_run:
        campaign_id = ensure_campaign_exists(cur, config)
    else:
        cur.execute(
            "SELECT id FROM irradiation_campaigns WHERE campaign_name = %s",
            (campaign_name,)
        )
        row = cur.fetchone()
        campaign_id = row[0] if row else None

    # Find all measurement .txt files
    txt_files = find_txt_files(folder_path)
    print(f"  Found {len(txt_files)} measurement files in {config['folder']}")

    if not txt_files:
        return 0, 0, 0, 0

    files_loaded = 0
    files_skipped = 0
    files_error = 0
    total_points = 0

    for idx, fpath in enumerate(txt_files):
        filename = os.path.basename(fpath)

        # Extract metadata from filename
        raw_chip_id = extract_chip_id(filename)
        measurement_type = extract_measurement_type(filename)
        measurement_category = categorize_measurement(measurement_type, filename)
        device_id = extract_device_id_from_path(fpath, config["folder"])

        # Resolve device type from chip ID
        device_type, manufacturer = match_device_from_library(
            raw_chip_id, device_library
        )

        # File hash for dedup
        file_hash = compute_file_hash(fpath)

        # Check if already loaded
        cur.execute("SELECT id FROM baselines_metadata WHERE file_hash = %s",
                    (file_hash,))
        if cur.fetchone():
            files_skipped += 1
            continue

        # Parse the file
        headers, data_rows, header_meta = parse_keithley_txt(fpath)

        if not headers or not data_rows:
            if not dry_run:
                print(f"    SKIP (empty/unparseable): {filename}")
            files_skipped += 1
            continue

        if dry_run:
            # In dry-run mode, just report what would be ingested
            files_loaded += 1
            total_points += len(data_rows)
            if (idx + 1) % 20 == 0 or idx == 0:
                print(f"    [{idx+1}/{len(txt_files)}] "
                      f"{filename}: {len(data_rows)} pts, "
                      f"chip={raw_chip_id}, type={measurement_type}, "
                      f"device={device_type}")
            continue

        # Determine irrad_role from measurement type
        irrad_role = 'post_irrad'  # default for irradiation data
        if measurement_type and re.search(r'irrad', measurement_type.lower()):
            irrad_role = 'post_irrad'
        elif measurement_type and measurement_type.lower().startswith('idv'):
            # IDVGfwd/IDVDfwd with run000 are typically pre-irrad characterization
            m_run = re.search(r'_run(\d+)_', filename)
            if m_run and int(m_run.group(1)) == 0:
                irrad_role = 'pre_irrad'
            else:
                irrad_role = 'post_irrad'

        # Extract drain bias from header for IdVg measurements
        drain_bias_value = header_meta.get('drain_voltage')

        # Insert metadata
        try:
            cur.execute("""
                INSERT INTO baselines_metadata (
                    experiment, device_id, measurement_type,
                    measurement_category, filename, csv_path,
                    columns, num_points,
                    drain_bias_value,
                    file_hash, device_type, manufacturer,
                    data_source, irrad_campaign_id, irrad_role
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s, %s
                ) RETURNING id
            """, (
                config["folder"], device_id, measurement_type,
                measurement_category, filename, fpath,
                ','.join(headers), len(data_rows),
                drain_bias_value,
                file_hash, device_type, manufacturer,
                'irradiation', campaign_id, irrad_role,
            ))
            meta_id = cur.fetchone()[0]
        except Exception as e:
            print(f"    ERROR (metadata): {filename}: {e}")
            conn.rollback()
            files_error += 1
            continue

        # Map columns and build measurement batch
        batch = []
        for point_idx, row in enumerate(data_rows):
            mapped = map_irrad_columns(headers, row)
            batch.append((
                meta_id, point_idx,
                mapped['v_gate'], mapped['i_gate'],
                mapped['v_drain'], mapped['i_drain'],
                None, None,  # rds, bv
                mapped['time_val'],
                0,  # step_index
            ))

        if batch:
            try:
                execute_values(cur, """
                    INSERT INTO baselines_measurements
                    (metadata_id, point_index, v_gate, i_gate, v_drain,
                     i_drain, rds, bv, time_val, step_index)
                    VALUES %s
                """, batch, page_size=5000)

                total_points += len(batch)
                files_loaded += 1

                if (idx + 1) % 50 == 0:
                    print(f"    [{idx+1}/{len(txt_files)}] "
                          f"{files_loaded} loaded, {total_points} pts "
                          f"({filename})")

            except Exception as e:
                print(f"    ERROR (data): {filename}: {e}")
                conn.rollback()
                files_error += 1
                continue

        # Commit every 50 files
        if files_loaded % 50 == 0 and files_loaded > 0:
            conn.commit()

    conn.commit()
    return files_loaded, files_skipped, files_error, total_points


def main():
    parser = argparse.ArgumentParser(
        description="Ingest irradiation measurement data from "
                    "Measurements/Irradiation/ into the database."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Scan and parse files but don't modify the DB")
    parser.add_argument("--rebuild", action="store_true",
                        help="Delete all irradiation records and re-ingest")
    parser.add_argument("--campaign", type=str, default=None,
                        help="Only ingest a specific campaign folder name")
    args = parser.parse_args()

    print("=" * 70)
    print("Irradiation Data Ingestion")
    print(f"Source: {IRRADIATION_ROOT}")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    if args.dry_run:
        print("MODE: DRY RUN (no database changes)")
    print("=" * 70)

    if not os.path.isdir(IRRADIATION_ROOT):
        print(f"\nERROR: Irradiation root not found: {IRRADIATION_ROOT}")
        sys.exit(1)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Ensure schema is ready
    if not args.dry_run:
        ensure_data_source_column(cur)
        conn.commit()

    # Optionally rebuild
    if args.rebuild and not args.dry_run:
        print("\nRebuilding: deleting all irradiation records...")
        cur.execute(
            "DELETE FROM baselines_metadata WHERE data_source = 'irradiation'"
        )
        deleted = cur.rowcount
        conn.commit()
        print(f"  Deleted {deleted} metadata records "
              "(CASCADE removes measurements).")

    # Load device library for matching
    device_library = load_device_library(cur)
    print(f"\nDevice library: {len(device_library)} entries")

    # Filter campaigns if --campaign specified
    campaigns = CAMPAIGN_CONFIGS
    if args.campaign:
        campaigns = [c for c in campaigns if c["folder"] == args.campaign]
        if not campaigns:
            print(f"\nERROR: Unknown campaign folder: {args.campaign}")
            print("Available folders:",
                  ", ".join(c["folder"] for c in CAMPAIGN_CONFIGS))
            sys.exit(1)

    # Ingest each campaign
    t0 = perf_counter()
    grand_loaded = 0
    grand_skipped = 0
    grand_error = 0
    grand_points = 0

    for config in campaigns:
        print(f"\n{'─' * 60}")
        print(f"Campaign: {config['campaign_name']} ({config['folder']})")
        print(f"{'─' * 60}")

        loaded, skipped, errors, points = ingest_campaign(
            cur, conn, config, device_library, dry_run=args.dry_run
        )

        grand_loaded += loaded
        grand_skipped += skipped
        grand_error += errors
        grand_points += points

        print(f"  Result: {loaded} loaded, {skipped} skipped, "
              f"{errors} errors, {points} points")

    elapsed = perf_counter() - t0

    # Summary
    print(f"\n{'=' * 70}")
    print("INGESTION COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Files loaded:  {grand_loaded}")
    print(f"  Files skipped: {grand_skipped} (already in DB or empty)")
    print(f"  Files errored: {grand_error}")
    print(f"  Total points:  {grand_points}")
    print(f"  Elapsed:       {elapsed:.1f}s")

    if not args.dry_run and grand_loaded > 0:
        # Show per-campaign counts
        print(f"\n  Per-campaign summary:")
        cur.execute("""
            SELECT ic.campaign_name, md.irrad_role,
                   COUNT(DISTINCT md.id) AS n_files,
                   SUM(md.num_points) AS n_points
            FROM baselines_metadata md
            JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
            WHERE md.data_source = 'irradiation'
            GROUP BY ic.campaign_name, md.irrad_role
            ORDER BY ic.campaign_name, md.irrad_role
        """)
        for campaign_name, role, n_files, n_points in cur.fetchall():
            print(f"    {campaign_name} / {role}: "
                  f"{n_files} files, {n_points} points")

    cur.close()
    conn.close()

    print(f"\n{'=' * 70}")
    if args.dry_run:
        print("DRY RUN complete — no changes were made.")
    else:
        print("Done!")
    print("=" * 70)


if __name__ == "__main__":
    main()
