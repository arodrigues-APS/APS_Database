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
    python3 ingestion_irradiation.py [--dry-run] [--rebuild]
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
from common import (apply_schema,
                    load_device_library, load_device_mapping_rules, match_device,
                    compute_file_hash, categorize_measurement,
                    sweep_stats, refine_category_by_sweep)


# ── Irradiation root ────────────────────────────────────────────────────────
IRRADIATION_ROOT = os.path.join(DATA_ROOT, "Measurements", "Irradiation")

# ── Campaign discovery ──────────────────────────────────────────────────────
# Campaigns are managed via the Flask /irradiation UI.  Each campaign row has
# a folder_name column linking it to a subdirectory of Measurements/Irradiation/.
# This script discovers campaigns from the DB rather than hardcoding them.

def load_campaigns_from_db(cur):
    """
    Load campaigns that have a folder_name assigned from the database.
    Returns list of dicts with keys: id, campaign_name, folder_name.
    """
    # Ensure folder_name column exists (idempotent)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE irradiation_campaigns ADD COLUMN folder_name TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        SELECT id, campaign_name, folder_name
        FROM irradiation_campaigns
        WHERE folder_name IS NOT NULL AND folder_name != ''
        ORDER BY campaign_name
    """)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def discover_unmapped_folders(cur):
    """
    List Irradiation subfolders that exist on disk but have no campaign
    with a matching folder_name in the DB.
    """
    if not os.path.isdir(IRRADIATION_ROOT):
        return []
    all_folders = set(
        d for d in os.listdir(IRRADIATION_ROOT)
        if os.path.isdir(os.path.join(IRRADIATION_ROOT, d))
        and not d.startswith('.')
    )
    cur.execute("""
        SELECT folder_name FROM irradiation_campaigns
        WHERE folder_name IS NOT NULL AND folder_name != ''
    """)
    mapped = set(r[0] for r in cur.fetchall())
    return sorted(all_folders - mapped)


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


def _normalise_header(header):
    return header.lower().strip().replace(" ", "_")


def _assign_mapped_value(result, header, val):
    hl = _normalise_header(header)

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
    elif hl in ('fluence', 'fluence_cm2', 'fluence_per_cm2'):
        result['fluence'] = val


def _is_fluence_monitor_header(headers):
    labels = [_normalise_header(h) for h in headers]
    return (
        len(labels) >= 7
        and labels[0] in ('time', 'time_val', 't')
        and labels[1] in ('vd', 'vds')
        and labels[2] in ('id', 'ids')
        and labels[3] in ('vg', 'vgs')
        and labels[4] in ('ig', 'igs')
        and labels[-2] in ('arduino_ms', 'arduinoms')
        and labels[-1] in ('fluence', 'fluence_cm2', 'fluence_per_cm2')
    )


def map_irrad_columns(headers, row):
    """
    Map Keithley .txt columns to the standard measurement schema.

    Handles the column naming variants across campaigns:
      - Vg / Vgs / Vd / Vds  (gate/drain voltage)
      - Ig / Igs / Id / Ids  (gate/drain current)
      - time                 (time column)
      - fluence              (cumulative fluence at this sample, ions/cm² —
                              present in 7-col monitoring files only)

    Returns dict with keys: v_gate, i_gate, v_drain, i_drain, time_val,
    fluence.
    """
    result = {
        'v_gate': None, 'i_gate': None,
        'v_drain': None, 'i_drain': None,
        'time_val': None,
        'fluence': None,
    }

    if _is_fluence_monitor_header(headers) and len(row) == 5:
        # Some HIRFL/RADEF logs drop Vgs/Igs after the header and continue as:
        # time, Vds, Ids, arduino_ms, fluence.
        compact_headers = [
            headers[0], headers[1], headers[2], headers[-2], headers[-1]
        ]
        for i, h in enumerate(compact_headers):
            _assign_mapped_value(result, h, row[i])
        return result

    for i, h in enumerate(headers):
        if i >= len(row):
            break
        val = row[i]
        _assign_mapped_value(result, h, val)

    return result


def find_txt_files(campaign_folder_path):
    """
    Recursively find all .txt files in a campaign folder that look like
    Keithley measurement data (match the naming convention).

    Skips non-measurement files (scripts, readmes, logbooks, etc.)
    """
    txt_files = []
    skip_dirs = {'Script', 'script', 'Scripts', 'png', 'Degradation_rates',
                 '__pycache__', '.cache', 'ProjectMedia', 'Images',
                 'NI Project Data', 'Automation Examples',
                 'Getting Started Workbook', 'Tasks',
                 'CAMAC_CaMay22', 'CNAFS',
                 'Other_data_from_GSI', 'Corinna',
                 'Script_we_used_GSI_2024',
                 'ANSTO_Microscope_Images', 'Data_analysis_paper',
                 'DLTS&MCTS CPW4 B4'}

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


def get_campaign_id(cur, campaign):
    """
    Look up the campaign id from the database.
    campaign is a dict with at least 'id' key.
    """
    return campaign["id"]


def ensure_data_source_column(cur):
    """Add irradiation-specific columns to baselines_metadata /
    baselines_measurements if they don't already exist.

    fluence_at_meas (metadata): max cumulative fluence observed in the
    file, ions/cm².  For 7-col monitoring files this is the last/highest
    value of the fluence column; NULL for 4-col IV sweeps.

    fluence (measurements): per-sample cumulative fluence, ions/cm².
    Set only for irradiation 7-col monitoring rows; NULL elsewhere.
    """
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE baselines_metadata
                ADD COLUMN data_source TEXT DEFAULT 'baselines';
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE baselines_metadata
                ADD COLUMN fluence_at_meas DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE baselines_measurements
                ADD COLUMN fluence DOUBLE PRECISION;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_baselines_meta_data_source
            ON baselines_metadata(data_source);
    """)


def ingest_campaign(cur, conn, campaign, device_library, rules, dry_run=False):
    """
    Ingest all measurement files from one irradiation campaign folder.

    campaign is a dict with keys: id, campaign_name, folder_name.

    Returns (files_loaded, files_skipped, files_error, total_points).
    """
    folder_name = campaign["folder_name"]
    folder_path = os.path.join(IRRADIATION_ROOT, folder_name)
    campaign_name = campaign["campaign_name"]
    campaign_id = campaign["id"]

    if not os.path.isdir(folder_path):
        print(f"  WARNING: folder not found: {folder_path}")
        return 0, 0, 0, 0

    # Find all measurement .txt files
    txt_files = find_txt_files(folder_path)
    print(f"  Found {len(txt_files)} measurement files in {folder_name}")

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
        device_id = extract_device_id_from_path(fpath, folder_name)

        # Resolve device type from file path
        device_type, manufacturer = match_device(fpath, 'irradiation', rules, device_library)

        # File hash for dedup
        file_hash = compute_file_hash(fpath)

        # Check if already loaded; if present, still refresh device mapping so
        # Flask edits to device_mapping_rules propagate without full rebuild.
        cur.execute(
            "SELECT id, device_type, manufacturer FROM baselines_metadata WHERE file_hash = %s",
            (file_hash,),
        )
        existing = cur.fetchone()
        if existing:
            existing_id, old_device_type, old_manufacturer = existing
            if (old_device_type != device_type) or (old_manufacturer != manufacturer):
                cur.execute(
                    """
                    UPDATE baselines_metadata
                    SET device_type = %s,
                        manufacturer = %s
                    WHERE id = %s
                    """,
                    (device_type, manufacturer, existing_id),
                )
                conn.commit()
            files_skipped += 1
            continue

        # Parse the file
        headers, data_rows, header_meta = parse_keithley_txt(fpath)

        if not headers or not data_rows:
            if not dry_run:
                print(f"    SKIP (empty/unparseable): {filename}")
            files_skipped += 1
            continue

        # Refine the string-based category using the actual sweep range.
        # Handles IDVDfwd (→ Blocking) and IDVDrev (→ 3rd_Quadrant) that the
        # regex classifier routes to 'IdVd' based on filename alone.
        stats = sweep_stats(headers, data_rows, map_irrad_columns)
        refined, reason = refine_category_by_sweep(measurement_category, stats)
        if refined != measurement_category:
            if not dry_run or (idx + 1) % 20 == 0 or idx == 0:
                print(f"    RECLASSIFY {filename}: "
                      f"{measurement_category} → {refined} ({reason})")
            measurement_category = refined

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
                folder_name, device_id, measurement_type,
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
        max_fluence = None
        for point_idx, row in enumerate(data_rows):
            mapped = map_irrad_columns(headers, row)
            f = mapped['fluence']
            if isinstance(f, (int, float)) and (max_fluence is None or f > max_fluence):
                max_fluence = f
            batch.append((
                meta_id, point_idx,
                mapped['v_gate'], mapped['i_gate'],
                mapped['v_drain'], mapped['i_drain'],
                None, None,  # rds, bv
                mapped['time_val'],
                0,  # step_index
                f,
            ))

        if batch:
            try:
                execute_values(cur, """
                    INSERT INTO baselines_measurements
                    (metadata_id, point_index, v_gate, i_gate, v_drain,
                     i_drain, rds, bv, time_val, step_index, fluence)
                    VALUES %s
                """, batch, page_size=5000)

                if max_fluence is not None:
                    cur.execute(
                        "UPDATE baselines_metadata "
                        "SET fluence_at_meas = %s WHERE id = %s",
                        (max_fluence, meta_id),
                    )

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


def backfill_existing_fluence(cur, conn, campaign_folder=None, dry_run=False):
    """
    Repair existing irradiation waveform rows from the source files.

    This updates only columns affected by the mixed 7-column/5-column monitor
    format: v_gate, i_gate, fluence, and metadata fluence_at_meas.
    """
    where_campaign = ""
    params = []
    if campaign_folder:
        where_campaign = "AND ic.folder_name = %s"
        params.append(campaign_folder)

    cur.execute(f"""
        SELECT md.id, md.filename, md.csv_path, md.num_points,
               ic.campaign_name, ic.folder_name
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
        WHERE md.measurement_category = 'Irradiation'
          AND md.columns ILIKE '%%fluence%%'
          {where_campaign}
        ORDER BY ic.campaign_name, md.id
    """, params)
    records = cur.fetchall()

    files_seen = len(records)
    files_updated = 0
    files_error = 0
    rows_updated = 0
    rows_with_fluence = 0
    shortened_rows = 0

    for idx, (meta_id, filename, csv_path, num_points,
              campaign_name, _folder_name) in enumerate(records, start=1):
        if not csv_path or not os.path.isfile(csv_path):
            print(f"  ERROR: source file missing for metadata {meta_id}: "
                  f"{csv_path}")
            files_error += 1
            continue

        headers, data_rows, _header_meta = parse_keithley_txt(csv_path)
        if not headers or not data_rows:
            print(f"  ERROR: unable to parse metadata {meta_id}: {filename}")
            files_error += 1
            continue

        cur.execute(
            "SELECT COUNT(*) FROM baselines_measurements WHERE metadata_id = %s",
            (meta_id,),
        )
        db_points = cur.fetchone()[0]
        if db_points != len(data_rows):
            print(f"  ERROR: point-count mismatch for metadata {meta_id} "
                  f"({filename}): DB={db_points}, file={len(data_rows)}")
            files_error += 1
            continue

        is_fluence_monitor = _is_fluence_monitor_header(headers)
        file_shortened_rows = 0
        file_rows_with_fluence = 0
        max_fluence = None
        updates = []

        for point_idx, row in enumerate(data_rows):
            if is_fluence_monitor and len(row) == 5:
                file_shortened_rows += 1
            mapped = map_irrad_columns(headers, row)
            f = mapped['fluence']
            if isinstance(f, (int, float)):
                file_rows_with_fluence += 1
                if max_fluence is None or f > max_fluence:
                    max_fluence = f
            updates.append((
                meta_id, point_idx,
                mapped['v_gate'], mapped['i_gate'], f,
            ))

        if dry_run:
            files_updated += 1
            rows_updated += len(updates)
            rows_with_fluence += file_rows_with_fluence
            shortened_rows += file_shortened_rows
            continue

        try:
            execute_values(cur, """
                UPDATE baselines_measurements AS bm
                SET v_gate = data.v_gate,
                    i_gate = data.i_gate,
                    fluence = data.fluence
                FROM (VALUES %s) AS data(
                    metadata_id, point_index, v_gate, i_gate, fluence
                )
                WHERE bm.metadata_id = data.metadata_id
                  AND bm.point_index = data.point_index
            """, updates, template=(
                "(%s::integer, %s::integer, %s::double precision, "
                "%s::double precision, %s::double precision)"
            ), page_size=5000)
            cur.execute(
                "UPDATE baselines_metadata "
                "SET fluence_at_meas = %s WHERE id = %s",
                (max_fluence, meta_id),
            )
        except Exception as e:
            print(f"  ERROR: update failed for metadata {meta_id} "
                  f"({filename}): {e}")
            conn.rollback()
            files_error += 1
            continue

        files_updated += 1
        rows_updated += len(updates)
        rows_with_fluence += file_rows_with_fluence
        shortened_rows += file_shortened_rows

        if files_updated % 25 == 0:
            conn.commit()
            print(f"  [{idx}/{files_seen}] repaired {files_updated} files "
                  f"({campaign_name})")

    if not dry_run:
        conn.commit()

    return {
        'files_seen': files_seen,
        'files_updated': files_updated,
        'files_error': files_error,
        'rows_updated': rows_updated,
        'rows_with_fluence': rows_with_fluence,
        'shortened_rows': shortened_rows,
    }


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
    parser.add_argument("--backfill-fluence", action="store_true",
                        help="Repair existing irradiation monitor fluence and "
                             "mixed 7/5-column Vgs/Igs rows from source files")
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
    if not args.dry_run:
        apply_schema(conn)
    cur = conn.cursor()

    # Ensure schema is ready
    if not args.dry_run:
        ensure_data_source_column(cur)
        conn.commit()

    if args.backfill_fluence:
        print("\nBackfilling existing irradiation fluence/waveform rows...")
        if args.campaign:
            print(f"Campaign folder filter: {args.campaign}")
        t0 = perf_counter()
        result = backfill_existing_fluence(
            cur, conn, campaign_folder=args.campaign, dry_run=args.dry_run
        )
        elapsed = perf_counter() - t0

        print(f"\n{'=' * 70}")
        print("FLUENCE BACKFILL COMPLETE")
        print(f"{'=' * 70}")
        print(f"  Files scanned:        {result['files_seen']}")
        print(f"  Files repaired:       {result['files_updated']}")
        print(f"  Files errored:        {result['files_error']}")
        print(f"  Rows updated:         {result['rows_updated']}")
        print(f"  Rows with fluence:    {result['rows_with_fluence']}")
        print(f"  Short 5-col rows:     {result['shortened_rows']}")
        print(f"  Elapsed:              {elapsed:.1f}s")
        cur.close()
        conn.close()
        print("=" * 70)
        if args.dry_run:
            print("DRY RUN complete — no changes were made.")
        else:
            print("Done!")
        print("=" * 70)
        return

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

    rules = load_device_mapping_rules(cur, 'irradiation')
    print(f"Irradiation device-matching rules: {len(rules)}")

    # Load campaigns from DB (only those with a folder_name assigned)
    campaigns = load_campaigns_from_db(cur)
    conn.commit()

    if args.campaign:
        campaigns = [c for c in campaigns
                     if c["folder_name"] == args.campaign]
        if not campaigns:
            print(f"\nERROR: No campaign mapped to folder: {args.campaign}")
            print("Create a campaign via the /irradiation web UI and "
                  "assign this folder.")
            sys.exit(1)

    if not campaigns:
        print("\nNo campaigns with folder_name assignments found.")
        print("Use the /irradiation web UI to create campaigns and "
              "assign Data Folders.")
        sys.exit(0)

    print(f"\nCampaigns to ingest: {len(campaigns)}")
    for c in campaigns:
        print(f"  {c['campaign_name']} -> {c['folder_name']}")

    # Report unmapped folders
    unmapped = discover_unmapped_folders(cur)
    if unmapped:
        print(f"\nWARNING: {len(unmapped)} folder(s) without a campaign:")
        for f in unmapped:
            print(f"  {f}")
        print("  Assign them via the /irradiation web UI to include "
              "their data.")

    # Ingest each campaign
    t0 = perf_counter()
    grand_loaded = 0
    grand_skipped = 0
    grand_error = 0
    grand_points = 0

    for campaign in campaigns:
        print(f"\n{'─' * 60}")
        print(f"Campaign: {campaign['campaign_name']} "
              f"({campaign['folder_name']})")
        print(f"{'─' * 60}")

        loaded, skipped, errors, points = ingest_campaign(
            cur, conn, campaign, device_library, rules, dry_run=args.dry_run
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
