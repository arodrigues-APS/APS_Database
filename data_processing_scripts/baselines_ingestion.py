#!/usr/bin/env python3
"""
Baselines Data Ingestion Script
================================
Parses pristine MOSFET measurement CSV/XLS files and their associated TSP files
from /home/apsadmin/APS_Database/Pristine measurements/
and loads them into the PostgreSQL database for the Superset "Baselines" dashboard.

Each CSV/XLS measurement file is paired with its .tsp file (same base name in
the sibling lib/ directory) to capture the instrument run parameters (sweep range,
bias, compliance, measurement time, etc.).

Tables created:
  - baselines_metadata:      One row per measurement file with TSP run parameters
  - baselines_measurements:  All data points with FK to metadata
  - baselines_view:          Denormalized view joining both tables

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 baselines_ingestion.py
"""

import os
import re
import csv
import sys
import hashlib
from pathlib import Path
from time import perf_counter

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values

try:
    import pandas as pd
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "openpyxl", "xlrd"])
    import pandas as pd

from luaparser import ast as lua_ast
from luaparser import astnodes


# ── Configuration ────────────────────────────────────────────────────────────
PRISTINE_ROOT = "/home/apsadmin/APS_Database/Pristine measurements"
DB_HOST = "localhost"
DB_PORT = 5435
DB_NAME = "mosfets"
DB_USER = "postgres"
DB_PASSWORD = "APSLab"

# Set to True to drop existing baseline tables and rebuild from scratch
REBUILD = False


# ── TSP Parser (using luaparser) ──────────────────────────────────────────────

def _lua_node_to_python(node):
    """Recursively convert a luaparser AST node to a Python value."""
    if isinstance(node, astnodes.Number):
        return node.n
    if isinstance(node, astnodes.String):
        s = node.s
        return s.decode() if isinstance(s, bytes) else s
    if isinstance(node, astnodes.UMinusOp):
        val = _lua_node_to_python(node.operand)
        return -val if isinstance(val, (int, float)) else val
    if isinstance(node, astnodes.TrueExpr):
        return True
    if isinstance(node, astnodes.FalseExpr):
        return False
    if isinstance(node, astnodes.Name):
        return node.id
    if isinstance(node, astnodes.Nil):
        return None
    if isinstance(node, astnodes.Table):
        return _lua_table_to_python(node)
    return None


def _lua_table_to_python(table_node):
    """Convert a luaparser Table AST node to a Python dict or list."""
    fields = table_node.fields
    if not fields:
        return {}

    has_named_keys = any(
        isinstance(f, astnodes.Field) and f.key and isinstance(f.key, astnodes.Name)
        for f in fields
    )

    if has_named_keys:
        result = {}
        positional = []
        for f in fields:
            if not isinstance(f, astnodes.Field):
                continue
            if f.key and isinstance(f.key, astnodes.Name):
                result[f.key.id] = _lua_node_to_python(f.value)
            else:
                positional.append(_lua_node_to_python(f.value))
        # Merge positional items into the dict for mixed tables like Bias/Sweep
        if positional:
            result['_positional'] = positional
        return result
    else:
        return [
            _lua_node_to_python(f.value) if isinstance(f, astnodes.Field)
            else _lua_node_to_python(f)
            for f in fields
        ]


def _extract_positional(obj):
    """Get positional values from a mixed Lua table (dict with _positional key)."""
    if isinstance(obj, dict) and '_positional' in obj:
        return obj['_positional']
    if isinstance(obj, list):
        return obj
    return []


def parse_tsp_file(filepath):
    """
    Parse a .tsp file using luaparser and extract measurement parameters.

    Returns a dict with keys for sweep, bias, compliance, timing, etc.
    """
    result = {
        'sweep_start': None, 'sweep_stop': None, 'sweep_points': None,
        'bias_value': None, 'bias_channel': None,
        'compliance_ch1': None, 'compliance_ch2': None,
        'meas_time': None, 'hold_time': None, 'plc': None,
        'sample_num': None, 'sweep_mode': None,
        'step_num': None, 'step_start': None, 'step_stop': None,
        'delay_time': None, 'dc_only': None,
        'meas_channels': '',
        'raw_tsp': '',
    }

    try:
        with open(filepath, 'r', errors='replace') as f:
            content = f.read()
    except Exception as e:
        print(f"  Warning: Could not read TSP {filepath}: {e}")
        return result

    result['raw_tsp'] = content[:4000]

    # Parse Lua source into AST and extract local variable assignments
    assignments = {}
    try:
        tree = lua_ast.parse(content)
        for node in lua_ast.walk(tree):
            if isinstance(node, astnodes.LocalAssign):
                for target, val in zip(node.targets, node.values):
                    assignments[target.id] = _lua_node_to_python(val)
    except Exception:
        return result

    # ── Extract structured parameters from parsed assignments ──

    # Common parameters (dict)
    common = assignments.get('Common', {})
    if isinstance(common, dict):
        result['sweep_points'] = common.get('sweepnum')
        result['meas_time'] = common.get('meastime')
        result['hold_time'] = common.get('holdtime')
        result['plc'] = common.get('PLC')
        result['sample_num'] = common.get('samplenum')
        result['sweep_mode'] = common.get('sweepmode')
        result['step_num'] = common.get('stepnum')
        result['delay_time'] = common.get('delaytime')
        result['dc_only'] = common.get('dconly')

    # Sweep parameters (list of tables, each may be mixed positional+named)
    sweep = assignments.get('Sweep', {})
    if isinstance(sweep, list) and len(sweep) > 0:
        s0 = sweep[0]
        pos = _extract_positional(s0)
        if len(pos) >= 3:
            result['sweep_start'] = pos[1]
            result['sweep_stop'] = pos[2]
        elif isinstance(s0, list) and len(s0) >= 3:
            result['sweep_start'] = s0[1]
            result['sweep_stop'] = s0[2]

    # Bias parameters
    bias = assignments.get('Bias', {})
    if isinstance(bias, list) and len(bias) > 0:
        b0 = bias[0]
        pos = _extract_positional(b0)
        if len(pos) >= 2:
            result['bias_channel'] = pos[0]
            result['bias_value'] = pos[1]
        elif isinstance(b0, list) and len(b0) >= 2:
            result['bias_channel'] = b0[0]
            result['bias_value'] = b0[1]

    # Step parameters
    step = assignments.get('Step', {})
    if isinstance(step, list) and len(step) > 0:
        st0 = step[0]
        pos = _extract_positional(st0)
        if len(pos) >= 3:
            result['step_start'] = pos[1]
            result['step_stop'] = pos[2]
        elif isinstance(st0, list) and len(st0) >= 3:
            result['step_start'] = st0[1]
            result['step_stop'] = st0[2]

    # Compliance
    compliance = assignments.get('Compliance', {})
    if isinstance(compliance, list):
        for c in compliance:
            vals = c if isinstance(c, list) else _extract_positional(c)
            if isinstance(vals, list) and len(vals) >= 2:
                ch, val = vals[0], vals[1]
                if ch == 1:
                    result['compliance_ch1'] = val
                elif ch == 2:
                    result['compliance_ch2'] = val
                elif ch == 4 and result['compliance_ch1'] is None:
                    result['compliance_ch1'] = val

    # Measurement channels from Measseq
    measseq = assignments.get('Measseq', {})
    if isinstance(measseq, list):
        channels = []
        for entry in measseq:
            items = entry if isinstance(entry, list) else _extract_positional(entry)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and not item.isdigit():
                        channels.append(item)
        result['meas_channels'] = ','.join(channels)

    return result


# ── File Parsers ─────────────────────────────────────────────────────────────

def parse_csv_file(filepath):
    """Parse a measurement CSV file. Returns (headers, rows)."""
    headers = []
    rows = []

    try:
        with open(filepath, 'r', errors='replace') as f:
            reader = csv.reader(f)
            header_row = next(reader, None)
            if header_row is None:
                return headers, rows

            headers = [h.strip() for h in header_row]

            # Skip GROUP row if present
            next_row = next(reader, None)
            if next_row and any('GROUP' in str(c).upper() for c in next_row):
                pass  # skip
            elif next_row:
                # Not a GROUP row, treat as data
                try:
                    float_row = [float(c.strip()) if c.strip() else None for c in next_row]
                    rows.append(float_row)
                except (ValueError, IndexError):
                    pass

            for row in reader:
                if not row or all(cell.strip() == '' for cell in row):
                    continue
                try:
                    float_row = []
                    for cell in row:
                        cell = cell.strip()
                        if cell == '' or cell.upper() == 'NAN':
                            float_row.append(None)
                        else:
                            float_row.append(float(cell))
                    rows.append(float_row)
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        print(f"  Warning: Could not read CSV {filepath}: {e}")

    return headers, rows


def parse_xls_file(filepath):
    """Parse a measurement XLS/XLSX file. Returns (headers, rows)."""
    headers = []
    rows = []

    try:
        df = pd.read_excel(filepath, header=0)
        headers = [str(c).strip() for c in df.columns]

        # Skip rows that look like GROUP headers
        for _, row in df.iterrows():
            vals = row.values
            if any('GROUP' in str(v).upper() for v in vals if pd.notna(v)):
                continue
            float_row = []
            for v in vals:
                if pd.isna(v):
                    float_row.append(None)
                else:
                    try:
                        float_row.append(float(v))
                    except (ValueError, TypeError):
                        float_row.append(None)
            rows.append(float_row)
    except Exception as e:
        print(f"  Warning: Could not read XLS {filepath}: {e}")

    return headers, rows


# ── Measurement Type Classifier ──────────────────────────────────────────────

def classify_measurement(filename):
    """
    Classify the measurement type from the filename.
    Returns (device_id, measurement_type).

    Handles patterns like:
      B3D1_IdVg_Vd50mV          -> (B3D1, IdVg_Vd50mV)
      DUT10_3rd_Vg0V            -> (DUT10, 3rd_Vg0V)
      C5_IdVd_Rds_pristine      -> (C5, IdVd_Rds_pristine)
      R7_IdVd_Rds_on_pristine   -> (R7, IdVd_Rds_on_pristine)
      X8Y12_30_vd50mV_IdVg      -> (X8Y12_30, vd50mV_IdVg)
      IdVd_Vg_Tp220u            -> (unknown, IdVd_Vg_Tp220u)
      W1_3rdQuad_Vg_0V_500V     -> (W1, 3rdQuad_Vg_0V_500V)
    """
    stem = Path(filename).stem

    # Remove _appendN suffix for classification (keep in measurement_type)
    # stem_clean = re.sub(r'_append\d*$', '', stem)

    # Pattern priority order:
    # 1. B##D# or B##_##D# patterns (wafer batch / die)
    m = re.match(r'^(B\d+(?:_\d+)?D\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 2. DUT## patterns
    m = re.match(r'^(DUT\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 3. C# or C## (Cree samples) - be careful not to match C2M... device types
    m = re.match(r'^(C\d{1,2})_(.+)$', stem, re.IGNORECASE)
    if m and not m.group(1).upper().startswith('C2M') and not m.group(1).upper().startswith('C3M'):
        return m.group(1), m.group(2)

    # 4. R# or R## (Rohm samples)
    m = re.match(r'^(R\d{1,2})_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 5. W# patterns (wafer)
    m = re.match(r'^(W\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 6. X##Y##_## (Hitachi wafer coordinates)
    m = re.match(r'^(X\d+Y\d+_\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 7. Trench_DUT## pattern
    m = re.match(r'^(Trench_DUT\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 8a. C2M/C3M device names with DUT number (e.g. C2M0080120D_DUT2_IdVg_Vd1)
    m = re.match(r'^(C[23]M\d+D?_DUT\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 8b. C2M/C3M followed by _DUT (no trailing D on part number)
    m = re.match(r'^(C[23]M\d+_DUT\d+)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 8c. C2M/C3M device names without DUT (fallback, use non-greedy match)
    m = re.match(r'^(C[23]M\d+\w?)_(.+)$', stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # 9. Generic: take first token before _ as device
    m = re.match(r'^([^_]+)_(.+)$', stem)
    if m:
        return m.group(1), m.group(2)

    return 'unknown', stem


def categorize_measurement(measurement_type):
    """
    Group raw measurement_type strings into a handful of useful categories
    for filtering in Superset.  Returns one of:
        IdVg, IdVd, 3rd_Quadrant, Blocking, Igss, Vth, Rdson, Other
    """
    t = measurement_type or ''
    tl = t.lower()

    # Order matters: check more specific patterns first
    if re.search(r'idvg|id_vg|vd\d+mv|vd50|vd100|vd500', tl):
        return 'IdVg'
    if re.search(r'idvd|id_vd|rds_|rds_on|rdson|_rds', tl) and 'igss' not in tl:
        return 'IdVd'
    if re.search(r'3rd|quad|third', tl):
        return '3rd_Quadrant'
    if re.search(r'block|bvdss|idss|idvdss|dvdss|dvd_vg|listv', tl):
        return 'Blocking'
    if re.search(r'igss', tl):
        return 'Igss'
    if re.search(r'\bvth\b|vth_', tl):
        return 'Vth'
    if re.search(r'rdson', tl):
        return 'Rdson'
    if re.search(r'irrad', tl):
        return 'Irradiation'
    return 'Other'


# ── TSP File Matching ────────────────────────────────────────────────────────

def find_matching_tsp(csv_path):
    """
    Given a CSV path, find the matching TSP file.

    Strategy:
      1. Look in sibling lib/ directory with exact filename match
      2. Strip _appendN suffix and try again
      3. Search up the directory tree for lib/ folders
    """
    csv_p = Path(csv_path)
    stem = csv_p.stem

    # Build list of stems to try (exact, then without _append suffix)
    stems_to_try = [stem]
    stripped = re.sub(r'_append\d*$', '', stem)
    if stripped != stem:
        stems_to_try.append(stripped)

    # Search in parent directories for lib/ folders
    search_dirs = []
    p = csv_p.parent
    for _ in range(5):
        lib_dir = p / 'lib'
        if lib_dir.is_dir():
            search_dirs.append(lib_dir)
        p = p.parent

    for search_stem in stems_to_try:
        for lib_dir in search_dirs:
            tsp_file = lib_dir / f'{search_stem}.tsp'
            if tsp_file.exists():
                return str(tsp_file)

    return None


# ── Column Mapping ───────────────────────────────────────────────────────────

def map_columns(headers, row):
    """Map CSV columns to standard schema columns."""
    result = {
        'v_gate': None, 'i_gate': None,
        'v_drain': None, 'i_drain': None,
        'rds': None, 'bv': None, 'time_val': None,
    }

    for i, h in enumerate(headers):
        if i >= len(row):
            break
        val = row[i]
        hl = h.lower().strip()
        base = re.sub(r'\(\d+\)', '', hl).strip()

        if base in ('v_gate', 'vgs', 'vg'):
            if result['v_gate'] is None:
                result['v_gate'] = val
        elif base in ('i_gate', 'igs', 'ig'):
            if result['i_gate'] is None:
                result['i_gate'] = val
        elif base in ('v_drain', 'vds', 'vd'):
            if result['v_drain'] is None:
                result['v_drain'] = val
        elif base in ('i_drain', 'ids', 'id'):
            if result['i_drain'] is None:
                result['i_drain'] = val
        elif base in ('rds', 'r_ds', 'rdson'):
            if result['rds'] is None:
                result['rds'] = val
        elif base in ('bv', 'bvdss'):
            if result['bv'] is None:
                result['bv'] = val
        elif base in ('time', 'time_val', 't'):
            if result['time_val'] is None:
                result['time_val'] = val

    return result


def expand_multistep_rows(headers, rows):
    """
    For multi-step CSV files with columns like V_Drain(1), I_Drain(1), V_Gate(1),
    V_Drain(2), I_Drain(2), V_Gate(2), ... expand into separate rows per step.

    Returns list of (step_index, mapped_values_dict, point_index)
    """
    # Detect numbered columns
    numbered_cols = {}
    unnumbered_cols = []
    for i, h in enumerate(headers):
        m = re.match(r'(.+)\((\d+)\)', h)
        if m:
            base = m.group(1).strip()
            step = int(m.group(2))
            if step not in numbered_cols:
                numbered_cols[step] = {}
            numbered_cols[step][base] = i
        else:
            unnumbered_cols.append((i, h))

    results = []

    if not numbered_cols:
        # Simple single-step file
        for pidx, row in enumerate(rows):
            mapped = map_columns(headers, row)
            results.append((0, mapped, pidx))
    else:
        # Multi-step file
        for pidx, row in enumerate(rows):
            for step_idx in sorted(numbered_cols.keys()):
                cols = numbered_cols[step_idx]
                mapped = {
                    'v_gate': None, 'i_gate': None,
                    'v_drain': None, 'i_drain': None,
                    'rds': None, 'bv': None, 'time_val': None,
                }
                for base_name, col_idx in cols.items():
                    bl = base_name.lower()
                    val = row[col_idx] if col_idx < len(row) else None

                    if bl in ('v_gate', 'vgs', 'vg'):
                        mapped['v_gate'] = val
                    elif bl in ('i_gate', 'igs', 'ig'):
                        mapped['i_gate'] = val
                    elif bl in ('v_drain', 'vds', 'vd'):
                        mapped['v_drain'] = val
                    elif bl in ('i_drain', 'ids', 'id'):
                        mapped['i_drain'] = val
                    elif bl in ('rds',):
                        mapped['rds'] = val
                    elif bl in ('bv',):
                        mapped['bv'] = val
                    elif bl in ('time',):
                        mapped['time_val'] = val

                results.append((step_idx, mapped, pidx))

    return results


# ── Utility ──────────────────────────────────────────────────────────────────

def compute_file_hash(filepath):
    """Compute MD5 hash of a file for deduplication."""
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def extract_experiment_name(csv_path):
    """Extract the experiment name (top-level folder under Pristine measurements/)."""
    pristine_root = Path(PRISTINE_ROOT)
    csv_p = Path(csv_path)
    try:
        rel = csv_p.relative_to(pristine_root)
        return rel.parts[0]
    except ValueError:
        return 'unknown'


# ── Database Schema ──────────────────────────────────────────────────────────

CREATE_SCHEMA_SQL = """
-- Metadata table: one row per measurement file with TSP run parameters
CREATE TABLE IF NOT EXISTS baselines_metadata (
    id SERIAL PRIMARY KEY,
    experiment TEXT NOT NULL,
    device_id TEXT,
    measurement_type TEXT,
    measurement_category TEXT,
    filename TEXT NOT NULL,
    csv_path TEXT NOT NULL,
    tsp_path TEXT,
    columns TEXT,
    num_points INTEGER,
    sweep_start DOUBLE PRECISION,
    sweep_stop DOUBLE PRECISION,
    sweep_points INTEGER,
    bias_value DOUBLE PRECISION,
    bias_channel INTEGER,
    compliance_ch1 DOUBLE PRECISION,
    compliance_ch2 DOUBLE PRECISION,
    meas_time DOUBLE PRECISION,
    hold_time DOUBLE PRECISION,
    plc DOUBLE PRECISION,
    sample_num INTEGER,
    sweep_mode INTEGER,
    step_num INTEGER,
    step_start DOUBLE PRECISION,
    step_stop DOUBLE PRECISION,
    delay_time DOUBLE PRECISION,
    dc_only INTEGER,
    meas_channels TEXT,
    raw_tsp TEXT,
    file_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_hash)
);

-- Measurements table: all data points
CREATE TABLE IF NOT EXISTS baselines_measurements (
    id BIGSERIAL PRIMARY KEY,
    metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    point_index INTEGER NOT NULL,
    v_gate DOUBLE PRECISION,
    i_gate DOUBLE PRECISION,
    v_drain DOUBLE PRECISION,
    i_drain DOUBLE PRECISION,
    rds DOUBLE PRECISION,
    bv DOUBLE PRECISION,
    time_val DOUBLE PRECISION,
    step_index INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_baselines_meas_meta ON baselines_measurements(metadata_id);
CREATE INDEX IF NOT EXISTS idx_baselines_meas_vd ON baselines_measurements(v_drain);
CREATE INDEX IF NOT EXISTS idx_baselines_meas_vg ON baselines_measurements(v_gate);
CREATE INDEX IF NOT EXISTS idx_baselines_meas_id_drain ON baselines_measurements(i_drain);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_experiment ON baselines_metadata(experiment);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_device ON baselines_metadata(device_id);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_type ON baselines_metadata(measurement_type);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_category ON baselines_metadata(measurement_category);

-- Denormalized view for easy Superset querying
-- NOTE: CASE expressions replace Keithley 9.9E37 overflow sentinel values
-- with NULL so that Superset range filters/sliders show sensible bounds.
CREATE OR REPLACE VIEW baselines_view AS
SELECT
    m.id AS measurement_id,
    md.experiment,
    md.device_id,
    md.measurement_type,
    md.measurement_category,
    md.filename,
    md.sweep_start,
    md.sweep_stop,
    md.sweep_points,
    md.bias_value,
    md.compliance_ch1,
    md.compliance_ch2,
    md.meas_time,
    md.hold_time,
    md.plc,
    md.sample_num,
    md.step_num,
    md.step_start,
    md.step_stop,
    m.point_index,
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN m.v_gate ELSE NULL END     AS v_gate,
    CASE WHEN m.i_gate IS NOT NULL AND ABS(m.i_gate) < 1e30
         THEN m.i_gate ELSE NULL END     AS i_gate,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN m.v_drain ELSE NULL END    AS v_drain,
    CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
         THEN m.i_drain ELSE NULL END    AS i_drain,
    m.rds,
    m.bv,
    m.time_val,
    m.step_index
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id;
"""


# ── Main Ingestion ───────────────────────────────────────────────────────────

def main():
    start_time = perf_counter()

    print("=" * 70)
    print("Baselines Data Ingestion")
    print("=" * 70)
    print(f"Source: {PRISTINE_ROOT}")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print()

    # Connect
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Drop existing baseline tables if rebuilding
    if REBUILD:
        print("Dropping existing baseline tables...")
        cur.execute("DROP VIEW IF EXISTS baselines_view CASCADE")
        cur.execute("DROP TABLE IF EXISTS baselines_measurements CASCADE")
        cur.execute("DROP TABLE IF EXISTS baselines_metadata CASCADE")
        conn.commit()
        print("  Dropped.")

    # Create schema
    print("Creating schema...")
    cur.execute(CREATE_SCHEMA_SQL)
    conn.commit()
    print("  Schema ready.")

    # Find all measurement files (CSV + XLS)
    measurement_files = []
    for root, dirs, files in os.walk(PRISTINE_ROOT):
        for f in sorted(files):
            fl = f.lower()
            if fl.endswith('.csv') or fl.endswith('.xls') or fl.endswith('.xlsx'):
                measurement_files.append(os.path.join(root, f))

    print(f"\nFound {len(measurement_files)} measurement files to process.")

    # Track statistics
    total_points = 0
    files_loaded = 0
    files_skipped = 0
    files_error = 0
    experiment_stats = {}

    for idx, fpath in enumerate(measurement_files):
        filename = os.path.basename(fpath)
        experiment = extract_experiment_name(fpath)
        device_id, measurement_type = classify_measurement(filename)
        measurement_category = categorize_measurement(measurement_type)

        # File hash for dedup
        file_hash = compute_file_hash(fpath)

        # Check if already loaded
        cur.execute("SELECT id FROM baselines_metadata WHERE file_hash = %s", (file_hash,))
        if cur.fetchone():
            files_skipped += 1
            continue

        # Parse file
        fl = filename.lower()
        if fl.endswith('.csv'):
            headers, rows = parse_csv_file(fpath)
        elif fl.endswith('.xls') or fl.endswith('.xlsx'):
            headers, rows = parse_xls_file(fpath)
        else:
            files_skipped += 1
            continue

        if not headers or not rows:
            if (idx + 1) % 100 == 0:
                print(f"  [{idx+1}/{len(measurement_files)}] SKIP (empty): {filename}")
            files_skipped += 1
            continue

        # Find and parse matching TSP
        tsp_path = find_matching_tsp(fpath)
        tsp_params = {}
        if tsp_path:
            tsp_params = parse_tsp_file(tsp_path)

        # Insert metadata
        try:
            cur.execute("""
                INSERT INTO baselines_metadata (
                    experiment, device_id, measurement_type, measurement_category,
                    filename, csv_path, tsp_path,
                    columns, num_points,
                    sweep_start, sweep_stop, sweep_points,
                    bias_value, bias_channel,
                    compliance_ch1, compliance_ch2,
                    meas_time, hold_time, plc, sample_num,
                    sweep_mode, step_num, step_start, step_stop,
                    delay_time, dc_only, meas_channels, raw_tsp,
                    file_hash
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s
                ) RETURNING id
            """, (
                experiment, device_id, measurement_type, measurement_category,
                filename, fpath, tsp_path,
                ','.join(headers), len(rows),
                tsp_params.get('sweep_start'), tsp_params.get('sweep_stop'),
                tsp_params.get('sweep_points'),
                tsp_params.get('bias_value'), tsp_params.get('bias_channel'),
                tsp_params.get('compliance_ch1'), tsp_params.get('compliance_ch2'),
                tsp_params.get('meas_time'), tsp_params.get('hold_time'),
                tsp_params.get('plc'), tsp_params.get('sample_num'),
                tsp_params.get('sweep_mode'), tsp_params.get('step_num'),
                tsp_params.get('step_start'), tsp_params.get('step_stop'),
                tsp_params.get('delay_time'), tsp_params.get('dc_only'),
                tsp_params.get('meas_channels', ''), tsp_params.get('raw_tsp', ''),
                file_hash
            ))
            meta_id = cur.fetchone()[0]
        except Exception as e:
            print(f"  [{idx+1}/{len(measurement_files)}] ERROR (metadata): {filename}: {e}")
            conn.rollback()
            files_error += 1
            continue

        # Expand multi-step rows and insert measurements
        expanded = expand_multistep_rows(headers, rows)

        if expanded:
            batch = []
            for step_idx, mapped, point_idx in expanded:
                batch.append((
                    meta_id, point_idx,
                    mapped['v_gate'], mapped['i_gate'],
                    mapped['v_drain'], mapped['i_drain'],
                    mapped['rds'], mapped['bv'], mapped['time_val'],
                    step_idx
                ))

            try:
                execute_values(cur, """
                    INSERT INTO baselines_measurements
                    (metadata_id, point_index, v_gate, i_gate, v_drain, i_drain, rds, bv, time_val, step_index)
                    VALUES %s
                """, batch, page_size=5000)

                total_points += len(batch)
                files_loaded += 1

                # Track per-experiment stats
                if experiment not in experiment_stats:
                    experiment_stats[experiment] = {'files': 0, 'points': 0, 'tsp': 0}
                experiment_stats[experiment]['files'] += 1
                experiment_stats[experiment]['points'] += len(batch)
                if tsp_path:
                    experiment_stats[experiment]['tsp'] += 1

                if (idx + 1) % 50 == 0:
                    tsp_status = "TSP" if tsp_path else "no-TSP"
                    print(f"  [{idx+1}/{len(measurement_files)}] Progress: {files_loaded} loaded, "
                          f"{total_points} pts so far ({experiment}/{filename})")

            except Exception as e:
                print(f"  [{idx+1}/{len(measurement_files)}] ERROR (data): {filename}: {e}")
                conn.rollback()
                files_error += 1
                continue

        # Commit every 50 files
        if files_loaded % 50 == 0:
            conn.commit()

    conn.commit()

    # Print results
    elapsed = perf_counter() - start_time

    print("\n" + "=" * 70)
    print("Ingestion complete!")
    print(f"  Time taken:    {elapsed:.1f} seconds")
    print(f"  Files loaded:  {files_loaded}")
    print(f"  Files skipped: {files_skipped}")
    print(f"  Files errored: {files_error}")
    print(f"  Total points:  {total_points}")
    print()
    print("Per-experiment breakdown:")
    for exp in sorted(experiment_stats.keys()):
        s = experiment_stats[exp]
        print(f"  {exp}:")
        print(f"    Files: {s['files']}  |  Points: {s['points']}  |  With TSP: {s['tsp']}")
    print("=" * 70)

    # Verify
    cur.execute("SELECT COUNT(*) FROM baselines_metadata")
    meta_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM baselines_measurements")
    meas_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM baselines_metadata WHERE tsp_path IS NOT NULL")
    tsp_count = cur.fetchone()[0]
    print(f"\nDatabase totals:")
    print(f"  baselines_metadata:     {meta_count} rows ({tsp_count} with TSP)")
    print(f"  baselines_measurements: {meas_count} rows")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
