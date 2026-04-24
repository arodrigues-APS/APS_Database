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
    python3 ingestion_baselines.py
"""

import os
import re
import csv
import sys
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
from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from common import (load_device_library, compute_file_hash, find_matching_tsp,
                    map_columns, expand_multistep_rows, categorize_measurement,
                    sweep_stats, refine_category_by_sweep)

PRISTINE_ROOT = "/home/arodrigues/APS_Database/Measurements/Pristine"

# Set to True to drop existing baseline tables and rebuild from scratch
REBUILD = False


# ── Device Library (loaded from SQL at runtime) ─────────────────────────────
# The device_library table is managed through Superset's SQL Lab interface.
# Admins add/edit/remove rows there; the ingestion script reads the table
# each time it runs.  Each row has a part_number (used as the search pattern
# in filenames/paths) plus metadata columns.
# load_device_library() is imported from common.py.


def match_device_type(filepath, device_library):
    """
    Try to identify the commercial device type from the file path.

    Three-pass matching strategy:
      1. Substring match: search for each part_number (case-insensitive) in
         the full file path.  Library is pre-sorted longest-first so more
         specific part numbers win.
      2. Prefix match: if the path contains a part_number base (e.g.
         "C2M0080120" from "C2M0080120D") followed by an underscore, match
         it.  Handles filenames like "C2M0080120_DUT01_IdVg.csv".
      3. Experiment-name heuristic: when the experiment folder encodes the
         manufacturer and Rds(on) (e.g. "Rohm_30mOhm_preIV_…"), look up the
         unique device in the library that matches that manufacturer + Rds(on).
         For the mixed Cree_80mOhm experiment, device_id prefixes "I" and
         "ROHM"/"INFINEON" override the default Wolfspeed assignment.

    Returns (part_number, manufacturer) on match, or (None, None).
    """
    import re
    path_upper = filepath.upper()
    filename_upper = os.path.basename(filepath).upper()

    # Pass 1 – exact part-number substring
    for entry in device_library:
        if entry["part_number"].upper() in path_upper:
            return entry["part_number"], entry["manufacturer"]

    # Pass 2 – part-number prefix match (handles trailing D or other suffixes)
    for entry in device_library:
        pn = entry["part_number"].upper()
        if len(pn) > 4 and pn[-1].isalpha():
            prefix = pn[:-1]
            if re.search(prefix + r'[_\b]', path_upper):
                return entry["part_number"], entry["manufacturer"]

    # Pass 3 – experiment-name heuristic.
    # Maps experiment folder patterns directly to (part_number, manufacturer)
    # to avoid ambiguity when multiple library entries share the same rdson.
    # The mixed Cree_80mOhm experiment overrides the default for Infineon/Rohm
    # files based on filename prefix.

    def _lookup_part(part_number):
        for entry in device_library:
            if entry["part_number"] == part_number:
                return entry["part_number"], entry["manufacturer"]
        return None, None

    _EXPERIMENT_RULES = [
        ("ROHM_30MOHM",     "SCT3030AL"),
        ("INFINEON_90MOHM", "IMW120R090M1H"),
        ("CREE_25MOHM",     "C2M0025120D"),
        ("CREE_80MOHM",     "C2M0080120D"),
    ]

    for pattern, default_part in _EXPERIMENT_RULES:
        if pattern not in path_upper:
            continue
        # For mixed experiments (Cree_80mOhm has Wolfspeed + Infineon + Rohm),
        # check device_id prefix in the filename to override the default.
        if pattern == "CREE_80MOHM":
            if re.match(r'I\d', filename_upper) or "INFINEON" in filename_upper:
                return _lookup_part("IMW120R090M1H")
            if "ROHM" in filename_upper:
                return _lookup_part("SCT3030AL")
        return _lookup_part(default_part)

    return None, None


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
    drain_channels = set()
    if isinstance(measseq, list):
        channels = []
        for entry in measseq:
            items = entry if isinstance(entry, list) else _extract_positional(entry)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and not item.isdigit():
                        channels.append(item)
                # Identify drain channel numbers from Measseq entries
                # that contain 'I_Drain' or 'V_Drain' measurement names
                names = [x for x in items if isinstance(x, str)]
                if any('drain' in n.lower() for n in names):
                    ch = items[0] if items else None
                    if isinstance(ch, (int, float)):
                        drain_channels.add(int(ch))
        result['meas_channels'] = ','.join(channels)

    # Drain-specific bias: scan ALL bias entries for one matching a drain
    # channel.  This handles multi-bias TSPs (e.g. Hitachi combine mode)
    # where the first bias entry may be for a non-drain channel.
    bias = assignments.get('Bias', {})
    if isinstance(bias, list) and drain_channels:
        for b_entry in bias:
            pos = _extract_positional(b_entry) if not isinstance(b_entry, list) else b_entry
            if isinstance(pos, list) and len(pos) >= 2:
                b_ch = pos[0]
                b_val = pos[1]
                if isinstance(b_ch, (int, float)) and int(b_ch) in drain_channels:
                    result['drain_bias_value'] = b_val
                    break

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


# categorize_measurement() is imported from common.py.


# ── Irradiation Detection (post-ingestion, data-based) ──────────────────────

FLAG_IRRADIATED_SQL = """
-- Purely data-driven detection of likely irradiated measurements.
-- Works in three passes:
--   1. Extract approximate Vth per file from dedicated Vth sweeps only
--      (NOT IdVg — those have high drain bias that causes false positives)
--   2. Compare each file's Vth against its device_type population median
--      (computed from BASE files only, excluding _append re-measurements);
--      flag outliers whose Vth falls below (median - 3 × IQR) or below 0 V
--   3. Smart sibling propagation:
--      - If a BASE Vth file is flagged → the device was irradiated before
--        any measurement → flag ALL files for that (device_id, experiment)
--      - If only APPEND Vth files are flagged → only post-irradiation
--        re-measurements are affected → flag only _append files

-- Reset flags for baselines records only (SC records managed by ingestion_sc.py)
UPDATE baselines_metadata SET is_likely_irradiated = FALSE
WHERE data_source IS NULL OR data_source = 'baselines';

WITH
-- Step 1: per-file Vth extraction from dedicated Vth sweeps only
-- Vth ≈ lowest Vg where |Id| first exceeds 1 mA
-- IMPORTANT: restricted to measurement_category = 'Vth' because IdVg files
-- at high drain bias (Vd=2-4V) show |Id| > 1mA at Vg=0 even on pristine
-- devices, which would cause false positives.
per_file_vth AS (
    SELECT md.id AS metadata_id,
           md.device_type,
           md.device_id,
           md.experiment,
           md.measurement_type,
           MIN(m.v_gate) AS vth_approx,
           (md.measurement_type !~ '_append') AS is_base
    FROM baselines_measurements m
    JOIN baselines_metadata md ON m.metadata_id = md.id
    WHERE md.measurement_category = 'Vth'
      AND (md.data_source IS NULL OR md.data_source = 'baselines')
      AND m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
      AND m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
      AND ABS(m.i_drain) > 0.001   -- 1 mA threshold
    GROUP BY md.id, md.device_type, md.device_id, md.experiment,
             md.measurement_type
),

-- Step 2a: population statistics from BASE files only
-- Using only base (pre-irradiation) files ensures the reference distribution
-- is not contaminated by post-irradiation shifts.
-- Percentiles are robust against outliers.
pop_stats AS (
    SELECT device_type,
           PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY vth_approx) AS median_vth,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vth_approx) AS q1_vth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vth_approx) AS q3_vth
    FROM per_file_vth
    WHERE device_type IS NOT NULL AND is_base
    GROUP BY device_type
),

-- Step 2b: flag Vth files whose extracted Vth is anomalous
-- Criteria (must satisfy ANY):
--   a) Vth < 0 V  (device is normally-on → heavy irradiation damage)
--   b) Vth < median - 3 × IQR  (statistical outlier on the low side)
flagged_vth_files AS (
    SELECT f.metadata_id, f.device_id, f.experiment, f.is_base
    FROM per_file_vth f
    LEFT JOIN pop_stats p ON f.device_type = p.device_type
    WHERE f.vth_approx < 0
       OR (p.device_type IS NOT NULL
           AND f.vth_approx < p.median_vth - 3.0 * (p.q3_vth - p.q1_vth))
),

-- Step 3a: BASE Vth flagged → device was irradiated before any measurement
-- → flag ALL files for this (device_id, experiment)
device_fully_irradiated AS (
    SELECT DISTINCT device_id, experiment
    FROM flagged_vth_files WHERE is_base
),

-- Step 3b: only APPEND Vth flagged → only post-irradiation re-measurements
-- → flag only _append files for this (device_id, experiment)
device_append_irradiated AS (
    SELECT DISTINCT device_id, experiment
    FROM flagged_vth_files
    WHERE NOT is_base
      AND (device_id, experiment) NOT IN (
          SELECT device_id, experiment FROM device_fully_irradiated)
),

-- Collect all file IDs that should be flagged
all_flagged AS (
    -- The directly-flagged Vth files themselves
    SELECT metadata_id FROM flagged_vth_files
    UNION
    -- All files for fully-irradiated devices
    SELECT md.id FROM baselines_metadata md
    JOIN device_fully_irradiated d
      ON md.device_id = d.device_id AND md.experiment = d.experiment
    UNION
    -- Only _append files for append-irradiated devices
    SELECT md.id FROM baselines_metadata md
    JOIN device_append_irradiated d
      ON md.device_id = d.device_id AND md.experiment = d.experiment
    WHERE md.measurement_type ~ '_append'
)

UPDATE baselines_metadata SET is_likely_irradiated = TRUE
WHERE id IN (SELECT metadata_id FROM all_flagged);
"""


# find_matching_tsp() is imported from common.py.


# map_columns() and expand_multistep_rows() are imported from common.py.


# compute_file_hash() is imported from common.py.


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
-- ── Device Library table ────────────────────────────────────────────────
-- Managed by admins via Superset SQL Lab.  Each row is a known commercial
-- device.  The part_number is used as a case-insensitive substring match
-- against file paths during ingestion.
CREATE TABLE IF NOT EXISTS device_library (
    id SERIAL PRIMARY KEY,
    part_number  TEXT NOT NULL UNIQUE,
    device_category TEXT,          -- MOSFET, Diode, etc.
    manufacturer TEXT,             -- Wolfspeed, Infineon, Rohm, ...
    voltage_rating TEXT,           -- e.g. '1200 V'
    rdson_mohm   TEXT,             -- e.g. '80' (mOhm), NULL for diodes
    current_rating_a TEXT,         -- e.g. '33' (A), NULL for MOSFETs
    package_type TEXT,             -- bare_die, TO-247, home_made_TO, etc.
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
    drain_bias_value DOUBLE PRECISION,
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
    device_type TEXT,
    manufacturer TEXT,
    is_likely_irradiated BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_hash)
);

-- Add device_type/manufacturer/drain_bias_value to existing tables (idempotent)
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN device_type TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN manufacturer TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN drain_bias_value DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN is_likely_irradiated BOOLEAN NOT NULL DEFAULT FALSE;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- Irradiation campaign link (idempotent; FK omitted here so this runs even
-- if irradiation_campaigns doesn't exist yet -- seed_irradiation_campaigns.py
-- adds the proper FK constraint when the campaigns table is present)
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN irrad_campaign_id INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN irrad_role TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_campaign
    ON baselines_metadata(irrad_campaign_id);

-- Audit columns written by promote_pre_irrad_to_baselines.py.
-- Track the gate decision that flipped a pre_irrad file's data_source to
-- 'baselines' (or recorded why it was left as 'irradiation').
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN promotion_decision TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN promotion_reason TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN promotion_ts TIMESTAMP;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN gate_params JSONB;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_baselines_meta_promotion
    ON baselines_metadata(promotion_decision);

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
-- DROP before CREATE so adding new columns never hits the
-- "cannot change name of view column" error from CREATE OR REPLACE.
DROP VIEW IF EXISTS baselines_view CASCADE;
CREATE VIEW baselines_view AS
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
    -- _r columns: rounded to 2 d.p. for use as chart x-axes and range-filter targets.
    -- They are NULL wherever the raw value was a Keithley overflow sentinel (9.91e+37).
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END  AS v_gate_r,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN ROUND(m.v_drain::numeric, 2)::double precision ELSE NULL END AS v_drain_r,
    -- _bin columns: rounded to 2 d.p. (0.01 V) for chart x-axes.
    -- Matches the resolution used by baselines_view_device_library and sc_ruggedness_view.
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END  AS v_gate_bin,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN ROUND(m.v_drain::numeric, 2)::double precision ELSE NULL END AS v_drain_bin,
    m.rds,
    m.bv,
    m.time_val,
    m.step_index,
    md.is_likely_irradiated
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id;

-- Device-library view: includes device_type and manufacturer columns
DROP VIEW IF EXISTS baselines_view_device_library;
CREATE VIEW baselines_view_device_library AS
SELECT
    m.id AS measurement_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
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
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END  AS v_gate_r,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN ROUND(m.v_drain::numeric, 2)::double precision ELSE NULL END AS v_drain_r,
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END  AS v_gate_bin,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 2)::double precision
        ELSE NULL
    END AS v_drain_bin,
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(ROUND(m.v_gate::numeric, 2))::double precision ELSE NULL END  AS v_gate_bias,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric)::double precision
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(ROUND(m.v_drain::numeric, 2))::double precision
        ELSE NULL
    END AS v_drain_bias,
    m.rds,
    m.bv,
    m.time_val,
    m.step_index,
    md.is_likely_irradiated
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id
WHERE md.data_source IS NULL OR md.data_source = 'baselines';

-- Per-run max |i_drain|, used to detect compliance-limited points.
-- A point is considered compliance-limited when |i_drain| >= 99% of the
-- run's overall max.  This catches the flat plateau that appears when the
-- instrument clamps the current, without requiring TSP compliance metadata.
CREATE MATERIALIZED VIEW IF NOT EXISTS baselines_run_max_current AS
SELECT metadata_id,
       MAX(ABS(i_drain)) AS max_abs_i_drain
FROM baselines_measurements
WHERE i_drain IS NOT NULL AND ABS(i_drain) < 1e30
GROUP BY metadata_id;

-- Per-device view: averaged per device_id at each voltage bin.
-- A device measured in multiple files (e.g. Vth + Vth_append1) contributes
-- exactly one value per bin.  Points at >=99% of a run's max |i_drain| are
-- excluded so compliance-clamped data does not distort the mean.
--
-- Used by both baselines_device_averages (for dashboard curve charts) and
-- CALCULATED_PARAMS_SQL (for per-device parameter extraction).
-- Bin resolution rationale (per category):
--   IdVd / 3rd_Quadrant:  Vg is a STEP bias set to a few nominal values
--     (3-12 per run) with ±0.05 V of instrument jitter.  Vd is swept,
--     but different devices use different sweep grids (steps of 0.01,
--     0.05, 0.1 V; various offsets).  Fine-grained binning fragments
--     each nominal step into sub-bins covering different device subsets,
--     and the chart's final AVG mixes those subsets unequally at every
--     point — producing ~0.1 V-period spikes in the averaged curve.
--     Snapping Vg to integer and Vd to 0.1 V at the per-device stage
--     aligns all devices onto a shared grid so baselines_device_averages
--     is a clean weighted mean.  Rds_on slope still has ~20 points per
--     device in the 0-2 V region — ample for the linear fit.
--   IdVg / Vth / Subthreshold / Igss:  Vg is SWEPT — keep 0.01 V so
--     Vth, gfs, and SS retain resolution.  Vd comes from drain_bias_value
--     (exact step) or the sampled Vd for categories without a bias value.
--   Other categories (Blocking, Bodydiode, ChannelDiode):  unchanged at
--     0.01 V resolution.
CREATE VIEW baselines_per_device AS
SELECT
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.measurement_category,
    md.is_likely_irradiated,
    CASE
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant')
        THEN ROUND(m.v_gate::numeric, 0)::double precision
        ELSE ROUND(m.v_gate::numeric, 2)::double precision
    END AS v_gate_bin,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant')
             AND m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 1)::double precision
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 2)::double precision
        ELSE NULL
    END AS v_drain_bin,
    AVG(m.i_drain)               AS dev_avg_i_drain,
    AVG(m.i_gate)                AS dev_avg_i_gate,
    AVG(ABS(m.i_drain))          AS dev_avg_abs_i_drain,
    AVG(ABS(m.i_gate))           AS dev_avg_abs_i_gate,
    COUNT(*)                     AS dev_n_points,
    COUNT(DISTINCT md.id)        AS dev_n_runs
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id
LEFT JOIN baselines_run_max_current rmc ON rmc.metadata_id = md.id
WHERE md.device_type IS NOT NULL
  AND (md.data_source IS NULL OR md.data_source = 'baselines')
  AND (m.v_gate IS NULL OR ABS(m.v_gate) < 1e30)
  AND (m.v_drain IS NULL OR ABS(m.v_drain) < 1e30)
  AND (m.i_drain IS NULL OR ABS(m.i_drain) < 1e30)
  AND (m.i_gate IS NULL OR ABS(m.i_gate) < 1e30)
  AND (m.i_drain IS NULL
       OR rmc.max_abs_i_drain IS NULL
       OR ABS(m.i_drain) < 0.99 * rmc.max_abs_i_drain)
GROUP BY
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.measurement_category,
    md.is_likely_irradiated,
    CASE
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant')
        THEN ROUND(m.v_gate::numeric, 0)::double precision
        ELSE ROUND(m.v_gate::numeric, 2)::double precision
    END,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant')
             AND m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 1)::double precision
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 2)::double precision
        ELSE NULL
    END;

-- Averaged device performance view: pre-aggregated per voltage bin.
-- Used by the "Baselines Device Library" dashboard to show mean ± spread
-- for each device_type / measurement_category, averaged across all runs.
--
-- Reads from baselines_per_device (Stage 1) and averages across devices
-- (Stage 2).  Without this two-stage aggregation, multi-file devices are
-- over-represented and distort the group mean.
CREATE VIEW baselines_device_averages AS
SELECT
    sub.*,
    ROUND(sub.v_gate_bin)::double precision AS v_gate_bias,
    ROUND(sub.v_drain_bin)::double precision AS v_drain_bias
FROM (
    SELECT
        device_type,
        manufacturer,
        measurement_category,
        is_likely_irradiated,
        v_gate_bin,
        v_drain_bin,
        AVG(dev_avg_i_drain)               AS avg_i_drain,
        STDDEV(dev_avg_i_drain)            AS std_i_drain,
        MIN(dev_avg_i_drain)               AS min_i_drain,
        MAX(dev_avg_i_drain)               AS max_i_drain,
        AVG(dev_avg_i_drain) + COALESCE(STDDEV(dev_avg_i_drain), 0) AS upper_i_drain,
        AVG(dev_avg_i_drain) - COALESCE(STDDEV(dev_avg_i_drain), 0) AS lower_i_drain,
        AVG(dev_avg_i_gate)                AS avg_i_gate,
        STDDEV(dev_avg_i_gate)             AS std_i_gate,
        AVG(dev_avg_abs_i_drain)           AS avg_abs_i_drain,
        AVG(dev_avg_abs_i_gate)            AS avg_abs_i_gate,
        SUM(dev_n_points)                  AS n_points,
        COUNT(*)                           AS n_devices,
        SUM(dev_n_runs)                    AS n_runs
    FROM baselines_per_device
    GROUP BY
        device_type,
        manufacturer,
        measurement_category,
        is_likely_irradiated,
        v_gate_bin,
        v_drain_bin
) sub;
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
        cur.execute("DROP VIEW IF EXISTS baselines_device_averages CASCADE")
        cur.execute("DROP VIEW IF EXISTS baselines_per_device CASCADE")
        cur.execute("DROP VIEW IF EXISTS baselines_view_device_library CASCADE")
        cur.execute("DROP VIEW IF EXISTS baselines_view CASCADE")
        cur.execute("DROP TABLE IF EXISTS baselines_measurements CASCADE")
        cur.execute("DROP TABLE IF EXISTS baselines_metadata CASCADE")
        conn.commit()
        print("  Dropped.")

    # Drop views before (re)creating – views are derived and safe to recreate
    cur.execute("DROP VIEW IF EXISTS baselines_device_averages CASCADE")
    cur.execute("DROP VIEW IF EXISTS baselines_per_device CASCADE")
    cur.execute("DROP VIEW IF EXISTS baselines_view_device_library CASCADE")
    cur.execute("DROP VIEW IF EXISTS baselines_view CASCADE")
    conn.commit()

    # Create schema
    print("Creating schema...")
    cur.execute(CREATE_SCHEMA_SQL)
    conn.commit()
    print("  Schema ready.")

    # Load device library from DB
    print("\nLoading device library...")
    device_library = load_device_library(cur)
    print(f"  {len(device_library)} devices in library.")
    if not device_library:
        print("  WARNING: device_library table is empty.")
        print("  Run  python3 seed_device_library.py  to populate it,")
        print("  or add devices via Superset SQL Lab.")

    # Find all measurement files (CSV + XLS)
    measurement_files = []
    for root, dirs, files in os.walk(PRISTINE_ROOT):
        for f in sorted(files):
            fl = f.lower()
            if fl.endswith('.csv') or fl.endswith('.xls') or fl.endswith('.xlsx'):
                measurement_files.append(os.path.join(root, f))

    print(f"\nFound {len(measurement_files)} measurement files to process.")

    # Sync deletions: remove baselines DB records for files no longer on disk.
    # Only touch baselines records (data_source IS NULL or 'baselines') —
    # SC ruggedness records are managed by ingestion_sc.py.
    print("\nSyncing deletions...")
    on_disk_paths = set(measurement_files)
    cur.execute(
        "SELECT id, csv_path FROM baselines_metadata "
        "WHERE csv_path IS NOT NULL "
        "  AND (data_source IS NULL OR data_source = 'baselines')"
    )
    db_rows = cur.fetchall()
    stale_ids = [row[0] for row in db_rows if row[1] not in on_disk_paths]
    if stale_ids:
        cur.execute("DELETE FROM baselines_metadata WHERE id = ANY(%s)", (stale_ids,))
        conn.commit()
        print(f"  Removed {len(stale_ids)} stale record(s) (ON DELETE CASCADE cleans measurements).")
    else:
        print("  No stale records found.")

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
        device_type, manufacturer = match_device_type(fpath, device_library)

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

        # Refine the string-based category using the actual sweep range.
        # Catches measurement_types that match the IdVd regex but describe
        # Blocking (e.g. "IdVd_Blocking") or 3rd-quadrant sweeps.
        stats = sweep_stats(headers, rows, map_columns)
        refined, reason = refine_category_by_sweep(measurement_category, stats)
        if refined != measurement_category:
            print(f"  RECLASSIFY {filename}: "
                  f"{measurement_category} → {refined} ({reason})")
            measurement_category = refined

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
                    bias_value, bias_channel, drain_bias_value,
                    compliance_ch1, compliance_ch2,
                    meas_time, hold_time, plc, sample_num,
                    sweep_mode, step_num, step_start, step_stop,
                    delay_time, dc_only, meas_channels, raw_tsp,
                    file_hash, device_type, manufacturer
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                ) RETURNING id
            """, (
                experiment, device_id, measurement_type, measurement_category,
                filename, fpath, tsp_path,
                ','.join(headers), len(rows),
                tsp_params.get('sweep_start'), tsp_params.get('sweep_stop'),
                tsp_params.get('sweep_points'),
                tsp_params.get('bias_value'), tsp_params.get('bias_channel'),
                tsp_params.get('drain_bias_value'),
                tsp_params.get('compliance_ch1'), tsp_params.get('compliance_ch2'),
                tsp_params.get('meas_time'), tsp_params.get('hold_time'),
                tsp_params.get('plc'), tsp_params.get('sample_num'),
                tsp_params.get('sweep_mode'), tsp_params.get('step_num'),
                tsp_params.get('step_start'), tsp_params.get('step_stop'),
                tsp_params.get('delay_time'), tsp_params.get('dc_only'),
                tsp_params.get('meas_channels', ''), tsp_params.get('raw_tsp', ''),
                file_hash, device_type, manufacturer
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

                # Fallback: for IdVg/Vth runs where TSP didn't provide a
                # drain bias, compute the per-run mean V_drain from the data
                # so that all points in the run share one v_drain_bin.
                # Without this, dual-sweep Vth tests (V_drain swept in sync
                # with V_gate) get fragmented across multiple bias bins.
                if (measurement_category in ('IdVg', 'Vth')
                        and tsp_params.get('drain_bias_value') is None):
                    v_drain_vals = [
                        r[4] for r in batch   # v_drain is index 4 in batch tuple
                        if r[4] is not None and abs(r[4]) < 1e30
                    ]
                    if v_drain_vals:
                        mean_vd = sum(v_drain_vals) / len(v_drain_vals)
                        cur.execute(
                            "UPDATE baselines_metadata "
                            "SET drain_bias_value = %s WHERE id = %s",
                            (mean_vd, meta_id)
                        )

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

    # Flag likely-irradiated measurements (data-driven Vth analysis)
    print("\nFlagging likely-irradiated measurements (Vth analysis)...")
    cur.execute(FLAG_IRRADIATED_SQL)
    cur.execute(
        "SELECT COUNT(*) FROM baselines_metadata "
        "WHERE is_likely_irradiated AND (data_source IS NULL OR data_source = 'baselines')"
    )
    n_flagged = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM baselines_metadata "
        "WHERE data_source IS NULL OR data_source = 'baselines'"
    )
    n_total = cur.fetchone()[0]
    conn.commit()
    print(f"  Flagged {n_flagged} of {n_total} records as likely irradiated.")
    if n_flagged:
        cur.execute("""
            SELECT experiment, COUNT(*), COUNT(DISTINCT device_id)
            FROM baselines_metadata
            WHERE is_likely_irradiated
              AND (data_source IS NULL OR data_source = 'baselines')
            GROUP BY experiment ORDER BY experiment
        """)
        for exp, cnt, ndev in cur.fetchall():
            print(f"    {exp}: {cnt} files, {ndev} devices")

    # ── Backfill irradiation campaign links ──────────────────────────────
    # Propagates irrad_campaign_id / irrad_role from experiment_campaign_map
    # to baselines_metadata.  No-op if the table doesn't exist yet.
    try:
        cur.execute("""
            UPDATE baselines_metadata md
            SET irrad_campaign_id = ecm.campaign_id,
                irrad_role        = ecm.role
            FROM experiment_campaign_map ecm
            WHERE md.experiment = ecm.experiment
              AND (md.irrad_campaign_id IS DISTINCT FROM ecm.campaign_id
                   OR md.irrad_role IS DISTINCT FROM ecm.role)
        """)
        n_linked = cur.rowcount
        conn.commit()
        if n_linked:
            print(f"\n  Linked {n_linked} metadata rows to irradiation campaigns.")
    except Exception:
        conn.rollback()

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

    # Verify (baselines only)
    cur.execute(
        "SELECT COUNT(*) FROM baselines_metadata "
        "WHERE data_source IS NULL OR data_source = 'baselines'"
    )
    meta_count = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM baselines_measurements m "
        "JOIN baselines_metadata md ON m.metadata_id = md.id "
        "WHERE md.data_source IS NULL OR md.data_source = 'baselines'"
    )
    meas_count = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM baselines_metadata "
        "WHERE tsp_path IS NOT NULL AND (data_source IS NULL OR data_source = 'baselines')"
    )
    tsp_count = cur.fetchone()[0]
    print(f"\nBaselines totals:")
    print(f"  baselines_metadata:     {meta_count} rows ({tsp_count} with TSP)")
    print(f"  baselines_measurements: {meas_count} rows")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
