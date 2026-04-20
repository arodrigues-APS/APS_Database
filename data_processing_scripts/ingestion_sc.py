#!/usr/bin/env python3
"""
Short-Circuit Ruggedness Data Ingestion Script
================================================
Parses pre-SC and post-SC MOSFET measurement CSV files from:
  1. /home/arodrigues/NAS/Common_Files/Short Circuit Measurements/ForDataAnalysis/
  2. /home/arodrigues/NAS/Common_Files/Short Circuit Measurements/curvetracermeasurements/

Loads them into the existing baselines_metadata / baselines_measurements tables
with additional SC-specific columns (data_source='sc_ruggedness').

Also handles oscilloscope SC event waveform CSVs (time-domain Vds/Id/Vgs captures).

Usage:
    python3 ingestion_sc.py              # full ingestion
    python3 ingestion_sc.py --subset     # only C3M0075120D for testing
    python3 ingestion_sc.py --rebuild    # drop SC data and re-ingest
"""

import os
import re
import csv
import sys
import argparse
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


# ── Configuration ────────────────────────────────────────────────────────────
from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from common import (load_device_library, compute_file_hash, find_matching_tsp,
                    map_columns, expand_multistep_rows,
                    categorize_measurement as categorize_sc_measurement)

SC_ROOTS = [
    "/home/arodrigues/NAS/Common_Files/Short Circuit Measurements/ForDataAnalysis",
    "/home/arodrigues/NAS/Common_Files/Short Circuit Measurements/curvetracermeasurements",
]

# File extensions to skip (non-data files found in SC directories)
SKIP_EXTENSIONS = {
    '.mat', '.png', '.gif', '.jpg', '.jpeg', '.bmp', '.tif',
    '.m', '.asv', '.fig',
    '.pdf', '.rtf', '.doc', '.docx',
    '.set', '.h5', '.hdf5',
    '.tsp',  # TSP files are parsed separately, not as measurement data
    '.xlsx', '.xls',  # no Excel files to process in SC data
}

# Files to explicitly skip
SKIP_FILES = {'MATLABscript.m', 'Document.rtf', '~$cument.rtf', 'current_list.csv'}

# Directories to skip entirely
SKIP_DIRS = {'New folder', 'Other', 'FailureAnalysis', '__pycache__'}


# ── Device Type Mapping ──────────────────────────────────────────────────────
# Maps top-level directory names to (device_type, manufacturer)

DEVICE_DIR_MAP = {
    # ForDataAnalysis
    'C2M0080120D':   ('C2M0080120D',     'Wolfspeed'),
    'C3M0075120D':   ('C3M0075120D',     'Wolfspeed'),
    'C2M_160mohm':   ('C2M0280120D',     'Wolfspeed'),
    'IMW120R060':    ('IMW120R060M1H',   'Infineon'),
    'SCT2080':       ('SCT2080KE',       'Rohm'),
    'SCT3080':       ('SCT3080AL',       'Rohm'),
    'LF':            ('LSIC1MO120E0080', 'Littlefuse'),
    'STM':           ('SCTW35N65G2V',    'STMicroelectronics'),
    'STMGen2':       ('SCTW35N65G2V',    'STMicroelectronics'),
    # curvetracermeasurements
    'CREE3Pin2G':           ('C2M0080120D',     'Wolfspeed'),
    'CREE3Pin3G':           ('C3M0075120D',     'Wolfspeed'),
    'CREE4Pin3G':           ('C3M0075120D',     'Wolfspeed'),
    'Infineon3Pin':         ('IMW120R090M1H',   'Infineon'),
    'infineon4Pin':         ('IMW120R060M1H',   'Infineon'),
    'RohmPlanar':           ('SCT3030AL',       'Rohm'),
    'RohmTrench':           ('SCT2080KE',       'Rohm'),
    'Littlefuse':           ('LSIC1MO120E0080', 'Littlefuse'),
    'STMicroelectronic':    ('SCTW35N65G2V',    'STMicroelectronics'),
    'STMicroGen2':          ('SCTW35N65G2V',    'STMicroelectronics'),
    'SiIGBT':               (None,              None),
    'DUTbodydiodecharacterisation': (None,       None),
    'hysteresis':           (None,              None),
    'hightemperatureLeakagecurrent': (None,      None),
    'IV29062020':           (None,              None),
}


# ── Schema Changes ───────────────────────────────────────────────────────────

ALTER_SCHEMA_SQL = """
-- Add SC-specific columns to baselines_metadata (idempotent)
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN data_source TEXT DEFAULT 'baselines'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN test_condition TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sc_voltage_v DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sc_duration_us DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sc_vgs_on_v DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sc_vgs_off_v DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sc_condition_label TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sc_sequence_num INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sample_group TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN is_sc_degraded BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Backfill existing rows
UPDATE baselines_metadata SET data_source = 'baselines' WHERE data_source IS NULL;

-- New indexes for SC queries
CREATE INDEX IF NOT EXISTS idx_baselines_meta_data_source ON baselines_metadata(data_source);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_test_condition ON baselines_metadata(test_condition);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_sample_group ON baselines_metadata(sample_group);
"""


# ── SC Views ─────────────────────────────────────────────────────────────────

SC_VIEWS_SQL = """
-- Main SC ruggedness view
DROP VIEW IF EXISTS sc_ruggedness_view CASCADE;
CREATE VIEW sc_ruggedness_view AS
SELECT
    m.id AS measurement_id,
    md.id AS metadata_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.measurement_type,
    md.measurement_category,
    md.filename,
    md.test_condition,
    md.sc_voltage_v,
    md.sc_duration_us,
    md.sc_vgs_on_v,
    md.sc_vgs_off_v,
    md.sc_condition_label,
    md.sc_sequence_num,
    md.sample_group,
    md.is_sc_degraded,
    md.sweep_start, md.sweep_stop, md.sweep_points,
    md.bias_value, md.drain_bias_value,
    m.point_index,
    m.step_index,
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN m.v_gate ELSE NULL END AS v_gate,
    CASE WHEN m.i_gate IS NOT NULL AND ABS(m.i_gate) < 1e30
         THEN m.i_gate ELSE NULL END AS i_gate,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN m.v_drain ELSE NULL END AS v_drain,
    CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
         THEN m.i_drain ELSE NULL END AS i_drain,
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END AS v_gate_r,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN ROUND(m.v_drain::numeric, 2)::double precision ELSE NULL END AS v_drain_r,
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END AS v_gate_bin,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 2)::double precision
        ELSE NULL
    END AS v_drain_bin,
    m.rds, m.bv, m.time_val
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id
WHERE md.data_source = 'sc_ruggedness';

-- SC waveform view (time-domain oscilloscope captures)
DROP VIEW IF EXISTS sc_waveform_view CASCADE;
CREATE VIEW sc_waveform_view AS
SELECT
    m.id AS measurement_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.sample_group,
    md.sc_voltage_v,
    md.sc_duration_us,
    md.sc_vgs_on_v,
    md.sc_vgs_off_v,
    md.sc_condition_label,
    md.filename,
    m.time_val * 1e6 AS time_us,
    m.v_drain AS vds,
    m.i_drain AS id_drain,
    m.v_gate AS vgs,
    m.point_index
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id
WHERE md.data_source = 'sc_ruggedness'
  AND md.measurement_category = 'SC_Waveform';

-- Degradation summary view
DROP VIEW IF EXISTS sc_degradation_summary CASCADE;
CREATE VIEW sc_degradation_summary AS
SELECT
    md.device_type,
    md.manufacturer,
    md.sample_group,
    md.test_condition,
    md.sc_voltage_v,
    md.sc_duration_us,
    md.sc_condition_label,
    md.measurement_category,
    ROUND(m.v_gate::numeric, 2)::double precision  AS v_gate_bin,
    ROUND(m.v_drain::numeric, 2)::double precision AS v_drain_bin,
    AVG(m.i_drain) AS avg_i_drain,
    AVG(m.i_gate) AS avg_i_gate,
    AVG(ABS(m.i_drain)) AS avg_abs_i_drain,
    COUNT(*) AS n_points
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id
WHERE md.data_source = 'sc_ruggedness'
  AND md.measurement_category NOT IN ('SC_Waveform')
  AND (m.v_gate  IS NULL OR ABS(m.v_gate)  < 1e30)
  AND (m.i_drain IS NULL OR ABS(m.i_drain) < 1e30)
  AND (m.v_drain IS NULL OR ABS(m.v_drain) < 1e30)
  -- gate-swept categories must have a non-NULL v_gate so v_gate_bin is valid
  AND NOT (md.measurement_category IN ('IdVg','Vth','Igss','Subthreshold')
           AND m.v_gate IS NULL)
  -- drain-swept categories must have a non-NULL v_drain so v_drain_bin is valid
  AND NOT (md.measurement_category IN ('IdVd','Blocking','3rd_Quadrant','Bodydiode')
           AND m.v_drain IS NULL)
GROUP BY md.device_type, md.manufacturer, md.sample_group,
         md.test_condition, md.sc_voltage_v, md.sc_duration_us,
         md.sc_condition_label, md.measurement_category,
         ROUND(m.v_gate::numeric, 2)::double precision,
         ROUND(m.v_drain::numeric, 2)::double precision;
"""


# ── Post-Ingestion SC Degradation Flagging ───────────────────────────────────

FLAG_SC_DEGRADED_SQL = """
-- Reset flags for SC data
UPDATE baselines_metadata SET is_sc_degraded = FALSE
WHERE data_source = 'sc_ruggedness';

WITH
-- Step 1: Extract approximate Vth per file from IdVg/Vth measurements
-- Vth = lowest Vg where |Id| first exceeds 1 mA
sc_per_file_vth AS (
    SELECT md.id AS metadata_id,
           md.device_type,
           md.device_id,
           md.sample_group,
           md.test_condition,
           md.sc_condition_label,
           MIN(m.v_gate) AS vth_approx
    FROM baselines_measurements m
    JOIN baselines_metadata md ON m.metadata_id = md.id
    WHERE md.data_source = 'sc_ruggedness'
      AND md.measurement_category IN ('IdVg', 'Vth', 'Subthreshold')
      AND m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
      AND m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
      AND ABS(m.i_drain) > 0.001
    GROUP BY md.id, md.device_type, md.device_id, md.sample_group,
             md.test_condition, md.sc_condition_label
),

-- Step 2: Pristine population statistics per device_type
pristine_stats AS (
    SELECT device_type,
           PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY vth_approx) AS median_vth,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vth_approx) AS q1_vth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vth_approx) AS q3_vth
    FROM sc_per_file_vth
    WHERE test_condition = 'pristine'
      AND device_type IS NOT NULL
    GROUP BY device_type
),

-- Step 3: Flag post-SC files with significant Vth shift
flagged_sc AS (
    SELECT f.metadata_id, f.sample_group
    FROM sc_per_file_vth f
    LEFT JOIN pristine_stats p ON f.device_type = p.device_type
    WHERE f.test_condition = 'post_sc'
      AND (f.vth_approx < 0
           OR (p.device_type IS NOT NULL
               AND f.vth_approx < p.median_vth - 3.0 * (p.q3_vth - p.q1_vth)))
),

-- Step 4: Propagate flag to all post-SC files for the same sample_group
all_flagged AS (
    SELECT md.id AS metadata_id
    FROM baselines_metadata md
    JOIN flagged_sc f ON md.sample_group = f.sample_group
    WHERE md.data_source = 'sc_ruggedness'
      AND md.test_condition = 'post_sc'
)

UPDATE baselines_metadata SET is_sc_degraded = TRUE
WHERE id IN (SELECT metadata_id FROM all_flagged);
"""


# ── SC Condition Parsing ─────────────────────────────────────────────────────

def _convert_point_decimal(s):
    """Convert 'point' notation to decimal: '2point5' -> 2.5"""
    if s is None:
        return None
    s = str(s)
    s = re.sub(r'point', '.', s, flags=re.IGNORECASE)
    try:
        return float(s)
    except ValueError:
        return None


def parse_sc_condition(path_str):
    """
    Parse SC event conditions from a directory path or filename.

    Returns dict with keys: sc_voltage_v, sc_duration_us, sc_vgs_on_v,
    sc_vgs_off_v, sc_condition_label, sc_sequence_num.
    All values None if no SC condition found.
    """
    result = {
        'sc_voltage_v': None,
        'sc_duration_us': None,
        'sc_vgs_on_v': None,
        'sc_vgs_off_v': None,
        'sc_condition_label': None,
        'sc_sequence_num': None,
    }

    if not path_str:
        return result

    # Normalize path separators
    s = path_str.replace('\\', '/')

    # Try each directory component and the filename
    parts = s.split('/')

    voltage = None
    duration = None
    vgs_on = None
    vgs_off = None
    sequence = None

    for part in parts:
        # Pattern: {N}_after{V}V{T}us  (numbered sequence)
        m = re.match(r'^(\d+)_after(\d+)V(\d+(?:point\d+)?)us', part, re.IGNORECASE)
        if m:
            sequence = int(m.group(1))
            voltage = float(m.group(2))
            duration = _convert_point_decimal(m.group(3))
            continue

        # Pattern: afterSC{V}V{T}us[Vgs{on}[V][minus{off}V]]
        # Also handles: after{V}V{T}us, postSC{V}V{T}us, post{V}V{T}us
        m = re.match(
            r'(?:after|post)(?:_)?(?:SC)?[_]?(\d+)V[_]?(\d+(?:point\d+)?(?:\.\d+)?)us'
            r'(?:[_]?[Vv]gs(\d+)(?:V)?(?:minus|_minus_?|_)(\d+)V?)?',
            part, re.IGNORECASE
        )
        if m:
            voltage = float(m.group(1))
            duration = _convert_point_decimal(m.group(2))
            if m.group(3):
                vgs_on = float(m.group(3))
            if m.group(4):
                vgs_off = -float(m.group(4))
            continue

        # Pattern: afterSC{V}V  (voltage only, in container dir like IM_C2M_Feb1_afterSC800V)
        m = re.search(r'afterSC(\d+)V$', part, re.IGNORECASE)
        if m:
            voltage = float(m.group(1))
            continue

        # Pattern: {V}V_{T}us or {V}V{T}us (bare condition in subdir)
        m = re.match(r'^(\d+)V[_]?(\d+(?:point\d+)?(?:\.\d+)?)us$', part, re.IGNORECASE)
        if m:
            voltage = float(m.group(1))
            duration = _convert_point_decimal(m.group(2))
            continue

        # Pattern: Vgs{on}V{_}{off}V or Vgs{on}minus{off}V (standalone Vgs part)
        m = re.search(
            r'[Vv]gs(\d+)(?:V)?(?:minus|_minus_?|_)(\d+)V?',
            part
        )
        if m and vgs_on is None:
            vgs_on = float(m.group(1))
            vgs_off = -float(m.group(2))

        # Pattern: Vgs{on}V_{off}V (positive off, e.g. Vgs15V_0V)
        m = re.search(r'[Vv]gs(\d+)V[_]0V', part)
        if m and vgs_on is None:
            vgs_on = float(m.group(1))
            vgs_off = 0.0

        # Ordinal sequence from directory name (first, second, ...)
        ordinals = {'first': 1, 'second': 2, 'third': 3, 'fourth': 4,
                     'fifth': 5, 'sixth': 6, 'seventh': 7, 'eighth': 8}
        for word, num in ordinals.items():
            if word in part.lower() and sequence is None:
                sequence = num

    # Also check filename for SC waveform CSVs
    fname = parts[-1] if parts else ''
    # Pattern: {prefix}{V}V{T}us.csv (e.g. Rohm600V8us.csv, 600V2us.csv)
    m = re.match(
        r'^(?:[A-Za-z_]*?)(\d+)V(\d+(?:point\d+)?(?:\.\d+)?)us\.csv$',
        fname, re.IGNORECASE
    )
    if m and voltage is None:
        voltage = float(m.group(1))
        duration = _convert_point_decimal(m.group(2))

    # Also check for postSC_ in filename
    m = re.match(
        r'^postSC[_](\d+)V[_](\d+(?:point\d+)?)us',
        fname, re.IGNORECASE
    )
    if m and voltage is None:
        voltage = float(m.group(1))
        duration = _convert_point_decimal(m.group(2))

    if voltage is not None:
        result['sc_voltage_v'] = voltage
        result['sc_duration_us'] = duration
        result['sc_vgs_on_v'] = vgs_on
        result['sc_vgs_off_v'] = vgs_off
        result['sc_sequence_num'] = sequence

        # Build human-readable label
        parts_label = [f"{int(voltage)}V"]
        if duration is not None:
            # Format nicely: 3.0 -> "3us", 2.5 -> "2.5us"
            if duration == int(duration):
                parts_label.append(f"{int(duration)}us")
            else:
                parts_label.append(f"{duration}us")
        if vgs_on is not None:
            parts_label.append(f"Vgs{int(vgs_on)}")
        if vgs_off is not None:
            if vgs_off == 0:
                parts_label.append("0V")
            else:
                parts_label.append(f"minus{int(abs(vgs_off))}V")
        result['sc_condition_label'] = '_'.join(parts_label)

    return result


# ── Test Condition Classification ────────────────────────────────────────────

# Patterns that indicate post-SC data
POST_SC_PATTERNS = [
    r'post_SC_',
    r'post_sc_',
    r'postSC',
    r'post\d+V',
    r'after.*SC',
    r'afterSC',
    r'after\d+V',
    r'\d+_after\d+V',
    r'_afterSC\d+V',
    r'posttailcurrent',
]

# Patterns that indicate pristine data
PRISTINE_PATTERNS = [
    r'preSC',
    r'pre_SC',
    r'beforeSC',
    r'^static$',
]

_post_sc_re = re.compile('|'.join(POST_SC_PATTERNS), re.IGNORECASE)
_pristine_re = re.compile('|'.join(PRISTINE_PATTERNS), re.IGNORECASE)


def classify_test_condition(csv_path, root_dir):
    """
    Determine whether a file represents pristine or post-SC data.

    Returns (test_condition, sample_group) where:
      test_condition: 'pristine' or 'post_sc'
      sample_group: string identifying the physical device sample
    """
    rel_path = os.path.relpath(csv_path, root_dir)
    path_parts = rel_path.split(os.sep)

    # Check if any path component indicates post-SC
    full_path_str = '/'.join(path_parts)
    is_post_sc = bool(_post_sc_re.search(full_path_str))
    is_pristine = bool(_pristine_re.search(full_path_str))

    if is_post_sc and not is_pristine:
        test_condition = 'post_sc'
    elif is_pristine and not is_post_sc:
        test_condition = 'pristine'
    elif is_post_sc and is_pristine:
        # Ambiguous -- check which is more specific in the path
        # post-SC in a deeper directory wins
        test_condition = 'post_sc'
    else:
        # Default: if no SC-related keywords, treat as pristine
        test_condition = 'pristine'

    # Extract sample_group: the sample identifier that links pre/post measurements
    sample_group = _extract_sample_group(path_parts, root_dir)

    return test_condition, sample_group


def _extract_sample_group(path_parts, root_dir):
    """
    Extract a sample group identifier from the path hierarchy.

    For ForDataAnalysis:
      C3M0075120D/post_SC_IM_H3_C4/afterSC600V3us/IdVg.csv -> IM_H3_C4
      C3M0075120D/C3M_C16/IdVg_Vd.csv -> C3M_C16
      C2M0080120D/IMC3/post_SC_IMC3/IdVg.csv -> IMC3
      C2M0080120D/CSV/IMC31/600V2us.csv -> IMC31
      C2M0080120D/1_after400V16us/1_IdVg.csv -> unknown_seq1
      SCT2080/IM_HR_R2/IdVg.csv -> IM_HR_R2
      SCT2080/Rohm600V8us.csv -> SCT2080_waveform

    For curvetracermeasurements:
      Infineon3Pin/I1/post400V22us/IdVg.csv -> I1
      RohmTrench/RT3/IdVg.csv -> RT3
    """
    if len(path_parts) < 2:
        return 'unknown'

    device_dir = path_parts[0]  # top-level device directory

    # curvetracermeasurements style: Device/Sample/[postCondition/]file.csv
    root_name = os.path.basename(root_dir)
    if root_name == 'curvetracermeasurements':
        if len(path_parts) >= 3:
            return path_parts[1]  # sample dir (I1, RT3, L12, etc.)
        return device_dir

    # ForDataAnalysis style -- more complex hierarchy
    # Check for post_SC_{sample} pattern
    for part in path_parts[1:]:
        m = re.match(r'^post_SC_(.+)$', part, re.IGNORECASE)
        if m:
            return m.group(1)

    # Check for CSV/{sample}/ pattern (SC waveform dirs)
    for i, part in enumerate(path_parts):
        if part == 'CSV' and i + 1 < len(path_parts) - 1:
            return path_parts[i + 1]

    # Check for IM_C2M_{name}_preSC / IM_C2M_{name}_afterSC patterns
    for part in path_parts[1:]:
        m = re.match(r'^IM_C2?M?_(.+?)_(?:pre|after)SC', part, re.IGNORECASE)
        if m:
            return m.group(1)

    # Check for {N}_after{V}V{T}us pattern (numbered sequences)
    for part in path_parts[1:]:
        m = re.match(r'^(\d+)_after\d+V', part)
        if m:
            return f"unknown_seq{m.group(1)}"

    # SC waveform files at top level (e.g. SCT2080/Rohm600V8us.csv)
    fname = path_parts[-1]
    if re.match(r'^[A-Za-z_]*?\d+V\d+', fname):
        return f"{device_dir}_waveform"

    # Default: use the first subdirectory under device as sample
    if len(path_parts) >= 3:
        candidate = path_parts[1]
        # Skip directories that are clearly SC conditions, not samples
        if not re.match(r'^\d+_after|^post|^after|^CSV$|^static$', candidate, re.IGNORECASE):
            return candidate

    if len(path_parts) >= 2 and path_parts[1] != path_parts[-1]:
        return path_parts[1]

    return 'unknown'


# ── Measurement Classification ───────────────────────────────────────────────

def classify_sc_measurement(filename, path_parts):
    """
    Classify measurement from SC data filename and path.

    Returns (device_id, measurement_type).
    In SC data, device_id often comes from the sample directory, not the filename.
    """
    stem = Path(filename).stem

    # Strip numbered prefix (e.g. "1_IdVg" -> "IdVg")
    stem_clean = re.sub(r'^\d+_', '', stem)

    # Strip postSC condition prefix (e.g. "postSC_800V_2point5us_IdVg" -> "IdVg")
    stem_clean = re.sub(
        r'^postSC[_]?\d+V[_]?\d+(?:point\d+)?us[_]?',
        '', stem_clean, flags=re.IGNORECASE
    )

    # Try standard device_id extraction from filename (for files like C11_IdVg_Vd1020.csv)
    # Pattern: {DeviceID}_{MeasurementType}
    m = re.match(r'^(C\d{1,2}|C[23]M_C\d+|R\d{1,2}|I\d{1,2}|IB\d+|L\d{1,2}|ST\d+|RT\d+|DUT\d+|IMC\d+|IM_HR_R\d+)_(.+)$', stem_clean, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    # For generic filenames (IdVg.csv, blocking.csv), device_id comes from path
    device_id = _device_id_from_path(path_parts)

    return device_id, stem_clean


def _device_id_from_path(path_parts):
    """Extract device_id from the sample directory in the path."""
    if len(path_parts) < 2:
        return 'unknown'

    # For curvetracermeasurements: path_parts[1] is the sample (I1, RT3, etc.)
    # For ForDataAnalysis: varies, but sample is usually path_parts[1] or extracted from post_SC_

    for part in path_parts[1:-1]:  # exclude device dir and filename
        # Skip SC condition directories
        if re.match(r'^(?:post|after|postSC|\d+_after)\d*V?', part, re.IGNORECASE):
            continue
        if part in ('CSV', 'static', 'data', 'temp', 'lib',
                    'subthreshold measurements', '4Pin', 'reverserecoveryandSC'):
            continue
        if re.match(r'^\d+V[_]?\d+', part):  # bare condition like 800V_3us
            continue
        # Use first non-condition directory as device_id
        return part

    return 'unknown'


# categorize_sc_measurement() is imported from common.categorize_measurement.


# ── CSV Parsing ──────────────────────────────────────────────────────────────

def detect_sc_waveform(filepath):
    """
    Detect if a CSV file is an oscilloscope SC event waveform.

    Returns True if it matches known SC waveform patterns:
      1. Filename matches {prefix}{V}V{T}us.csv
      2. File is in a CSV/ subdirectory with bare condition filenames
      3. First row is empty/commas-only (headerless oscilloscope dump)
      4. Header row contains 'time' and 'Vds'
    """
    fname = os.path.basename(filepath)

    # Pattern 1: filename like Rohm600V8us.csv, 600V2us.csv, C2M_IMC32_600V7us.csv
    if re.match(r'^(?:[A-Za-z_]*?)(\d+)V(\d+(?:\.\d+)?)us\.csv$', fname, re.IGNORECASE):
        return True

    # Pattern 2: in a CSV/ subdirectory
    if '/CSV/' in filepath or '\\CSV\\' in filepath:
        if re.match(r'^\d+V\d+', fname):
            return True

    # Pattern 3 & 4: peek at the first line
    try:
        with open(filepath, 'r', errors='replace') as f:
            first_line = f.readline().strip()
            if not first_line or first_line == ',,,':
                return True
            if 'vds' in first_line.lower() and 'time' in first_line.lower():
                return True
    except Exception:
        pass

    return False


def parse_sc_waveform(filepath):
    """
    Parse an oscilloscope SC waveform CSV.

    Returns (headers, rows) where:
      headers = ['time', 'v_drain', 'i_drain', 'v_gate']
      rows = list of [time, vds, id, vgs] float lists
    """
    headers = ['time', 'v_drain', 'i_drain', 'v_gate']
    rows = []

    try:
        with open(filepath, 'r', errors='replace') as f:
            reader = csv.reader(f)
            first_row = next(reader, None)

            if first_row is None:
                return headers, rows

            # Check if first row is header or empty
            first_str = ','.join(first_row).strip().lower()
            if first_str in ('', ',,,', ',,') or 'time' in first_str or 'vds' in first_str:
                pass  # skip header/empty row
            else:
                # First row is data
                try:
                    float_row = [float(c.strip()) if c.strip() else None for c in first_row[:4]]
                    if len(float_row) >= 4:
                        rows.append(float_row)
                except (ValueError, IndexError):
                    pass

            for row in reader:
                if not row or len(row) < 4:
                    continue
                try:
                    float_row = [float(c.strip()) if c.strip() else None for c in row[:4]]
                    rows.append(float_row)
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        print(f"  Warning: Could not read SC waveform {filepath}: {e}")

    return headers, rows


def parse_keithley_csv(filepath):
    """
    Parse a Keithley curve-tracer CSV with the 2-header-row format:
      Row 1: A(X),B(Y1),C,D(X),... (instrument channel labels)
      Row 2: V_Gate(1),V_Drain(1),I_Drain(1),... (actual column names)
      Row 3+: data

    Returns (headers, rows) where headers is from row 2.
    """
    headers = []
    rows = []

    try:
        with open(filepath, 'r', errors='replace') as f:
            reader = csv.reader(f)
            row1 = next(reader, None)
            if row1 is None:
                return headers, rows

            # Detect Keithley 2-header format: row 1 starts with A(X) or similar
            is_keithley_2header = False
            if row1 and len(row1) > 0:
                first_cell = row1[0].strip()
                if re.match(r'^[A-Z]\(', first_cell):
                    is_keithley_2header = True

            if is_keithley_2header:
                # Row 1 was channel labels, row 2 has real column names
                row2 = next(reader, None)
                if row2:
                    headers = [h.strip() for h in row2]
            else:
                # Single-header format (or no recognized header)
                headers = [h.strip() for h in row1]

            # Read data rows
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


# map_columns() and expand_multistep_rows() are imported from common.py.


# compute_file_hash() is imported from common.py.


def extract_experiment_name(csv_path, root_dir):
    """
    Extract the experiment name from the path.
    Format: SC_fda_{DeviceDir} or SC_ct_{DeviceDir}
    """
    root_name = os.path.basename(root_dir)
    prefix = 'SC_fda' if root_name == 'ForDataAnalysis' else 'SC_ct'

    rel = os.path.relpath(csv_path, root_dir)
    parts = rel.split(os.sep)
    if parts:
        return f"{prefix}_{parts[0]}"
    return f"{prefix}_unknown"


def map_device_type(csv_path, root_dir, device_library=None):
    """
    Map a file path to (device_type, manufacturer) using the directory mapping table.
    Falls back to substring search against device_library.
    """
    rel = os.path.relpath(csv_path, root_dir)
    parts = rel.split(os.sep)

    if parts:
        device_dir = parts[0]
        if device_dir in DEVICE_DIR_MAP:
            dt, mfr = DEVICE_DIR_MAP[device_dir]
            if dt is not None:
                return dt, mfr

    # Fallback: substring match against device_library
    if device_library:
        path_upper = csv_path.upper()
        for entry in device_library:
            if entry['part_number'].upper() in path_upper:
                return entry['part_number'], entry['manufacturer']

    return None, None


# load_device_library() is imported from common.py.


# find_matching_tsp() is imported from common.py.


# ── TSP Parser ───────────────────────────────────────────────────────────────

def parse_tsp_file(filepath):
    """
    Parse a .tsp file. Delegates to ingestion_baselines's full parser;
    falls back to empty params if unavailable.
    """
    try:
        from ingestion_baselines import parse_tsp_file as _parse_tsp_full
        return _parse_tsp_full(filepath)
    except ImportError:
        pass

    # Minimal fallback: return empty params
    return {
        'sweep_start': None, 'sweep_stop': None, 'sweep_points': None,
        'bias_value': None, 'bias_channel': None,
        'compliance_ch1': None, 'compliance_ch2': None,
        'meas_time': None, 'hold_time': None, 'plc': None,
        'sample_num': None, 'sweep_mode': None,
        'step_num': None, 'step_start': None, 'step_stop': None,
        'delay_time': None, 'dc_only': None,
        'meas_channels': '', 'raw_tsp': '',
        'drain_bias_value': None,
    }


# ── Main Ingestion ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Short Circuit Data Ingestion')
    parser.add_argument('--subset', action='store_true',
                        help='Only ingest C3M0075120D from ForDataAnalysis (for testing)')
    parser.add_argument('--rebuild', action='store_true',
                        help='Drop all SC data and re-ingest from scratch')
    args = parser.parse_args()

    start_time = perf_counter()

    print("=" * 70)
    print("Short-Circuit Ruggedness Data Ingestion")
    print("=" * 70)
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    if args.subset:
        print("Mode: SUBSET (C3M0075120D only)")
    print()

    # Connect
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Optionally rebuild SC data
    if args.rebuild:
        print("Dropping existing SC data...")
        cur.execute("DELETE FROM baselines_metadata WHERE data_source = 'sc_ruggedness'")
        conn.commit()
        n_deleted = cur.rowcount
        print(f"  Deleted {n_deleted} SC metadata rows (CASCADE cleans measurements).")

    # Apply schema changes
    print("Applying schema changes...")
    cur.execute(ALTER_SCHEMA_SQL)
    conn.commit()
    print("  Schema ready.")

    # Create SC views
    print("Creating SC views...")
    cur.execute(SC_VIEWS_SQL)
    conn.commit()
    print("  Views ready.")

    # Load device library
    print("\nLoading device library...")
    device_library = load_device_library(cur)
    print(f"  {len(device_library)} devices in library.")

    # Collect CSV files from all roots
    measurement_files = []  # list of (filepath, root_dir)

    for root_dir in SC_ROOTS:
        if not os.path.isdir(root_dir):
            print(f"  WARNING: Root not found: {root_dir}")
            continue

        for dirpath, dirnames, files in os.walk(root_dir):
            # Skip unwanted directories
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]

            rel_to_root = os.path.relpath(dirpath, root_dir)
            parts = rel_to_root.split(os.sep)
            device_dir = parts[0] if parts else ''

            # In subset mode, only process C3M0075120D
            if args.subset and device_dir != 'C3M0075120D':
                continue

            for fname in sorted(files):
                fl = fname.lower()
                ext = os.path.splitext(fl)[1]

                # Skip non-CSV and known junk
                if ext != '.csv':
                    continue
                if fname in SKIP_FILES:
                    continue

                fpath = os.path.join(dirpath, fname)
                measurement_files.append((fpath, root_dir))

    print(f"\nFound {len(measurement_files)} CSV files to process.")

    # Sync deletions: remove SC DB records for files no longer on disk
    if not args.subset:
        print("\nSyncing deletions...")
        on_disk_paths = set(fpath for fpath, _ in measurement_files)
        cur.execute(
            "SELECT id, csv_path FROM baselines_metadata "
            "WHERE csv_path IS NOT NULL AND data_source = 'sc_ruggedness'"
        )
        db_rows = cur.fetchall()
        stale_ids = [row[0] for row in db_rows if row[1] not in on_disk_paths]
        if stale_ids:
            cur.execute("DELETE FROM baselines_metadata WHERE id = ANY(%s)",
                        (stale_ids,))
            conn.commit()
            print(f"  Removed {len(stale_ids)} stale SC record(s).")
        else:
            print("  No stale records found.")

    # Track statistics
    total_points = 0
    files_loaded = 0
    files_skipped = 0
    files_error = 0
    files_waveform = 0
    experiment_stats = {}

    for idx, (fpath, root_dir) in enumerate(measurement_files):
        filename = os.path.basename(fpath)
        rel_path = os.path.relpath(fpath, root_dir)
        path_parts = rel_path.split(os.sep)

        # File hash for dedup
        try:
            file_hash = compute_file_hash(fpath)
        except Exception as e:
            print(f"  [{idx+1}] ERROR (hash): {filename}: {e}")
            files_error += 1
            continue

        # Check if already loaded
        cur.execute("SELECT id FROM baselines_metadata WHERE file_hash = %s", (file_hash,))
        if cur.fetchone():
            files_skipped += 1
            continue

        # Classify
        experiment = extract_experiment_name(fpath, root_dir)
        test_condition, sample_group = classify_test_condition(fpath, root_dir)
        sc_cond = parse_sc_condition(fpath)
        device_type, manufacturer = map_device_type(fpath, root_dir, device_library)

        # Detect SC waveform vs curve tracer CSV
        is_waveform = detect_sc_waveform(fpath)

        if is_waveform:
            headers, rows = parse_sc_waveform(fpath)
            measurement_category = 'SC_Waveform'
            device_id = sample_group
            measurement_type = Path(filename).stem
            # SC waveforms are always post_sc data showing the actual event
            test_condition = 'post_sc'
        else:
            headers, rows = parse_keithley_csv(fpath)
            device_id, measurement_type = classify_sc_measurement(filename, path_parts)
            measurement_category = categorize_sc_measurement(measurement_type, filename)

        # Fallback: use sample_group as device_id when filename didn't yield one
        if device_id == 'unknown' and sample_group != 'unknown':
            device_id = sample_group
        # Reverse fallback: use device_id as sample_group when path didn't yield one
        if sample_group == 'unknown' and device_id != 'unknown':
            sample_group = device_id

        if not rows:
            files_skipped += 1
            continue

        # Find and parse matching TSP (if available)
        tsp_path = find_matching_tsp(fpath)
        tsp_params = {}
        if tsp_path:
            tsp_params = parse_tsp_file(tsp_path)

        # Build SC condition label from directory name
        sc_label = sc_cond.get('sc_condition_label')

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
                    file_hash, device_type, manufacturer,
                    data_source, test_condition,
                    sc_voltage_v, sc_duration_us,
                    sc_vgs_on_v, sc_vgs_off_v,
                    sc_condition_label, sc_sequence_num,
                    sample_group
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
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s
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
                file_hash, device_type, manufacturer,
                'sc_ruggedness', test_condition,
                sc_cond['sc_voltage_v'], sc_cond['sc_duration_us'],
                sc_cond['sc_vgs_on_v'], sc_cond['sc_vgs_off_v'],
                sc_label, sc_cond['sc_sequence_num'],
                sample_group,
            ))
            meta_id = cur.fetchone()[0]
        except Exception as e:
            print(f"  [{idx+1}/{len(measurement_files)}] ERROR (metadata): {filename}: {e}")
            conn.rollback()
            files_error += 1
            continue

        # Expand and insert measurements
        if is_waveform:
            # SC waveform: simple 4-column mapping
            batch = []
            for pidx, row in enumerate(rows):
                batch.append((
                    meta_id, pidx,
                    row[3] if len(row) > 3 else None,  # v_gate = Vgs
                    None,  # i_gate
                    row[1] if len(row) > 1 else None,  # v_drain = Vds
                    row[2] if len(row) > 2 else None,  # i_drain = Id
                    None,  # rds
                    None,  # bv
                    row[0] if len(row) > 0 else None,  # time_val
                    0,     # step_index
                ))
        else:
            # Curve tracer: use expand_multistep_rows
            expanded = expand_multistep_rows(headers, rows)
            batch = []
            for step_idx, mapped, point_idx in expanded:
                batch.append((
                    meta_id, point_idx,
                    mapped['v_gate'], mapped['i_gate'],
                    mapped['v_drain'], mapped['i_drain'],
                    mapped['rds'], mapped['bv'], mapped['time_val'],
                    step_idx,
                ))

        if batch:
            try:
                execute_values(cur, """
                    INSERT INTO baselines_measurements
                    (metadata_id, point_index, v_gate, i_gate, v_drain, i_drain, rds, bv, time_val, step_index)
                    VALUES %s
                """, batch, page_size=5000)

                # Fallback drain_bias_value for IdVg/Vth runs without TSP
                if (measurement_category in ('IdVg', 'Vth', 'Subthreshold')
                        and tsp_params.get('drain_bias_value') is None
                        and not is_waveform):
                    v_drain_vals = [
                        r[4] for r in batch
                        if r[4] is not None and abs(r[4]) < 1e30
                    ]
                    if v_drain_vals:
                        mean_vd = sum(v_drain_vals) / len(v_drain_vals)
                        cur.execute(
                            "UPDATE baselines_metadata SET drain_bias_value = %s WHERE id = %s",
                            (mean_vd, meta_id)
                        )

                total_points += len(batch)
                files_loaded += 1
                if is_waveform:
                    files_waveform += 1

                # Track per-experiment stats
                if experiment not in experiment_stats:
                    experiment_stats[experiment] = {'files': 0, 'points': 0, 'tsp': 0, 'waveforms': 0}
                experiment_stats[experiment]['files'] += 1
                experiment_stats[experiment]['points'] += len(batch)
                if tsp_path:
                    experiment_stats[experiment]['tsp'] += 1
                if is_waveform:
                    experiment_stats[experiment]['waveforms'] += 1

                if (idx + 1) % 100 == 0:
                    print(f"  [{idx+1}/{len(measurement_files)}] Progress: {files_loaded} loaded, "
                          f"{total_points} pts ({experiment}/{filename})")

            except Exception as e:
                print(f"  [{idx+1}/{len(measurement_files)}] ERROR (data): {filename}: {e}")
                conn.rollback()
                files_error += 1
                continue

        # Commit every 100 files
        if files_loaded % 100 == 0 and files_loaded > 0:
            conn.commit()

    conn.commit()

    # Flag SC-degraded measurements
    print("\nFlagging SC-degraded measurements (Vth analysis)...")
    try:
        cur.execute(FLAG_SC_DEGRADED_SQL)
        cur.execute("SELECT COUNT(*) FROM baselines_metadata WHERE data_source = 'sc_ruggedness' AND is_sc_degraded")
        n_flagged = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM baselines_metadata WHERE data_source = 'sc_ruggedness'")
        n_total = cur.fetchone()[0]
        conn.commit()
        print(f"  Flagged {n_flagged} of {n_total} SC records as degraded.")
        if n_flagged:
            cur.execute("""
                SELECT experiment, COUNT(*), COUNT(DISTINCT sample_group)
                FROM baselines_metadata
                WHERE data_source = 'sc_ruggedness' AND is_sc_degraded
                GROUP BY experiment ORDER BY experiment
            """)
            for exp, cnt, ngrp in cur.fetchall():
                print(f"    {exp}: {cnt} files, {ngrp} sample groups")
    except Exception as e:
        print(f"  Warning: SC degradation flagging failed: {e}")
        conn.rollback()

    # Refresh materialized view if it exists (for baselines compatibility)
    try:
        cur.execute("REFRESH MATERIALIZED VIEW baselines_run_max_current")
        conn.commit()
        print("  Refreshed baselines_run_max_current materialized view.")
    except Exception:
        conn.rollback()

    # Print results
    elapsed = perf_counter() - start_time

    print("\n" + "=" * 70)
    print("Ingestion complete!")
    print(f"  Time taken:     {elapsed:.1f} seconds")
    print(f"  Files loaded:   {files_loaded}")
    print(f"  SC waveforms:   {files_waveform}")
    print(f"  Files skipped:  {files_skipped} (already loaded or empty)")
    print(f"  Files errored:  {files_error}")
    print(f"  Total points:   {total_points}")
    print()
    print("Per-experiment breakdown:")
    for exp in sorted(experiment_stats.keys()):
        s = experiment_stats[exp]
        print(f"  {exp}:")
        print(f"    Files: {s['files']}  |  Points: {s['points']}  |  "
              f"With TSP: {s['tsp']}  |  Waveforms: {s['waveforms']}")
    print("=" * 70)

    # Verify
    cur.execute("SELECT COUNT(*) FROM baselines_metadata WHERE data_source = 'sc_ruggedness'")
    meta_count = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM baselines_measurements m
        JOIN baselines_metadata md ON m.metadata_id = md.id
        WHERE md.data_source = 'sc_ruggedness'
    """)
    meas_count = cur.fetchone()[0]
    print(f"\nSC database totals:")
    print(f"  SC metadata rows:     {meta_count}")
    print(f"  SC measurement rows:  {meas_count}")

    # Quick sanity check
    cur.execute("""
        SELECT test_condition, measurement_category, COUNT(*)
        FROM baselines_metadata
        WHERE data_source = 'sc_ruggedness'
        GROUP BY test_condition, measurement_category
        ORDER BY test_condition, measurement_category
    """)
    print("\nBreakdown by test_condition x measurement_category:")
    for tc, mc, cnt in cur.fetchall():
        print(f"  {tc or 'NULL':12s} | {mc or 'NULL':20s} | {cnt}")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
