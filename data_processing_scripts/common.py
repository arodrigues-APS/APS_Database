"""
Shared utility functions for APS Database ingestion scripts.

Extracted from baselines_ingestion.py and sc_ingestion.py to eliminate
duplication.  Both scripts import from here instead of maintaining their
own copies.
"""

import os
import re
import hashlib
from pathlib import Path


# ── Device Library ──────────────────────────────────────────────────────────

def load_device_library(cur):
    """
    Load the device library from the device_library SQL table.
    Returns a list of dicts sorted by part_number length descending
    (so longer/more-specific part numbers match first).
    """
    cur.execute("""
        SELECT part_number, device_category, manufacturer,
               voltage_rating, rdson_mohm, current_rating_a, package_type
        FROM device_library
        ORDER BY LENGTH(part_number) DESC
    """)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── File Hashing ────────────────────────────────────────────────────────────

def compute_file_hash(filepath):
    """Compute MD5 hash of a file for deduplication."""
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


# ── TSP File Matching ───────────────────────────────────────────────────────

def find_matching_tsp(csv_path):
    """
    Given a CSV path, find the matching TSP file in sibling lib/ directories.

    Strategy:
      1. Look in sibling lib/ directory with exact filename match
      2. Strip _appendN suffix and try again
      3. Strip numbered prefix (e.g. "1_IdVg" -> "IdVg") and try again
      4. Search up the directory tree for lib/ folders
    """
    csv_p = Path(csv_path)
    stem = csv_p.stem

    # Build list of stems to try
    stems_to_try = [stem]
    stripped = re.sub(r'_append\d*$', '', stem)
    if stripped != stem:
        stems_to_try.append(stripped)
    # Also try without numbered prefix (e.g. "1_IdVg" -> "IdVg")
    stripped2 = re.sub(r'^\d+_', '', stem)
    if stripped2 != stem and stripped2 not in stems_to_try:
        stems_to_try.append(stripped2)

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


# ── Column Mapping ──────────────────────────────────────────────────────────

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
    For multi-step CSV files with columns like V_Drain(1), I_Drain(1),
    V_Drain(2), I_Drain(2), ... expand into separate rows per step.

    Returns list of (step_index, mapped_values_dict, point_index)
    """
    # Detect numbered columns
    numbered_cols = {}
    for i, h in enumerate(headers):
        m = re.match(r'(.+)\((\d+)\)', h)
        if m:
            base = m.group(1).strip()
            step = int(m.group(2))
            if step not in numbered_cols:
                numbered_cols[step] = {}
            numbered_cols[step][base] = i

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


# ── Measurement Category Classifier ────────────────────────────────────────

def categorize_measurement(measurement_type, filename=''):
    """
    Group raw measurement_type strings into dashboard-filterable categories.

    Unified classifier covering both baselines and SC data.  More specific
    categories (SC_Waveform, Bodydiode, etc.) are checked before the generic
    ones so that e.g. "bodydiode_IdVd" maps to Bodydiode, not IdVd.

    Returns one of:
        SC_Waveform, Bodydiode, Subthreshold, Hysteresis, ChannelDiode,
        IdVg, IdVd, 3rd_Quadrant, Blocking, Igss, Vth, Rdson, Irradiation,
        Other
    """
    t = measurement_type or ''
    tl = t.lower()

    # Specialized categories (check first)
    if 'waveform' in tl or 'sc_waveform' in tl:
        return 'SC_Waveform'
    if re.search(r'bodydiode|body_diode|bodydiodev', tl):
        return 'Bodydiode'
    if re.search(r'subthreshold|subth', tl):
        return 'Subthreshold'
    if re.search(r'hysteresis|hyst', tl):
        return 'Hysteresis'
    if re.search(r'channeldiode|moschanneldiode', tl):
        return 'ChannelDiode'

    # Standard categories (union of baselines and SC patterns)
    if re.search(r'idvg|id_vg|vd\d+mv|vd\d+v?$|_vd\d|vd5$|vd5v|vd50|vd100|vd500', tl):
        return 'IdVg'
    if re.search(r'idvd|id_vd|rds_|rds_on|rdson|_rds|_vg\d|vg101520|idvvdvg', tl) and 'igss' not in tl:
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
