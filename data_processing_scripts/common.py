"""
Shared utility functions for APS Database ingestion scripts.

Extracted from ingestion_baselines.py and ingestion_sc.py to eliminate
duplication.  Both scripts import from here instead of maintaining their
own copies.
"""

import os
import re
import hashlib
from pathlib import Path


# ── Schema migrations ───────────────────────────────────────────────────────

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schema"


def apply_schema(conn):
    """
    Apply every .sql in schema/ in lexicographic order.

    Files are idempotent (CREATE TABLE IF NOT EXISTS + DO $$ ALTER TABLE
    ... ADD COLUMN ... EXCEPTION WHEN duplicate_column $$) so calling
    this at startup is safe.  Callers: server.py on boot, every
    ingestion_*.py before first write.
    """
    if not SCHEMA_DIR.is_dir():
        return
    cur = conn.cursor()
    for sql_path in sorted(SCHEMA_DIR.glob("*.sql")):
        cur.execute(sql_path.read_text())
    conn.commit()
    cur.close()


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


# ── Device Mapping Rules (Phase 1) ─────────────────────────────────────────
#
# Unified device-matching helper that consults the device_mapping_rules
# table (seeded from the three legacy hardcoded dicts) with a scope-aware
# fallback to device_library substring matching.
#
# The per-scope ordering matters.  Legacy scripts applied rules and the
# library in different orders — swapping this in Phase 1.4-1.6 without
# preserving those orderings will silently mis-assign devices.
#
#   baselines:    library-substring  ->  library-prefix  ->  rules
#   sc:           rules              ->  library-substring
#   irradiation:  rules              ->  library-substring
#   avalanche:    rules              ->  library-substring
#
# Rules are pre-filtered by scope and pre-sorted by (priority DESC,
# LENGTH(pattern) DESC) at load time so the matcher is a simple linear
# scan.  substring matches are case-insensitive; regex matches run with
# re.IGNORECASE.


def load_device_mapping_rules(cur, scope):
    """
    Load device_mapping_rules for the given scope (including scope='all').
    Rows are pre-joined with device_library to populate manufacturer, and
    pre-sorted by (priority DESC, LENGTH(pattern) DESC) so _apply_rules can
    just iterate.
    """
    cur.execute("""
        SELECT dmr.pattern, dmr.pattern_type, dmr.priority,
               dmr.part_number, dl.manufacturer
        FROM device_mapping_rules dmr
        JOIN device_library dl ON dmr.part_number = dl.part_number
        WHERE dmr.scope IN (%s, 'all')
        ORDER BY dmr.priority DESC, LENGTH(dmr.pattern) DESC
    """, (scope,))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _apply_rules(path_upper, rules):
    for rule in rules:
        pattern = rule["pattern"]
        if rule["pattern_type"] == "substring":
            if pattern.upper() in path_upper:
                return rule["part_number"], rule["manufacturer"]
        else:  # regex
            if re.search(pattern, path_upper, re.IGNORECASE):
                return rule["part_number"], rule["manufacturer"]
    return None, None


def _library_substring(path_upper, device_library):
    for entry in device_library:
        pn = entry["part_number"]
        if pn and pn.upper() in path_upper:
            return pn, entry.get("manufacturer")
    return None, None


def _library_prefix(path_upper, device_library):
    """
    Legacy baselines Pass-2: for part_numbers ending in a letter (most SiC
    MOSFET part numbers do — e.g. C2M0080120D, SCT3030AL), strip the
    trailing letter and look for the prefix followed by underscore.  This
    handles filenames like "C2M0080120_DUT01_IdVg.csv".

    Keeps the exact (buggy) r'[_\\b]' character class from the original —
    \\b inside a character class is backspace, not a word boundary, so in
    practice this only fires on underscore.  Replicated verbatim so the
    parity harness matches legacy behaviour.
    """
    for entry in device_library:
        pn = entry["part_number"]
        if not pn:
            continue
        pnu = pn.upper()
        if len(pnu) > 4 and pnu[-1].isalpha():
            prefix = pnu[:-1]
            if re.search(prefix + r"[_\b]", path_upper):
                return pn, entry.get("manufacturer")
    return None, None


def match_device(path, scope, rules, device_library):
    """
    Resolve a file path to (part_number, manufacturer) using the
    device_mapping_rules table + device_library fallback.

    Arguments:
        path            Case-insensitive file path to match against.  For
                        scope='sc' callers MUST pass a path relative to
                        the SC root (e.g. "SCT2080/DUT1/IdVg.csv"); SC
                        patterns are anchored at the start of the path to
                        emulate legacy parts[0] exact-match behaviour.
                        Other scopes accept the full path.
        scope           One of 'baselines', 'sc', 'irradiation',
                        'avalanche'.  Controls rule-vs-library ordering.
        rules           Output of load_device_mapping_rules(cur, scope).
        device_library  List of dicts from load_device_library(cur).

    Returns (part_number, manufacturer) or (None, None) if nothing matches.
    """
    if not path:
        return None, None
    path_upper = path.upper()

    if scope == "baselines":
        # Pass 1: library substring
        pn, mfr = _library_substring(path_upper, device_library)
        if pn:
            return pn, mfr
        # Pass 2: library prefix (trailing-letter strip)
        pn, mfr = _library_prefix(path_upper, device_library)
        if pn:
            return pn, mfr
        # Pass 3: experiment rules
        return _apply_rules(path_upper, rules)

    # sc, irradiation, avalanche: rules first, then library substring
    pn, mfr = _apply_rules(path_upper, rules)
    if pn:
        return pn, mfr
    return _library_substring(path_upper, device_library)


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

    # 3rd-quadrant (checked before IdVg/IdVd — filenames like "3rd_Vg0V"
    # contain substrings that match the IdVd regex via `_vg\d`).
    if re.search(r'3rd|quad|third', tl):
        return '3rd_Quadrant'

    # Blocking (checked before IdVd — "Idss…" filenames contain "id" fragments
    # that could be picked up by the IdVd regex in edge cases).
    # `dvd_vg` was removed: it was intended to catch standalone Blocking
    # filenames but only ever matched `IdVd_Vg*` as a substring.
    if re.search(r'block|bvdss|idss|idvdss|dvdss|listv', tl):
        return 'Blocking'

    # Standard categories (union of baselines and SC patterns)
    if re.search(r'idvg|id_vg|vd\d+mv|vd\d+v?$|_vd\d|vd5$|vd5v|vd50|vd100|vd500', tl):
        return 'IdVg'
    if re.search(r'idvd|id_vd|rds_|rds_on|rdson|_rds|_vg\d|vg101520|idvvdvg', tl) and 'igss' not in tl:
        return 'IdVd'
    if re.search(r'igss', tl):
        return 'Igss'
    if re.search(r'\bvth\b|vth_', tl):
        return 'Vth'
    if re.search(r'rdson', tl):
        return 'Rdson'
    if re.search(r'irrad', tl):
        return 'Irradiation'
    return 'Other'


# ── Sweep-range-aware category refinement ──────────────────────────────────
#
# The string-based classifier above operates on the filename/measurement_type
# alone.  Some source conventions (notably the irradiation campaigns' IDVDfwd
# for blocking sweeps and IDVDrev for body-diode sweeps) label files in ways
# that collide with the "IdVd" regex but describe completely different tests.
# These functions catch that by inspecting the actual sweep range.

# Thresholds calibrated against the current corpus:
#   Real linear-region IdVd : Vd ≤ 15 V, |Id| up to tens of A.
#   Blocking / BVDSS        : Vd ≥ 50 V (up to rated BV), |Id| in leakage range.
#   3rd-quadrant / body Dio : Vd swept negative, Id noticeably negative.
_REFINE_BLOCKING_VD_MIN   = 30.0   # V — no linear-region IdVd reaches 30 V
_REFINE_BLOCKING_ID_ABS   = 1.0    # A — blocking sweeps show leakage, not conduction
_REFINE_Q3_VD_MAX         = 0.5    # V — reverse sweep never goes strongly positive
_REFINE_Q3_VD_MIN         = -0.1   # V — must actually excurse negative
_REFINE_Q3_ID_MIN         = -1e-7  # A — measurable reverse current (rules out noise)


def sweep_stats(headers, rows, map_fn):
    """
    Scan already-parsed rows once and return drain-voltage/current extrema.

    Returns dict with keys vd_min, vd_max, id_min, id_max, id_abs_max, n_pts.
    Missing/non-numeric values are skipped.  Returns None for each field if
    no numeric values were found.
    """
    vd_min = vd_max = id_min = id_max = id_abs_max = None
    n = 0
    for row in rows:
        mapped = map_fn(headers, row)
        vd = mapped.get('v_drain')
        idv = mapped.get('i_drain')
        if isinstance(vd, (int, float)):
            if vd_min is None or vd < vd_min: vd_min = vd
            if vd_max is None or vd > vd_max: vd_max = vd
        if isinstance(idv, (int, float)):
            if id_min is None or idv < id_min: id_min = idv
            if id_max is None or idv > id_max: id_max = idv
            av = abs(idv)
            if id_abs_max is None or av > id_abs_max: id_abs_max = av
        n += 1
    return {
        'vd_min': vd_min, 'vd_max': vd_max,
        'id_min': id_min, 'id_max': id_max,
        'id_abs_max': id_abs_max, 'n_pts': n,
    }


def refine_category_by_sweep(category, stats):
    """
    Correct an 'IdVd' label that actually describes a Blocking or
    3rd-Quadrant sweep, based on observed drain-voltage/current range.

    Only 'IdVd' is ever second-guessed — other categories are returned
    unchanged.  Returns (refined_category, reason) where reason is a short
    string usable for logging (empty when no change was made).
    """
    if category != 'IdVd' or not stats:
        return category, ''

    vd_max = stats.get('vd_max')
    vd_min = stats.get('vd_min')
    id_min = stats.get('id_min')
    id_abs = stats.get('id_abs_max')

    if (vd_max is not None and vd_max >= _REFINE_BLOCKING_VD_MIN
            and (id_abs is None or id_abs < _REFINE_BLOCKING_ID_ABS)):
        return 'Blocking', (f'Vd_max={vd_max:.1f} V ≥ {_REFINE_BLOCKING_VD_MIN:g} V '
                            f'with |Id|_max={0.0 if id_abs is None else id_abs:.3g} A')

    if (vd_max is not None and vd_max <= _REFINE_Q3_VD_MAX
            and vd_min is not None and vd_min < _REFINE_Q3_VD_MIN
            and id_min is not None and id_min < _REFINE_Q3_ID_MIN):
        return '3rd_Quadrant', (f'Vd=[{vd_min:.2f}, {vd_max:.2f}] V with '
                                f'Id_min={id_min:.3g} A (reverse sweep)')

    return category, ''
