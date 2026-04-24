#!/usr/bin/env python3
"""
Avalanche waveform ingestion for APS database.

Ingests Keysight HDF5 waveform captures from the Avalanche Measurements corpus
into baselines_metadata (data_source='avalanche') and baselines_measurements.

Key behaviours
- Handles single-file captures and split _ch1/_ch2/_ch3 captures.
- Extracts inductance (L) from folder/filename tokens; falls back to the
  avalanche_campaigns table populated via the Flask UI.
- Outcome defaults to 'unknown'; overridden per-folder via avalanche_campaigns.
- Measurement date is parsed from the HDF5 Frame/TheFrame.Date field.
- Each HDF5 file is opened at most twice (metadata pass, data pass), with all
  channels from a single file read in one open per pass.
- Fast-path duplicate check by csv_path before computing MD5 hash.

Usage:
    python ingestion_avalanche.py --dry-run
    python ingestion_avalanche.py
    python ingestion_avalanche.py --rebuild
    python ingestion_avalanche.py --max-points 20000
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from time import perf_counter

try:
    import psycopg2
    from psycopg2.extras import Json, execute_values
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import Json, execute_values

try:
    import numpy as np
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy"])
    import numpy as np

try:
    import h5py
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "h5py"])
    import h5py

from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, NAS_ROOT
from common import compute_file_hash, load_device_library


AVALANCHE_ROOT = os.path.join(NAS_ROOT, "Avalanche Measurements")
VALID_EXTENSIONS = {".h5", ".hdf5"}
SKIP_FILES = {"Thumbs.db"}


ALTER_SCHEMA_SQL = """
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN data_source TEXT DEFAULT 'baselines'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN test_condition TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN sample_group TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_family TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_mode TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_energy_j DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_peak_current_a DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_gate_bias_v DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_gate_bias_raw TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_shot_index INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_condition_label TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_temperature_c DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_channel_count INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_downsample_factor INTEGER; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_inductance_mh DOUBLE PRECISION; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_outcome TEXT DEFAULT 'unknown'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE baselines_metadata ADD COLUMN avalanche_measured_at TIMESTAMP; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_baselines_meta_data_source ON baselines_metadata(data_source);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_avl_family ON baselines_metadata(avalanche_family);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_avl_mode ON baselines_metadata(avalanche_mode);

CREATE TABLE IF NOT EXISTS avalanche_campaigns (
    id                 SERIAL PRIMARY KEY,
    folder_path        TEXT NOT NULL UNIQUE,
    campaign_name      TEXT NOT NULL,
    inductance_mh      DOUBLE PRECISION,
    temperature_c      DOUBLE PRECISION,
    device_part_number TEXT,
    outcome_default    TEXT DEFAULT 'unknown',
    notes              TEXT
);
"""


AVALANCHE_VIEW_SQL = """
DROP VIEW IF EXISTS avalanche_waveform_view CASCADE;
CREATE VIEW avalanche_waveform_view AS
SELECT
    m.id AS measurement_id,
    md.id AS metadata_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.filename,
    md.csv_path,
    md.avalanche_family,
    md.avalanche_mode,
    md.avalanche_energy_j,
    md.avalanche_peak_current_a,
    md.avalanche_inductance_mh,
    md.avalanche_gate_bias_v,
    md.avalanche_gate_bias_raw,
    md.avalanche_shot_index,
    md.avalanche_condition_label,
    md.avalanche_temperature_c,
    md.avalanche_outcome,
    md.avalanche_measured_at,
    md.avalanche_channel_count,
    md.avalanche_downsample_factor,
    m.point_index,
    m.time_val,
    m.time_val * 1e6 AS time_us,
    m.v_drain AS vds,
    m.i_drain AS id_drain,
    m.v_gate AS vgs,
    m.i_gate AS igs
FROM baselines_measurements m
JOIN baselines_metadata md ON m.metadata_id = md.id
WHERE md.data_source = 'avalanche';

DROP VIEW IF EXISTS avalanche_summary_view CASCADE;
CREATE VIEW avalanche_summary_view AS
SELECT
    md.id                                       AS metadata_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.sample_group,
    md.avalanche_family,
    md.avalanche_mode,
    md.avalanche_energy_j,
    md.avalanche_peak_current_a,
    md.avalanche_inductance_mh,
    md.avalanche_gate_bias_v,
    md.avalanche_gate_bias_raw,
    md.avalanche_shot_index,
    md.avalanche_condition_label,
    md.avalanche_temperature_c,
    md.avalanche_outcome,
    md.avalanche_measured_at,
    md.avalanche_channel_count,
    md.num_points,
    MAX(m.v_drain)                              AS max_vds,
    MIN(m.v_drain)                              AS min_vds,
    MAX(m.i_drain)                              AS max_id,
    MIN(m.i_drain)                              AS min_id,
    MAX(ABS(m.i_drain))                         AS peak_id,
    MAX(m.v_gate)                               AS max_vgs,
    (MAX(m.time_val) - MIN(m.time_val))         AS pulse_duration_s,
    COUNT(*)                                    AS n_points
FROM baselines_metadata md
JOIN baselines_measurements m ON m.metadata_id = md.id
WHERE md.data_source = 'avalanche'
GROUP BY md.id;
"""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _decode_value(v):
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="ignore")
    if isinstance(v, np.ndarray) and v.shape == ():
        return v.item()
    if isinstance(v, np.generic):
        return v.item()
    return v


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(v):
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _sanitize_experiment_token(s):
    return re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_") or "unknown"


# ── Campaign lookup ───────────────────────────────────────────────────────────

def load_avalanche_campaigns(cur):
    """
    Load avalanche_campaigns table into a dict keyed by folder_path.
    Returns {} gracefully if the table does not yet exist.
    """
    try:
        cur.execute("""
            SELECT folder_path, inductance_mh, temperature_c,
                   device_part_number, outcome_default
            FROM avalanche_campaigns
        """)
        return {
            row[0]: {
                "inductance_mh":      row[1],
                "temperature_c":      row[2],
                "device_part_number": row[3],
                "outcome_default":    row[4] or "unknown",
            }
            for row in cur.fetchall()
        }
    except Exception:
        cur.connection.rollback()
        return {}


# ── Filename / path parsing ───────────────────────────────────────────────────

def parse_gate_bias(raw_token):
    """
    Parse Vg token variants:
      -1000001  → gate=-10 V, shot=1   (7-digit: gate|5-digit-shot)
      000000    → gate=0 V,  shot=0
      p500001   → gate=+5 V, shot=1
      -10       → gate=-10 V, shot=None
      -10-rep00001 → gate=-10 V, shot=1  (dash-rep separator)
    """
    if not raw_token:
        return None, None

    token = raw_token.strip()
    sign = 1
    if token.startswith("-"):
        sign = -1
        token_body = token[1:]
    elif token.lower().startswith("p"):
        token_body = token[1:]
    else:
        token_body = token

    # Dash-rep separator: 10-rep00001 → gate=sign*10, shot=1
    m = re.match(r"^(\d+)-rep(\d+)$", token_body, re.IGNORECASE)
    if m:
        return sign * float(m.group(1)), int(m.group(2))

    # Pure digits, possibly with encoded shot in last 5 places
    if re.fullmatch(r"\d+", token_body):
        if len(token_body) > 5:
            gate_digits = token_body[:-5] or "0"
            return sign * float(gate_digits), int(token_body[-5:])
        return sign * float(token_body), None

    # Decimal with no shot
    if re.fullmatch(r"\d+(?:\.\d+)?", token_body):
        return sign * float(token_body), None

    return None, None


def parse_filename_metadata(base_stem):
    info = {
        "device_id":              None,
        "avalanche_energy_j":     None,
        "avalanche_peak_current_a": None,
        "avalanche_gate_bias_v":  None,
        "avalanche_gate_bias_raw": None,
        "avalanche_shot_index":   None,
        "sample_group":           None,
    }

    stem = base_stem.strip()

    if stem.lower().startswith("test measurement "):
        device_guess = stem[len("Test Measurement "):].strip()
        info["device_id"] = device_guess or stem
    else:
        info["device_id"] = stem.split("_")[0] if "_" in stem else stem

    # Peak current: e.g. 25A
    m = re.search(r"(?:^|_)(\d+(?:\.\d+)?)A(?:_|$)", stem, re.IGNORECASE)
    if m:
        info["avalanche_peak_current_a"] = _safe_float(m.group(1))

    # Energy with explicit J suffix: e.g. 0.5J, 1.02J, 0.5J00002 (J may be followed by shot)
    m = re.search(r"(?:^|_)(\d+(?:\.\d+)?)J", stem, re.IGNORECASE)
    if m:
        info["avalanche_energy_j"] = _safe_float(m.group(1))

    # Fallback: fused decimal + 5-digit shot with no unit letter
    # Matches e.g. 0.1200009 → energy=0.12, shot=9 (UIDSelam naming)
    if info["avalanche_energy_j"] is None:
        m = re.search(r"(?:^|_)(\d+\.\d{1,3})(\d{5})(?:_|$)", stem)
        if m:
            info["avalanche_energy_j"] = _safe_float(m.group(1))
            if info["avalanche_shot_index"] is None:
                info["avalanche_shot_index"] = _safe_int(m.group(2))

    # Gate bias
    gate_tokens = re.findall(r"Vg([^_]+)", stem, re.IGNORECASE)
    if gate_tokens:
        gate_raw = gate_tokens[-1]
        gate_v, shot = parse_gate_bias(gate_raw)
        info["avalanche_gate_bias_raw"] = gate_raw
        info["avalanche_gate_bias_v"] = gate_v
        info["avalanche_shot_index"] = shot

    # Shot index tail fallback (only if not already set above)
    if info["avalanche_shot_index"] is None:
        tail = re.search(r"(\d{5,})$", stem)
        if tail:
            info["avalanche_shot_index"] = _safe_int(tail.group(1))

    info["sample_group"] = info["device_id"]
    return info


def parse_inductance_from_rel_path(rel_path, family, campaign_lookup):
    """
    Extract inductance (mH) from the relative path, trying three token patterns
    in order of specificity, then falling back to the campaign lookup table.

      0p47mH        → 0.47  (p as decimal separator, UIS_2018_botnk filenames)
      2-7mH         → 2.7   (dash as decimal separator, Series subfolders)
      0.25mH        → 0.25  (standard decimal)
    """
    # p-decimal: 0p47mH → 0.47
    m = re.search(r"(\d+)p(\d+)mH", rel_path, re.IGNORECASE)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")

    # Dash-decimal: 2-7mH → 2.7, 1-47mH → 1.47
    m = re.search(r"(\d+)-(\d+)mH", rel_path, re.IGNORECASE)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")

    # Standard decimal: 0.25mH, 5.1mH
    m = re.search(r"(\d+(?:\.\d+)?)mH", rel_path, re.IGNORECASE)
    if m:
        return _safe_float(m.group(1))

    # Campaign table fallback
    return campaign_lookup.get(family, {}).get("inductance_mh")


def parse_family_mode_experiment(file_path):
    rel = os.path.relpath(file_path, AVALANCHE_ROOT)
    parts = rel.split(os.sep)

    family = "unknown"
    if parts:
        if parts[0] == "Selam" and len(parts) > 1 and os.path.splitext(parts[1])[1] == "":
            family = parts[1]
        else:
            family = parts[0]

    rel_lower = rel.lower()
    fam_lower = family.lower()

    if "uis" in fam_lower or "uis" in rel_lower:
        mode = "UIS"
    elif "uid" in fam_lower or "uid" in rel_lower:
        mode = "UID"
    elif fam_lower.startswith("rt") or Path(file_path).stem.lower().startswith("rt"):
        mode = "RT"
    elif "test" in fam_lower:
        mode = "Test"
    elif family == "Selam":
        # Root-level Selam captures (C*, RP*, etc.) are UIS experiments
        mode = "UIS"
    else:
        mode = "Avalanche"

    experiment = f"AVL_{_sanitize_experiment_token(family)}"
    return family, mode, experiment


def map_device_type(paths, device_library):
    joined = " ".join(paths).upper()
    for entry in device_library:
        pn = (entry.get("part_number") or "").upper()
        if pn and pn in joined:
            return entry.get("part_number"), entry.get("manufacturer")
    return None, None


# ── File collection ───────────────────────────────────────────────────────────

def collect_h5_groups(root_dir, limit_groups=0):
    """Group split-channel files (_ch1/_ch2/_ch3) by their shared base stem."""
    groups = {}

    for dirpath, _, files in os.walk(root_dir):
        for fname in sorted(files):
            if fname in SKIP_FILES:
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in VALID_EXTENSIONS:
                continue

            full_path = os.path.join(dirpath, fname)
            stem = Path(fname).stem
            m = re.match(r"^(.*)_ch\d+$", stem, re.IGNORECASE)

            if m:
                base_stem = m.group(1)
                group_key = os.path.join(dirpath, base_stem.lower())
                grouped_channels = True
            else:
                base_stem = stem
                group_key = full_path
                grouped_channels = False

            rec = groups.setdefault(group_key, {
                "base_stem": base_stem,
                "paths": [],
                "grouped_channels": grouped_channels,
            })
            rec["paths"].append(full_path)

    ordered = [groups[k] for k in sorted(groups.keys())]
    if limit_groups and limit_groups > 0:
        ordered = ordered[:limit_groups]
    return ordered


# ── HDF5 parsing ──────────────────────────────────────────────────────────────

def read_frame_info(h5f):
    frame = {}
    if "Frame" not in h5f or "TheFrame" not in h5f["Frame"]:
        return frame
    dset = h5f["Frame"]["TheFrame"]
    try:
        row = dset[()]
        for name in (dset.dtype.names or []):
            frame[name] = _decode_value(row[name])
    except Exception:
        pass
    return frame


def _series_abs_max(arr):
    if arr.size == 0:
        return 0.0
    try:
        return float(np.nanmax(np.abs(arr)))
    except ValueError:
        return 0.0


def map_channel_roles(series):
    amp, volt, other = [], [], []
    for s in series:
        unit = (s.get("y_units") or "").lower()
        if "amp" in unit:
            amp.append(s)
        elif "volt" in unit:
            volt.append(s)
        else:
            other.append(s)

    amp.sort(key=lambda x: _series_abs_max(x["data"]), reverse=True)
    volt.sort(key=lambda x: _series_abs_max(x["data"]), reverse=True)

    mapped = {
        "i_drain": amp[0]["data"] if amp else None,
        "i_gate":  amp[1]["data"] if len(amp) > 1 else None,
        "v_drain": volt[0]["data"] if volt else None,
        "v_gate":  volt[-1]["data"] if len(volt) > 1 else None,
        "notes": [],
    }
    if len(volt) > 2:
        mapped["notes"].append(f"extra_voltage_channels={len(volt)}")
    if other:
        mapped["notes"].append(f"other_unit_channels={len(other)}")
    return mapped


def build_waveform(group_paths, max_points):
    """
    Parse all channels from grouped files, downsample, and return arrays.

    Each file is opened at most twice:
      Pass 1 — read channel metadata (attrs + shape) from every file.
      Pass 2 — group paths and read all channels from each file in one open.
    """
    # Pass 1: metadata only
    all_descs = []
    frame_info = {}
    for p in sorted(group_paths):
        with h5py.File(p, "r") as h5f:
            fi = read_frame_info(h5f)
            if fi:
                frame_info = fi
            if "Waveforms" not in h5f:
                continue
            for ch_name, g in h5f["Waveforms"].items():
                data_sets = [k for k in g.keys() if k.lower().endswith("data")] or list(g.keys())
                if not data_sets:
                    continue
                dset = g[data_sets[0]]
                if len(dset.shape) != 1:
                    continue
                all_descs.append({
                    "file_path":    p,
                    "dataset_path": dset.name,
                    "channel_name": str(ch_name),
                    "y_units":      str(_decode_value(g.attrs.get("YUnits", ""))),
                    "x_inc":        _safe_float(_decode_value(g.attrs.get("XInc", 1.0))) or 1.0,
                    "x_org":        _safe_float(_decode_value(g.attrs.get("XOrg", 0.0))) or 0.0,
                    "num_points":   int(dset.shape[0]),
                })

    if not all_descs:
        raise ValueError("No channel datasets found")

    n_ref = min(d["num_points"] for d in all_descs)
    if n_ref <= 0:
        raise ValueError("No data points")

    stride = max(1, int(math.ceil(n_ref / float(max_points)))) if max_points and max_points > 0 else 1
    n_sampled = int(math.ceil(n_ref / float(stride)))

    # Pass 2: read data — one file open per unique path, all channels at once
    by_file = defaultdict(list)
    for d in all_descs:
        by_file[d["file_path"]].append(d)

    data_cache = {}
    for p in sorted(by_file):
        with h5py.File(p, "r") as h5f:
            for d in by_file[p]:
                data_cache[(p, d["dataset_path"])] = np.asarray(
                    h5f[d["dataset_path"]][:n_ref:stride], dtype=np.float64
                )

    sampled = []
    for d in all_descs:
        sampled.append({
            "channel_name": d["channel_name"],
            "y_units":      d["y_units"],
            "x_inc":        d["x_inc"],
            "x_org":        d["x_org"],
            "data":         data_cache[(d["file_path"], d["dataset_path"])],
        })

    time_ref = sampled[0]
    idx = np.arange(n_sampled, dtype=np.float64)
    time_val = time_ref["x_org"] + idx * (time_ref["x_inc"] * stride)

    mapped = map_channel_roles(sampled)

    meta = {
        "raw_points":        int(n_ref),
        "sampled_points":    int(n_sampled),
        "downsample_factor": int(stride),
        "channel_count":     len(sampled),
        "channel_units":     [s["y_units"] for s in sampled],
        "frame_info":        frame_info,
        "notes":             mapped.get("notes", []),
    }
    return time_val, mapped, meta


def build_group_hash(paths):
    parts = [compute_file_hash(p) for p in sorted(paths)]
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()


def format_condition_label(mode, info, inductance_mh=None):
    bits = []
    if mode:
        bits.append(mode)
    if info.get("avalanche_peak_current_a") is not None:
        bits.append(f"{info['avalanche_peak_current_a']}A")
    if info.get("avalanche_energy_j") is not None:
        bits.append(f"{info['avalanche_energy_j']}J")
    if inductance_mh is not None:
        bits.append(f"{inductance_mh}mH")
    if info.get("avalanche_gate_bias_raw"):
        bits.append(f"Vg{info['avalanche_gate_bias_raw']}")
    if info.get("avalanche_shot_index") is not None:
        bits.append(f"shot{info['avalanche_shot_index']}")
    return "_".join(bits) if bits else mode or "avalanche"


def value_or_none(arr, idx):
    if arr is None:
        return None
    v = arr[idx]
    return None if np.isnan(v) else float(v)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Ingest avalanche HDF5 waveform data")
    ap.add_argument("--root", default=AVALANCHE_ROOT)
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse and summarize without DB writes")
    ap.add_argument("--rebuild", action="store_true",
                    help="Delete existing avalanche rows before ingest")
    ap.add_argument("--limit-groups", type=int, default=0,
                    help="Only process first N grouped captures")
    ap.add_argument("--max-points", type=int, default=5000,
                    help="Maximum sampled points per grouped capture")
    args = ap.parse_args()

    t0 = perf_counter()

    print("=" * 72)
    print("Avalanche HDF5 Ingestion")
    print("=" * 72)
    print(f"Root:       {args.root}")
    print(f"Target:     postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Dry run:    {args.dry_run}")
    print(f"Max points: {args.max_points}")

    if not os.path.isdir(args.root):
        print(f"ERROR: root not found: {args.root}")
        sys.exit(1)

    groups = collect_h5_groups(args.root, limit_groups=args.limit_groups)
    print(f"\nGrouped captures found: {len(groups)}")
    if not groups:
        print("Nothing to do.")
        return

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        if not args.dry_run:
            print("\nApplying schema updates...")
            cur.execute(ALTER_SCHEMA_SQL)
            conn.commit()

            if args.rebuild:
                print("Deleting existing avalanche rows...")
                cur.execute("DELETE FROM baselines_metadata WHERE data_source = 'avalanche'")
                conn.commit()
                print(f"  Deleted {cur.rowcount} metadata rows")

        campaign_lookup = load_avalanche_campaigns(cur)
        print(f"Campaign overrides loaded: {len(campaign_lookup)}")

        device_library = load_device_library(cur)
        print(f"Device library entries:   {len(device_library)}")

        loaded = skipped = errors = invalid_files = 0
        total_points = 0

        for idx, g in enumerate(groups, start=1):
            paths = sorted(g["paths"])
            base_stem = g["base_stem"]

            # Fast-path duplicate check by csv_path (avoids MD5 on large files)
            csv_path = ";".join(paths)
            if not args.dry_run:
                cur.execute(
                    "SELECT id FROM baselines_metadata WHERE csv_path = %s AND data_source = 'avalanche'",
                    (csv_path,),
                )
                if cur.fetchone():
                    skipped += 1
                    continue

            # Hash check (handles files moved/renamed with same content)
            try:
                file_hash = build_group_hash(paths)
            except Exception as e:
                print(f"  [{idx}] ERROR hash ({base_stem}): {e}")
                errors += 1
                continue

            if not args.dry_run:
                cur.execute("SELECT id FROM baselines_metadata WHERE file_hash = %s", (file_hash,))
                if cur.fetchone():
                    skipped += 1
                    continue

            family, mode, experiment = parse_family_mode_experiment(paths[0])
            file_info = parse_filename_metadata(base_stem)

            try:
                time_val, mapped, wf_meta = build_waveform(paths, args.max_points)
            except OSError as e:
                invalid_files += 1
                errors += 1
                print(f"  [{idx}] INVALID HDF5 ({base_stem}): {e}")
                continue
            except Exception as e:
                errors += 1
                print(f"  [{idx}] ERROR parse ({base_stem}): {e}")
                continue

            n_pts = len(time_val)
            if n_pts == 0:
                skipped += 1
                continue

            # ── Derived metadata ──────────────────────────────────────────
            rel_path = os.path.relpath(paths[0], args.root)
            inductance_mh = parse_inductance_from_rel_path(rel_path, family, campaign_lookup)

            campaign_meta = campaign_lookup.get(family, {})
            outcome = campaign_meta.get("outcome_default") or "unknown"
            temperature_c = (
                file_info.get("avalanche_temperature_c")
                or campaign_meta.get("temperature_c")
            )

            device_type, manufacturer = map_device_type(paths, device_library)
            if device_type is None and campaign_meta.get("device_part_number"):
                device_type = campaign_meta["device_part_number"]

            # Measurement date from HDF5 Frame
            raw_date = wf_meta.get("frame_info", {}).get("Date", "")
            measured_at = None
            if raw_date:
                for fmt in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                    try:
                        measured_at = datetime.strptime(str(raw_date).strip(), fmt)
                        break
                    except ValueError:
                        pass

            # Columns actually present in this capture
            cols = ["time_val"]
            if mapped.get("v_drain") is not None:
                cols.append("v_drain")
            if mapped.get("i_drain") is not None:
                cols.append("i_drain")
            if mapped.get("v_gate") is not None:
                cols.append("v_gate")
            if mapped.get("i_gate") is not None:
                cols.append("i_gate")
            columns_str = ",".join(cols)

            condition_label = format_condition_label(mode, file_info, inductance_mh)
            measurement_type = mode or "Avalanche"

            gate_payload = {
                "raw_num_points":    wf_meta["raw_points"],
                "sampled_points":    wf_meta["sampled_points"],
                "channel_units":     wf_meta["channel_units"],
                "frame_info":        wf_meta["frame_info"],
                "notes":             wf_meta["notes"],
            }

            if args.dry_run:
                loaded += 1
                total_points += n_pts
                if idx % 50 == 0 or idx == 1:
                    l_str = f"{inductance_mh}mH" if inductance_mh is not None else "L=?"
                    print(
                        f"  [{idx}/{len(groups)}] {base_stem}: "
                        f"{n_pts}pts  {l_str}  "
                        f"outcome={outcome}  date={measured_at}"
                    )
                continue

            filename = f"{base_stem}.h5" if g["grouped_channels"] else os.path.basename(paths[0])

            try:
                cur.execute(
                    """
                    INSERT INTO baselines_metadata (
                        experiment, device_id, measurement_type, measurement_category,
                        filename, csv_path, columns, num_points,
                        file_hash, device_type, manufacturer,
                        data_source, test_condition, sample_group,
                        avalanche_family, avalanche_mode,
                        avalanche_energy_j, avalanche_peak_current_a,
                        avalanche_inductance_mh,
                        avalanche_gate_bias_v, avalanche_gate_bias_raw,
                        avalanche_shot_index, avalanche_condition_label,
                        avalanche_temperature_c, avalanche_outcome,
                        avalanche_measured_at,
                        avalanche_channel_count, avalanche_downsample_factor,
                        gate_params
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s
                    ) RETURNING id
                    """,
                    (
                        experiment,
                        file_info.get("device_id"),
                        measurement_type,
                        "Avalanche_Waveform",
                        filename,
                        csv_path,
                        columns_str,
                        n_pts,
                        file_hash,
                        device_type,
                        manufacturer,
                        "avalanche",
                        "avalanche",
                        file_info.get("sample_group"),
                        family,
                        mode,
                        file_info.get("avalanche_energy_j"),
                        file_info.get("avalanche_peak_current_a"),
                        inductance_mh,
                        file_info.get("avalanche_gate_bias_v"),
                        file_info.get("avalanche_gate_bias_raw"),
                        file_info.get("avalanche_shot_index"),
                        condition_label,
                        temperature_c,
                        outcome,
                        measured_at,
                        wf_meta.get("channel_count"),
                        wf_meta.get("downsample_factor"),
                        Json(gate_payload),
                    ),
                )
                meta_id = cur.fetchone()[0]
            except Exception as e:
                errors += 1
                conn.rollback()
                print(f"  [{idx}] ERROR metadata ({base_stem}): {e}")
                continue

            batch = [
                (
                    meta_id, pidx,
                    value_or_none(mapped.get("v_gate"), pidx),
                    value_or_none(mapped.get("i_gate"), pidx),
                    value_or_none(mapped.get("v_drain"), pidx),
                    value_or_none(mapped.get("i_drain"), pidx),
                    None, None,
                    float(time_val[pidx]),
                    0,
                )
                for pidx in range(n_pts)
            ]

            try:
                execute_values(
                    cur,
                    """
                    INSERT INTO baselines_measurements
                    (metadata_id, point_index, v_gate, i_gate, v_drain, i_drain,
                     rds, bv, time_val, step_index)
                    VALUES %s
                    """,
                    batch,
                    page_size=5000,
                )
                loaded += 1
                total_points += n_pts
            except Exception as e:
                errors += 1
                conn.rollback()
                print(f"  [{idx}] ERROR measurements ({base_stem}): {e}")
                continue

            if loaded % 25 == 0:
                conn.commit()

            if idx % 50 == 0 or idx == 1:
                print(
                    f"  [{idx}/{len(groups)}] loaded={loaded} "
                    f"points={total_points} ({base_stem})"
                )

        if not args.dry_run:
            conn.commit()
            print("\nRefreshing avalanche view...")
            cur.execute(AVALANCHE_VIEW_SQL)
            conn.commit()

        elapsed = perf_counter() - t0
        print("\n" + "=" * 72)
        print("AVALANCHE INGEST SUMMARY")
        print("=" * 72)
        print(f"  Captures loaded:   {loaded}")
        print(f"  Captures skipped:  {skipped}")
        print(f"  Captures errored:  {errors}")
        print(f"  Invalid HDF5:      {invalid_files}")
        print(f"  Points processed:  {total_points}")
        print(f"  Elapsed:           {elapsed:.1f}s")

        if args.dry_run:
            print("\nDRY RUN complete — no DB writes were made.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
