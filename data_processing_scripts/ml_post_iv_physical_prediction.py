#!/usr/bin/env python3
"""
V2 Post-IV Physical Degradation Prediction
==========================================
Builds a conservative, validation-gated physical response workflow for
post-stress IV behavior. V2 predicts physical degradation parameters first:

  * IdVg -> delta_vth_v
  * IdVd -> log_rdson_ratio

It does not mutate the legacy iv_prediction_* tables. Curve reconstruction is
V2-only, confidence-labeled, and limited to validated IdVg/IdVd physical
parameter predictions. Blocking and 3rd_Quadrant are intentionally out of V1
scope until separate physical envelope/diode models are validated.

Typical usage:
    python3 ml_post_iv_physical_prediction.py --rebuild-sql
    python3 ml_post_iv_physical_prediction.py --extract-features
    python3 ml_post_iv_physical_prediction.py --build-pairs --include-library-pristine
    python3 ml_post_iv_physical_prediction.py --audit-library-pristine
    python3 ml_post_iv_physical_prediction.py --train --reference-tier both
    python3 ml_post_iv_physical_prediction.py --validate --validation-mode both --reference-tier both
    python3 ml_post_iv_physical_prediction.py --predict-curves --reference-tier both
"""

import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    from psycopg2.extras import Json, RealDictCursor, execute_values
except ImportError:
    sys.exit("psycopg2 is required: pip install psycopg2-binary")

from db_config import get_connection

try:
    from extract_damage_metrics import (
        PARAM_KEYS,
        apply_extraction,
        fetch_existing_gate_params,
        fetch_extracted,
    )
except ImportError:
    PARAM_KEYS = ()
    apply_extraction = None
    fetch_existing_gate_params = None
    fetch_extracted = None


REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "schema" / "024_iv_physical_prediction.sql"
OUT_DIR = REPO_ROOT / "out" / "iv_physical_prediction"
MODEL_DIR = OUT_DIR / "models"

MODEL_VERSION = "v2.2-physical-donor-library-pristine"
ALGORITHM = "support_gated_weighted_median_donor_v1"

CURVE_TO_TARGET = {
    "IdVg": "delta_vth_v",
    "IdVd": "log_rdson_ratio",
}
MEASUREMENT_CATEGORY_TO_CURVE = {
    "IdVg": "IdVg",
    "Vth": "IdVg",
    "IdVd": "IdVd",
}
TARGET_LABELS = {
    "delta_vth_v": "IdVg / delta_vth_v",
    "log_rdson_ratio": "IdVd / log_rdson_ratio",
}
OUT_OF_SCOPE_CURVES = ("Blocking", "3rd_Quadrant")
VALIDATION_MODES = ("within_condition", "leave_condition")
VALIDATION_MODE_LABELS = {
    "within_condition": "Within-condition validation",
    "leave_condition": "Leave-condition validation",
}
REFERENCE_TIERS = ("strict_pre_irrad", "library_pristine")
REFERENCE_TIER_LABELS = {
    "strict_pre_irrad": "Strict same-device reference",
    "library_pristine": "Library pristine reference",
}
REFERENCE_TIER_OPTIONS = {
    "strict": ("strict_pre_irrad",),
    "library": ("library_pristine",),
    "both": REFERENCE_TIERS,
}
LIBRARY_REFERENCE_METHOD = "single_best_compatible_feature"
BROAD_ION_NEIGHBORHOOD_ENABLED = False
CONFIDENCE_LEVELS = ("strong", "weak", "unsupported")
STRONG_SUPPORTED_FRACTION_MIN = 0.10
PREDICTION_STRESS_TYPES = ("irradiation",)
DEFAULT_PREDICTION_DONOR_MODE = "within_condition"

MIN_DONORS = 3
NEAREST_K = 7
VALIDATION_GATES = {
    "delta_vth_v": {
        "min_supported_validation_pairs": 10,
        "median_abs_residual_max": 0.5,
        "p90_abs_residual_max": 2.0,
    },
    "log_rdson_ratio": {
        "min_supported_validation_pairs": 10,
        "median_abs_residual_max": 0.25,
        "p90_abs_residual_max": 0.75,
    },
}
NUMERIC_FEATURES = {
    "sc": ("sc_voltage_v", "sc_duration_us", "log_sc_v_us"),
    "irradiation": (
        "beam_energy_mev",
        "let_surface",
        "let_bragg_peak",
        "range_um",
        "log_fluence_at_meas",
    ),
}

V2_TABLES = (
    "iv_physical_curve_points",
    "iv_physical_parameter_predictions",
    "iv_physical_validation_residuals",
    "iv_physical_model_runs",
    "iv_physical_response_pairs",
    "iv_physical_curve_features",
)
V2_DOWNSTREAM_TABLES = (
    "iv_physical_curve_points",
    "iv_physical_parameter_predictions",
    "iv_physical_validation_residuals",
    "iv_physical_model_runs",
    "iv_physical_response_pairs",
)
LEGACY_TABLES = (
    "iv_prediction_pair_grid",
    "iv_prediction_model_runs",
    "iv_prediction_batches",
    "iv_prediction_points",
    "iv_prediction_validation_residuals",
)


def finite(value):
    if value is None:
        return False
    try:
        value = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(value) and abs(value) < 1e30


def safe_float(value):
    if not finite(value):
        return None
    return float(value)


def clean_text(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value or value.lower() in {"unknown", "none", "null", "nan"}:
        return None
    return value


def parse_rating(value, kind):
    """Parse a loose device_library rating string into a numeric value."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if not match:
        return None
    try:
        number = float(match.group(0))
    except ValueError:
        return None
    lower = text.lower()
    if kind == "voltage" and "kv" in lower:
        number *= 1000.0
    if kind == "rdson":
        if "ohm" in lower and "mohm" not in lower:
            number *= 1000.0
    return number if math.isfinite(number) else None


def curve_family_for(row):
    category = row.get("measurement_category")
    return MEASUREMENT_CATEGORY_TO_CURVE.get(category)


def metric_from_gate_params(gate_params, key):
    if not gate_params:
        return None
    value = gate_params.get(key)
    return safe_float(value)


def is_pristine_reference(row):
    source = row.get("data_source") or "baselines"
    if row.get("irrad_role") is not None:
        return row.get("irrad_role") == "pre_irrad"
    if row.get("test_condition") is not None:
        return row.get("test_condition") in ("pristine", "pre_avalanche")
    return (
        source == "baselines"
    )


def is_library_pristine_reference(row):
    """Return True for explicitly known-pristine library candidates."""
    if row.get("quality_status") != "usable":
        return False
    if clean_text(row.get("device_type")) is None:
        return False
    if row.get("is_likely_irradiated"):
        return False
    decision = row.get("promotion_decision")
    if decision and str(decision).startswith("rejected_"):
        return False

    source = row.get("data_source") or "baselines"
    test_condition = row.get("test_condition")
    irrad_role = row.get("irrad_role")
    if irrad_role == "pre_irrad":
        return True
    if source == "sc_ruggedness" and test_condition == "pristine":
        return True
    if test_condition == "pre_avalanche":
        return True
    if source == "baselines" and test_condition is None and irrad_role is None:
        return True
    return False


def reference_tiers_for_option(option):
    try:
        return REFERENCE_TIER_OPTIONS[option]
    except KeyError as exc:
        raise ValueError(f"unknown reference tier option: {option}") from exc


def physical_device_key(row):
    source = row.get("data_source") or "baselines"
    experiment = clean_text(row.get("experiment")) or "no-experiment"
    device_id = clean_text(row.get("device_id"))
    sample_group = clean_text(row.get("sample_group"))

    if source == "sc_ruggedness":
        ident = sample_group or device_id
        return f"sc:{experiment}:{ident}" if ident else None

    if row.get("irrad_campaign_id") is not None or row.get("irrad_role"):
        ident = device_id or sample_group
        campaign = row.get("irrad_campaign_id") or experiment
        return f"irrad:{campaign}:{ident}" if ident else None

    ident = device_id or sample_group
    return f"baseline:{experiment}:{ident}" if ident else None


def quality_for_feature(row, curve_family, vth_v, rdson_mohm):
    flags = []
    status = "usable"

    if curve_family is None:
        return "out_of_scope", ["out_of_scope_curve_family"]

    if clean_text(row.get("device_type")) is None:
        flags.append("missing_device_type")
    if clean_text(row.get("physical_device_key")) is None:
        flags.append("missing_physical_device_key")

    decision = row.get("promotion_decision")
    if decision and str(decision).startswith("rejected_"):
        flags.append("rejected_pristine_reference")

    if row.get("is_likely_irradiated") and is_pristine_reference(row):
        flags.append("likely_irradiated_pristine_reference")

    gate_params = row.get("gate_params") or {}
    if not gate_params:
        flags.append("missing_gate_params")

    if curve_family == "IdVg":
        if vth_v is None:
            flags.append("missing_vth_v")
        elif abs(vth_v) > 100.0:
            flags.append("nonsensical_vth_v")
    elif curve_family == "IdVd":
        if rdson_mohm is None:
            flags.append("missing_rdson_mohm")
        elif rdson_mohm <= 0.0 or rdson_mohm > 1e7:
            flags.append("nonsensical_rdson_mohm")

    hard_exclusions = {
        "missing_device_type",
        "missing_physical_device_key",
        "rejected_pristine_reference",
        "likely_irradiated_pristine_reference",
        "nonsensical_vth_v",
        "nonsensical_rdson_mohm",
    }
    missing_metric = {"missing_vth_v", "missing_rdson_mohm"}
    if any(flag in hard_exclusions for flag in flags):
        status = "excluded"
    elif any(flag in missing_metric for flag in flags):
        status = "missing_metric"
    return status, flags


def target_value(pair):
    if pair["target_type"] == "delta_vth_v":
        return safe_float(pair.get("delta_vth_v"))
    if pair["target_type"] == "log_rdson_ratio":
        return safe_float(pair.get("log_rdson_ratio"))
    return None


def percentile(values, q):
    vals = sorted(float(v) for v in values if finite(v))
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def robust_scale(values):
    vals = sorted(float(v) for v in values if finite(v))
    if len(vals) < 2:
        return {"median": vals[0] if vals else None, "scale": 1.0, "n": len(vals)}
    med = percentile(vals, 0.5)
    q1 = percentile(vals, 0.25)
    q3 = percentile(vals, 0.75)
    iqr = (q3 - q1) if q1 is not None and q3 is not None else 0.0
    if iqr and abs(iqr) > 1e-12:
        scale = abs(iqr) / 1.349
    else:
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / max(len(vals) - 1, 1)
        scale = math.sqrt(var)
    return {"median": med, "scale": max(scale, 1.0), "n": len(vals)}


def weighted_quantile(values, weights, q):
    rows = sorted(
        (float(v), max(float(w), 0.0))
        for v, w in zip(values, weights)
        if finite(v) and finite(w) and float(w) > 0.0
    )
    if not rows:
        return None
    total = sum(w for _, w in rows)
    if total <= 0:
        return None
    cutoff = q * total
    acc = 0.0
    for value, weight in rows:
        acc += weight
        if acc >= cutoff:
            return value
    return rows[-1][0]


def row_feature_value(row, name):
    if name == "log_sc_v_us":
        v = safe_float(row.get("sc_voltage_v"))
        d = safe_float(row.get("sc_duration_us"))
        if v is None or d is None or v <= 0 or d <= 0:
            return None
        return math.log10(v * d + 1.0)
    if name == "log_fluence_at_meas":
        f = safe_float(row.get("fluence_at_meas"))
        if f is None or f < 0:
            return None
        return math.log10(f + 1.0)
    return safe_float(row.get(name))


def schema_sql():
    return SCHEMA_PATH.read_text()


def apply_schema(conn):
    with conn.cursor() as cur:
        cur.execute(schema_sql())
    conn.commit()


def truncate_tables(conn, tables):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE " + ", ".join(tables) + " RESTART IDENTITY")
    conn.commit()


def old_prediction_counts(conn):
    counts = {}
    with conn.cursor() as cur:
        for table in LEGACY_TABLES:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
            if cur.fetchone()[0] is None:
                counts[table] = None
                continue
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
    return counts


def print_old_counts(conn, label):
    counts = old_prediction_counts(conn)
    print(f"\nLegacy iv_prediction_* counts ({label}; not modified by V2):")
    for table in LEGACY_TABLES:
        print(f"  {table}: {counts[table]}")


def warn_gate_param_coverage(conn):
    sql = """
        SELECT measurement_category,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE gate_params ? 'vth_v') AS with_vth,
               COUNT(*) FILTER (WHERE gate_params ? 'rdson_mohm') AS with_rdson
        FROM baselines_metadata
        WHERE measurement_category IN ('IdVg', 'Vth', 'IdVd')
        GROUP BY measurement_category
        ORDER BY measurement_category
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    print("\ngate_params coverage before feature extraction:")
    for category, total, with_vth, with_rdson in rows:
        usable = with_vth if category in ("IdVg", "Vth") else with_rdson
        frac = usable / total if total else 0.0
        print(f"  {category}: {usable}/{total} target metrics ({frac:.1%})")
        if total and frac < 0.5:
            print("    WARNING: low metric coverage. Consider running:")
            print("      python3 data_processing_scripts/extract_damage_metrics.py")


def refresh_damage_metrics(conn, rebuild=False, device_type=None):
    """Refresh gate_params using the existing extract_damage_metrics helpers."""
    if not all([fetch_extracted, fetch_existing_gate_params, apply_extraction]):
        sys.exit(
            "ERROR: could not import extract_damage_metrics.py. "
            "Run that script directly before --extract-features."
        )

    print("\nRefreshing gate_params via extract_damage_metrics.py helpers ...")
    with conn.cursor() as cur:
        rows = fetch_extracted(cur, device_type=device_type, rebuild=rebuild)
        existing = {} if rebuild else fetch_existing_gate_params(
            cur, device_type=device_type
        )

        updated = 0
        skipped_already_done = 0
        skipped_no_params = 0
        for row in rows:
            metadata_id = row["metadata_id"]
            extracted = {key: row.get(key) for key in PARAM_KEYS}
            if not any(value is not None for value in extracted.values()):
                skipped_no_params += 1
                continue
            if not rebuild:
                have = existing.get(metadata_id, set())
                to_add = {
                    key
                    for key, value in extracted.items()
                    if value is not None and key not in have
                }
                if not to_add:
                    skipped_already_done += 1
                    continue
            if apply_extraction(cur, metadata_id, extracted):
                updated += 1
    conn.commit()
    print(f"  candidate files returned by extractor: {len(rows)}")
    print(f"  gate_params updated: {updated}")
    print(f"  skipped (no params): {skipped_no_params}")
    print(f"  skipped (already populated): {skipped_already_done}")


def fetch_feature_source_rows(conn):
    sql = """
        SELECT
            md.id AS metadata_id,
            md.experiment,
            md.device_id,
            md.sample_group,
            md.device_type,
            md.manufacturer,
            md.measurement_category,
            md.measurement_type,
            md.filename,
            md.csv_path,
            md.created_at AS metadata_created_at,
            md.bias_value,
            md.drain_bias_value,
            md.sweep_start,
            md.sweep_stop,
            md.sweep_points,
            md.step_num,
            md.step_start,
            md.step_stop,
            md.data_source,
            md.test_condition,
            md.irrad_role,
            md.irrad_campaign_id,
            md.irrad_run_id,
            md.fluence_at_meas,
            md.sc_voltage_v,
            md.sc_duration_us,
            md.sc_vgs_on_v,
            md.sc_vgs_off_v,
            md.sc_condition_label,
            md.sc_sequence_num,
            md.promotion_decision,
            md.is_likely_irradiated,
            md.gate_params,
            dl.voltage_rating AS library_voltage_rating,
            dl.rdson_mohm AS library_rdson_mohm,
            dl.current_rating_a AS library_current_rating_a,
            dl.package_type AS library_package_type,
            COALESCE(md.manufacturer, dl.manufacturer) AS resolved_manufacturer,
            ir.ion_species,
            ir.beam_energy_mev,
            ir.let_surface,
            ir.let_bragg_peak,
            ir.range_um,
            COALESCE(ir.beam_type, ic.beam_type) AS beam_type
        FROM baselines_metadata md
        LEFT JOIN device_library dl
               ON lower(dl.part_number) = lower(md.device_type)
        LEFT JOIN irradiation_runs ir
               ON ir.id = md.irrad_run_id
        LEFT JOIN irradiation_campaigns ic
               ON ic.id = md.irrad_campaign_id
        WHERE md.measurement_category IN ('IdVg', 'Vth', 'IdVd')
        ORDER BY md.id
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def build_feature_tuple(row):
    curve = curve_family_for(row)
    target = CURVE_TO_TARGET.get(curve)
    gate_params = row.get("gate_params") or {}
    vth_v = metric_from_gate_params(gate_params, "vth_v")
    rdson_mohm = metric_from_gate_params(gate_params, "rdson_mohm")
    bvdss_v = metric_from_gate_params(gate_params, "bvdss_v")
    vsd_v = metric_from_gate_params(gate_params, "vsd_v")
    pkey = physical_device_key(row)
    enriched = dict(row)
    enriched["physical_device_key"] = pkey
    status, flags = quality_for_feature(enriched, curve, vth_v, rdson_mohm)

    return (
        row["metadata_id"],
        curve,
        target,
        row["measurement_category"],
        row.get("measurement_type"),
        row.get("filename"),
        row.get("csv_path"),
        row.get("metadata_created_at"),
        safe_float(row.get("bias_value")),
        safe_float(row.get("drain_bias_value")),
        safe_float(row.get("sweep_start")),
        safe_float(row.get("sweep_stop")),
        row.get("sweep_points"),
        row.get("step_num"),
        safe_float(row.get("step_start")),
        safe_float(row.get("step_stop")),
        row.get("experiment"),
        row.get("data_source") or "baselines",
        row.get("test_condition"),
        row.get("irrad_role"),
        row.get("device_id"),
        row.get("sample_group"),
        pkey,
        row.get("device_type"),
        row.get("resolved_manufacturer") or row.get("manufacturer"),
        parse_rating(row.get("library_voltage_rating"), "voltage"),
        parse_rating(row.get("library_rdson_mohm"), "rdson"),
        parse_rating(row.get("library_current_rating_a"), "current"),
        row.get("library_package_type"),
        vth_v,
        rdson_mohm,
        bvdss_v,
        vsd_v,
        Json(gate_params),
        safe_float(row.get("sc_voltage_v")),
        safe_float(row.get("sc_duration_us")),
        safe_float(row.get("sc_vgs_on_v")),
        safe_float(row.get("sc_vgs_off_v")),
        row.get("sc_condition_label"),
        row.get("sc_sequence_num"),
        row.get("irrad_campaign_id"),
        row.get("irrad_run_id"),
        row.get("ion_species"),
        safe_float(row.get("beam_energy_mev")),
        safe_float(row.get("let_surface")),
        safe_float(row.get("let_bragg_peak")),
        safe_float(row.get("range_um")),
        row.get("beam_type"),
        safe_float(row.get("fluence_at_meas")),
        row.get("promotion_decision"),
        row.get("is_likely_irradiated"),
        status,
        flags,
    )


def extract_features(conn):
    apply_schema(conn)
    warn_gate_param_coverage(conn)
    print("\nRebuilding V2 physical feature snapshot ...")
    truncate_tables(conn, V2_TABLES)

    rows = fetch_feature_source_rows(conn)
    values = [build_feature_tuple(row) for row in rows]
    columns = (
        "metadata_id",
        "curve_family",
        "target_type",
        "measurement_category",
        "measurement_type",
        "filename",
        "csv_path",
        "metadata_created_at",
        "bias_value",
        "drain_bias_value",
        "sweep_start",
        "sweep_stop",
        "sweep_points",
        "step_num",
        "step_start",
        "step_stop",
        "experiment",
        "data_source",
        "test_condition",
        "irrad_role",
        "device_id",
        "sample_group",
        "physical_device_key",
        "device_type",
        "manufacturer",
        "voltage_rating_v",
        "rdson_rating_mohm",
        "current_rating_a",
        "package_type",
        "vth_v",
        "rdson_mohm",
        "bvdss_v",
        "vsd_v",
        "gate_params_snapshot",
        "sc_voltage_v",
        "sc_duration_us",
        "sc_vgs_on_v",
        "sc_vgs_off_v",
        "sc_condition_label",
        "sc_sequence_num",
        "irrad_campaign_id",
        "irrad_run_id",
        "ion_species",
        "beam_energy_mev",
        "let_surface",
        "let_bragg_peak",
        "range_um",
        "beam_type",
        "fluence_at_meas",
        "promotion_decision",
        "is_likely_irradiated",
        "quality_status",
        "quality_flags",
    )
    if values:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO iv_physical_curve_features ({', '.join(columns)})
                VALUES %s
                """,
                values,
                page_size=1000,
            )
        conn.commit()
    print(f"  inserted feature rows: {len(values)}")
    print_feature_coverage(conn)


def print_feature_coverage(conn):
    queries = [
        (
            "coverage by source / curve / quality",
            """
            SELECT data_source, curve_family, quality_status, COUNT(*)
            FROM iv_physical_curve_features
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
        ),
        (
            "coverage by device_type / curve",
            """
            SELECT COALESCE(device_type, '<null>'), curve_family,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE quality_status = 'usable') AS usable
            FROM iv_physical_curve_features
            GROUP BY 1, 2
            ORDER BY usable DESC, total DESC, 1, 2
            LIMIT 40
            """,
        ),
        (
            "coverage by stress condition",
            """
            SELECT data_source,
                   COALESCE(test_condition, irrad_role, '<none>') AS stress_condition,
                   curve_family,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE quality_status = 'usable') AS usable
            FROM iv_physical_curve_features
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
        ),
    ]
    with conn.cursor() as cur:
        for title, sql in queries:
            print(f"\n{title}:")
            cur.execute(sql)
            for row in cur.fetchall():
                print("  " + " | ".join(str(v) for v in row))

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT flag, COUNT(*)
            FROM iv_physical_curve_features f
            CROSS JOIN LATERAL unnest(f.quality_flags) AS flag
            GROUP BY flag
            ORDER BY COUNT(*) DESC, flag
            """
        )
        rows = cur.fetchall()
    if rows:
        print("\nfeature quality flag counts:")
        for flag, count in rows:
            print(f"  {flag}: {count}")


def load_features(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM iv_physical_curve_features
            WHERE curve_family IN ('IdVg', 'IdVd')
            ORDER BY id
            """
        )
        return list(cur.fetchall())


def abs_diff_if_both(left, right):
    left = safe_float(left)
    right = safe_float(right)
    if left is None or right is None:
        return None
    return abs(left - right)


def ranges_overlap(pre_start, pre_stop, post_start, post_stop):
    vals = [safe_float(v) for v in (pre_start, pre_stop, post_start, post_stop)]
    if any(v is None for v in vals):
        return True
    pre_lo, pre_hi = sorted(vals[:2])
    post_lo, post_hi = sorted(vals[2:])
    return max(pre_lo, post_lo) <= min(pre_hi, post_hi)


def baseline_metric_value(feature):
    if feature["target_type"] == "delta_vth_v":
        return safe_float(feature.get("vth_v"))
    if feature["target_type"] == "log_rdson_ratio":
        rdson = safe_float(feature.get("rdson_mohm"))
        if rdson is None or rdson <= 0:
            return None
        return math.log(rdson)
    return None


def _fmt_group_value(value):
    value = safe_float(value)
    if value is None:
        return "missing"
    return f"{value:.6g}"


def library_reference_group_key(post):
    bias_value = (
        post.get("drain_bias_value")
        if post["target_type"] == "delta_vth_v"
        else post.get("bias_value")
    )
    sweep_start = _fmt_group_value(post.get("sweep_start"))
    sweep_stop = _fmt_group_value(post.get("sweep_stop"))
    sweep_points = post.get("sweep_points")
    points = str(sweep_points) if sweep_points is not None else "missing"
    return "|".join(
        [
            "library_pristine",
            str(post.get("target_type")),
            str(post.get("curve_family")),
            str(post.get("device_type")),
            f"bias={_fmt_group_value(bias_value)}",
            f"sweep={sweep_start}:{sweep_stop}",
            f"points={points}",
        ]
    )


def baseline_reference_stats(compatible_rows, target_type):
    values = []
    for row in compatible_rows:
        candidate = row[3]
        value = baseline_metric_value(candidate)
        if value is not None:
            values.append(value)
    if not values:
        return {
            "baseline_reference_count": 0,
            "baseline_reference_spread": None,
        }
    if len(values) == 1:
        spread = 0.0
    else:
        spread = percentile(values, 0.75) - percentile(values, 0.25)
    return {
        "baseline_reference_count": len(values),
        "baseline_reference_spread": spread,
    }


def setup_compatibility_score(pre, post, stress_type):
    """Return (score, flags, reject_reason) for a candidate pre feature."""
    score = 0.0
    flags = []

    if pre.get("measurement_category") == post.get("measurement_category"):
        flags.append("same_measurement_category")
    else:
        score += 2.0
        flags.append("mixed_measurement_category")

    if post["target_type"] == "delta_vth_v":
        diff = abs_diff_if_both(pre.get("drain_bias_value"), post.get("drain_bias_value"))
        if diff is not None:
            if diff > 0.25:
                return None, flags, "incompatible_drain_bias"
            score += diff
            flags.append("compatible_drain_bias")
        else:
            score += 0.5
            flags.append("missing_drain_bias_for_compatibility")

    if post["target_type"] == "log_rdson_ratio":
        diff = abs_diff_if_both(pre.get("bias_value"), post.get("bias_value"))
        if diff is not None:
            if diff > 0.75:
                return None, flags, "incompatible_gate_bias"
            score += diff
            flags.append("compatible_gate_bias")
        else:
            score += 0.5
            flags.append("missing_gate_bias_for_compatibility")

    if not ranges_overlap(
        pre.get("sweep_start"), pre.get("sweep_stop"),
        post.get("sweep_start"), post.get("sweep_stop"),
    ):
        return None, flags, "incompatible_sweep_range"
    flags.append("compatible_sweep_range")

    pre_points = pre.get("sweep_points")
    post_points = post.get("sweep_points")
    if pre_points is not None and post_points is not None:
        try:
            point_delta = abs(int(pre_points) - int(post_points))
        except (TypeError, ValueError):
            point_delta = 0
        if point_delta > 0:
            score += min(point_delta / 100.0, 1.0)
            flags.append("different_sweep_points")
        else:
            flags.append("same_sweep_points")

    if stress_type == "sc":
        pre_seq = pre.get("sc_sequence_num")
        post_seq = post.get("sc_sequence_num")
        if pre_seq is not None and post_seq is not None and pre_seq > post_seq:
            return None, flags, "pre_sc_sequence_after_post"
        flags.append("compatible_sc_sequence")

    pre_ts = pre.get("metadata_created_at")
    post_ts = post.get("metadata_created_at")
    if pre_ts is not None and post_ts is not None:
        if pre_ts > post_ts:
            score += 0.25
            flags.append("pre_metadata_created_after_post")
        delta_days = abs((post_ts - pre_ts).total_seconds()) / 86400.0
        score += min(delta_days / 365.0, 1.0)
        flags.append("closest_metadata_created_at")

    return score, flags, None


def choose_pre_feature(candidates, post, stress_type):
    scored = []
    reject_reasons = Counter()
    for candidate in candidates:
        score, flags, reason = setup_compatibility_score(candidate, post, stress_type)
        if reason:
            reject_reasons[reason] += 1
            continue
        scored.append((score, candidate.get("metadata_created_at"), candidate["metadata_id"], candidate, flags))
    if not scored:
        reason = reject_reasons.most_common(1)[0][0] if reject_reasons else "no_compatible_pre_candidate"
        return None, [], reason

    scored.sort(key=lambda item: (
        item[0],
        item[1] is None,
        item[1] or datetime.min.replace(tzinfo=None),
        item[2],
    ))
    best = scored[0]
    if len(scored) > 1 and abs(scored[1][0] - best[0]) < 1e-12:
        return None, [], "ambiguous_equally_compatible_pre_candidates"
    flags = ["compatible_pre_selected"] + best[4]
    return best[3], flags, None


def compatible_library_rows(candidates, post):
    scored = []
    reject_reasons = Counter()
    for candidate in candidates:
        if candidate.get("physical_device_key") == post.get("physical_device_key"):
            reject_reasons["library_candidate_same_physical_device"] += 1
            continue
        score, flags, reason = setup_compatibility_score(candidate, post, "irradiation")
        if reason:
            reject_reasons[reason] += 1
            continue
        scored.append((
            score,
            candidate.get("metadata_created_at"),
            candidate["metadata_id"],
            candidate,
            flags,
        ))
    scored.sort(key=lambda item: (
        item[0],
        item[1] is None,
        item[1] or datetime.min.replace(tzinfo=None),
        item[2],
    ))
    return scored, reject_reasons


def choose_library_feature(candidates, post):
    scored, reject_reasons = compatible_library_rows(candidates, post)
    if not scored:
        reason = (
            reject_reasons.most_common(1)[0][0]
            if reject_reasons
            else "no_compatible_library_pristine_candidate"
        )
        return None, [], reason, {}

    stats = baseline_reference_stats(scored, post["target_type"])
    if stats["baseline_reference_count"] <= 0:
        return None, [], "library_pristine_candidates_missing_metric", stats

    best = scored[0]
    selection_flags = list(best[4])
    flags = [
        "library_pristine_reference",
        "not_same_physical_device",
        "same_device_type",
        "baseline_spread_checked",
    ]
    if "compatible_drain_bias" in selection_flags or "compatible_gate_bias" in selection_flags:
        flags.append("compatible_bias")
    flags.extend(selection_flags)
    stats.update({
        "baseline_reference_method": LIBRARY_REFERENCE_METHOD,
        "library_reference_group_key": library_reference_group_key(post),
    })
    return best[3], flags, None, stats


def response_from_features(pre, post):
    if post["target_type"] == "delta_vth_v":
        pre_v = safe_float(pre.get("vth_v"))
        post_v = safe_float(post.get("vth_v"))
        if pre_v is None or post_v is None:
            return None, "missing_vth_pair_value"
        delta = post_v - pre_v
        if not finite(delta) or abs(delta) > 50.0:
            return None, "nonsensical_delta_vth_v"
        return delta, None

    if post["target_type"] == "log_rdson_ratio":
        pre_r = safe_float(pre.get("rdson_mohm"))
        post_r = safe_float(post.get("rdson_mohm"))
        if pre_r is None or post_r is None or pre_r <= 0 or post_r <= 0:
            return None, "missing_or_nonpositive_rdson_pair_value"
        ratio = math.log(post_r / pre_r)
        if not finite(ratio) or abs(ratio) > 10.0:
            return None, "nonsensical_log_rdson_ratio"
        return ratio, None

    return None, "unknown_target_type"


def pair_tuple(
    pre,
    post,
    stress_type,
    response_value,
    selection_flags=None,
    reference_tier="strict_pre_irrad",
    baseline_reference_count=1,
    baseline_reference_spread=0.0,
    baseline_reference_method="strict_same_physical_device",
    library_reference_group_key=None,
):
    target = post["target_type"]
    pair_key = (
        f"{stress_type}:{reference_tier}:{target}:"
        f"pre{pre['metadata_id']}:post{post['metadata_id']}"
    )
    split_group = f"{stress_type}:{reference_tier}:{target}:{post['physical_device_key']}"
    same_physical_device = pre.get("physical_device_key") == post.get("physical_device_key")
    flags = [f"reference_tier_{reference_tier}"]
    flags.extend(selection_flags or [])
    if reference_tier == "library_pristine":
        pairing_method = "same_device_type_library_pristine_to_post_irrad"
        flags.extend(["same_device_type"])
    elif stress_type == "sc":
        flags.extend([
            "strict_device_type_match",
            "strict_physical_device_key_match",
            "strict_sc_sample_group_match",
        ])
        pairing_method = "same_device_type_sample_group_pristine_to_post_sc"
    else:
        flags.extend([
            "strict_device_type_match",
            "strict_physical_device_key_match",
            "strict_irrad_device_key_match",
        ])
        pairing_method = "same_device_type_device_key_pre_to_post_irrad"

    delta_vth_v = response_value if target == "delta_vth_v" else None
    log_rdson_ratio = response_value if target == "log_rdson_ratio" else None

    return (
        pair_key,
        stress_type,
        pairing_method,
        reference_tier,
        post["curve_family"],
        target,
        pre["id"],
        post["id"],
        pre["metadata_id"],
        post["metadata_id"],
        post["physical_device_key"],
        split_group,
        same_physical_device,
        post["device_type"],
        post.get("manufacturer") or pre.get("manufacturer"),
        post.get("voltage_rating_v") or pre.get("voltage_rating_v"),
        post.get("rdson_rating_mohm") or pre.get("rdson_rating_mohm"),
        post.get("current_rating_a") or pre.get("current_rating_a"),
        post.get("package_type") or pre.get("package_type"),
        pre.get("vth_v"),
        post.get("vth_v"),
        pre.get("rdson_mohm"),
        post.get("rdson_mohm"),
        delta_vth_v,
        log_rdson_ratio,
        post.get("sc_voltage_v"),
        post.get("sc_duration_us"),
        post.get("sc_vgs_on_v"),
        post.get("sc_vgs_off_v"),
        post.get("sc_condition_label"),
        post.get("sc_sequence_num"),
        post.get("irrad_campaign_id"),
        post.get("irrad_run_id"),
        post.get("ion_species"),
        post.get("beam_energy_mev"),
        post.get("let_surface"),
        post.get("let_bragg_peak"),
        post.get("range_um"),
        post.get("beam_type"),
        post.get("fluence_at_meas"),
        baseline_reference_count,
        baseline_reference_spread,
        baseline_reference_method,
        library_reference_group_key,
        "usable",
        flags,
    )


def library_pristine_index(features):
    index = defaultdict(list)
    for feature in features:
        if not is_library_pristine_reference(feature):
            continue
        key = (
            feature["target_type"],
            feature["curve_family"],
            feature["device_type"],
        )
        index[key].append(feature)
    return index


def library_pair_for_post(post, library_pre):
    key = (
        post["target_type"],
        post["curve_family"],
        post["device_type"],
    )
    candidates = library_pre.get(key, [])
    if not candidates:
        return None, "irrad_no_library_pristine_candidate"
    pre, selection_flags, reason, stats = choose_library_feature(candidates, post)
    if reason:
        return None, reason
    response, reason = response_from_features(pre, post)
    if reason:
        return None, reason
    return pair_tuple(
        pre,
        post,
        "irradiation",
        response,
        selection_flags,
        reference_tier="library_pristine",
        baseline_reference_count=stats.get("baseline_reference_count"),
        baseline_reference_spread=stats.get("baseline_reference_spread"),
        baseline_reference_method=stats.get("baseline_reference_method"),
        library_reference_group_key=stats.get("library_reference_group_key"),
    ), None


def build_pairs(conn, include_library_pristine=False):
    apply_schema(conn)
    mode = "strict + library-pristine" if include_library_pristine else "strict"
    print(f"\nRebuilding {mode} V2 response pairs ...")
    truncate_tables(conn, V2_DOWNSTREAM_TABLES)
    features = load_features(conn)
    usable = [f for f in features if f["quality_status"] == "usable"]
    reasons = Counter()
    pairs = []

    sc_pre = defaultdict(list)
    ir_pre = defaultdict(list)
    for feature in usable:
        key = (
            feature["target_type"],
            feature["curve_family"],
            feature["device_type"],
            feature["physical_device_key"],
        )
        if feature["data_source"] == "sc_ruggedness" and feature["test_condition"] == "pristine":
            sc_pre[key].append(feature)
        if feature["irrad_role"] == "pre_irrad":
            ir_pre[key].append(feature)
    library_pre = library_pristine_index(usable)

    for post in usable:
        key = (
            post["target_type"],
            post["curve_family"],
            post["device_type"],
            post["physical_device_key"],
        )
        if post["data_source"] == "sc_ruggedness" and post["test_condition"] == "post_sc":
            if not finite(post.get("sc_voltage_v")) or not finite(post.get("sc_duration_us")):
                reasons["sc_post_missing_voltage_or_duration"] += 1
                continue
            candidates = sc_pre.get(key, [])
            if not candidates:
                reasons["sc_no_matching_pristine_feature"] += 1
                continue
            pre, selection_flags, reason = choose_pre_feature(candidates, post, "sc")
            if reason:
                reasons[reason] += 1
                continue
            response, reason = response_from_features(pre, post)
            if reason:
                reasons[reason] += 1
                continue
            pairs.append(pair_tuple(pre, post, "sc", response, selection_flags))

        if post["irrad_role"] == "post_irrad":
            if post.get("irrad_run_id") is None:
                reasons["irrad_post_missing_irrad_run_id"] += 1
                continue
            candidates = ir_pre.get(key, [])
            if not candidates:
                strict_reason = "irrad_no_matching_pre_irrad_feature"
            else:
                pre, selection_flags, strict_reason = choose_pre_feature(
                    candidates, post, "irradiation"
                )
                if not strict_reason:
                    response, strict_reason = response_from_features(pre, post)
                if not strict_reason:
                    pairs.append(pair_tuple(pre, post, "irradiation", response, selection_flags))
                    continue

            if include_library_pristine:
                pair, library_reason = library_pair_for_post(post, library_pre)
                if pair:
                    reasons[f"library_fallback_from_{strict_reason}"] += 1
                    pairs.append(pair)
                    continue
                reasons[library_reason] += 1
            else:
                reasons[strict_reason] += 1

    columns = (
        "pair_key",
        "stress_type",
        "pairing_method",
        "reference_tier",
        "curve_family",
        "target_type",
        "pre_feature_id",
        "post_feature_id",
        "pre_metadata_id",
        "post_metadata_id",
        "physical_device_key",
        "split_group",
        "same_physical_device",
        "device_type",
        "manufacturer",
        "voltage_rating_v",
        "rdson_rating_mohm",
        "current_rating_a",
        "package_type",
        "pre_vth_v",
        "post_vth_v",
        "pre_rdson_mohm",
        "post_rdson_mohm",
        "delta_vth_v",
        "log_rdson_ratio",
        "sc_voltage_v",
        "sc_duration_us",
        "sc_vgs_on_v",
        "sc_vgs_off_v",
        "sc_condition_label",
        "sc_sequence_num",
        "irrad_campaign_id",
        "irrad_run_id",
        "ion_species",
        "beam_energy_mev",
        "let_surface",
        "let_bragg_peak",
        "range_um",
        "beam_type",
        "fluence_at_meas",
        "baseline_reference_count",
        "baseline_reference_spread",
        "baseline_reference_method",
        "library_reference_group_key",
        "quality_status",
        "quality_flags",
    )
    if pairs:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO iv_physical_response_pairs ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (pair_key) DO NOTHING
                """,
                pairs,
                page_size=1000,
            )
        conn.commit()

    print_pair_summary(conn, reasons)


def print_pair_summary(conn, reasons):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT reference_tier, target_type, stress_type, COUNT(*)
            FROM iv_physical_response_pairs
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """
        )
        print("\npair count by reference tier / target / stress:")
        for reference_tier, target, stress, count in cur.fetchall():
            print(f"  {reference_tier} | {target} | {stress}: {count}")

        cur.execute(
            """
            SELECT reference_tier, device_type, target_type, COUNT(*)
            FROM iv_physical_response_pairs
            GROUP BY 1, 2, 3
            ORDER BY COUNT(*) DESC, 1, 2, 3
            LIMIT 60
            """
        )
        print("\npair count by reference tier / device_type / target:")
        for reference_tier, device_type, target, count in cur.fetchall():
            print(f"  {reference_tier} | {device_type} | {target}: {count}")

        cur.execute(
            """
            SELECT flag, COUNT(*)
            FROM iv_physical_curve_features f
            CROSS JOIN LATERAL unnest(f.quality_flags) AS flag
            GROUP BY flag
            ORDER BY COUNT(*) DESC, flag
            """
        )
        feature_reasons = Counter(dict(cur.fetchall()))

    combined = Counter()
    combined.update(feature_reasons)
    combined.update(reasons)
    if combined:
        print("\nunsupported / missing reason counts:")
        for reason, count in combined.most_common():
            print(f"  {reason}: {count}")


def _audit_summary_value(values, q):
    value = percentile(values, q)
    return f"{value:.6g}" if value is not None else "NULL"


def print_audit_group(rows, title, fields):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(field) for field in fields)].append(row)
    print(f"\n{title}:")
    print("group | n | median_abs_library_error | p90_abs_library_error | median_abs_strict_delta | median_baseline_spread")
    for key, group_rows in sorted(groups.items(), key=lambda item: (-len(item[1]), str(item[0]))):
        errors = [row["abs_library_error"] for row in group_rows]
        strict_abs = [abs(row["strict_response"]) for row in group_rows]
        spreads = [
            row["baseline_reference_spread"]
            for row in group_rows
            if row.get("baseline_reference_spread") is not None
        ]
        key_s = " / ".join(str(part) for part in key)
        print(
            f"  {key_s} | {len(group_rows)} | "
            f"{_audit_summary_value(errors, 0.5)} | "
            f"{_audit_summary_value(errors, 0.9)} | "
            f"{_audit_summary_value(strict_abs, 0.5)} | "
            f"{_audit_summary_value(spreads, 0.5)}"
        )


def audit_library_pristine(conn):
    apply_schema(conn)
    features = load_features(conn)
    feature_by_id = {feature["id"]: feature for feature in features}
    library_pre = library_pristine_index(
        feature for feature in features if feature["quality_status"] == "usable"
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM iv_physical_response_pairs
            WHERE quality_status = 'usable'
              AND stress_type = 'irradiation'
              AND reference_tier = 'strict_pre_irrad'
            ORDER BY id
            """
        )
        strict_pairs = list(cur.fetchall())

    audit_rows = []
    reasons = Counter()
    for pair in strict_pairs:
        post = feature_by_id.get(pair["post_feature_id"])
        if post is None:
            reasons["missing_post_feature"] += 1
            continue
        key = (
            post["target_type"],
            post["curve_family"],
            post["device_type"],
        )
        candidates = library_pre.get(key, [])
        pre, selection_flags, reason, stats = choose_library_feature(candidates, post)
        if reason:
            reasons[reason] += 1
            continue
        library_response, reason = response_from_features(pre, post)
        if reason:
            reasons[reason] += 1
            continue
        strict_response = target_value(pair)
        if strict_response is None:
            reasons["missing_strict_response"] += 1
            continue
        audit_rows.append({
            "stress_type": pair["stress_type"],
            "target_type": pair["target_type"],
            "curve_family": pair["curve_family"],
            "reference_tier": "library_pristine",
            "device_type": pair["device_type"],
            "ion_species": pair.get("ion_species"),
            "irrad_run_id": pair.get("irrad_run_id"),
            "strict_response": strict_response,
            "library_response": library_response,
            "abs_library_error": abs(library_response - strict_response),
            "baseline_reference_count": stats.get("baseline_reference_count"),
            "baseline_reference_spread": stats.get("baseline_reference_spread"),
            "library_pre_metadata_id": pre["metadata_id"],
            "post_metadata_id": post["metadata_id"],
        })

    print("\nLibrary-pristine audit against existing strict irradiation pairs:")
    print(f"  strict irradiation pairs: {len(strict_pairs)}")
    print(f"  auditable strict pairs: {len(audit_rows)}")
    if reasons:
        print("  audit skip reasons:")
        for reason, count in reasons.most_common():
            print(f"    {reason}: {count}")
    if not audit_rows:
        return
    print_audit_group(audit_rows, "by stress / target / reference tier", (
        "stress_type", "target_type", "reference_tier",
    ))
    print_audit_group(audit_rows, "by target / device type", (
        "target_type", "device_type",
    ))
    print_audit_group(audit_rows, "by target / ion species", (
        "target_type", "ion_species",
    ))
    print_audit_group(audit_rows, "by target / irradiation run", (
        "target_type", "irrad_run_id",
    ))
    print_audit_group(audit_rows, "by curve family", ("curve_family",))


def load_pairs(conn, reference_tier="both"):
    tiers = reference_tiers_for_option(reference_tier)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM iv_physical_response_pairs
            WHERE quality_status = 'usable'
              AND reference_tier = ANY(%s)
            ORDER BY id
            """,
            (list(tiers),),
        )
        return list(cur.fetchall())


def fit_feature_scales(pairs):
    scales = {}
    for stress_type, feature_names in NUMERIC_FEATURES.items():
        for target_type in TARGET_LABELS:
            group = [
                row
                for row in pairs
                if row["stress_type"] == stress_type
                and row["target_type"] == target_type
            ]
            key = f"{stress_type}|{target_type}"
            scales[key] = {
                name: robust_scale(row_feature_value(row, name) for row in group)
                for name in feature_names
            }
    return scales


def pair_counts(pairs):
    counts = Counter()
    for pair in pairs:
        counts[f"{pair['reference_tier']}|{pair['stress_type']}|{pair['target_type']}"] += 1
    return dict(sorted(counts.items()))


def feature_config(scales, pairs):
    reference_tiers = sorted({pair["reference_tier"] for pair in pairs})
    return {
        "model_version": MODEL_VERSION,
        "algorithm": ALGORITHM,
        "supported_curve_families": ["IdVg", "IdVd"],
        "out_of_scope_curve_families": list(OUT_OF_SCOPE_CURVES),
        "targets": {
            "IdVg/Vth": "delta_vth_v",
            "IdVd": "log_rdson_ratio",
        },
        "measurement_category_to_curve_family": MEASUREMENT_CATEGORY_TO_CURVE,
        "validation_modes": VALIDATION_MODES,
        "reference_tiers": reference_tiers,
        "reference_tier_labels": REFERENCE_TIER_LABELS,
        "min_donors": MIN_DONORS,
        "nearest_k": NEAREST_K,
        "numeric_features": NUMERIC_FEATURES,
        "feature_scales": scales,
        "pair_counts": pair_counts(pairs),
        "categorical_policy": {
            "device_type": "exact_match_required",
            "reference_tier": "same_reference_tier_required",
            "ion_species": (
                "tier_a_exact_match_required; "
                "tier_b_broad_ion_let_neighborhood_disabled_until_validated"
            ),
        },
        "broad_ion_neighborhood_enabled": BROAD_ION_NEIGHBORHOOD_ENABLED,
        "validation_gates": VALIDATION_GATES,
    }


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def train(conn, reference_tier="both"):
    apply_schema(conn)
    pairs = load_pairs(conn, reference_tier=reference_tier)
    scales = fit_feature_scales(pairs)
    config = feature_config(scales, pairs)
    status = "pending_validation" if pairs else "unsupported"
    notes = (
        "Physical donor model trained for IdVg/IdVd parameter deltas only; "
        "curve reconstruction is disabled by default; run --predict-curves "
        "to persist exploratory confidence-labeled V2 curves. "
        f"Reference tier mode: {reference_tier}."
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO iv_physical_model_runs (
                model_version, algorithm, target_stress_types, curve_families,
                train_pairs, validation_pairs, supported_validation_pairs,
                unsupported_validation_pairs, model_status, metrics,
                feature_config, notes
            )
            VALUES (
                %s, %s, %s, %s,
                %s, NULL, NULL,
                NULL, %s, %s,
                %s, %s
            )
            RETURNING id
            """,
            (
                MODEL_VERSION,
                ALGORITHM,
                ["sc", "irradiation"],
                ["IdVg", "IdVd"],
                len(pairs),
                status,
                Json({}),
                Json(config),
                notes,
            ),
        )
        run_id = cur.fetchone()[0]
    conn.commit()

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    artifact_path = MODEL_DIR / f"{run_id}.json"
    artifact = {
        "model_run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "feature_config": config,
        "training_pair_ids": [int(p["id"]) for p in pairs],
        "training_pair_keys": [p["pair_key"] for p in pairs],
        "training_pair_count": len(pairs),
        "reference_tier_mode": reference_tier,
    }
    artifact_path.write_text(json.dumps(artifact, indent=2, default=json_default))
    rel_artifact = str(artifact_path.relative_to(REPO_ROOT))
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE iv_physical_model_runs SET artifact_path = %s WHERE id = %s",
            (rel_artifact, run_id),
        )
    conn.commit()

    print("\nTraining complete:")
    print(f"  model_run_id: {run_id}")
    print(f"  train_pairs: {len(pairs)}")
    print(f"  artifact_path: {rel_artifact}")
    print("  curve reconstruction: disabled until --predict-curves")
    return run_id


def latest_model_run_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM iv_physical_model_runs ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
    return row[0] if row else None


def load_run_config(conn, run_id):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT feature_config FROM iv_physical_model_runs WHERE id = %s",
            (run_id,),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"model run {run_id} does not exist")
    return row["feature_config"] or {}


def normalized_distance(target, donor, config):
    stress_type = target["stress_type"]
    target_type = target["target_type"]
    feature_names = NUMERIC_FEATURES[stress_type]
    scale_group = (config.get("feature_scales") or {}).get(
        f"{stress_type}|{target_type}", {}
    )
    total = 0.0
    dims = 0
    for name in feature_names:
        tv = row_feature_value(target, name)
        dv = row_feature_value(donor, name)
        if tv is None or dv is None:
            continue
        scale_info = scale_group.get(name) or {}
        scale = safe_float(scale_info.get("scale")) or 1.0
        if scale <= 0:
            scale = 1.0
        total += ((tv - dv) / scale) ** 2
        dims += 1
    if dims == 0:
        return None, 0
    return math.sqrt(total / dims), dims


def same_sc_condition(left, right):
    return (
        safe_float(left.get("sc_voltage_v")) == safe_float(right.get("sc_voltage_v"))
        and safe_float(left.get("sc_duration_us")) == safe_float(right.get("sc_duration_us"))
    )


def same_irrad_condition(left, right):
    left_run = left.get("irrad_run_id")
    right_run = right.get("irrad_run_id")
    return left_run is not None and right_run is not None and left_run == right_run


def donor_allowed_for_validation(target, donor, validation_mode):
    if donor["id"] == target["id"]:
        return False
    if donor["split_group"] == target["split_group"]:
        return False
    if validation_mode == "leave_condition":
        if target["stress_type"] == "sc" and same_sc_condition(target, donor):
            return False
        if target["stress_type"] == "irradiation" and same_irrad_condition(target, donor):
            return False
    return True


def predict_from_donors(target, pairs, config, validation_mode="within_condition"):
    min_donors = int(config.get("min_donors") or MIN_DONORS)
    nearest_k = int(config.get("nearest_k") or NEAREST_K)
    candidates = [
        donor
        for donor in pairs
        if donor["target_type"] == target["target_type"]
        and donor["stress_type"] == target["stress_type"]
        and donor["reference_tier"] == target["reference_tier"]
        and donor["device_type"] == target["device_type"]
        and donor_allowed_for_validation(target, donor, validation_mode)
    ]
    if len(candidates) < min_donors:
        return {
            "support_status": "unsupported",
            "unsupported_reason": "insufficient_same_reference_tier_device_type_donor_pairs",
            "donor_count": len(candidates),
        }

    if target["stress_type"] == "irradiation" and target.get("ion_species"):
        same_ion = [
            donor
            for donor in candidates
            if donor.get("ion_species")
            and donor["ion_species"].lower() == target["ion_species"].lower()
        ]
        if len(same_ion) < min_donors:
            if (
                target.get("reference_tier") == "library_pristine"
                and config.get("broad_ion_neighborhood_enabled")
            ):
                return {
                    "support_status": "unsupported",
                    "unsupported_reason": "broad_ion_neighborhood_policy_not_implemented",
                    "donor_count": len(same_ion),
                }
            return {
                "support_status": "unsupported",
                "unsupported_reason": (
                    "insufficient_same_ion_species_donor_pairs"
                    if target.get("reference_tier") != "library_pristine"
                    else "insufficient_same_ion_species_donor_pairs_broad_policy_not_validated"
                ),
                "donor_count": len(same_ion),
            }
        candidates = same_ion

    distances = []
    for donor in candidates:
        distance, dims = normalized_distance(target, donor, config)
        if distance is None:
            continue
        value = target_value(donor)
        if value is None:
            continue
        distances.append((distance, dims, donor, value))
    if len(distances) < min_donors:
        return {
            "support_status": "unsupported",
            "unsupported_reason": "insufficient_comparable_numeric_features",
            "donor_count": len(distances),
        }

    distances.sort(key=lambda item: (item[0], item[2]["pair_key"]))
    chosen = distances[: max(min_donors, min(nearest_k, len(distances)))]
    values = [item[3] for item in chosen]
    weights = [1.0 / max(item[0], 0.05) for item in chosen]
    prediction = weighted_quantile(values, weights, 0.5)
    p10 = weighted_quantile(values, weights, 0.10)
    p90 = weighted_quantile(values, weights, 0.90)
    mean_distance = sum(item[0] for item in chosen) / len(chosen)
    return {
        "support_status": "ok",
        "predicted_value": prediction,
        "predicted_p10": p10,
        "predicted_p90": p90,
        "donor_count": len(chosen),
        "donor_distance": mean_distance,
        "donor_pair_keys": [item[2]["pair_key"] for item in chosen],
    }


def validation_tuple(run_id, validation_mode, pair, pred):
    observed = target_value(pair)
    predicted = pred.get("predicted_value")
    residual = None
    abs_residual = None
    if observed is not None and predicted is not None:
        residual = predicted - observed
        abs_residual = abs(residual)

    return (
        run_id,
        validation_mode,
        pair["id"],
        pair["pair_key"],
        pair["split_group"],
        pair["reference_tier"],
        pair["stress_type"],
        pair["curve_family"],
        pair["target_type"],
        observed,
        predicted,
        pred.get("predicted_p10"),
        pred.get("predicted_p90"),
        residual,
        abs_residual,
        pair.get("device_type"),
        pair.get("manufacturer"),
        pair.get("physical_device_key"),
        pair.get("sc_voltage_v"),
        pair.get("sc_duration_us"),
        pair.get("irrad_run_id"),
        pair.get("ion_species"),
        pair.get("beam_energy_mev"),
        pair.get("let_surface"),
        pair.get("let_bragg_peak"),
        pair.get("range_um"),
        pair.get("fluence_at_meas"),
        pred.get("donor_pair_keys"),
        pred.get("donor_count"),
        pred.get("donor_distance"),
        pred.get("support_status"),
        pred.get("unsupported_reason"),
    )


def _gate_metrics(values, total, unsupported, target_type):
    gate = VALIDATION_GATES[target_type]
    med = percentile(values, 0.5)
    p90 = percentile(values, 0.9)
    supported = len(values)
    gate_pass = (
        supported >= gate["min_supported_validation_pairs"]
        and med is not None
        and p90 is not None
        and med <= gate["median_abs_residual_max"]
        and p90 <= gate["p90_abs_residual_max"]
    )
    return {
        "label": TARGET_LABELS[target_type],
        "validation_pairs": total,
        "supported_validation_pairs": supported,
        "unsupported_validation_pairs": unsupported,
        "median_abs_residual": med,
        "p90_abs_residual": p90,
        "gate_pass": gate_pass,
        "gate": gate,
    }


def summarize_validation(residual_rows):
    by_ref_stress_target = defaultdict(list)
    by_ref_target = defaultdict(list)
    unsupported_ref_stress_target = Counter()
    unsupported_ref_target = Counter()
    totals_ref_stress_target = Counter()
    totals_ref_target = Counter()

    for row in residual_rows:
        validation_mode = row[1]
        reference_tier = row[5]
        stress_type = row[6]
        target_type = row[8]
        stress_key = (validation_mode, reference_tier, stress_type, target_type)
        target_key = (validation_mode, reference_tier, target_type)
        totals_ref_stress_target[stress_key] += 1
        totals_ref_target[target_key] += 1
        if row[30] == "ok" and row[14] is not None:
            by_ref_stress_target[stress_key].append(row[14])
            by_ref_target[target_key].append(row[14])
        else:
            unsupported_ref_stress_target[stress_key] += 1
            unsupported_ref_target[target_key] += 1

    metrics = {
        "validation_gates": VALIDATION_GATES,
        "curve_reconstruction_enabled": False,
        "out_of_scope_curve_families": list(OUT_OF_SCOPE_CURVES),
        "validation_modes": {},
        "validation_pairs_by_mode_reference_stress_target": {},
    }

    all_pass = True
    any_supported = False
    for validation_mode in sorted({key[0] for key in totals_ref_stress_target}):
        mode_metrics = {
            "label": VALIDATION_MODE_LABELS.get(validation_mode, validation_mode),
            "reference_targets": {},
            "reference_stress_targets": {},
        }
        for reference_tier in REFERENCE_TIERS:
            for target_type in TARGET_LABELS:
                target_key = (validation_mode, reference_tier, target_type)
                if totals_ref_target.get(target_key, 0) == 0:
                    continue
                metric_key = f"{reference_tier}|{target_type}"
                target_metrics = _gate_metrics(
                    by_ref_target.get(target_key, []),
                    totals_ref_target[target_key],
                    unsupported_ref_target.get(target_key, 0),
                    target_type,
                )
                target_metrics["reference_tier"] = reference_tier
                mode_metrics["reference_targets"][metric_key] = target_metrics

        for stress_key in sorted(
            k for k in totals_ref_stress_target if k[0] == validation_mode
        ):
            _, reference_tier, stress_type, target_type = stress_key
            metric_key = f"{reference_tier}|{stress_type}|{target_type}"
            gate_metrics = _gate_metrics(
                by_ref_stress_target.get(stress_key, []),
                totals_ref_stress_target[stress_key],
                unsupported_ref_stress_target.get(stress_key, 0),
                target_type,
            )
            gate_metrics["reference_tier"] = reference_tier
            gate_metrics["stress_type"] = stress_type
            gate_metrics["target_type"] = target_type
            mode_metrics["reference_stress_targets"][metric_key] = gate_metrics
            metrics["validation_pairs_by_mode_reference_stress_target"][
                f"{validation_mode}|{metric_key}"
            ] = totals_ref_stress_target[stress_key]
            any_supported = any_supported or gate_metrics["supported_validation_pairs"] > 0
            all_pass = all_pass and gate_metrics["gate_pass"]

        # Backward-compatible aliases are strict-only, so library gates cannot
        # alter strict-pair reporting in existing consumers.
        mode_metrics["targets"] = {
            key.split("|", 1)[1]: value
            for key, value in mode_metrics["reference_targets"].items()
            if key.startswith("strict_pre_irrad|")
        }
        mode_metrics["stress_targets"] = {
            key.split("|", 1)[1]: value
            for key, value in mode_metrics["reference_stress_targets"].items()
            if key.startswith("strict_pre_irrad|")
        }
        metrics["validation_modes"][validation_mode] = mode_metrics

    first_mode = next(iter(metrics["validation_modes"]), None)
    metrics["targets"] = (
        metrics["validation_modes"][first_mode]["targets"] if first_mode else {}
    )
    metrics["stress_targets"] = (
        metrics["validation_modes"][first_mode]["stress_targets"] if first_mode else {}
    )
    metrics["reference_targets"] = (
        metrics["validation_modes"][first_mode]["reference_targets"] if first_mode else {}
    )
    metrics["reference_stress_targets"] = (
        metrics["validation_modes"][first_mode]["reference_stress_targets"]
        if first_mode else {}
    )

    if not any_supported:
        status = "unsupported"
    elif all_pass:
        status = "usable"
    else:
        status = "weak_validation"
    return metrics, status


def validate(conn, model_run_id=None, validation_mode="within_condition", reference_tier="both"):
    apply_schema(conn)
    run_id = model_run_id or latest_model_run_id(conn)
    if run_id is None:
        print("No model run found; train first with --train.", file=sys.stderr)
        return None
    config = load_run_config(conn, run_id)
    if not config:
        pairs = load_pairs(conn, reference_tier=reference_tier)
        config = feature_config(fit_feature_scales(pairs), pairs)
    pairs = load_pairs(conn, reference_tier=reference_tier)

    modes = list(VALIDATION_MODES) if validation_mode == "both" else [validation_mode]
    tiers = list(reference_tiers_for_option(reference_tier))

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM iv_physical_validation_residuals
            WHERE model_run_id = %s
              AND validation_mode = ANY(%s)
              AND reference_tier = ANY(%s)
            """,
            (run_id, modes, tiers),
        )
    conn.commit()

    residual_rows = []
    for mode in modes:
        for pair in pairs:
            pred = predict_from_donors(pair, pairs, config, validation_mode=mode)
            residual_rows.append(validation_tuple(run_id, mode, pair, pred))

    columns = (
        "model_run_id",
        "validation_mode",
        "pair_id",
        "pair_key",
        "split_group",
        "reference_tier",
        "stress_type",
        "curve_family",
        "target_type",
        "observed_value",
        "predicted_value",
        "predicted_p10",
        "predicted_p90",
        "residual",
        "abs_residual",
        "device_type",
        "manufacturer",
        "physical_device_key",
        "sc_voltage_v",
        "sc_duration_us",
        "irrad_run_id",
        "ion_species",
        "beam_energy_mev",
        "let_surface",
        "let_bragg_peak",
        "range_um",
        "fluence_at_meas",
        "donor_pair_keys",
        "donor_count",
        "donor_distance",
        "support_status",
        "unsupported_reason",
    )
    if residual_rows:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO iv_physical_validation_residuals ({', '.join(columns)})
                VALUES %s
                """,
                residual_rows,
                page_size=1000,
            )
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT model_run_id, validation_mode, pair_id, pair_key, split_group,
                   reference_tier,
                   stress_type, curve_family, target_type, observed_value,
                   predicted_value, predicted_p10, predicted_p90, residual,
                   abs_residual, device_type, manufacturer, physical_device_key,
                   sc_voltage_v, sc_duration_us, irrad_run_id, ion_species,
                   beam_energy_mev, let_surface, let_bragg_peak, range_um,
                   fluence_at_meas, donor_pair_keys, donor_count, donor_distance,
                   support_status, unsupported_reason
            FROM iv_physical_validation_residuals
            WHERE model_run_id = %s
            ORDER BY validation_mode, pair_id
            """,
            (run_id,),
        )
        all_residual_rows = cur.fetchall()

    metrics, status = summarize_validation(all_residual_rows)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM iv_physical_curve_points WHERE model_run_id = %s)",
            (run_id,),
        )
        metrics["curve_reconstruction_enabled"] = bool(cur.fetchone()[0])
    supported = sum(
        target["supported_validation_pairs"]
        for mode in metrics["validation_modes"].values()
        for target in mode["reference_stress_targets"].values()
    )
    unsupported = sum(
        target["unsupported_validation_pairs"]
        for mode in metrics["validation_modes"].values()
        for target in mode["reference_stress_targets"].values()
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE iv_physical_model_runs
            SET validation_pairs = %s,
                supported_validation_pairs = %s,
                unsupported_validation_pairs = %s,
                metrics = %s,
                model_status = %s
            WHERE id = %s
            """,
            (
                len(all_residual_rows),
                supported,
                unsupported,
                Json(metrics),
                status,
                run_id,
            ),
        )
    conn.commit()

    print_validation_summary(run_id, metrics, status)
    print_reserved_prediction_counts(conn)
    return run_id


def print_validation_summary(run_id, metrics, status):
    print("\nValidation summary:")
    print(f"  model_run_id: {run_id}")
    print(f"  model_status: {status}")
    for mode, mode_metrics in metrics["validation_modes"].items():
        print(f"  {mode} ({mode_metrics['label']}):")
        for stress_target, target_metrics in mode_metrics["reference_stress_targets"].items():
            med = target_metrics["median_abs_residual"]
            p90 = target_metrics["p90_abs_residual"]
            med_s = f"{med:.6g}" if med is not None else "NULL"
            p90_s = f"{p90:.6g}" if p90 is not None else "NULL"
            print(f"    {stress_target}:")
            print(f"      validation_pairs: {target_metrics['validation_pairs']}")
            print(f"      supported: {target_metrics['supported_validation_pairs']}")
            print(f"      unsupported: {target_metrics['unsupported_validation_pairs']}")
            print(f"      median_abs_residual: {med_s}")
            print(f"      p90_abs_residual: {p90_s}")
            print(f"      gate_pass: {target_metrics['gate_pass']}")


def prediction_parameter_sane(target_type, value):
    value = safe_float(value)
    if value is None:
        return False
    if target_type == "delta_vth_v":
        return abs(value) <= 50.0
    if target_type == "log_rdson_ratio":
        return abs(value) <= 10.0
    return False


def load_validation_device_gates(conn, model_run_id):
    sql = """
        SELECT validation_mode,
               reference_tier,
               stress_type,
               target_type,
               device_type,
               COUNT(*) AS total_pairs,
               COUNT(*) FILTER (
                   WHERE support_status = 'ok'
                     AND abs_residual IS NOT NULL
               ) AS supported_pairs,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY abs_residual)
                   FILTER (WHERE support_status = 'ok' AND abs_residual IS NOT NULL)
                   AS median_abs_residual,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY abs_residual)
                   FILTER (WHERE support_status = 'ok' AND abs_residual IS NOT NULL)
                   AS p90_abs_residual
        FROM iv_physical_validation_residuals
        WHERE model_run_id = %s
        GROUP BY validation_mode, reference_tier, stress_type, target_type, device_type
    """
    gates = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (model_run_id,))
        rows = list(cur.fetchall())
    for row in rows:
        target_type = row["target_type"]
        gate = VALIDATION_GATES[target_type]
        total = int(row["total_pairs"] or 0)
        supported = int(row["supported_pairs"] or 0)
        med = safe_float(row.get("median_abs_residual"))
        p90 = safe_float(row.get("p90_abs_residual"))
        supported_fraction = supported / total if total else 0.0
        gate_pass = (
            supported >= gate["min_supported_validation_pairs"]
            and med is not None
            and p90 is not None
            and med <= gate["median_abs_residual_max"]
            and p90 <= gate["p90_abs_residual_max"]
        )
        key = (
            row["validation_mode"],
            row["reference_tier"],
            row["stress_type"],
            target_type,
            row["device_type"],
        )
        gates[key] = {
            "validation_gate_pass": gate_pass,
            "validation_supported_fraction": supported_fraction,
            "validation_supported_pairs": supported,
            "validation_total_pairs": total,
            "median_abs_residual": med,
            "p90_abs_residual": p90,
        }
    return gates


def classify_prediction_confidence(pair, pred, device_gates):
    reasons = []
    target_type = pair["target_type"]
    predicted = pred.get("predicted_value")
    if pred.get("support_status") != "ok" or not prediction_parameter_sane(target_type, predicted):
        reason = pred.get("unsupported_reason") or "no_numeric_donor_prediction"
        if pred.get("support_status") == "ok":
            reason = "nonsensical_predicted_parameter"
        return {
            "confidence_level": "unsupported",
            "confidence_score": 0.0,
            "confidence_reasons": [reason],
            "validation_gate_pass": False,
            "validation_supported_fraction": 0.0,
            "validation_supported_pairs": 0,
            "validation_total_pairs": 0,
        }

    gate_key = (
        "leave_condition",
        pair["reference_tier"],
        pair["stress_type"],
        target_type,
        pair.get("device_type"),
    )
    gate_info = device_gates.get(gate_key) or {}
    gate_pass = bool(gate_info.get("validation_gate_pass"))
    supported_fraction = safe_float(gate_info.get("validation_supported_fraction")) or 0.0
    supported_pairs = int(gate_info.get("validation_supported_pairs") or 0)
    total_pairs = int(gate_info.get("validation_total_pairs") or 0)

    if not gate_info:
        reasons.append("leave_condition_device_gate_missing")
    elif not gate_pass:
        reasons.append("leave_condition_device_gate_failed")
    if supported_fraction < STRONG_SUPPORTED_FRACTION_MIN:
        reasons.append("low_leave_condition_supported_fraction")

    baseline_ok = True
    spread = safe_float(pair.get("baseline_reference_spread"))
    if pair.get("reference_tier") == "library_pristine":
        max_spread = VALIDATION_GATES[target_type]["median_abs_residual_max"]
        if spread is None:
            baseline_ok = False
            reasons.append("missing_library_baseline_spread")
        elif spread > max_spread:
            baseline_ok = False
            reasons.append("library_baseline_spread_above_gate")

    strong = (
        gate_pass
        and supported_fraction >= STRONG_SUPPORTED_FRACTION_MIN
        and baseline_ok
    )
    if strong:
        level = "strong"
        score = min(0.99, 0.90 + min(supported_fraction, 1.0) * 0.09)
        if not reasons:
            reasons.append("leave_condition_device_gate_passed")
    else:
        level = "weak"
        score = 0.55
        if pred.get("donor_count"):
            score += min(float(pred["donor_count"]) / max(NEAREST_K, 1), 1.0) * 0.10
        if gate_pass:
            score += 0.10
        if baseline_ok:
            score += 0.05
        score = min(score, 0.79)
        if not reasons:
            reasons.append("numeric_prediction_without_strong_confidence")

    return {
        "confidence_level": level,
        "confidence_score": score,
        "confidence_reasons": sorted(set(reasons)),
        "validation_gate_pass": gate_pass,
        "validation_supported_fraction": supported_fraction,
        "validation_supported_pairs": supported_pairs,
        "validation_total_pairs": total_pairs,
    }


def prediction_tuple(run_id, donor_mode, pair, pred, confidence):
    return (
        run_id,
        pair["id"],
        pair["pair_key"],
        pair.get("pre_feature_id"),
        pair.get("post_feature_id"),
        pair.get("pre_metadata_id"),
        pair.get("post_metadata_id"),
        pair["target_type"],
        pair["curve_family"],
        pred.get("predicted_value"),
        pred.get("predicted_p10"),
        pred.get("predicted_p90"),
        pair["stress_type"],
        pair["reference_tier"],
        pred.get("donor_pair_keys"),
        pred.get("donor_count"),
        pred.get("donor_distance"),
        pred.get("support_status"),
        pred.get("unsupported_reason"),
        pair.get("sc_voltage_v"),
        pair.get("sc_duration_us"),
        pair.get("sc_condition_label"),
        pair.get("irrad_run_id"),
        pair.get("ion_species"),
        pair.get("beam_energy_mev"),
        pair.get("let_surface"),
        pair.get("let_bragg_peak"),
        pair.get("range_um"),
        pair.get("beam_type"),
        pair.get("fluence_at_meas"),
        donor_mode,
        confidence.get("validation_gate_pass"),
        confidence.get("validation_supported_fraction"),
        confidence.get("validation_supported_pairs"),
        confidence.get("validation_total_pairs"),
        pair.get("baseline_reference_count"),
        pair.get("baseline_reference_spread"),
        pair.get("baseline_reference_method"),
        confidence["confidence_level"],
        confidence["confidence_score"],
        confidence["confidence_reasons"],
        [
            f"reference_tier_{pair['reference_tier']}",
            f"confidence_{confidence['confidence_level']}",
            "exploratory_curve_prediction",
        ],
    )


def delete_parameter_predictions(conn, model_run_id, prediction_stress, tiers):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM iv_physical_parameter_predictions
            WHERE model_run_id = %s
              AND stress_type = %s
              AND reference_tier = ANY(%s)
            """,
            (model_run_id, prediction_stress, list(tiers)),
        )
    conn.commit()


def predict_parameters(
    conn,
    model_run_id=None,
    prediction_stress="irradiation",
    reference_tier="both",
    donor_mode=DEFAULT_PREDICTION_DONOR_MODE,
):
    apply_schema(conn)
    if prediction_stress not in PREDICTION_STRESS_TYPES:
        raise ValueError(f"unsupported prediction stress: {prediction_stress}")
    run_id = model_run_id or latest_model_run_id(conn)
    if run_id is None:
        print("No model run found; train first with --train.", file=sys.stderr)
        return None

    config = load_run_config(conn, run_id)
    all_pairs = load_pairs(conn, reference_tier=reference_tier)
    if not config:
        config = feature_config(fit_feature_scales(all_pairs), all_pairs)
    pairs = [pair for pair in all_pairs if pair["stress_type"] == prediction_stress]
    tiers = reference_tiers_for_option(reference_tier)
    device_gates = load_validation_device_gates(conn, run_id)
    delete_parameter_predictions(conn, run_id, prediction_stress, tiers)

    rows = []
    confidence_counts = Counter()
    for pair in pairs:
        pred = predict_from_donors(pair, all_pairs, config, validation_mode=donor_mode)
        confidence = classify_prediction_confidence(pair, pred, device_gates)
        confidence_counts[confidence["confidence_level"]] += 1
        rows.append(prediction_tuple(run_id, donor_mode, pair, pred, confidence))

    columns = (
        "model_run_id",
        "pair_id",
        "pair_key",
        "source_feature_id",
        "post_feature_id",
        "source_metadata_id",
        "post_metadata_id",
        "target_type",
        "curve_family",
        "predicted_value",
        "predicted_p10",
        "predicted_p90",
        "stress_type",
        "reference_tier",
        "donor_pair_keys",
        "donor_count",
        "donor_distance",
        "support_status",
        "unsupported_reason",
        "sc_voltage_v",
        "sc_duration_us",
        "sc_condition_label",
        "irrad_run_id",
        "ion_species",
        "beam_energy_mev",
        "let_surface",
        "let_bragg_peak",
        "range_um",
        "beam_type",
        "fluence_at_meas",
        "validation_mode_used",
        "validation_gate_pass",
        "validation_supported_fraction",
        "validation_supported_pairs",
        "validation_total_pairs",
        "baseline_reference_count",
        "baseline_reference_spread",
        "baseline_reference_method",
        "confidence_level",
        "confidence_score",
        "confidence_reasons",
        "physics_flags",
    )
    if rows:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO iv_physical_parameter_predictions ({', '.join(columns)})
                VALUES %s
                """,
                rows,
                page_size=1000,
            )
        conn.commit()

    print("\nParameter prediction complete:")
    print(f"  model_run_id: {run_id}")
    print(f"  prediction_stress: {prediction_stress}")
    print(f"  parameter_predictions: {len(rows)}")
    for level in CONFIDENCE_LEVELS:
        print(f"  {level}: {confidence_counts.get(level, 0)}")
    return run_id


def load_parameter_predictions_for_curves(conn, model_run_id, prediction_stress, tiers):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT pp.*,
                   f.metadata_id AS source_feature_metadata_id,
                   f.drain_bias_value AS source_drain_bias_value,
                   f.bias_value AS source_bias_value
            FROM iv_physical_parameter_predictions pp
            LEFT JOIN iv_physical_curve_features f
              ON f.id = pp.source_feature_id
            WHERE pp.model_run_id = %s
              AND pp.stress_type = %s
              AND pp.reference_tier = ANY(%s)
            ORDER BY pp.id
            """,
            (model_run_id, prediction_stress, list(tiers)),
        )
        return list(cur.fetchall())


def load_source_measurements(conn, metadata_id):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, point_index, v_gate, v_drain, i_drain
            FROM baselines_measurements
            WHERE metadata_id = %s
            ORDER BY point_index, id
            """,
            (metadata_id,),
        )
        return list(cur.fetchall())


def reconstruct_curve_rows(param, source_points):
    predicted = safe_float(param.get("predicted_value"))
    if not prediction_parameter_sane(param["target_type"], predicted):
        return [], "nonsensical_predicted_parameter"
    rows = []
    for point in source_points:
        current = safe_float(point.get("i_drain"))
        if current is None:
            continue
        if param["target_type"] == "delta_vth_v":
            source_x = safe_float(point.get("v_gate"))
            if source_x is None:
                continue
            predicted_x = source_x + predicted
            x_axis_name = "v_gate"
            bias_axis_name = "v_drain"
            bias_value = safe_float(param.get("source_drain_bias_value"))
            if bias_value is None:
                bias_value = safe_float(point.get("v_drain"))
            predicted_current = current
        elif param["target_type"] == "log_rdson_ratio":
            source_x = safe_float(point.get("v_drain"))
            if source_x is None:
                continue
            predicted_x = source_x
            x_axis_name = "v_drain"
            bias_axis_name = "v_gate"
            bias_value = safe_float(point.get("v_gate"))
            if bias_value is None:
                bias_value = safe_float(param.get("source_bias_value"))
            predicted_current = current * math.exp(-predicted)
        else:
            return [], "unknown_target_type"
        if not finite(predicted_x) or not finite(predicted_current):
            continue
        rows.append((
            param["id"],
            param["model_run_id"],
            param.get("source_metadata_id"),
            param.get("source_feature_id"),
            param.get("pair_id"),
            param["target_type"],
            param["curve_family"],
            param["reference_tier"],
            x_axis_name,
            predicted_x,
            source_x,
            predicted_x,
            bias_axis_name,
            bias_value,
            point.get("point_index"),
            current,
            predicted_current,
            predicted,
            param.get("predicted_p10"),
            param.get("predicted_p90"),
            param.get("donor_pair_keys"),
            param.get("donor_count"),
            param.get("donor_distance"),
            param.get("support_status"),
            param.get("unsupported_reason"),
            param.get("confidence_level"),
            param.get("confidence_score"),
            param.get("confidence_reasons"),
            param.get("physics_flags"),
            "ok",
        ))
    if not rows:
        return [], "no_valid_source_curve_points"
    return rows, None


def mark_parameter_curve_unsupported(conn, param_id, reason):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE iv_physical_parameter_predictions
            SET confidence_level = 'unsupported',
                confidence_score = 0.0,
                unsupported_reason = COALESCE(unsupported_reason, %s),
                confidence_reasons = ARRAY(
                    SELECT DISTINCT reason
                    FROM unnest(confidence_reasons || ARRAY[%s]::text[]) AS t(reason)
                ),
                physics_flags = ARRAY(
                    SELECT DISTINCT flag
                    FROM unnest(COALESCE(physics_flags, ARRAY[]::text[]) || ARRAY[%s]::text[]) AS t(flag)
                )
            WHERE id = %s
            """,
            (reason, reason, reason, param_id),
        )


def delete_curve_points(conn, model_run_id, prediction_stress, tiers):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM iv_physical_curve_points cp
            USING iv_physical_parameter_predictions pp
            WHERE cp.parameter_prediction_id = pp.id
              AND pp.model_run_id = %s
              AND pp.stress_type = %s
              AND pp.reference_tier = ANY(%s)
            """,
            (model_run_id, prediction_stress, list(tiers)),
        )
    conn.commit()


def set_curve_reconstruction_metric(conn, model_run_id, enabled):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT metrics FROM iv_physical_model_runs WHERE id = %s", (model_run_id,))
        row = cur.fetchone()
    metrics = (row or {}).get("metrics") or {}
    metrics["curve_reconstruction_enabled"] = bool(enabled)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE iv_physical_model_runs SET metrics = %s WHERE id = %s",
            (Json(metrics), model_run_id),
        )
    conn.commit()


def generate_curves(
    conn,
    model_run_id=None,
    prediction_stress="irradiation",
    reference_tier="both",
):
    apply_schema(conn)
    if prediction_stress not in PREDICTION_STRESS_TYPES:
        raise ValueError(f"unsupported prediction stress: {prediction_stress}")
    run_id = model_run_id or latest_model_run_id(conn)
    if run_id is None:
        print("No model run found; train first with --train.", file=sys.stderr)
        return None
    tiers = reference_tiers_for_option(reference_tier)
    delete_curve_points(conn, run_id, prediction_stress, tiers)
    params = load_parameter_predictions_for_curves(conn, run_id, prediction_stress, tiers)
    if not params:
        print("No parameter predictions found; run --predict-parameters first.", file=sys.stderr)
        return run_id

    point_columns = (
        "parameter_prediction_id",
        "model_run_id",
        "source_metadata_id",
        "source_feature_id",
        "pair_id",
        "target_type",
        "curve_family",
        "reference_tier",
        "x_axis_name",
        "x_value",
        "source_x_value",
        "predicted_x_value",
        "bias_axis_name",
        "bias_value",
        "point_index",
        "pristine_i_drain",
        "predicted_post_i_drain",
        "predicted_parameter_value",
        "predicted_parameter_p10",
        "predicted_parameter_p90",
        "donor_pair_keys",
        "donor_count",
        "donor_distance",
        "support_status",
        "unsupported_reason",
        "confidence_level",
        "confidence_score",
        "confidence_reasons",
        "physics_flags",
        "prediction_status",
    )
    inserted = 0
    skipped = Counter()
    batch = []
    with conn.cursor() as cur:
        for param in params:
            if param.get("confidence_level") == "unsupported" or param.get("support_status") != "ok":
                skipped["unsupported_parameter"] += 1
                continue
            source_metadata_id = param.get("source_metadata_id") or param.get("source_feature_metadata_id")
            if source_metadata_id is None:
                mark_parameter_curve_unsupported(conn, param["id"], "missing_source_metadata_id")
                skipped["missing_source_metadata_id"] += 1
                continue
            source_points = load_source_measurements(conn, source_metadata_id)
            if not source_points:
                mark_parameter_curve_unsupported(conn, param["id"], "missing_source_curve_points")
                skipped["missing_source_curve_points"] += 1
                continue
            rows, reason = reconstruct_curve_rows(param, source_points)
            if reason:
                mark_parameter_curve_unsupported(conn, param["id"], reason)
                skipped[reason] += 1
                continue
            batch.extend(rows)
            if len(batch) >= 5000:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO iv_physical_curve_points ({', '.join(point_columns)})
                    VALUES %s
                    """,
                    batch,
                    page_size=1000,
                )
                inserted += len(batch)
                batch = []
        if batch:
            execute_values(
                cur,
                f"""
                INSERT INTO iv_physical_curve_points ({', '.join(point_columns)})
                VALUES %s
                """,
                batch,
                page_size=1000,
            )
            inserted += len(batch)
    conn.commit()
    set_curve_reconstruction_metric(conn, run_id, inserted > 0)

    print("\nCurve generation complete:")
    print(f"  model_run_id: {run_id}")
    print(f"  prediction_stress: {prediction_stress}")
    print(f"  parameter_predictions_seen: {len(params)}")
    print(f"  curve_points_inserted: {inserted}")
    if skipped:
        print("  skipped:")
        for reason, count in skipped.most_common():
            print(f"    {reason}: {count}")
    return run_id


def print_reserved_prediction_counts(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM iv_physical_parameter_predictions")
        param_n = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM iv_physical_curve_points")
        curve_n = cur.fetchone()[0]
    print("\nReserved prediction outputs:")
    print(f"  iv_physical_parameter_predictions: {param_n}")
    print(f"  iv_physical_curve_points: {curve_n}")


def rebuild_sql(conn):
    apply_schema(conn)
    print(f"Applied {SCHEMA_PATH.relative_to(REPO_ROOT)}")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE 'iv_physical_%'
            ORDER BY table_name
            """
        )
        rows = [r[0] for r in cur.fetchall()]
    print("\nV2 tables present:")
    for table in rows:
        print(f"  {table}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--rebuild-sql", action="store_true",
                        help="Apply schema/024_iv_physical_prediction.sql")
    parser.add_argument("--extract-features", action="store_true",
                        help="Snapshot physical curve features from gate_params")
    parser.add_argument("--refresh-damage", action="store_true",
                        help="Refresh gate_params via extract_damage_metrics.py before snapshotting")
    parser.add_argument("--refresh-damage-rebuild", action="store_true",
                        help="Recompute all gate_params during --refresh-damage")
    parser.add_argument("--device-type",
                        help="Restrict --refresh-damage to one device_type")
    parser.add_argument("--build-pairs", action="store_true",
                        help="Build strict pre/post physical response pairs")
    parser.add_argument("--include-library-pristine", action="store_true",
                        help="Allow irradiation post rows without strict pairs to use library pristine references")
    parser.add_argument("--train", action="store_true",
                        help="Create a support-gated donor model run")
    parser.add_argument("--validate", action="store_true",
                        help="Run support-gated validation for a model run")
    parser.add_argument("--predict-parameters", action="store_true",
                        help="Persist confidence-labeled V2 parameter predictions for existing response pairs")
    parser.add_argument("--generate-curves", action="store_true",
                        help="Reconstruct V2 IdVg/IdVd curve points from persisted parameter predictions")
    parser.add_argument("--predict-curves", action="store_true",
                        help="Shorthand for --predict-parameters --generate-curves")
    parser.add_argument("--reference-tier",
                        choices=["strict", "library", "both"],
                        default="both",
                        help="Reference tier(s) to use for training, validation, and prediction")
    parser.add_argument("--audit-library-pristine", action="store_true",
                        help="Compare hypothetical library-pristine deltas against existing strict irradiation pairs")
    parser.add_argument("--validation-mode",
                        choices=["within_condition", "leave_condition", "both"],
                        default="within_condition",
                        help="Validation donor exclusion mode")
    parser.add_argument("--prediction-stress",
                        choices=PREDICTION_STRESS_TYPES,
                        default="irradiation",
                        help="Stress type to generate predictions for")
    parser.add_argument("--prediction-donor-mode",
                        choices=VALIDATION_MODES,
                        default=DEFAULT_PREDICTION_DONOR_MODE,
                        help="Donor exclusion mode used for exploratory parameter prediction")
    parser.add_argument("--model-run-id", type=int,
                        help="Model run to validate; defaults to latest")
    args = parser.parse_args()
    predict_parameters_requested = args.predict_parameters or args.predict_curves
    generate_curves_requested = args.generate_curves or args.predict_curves

    if not any(
        [
            args.rebuild_sql,
            args.refresh_damage,
            args.extract_features,
            args.build_pairs,
            args.train,
            args.validate,
            args.audit_library_pristine,
            predict_parameters_requested,
            generate_curves_requested,
        ]
    ):
        parser.print_help()
        return

    conn = get_connection()
    trained_run_id = None
    try:
        print_old_counts(conn, "before")
        if args.rebuild_sql:
            rebuild_sql(conn)
        if args.refresh_damage:
            refresh_damage_metrics(
                conn,
                rebuild=args.refresh_damage_rebuild,
                device_type=args.device_type,
            )
        if args.extract_features:
            extract_features(conn)
        if args.build_pairs:
            build_pairs(conn, include_library_pristine=args.include_library_pristine)
        if args.audit_library_pristine:
            audit_library_pristine(conn)
        if args.train:
            trained_run_id = train(conn, reference_tier=args.reference_tier)
        if args.validate:
            trained_run_id = validate(
                conn,
                args.model_run_id or trained_run_id,
                validation_mode=args.validation_mode,
                reference_tier=args.reference_tier,
            )
        if predict_parameters_requested:
            trained_run_id = predict_parameters(
                conn,
                args.model_run_id or trained_run_id,
                prediction_stress=args.prediction_stress,
                reference_tier=args.reference_tier,
                donor_mode=args.prediction_donor_mode,
            )
        if generate_curves_requested:
            generate_curves(
                conn,
                args.model_run_id or trained_run_id,
                prediction_stress=args.prediction_stress,
                reference_tier=args.reference_tier,
            )
            print_reserved_prediction_counts(conn)
        print_old_counts(conn, "after")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
