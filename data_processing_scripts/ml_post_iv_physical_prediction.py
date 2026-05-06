#!/usr/bin/env python3
"""
V2 Post-IV Physical Degradation Prediction
==========================================
Builds a conservative, validation-gated physical response workflow for
post-stress IV behavior. V2 predicts physical degradation parameters first:

  * IdVg -> delta_vth_v
  * IdVd -> log_rdson_ratio

It does not mutate the legacy iv_prediction_* tables and it does not
reconstruct curves. Blocking and 3rd_Quadrant are intentionally out of V1
scope until separate physical envelope/diode models are validated.

Typical usage:
    python3 ml_post_iv_physical_prediction.py --rebuild-sql
    python3 ml_post_iv_physical_prediction.py --extract-features
    python3 ml_post_iv_physical_prediction.py --build-pairs
    python3 ml_post_iv_physical_prediction.py --train
    python3 ml_post_iv_physical_prediction.py --validate
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


REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "schema" / "024_iv_physical_prediction.sql"
OUT_DIR = REPO_ROOT / "out" / "iv_physical_prediction"
MODEL_DIR = OUT_DIR / "models"

MODEL_VERSION = "v2.0-physical-donor"
ALGORITHM = "support_gated_weighted_median_donor_v1"

CURVE_TO_TARGET = {
    "IdVg": "delta_vth_v",
    "IdVd": "log_rdson_ratio",
}
TARGET_TO_CURVE = {v: k for k, v in CURVE_TO_TARGET.items()}
TARGET_LABELS = {
    "delta_vth_v": "IdVg / delta_vth_v",
    "log_rdson_ratio": "IdVd / log_rdson_ratio",
}
SUPPORTED_CATEGORIES = set(CURVE_TO_TARGET)
OUT_OF_SCOPE_CURVES = ("Blocking", "3rd_Quadrant")

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
        if "ohm" in lower and "mohm" not in lower and "mω" not in lower:
            number *= 1000.0
    return number if math.isfinite(number) else None


def curve_family_for(row):
    category = row.get("measurement_category")
    if category in SUPPORTED_CATEGORIES:
        return category
    return None


def metric_from_gate_params(gate_params, key):
    if not gate_params:
        return None
    value = gate_params.get(key)
    return safe_float(value)


def is_pristine_reference(row):
    source = row.get("data_source") or "baselines"
    return (
        source == "baselines"
        or row.get("test_condition") == "pristine"
        or row.get("irrad_role") == "pre_irrad"
    )


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
        WHERE measurement_category IN ('IdVg', 'IdVd')
        GROUP BY measurement_category
        ORDER BY measurement_category
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    print("\ngate_params coverage before feature extraction:")
    for category, total, with_vth, with_rdson in rows:
        usable = with_vth if category == "IdVg" else with_rdson
        frac = usable / total if total else 0.0
        print(f"  {category}: {usable}/{total} target metrics ({frac:.1%})")
        if total and frac < 0.5:
            print("    WARNING: low metric coverage. Consider running:")
            print("      python3 data_processing_scripts/extract_damage_metrics.py")


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
        WHERE md.measurement_category IN ('IdVg', 'IdVd')
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


def choose_pre_feature(candidates):
    return sorted(candidates, key=lambda f: (f["metadata_id"], f["id"]))[0]


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


def pair_tuple(pre, post, stress_type, response_value):
    target = post["target_type"]
    pair_key = f"{stress_type}:{target}:pre{pre['metadata_id']}:post{post['metadata_id']}"
    split_group = f"{stress_type}:{target}:{post['physical_device_key']}"
    flags = ["strict_device_type_match", "strict_physical_device_key_match"]
    if stress_type == "sc":
        flags.append("strict_sc_sample_group_match")
        pairing_method = "same_device_type_sample_group_pristine_to_post_sc"
    else:
        flags.append("strict_irrad_device_key_match")
        pairing_method = "same_device_type_device_key_pre_to_post_irrad"

    delta_vth_v = response_value if target == "delta_vth_v" else None
    log_rdson_ratio = response_value if target == "log_rdson_ratio" else None

    return (
        pair_key,
        stress_type,
        pairing_method,
        post["curve_family"],
        target,
        pre["id"],
        post["id"],
        pre["metadata_id"],
        post["metadata_id"],
        post["physical_device_key"],
        split_group,
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
        "usable",
        flags,
    )


def build_pairs(conn):
    apply_schema(conn)
    print("\nRebuilding strict V2 response pairs ...")
    truncate_tables(conn, V2_DOWNSTREAM_TABLES)
    features = load_features(conn)
    usable = [f for f in features if f["quality_status"] == "usable"]
    reasons = Counter()
    pairs = []

    by_key = defaultdict(list)
    for feature in usable:
        key = (
            feature["target_type"],
            feature["curve_family"],
            feature["device_type"],
            feature["physical_device_key"],
        )
        by_key[key].append(feature)

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
            pre = choose_pre_feature(candidates)
            response, reason = response_from_features(pre, post)
            if reason:
                reasons[reason] += 1
                continue
            pairs.append(pair_tuple(pre, post, "sc", response))

        if post["irrad_role"] == "post_irrad":
            if post.get("irrad_run_id") is None:
                reasons["irrad_post_missing_irrad_run_id"] += 1
                continue
            candidates = ir_pre.get(key, [])
            if not candidates:
                reasons["irrad_no_matching_pre_irrad_feature"] += 1
                continue
            pre = choose_pre_feature(candidates)
            response, reason = response_from_features(pre, post)
            if reason:
                reasons[reason] += 1
                continue
            pairs.append(pair_tuple(pre, post, "irradiation", response))

    columns = (
        "pair_key",
        "stress_type",
        "pairing_method",
        "curve_family",
        "target_type",
        "pre_feature_id",
        "post_feature_id",
        "pre_metadata_id",
        "post_metadata_id",
        "physical_device_key",
        "split_group",
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
            SELECT target_type, stress_type, COUNT(*)
            FROM iv_physical_response_pairs
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        )
        print("\npair count by target / stress:")
        for target, stress, count in cur.fetchall():
            print(f"  {target} | {stress}: {count}")

        cur.execute(
            """
            SELECT device_type, target_type, COUNT(*)
            FROM iv_physical_response_pairs
            GROUP BY 1, 2
            ORDER BY COUNT(*) DESC, 1, 2
            LIMIT 60
            """
        )
        print("\npair count by device_type / target:")
        for device_type, target, count in cur.fetchall():
            print(f"  {device_type} | {target}: {count}")

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


def load_pairs(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM iv_physical_response_pairs
            WHERE quality_status = 'usable'
            ORDER BY id
            """
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
        counts[f"{pair['stress_type']}|{pair['target_type']}"] += 1
    return dict(sorted(counts.items()))


def feature_config(scales, pairs):
    return {
        "model_version": MODEL_VERSION,
        "algorithm": ALGORITHM,
        "supported_curve_families": ["IdVg", "IdVd"],
        "out_of_scope_curve_families": list(OUT_OF_SCOPE_CURVES),
        "targets": {
            "IdVg": "delta_vth_v",
            "IdVd": "log_rdson_ratio",
        },
        "min_donors": MIN_DONORS,
        "nearest_k": NEAREST_K,
        "numeric_features": NUMERIC_FEATURES,
        "feature_scales": scales,
        "pair_counts": pair_counts(pairs),
        "categorical_policy": {
            "device_type": "exact_match_required",
            "ion_species": "exact_match_required_for_irradiation_when_present",
        },
        "validation_gates": VALIDATION_GATES,
    }


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def train(conn):
    apply_schema(conn)
    pairs = load_pairs(conn)
    scales = fit_feature_scales(pairs)
    config = feature_config(scales, pairs)
    status = "pending_validation" if pairs else "unsupported"
    notes = (
        "Physical donor model trained for IdVg/IdVd parameter deltas only; "
        "curve reconstruction is disabled until validation gates pass."
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
        "training_pair_count": len(pairs),
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
    print("  curve reconstruction: disabled")
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


def predict_from_donors(target, pairs, config):
    min_donors = int(config.get("min_donors") or MIN_DONORS)
    nearest_k = int(config.get("nearest_k") or NEAREST_K)
    candidates = [
        donor
        for donor in pairs
        if donor["id"] != target["id"]
        and donor["target_type"] == target["target_type"]
        and donor["stress_type"] == target["stress_type"]
        and donor["device_type"] == target["device_type"]
        and donor["split_group"] != target["split_group"]
    ]
    if len(candidates) < min_donors:
        return {
            "support_status": "unsupported",
            "unsupported_reason": "insufficient_same_device_type_donor_pairs",
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
            return {
                "support_status": "unsupported",
                "unsupported_reason": "insufficient_same_ion_species_donor_pairs",
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


def validation_tuple(run_id, pair, pred):
    observed = target_value(pair)
    predicted = pred.get("predicted_value")
    residual = None
    abs_residual = None
    if observed is not None and predicted is not None:
        residual = predicted - observed
        abs_residual = abs(residual)

    return (
        run_id,
        pair["id"],
        pair["pair_key"],
        pair["split_group"],
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


def summarize_validation(residual_rows):
    by_target = defaultdict(list)
    unsupported = Counter()
    totals = Counter()
    stress_counts = Counter()
    for row in residual_rows:
        target = row[6]
        totals[target] += 1
        stress_counts[f"{row[4]}|{target}"] += 1
        if row[28] == "ok" and row[12] is not None:
            by_target[target].append(row[12])
        else:
            unsupported[target] += 1

    metrics = {
        "validation_gates": VALIDATION_GATES,
        "curve_reconstruction_enabled": False,
        "out_of_scope_curve_families": list(OUT_OF_SCOPE_CURVES),
        "targets": {},
        "validation_pairs_by_stress_target": dict(sorted(stress_counts.items())),
    }
    all_pass = True
    any_supported = False
    for target in TARGET_LABELS:
        values = by_target.get(target, [])
        gate = VALIDATION_GATES[target]
        med = percentile(values, 0.5)
        p90 = percentile(values, 0.9)
        supported = len(values)
        total = totals.get(target, 0)
        target_pass = (
            supported >= gate["min_supported_validation_pairs"]
            and med is not None
            and p90 is not None
            and med <= gate["median_abs_residual_max"]
            and p90 <= gate["p90_abs_residual_max"]
        )
        any_supported = any_supported or supported > 0
        all_pass = all_pass and target_pass
        metrics["targets"][target] = {
            "label": TARGET_LABELS[target],
            "validation_pairs": total,
            "supported_validation_pairs": supported,
            "unsupported_validation_pairs": unsupported.get(target, 0),
            "median_abs_residual": med,
            "p90_abs_residual": p90,
            "gate_pass": target_pass,
            "gate": gate,
        }

    if not any_supported:
        status = "unsupported"
    elif all_pass:
        status = "usable"
    else:
        status = "weak_validation"
    return metrics, status


def validate(conn, model_run_id=None):
    apply_schema(conn)
    run_id = model_run_id or latest_model_run_id(conn)
    if run_id is None:
        print("No model run found; train first with --train.", file=sys.stderr)
        return None
    config = load_run_config(conn, run_id)
    if not config:
        pairs = load_pairs(conn)
        config = feature_config(fit_feature_scales(pairs), pairs)
    pairs = load_pairs(conn)

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM iv_physical_validation_residuals WHERE model_run_id = %s",
            (run_id,),
        )
    conn.commit()

    residual_rows = []
    for pair in pairs:
        pred = predict_from_donors(pair, pairs, config)
        residual_rows.append(validation_tuple(run_id, pair, pred))

    columns = (
        "model_run_id",
        "pair_id",
        "pair_key",
        "split_group",
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

    metrics, status = summarize_validation(residual_rows)
    supported = sum(
        target["supported_validation_pairs"]
        for target in metrics["targets"].values()
    )
    unsupported = sum(
        target["unsupported_validation_pairs"]
        for target in metrics["targets"].values()
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
                len(residual_rows),
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
    for target, target_metrics in metrics["targets"].items():
        med = target_metrics["median_abs_residual"]
        p90 = target_metrics["p90_abs_residual"]
        med_s = f"{med:.6g}" if med is not None else "NULL"
        p90_s = f"{p90:.6g}" if p90 is not None else "NULL"
        print(f"  {target}:")
        print(f"    validation_pairs: {target_metrics['validation_pairs']}")
        print(f"    supported: {target_metrics['supported_validation_pairs']}")
        print(f"    unsupported: {target_metrics['unsupported_validation_pairs']}")
        print(f"    median_abs_residual: {med_s}")
        print(f"    p90_abs_residual: {p90_s}")
        print(f"    gate_pass: {target_metrics['gate_pass']}")


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
    parser.add_argument("--build-pairs", action="store_true",
                        help="Build strict pre/post physical response pairs")
    parser.add_argument("--train", action="store_true",
                        help="Create a support-gated donor model run")
    parser.add_argument("--validate", action="store_true",
                        help="Run leave-one-device validation for a model run")
    parser.add_argument("--model-run-id", type=int,
                        help="Model run to validate; defaults to latest")
    args = parser.parse_args()

    if not any(
        [
            args.rebuild_sql,
            args.extract_features,
            args.build_pairs,
            args.train,
            args.validate,
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
        if args.extract_features:
            extract_features(conn)
        if args.build_pairs:
            build_pairs(conn)
        if args.train:
            trained_run_id = train(conn)
        if args.validate:
            validate(conn, args.model_run_id or trained_run_id)
        print_old_counts(conn, "after")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
