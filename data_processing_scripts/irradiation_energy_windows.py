#!/usr/bin/env python3
"""
Infer active-beam and energy-integration windows for irradiation waveforms.

This script preserves raw baselines_measurements rows.  It writes derived
window metadata to irradiation_waveform_windows and optional per-row flags to
irradiation_waveform_point_flags so energy calculations can be audited back to
the original .txt file.
"""

from __future__ import annotations

import argparse
import math
from collections import Counter
from dataclasses import dataclass
from time import perf_counter

try:
    from psycopg2.extras import Json, execute_values
except ImportError:  # pragma: no cover - production environments install it
    Json = None
    execute_values = None

try:
    from common import apply_schema
    from db_config import get_connection
except ImportError:  # pragma: no cover - package import path for tests
    from .common import apply_schema
    from .db_config import get_connection


FINITE_LIMIT = 1e30
DEFAULT_SETTINGS = {
    "fluence_abs_epsilon": 1e-9,
    "fluence_rel_epsilon": 1e-9,
    "min_fluence_span_for_active": 1.0,
    "compliance_fraction": 0.995,
    "heuristic_failure_current_a": 1e-2,
}


@dataclass(frozen=True)
class WaveformPoint:
    point_index: int | None
    time_s: float | None
    vds: float | None
    id_drain: float | None
    igs: float | None
    fluence: float | None


def finite_float(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out) or abs(out) >= FINITE_LIMIT:
        return None
    return out


def _field(row, key, pos):
    if isinstance(row, dict):
        return row.get(key)
    return row[pos]


def normalize_points(points):
    out = []
    for idx, row in enumerate(points):
        point_index = _field(row, "point_index", 0)
        if point_index is None:
            point_index = idx
        out.append(WaveformPoint(
            point_index=int(point_index),
            time_s=finite_float(_field(row, "time_val", 1)),
            vds=finite_float(_field(row, "v_drain", 2)),
            id_drain=finite_float(_field(row, "i_drain", 3)),
            igs=finite_float(_field(row, "i_gate", 4)),
            fluence=finite_float(_field(row, "fluence", 5)),
        ))
    return sorted(out, key=lambda p: (
        p.point_index if p.point_index is not None else 10**12
    ))


def _settings(settings):
    out = dict(DEFAULT_SETTINGS)
    if settings:
        out.update(settings)
    return out


def _fluence_tolerance(points, settings):
    finite_fluence = [abs(p.fluence) for p in points if p.fluence is not None]
    max_abs = max(finite_fluence) if finite_fluence else 0.0
    return max(
        float(settings["fluence_abs_epsilon"]),
        float(settings["fluence_rel_epsilon"]) * max_abs,
    )


def _positive_compliance(compliance_ch1=None, compliance_ch2=None):
    vals = [
        finite_float(compliance_ch1),
        finite_float(compliance_ch2),
    ]
    vals = [abs(v) for v in vals if v is not None and abs(v) > 0.0]
    return min(vals) if vals else None


def _first_current_crossing(points, start_s, end_s, threshold_a, fraction):
    if threshold_a is None or start_s is None or end_s is None:
        return None
    limit = abs(threshold_a) * fraction
    for point in points:
        if point.time_s is None or point.id_drain is None:
            continue
        if point.time_s < start_s or point.time_s > end_s:
            continue
        if abs(point.id_drain) >= limit:
            return point.time_s
    return None


def _first_heuristic_failure(points, start_s, end_s, threshold_a):
    if start_s is None or end_s is None:
        return None
    for point in points:
        if point.time_s is None or point.id_drain is None:
            continue
        if point.time_s < start_s or point.time_s > end_s:
            continue
        if abs(point.id_drain) >= threshold_a:
            return point.time_s
    return None


def _flag_points(metadata_id, points, window):
    active_start = window["active_start_s"]
    active_end = window["active_end_s"]
    energy_start = window["energy_start_s"]
    energy_end = window["energy_end_s"]
    failure_time = window["failure_time_s"]
    flags = []
    for point in points:
        is_active = (
            point.time_s is not None
            and active_start is not None
            and active_end is not None
            and active_start <= point.time_s <= active_end
        )
        is_pre_failure = (
            point.time_s is not None
            and (failure_time is None or point.time_s <= failure_time)
        )
        is_integrable = (
            point.time_s is not None
            and energy_start is not None
            and energy_end is not None
            and energy_start <= point.time_s <= energy_end
            and is_pre_failure
        )
        if point.time_s is None:
            reason = "no_time"
        elif active_start is None or active_end is None:
            reason = "active_window_unknown"
        elif not is_active:
            reason = "outside_active_window"
        elif not is_pre_failure:
            reason = "post_failure_or_compliance"
        elif energy_start is None or energy_end is None:
            reason = "energy_window_unknown"
        else:
            reason = None
        flags.append({
            "metadata_id": metadata_id,
            "point_index": point.point_index,
            "is_active_beam": is_active,
            "is_pre_failure": is_pre_failure,
            "is_energy_integrable": is_integrable,
            "exclusion_reason": reason,
        })
    return flags


def infer_energy_window(
        points, *, metadata_id=None, compliance_ch1=None, compliance_ch2=None,
        settings=None):
    """
    Infer a conservative active-beam and energy window from waveform points.

    Fluence progression is the only high-confidence active-beam basis here.
    Files without fluence, or with significant fluence resets, are marked
    non-comparable instead of being treated as whole-file irradiation.
    """
    cfg = _settings(settings)
    points = normalize_points(points)
    times = [p.time_s for p in points if p.time_s is not None]

    window = {
        "metadata_id": metadata_id,
        "active_start_s": None,
        "active_end_s": None,
        "energy_start_s": None,
        "energy_end_s": None,
        "active_window_basis": "unknown_no_time",
        "active_window_confidence": 0.0,
        "energy_censored_reason": "active_window_unknown",
        "compliance_source": None,
        "compliance_current_a": None,
        "failure_time_s": None,
        "energy_is_comparable": False,
        "settings": cfg,
    }

    if not points or not times:
        return window, _flag_points(metadata_id, points, window)

    fluence_points = [
        p for p in points
        if p.time_s is not None and p.fluence is not None
    ]
    positive_edges = []
    negative_edges = []
    if len(fluence_points) >= 2:
        tol = _fluence_tolerance(fluence_points, cfg)
        for prev, curr in zip(fluence_points, fluence_points[1:]):
            delta = curr.fluence - prev.fluence
            if delta > tol:
                positive_edges.append((prev.time_s, curr.time_s, delta))
            elif delta < -tol:
                negative_edges.append((prev.time_s, curr.time_s, delta))

        fluence_vals = [p.fluence for p in fluence_points]
        fluence_span = max(fluence_vals) - min(fluence_vals)
        if negative_edges:
            window.update({
                "active_window_basis": "fluence_reset_artifact",
                "active_window_confidence": 0.35,
                "energy_censored_reason": "fluence_reset_artifact",
            })
            if positive_edges:
                window["active_start_s"] = min(edge[0] for edge in positive_edges)
                window["active_end_s"] = max(edge[1] for edge in positive_edges)
        elif (positive_edges
              and fluence_span >= cfg["min_fluence_span_for_active"]):
            active_start = min(edge[0] for edge in positive_edges)
            active_end = max(edge[1] for edge in positive_edges)
            window.update({
                "active_start_s": active_start,
                "active_end_s": active_end,
                "energy_start_s": active_start,
                "energy_end_s": active_end,
                "active_window_basis": "fluence_positive_delta",
                "active_window_confidence": 0.95,
                "energy_censored_reason": "none",
                "energy_is_comparable": True,
            })
        else:
            window.update({
                "active_window_basis": "fluence_static_or_missing_progression",
                "active_window_confidence": 0.20,
                "energy_censored_reason": "active_window_unknown",
            })
    else:
        window.update({
            "active_window_basis": "unknown_no_fluence",
            "active_window_confidence": 0.0,
            "energy_censored_reason": "active_window_unknown",
        })

    compliance = _positive_compliance(compliance_ch1, compliance_ch2)
    if window["energy_start_s"] is not None and window["energy_end_s"] is not None:
        if compliance is not None:
            crossing = _first_current_crossing(
                points, window["energy_start_s"], window["energy_end_s"],
                compliance, cfg["compliance_fraction"])
            if crossing is not None:
                window.update({
                    "energy_end_s": crossing,
                    "energy_censored_reason": "current_compliance",
                    "compliance_source": "metadata",
                    "compliance_current_a": compliance,
                    "failure_time_s": crossing,
                    "energy_is_comparable": False,
                })
        else:
            failure_time = _first_heuristic_failure(
                points, window["energy_start_s"], window["energy_end_s"],
                cfg["heuristic_failure_current_a"])
            if failure_time is not None:
                window.update({
                    "energy_end_s": failure_time,
                    "energy_censored_reason": "heuristic_current_plateau",
                    "compliance_source": "heuristic",
                    "compliance_current_a": cfg["heuristic_failure_current_a"],
                    "failure_time_s": failure_time,
                    "energy_is_comparable": False,
                })

    return window, _flag_points(metadata_id, points, window)


def fetch_metadata(cur, args):
    where = [
        "md.irrad_campaign_id IS NOT NULL",
        "md.measurement_category = 'Irradiation'",
    ]
    params = []
    if args.campaign:
        where.append("(ic.campaign_name = %s OR ic.folder_name = %s)")
        params.extend([args.campaign, args.campaign])
    if args.metadata_id:
        where.append("md.id = ANY(%s)")
        params.append(args.metadata_id)
    cur.execute(f"""
        SELECT md.id, md.filename, md.compliance_ch1, md.compliance_ch2
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
        WHERE {' AND '.join(where)}
        ORDER BY md.id
        {f'LIMIT {int(args.limit)}' if args.limit else ''}
    """, params)
    return cur.fetchall()


def fetch_points(cur, metadata_id):
    cur.execute("""
        SELECT point_index, time_val, v_drain, i_drain, i_gate, fluence
        FROM baselines_measurements
        WHERE metadata_id = %s
        ORDER BY point_index
    """, (metadata_id,))
    return cur.fetchall()


WINDOW_COLUMNS = (
    "metadata_id", "active_start_s", "active_end_s", "energy_start_s",
    "energy_end_s", "active_window_basis", "active_window_confidence",
    "energy_censored_reason", "compliance_source", "compliance_current_a",
    "failure_time_s", "energy_is_comparable", "settings",
)


def upsert_windows(cur, windows):
    if not windows:
        return
    values = []
    for window in windows:
        row = []
        for col in WINDOW_COLUMNS:
            value = window[col]
            if col == "settings" and Json is not None:
                value = Json(value)
            row.append(value)
        values.append(tuple(row))
    execute_values(cur, f"""
        INSERT INTO irradiation_waveform_windows
            ({', '.join(WINDOW_COLUMNS)})
        VALUES %s
        ON CONFLICT (metadata_id) DO UPDATE SET
            active_start_s = EXCLUDED.active_start_s,
            active_end_s = EXCLUDED.active_end_s,
            energy_start_s = EXCLUDED.energy_start_s,
            energy_end_s = EXCLUDED.energy_end_s,
            active_window_basis = EXCLUDED.active_window_basis,
            active_window_confidence = EXCLUDED.active_window_confidence,
            energy_censored_reason = EXCLUDED.energy_censored_reason,
            compliance_source = EXCLUDED.compliance_source,
            compliance_current_a = EXCLUDED.compliance_current_a,
            failure_time_s = EXCLUDED.failure_time_s,
            energy_is_comparable = EXCLUDED.energy_is_comparable,
            settings = EXCLUDED.settings,
            updated_at = CURRENT_TIMESTAMP
    """, values, page_size=500)


def replace_point_flags(cur, metadata_ids, flags):
    if not metadata_ids:
        return
    cur.execute(
        "DELETE FROM irradiation_waveform_point_flags WHERE metadata_id = ANY(%s)",
        (metadata_ids,),
    )
    if not flags:
        return
    values = [
        (
            row["metadata_id"], row["point_index"], row["is_active_beam"],
            row["is_pre_failure"], row["is_energy_integrable"],
            row["exclusion_reason"],
        )
        for row in flags
    ]
    execute_values(cur, """
        INSERT INTO irradiation_waveform_point_flags
            (metadata_id, point_index, is_active_beam, is_pre_failure,
             is_energy_integrable, exclusion_reason)
        VALUES %s
        ON CONFLICT (metadata_id, point_index) DO UPDATE SET
            is_active_beam = EXCLUDED.is_active_beam,
            is_pre_failure = EXCLUDED.is_pre_failure,
            is_energy_integrable = EXCLUDED.is_energy_integrable,
            exclusion_reason = EXCLUDED.exclusion_reason,
            updated_at = CURRENT_TIMESTAMP
    """, values, page_size=5000)


def summarize(windows):
    by_basis = Counter(w["active_window_basis"] for w in windows)
    by_censor = Counter(w["energy_censored_reason"] for w in windows)
    comparable = sum(1 for w in windows if w["energy_is_comparable"])
    return by_basis, by_censor, comparable


def main():
    ap = argparse.ArgumentParser(
        description="Infer active-beam and energy windows for irradiation waveforms."
    )
    ap.add_argument("--campaign", help="Filter by campaign_name or folder_name")
    ap.add_argument("--metadata-id", type=int, action="append",
                    help="Analyze one metadata_id; can be repeated")
    ap.add_argument("--limit", type=int, help="Process at most N metadata rows")
    ap.add_argument("--dry-run", action="store_true",
                    help="Infer and summarize without writing")
    ap.add_argument("--no-point-flags", action="store_true",
                    help="Skip irradiation_waveform_point_flags writes")
    args = ap.parse_args()

    t0 = perf_counter()
    conn = get_connection()
    conn.autocommit = False
    try:
        apply_schema(conn)
        with conn.cursor() as cur:
            records = fetch_metadata(cur, args)
            windows = []
            all_flags = []
            for metadata_id, _filename, compliance_ch1, compliance_ch2 in records:
                points = fetch_points(cur, metadata_id)
                window, flags = infer_energy_window(
                    points,
                    metadata_id=metadata_id,
                    compliance_ch1=compliance_ch1,
                    compliance_ch2=compliance_ch2,
                )
                windows.append(window)
                all_flags.extend(flags)

            basis_counts, censor_counts, comparable = summarize(windows)
            if args.dry_run:
                conn.rollback()
            else:
                upsert_windows(cur, windows)
                if not args.no_point_flags:
                    replace_point_flags(
                        cur, [row[0] for row in records], all_flags)
                conn.commit()

        elapsed = perf_counter() - t0
        print("\nIrradiation energy-window inference")
        print(f"  mode:              {'dry-run' if args.dry_run else 'applied'}")
        print(f"  waveform files:    {len(windows)}")
        print(f"  comparable energy: {comparable}")
        print("  active basis:")
        for basis, count in sorted(basis_counts.items()):
            print(f"    {basis:36s} {count}")
        print("  censor reason:")
        for reason, count in sorted(censor_counts.items()):
            print(f"    {reason:36s} {count}")
        print(f"  elapsed:           {elapsed:.1f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
