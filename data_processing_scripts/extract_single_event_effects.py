#!/usr/bin/env python3
"""
Extract SEB / SELC-I / SELC-II events from irradiation monitor waveforms.

The detector is intentionally evidence-first:
  * it estimates per-file noise from robust current differences;
  * it extracts positive leakage-current steps or short ramps;
  * it classifies each event from delta-|Id|, delta-|Ig|, their ratio,
    mA-scale current, Vds collapse, and trace-abort evidence;
  * it stores both event rows and per-file rates so Superset can plot event
    frequency against LET.

Definitions used here:
  * SEB: hard-failure candidate. Large mA-scale drain jump with mA gate
    current, Vds collapse, trace abort, or very high drain current.
  * SELCI: drain-gate / gate-oxide path. Delta |Id| and Delta |Ig| rise
    together with comparable magnitude.
  * SELCII: drain-source path. Delta |Id| is much larger than Delta |Ig|, or
    gate current is absent/flat.

Usage:
    python3 extract_single_event_effects.py --dry-run
    python3 extract_single_event_effects.py --rebuild
    python3 extract_single_event_effects.py --campaign GSI_March_2025 --rebuild
    python3 extract_single_event_effects.py --metadata-id 11138 --rebuild
"""

import argparse
import json
import math
import statistics
import sys
from dataclasses import asdict, dataclass
from time import perf_counter

try:
    import psycopg2
    from psycopg2.extras import Json, execute_values
except ImportError:
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import Json, execute_values

from common import apply_schema
from db_config import get_connection


DETECTOR_VERSION = "single_event_detector_v1"
EVENT_TYPES = ("SEB", "SELCI", "SELCII", "MIXED", "UNKNOWN")


@dataclass(frozen=True)
class DetectorConfig:
    noise_sigma_factor: float = 8.0
    id_step_floor_a: float = 1e-6
    ig_step_floor_a: float = 1e-8
    context_points: int = 4
    ramp_window_points: int = 3
    merge_gap_points: int = 1
    max_current_abs_a: float = 1e30

    selc_i_ratio_min: float = 0.33
    selc_i_ratio_max: float = 3.0
    selc_ii_ratio_min: float = 10.0

    seb_id_abs_min_a: float = 1e-3
    seb_id_delta_min_a: float = 1e-3
    seb_ig_abs_min_a: float = 1e-3
    seb_ig_delta_min_a: float = 1e-3
    seb_hard_id_abs_min_a: float = 1e-2
    vds_collapse_min_before_v: float = 20.0
    vds_collapse_drop_v: float = 50.0
    vds_collapse_ratio: float = 0.5
    trace_abort_points: int = 5
    min_fluence_span_for_rate: float = 1.0


CREATE_VIEWS_SQL = """
CREATE OR REPLACE VIEW irradiation_single_event_view AS
SELECT
    e.id AS event_id,
    e.metadata_id,
    e.event_index,
    e.event_type,
    e.confidence,
    e.point_index_start,
    e.point_index_peak,
    e.point_index_end,
    e.cluster_width_points,
    e.time_start,
    e.time_peak,
    e.time_end,
    e.fluence_start,
    e.fluence_peak,
    e.fluence_end,
    e.vds_before_v,
    e.vds_after_v,
    e.vds_delta_v,
    e.id_before_a,
    e.id_after_a,
    e.ig_before_a,
    e.ig_after_a,
    e.delta_id_abs_a,
    e.delta_ig_abs_a,
    e.delta_id_signed_a,
    e.delta_ig_signed_a,
    e.id_slope_a_per_s,
    e.ig_slope_a_per_s,
    e.id_to_ig_delta_ratio,
    e.residual_id_minus_ig_a,
    e.evidence,
    s.detector_version,
    s.analyzed_at,
    s.fluence_span,
    s.event_rate_per_1e5_fluence,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.filename,
    md.irrad_role AS test_condition,
    ic.campaign_name,
    ic.facility,
    COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
    ir.ion_species,
    ir.beam_energy_mev,
    ir.let_surface AS let_mev_cm2_mg,
    ir.range_um
FROM irradiation_single_events e
JOIN irradiation_single_event_file_summary s ON s.metadata_id = e.metadata_id
JOIN baselines_metadata md ON md.id = e.metadata_id
JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
LEFT JOIN irradiation_runs ir ON ir.id = md.irrad_run_id;

CREATE OR REPLACE VIEW irradiation_single_event_file_frequency_view AS
WITH event_types(event_type) AS (
    VALUES ('SEB'), ('SELCI'), ('SELCII'), ('MIXED'), ('UNKNOWN')
)
SELECT
    s.metadata_id,
    et.event_type,
    CASE et.event_type
        WHEN 'SEB' THEN s.seb_count
        WHEN 'SELCI' THEN s.selc_i_count
        WHEN 'SELCII' THEN s.selc_ii_count
        WHEN 'MIXED' THEN s.mixed_count
        ELSE s.unknown_count
    END AS event_count,
    CASE
        WHEN s.duration_s > 0 THEN
            CASE et.event_type
                WHEN 'SEB' THEN s.seb_count
                WHEN 'SELCI' THEN s.selc_i_count
                WHEN 'SELCII' THEN s.selc_ii_count
                WHEN 'MIXED' THEN s.mixed_count
                ELSE s.unknown_count
            END / s.duration_s
        ELSE NULL
    END AS event_rate_per_s,
    CASE
        WHEN s.fluence_span >= 1 THEN
            CASE et.event_type
                WHEN 'SEB' THEN s.seb_count
                WHEN 'SELCI' THEN s.selc_i_count
                WHEN 'SELCII' THEN s.selc_ii_count
                WHEN 'MIXED' THEN s.mixed_count
                ELSE s.unknown_count
            END / s.fluence_span
        ELSE NULL
    END AS event_rate_per_fluence,
    CASE
        WHEN s.fluence_span >= 1 THEN
            CASE et.event_type
                WHEN 'SEB' THEN s.seb_count
                WHEN 'SELCI' THEN s.selc_i_count
                WHEN 'SELCII' THEN s.selc_ii_count
                WHEN 'MIXED' THEN s.mixed_count
                ELSE s.unknown_count
            END / s.fluence_span * 1e5
        ELSE NULL
    END AS event_rate_per_1e5_fluence,
    s.status,
    s.skip_reason,
    s.n_points,
    s.fluence_span,
    s.duration_s,
    s.id_step_threshold_a,
    s.ig_step_threshold_a,
    s.dominant_event_type,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.filename,
    md.irrad_role AS test_condition,
    ic.campaign_name,
    ic.facility,
    COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
    ir.ion_species,
    ir.beam_energy_mev,
    ir.let_surface AS let_mev_cm2_mg,
    ir.range_um
FROM irradiation_single_event_file_summary s
CROSS JOIN event_types et
JOIN baselines_metadata md ON md.id = s.metadata_id
JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
LEFT JOIN irradiation_runs ir ON ir.id = md.irrad_run_id;

CREATE OR REPLACE VIEW irradiation_single_event_let_frequency_view AS
SELECT
    campaign_name,
    facility,
    beam_type,
    ion_species,
    beam_energy_mev,
    let_mev_cm2_mg,
    range_um,
    device_type,
    manufacturer,
    event_type,
    COUNT(*) AS n_files,
    COUNT(*) FILTER (WHERE status = 'analyzed') AS n_analyzed_files,
    SUM(event_count) AS n_events,
    SUM(CASE WHEN fluence_span >= 1 THEN fluence_span ELSE NULL END)
        AS summed_fluence_span,
    SUM(duration_s) AS summed_duration_s,
    CASE WHEN SUM(CASE WHEN fluence_span >= 1 THEN fluence_span ELSE NULL END) > 0
         THEN SUM(event_count)
              / SUM(CASE WHEN fluence_span >= 1 THEN fluence_span ELSE NULL END)
         ELSE NULL END AS event_rate_per_fluence,
    CASE WHEN SUM(CASE WHEN fluence_span >= 1 THEN fluence_span ELSE NULL END) > 0
         THEN SUM(event_count)
              / SUM(CASE WHEN fluence_span >= 1 THEN fluence_span ELSE NULL END)
              * 1e5
         ELSE NULL END AS event_rate_per_1e5_fluence,
    CASE WHEN SUM(duration_s) > 0
         THEN SUM(event_count) / SUM(duration_s)
         ELSE NULL END AS event_rate_per_s
FROM irradiation_single_event_file_frequency_view
GROUP BY
    campaign_name, facility, beam_type, ion_species, beam_energy_mev,
    let_mev_cm2_mg, range_um, device_type, manufacturer, event_type;
"""


def finite_value(value, limit):
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or abs(value) >= limit:
        return None
    return value


def median_valid(values):
    vals = [v for v in values if v is not None and math.isfinite(v)]
    if not vals:
        return None
    return statistics.median(vals)


def min_valid(values):
    vals = [v for v in values if v is not None and math.isfinite(v)]
    return min(vals) if vals else None


def max_valid(values):
    vals = [v for v in values if v is not None and math.isfinite(v)]
    return max(vals) if vals else None


def robust_sigma(values):
    vals = [v for v in values if v is not None and math.isfinite(v)]
    if len(vals) < 5:
        return 0.0
    med = statistics.median(vals)
    mad = statistics.median(abs(v - med) for v in vals)
    sigma = 1.4826 * mad
    if sigma > 0:
        return sigma
    if len(vals) >= 2:
        return statistics.pstdev(vals)
    return 0.0


def delta_series(values):
    out = [None] * len(values)
    for i in range(1, len(values)):
        if values[i] is not None and values[i - 1] is not None:
            out[i] = values[i] - values[i - 1]
    return out


def positive_delta(delta):
    if delta is None or not math.isfinite(delta):
        return 0.0
    return max(0.0, delta)


def window_delta(values, center, width):
    before = median_valid(values[max(0, center - width):center])
    after = median_valid(values[center:min(len(values), center + width)])
    if before is None or after is None:
        return 0.0
    return max(0.0, after - before)


def cluster_indices(indices, merge_gap):
    if not indices:
        return []
    clusters = []
    start = prev = indices[0]
    for idx in indices[1:]:
        if idx - prev <= merge_gap + 1:
            prev = idx
            continue
        clusters.append((start, prev))
        start = prev = idx
    clusters.append((start, prev))
    return clusters


def edge_median(values, from_start=True, width=5):
    if from_start:
        return median_valid(values[:width])
    return median_valid(values[-width:])


def value_at_or_near(values, idx):
    if 0 <= idx < len(values) and values[idx] is not None:
        return values[idx]
    for offset in range(1, 4):
        left = idx - offset
        right = idx + offset
        if 0 <= left < len(values) and values[left] is not None:
            return values[left]
        if 0 <= right < len(values) and values[right] is not None:
            return values[right]
    return None


def safe_div(num, den):
    if num is None or den is None or den <= 0:
        return None
    return num / den


def confidence_for_ratio(ratio, low, high):
    if ratio is None or ratio <= 0:
        return 0.55
    center_distance = abs(math.log10(ratio))
    span = max(abs(math.log10(low)), abs(math.log10(high)))
    if span == 0:
        return 0.75
    return max(0.55, min(0.95, 0.95 - 0.35 * center_distance / span))


def classify_event(delta_id, delta_ig, id_after, ig_after, vds_before,
                   vds_after, points_after_event, id_thr, ig_thr, cfg):
    delta_id = max(0.0, delta_id or 0.0)
    delta_ig = max(0.0, delta_ig or 0.0)
    id_after = id_after or 0.0
    ig_after = ig_after or 0.0

    id_signal = delta_id >= id_thr
    ig_signal = delta_ig >= ig_thr
    ratio = delta_id / delta_ig if delta_ig > 0 else None

    vds_collapse = False
    if vds_before is not None and vds_after is not None:
        drop = vds_before - vds_after
        vds_collapse = (
            vds_before >= cfg.vds_collapse_min_before_v
            and (
                drop >= cfg.vds_collapse_drop_v
                or vds_after <= vds_before * cfg.vds_collapse_ratio
            )
        )

    trace_abort = points_after_event <= cfg.trace_abort_points
    drain_jump_ma = delta_id >= cfg.seb_id_delta_min_a
    gate_jump_ma = delta_ig >= cfg.seb_ig_delta_min_a
    gate_level_ma = ig_after >= cfg.seb_ig_abs_min_a
    hard_drain_level = id_after >= cfg.seb_hard_id_abs_min_a
    hard_drain_jump = delta_id >= cfg.seb_hard_id_abs_min_a

    if ((drain_jump_ma and (gate_jump_ma or gate_level_ma or vds_collapse))
            or (hard_drain_jump and (vds_collapse or trace_abort))
            or (hard_drain_level and vds_collapse and drain_jump_ma)):
        confidence = 0.90 if (gate_jump_ma or gate_level_ma) else 0.78
        if vds_collapse:
            confidence = min(0.98, confidence + 0.05)
        return "SEB", confidence, ratio, {
            "drain_jump_ma": drain_jump_ma,
            "gate_jump_ma": gate_jump_ma,
            "gate_level_ma": gate_level_ma,
            "hard_drain_level": hard_drain_level,
            "hard_drain_jump": hard_drain_jump,
            "vds_collapse": vds_collapse,
            "trace_abort": trace_abort,
        }

    if id_signal and ig_signal and ratio is not None:
        if cfg.selc_i_ratio_min <= ratio <= cfg.selc_i_ratio_max:
            return "SELCI", confidence_for_ratio(
                ratio, cfg.selc_i_ratio_min, cfg.selc_i_ratio_max), ratio, {
                    "id_signal": id_signal,
                    "ig_signal": ig_signal,
                    "ratio_band": "selc_i",
                }
        if ratio >= cfg.selc_ii_ratio_min:
            return "SELCII", 0.82, ratio, {
                "id_signal": id_signal,
                "ig_signal": ig_signal,
                "ratio_band": "selc_ii",
            }
        return "MIXED", 0.65, ratio, {
            "id_signal": id_signal,
            "ig_signal": ig_signal,
            "ratio_band": "between_selc_i_and_selc_ii",
        }

    if id_signal:
        return "SELCII", 0.76 if not ig_signal else 0.68, ratio, {
            "id_signal": id_signal,
            "ig_signal": ig_signal,
            "gate_absent_or_flat": not ig_signal,
        }

    return "UNKNOWN", 0.50, ratio, {
        "id_signal": id_signal,
        "ig_signal": ig_signal,
    }


def build_event(cluster, arrays, signals, thresholds, cfg):
    start, end = cluster
    n = len(arrays["id_abs"])
    id_thr, ig_thr = thresholds

    peak = max(range(start, end + 1),
               key=lambda i: signals["id"][i] + signals["ig"][i])
    before_slice = slice(max(0, start - cfg.context_points), start)
    after_slice = slice(end, min(n, end + cfg.context_points))

    id_before = median_valid(arrays["id_abs"][before_slice])
    id_after = median_valid(arrays["id_abs"][after_slice])
    ig_before = median_valid(arrays["ig_abs"][before_slice])
    ig_after = median_valid(arrays["ig_abs"][after_slice])
    sid_before = median_valid(arrays["id_signed"][before_slice])
    sid_after = median_valid(arrays["id_signed"][after_slice])
    sig_before = median_valid(arrays["ig_signed"][before_slice])
    sig_after = median_valid(arrays["ig_signed"][after_slice])

    if id_before is None or id_after is None:
        id_before = arrays["id_abs"][start - 1] if start > 0 else None
        id_after = arrays["id_abs"][end]
    if ig_before is None or ig_after is None:
        ig_before = arrays["ig_abs"][start - 1] if start > 0 else None
        ig_after = arrays["ig_abs"][end]

    delta_id = None if id_before is None or id_after is None else id_after - id_before
    delta_ig = None if ig_before is None or ig_after is None else ig_after - ig_before
    if (delta_id is None or delta_id < id_thr) and signals["id"][peak] >= id_thr:
        delta_id = signals["id"][peak]
    if (delta_ig is None or delta_ig < ig_thr) and signals["ig"][peak] >= ig_thr:
        delta_ig = signals["ig"][peak]

    if (delta_id is None or delta_id < id_thr) and (delta_ig is None or delta_ig < ig_thr):
        return None

    delta_id_signed = (
        None if sid_before is None or sid_after is None
        else sid_after - sid_before
    )
    delta_ig_signed = (
        None if sig_before is None or sig_after is None
        else sig_after - sig_before
    )

    vds_before = median_valid(arrays["vds"][before_slice])
    vds_after = median_valid(arrays["vds"][after_slice])
    vds_delta = (
        None if vds_before is None or vds_after is None
        else vds_after - vds_before
    )

    time_start = value_at_or_near(arrays["time"], start)
    time_peak = value_at_or_near(arrays["time"], peak)
    time_end = value_at_or_near(arrays["time"], end)
    fluence_start = value_at_or_near(arrays["fluence"], start)
    fluence_peak = value_at_or_near(arrays["fluence"], peak)
    fluence_end = value_at_or_near(arrays["fluence"], end)

    before_time = median_valid(arrays["time"][before_slice])
    after_time = median_valid(arrays["time"][after_slice])
    dt = None
    if before_time is not None and after_time is not None:
        dt = after_time - before_time
    id_slope = safe_div(max(0.0, delta_id or 0.0), dt)
    ig_slope = safe_div(max(0.0, delta_ig or 0.0), dt)

    event_type, confidence, ratio, evidence = classify_event(
        delta_id, delta_ig, id_after, ig_after, vds_before, vds_after,
        n - end - 1, id_thr, ig_thr, cfg)
    evidence.update({
        "id_signal_peak_a": signals["id"][peak],
        "ig_signal_peak_a": signals["ig"][peak],
        "id_threshold_a": id_thr,
        "ig_threshold_a": ig_thr,
        "context_points": cfg.context_points,
        "ramp_window_points": cfg.ramp_window_points,
    })

    residual = None
    if delta_id is not None and delta_ig is not None:
        residual = delta_id - delta_ig

    return {
        "event_type": event_type,
        "confidence": confidence,
        "point_index_start": arrays["point_index"][start],
        "point_index_peak": arrays["point_index"][peak],
        "point_index_end": arrays["point_index"][end],
        "cluster_width_points": end - start + 1,
        "time_start": time_start,
        "time_peak": time_peak,
        "time_end": time_end,
        "fluence_start": fluence_start,
        "fluence_peak": fluence_peak,
        "fluence_end": fluence_end,
        "vds_before_v": vds_before,
        "vds_after_v": vds_after,
        "vds_delta_v": vds_delta,
        "id_before_a": id_before,
        "id_after_a": id_after,
        "ig_before_a": ig_before,
        "ig_after_a": ig_after,
        "delta_id_abs_a": max(0.0, delta_id or 0.0),
        "delta_ig_abs_a": max(0.0, delta_ig or 0.0),
        "delta_id_signed_a": delta_id_signed,
        "delta_ig_signed_a": delta_ig_signed,
        "id_slope_a_per_s": id_slope,
        "ig_slope_a_per_s": ig_slope,
        "id_to_ig_delta_ratio": ratio,
        "residual_id_minus_ig_a": residual,
        "id_threshold_a": id_thr,
        "ig_threshold_a": ig_thr,
        "evidence": evidence,
    }


def detect_events_for_file(points, cfg):
    arrays = {
        "point_index": [],
        "time": [],
        "vds": [],
        "id_signed": [],
        "ig_signed": [],
        "id_abs": [],
        "ig_abs": [],
        "fluence": [],
    }

    for row in points:
        point_index, time_val, vds, i_drain, i_gate, fluence = row
        id_signed = finite_value(i_drain, cfg.max_current_abs_a)
        ig_signed = finite_value(i_gate, cfg.max_current_abs_a)
        arrays["point_index"].append(point_index)
        arrays["time"].append(finite_value(time_val, cfg.max_current_abs_a))
        arrays["vds"].append(finite_value(vds, cfg.max_current_abs_a))
        arrays["id_signed"].append(id_signed)
        arrays["ig_signed"].append(ig_signed)
        arrays["id_abs"].append(abs(id_signed) if id_signed is not None else None)
        arrays["ig_abs"].append(abs(ig_signed) if ig_signed is not None else None)
        arrays["fluence"].append(finite_value(fluence, cfg.max_current_abs_a))

    n = len(points)
    id_deltas = delta_series(arrays["id_abs"])
    ig_deltas = delta_series(arrays["ig_abs"])
    id_sigma = robust_sigma(id_deltas)
    ig_sigma = robust_sigma(ig_deltas)
    id_thr = max(cfg.id_step_floor_a, cfg.noise_sigma_factor * id_sigma)
    ig_thr = max(cfg.ig_step_floor_a, cfg.noise_sigma_factor * ig_sigma)

    signals = {"id": [0.0] * n, "ig": [0.0] * n}
    candidates = []
    for i in range(1, n):
        id_signal = max(
            positive_delta(id_deltas[i]),
            window_delta(arrays["id_abs"], i, cfg.ramp_window_points),
        )
        ig_signal = max(
            positive_delta(ig_deltas[i]),
            window_delta(arrays["ig_abs"], i, cfg.ramp_window_points),
        )
        signals["id"][i] = id_signal
        signals["ig"][i] = ig_signal
        # The taxonomy requested here is drain-current anchored.  Gate-only
        # excursions are useful noise diagnostics, but they are not SEB,
        # SELC-I, or SELC-II unless Id has at least a weak simultaneous rise.
        if id_signal >= id_thr or (
                ig_signal >= ig_thr and id_signal >= 0.25 * id_thr):
            candidates.append(i)

    events = []
    for cluster in cluster_indices(candidates, cfg.merge_gap_points):
        event = build_event(cluster, arrays, signals, (id_thr, ig_thr), cfg)
        if event is not None:
            events.append(event)

    counts = {event_type: 0 for event_type in EVENT_TYPES}
    for event in events:
        counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1

    valid_id = [v for v in arrays["id_abs"] if v is not None]
    valid_ig = [v for v in arrays["ig_abs"] if v is not None]
    valid_time = [v for v in arrays["time"] if v is not None]
    valid_fluence = [v for v in arrays["fluence"] if v is not None]
    valid_vds = [v for v in arrays["vds"] if v is not None]

    duration = None
    if valid_time:
        duration = max(valid_time) - min(valid_time)
        if duration <= 0:
            duration = None

    raw_fluence_span = None
    fluence_span = None
    if valid_fluence:
        raw_fluence_span = max(valid_fluence) - min(valid_fluence)
        if raw_fluence_span >= cfg.min_fluence_span_for_rate:
            fluence_span = raw_fluence_span

    event_count = len(events)
    rates = {
        "event_rate_per_s": safe_div(event_count, duration),
        "event_rate_per_fluence": safe_div(event_count, fluence_span),
        "event_rate_per_1e5_fluence": (
            safe_div(event_count, fluence_span) * 1e5
            if safe_div(event_count, fluence_span) is not None else None
        ),
    }

    dominant = None
    if event_count:
        dominant = max(EVENT_TYPES, key=lambda t: counts.get(t, 0))

    summary = {
        "status": "analyzed" if valid_id else "skipped",
        "skip_reason": None if valid_id else "no_valid_i_drain",
        "n_points": n,
        "n_valid_id": len(valid_id),
        "n_valid_ig": len(valid_ig),
        "has_gate_current": bool(valid_ig),
        "has_fluence": bool(valid_fluence),
        "time_start": min(valid_time) if valid_time else None,
        "time_stop": max(valid_time) if valid_time else None,
        "duration_s": duration,
        "fluence_start": valid_fluence[0] if valid_fluence else None,
        "fluence_stop": valid_fluence[-1] if valid_fluence else None,
        "fluence_min": min(valid_fluence) if valid_fluence else None,
        "fluence_max": max(valid_fluence) if valid_fluence else None,
        "fluence_span": raw_fluence_span,
        "vds_initial_v": edge_median(arrays["vds"], True, cfg.context_points),
        "vds_final_v": edge_median(arrays["vds"], False, cfg.context_points),
        "vds_min_v": min_valid(valid_vds),
        "vds_max_v": max_valid(valid_vds),
        "vds_span_v": (
            max(valid_vds) - min(valid_vds) if valid_vds else None
        ),
        "id_initial_a": edge_median(arrays["id_abs"], True, cfg.context_points),
        "id_final_a": edge_median(arrays["id_abs"], False, cfg.context_points),
        "id_max_abs_a": max_valid(valid_id),
        "ig_initial_a": edge_median(arrays["ig_abs"], True, cfg.context_points),
        "ig_final_a": edge_median(arrays["ig_abs"], False, cfg.context_points),
        "ig_max_abs_a": max_valid(valid_ig),
        "id_noise_sigma_a": id_sigma,
        "ig_noise_sigma_a": ig_sigma,
        "id_step_threshold_a": id_thr,
        "ig_step_threshold_a": ig_thr,
        "event_count_total": event_count,
        "seb_count": counts["SEB"],
        "selc_i_count": counts["SELCI"],
        "selc_ii_count": counts["SELCII"],
        "mixed_count": counts["MIXED"],
        "unknown_count": counts["UNKNOWN"],
        "dominant_event_type": dominant,
        **rates,
    }
    return summary, events


def ensure_views(cur):
    cur.execute(CREATE_VIEWS_SQL)


def fetch_metadata(cur, args):
    where = [
        "md.irrad_campaign_id IS NOT NULL",
        "md.measurement_category = 'Irradiation'",
    ]
    params = []
    if args.campaign:
        where.append("(ic.campaign_name = %s OR ic.folder_name = %s)")
        params.extend([args.campaign, args.campaign])
    if args.device_type:
        where.append("md.device_type = %s")
        params.append(args.device_type)
    if args.metadata_id:
        where.append("md.id = ANY(%s)")
        params.append(args.metadata_id)

    cur.execute(f"""
        SELECT md.id, ic.campaign_name, md.device_type, md.device_id,
               md.filename
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
        WHERE {' AND '.join(where)}
        ORDER BY ic.campaign_name, md.id
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


def already_analyzed(cur, metadata_ids):
    if not metadata_ids:
        return set()
    cur.execute("""
        SELECT metadata_id
        FROM irradiation_single_event_file_summary
        WHERE metadata_id = ANY(%s)
    """, (metadata_ids,))
    return {row[0] for row in cur.fetchall()}


def delete_existing(cur, metadata_ids):
    if not metadata_ids:
        return
    cur.execute("""
        DELETE FROM irradiation_single_event_file_summary
        WHERE metadata_id = ANY(%s)
    """, (metadata_ids,))


SUMMARY_COLUMNS = (
    "metadata_id", "detector_version", "status", "skip_reason",
    "n_points", "n_valid_id", "n_valid_ig", "has_gate_current",
    "has_fluence", "time_start", "time_stop", "duration_s",
    "fluence_start", "fluence_stop", "fluence_min", "fluence_max",
    "fluence_span", "vds_initial_v", "vds_final_v", "vds_min_v",
    "vds_max_v", "vds_span_v", "id_initial_a", "id_final_a",
    "id_max_abs_a", "ig_initial_a", "ig_final_a", "ig_max_abs_a",
    "id_noise_sigma_a", "ig_noise_sigma_a", "id_step_threshold_a",
    "ig_step_threshold_a", "event_count_total", "seb_count",
    "selc_i_count", "selc_ii_count", "mixed_count", "unknown_count",
    "dominant_event_type", "event_rate_per_s", "event_rate_per_fluence",
    "event_rate_per_1e5_fluence", "settings",
)


EVENT_COLUMNS = (
    "metadata_id", "event_index", "event_type", "confidence",
    "point_index_start", "point_index_peak", "point_index_end",
    "cluster_width_points", "time_start", "time_peak", "time_end",
    "fluence_start", "fluence_peak", "fluence_end", "vds_before_v",
    "vds_after_v", "vds_delta_v", "id_before_a", "id_after_a",
    "ig_before_a", "ig_after_a", "delta_id_abs_a", "delta_ig_abs_a",
    "delta_id_signed_a", "delta_ig_signed_a", "id_slope_a_per_s",
    "ig_slope_a_per_s", "id_to_ig_delta_ratio",
    "residual_id_minus_ig_a", "id_threshold_a", "ig_threshold_a",
    "evidence",
)


def insert_results(cur, summaries, events):
    if summaries:
        values = []
        for row in summaries:
            values.append(tuple(row[col] for col in SUMMARY_COLUMNS))
        execute_values(cur, f"""
            INSERT INTO irradiation_single_event_file_summary
                ({', '.join(SUMMARY_COLUMNS)})
            VALUES %s
        """, values, page_size=500)

    if events:
        values = []
        for row in events:
            values.append(tuple(row[col] for col in EVENT_COLUMNS))
        execute_values(cur, f"""
            INSERT INTO irradiation_single_events
                ({', '.join(EVENT_COLUMNS)})
            VALUES %s
        """, values, page_size=1000)


def json_ready_config(cfg):
    return Json(asdict(cfg))


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--campaign",
                    help="Filter by campaign_name or folder_name")
    ap.add_argument("--device-type",
                    help="Filter by baselines_metadata.device_type")
    ap.add_argument("--metadata-id", type=int, action="append",
                    help="Analyze one metadata_id; can be repeated")
    ap.add_argument("--rebuild", action="store_true",
                    help="Overwrite existing detector output")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run detector and report counts without writing")
    ap.add_argument("--noise-sigma-factor", type=float,
                    default=DetectorConfig.noise_sigma_factor)
    ap.add_argument("--id-step-floor-a", type=float,
                    default=DetectorConfig.id_step_floor_a)
    ap.add_argument("--ig-step-floor-a", type=float,
                    default=DetectorConfig.ig_step_floor_a)
    ap.add_argument("--seb-id-abs-min-a", type=float,
                    default=DetectorConfig.seb_id_abs_min_a)
    ap.add_argument("--seb-ig-abs-min-a", type=float,
                    default=DetectorConfig.seb_ig_abs_min_a)
    args = ap.parse_args()

    cfg = DetectorConfig(
        noise_sigma_factor=args.noise_sigma_factor,
        id_step_floor_a=args.id_step_floor_a,
        ig_step_floor_a=args.ig_step_floor_a,
        seb_id_abs_min_a=args.seb_id_abs_min_a,
        seb_ig_abs_min_a=args.seb_ig_abs_min_a,
    )

    t0 = perf_counter()
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            apply_schema(conn)
            ensure_views(cur)
            conn.commit()

            records = fetch_metadata(cur, args)
            metadata_ids = [row[0] for row in records]
            if args.rebuild and not args.dry_run:
                delete_existing(cur, metadata_ids)
                conn.commit()

            skip_existing = set()
            if not args.rebuild:
                skip_existing = already_analyzed(cur, metadata_ids)

            summaries = []
            events = []
            counts = {event_type: 0 for event_type in EVENT_TYPES}
            analyzed_files = 0
            skipped_existing = 0

            for idx, (metadata_id, campaign_name, device_type, device_id,
                      filename) in enumerate(records, start=1):
                if metadata_id in skip_existing:
                    skipped_existing += 1
                    continue

                points = fetch_points(cur, metadata_id)
                summary, file_events = detect_events_for_file(points, cfg)
                summary["metadata_id"] = metadata_id
                summary["detector_version"] = DETECTOR_VERSION
                summary["settings"] = json_ready_config(cfg)
                summaries.append(summary)

                for event_index, event in enumerate(file_events, start=1):
                    event["metadata_id"] = metadata_id
                    event["event_index"] = event_index
                    event["evidence"] = Json(event["evidence"])
                    events.append(event)
                    counts[event["event_type"]] += 1

                analyzed_files += 1
                if idx % 50 == 0:
                    print(f"  [{idx}/{len(records)}] analyzed "
                          f"{analyzed_files} files ({campaign_name})",
                          flush=True)

            if args.dry_run:
                conn.rollback()
            else:
                insert_results(cur, summaries, events)
                conn.commit()

            elapsed = perf_counter() - t0
            print("\nSingle-event extraction summary")
            print(f"  mode:              {'dry-run' if args.dry_run else 'applied'}")
            print(f"  candidate files:   {len(records)}")
            print(f"  analyzed files:    {analyzed_files}")
            print(f"  skipped existing:  {skipped_existing}")
            print(f"  event rows:        {len(events)}")
            for event_type in EVENT_TYPES:
                print(f"  {event_type:7s}:          {counts[event_type]}")
            print(f"  elapsed:           {elapsed:.1f}s")

            if args.dry_run:
                print("\nDRY RUN: no detector rows were written.")
            else:
                print("\nViews refreshed:")
                print("  irradiation_single_event_view")
                print("  irradiation_single_event_file_frequency_view")
                print("  irradiation_single_event_let_frequency_view")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
