#!/usr/bin/env python3
"""Calibrate proxy waveform-distance constants against damage-confirmed pairs.

This is a small sanity harness, not a training pipeline.  It uses rank-1
strong/usable rows from damage_equivalence_match_view as sparse retrieval
truth and asks whether alternate proxy-distance settings retrieve the same
candidate condition from the same-device candidate pool.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg2.extras import RealDictCursor

try:
    from db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - exercised by package imports.
    from data_processing_scripts.db_config import get_connection

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
OUT_DIR = REPO_ROOT / "out" / "proxy_distance_calibration"

SETTING_FIELDS = (
    "setting_name",
    "description",
    "max_energy_log_delta",
    "collapse_delta_scale",
    "gate_delta_scale",
    "normalized_vds_delta_scale",
    "energy_log_weight",
    "same_path_penalty",
    "path_unknown_penalty",
    "path_mismatch_penalty",
    "duration_log_weight",
    "best_damage_distance_fallback",
    "energy_out_of_range_log_delta",
    "damage_signature_mismatch_distance",
    "measured_exact_waveform_max",
    "predicted_waveform_max",
    "device_run_waveform_max",
    "weak_waveform_max",
    "waveform_only_max",
    "high_confidence_combined_max",
)

POSITIVE_STATUSES = {
    "measured_damage_candidate",
    "predicted_damage_candidate",
    "device_run_measured_candidate",
    "weak_measured_candidate",
    "waveform_only_candidate",
}

STATUS_PRIORITY = {
    "measured_damage_candidate": 1,
    "predicted_damage_candidate": 2,
    "device_run_measured_candidate": 3,
    "weak_measured_candidate": 4,
    "analog_questionable": 5,
    "waveform_only_candidate": 5,
    "missing_damage_context": 5,
    "inspect_manually": 6,
    "missing_damage_signature_overlap": 6,
    "damage_signature_mismatch": 6,
    "energy_out_of_range": 7,
}

# Damage-signature evidence-coverage model.  These mirror the CASE expressions
# in schema/025_proxy_readiness_waveforms.sql (the `distances`/`coverage` CTEs)
# and must be kept in sync with them.  Weights are for confidence labeling and
# diagnostics only; they are deliberately NOT used to rank candidates.  See
# docs/damage_signature_metric_evidence_rollout_results_2026-06-25.md.
COVERAGE_AXIS_WEIGHTS = {
    "collapse_delta": 0.45,
    "normalized_vds_delta": 0.35,
    "gate_delta": 0.20,
}

# Evidence class -> (numeric tier, missing-axis penalty for the EXPERIMENTAL
# coverage-adjusted distance).  Lower tier is better.
EVIDENCE_CLASS_TIER = {
    "full_signature": 1,
    "collapse_bias_signature": 2,
    "collapse_gate_signature": 3,
    "collapse_only_signature": 4,
    "gate_only_signature": 5,
    "bias_only_signature": 6,
    "no_signature_overlap": 9,
}

EVIDENCE_CLASS_MISSING_AXIS_PENALTY = {
    "full_signature": 0.0,
    "collapse_bias_signature": 0.15,
    "collapse_gate_signature": 0.20,
    "collapse_only_signature": 0.40,
    "gate_only_signature": 0.65,
    "bias_only_signature": 0.80,
    "no_signature_overlap": 1.00,
}

VALIDATION_CANDIDATE_STATUSES = {
    "measured_damage_candidate",
}

CURATION_CANDIDATE_STATUSES = {
    "device_run_measured_candidate",
    "weak_measured_candidate",
    "waveform_only_candidate",
    "analog_questionable",
}

BLOCKED_CANDIDATE_STATUSES = {
    "energy_out_of_range",
    "missing_damage_signature_overlap",
    "damage_signature_mismatch",
}


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v is not None and str(v) != ""]
    return [str(value)]


def _dedupe(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def damage_signature_evidence(
    row: dict[str, Any],
    damage_signature_distance: float | None = None,
) -> dict[str, Any]:
    """Pure-Python mirror of the SQL evidence-coverage descriptors.

    Returns the overlap booleans, available/missing axis lists, axis mask,
    coverage score, evidence class/tier, and the experimental
    coverage-adjusted distance.  ``damage_signature_distance`` is optional; the
    adjusted distance is ``None`` when it is not supplied.
    """
    has_collapse = finite_float(row.get("collapse_delta")) is not None
    has_gate = finite_float(row.get("gate_delta")) is not None
    has_norm = finite_float(row.get("normalized_vds_delta")) is not None

    available = [
        name
        for name, present in (
            ("collapse_delta", has_collapse),
            ("gate_delta", has_gate),
            ("normalized_vds_delta", has_norm),
        )
        if present
    ]
    missing = [
        name
        for name, present in (
            ("collapse_delta", has_collapse),
            ("gate_delta", has_gate),
            ("normalized_vds_delta", has_norm),
        )
        if not present
    ]
    mask_parts = [
        label
        for label, present in (
            ("collapse", has_collapse),
            ("gate", has_gate),
            ("normalized_vds", has_norm),
        )
        if present
    ]
    axis_mask = "+".join(mask_parts) if mask_parts else "none"

    coverage_score = (
        (COVERAGE_AXIS_WEIGHTS["collapse_delta"] if has_collapse else 0.0)
        + (COVERAGE_AXIS_WEIGHTS["normalized_vds_delta"] if has_norm else 0.0)
        + (COVERAGE_AXIS_WEIGHTS["gate_delta"] if has_gate else 0.0)
    )

    if has_collapse and has_gate and has_norm:
        evidence_class = "full_signature"
    elif has_collapse and has_norm:
        evidence_class = "collapse_bias_signature"
    elif has_collapse and has_gate:
        evidence_class = "collapse_gate_signature"
    elif has_collapse:
        evidence_class = "collapse_only_signature"
    elif has_gate:
        evidence_class = "gate_only_signature"
    elif has_norm:
        evidence_class = "bias_only_signature"
    else:
        evidence_class = "no_signature_overlap"

    adjusted = None
    base = finite_float(damage_signature_distance)
    if base is not None:
        penalty = EVIDENCE_CLASS_MISSING_AXIS_PENALTY[evidence_class]
        adjusted = math.sqrt(base ** 2 + penalty ** 2)

    return {
        "has_collapse_overlap": has_collapse,
        "has_gate_overlap": has_gate,
        "has_normalized_vds_overlap": has_norm,
        "damage_signature_available_axes": available,
        "damage_signature_missing_axes": missing,
        "damage_signature_axis_mask": axis_mask,
        "damage_signature_coverage_score": coverage_score,
        "damage_signature_evidence_class": evidence_class,
        "damage_signature_evidence_tier": EVIDENCE_CLASS_TIER[evidence_class],
        "coverage_adjusted_damage_signature_distance": adjusted,
    }


def signature_claim_quality(row: dict[str, Any]) -> str:
    """Decision-facing quality class for the waveform signature axes.

    This is intentionally coarser than ``damage_signature_evidence_class``. It
    answers whether the available axes are rich enough to support a claim, and
    treats the avalanche normalized-Vds exclusion as an explicit axis-excluded
    state instead of ordinary missingness.
    """
    evidence_class = row.get("damage_signature_evidence_class")
    candidate_source = (row.get("candidate_source") or "").lower()
    has_collapse = bool(row.get("has_collapse_overlap"))
    has_norm = bool(row.get("has_normalized_vds_overlap"))

    if evidence_class == "full_signature":
        return "full"
    if evidence_class in {"collapse_bias_signature", "collapse_gate_signature"}:
        return "two_axis"
    if candidate_source == "avalanche" and has_collapse and not has_norm:
        return "axis_excluded"
    if evidence_class in {
        "collapse_only_signature",
        "gate_only_signature",
        "bias_only_signature",
    }:
        return "one_axis"
    return "no_overlap"


def proxy_claim(row: dict[str, Any]) -> dict[str, Any]:
    """Fail-closed candidate interpretation for proxy discovery rows.

    The SQL candidate views mirror this helper. It does not replace the ranking
    distances; it gives dashboards a decision-safe layer so proximity cannot be
    misread as validation.
    """
    candidate_status = row.get("candidate_status")
    match_scope = row.get("match_scope")
    damage_evidence_tier = row.get("damage_evidence_tier")
    measured_status = row.get("measured_comparability_status")
    measured_scope = row.get("measured_match_scope")
    measured_sign_mismatches = _as_int(row.get("measured_sign_mismatch_axis_count"))
    prediction_sign_mismatches = _as_int(
        row.get("prediction_sign_mismatch_axis_count")
    )
    axes_used = _as_int(row.get("damage_signature_axes_used"))
    signature_quality = row.get("signature_claim_quality") or signature_claim_quality(row)
    target_match_tier = row.get("target_match_tier")
    mechanism_ceiling = row.get("mechanism_status_ceiling")

    blockers = _as_list(row.get("candidate_blockers"))
    if match_scope == "cross_device":
        blockers.append("cross_device_screening_only")
    if damage_evidence_tier == "waveform_only":
        blockers.append("no_post_iv_damage_anchor")
    if measured_sign_mismatches > 0:
        blockers.append("measured_damage_sign_mismatch")
    if prediction_sign_mismatches > 0:
        blockers.append("predicted_damage_sign_mismatch")
    if axes_used < 2:
        blockers.append("insufficient_signature_axes_for_validation")
    if signature_quality == "axis_excluded":
        blockers.append("signature_axis_excluded")
    if target_match_tier == "energy_censored_damage_signature_only":
        blockers.append("target_energy_lower_bound_or_signature_only")
    if mechanism_ceiling == "analog_questionable":
        blockers.append("mechanism_analog_questionable")
    blockers = _dedupe(blockers)

    if (
        candidate_status in BLOCKED_CANDIDATE_STATUSES
        or "candidate_energy_below_censored_floor" in blockers
    ):
        status = "blocked"
        basis = "blocked_by_required_evidence"
    elif (
        match_scope == "same_device"
        and candidate_status in VALIDATION_CANDIDATE_STATUSES
        and damage_evidence_tier == "measured_damage"
        and measured_scope == "exact_condition"
        and measured_status in {"strong", "usable"}
        and measured_sign_mismatches == 0
        and axes_used >= 2
    ):
        status = "validation_candidate"
        basis = "same_device_measured_post_iv"
    elif (
        match_scope == "same_device"
        and (
            damage_evidence_tier == "measured_damage"
            or candidate_status in CURATION_CANDIDATE_STATUSES
        )
        and measured_sign_mismatches == 0
    ):
        status = "curation_candidate"
        basis = "same_device_needs_truth_curation"
    else:
        status = "screening_only"
        if match_scope == "cross_device":
            basis = "cross_device_screening"
        elif damage_evidence_tier == "waveform_only":
            basis = "waveform_only"
        elif signature_quality in {"one_axis", "axis_excluded", "no_overlap"}:
            basis = "limited_signature_axes"
        else:
            basis = "screening_evidence_only"

    summary = {
        "validation_candidate": (
            "Same-device measured post-IV evidence is strong enough for validation review."
        ),
        "curation_candidate": (
            "Same-device evidence exists, but human truth-label curation is still required."
        ),
        "screening_only": (
            "Useful for visual discovery only; blockers prevent validation language."
        ),
        "blocked": (
            "Required evidence is missing or contradictory; do not use as a proxy claim."
        ),
    }[status]

    return {
        "signature_claim_quality": signature_quality,
        "proxy_claim_status": status,
        "proxy_claim_basis": basis,
        "proxy_claim_blockers": blockers,
        "proxy_claim_summary": summary,
    }


@dataclass(frozen=True)
class DistanceSettings:
    setting_name: str
    description: str | None
    max_energy_log_delta: float
    collapse_delta_scale: float
    gate_delta_scale: float
    normalized_vds_delta_scale: float
    energy_log_weight: float
    same_path_penalty: float
    path_unknown_penalty: float
    path_mismatch_penalty: float
    duration_log_weight: float
    best_damage_distance_fallback: float
    energy_out_of_range_log_delta: float
    damage_signature_mismatch_distance: float
    measured_exact_waveform_max: float
    predicted_waveform_max: float
    device_run_waveform_max: float
    weak_waveform_max: float
    waveform_only_max: float
    high_confidence_combined_max: float

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "DistanceSettings":
        values = {field: row[field] for field in SETTING_FIELDS}
        for key, value in list(values.items()):
            if key not in {"setting_name", "description"} and value is not None:
                values[key] = float(value)
        return cls(**values)

    def with_name(self, name: str) -> "DistanceSettings":
        return replace(self, setting_name=name, description=f"calibration grid candidate {name}")


@dataclass
class ConfigResult:
    config: DistanceSettings
    total_truth_pairs: int
    target_event_cases: int
    evaluable_cases: int
    missing_truth_cases: int
    top1_hits: int
    top3_hits: int
    mean_truth_rank: float | None
    median_truth_rank: float | None
    spearman_damage_distance: float | None
    damage_correlation_pairs: int
    truth_not_blocked_hits: int

    @property
    def top1_rate(self) -> float | None:
        return rate(self.top1_hits, self.evaluable_cases)

    @property
    def top3_rate(self) -> float | None:
        return rate(self.top3_hits, self.evaluable_cases)

    @property
    def truth_not_blocked_rate(self) -> float | None:
        return rate(self.truth_not_blocked_hits, self.evaluable_cases)

    def sort_key(self) -> tuple[float, float, float, float, float]:
        spearman = self.spearman_damage_distance
        if spearman is None:
            spearman = -2.0
        mean_rank = self.mean_truth_rank if self.mean_truth_rank is not None else 1e9
        return (
            self.top1_rate or 0.0,
            self.top3_rate or 0.0,
            spearman,
            self.truth_not_blocked_rate or 0.0,
            -mean_rank,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "config": asdict(self.config),
            "total_truth_pairs": self.total_truth_pairs,
            "target_event_cases": self.target_event_cases,
            "evaluable_cases": self.evaluable_cases,
            "missing_truth_cases": self.missing_truth_cases,
            "top1_hits": self.top1_hits,
            "top1_rate": self.top1_rate,
            "top3_hits": self.top3_hits,
            "top3_rate": self.top3_rate,
            "mean_truth_rank": self.mean_truth_rank,
            "median_truth_rank": self.median_truth_rank,
            "spearman_damage_distance": self.spearman_damage_distance,
            "damage_correlation_pairs": self.damage_correlation_pairs,
            "truth_not_blocked_hits": self.truth_not_blocked_hits,
            "truth_not_blocked_rate": self.truth_not_blocked_rate,
        }


def rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def finite_float(value: Any) -> float | None:
    if value is None:
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    return value


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def rank_values(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i + 1
        while j < len(indexed) and indexed[j][1] == indexed[i][1]:
            j += 1
        rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[indexed[k][0]] = rank
        i = j
    return ranks


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0.0 or den_y == 0.0:
        return None
    return num / (den_x * den_y)


def spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    return pearson(rank_values(xs), rank_values(ys))


def load_default_settings(conn) -> DistanceSettings:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT setting_name, description, max_energy_log_delta,
                   collapse_delta_scale, gate_delta_scale,
                   normalized_vds_delta_scale, energy_log_weight,
                   same_path_penalty, path_unknown_penalty,
                   path_mismatch_penalty, duration_log_weight,
                   best_damage_distance_fallback,
                   energy_out_of_range_log_delta,
                   damage_signature_mismatch_distance,
                   measured_exact_waveform_max,
                   predicted_waveform_max,
                   device_run_waveform_max,
                   weak_waveform_max,
                   waveform_only_max,
                   high_confidence_combined_max
            FROM stress_proxy_distance_settings
            WHERE setting_name = 'default'
            """
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("stress_proxy_distance_settings default row is missing")
        return DistanceSettings.from_row(dict(row))


def load_truth_summary(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT pair_type, COUNT(*) AS truth_rows,
                   COUNT(DISTINCT right_irrad_run_id) AS irrad_runs
            FROM damage_equivalence_match_view
            WHERE match_rank = 1
              AND comparability_status IN ('strong', 'usable')
              AND pair_type IN ('sc_vs_irradiation', 'avalanche_vs_irradiation')
            GROUP BY pair_type
            ORDER BY pair_type
            """
        )
        return [dict(row) for row in cur.fetchall()]


def load_regression_checks(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE target_event_type = 'SEB'
                      AND candidate_source = 'avalanche'
                      AND candidate_status = 'measured_damage_candidate'
                ) AS seb_avalanche_measured_candidates,
                COUNT(*) FILTER (
                    WHERE target_event_type = 'SEB'
                      AND candidate_source = 'avalanche'
                      AND candidate_status = 'measured_damage_candidate'
                      AND candidate_rank = 1
                ) AS seb_avalanche_rank1_measured_targets,
                COUNT(*) FILTER (WHERE candidate_source = 'avalanche')
                    AS avalanche_candidate_rows,
                COUNT(*) FILTER (
                    WHERE candidate_source = 'avalanche'
                      AND normalized_vds_delta IS NULL
                ) AS avalanche_vds_delta_null_rows,
                COUNT(*) FILTER (
                    WHERE candidate_source = 'avalanche'
                      AND candidate_blockers @> ARRAY[
                          'normalized_vds_axis_excluded_avalanche_clamp'
                      ]::text[]
                ) AS avalanche_vds_omission_explained_rows
            FROM stress_proxy_candidate_view
            """
        )
        row = dict(cur.fetchone())

    seb_recovered = (row["seb_avalanche_measured_candidates"] or 0) > 0
    rank1_recovered = (row["seb_avalanche_rank1_measured_targets"] or 0) > 0
    avalanche_rows = row["avalanche_candidate_rows"] or 0
    vds_omitted = (
        avalanche_rows == (row["avalanche_vds_delta_null_rows"] or 0)
        and avalanche_rows == (row["avalanche_vds_omission_explained_rows"] or 0)
    )
    return [
        {
            "name": "phase4_avalanche_vds_axis_regression",
            "passed": bool(seb_recovered and rank1_recovered and vds_omitted),
            **row,
        }
    ]


def load_calibration_rows(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH truth AS (
                SELECT
                    pair_type,
                    device_type,
                    right_irrad_run_id,
                    left_sc_voltage_v,
                    left_sc_duration_us,
                    left_avalanche_sample_group,
                    nearest_distance AS truth_damage_distance,
                    comparability_status AS truth_comparability_status,
                    pair_type || '|' || device_type || '|' || right_irrad_run_id::text
                        || '|' || COALESCE(left_sc_voltage_v::text, '')
                        || '|' || COALESCE(left_sc_duration_us::text, '')
                        || '|' || COALESCE(left_avalanche_sample_group, '')
                        AS truth_pair_key
                FROM damage_equivalence_match_view
                WHERE match_rank = 1
                  AND comparability_status IN ('strong', 'usable')
                  AND pair_type IN ('sc_vs_irradiation', 'avalanche_vs_irradiation')
            ),
            target_events AS (
                SELECT
                    truth.*,
                    t.stress_record_key AS target_stress_record_key,
                    t.event_type AS target_event_type,
                    t.target_match_tier,
                    t.target_energy_floor_j,
                    t.electrical_terminal_energy_j AS target_energy_j,
                    t.normalized_vds AS target_normalized_vds,
                    t.vds_collapse_fraction AS target_vds_collapse_fraction,
                    t.gate_delta_fraction AS target_gate_delta_fraction,
                    t.stress_duration_s AS target_duration_s,
                    t.path_type AS target_path_type,
                    t.mechanistic_regime AS target_mechanistic_regime
                FROM truth
                JOIN (
                    SELECT
                        s.*,
                        'energy_comparable'::text AS target_match_tier,
                        NULL::double precision AS target_energy_floor_j
                    FROM stress_test_context_view s
                    WHERE s.source = 'irradiation'
                      AND s.event_record_type = 'detected_single_event'
                      AND s.device_type IS NOT NULL
                      AND COALESCE(s.energy_is_comparable, FALSE)
                      AND s.energy_level = 'event'
                      AND s.electrical_terminal_energy_basis = 'integrated_event_vds_id'
                      AND s.electrical_terminal_energy_j IS NOT NULL
                      AND s.electrical_terminal_energy_j > 0.0
                    UNION ALL
                    SELECT
                        s.*,
                        'energy_censored_damage_signature_only'::text AS target_match_tier,
                        CASE
                            WHEN COALESCE(s.energy_censored_reason, 'none') = 'failure_cutoff'
                              THEN s.event_energy_vds_id_j
                        END AS target_energy_floor_j
                    FROM stress_test_context_view s
                    WHERE s.source = 'irradiation'
                      AND s.event_record_type = 'detected_single_event'
                      AND s.device_type IS NOT NULL
                      AND COALESCE(s.energy_censored_reason, 'none') <> 'none'
                      AND NOT (
                          COALESCE(s.energy_is_comparable, FALSE)
                          AND s.energy_level = 'event'
                          AND s.electrical_terminal_energy_basis = 'integrated_event_vds_id'
                          AND s.electrical_terminal_energy_j IS NOT NULL
                          AND s.electrical_terminal_energy_j > 0.0
                      )
                ) t
                  ON t.device_type IS NOT DISTINCT FROM truth.device_type
                 AND t.irrad_run_id IS NOT DISTINCT FROM truth.right_irrad_run_id
            ),
            candidates AS (
                SELECT *
                FROM stress_test_context_view
                WHERE source IN ('sc', 'avalanche')
                  AND device_type IS NOT NULL
                  AND electrical_terminal_energy_j IS NOT NULL
                  AND electrical_terminal_energy_j > 0.0
            )
            SELECT
                te.truth_pair_key,
                te.pair_type AS truth_pair_type,
                te.device_type,
                te.right_irrad_run_id AS target_irrad_run_id,
                te.truth_damage_distance,
                te.truth_comparability_status,
                te.left_sc_voltage_v AS truth_sc_voltage_v,
                te.left_sc_duration_us AS truth_sc_duration_us,
                te.left_avalanche_sample_group AS truth_avalanche_sample_group,
                te.target_stress_record_key,
                te.target_event_type,
                te.target_match_tier,
                te.target_energy_floor_j,
                te.target_energy_j,
                c.stress_record_key AS candidate_stress_record_key,
                c.source AS candidate_source,
                c.physical_sample_key AS candidate_physical_sample_key,
                c.sample_group AS candidate_sample_group,
                c.sc_voltage_v AS candidate_sc_voltage_v,
                c.sc_duration_us AS candidate_sc_duration_us,
                c.electrical_terminal_energy_j AS candidate_energy_j,
                c.mechanistic_regime AS candidate_mechanistic_regime,
                CASE
                    WHEN te.target_match_tier = 'energy_comparable'
                      THEN ABS(LN(c.electrical_terminal_energy_j) - LN(te.target_energy_j))
                END AS log_energy_delta,
                CASE
                    WHEN c.vds_collapse_fraction IS NOT NULL
                     AND te.target_vds_collapse_fraction IS NOT NULL
                      THEN ABS(c.vds_collapse_fraction - te.target_vds_collapse_fraction)
                END AS collapse_delta,
                CASE
                    WHEN c.gate_delta_fraction IS NOT NULL
                     AND te.target_gate_delta_fraction IS NOT NULL
                      THEN ABS(c.gate_delta_fraction - te.target_gate_delta_fraction)
                END AS gate_delta,
                CASE
                    WHEN c.source = 'avalanche' THEN NULL::double precision
                    WHEN c.normalized_vds IS NOT NULL
                     AND te.target_normalized_vds IS NOT NULL
                      THEN ABS(c.normalized_vds - te.target_normalized_vds)
                END AS normalized_vds_delta,
                CASE
                    WHEN c.stress_duration_s IS NOT NULL AND c.stress_duration_s > 0.0
                     AND te.target_duration_s IS NOT NULL AND te.target_duration_s > 0.0
                      THEN ABS(LN(c.stress_duration_s) - LN(te.target_duration_s))
                END AS duration_log_delta,
                COALESCE(mech.path_penalty, 0.75) AS path_penalty,
                COALESCE(mech.preference, 3) AS mechanism_preference,
                mech.status_ceiling AS mechanism_status_ceiling,
                dm.nearest_distance AS damage_distance,
                dm.match_rank AS damage_match_rank,
                dm.comparability_status AS damage_comparability_status,
                CASE
                    WHEN te.pair_type = 'sc_vs_irradiation'
                     AND c.source = 'sc'
                     AND c.sc_voltage_v IS NOT DISTINCT FROM te.left_sc_voltage_v
                     AND c.sc_duration_us IS NOT DISTINCT FROM te.left_sc_duration_us
                        THEN TRUE
                    WHEN te.pair_type = 'avalanche_vs_irradiation'
                     AND c.source = 'avalanche'
                     AND LOWER(COALESCE(c.physical_sample_key, c.sample_group, '')) =
                         LOWER(COALESCE(te.left_avalanche_sample_group, ''))
                        THEN TRUE
                    ELSE FALSE
                END AS is_truth_candidate
            FROM target_events te
            JOIN candidates c ON c.device_type IS NOT DISTINCT FROM te.device_type
            CROSS JOIN stress_proxy_distance_settings settings
            LEFT JOIN LATERAL (
                SELECT rc.status_ceiling, rc.preference, rc.path_penalty
                FROM stress_regime_compatibility rc
                WHERE rc.target_regime = te.target_mechanistic_regime
                  AND (
                        rc.candidate_regime = c.mechanistic_regime
                     OR rc.candidate_regime = 'any'
                  )
                ORDER BY
                    CASE WHEN rc.candidate_regime = c.mechanistic_regime THEN 0 ELSE 1 END,
                    rc.preference ASC
                LIMIT 1
            ) mech ON TRUE
            LEFT JOIN damage_equivalence_match_view dm
              ON dm.device_type IS NOT DISTINCT FROM te.device_type
             AND dm.right_irrad_run_id IS NOT DISTINCT FROM te.right_irrad_run_id
             AND (
                (c.source = 'sc'
                 AND dm.pair_type = 'sc_vs_irradiation'
                 AND dm.left_sc_voltage_v IS NOT DISTINCT FROM c.sc_voltage_v
                 AND dm.left_sc_duration_us IS NOT DISTINCT FROM c.sc_duration_us)
                OR
                (c.source = 'avalanche'
                 AND dm.pair_type = 'avalanche_vs_irradiation'
                 AND LOWER(COALESCE(dm.left_avalanche_sample_group, '')) =
                     LOWER(COALESCE(c.physical_sample_key, c.sample_group, '')))
             )
            WHERE settings.setting_name = 'default'
            ORDER BY te.target_stress_record_key, c.source, c.stress_record_key
            """
        )
        return [dict(row) for row in cur.fetchall()]


def generate_grid(default: DistanceSettings, include_default: bool = True) -> list[DistanceSettings]:
    configs: list[DistanceSettings] = []
    if include_default:
        configs.append(default)

    collapse_scales = sorted({default.collapse_delta_scale, 0.20, 0.25, 0.30})
    gate_scales = sorted({default.gate_delta_scale, 0.15, 0.20, 0.25})
    norm_scales = sorted({default.normalized_vds_delta_scale, 0.10, 0.15, 0.20})
    energy_weights = sorted({default.energy_log_weight, 0.50, 1.00, 1.50})
    damage_signature_thresholds = sorted({default.damage_signature_mismatch_distance, 2.25, 2.50, 2.75})
    weak_thresholds = sorted({default.weak_waveform_max, 2.50, 3.00, 3.50})

    for collapse in collapse_scales:
        for gate in gate_scales:
            for norm in norm_scales:
                for energy_weight in energy_weights:
                    for damage_signature_threshold in damage_signature_thresholds:
                        for weak_threshold in weak_thresholds:
                            candidate = replace(
                                default,
                                setting_name=(
                                    f"grid_c{collapse:g}_g{gate:g}_n{norm:g}_"
                                    f"ew{energy_weight:g}_pt{damage_signature_threshold:g}_"
                                    f"ww{weak_threshold:g}"
                                ),
                                description="calibration grid candidate",
                                collapse_delta_scale=collapse,
                                gate_delta_scale=gate,
                                normalized_vds_delta_scale=norm,
                                energy_log_weight=energy_weight,
                                damage_signature_mismatch_distance=damage_signature_threshold,
                                weak_waveform_max=weak_threshold,
                            )
                            if candidate != default:
                                configs.append(candidate)
    return configs


def distance_terms(row: dict[str, Any], settings: DistanceSettings) -> dict[str, Any]:
    axis_terms: list[float] = []
    collapse = finite_float(row.get("collapse_delta"))
    gate = finite_float(row.get("gate_delta"))
    norm = finite_float(row.get("normalized_vds_delta"))
    if collapse is not None:
        axis_terms.append((collapse / settings.collapse_delta_scale) ** 2)
    if gate is not None:
        axis_terms.append((gate / settings.gate_delta_scale) ** 2)
    if norm is not None:
        axis_terms.append((norm / settings.normalized_vds_delta_scale) ** 2)

    if not axis_terms:
        return {
            "damage_signature_axes_used": 0,
            "damage_signature_axis_distance_sq": None,
            "signature_axis_distance": None,
            "damage_signature_distance": None,
            "waveform_distance": None,
            "combined_screening_distance": None,
        }

    damage_signature_axis_distance_sq = sum(axis_terms) / len(axis_terms)
    signature_axis_distance = math.sqrt(damage_signature_axis_distance_sq)
    path_penalty = finite_float(row.get("path_penalty")) or 0.0
    damage_signature_distance = math.sqrt(damage_signature_axis_distance_sq + path_penalty ** 2)

    energy_term = 0.0
    log_energy = finite_float(row.get("log_energy_delta"))
    if row.get("target_match_tier") == "energy_comparable" and log_energy is not None:
        energy_term = settings.energy_log_weight * log_energy ** 2

    duration = finite_float(row.get("duration_log_delta"))
    if duration is None:
        duration = 1.0
    waveform_distance = math.sqrt(
        energy_term
        + damage_signature_axis_distance_sq
        + path_penalty ** 2
        + settings.duration_log_weight * duration ** 2
    )
    damage_distance = finite_float(row.get("damage_distance"))
    if damage_distance is None:
        damage_distance = settings.best_damage_distance_fallback
    combined = math.sqrt(waveform_distance ** 2 + damage_distance ** 2)

    return {
        "damage_signature_axes_used": len(axis_terms),
        "damage_signature_axis_distance_sq": damage_signature_axis_distance_sq,
        "signature_axis_distance": signature_axis_distance,
        "damage_signature_distance": damage_signature_distance,
        "waveform_distance": waveform_distance,
        "combined_screening_distance": combined,
    }


def classify_row(row: dict[str, Any], settings: DistanceSettings, distances: dict[str, Any]) -> str:
    log_energy = finite_float(row.get("log_energy_delta"))
    if (
        row.get("target_match_tier") == "energy_comparable"
        and log_energy is not None
        and log_energy > settings.energy_out_of_range_log_delta
    ):
        status = "energy_out_of_range"
    elif distances["damage_signature_axes_used"] == 0:
        status = "missing_damage_signature_overlap"
    elif (
        distances["damage_signature_distance"] is not None
        and distances["damage_signature_distance"] > settings.damage_signature_mismatch_distance
    ):
        status = "damage_signature_mismatch"
    elif (
        row.get("damage_comparability_status") in {"strong", "usable"}
        and distances["waveform_distance"] is not None
        and distances["waveform_distance"] <= settings.measured_exact_waveform_max
    ):
        status = "measured_damage_candidate"
    elif (
        row.get("damage_comparability_status") == "weak"
        and distances["waveform_distance"] is not None
        and distances["waveform_distance"] <= settings.weak_waveform_max
    ):
        status = "weak_measured_candidate"
    elif (
        distances["waveform_distance"] is not None
        and distances["waveform_distance"] <= settings.waveform_only_max
    ):
        status = "waveform_only_candidate"
    elif row.get("damage_comparability_status") is None:
        status = "missing_damage_context"
    else:
        status = "inspect_manually"

    if row.get("mechanism_status_ceiling") == "analog_questionable" and status in POSITIVE_STATUSES:
        status = "analog_questionable"
    return status


def score_row(row: dict[str, Any], settings: DistanceSettings) -> dict[str, Any]:
    distances = distance_terms(row, settings)
    status = classify_row(row, settings, distances)
    floor = finite_float(row.get("target_energy_floor_j"))
    candidate_energy = finite_float(row.get("candidate_energy_j"))
    rank_penalty = 1 if floor is not None and candidate_energy is not None and candidate_energy < floor else 0
    evidence = damage_signature_evidence(row, distances.get("damage_signature_distance"))
    return {
        **distances,
        **evidence,
        "candidate_status": status,
        "candidate_status_priority": STATUS_PRIORITY.get(status, 7),
        "candidate_rank_penalty": rank_penalty,
        "mechanism_preference": _as_int(row.get("mechanism_preference")) or 3,
    }


def ranked_candidate_items(
    scored_rows: list[tuple[dict[str, Any], dict[str, Any]]]
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Mirror SQL's mask-aware candidate ranking for one target."""
    by_mask: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for row, scored in scored_rows:
        by_mask[str(scored.get("damage_signature_axis_mask") or "none")].append((row, scored))

    for mask_rows in by_mask.values():
        mask_rows.sort(
            key=lambda item: (
                null_last(item[1].get("signature_axis_distance")),
                null_last(item[0].get("damage_distance")),
                null_last(item[1].get("waveform_distance")),
                item[1].get("candidate_rank_penalty", 0),
                item[0].get("candidate_source") or "",
                item[0].get("candidate_stress_record_key") or "",
            )
        )
        for index, (_row, scored) in enumerate(mask_rows, start=1):
            scored["damage_signature_mask_rank"] = index

    return sorted(
        scored_rows,
        key=lambda item: (
            0 if item[0].get("match_scope", "same_device") == "same_device" else 1,
            item[1]["candidate_status_priority"],
            item[1]["candidate_rank_penalty"],
            item[1]["mechanism_preference"],
            item[1].get("damage_signature_mask_rank", 10**9),
            item[0].get("candidate_source") or "",
            item[0].get("candidate_stress_record_key") or "",
        ),
    )


def evaluate_config(
    rows: list[dict[str, Any]],
    settings: DistanceSettings,
    total_truth_pairs: int,
    target_event_cases: int,
) -> ConfigResult:
    by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_target[str(row["target_stress_record_key"])].append(row)

    truth_ranks: list[int] = []
    top1_hits = 0
    top3_hits = 0
    missing_truth_cases = 0
    truth_not_blocked_hits = 0
    damage_score_pairs: list[tuple[float, float]] = []

    for target_key, target_rows in by_target.items():
        scored_rows = []
        for row in target_rows:
            scored = score_row(row, settings)
            scored_rows.append((row, scored))
            damage_distance = finite_float(row.get("damage_distance"))
            waveform = scored.get("waveform_distance")
            if damage_distance is not None and waveform is not None:
                damage_score_pairs.append((float(waveform), damage_distance))

        scored_rows = ranked_candidate_items(scored_rows)
        truth_candidates = [
            (index + 1, row, scored)
            for index, (row, scored) in enumerate(scored_rows)
            if row.get("is_truth_candidate")
        ]
        if not truth_candidates:
            missing_truth_cases += 1
            continue
        best_truth_rank, _truth_row, truth_score = min(truth_candidates, key=lambda item: item[0])
        truth_ranks.append(best_truth_rank)
        if best_truth_rank == 1:
            top1_hits += 1
        if best_truth_rank <= 3:
            top3_hits += 1
        if truth_score["candidate_status"] not in {
            "energy_out_of_range",
            "missing_damage_signature_overlap",
            "damage_signature_mismatch",
        }:
            truth_not_blocked_hits += 1

    xs = [pair[0] for pair in damage_score_pairs]
    ys = [pair[1] for pair in damage_score_pairs]
    return ConfigResult(
        config=settings,
        total_truth_pairs=total_truth_pairs,
        target_event_cases=target_event_cases,
        evaluable_cases=len(truth_ranks),
        missing_truth_cases=missing_truth_cases,
        top1_hits=top1_hits,
        top3_hits=top3_hits,
        mean_truth_rank=(sum(truth_ranks) / len(truth_ranks)) if truth_ranks else None,
        median_truth_rank=median([float(rank) for rank in truth_ranks]),
        spearman_damage_distance=spearman(xs, ys),
        damage_correlation_pairs=len(damage_score_pairs),
        truth_not_blocked_hits=truth_not_blocked_hits,
    )


def null_last(value: Any) -> tuple[int, float]:
    value = finite_float(value)
    if value is None:
        return (1, 0.0)
    return (0, value)


def summarize_truth_cases(rows: list[dict[str, Any]], truth_summary: list[dict[str, Any]]) -> dict[str, Any]:
    target_keys = {row["target_stress_record_key"] for row in rows}
    truth_pair_keys = {row["truth_pair_key"] for row in rows}
    rows_by_truth = defaultdict(int)
    for row in rows:
        if row.get("is_truth_candidate"):
            rows_by_truth[row["truth_pair_key"]] += 1
    return {
        "truth_summary": truth_summary,
        "truth_pairs_with_target_events": len(truth_pair_keys),
        "target_event_cases": len(target_keys),
        "truth_pairs_with_candidate_rows": sum(1 for count in rows_by_truth.values() if count > 0),
        "candidate_pool_rows": len(rows),
    }


def fmt_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


def fmt_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


def render_report(
    default_result: ConfigResult,
    best_result: ConfigResult,
    top_results: list[ConfigResult],
    truth_case_summary: dict[str, Any],
    regression_checks: list[dict[str, Any]],
    generated_at: str,
) -> str:
    lines = [
        "# Proxy Distance Calibration Report",
        "",
        f"Generated: `{generated_at}`",
        "",
        "## Scope",
        "",
        "This is a sanity harness, not a training pipeline. It uses rank-1 ",
        "`strong`/`usable` rows from `damage_equivalence_match_view` as sparse ",
        "retrieval truth and evaluates same-device proxy candidate ranking. ",
        "The current corpus is tiny, so the results should guide review rather ",
        "than automatically overwrite seeded settings.",
        "",
        "## Truth Set",
        "",
        f"- Truth pairs with target events: {truth_case_summary['truth_pairs_with_target_events']}",
        f"- Target-event retrieval cases: {truth_case_summary['target_event_cases']}",
        f"- Candidate-pool rows evaluated: {truth_case_summary['candidate_pool_rows']}",
        "",
        "| Pair type | Truth rows | Irradiation runs |",
        "| --- | ---: | ---: |",
    ]
    for row in truth_case_summary["truth_summary"]:
        lines.append(f"| {row['pair_type']} | {row['truth_rows']} | {row['irrad_runs']} |")

    lines.extend([
        "",
        "## Named Regression Checks",
        "",
        regression_table(regression_checks),
        "",
        "## Default Settings Performance",
        "",
        result_table([default_result]),
        "",
        "## Best Grid Candidate",
        "",
        result_table([best_result]),
        "",
        "## Top Grid Candidates",
        "",
        result_table(top_results[:10]),
        "",
        "## Recommended Next Step",
        "",
        "Keep the database `default` row unless review explicitly accepts a new ",
        "seeded settings row. The best grid candidate is reported in ",
        "`best_settings.json` so any future tuning can be made as a small, ",
        "reviewable diff to `stress_proxy_distance_settings`.",
        "",
    ])
    return "\n".join(lines)


def regression_table(checks: list[dict[str, Any]]) -> str:
    lines = [
        "| Check | Passed | SEB avalanche measured | Rank-1 SEB avalanche measured | Avalanche Vds delta omitted | Omission explained |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for check in checks:
        lines.append(
            "| "
            + " | ".join(
                [
                    check["name"],
                    "yes" if check["passed"] else "no",
                    str(check["seb_avalanche_measured_candidates"]),
                    str(check["seb_avalanche_rank1_measured_targets"]),
                    f"{check['avalanche_vds_delta_null_rows']}/{check['avalanche_candidate_rows']}",
                    f"{check['avalanche_vds_omission_explained_rows']}/{check['avalanche_candidate_rows']}",
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def result_table(results: list[ConfigResult]) -> str:
    lines = [
        "| Setting | Evaluable cases | Top-1 | Top-3 | Mean truth rank | Spearman(score, damage) | Truth not blocked |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in results:
        lines.append(
            "| "
            + " | ".join(
                [
                    result.config.setting_name,
                    str(result.evaluable_cases),
                    fmt_rate(result.top1_rate),
                    fmt_rate(result.top3_rate),
                    fmt_float(result.mean_truth_rank),
                    fmt_float(result.spearman_damage_distance),
                    fmt_rate(result.truth_not_blocked_rate),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def write_outputs(
    out_dir: Path,
    default_settings: DistanceSettings,
    truth_case_summary: dict[str, Any],
    regression_checks: list[dict[str, Any]],
    results: list[ConfigResult],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    ranked = sorted(results, key=lambda result: result.sort_key(), reverse=True)
    default_result = next(result for result in results if result.config.setting_name == default_settings.setting_name)
    best_result = ranked[0]
    generated_at = datetime.now(timezone.utc).isoformat()

    report = render_report(
        default_result=default_result,
        best_result=best_result,
        top_results=ranked,
        truth_case_summary=truth_case_summary,
        regression_checks=regression_checks,
        generated_at=generated_at,
    )
    (out_dir / "report.md").write_text(report)

    payload = {
        "generated_at": generated_at,
        "truth_case_summary": truth_case_summary,
        "regression_checks": regression_checks,
        "default_settings": asdict(default_settings),
        "best_result": best_result.to_json(),
        "top_results": [result.to_json() for result in ranked[:25]],
        "all_results_count": len(results),
        "caveat": "Tiny corpus sanity harness; do not treat as trained constants.",
    }
    (out_dir / "results.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    (out_dir / "best_settings.json").write_text(
        json.dumps(asdict(best_result.config), indent=2, sort_keys=True)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=OUT_DIR,
        help="Directory for report.md, results.json, and best_settings.json.",
    )
    parser.add_argument(
        "--default-only",
        action="store_true",
        help="Evaluate only the current default settings row.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with get_connection() as conn:
        default_settings = load_default_settings(conn)
        truth_summary = load_truth_summary(conn)
        rows = load_calibration_rows(conn)
        regression_checks = load_regression_checks(conn)

    total_truth_pairs = sum(int(row["truth_rows"]) for row in truth_summary)
    target_event_cases = len({row["target_stress_record_key"] for row in rows})
    truth_case_summary = summarize_truth_cases(rows, truth_summary)

    configs = [default_settings] if args.default_only else generate_grid(default_settings)
    results = [
        evaluate_config(
            rows,
            config,
            total_truth_pairs=total_truth_pairs,
            target_event_cases=target_event_cases,
        )
        for config in configs
    ]
    write_outputs(
        args.out_dir,
        default_settings,
        truth_case_summary,
        regression_checks,
        results,
    )

    best = max(results, key=lambda result: result.sort_key())
    default = next(result for result in results if result.config.setting_name == default_settings.setting_name)
    print(f"Wrote {args.out_dir / 'report.md'}")
    print(f"Wrote {args.out_dir / 'results.json'}")
    print(f"Wrote {args.out_dir / 'best_settings.json'}")
    print(
        "Default: "
        f"top1={fmt_rate(default.top1_rate)}, "
        f"top3={fmt_rate(default.top3_rate)}, "
        f"spearman={fmt_float(default.spearman_damage_distance)}"
    )
    print(
        "Best: "
        f"{best.config.setting_name} "
        f"top1={fmt_rate(best.top1_rate)}, "
        f"top3={fmt_rate(best.top3_rate)}, "
        f"spearman={fmt_float(best.spearman_damage_distance)}"
    )


if __name__ == "__main__":
    main()
