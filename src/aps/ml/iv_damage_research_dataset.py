"""Audit, freeze, and grouped-split construction for V3 research curves."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
from statistics import median
from typing import Iterable, Mapping, Sequence

from aps.enrich.iv_parameters.contracts import ExtractionConfig, SweepPoint
from aps.enrich.iv_parameters.threshold_voltage import extract_vth
from aps.ml.iv_damage_research_contracts import (
    AuditedPair,
    HORIZON_STATUS,
    RESEARCH_PROTOCOL_ID,
    ResearchContractError,
    ResearchPair,
    ResearchPoint,
    SplitAssignment,
    VALIDATION_SCHEMES,
)


TARGET_CURRENT_CANDIDATES_A = (0.001, 0.01, 0.1)
MINIMUM_CURVE_POINTS = 20
GRID_POINTS = 64

CANDIDATE_QUERY = """
SELECT pair.id, pair.pair_key, pair.pre_feature_id, pair.post_feature_id,
       pair.pre_metadata_id, pair.post_metadata_id, pair.physical_device_key,
       pair.device_type, pair.manufacturer, campaign.campaign_name,
       pair.irrad_run_id::text, pair.ion_species, pair.beam_energy_mev,
       pair.let_surface, pair.range_um, pair.beam_type, pair.fluence_at_meas,
       pre.drain_bias_value, post.drain_bias_value
FROM iv_physical_response_pairs pair
JOIN iv_physical_curve_features pre ON pre.id = pair.pre_feature_id
JOIN iv_physical_curve_features post ON post.id = pair.post_feature_id
LEFT JOIN irradiation_campaigns campaign ON campaign.id = pair.irrad_campaign_id
WHERE pair.stress_type = 'irradiation'
  AND pair.target_type = 'delta_vth_v'
  AND pair.curve_family = 'IdVg'
  AND pair.reference_tier = 'strict_pre_irrad'
  AND pair.same_physical_device
  AND pair.quality_status = 'usable'
  AND COALESCE(pre.metadata_created_at, pre.created_at) <= (%s AT TIME ZONE 'UTC')
  AND COALESCE(post.metadata_created_at, post.created_at) <= (%s AT TIME ZONE 'UTC')
  AND pair.created_at <= (%s AT TIME ZONE 'UTC')
ORDER BY pair.pair_key
"""

POINT_QUERY = """
SELECT id, point_index, v_gate, i_drain, v_drain
FROM baselines_measurements
WHERE metadata_id = %s
ORDER BY point_index, id
"""


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)


def sha256_payload(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def point_payload(points: Sequence[ResearchPoint]) -> list[dict[str, object]]:
    return [
        {
            "source_point_id": point.source_point_id,
            "point_index": point.point_index,
            "v_gate_v": float(point.v_gate_v),
            "v_drain_v": None if point.v_drain_v is None else float(point.v_drain_v),
            "i_drain_a": float(point.i_drain_a),
        }
        for point in sorted(points, key=lambda item: (item.point_index, item.source_point_id))
    ]


def point_payload_hash(points: Sequence[ResearchPoint]) -> str:
    return sha256_payload(point_payload(points))


def extraction_config(target_current_a: float) -> ExtractionConfig:
    return ExtractionConfig(
        config_version=f"{RESEARCH_PROTOCOL_ID}:{target_current_a:.6g}A",
        target_type="delta_vth_v",
        target_current_a=float(target_current_a),
        required_vds_v=1.0,
        vds_tolerance_v=0.05,
        sweep_direction="ascending",
        log_current_interpolation=True,
        minimum_points=3,
    )


def extraction_config_payload(target_current_a: float) -> dict[str, object]:
    """Return the complete, canonical extraction contract frozen with each pair."""
    return asdict(extraction_config(target_current_a))


def pair_identity_payload(
    pair: ResearchPair,
    *,
    target_current_a: float,
    pre_point_hash: str,
    post_point_hash: str,
    pre_vth_v: float | None,
    post_vth_v: float | None,
    observed_delta_vth_v: float | None,
    extraction_diagnostics: Mapping[str, object],
    common_grid_point_count: int,
    admitted: bool,
    exclusion_reasons: Sequence[str],
) -> dict[str, object]:
    """Canonical identity for every frozen input, target, and audit decision."""
    return {
        "source_pair_id": pair.source_pair_id,
        "pair_key": pair.pair_key,
        "pre_feature_id": pair.pre_feature_id,
        "post_feature_id": pair.post_feature_id,
        "pre_metadata_id": pair.pre_metadata_id,
        "post_metadata_id": pair.post_metadata_id,
        "pre_point_hash": pre_point_hash,
        "post_point_hash": post_point_hash,
        "physical_device_key": pair.physical_device_key,
        "device_type": pair.device_type,
        "manufacturer": pair.manufacturer,
        "campaign_key": pair.campaign_key,
        "run_key": pair.run_key,
        "ion_species": pair.ion_species,
        "beam_energy_mev": pair.beam_energy_mev,
        "let_surface": pair.let_surface,
        "range_um": pair.range_um,
        "beam_type": pair.beam_type,
        "fluence": pair.fluence,
        "pre_vds_v": pair.pre_vds_v,
        "post_vds_v": pair.post_vds_v,
        "extraction_config": extraction_config_payload(target_current_a),
        "pre_vth_v": pre_vth_v,
        "post_vth_v": post_vth_v,
        "observed_delta_vth_v": observed_delta_vth_v,
        "extraction_diagnostics": dict(extraction_diagnostics),
        "common_grid_point_count": common_grid_point_count,
        "admitted": admitted,
        "exclusion_reasons": sorted(set(exclusion_reasons)),
    }


def _sweep_points(points: Sequence[ResearchPoint]) -> list[SweepPoint]:
    return [SweepPoint(point.point_index, point.v_gate_v, point.v_drain_v, point.i_drain_a) for point in points]


def _finite_points(rows: Iterable[Sequence[object]]) -> tuple[ResearchPoint, ...]:
    points = []
    for source_id, point_index, v_gate, i_drain, v_drain in rows:
        values = (v_gate, i_drain)
        if any(value is None or not math.isfinite(float(value)) or abs(float(value)) >= 1e30 for value in values):
            continue
        if v_drain is not None and (not math.isfinite(float(v_drain)) or abs(float(v_drain)) >= 1e30):
            v_drain = None
        points.append(
            ResearchPoint(
                int(source_id),
                int(point_index),
                float(v_gate),
                float(i_drain),
                None if v_drain is None else float(v_drain),
            )
        )
    return tuple(points)


def load_candidate_pairs(conn, *, source_cutoff: datetime | None = None) -> list[ResearchPair]:
    """Load only the V2 candidate index; truth is reloaded from raw points."""
    cutoff = source_cutoff or datetime.now(timezone.utc)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ResearchContractError("source_cutoff must include an explicit timezone")
    cursor = conn.cursor()
    try:
        cursor.execute(CANDIDATE_QUERY, (cutoff, cutoff, cutoff))
        candidates = []
        for row in cursor.fetchall():
            cursor.execute(POINT_QUERY, (row[4],))
            pre_points = _finite_points(cursor.fetchall())
            cursor.execute(POINT_QUERY, (row[5],))
            post_points = _finite_points(cursor.fetchall())
            candidates.append(
                ResearchPair(
                    source_pair_id=int(row[0]),
                    pair_key=str(row[1]),
                    pre_feature_id=int(row[2]),
                    post_feature_id=int(row[3]),
                    pre_metadata_id=int(row[4]),
                    post_metadata_id=int(row[5]),
                    physical_device_key=str(row[6]),
                    device_type=str(row[7]),
                    manufacturer=row[8],
                    campaign_key=row[9],
                    run_key=row[10],
                    ion_species=row[11],
                    beam_energy_mev=row[12],
                    let_surface=row[13],
                    range_um=row[14],
                    beam_type=row[15],
                    fluence=row[16],
                    pre_vds_v=row[17],
                    post_vds_v=row[18],
                    pre_points=pre_points,
                    post_points=post_points,
                )
            )
        return candidates
    finally:
        cursor.close()


def _support(points: Sequence[ResearchPoint]) -> tuple[float, float]:
    values = [point.v_gate_v for point in points]
    return min(values), max(values)


def audit_pair(pair: ResearchPair, *, target_current_a: float = 0.01) -> AuditedPair:
    reasons = []
    config = extraction_config(target_current_a)
    if len(pair.pre_points) < MINIMUM_CURVE_POINTS:
        reasons.append("insufficient_pre_points")
    if len(pair.post_points) < MINIMUM_CURVE_POINTS:
        reasons.append("insufficient_post_points")
    if pair.pre_vds_v is None or abs(float(pair.pre_vds_v) - 1.0) > 0.05:
        reasons.append("pre_vds_protocol_mismatch")
    if pair.post_vds_v is None or abs(float(pair.post_vds_v) - 1.0) > 0.05:
        reasons.append("post_vds_protocol_mismatch")

    pre_result = extract_vth(_sweep_points(pair.pre_points), config)
    post_result = extract_vth(_sweep_points(pair.post_points), config)
    if not pre_result.usable:
        reasons.extend(f"pre_{reason}" for reason in pre_result.quality_reasons)
    if not post_result.usable:
        reasons.extend(f"post_{reason}" for reason in post_result.quality_reasons)
    common_count = 0
    if pair.pre_points and pair.post_points:
        pre_min, pre_max = _support(pair.pre_points)
        post_min, post_max = _support(pair.post_points)
        common_min, common_max = max(pre_min, post_min), min(pre_max, post_max)
        if common_max <= common_min:
            reasons.append("no_common_voltage_support")
        else:
            common_count = GRID_POINTS

    pre_hash = point_payload_hash(pair.pre_points)
    post_hash = point_payload_hash(pair.post_points)
    pre_vth = float(pre_result.value) if pre_result.usable else None
    post_vth = float(post_result.value) if post_result.usable else None
    delta = None if pre_vth is None or post_vth is None else post_vth - pre_vth
    diagnostics = {
        "pre": dict(pre_result.diagnostics),
        "post": dict(post_result.diagnostics),
        "pre_uncertainty_v": pre_result.uncertainty,
        "post_uncertainty_v": post_result.uncertainty,
    }
    admitted = not reasons
    payload_hash = sha256_payload(
        pair_identity_payload(
            pair,
            target_current_a=target_current_a,
            pre_point_hash=pre_hash,
            post_point_hash=post_hash,
            pre_vth_v=pre_vth,
            post_vth_v=post_vth,
            observed_delta_vth_v=delta,
            extraction_diagnostics=diagnostics,
            common_grid_point_count=common_count,
            admitted=admitted,
            exclusion_reasons=reasons,
        )
    )
    return AuditedPair(
        pair,
        admitted,
        tuple(sorted(set(reasons))),
        pre_hash,
        post_hash,
        payload_hash,
        pre_vth,
        post_vth,
        delta,
        diagnostics,
        common_count,
    )


def target_current_sensitivity(
    pairs: Sequence[ResearchPair],
    candidates: Sequence[float] = TARGET_CURRENT_CANDIDATES_A,
) -> dict[str, object]:
    """Audit each threshold definition and apply the predeclared selection rule."""
    reports = []
    for current in candidates:
        audited = [audit_pair(pair, target_current_a=current) for pair in pairs]
        admitted = [row for row in audited if row.admitted]
        uncertainties = [
            float(row.extraction_diagnostics[role]["bracket_voltage_step_v"])
            for row in admitted
            for role in ("pre", "post")
        ]
        by_device_pre: dict[str, list[float]] = defaultdict(list)
        for row in admitted:
            by_device_pre[row.candidate.physical_device_key].append(float(row.pre_vth_v))
        spreads = [max(values) - min(values) for values in by_device_pre.values() if len(values) > 1]
        reports.append(
            {
                "target_current_a": float(current),
                "admitted_pairs": len(admitted),
                "admitted_devices": len(by_device_pre),
                "median_bracket_width_v": median(uncertainties) if uncertainties else None,
                "median_repeated_pre_spread_v": median(spreads) if spreads else 0.0,
                "delta_vth_min_v": min((row.observed_delta_vth_v for row in admitted), default=None),
                "delta_vth_max_v": max((row.observed_delta_vth_v for row in admitted), default=None),
            }
        )

    def rank(report: Mapping[str, object]) -> tuple[object, ...]:
        spread = report["median_repeated_pre_spread_v"]
        bracket = report["median_bracket_width_v"]
        return (
            -int(report["admitted_devices"]),
            -int(report["admitted_pairs"]),
            math.inf if spread is None else float(spread),
            math.inf if bracket is None else float(bracket),
            0 if math.isclose(float(report["target_current_a"]), 0.01) else 1,
        )

    selected = min(reports, key=rank) if reports else None
    return {
        "candidates": reports,
        "selected_target_current_a": None if selected is None else selected["target_current_a"],
    }


def deterministic_split_assignments(
    pairs: Sequence[AuditedPair], validation_scheme: str
) -> tuple[SplitAssignment, ...]:
    if validation_scheme not in VALIDATION_SCHEMES:
        raise ResearchContractError(f"unsupported validation scheme: {validation_scheme}")
    admitted = [row for row in pairs if row.admitted]
    field = {"leave_device": "physical_device_key", "leave_run": "run_key", "leave_campaign": "campaign_key"}[
        validation_scheme
    ]
    group_by_key = {}
    for row in admitted:
        value = getattr(row.candidate, field)
        if not str(value or "").strip():
            raise ResearchContractError(f"{validation_scheme} requires {field} for {row.candidate.pair_key}")
        group_by_key[row.candidate.pair_key] = str(value)

    # Connected components enforce both the requested holdout and the
    # non-negotiable physical-device boundary. If one device spans two runs or
    # campaigns, those condition groups are held out together.
    parent = {row.candidate.pair_key: row.candidate.pair_key for row in admitted}

    def find(key: str) -> str:
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    first_by_token = {}
    for row in admitted:
        key = row.candidate.pair_key
        tokens = (
            f"device:{row.candidate.physical_device_key}",
            f"group:{group_by_key[key]}",
        )
        for token in tokens:
            previous = first_by_token.setdefault(token, key)
            union(previous, key)

    components = defaultdict(list)
    for row in admitted:
        components[find(row.candidate.pair_key)].append(row)
    if len(components) < 2:
        raise ResearchContractError(f"{validation_scheme} requires at least two independent components")

    assignments = []
    ordered = sorted(components.values(), key=lambda rows: min(row.candidate.pair_key for row in rows))
    for fold, rows in enumerate(ordered):
        held_groups = "|".join(sorted({group_by_key[row.candidate.pair_key] for row in rows}))
        for row in rows:
            digest = sha256_payload(
                {
                    "pair_key": row.candidate.pair_key,
                    "scheme": validation_scheme,
                    "fold": fold,
                    "group": held_groups,
                }
            )
            assignments.append(
                SplitAssignment(
                    row.candidate.pair_key,
                    validation_scheme,
                    fold,
                    held_groups,
                    row.candidate.physical_device_key,
                    digest,
                )
            )
    return tuple(sorted(assignments, key=lambda item: item.pair_key))


def snapshot_payload(
    audited: Sequence[AuditedPair],
    *,
    snapshot_version: str,
    target_current_a: float,
    source_code_sha: str,
    source_fingerprint: str,
    source_cutoff: datetime | None = None,
) -> dict[str, object]:
    cutoff = source_cutoff or datetime.now(timezone.utc)
    payload = {
        "snapshot_version": snapshot_version,
        "research_protocol_id": RESEARCH_PROTOCOL_ID,
        "target_current_a": float(target_current_a),
        "horizon_status": HORIZON_STATUS,
        "source_cutoff": cutoff.isoformat(),
        "source_code_sha": source_code_sha,
        "source_fingerprint": source_fingerprint,
        "pair_payload_hashes": [
            row.pair_payload_hash for row in sorted(audited, key=lambda item: item.candidate.pair_key)
        ],
    }
    return {**payload, "snapshot_hash": sha256_payload(payload)}


def audit_report(pairs: Sequence[AuditedPair], sensitivity: Mapping[str, object]) -> dict[str, object]:
    admitted = [row for row in pairs if row.admitted]
    excluded = [row for row in pairs if not row.admitted]
    device_counts = Counter(row.candidate.physical_device_key for row in admitted)
    pre_reuse = Counter(row.candidate.pre_metadata_id for row in admitted)
    return {
        "claim_class": "retrospective_research",
        "horizon_status": HORIZON_STATUS,
        "candidate_pairs": len(pairs),
        "admitted_pairs": len(admitted),
        "excluded_pairs": len(excluded),
        "physical_devices": len(device_counts),
        "campaigns": len({row.candidate.campaign_key for row in admitted if row.candidate.campaign_key}),
        "runs": len({row.candidate.run_key for row in admitted if row.candidate.run_key}),
        "fluence_missing_pairs": sum(row.candidate.fluence is None for row in admitted),
        "shared_pre_curves": {str(key): count for key, count in pre_reuse.items() if count > 1},
        "device_pair_counts": dict(sorted(device_counts.items())),
        "excluded": [{"pair_key": row.candidate.pair_key, "reasons": list(row.exclusion_reasons)} for row in excluded],
        "target_current_sensitivity": dict(sensitivity),
        "limitations": {
            "measurement_horizon": "unknown_or_heterogeneous",
            "fluence": "missing values remain missing; no zero imputation",
            "replicates": "repeated files are not treated as controlled replicates",
            "claim": "retrospective research only; not decision eligible",
        },
    }
