"""Governed, resumable admission of audited raw V3 measurement evidence.

Planning is read-only with respect to PostgreSQL and writes a canonical plan
record below ``APS_IV_DAMAGE_GOVERNANCE_ROOT``. Approval is the first database
mutation. Application delegates acquisitions, extraction, and response
materialization to the existing governed writers and records each successful
item so a failed batch can be resumed without reinterpretation.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Mapping

from psycopg2.extras import Json

from aps.enrich.iv_parameters.contracts import ExtractionConfig
from aps.ml.iv_damage_curves import AcquisitionSpec, load_acquisition_sweep_points, register_acquisition
from aps.ml.iv_damage_evidence import (
    ObservationContext,
    ResponseUnitSpec,
    extract_and_persist_rdson,
    extract_and_persist_vth,
    materialize_response_unit,
)
from aps.ml.iv_damage_readiness import DOMAIN_REQUIRED_FEATURES


class EvidenceManifestError(RuntimeError):
    """A manifest or its lifecycle state is incomplete or contradictory."""


ALLOWED_ROLES = frozenset({"train", "calibration"})
RAW_SOURCE_RELATIONS = frozenset({"baselines_metadata", "baselines_measurements"})
FORBIDDEN_SOURCE_MARKERS = ("prediction", "validation", "residual", "iv_physical_", "model")
ITEM_ORDER = {"acquisition": 0, "stress_session": 1, "observation": 2, "response_unit": 3}
SAFE_BATCH_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def _plain(value):
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def canonical_json(value: object) -> str:
    return json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: object, name: str) -> str:
    result = str(value).strip() if value is not None else ""
    if not result:
        raise EvidenceManifestError(f"{name} is required")
    return result


def _batch_key(value: object) -> str:
    result = _text(value, "batch_key")
    if not SAFE_BATCH_KEY.fullmatch(result):
        raise EvidenceManifestError(
            "batch_key must be 1-128 ASCII letters, digits, dots, underscores, or hyphens"
        )
    return result


def _sha256_text(value: object, name: str) -> str:
    result = _text(value, name)
    if not SHA256_HEX.fullmatch(result):
        raise EvidenceManifestError(
            f"{name} must be exactly 64 lowercase hexadecimal characters"
        )
    return result


def _timestamp(value: object, name: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceManifestError(f"{name} must be ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise EvidenceManifestError(f"{name} must include a timezone")
    return result


def _array(manifest: Mapping[str, object], name: str) -> list[dict[str, object]]:
    value = manifest.get(name, [])
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise EvidenceManifestError(f"{name} must be an array of objects")
    return value


def _validate_source(
    item: Mapping[str, object], prefix: str, *, expected_relation: str
) -> None:
    relation = _text(item.get("source_relation"), f"{prefix}.source_relation")
    lowered = relation.lower()
    if (
        relation != expected_relation
        or relation not in RAW_SOURCE_RELATIONS
        or any(marker in lowered for marker in FORBIDDEN_SOURCE_MARKERS)
    ):
        raise EvidenceManifestError(
            f"{prefix}.source_relation must be {expected_relation!r}; got {relation!r}"
        )
    _sha256_text(item.get("source_checksum"), f"{prefix}.source_checksum")


def _positive_ids(value: object, name: str) -> list[int]:
    if (
        not isinstance(value, list)
        or not value
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in value
        )
        or len(set(value)) != len(value)
    ):
        raise EvidenceManifestError(
            f"{name} must be a nonempty array of unique positive integers"
        )
    return value


def validate_manifest(manifest: Mapping[str, object]) -> dict[str, object]:
    """Validate and normalize manifest V1 without touching the database."""
    if not isinstance(manifest, Mapping):
        raise EvidenceManifestError("manifest must be a JSON object")
    if manifest.get("manifest_version") != 1:
        raise EvidenceManifestError("manifest_version must be 1")
    batch_key = _batch_key(manifest.get("batch_key"))
    prepared_by = _text(manifest.get("prepared_by"), "prepared_by")
    prepared_at = _timestamp(manifest.get("prepared_at"), "prepared_at")
    source_cutoff = _timestamp(manifest.get("source_cutoff"), "source_cutoff")
    if prepared_at < source_cutoff:
        raise EvidenceManifestError("prepared_at cannot precede source_cutoff")
    claim = manifest.get("claim")
    if not isinstance(claim, Mapping):
        raise EvidenceManifestError("claim must be an object")
    if claim.get("stress_type") != "irradiation" or claim.get("target_type") != "delta_vth_v":
        raise EvidenceManifestError("manifest V1 activation is limited to irradiation/delta_vth_v")
    if claim.get("intended_split_role") not in ALLOWED_ROLES:
        raise EvidenceManifestError("historical intended_split_role must be train or calibration")
    if claim.get("reference_policy") != "same_device":
        raise EvidenceManifestError("activation evidence requires reference_policy=same_device")
    protocol = _text(claim.get("measurement_protocol_id"), "claim.measurement_protocol_id")
    horizon = claim.get("prediction_horizon_s")
    if (
        isinstance(horizon, bool)
        or not isinstance(horizon, (int, float))
        or not math.isfinite(horizon)
        or horizon <= 0
    ):
        raise EvidenceManifestError(
            "claim.prediction_horizon_s must be a positive finite signed value"
        )
    if claim.get("fixed_horizon") is not True:
        raise EvidenceManifestError("claim.fixed_horizon must be true")
    config = manifest.get("extraction_config")
    if not isinstance(config, Mapping):
        raise EvidenceManifestError("extraction_config must be an object")
    extraction = ExtractionConfig(**dict(config))
    if extraction.target_type != "delta_vth_v":
        raise EvidenceManifestError("extraction_config must target delta_vth_v")

    items: list[dict[str, object]] = []
    keys: set[str] = set()
    acquisition_keys: set[str] = set()
    acquisition_metadata_ids: set[int] = set()
    acquisition_by_key: dict[str, dict[str, object]] = {}
    observation_keys: set[str] = set()
    observation_by_key: dict[str, dict[str, object]] = {}
    session_keys: set[str] = set()
    session_by_key: dict[str, dict[str, object]] = {}
    unit_keys: set[str] = set()

    for section, item_type in (
        ("acquisitions", "acquisition"),
        ("stress_sessions", "stress_session"),
        ("observations", "observation"),
        ("response_units", "response_unit"),
    ):
        for index, original in enumerate(_array(manifest, section)):
            item = dict(original)
            item_key = _text(item.get("item_key"), f"{section}[{index}].item_key")
            if item_key in keys:
                raise EvidenceManifestError(f"duplicate item_key: {item_key}")
            keys.add(item_key)

            if item_type == "acquisition":
                _validate_source(
                    item,
                    f"{section}[{index}]",
                    expected_relation="baselines_metadata",
                )
                if item.get("measurement_protocol_id") != protocol:
                    raise EvidenceManifestError(
                        f"{item_key} uses a different measurement protocol"
                    )
                measured_at = _timestamp(
                    item.get("measured_at"), f"{item_key}.measured_at"
                )
                if measured_at > source_cutoff:
                    raise EvidenceManifestError(
                        f"{item_key}.measured_at exceeds source_cutoff"
                    )
                metadata_id = item.get("metadata_id")
                if (
                    isinstance(metadata_id, bool)
                    or not isinstance(metadata_id, int)
                    or metadata_id <= 0
                ):
                    raise EvidenceManifestError(
                        f"{item_key}.metadata_id must be positive"
                    )
                if metadata_id in acquisition_metadata_ids:
                    raise EvidenceManifestError(f"duplicate metadata_id: {metadata_id}")
                acquisition_metadata_ids.add(metadata_id)
                acquisition_key = _text(
                    item.get("acquisition_key"), f"{item_key}.acquisition_key"
                )
                if acquisition_key in acquisition_keys:
                    raise EvidenceManifestError(
                        f"duplicate acquisition_key: {acquisition_key}"
                    )
                acquisition_keys.add(acquisition_key)
                _text(
                    item.get("physical_device_key"),
                    f"{item_key}.physical_device_key",
                )
                if item.get("curve_family") != "IdVg":
                    raise EvidenceManifestError(
                        f"{item_key}.curve_family must be IdVg"
                    )
                identity_source = item.get("identity_source", "metadata_exact")
                if identity_source not in {"metadata_exact", "manual_review"}:
                    raise EvidenceManifestError(
                        f"{item_key}.identity_source is unsupported"
                    )
                if identity_source == "manual_review":
                    _text(item.get("reviewed_by"), f"{item_key}.reviewed_by")
                    _text(item.get("review_reason"), f"{item_key}.review_reason")
                acquisition_by_key[acquisition_key] = item

            elif item_type == "stress_session":
                if item.get("stress_type") != "irradiation":
                    raise EvidenceManifestError(
                        f"{item_key}.stress_type must be irradiation"
                    )
                features = item.get("stress_features")
                if not isinstance(features, Mapping):
                    raise EvidenceManifestError(
                        f"{item_key}.stress_features must be an object"
                    )
                missing = sorted(
                    name
                    for name in DOMAIN_REQUIRED_FEATURES["irradiation"]
                    if name != "pre_value" and features.get(name) is None
                )
                if missing:
                    raise EvidenceManifestError(
                        f"{item_key} missing irradiation features: {', '.join(missing)}"
                    )
                try:
                    session_horizon = float(features.get("prediction_horizon_s"))
                except (TypeError, ValueError):
                    session_horizon = -1.0
                if session_horizon != float(horizon):
                    raise EvidenceManifestError(
                        f"{item_key} does not carry the fixed prediction horizon"
                    )
                session_key = _text(
                    item.get("stress_session_key"),
                    f"{item_key}.stress_session_key",
                )
                if session_key in session_keys:
                    raise EvidenceManifestError(
                        f"duplicate stress_session_key: {session_key}"
                    )
                session_keys.add(session_key)
                for name in (
                    "physical_device_key",
                    "campaign_key",
                    "run_key",
                    "stress_condition_key",
                ):
                    _text(item.get(name), f"{item_key}.{name}")
                if features.get("stress_condition_key") != item.get(
                    "stress_condition_key"
                ):
                    raise EvidenceManifestError(
                        f"{item_key}.stress_condition_key differs from stress_features"
                    )
                identity_source = item.get(
                    "identity_source", "campaign_registry"
                )
                if identity_source not in {
                    "campaign_registry",
                    "logbook_exact",
                    "manual_review",
                }:
                    raise EvidenceManifestError(
                        f"{item_key}.identity_source is unsupported"
                    )
                if identity_source == "manual_review":
                    _text(item.get("reviewed_by"), f"{item_key}.reviewed_by")
                    _text(item.get("review_reason"), f"{item_key}.review_reason")
                session_by_key[session_key] = item

            elif item_type == "observation":
                _validate_source(
                    item,
                    f"{section}[{index}]",
                    expected_relation="baselines_measurements",
                )
                if item.get("acquisition_key") not in acquisition_keys:
                    raise EvidenceManifestError(
                        f"{item_key} references an acquisition not declared earlier"
                    )
                _text(
                    item.get("replicate_group_key"),
                    f"{item_key}.replicate_group_key",
                )
                _positive_ids(
                    item.get("source_row_ids"),
                    f"{item_key}.source_row_ids",
                )
                observation_keys.add(item_key)
                observation_by_key[item_key] = item

            else:
                session_key = item.get("stress_session_key")
                if session_key not in session_keys:
                    raise EvidenceManifestError(
                        f"{item_key} references a stress session not declared earlier"
                    )
                if (
                    item.get("stress_type") != "irradiation"
                    or item.get("target_type") != "delta_vth_v"
                ):
                    raise EvidenceManifestError(
                        f"{item_key} does not match the manifest claim"
                    )
                if item.get("reference_policy") != "same_device":
                    raise EvidenceManifestError(
                        f"{item_key} requires reference_policy=same_device"
                    )
                pre = item.get("pre_observation_keys")
                post = item.get("post_observation_keys")
                if (
                    not isinstance(pre, list)
                    or not isinstance(post, list)
                    or len(pre) < 2
                    or len(post) < 2
                ):
                    raise EvidenceManifestError(
                        f"{item_key} requires at least two pre and two post observations"
                    )
                if len(set(pre + post)) != len(pre) + len(post):
                    raise EvidenceManifestError(
                        f"{item_key} pre/post observations must be distinct"
                    )
                if any(key not in observation_keys for key in pre + post):
                    raise EvidenceManifestError(
                        f"{item_key} references an undeclared observation"
                    )
                if item.get("measurement_protocol_id") != protocol:
                    raise EvidenceManifestError(
                        f"{item_key} uses a different measurement protocol"
                    )
                unit_key = _text(item.get("unit_key"), f"{item_key}.unit_key")
                if unit_key in unit_keys:
                    raise EvidenceManifestError(f"duplicate unit_key: {unit_key}")
                unit_keys.add(unit_key)
                for name in (
                    "physical_device_key",
                    "device_type",
                    "campaign_key",
                    "run_key",
                    "ion_species",
                ):
                    _text(item.get(name), f"{item_key}.{name}")
                if item.get("minimum_replicates", 2) < 2:
                    raise EvidenceManifestError(
                        f"{item_key}.minimum_replicates must be at least two"
                    )
                features = item.get("stress_features")
                try:
                    response_horizon = float(
                        features.get("prediction_horizon_s")
                    )
                except (AttributeError, TypeError, ValueError):
                    response_horizon = -1.0
                if (
                    not isinstance(features, Mapping)
                    or response_horizon != float(horizon)
                ):
                    raise EvidenceManifestError(
                        f"{item_key} does not carry the fixed prediction horizon"
                    )
                session = session_by_key[session_key]
                for name in ("physical_device_key", "campaign_key", "run_key"):
                    if item.get(name) != session.get(name):
                        raise EvidenceManifestError(
                            f"{item_key}.{name} differs from its stress session"
                        )
                if dict(features) != dict(session["stress_features"]):
                    raise EvidenceManifestError(
                        f"{item_key}.stress_features differs from its stress session"
                    )
                response_device = item["physical_device_key"]
                acquisitions = [
                    acquisition_by_key[
                        observation_by_key[key]["acquisition_key"]
                    ]
                    for key in pre + post
                ]
                if any(
                    acquisition["physical_device_key"] != response_device
                    for acquisition in acquisitions
                ):
                    raise EvidenceManifestError(
                        f"{item_key} observations are not all from the response device"
                    )
                pre_groups = {
                    observation_by_key[key]["replicate_group_key"] for key in pre
                }
                post_groups = {
                    observation_by_key[key]["replicate_group_key"] for key in post
                }
                if (
                    len(pre_groups) != 1
                    or len(post_groups) != 1
                    or pre_groups == post_groups
                ):
                    raise EvidenceManifestError(
                        f"{item_key} requires distinct pre/post replicate groups"
                    )
                pre_times = [
                    _timestamp(
                        acquisition_by_key[
                            observation_by_key[key]["acquisition_key"]
                        ]["measured_at"],
                        "measured_at",
                    )
                    for key in pre
                ]
                post_times = [
                    _timestamp(
                        acquisition_by_key[
                            observation_by_key[key]["acquisition_key"]
                        ]["measured_at"],
                        "measured_at",
                    )
                    for key in post
                ]
                if min(post_times) <= max(pre_times):
                    raise EvidenceManifestError(
                        f"{item_key} post observations must follow all pre observations"
                    )

            items.append(
                {"item_key": item_key, "item_type": item_type, "payload": item}
            )

    if not unit_keys:
        raise EvidenceManifestError("manifest requires at least one response unit")

    normalized = _plain(dict(manifest))
    normalized["batch_key"] = batch_key
    normalized["prepared_by"] = prepared_by
    normalized["prepared_at"] = prepared_at.isoformat()
    normalized["source_cutoff"] = source_cutoff.isoformat()
    return {"manifest": normalized, "items": items}


def _cohort_report(manifest: Mapping[str, object]) -> dict[str, object]:
    responses = _array(manifest, "response_units")
    cohorts: dict[str, dict[str, set[str]]] = {}
    for row in responses:
        key = "|".join((
            str(row.get("device_type", "")), str(row.get("ion_species", "")),
            str(row.get("measurement_protocol_id", "")),
            str(manifest["claim"]["prediction_horizon_s"]),
        ))
        entry = cohorts.setdefault(key, {"groups": set(), "devices": set(), "campaigns": set()})
        entry["groups"].add(str(row.get("unit_key")))
        entry["devices"].add(str(row.get("physical_device_key")))
        entry["campaigns"].add(str(row.get("campaign_key")))
    ranked = sorted(({
        "cohort_key": key, "independent_groups": len(value["groups"]),
        "physical_devices": len(value["devices"]), "campaigns": len(value["campaigns"]),
    } for key, value in cohorts.items()), key=lambda row: (
        -row["independent_groups"], -row["physical_devices"], -row["campaigns"], row["cohort_key"]
    ))
    return {"ranked_cohorts": ranked, "selected_cohort": ranked[0]["cohort_key"] if ranked else None}


def plan_evidence(conn, manifest: Mapping[str, object]) -> dict[str, object]:
    validated = validate_manifest(manifest)
    normalized = validated["manifest"]
    exclusions = []
    acquisition_audits: dict[str, dict[str, object]] = {}
    with conn.cursor() as cursor:
        for item in _array(normalized, "acquisitions"):
            cursor.execute(
                """
                SELECT device_id, device_type, file_hash
                FROM baselines_metadata
                WHERE id = %s
                """,
                (item["metadata_id"],),
            )
            row = cursor.fetchone()
            if row is None:
                exclusions.append(
                    {"item_key": item["item_key"], "reason": "metadata_missing"}
                )
                continue
            if str(row[2]) != str(item["source_checksum"]):
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "source_checksum_mismatch",
                    }
                )
            if (
                item.get("identity_source", "metadata_exact") == "metadata_exact"
                and str(row[0]) != str(item.get("physical_device_key"))
            ):
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "physical_device_identity_mismatch",
                    }
                )
            cursor.execute(
                """
                SELECT id, point_index, v_gate, v_drain, i_drain
                FROM baselines_measurements
                WHERE metadata_id = %s
                ORDER BY point_index, id
                """,
                (item["metadata_id"],),
            )
            point_rows = cursor.fetchall()
            if len(point_rows) < 3:
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "insufficient_raw_points",
                    }
                )
                continue
            point_indexes = [row[1] for row in point_rows]
            if len(set(point_indexes)) != len(point_indexes):
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "duplicate_point_index",
                    }
                )
                continue
            try:
                payload = [
                    {
                        "point_index": int(point_index),
                        "v_gate": None if v_gate is None else float(v_gate),
                        "v_drain": None if v_drain is None else float(v_drain),
                        "i_drain": float(i_drain),
                    }
                    for _, point_index, v_gate, v_drain, i_drain in point_rows
                ]
                payload_sha = sha256(payload)
            except (TypeError, ValueError):
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "nonfinite_or_invalid_raw_points",
                    }
                )
                continue
            acquisition_audits[item["acquisition_key"]] = {
                "row_ids": [int(point_id) for point_id, *_ in point_rows],
                "point_payload_sha": payload_sha,
                "device_type": row[1],
            }

        for item in _array(normalized, "observations"):
            audit = acquisition_audits.get(item["acquisition_key"])
            if audit is None:
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "acquisition_not_auditable",
                    }
                )
                continue
            if item["source_row_ids"] != audit["row_ids"]:
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "source_row_ids_mismatch",
                    }
                )
            if item["source_checksum"] != audit["point_payload_sha"]:
                exclusions.append(
                    {
                        "item_key": item["item_key"],
                        "reason": "measurement_checksum_mismatch",
                    }
                )

    observation_by_key = {
        row["item_key"]: row for row in _array(normalized, "observations")
    }
    response_units = _array(normalized, "response_units")
    for response in response_units:
        for observation_key in (
            response["pre_observation_keys"]
            + response["post_observation_keys"]
        ):
            observation = observation_by_key[observation_key]
            audit = acquisition_audits.get(observation["acquisition_key"])
            if audit is not None and audit["device_type"] != response["device_type"]:
                exclusions.append(
                    {
                        "item_key": response["item_key"],
                        "reason": "device_type_mismatch",
                    }
                )
                break

    role = normalized["claim"]["intended_split_role"]
    independent_groups = {
        (
            row["physical_device_key"],
            row["stress_session_key"],
            row["target_type"],
        )
        for row in response_units
    }
    role_devices = {row["physical_device_key"] for row in response_units}
    report = {
        "manifest_version": 1,
        "batch_key": normalized["batch_key"],
        "prepared_by": normalized["prepared_by"],
        "manifest_sha": sha256(normalized),
        "admissible": not exclusions,
        "exclusions": exclusions,
        "item_counts": {
            kind: sum(
                item["item_type"] == kind for item in validated["items"]
            )
            for kind in ITEM_ORDER
        },
        "cohort_audit": _cohort_report(normalized),
        "collection_deficits": {
            "training_groups": (
                max(0, 30 - len(independent_groups))
                if role == "train"
                else 30
            ),
            "calibration_groups": (
                max(0, 10 - len(independent_groups))
                if role == "calibration"
                else 10
            ),
            "calibration_devices": (
                max(0, 10 - len(role_devices))
                if role == "calibration"
                else 10
            ),
            "sealed_external_groups": 30,
            "external_devices": 10,
        },
        "manifest": normalized,
    }
    return report


def write_plan(
    report: Mapping[str, object],
    governance_root: Path,
    report_json: Path,
) -> Path:
    if not report.get("admissible"):
        raise EvidenceManifestError(
            "manifest plan contains exclusions and cannot be approved"
        )
    batch_key = _batch_key(report.get("batch_key"))
    manifest_sha = _sha256_text(report.get("manifest_sha"), "manifest_sha")
    plans = governance_root.resolve() / "evidence-plans"
    plans.mkdir(mode=0o750, parents=True, exist_ok=True)
    payload = canonical_json(report) + "\n"
    plan_path = _plan_path(governance_root, batch_key, manifest_sha)
    if plan_path.exists() and plan_path.read_text() != payload:
        raise EvidenceManifestError(
            "existing plan path contains different content"
        )
    temporary = plan_path.with_suffix(".partial")
    temporary.write_text(payload)
    os.chmod(temporary, 0o640)
    temporary.replace(plan_path)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    return plan_path


def _plan_path(root: Path, batch_key: str, expected_sha: str) -> Path:
    plans = root.resolve() / "evidence-plans"
    candidate = (
        plans
        / (
            f"{_batch_key(batch_key)}--"
            f"{_sha256_text(expected_sha, 'expected_plan_sha')}.json"
        )
    ).resolve()
    if candidate.parent != plans:
        raise EvidenceManifestError(
            "canonical plan path escapes the governance root"
        )
    return candidate


def approve_evidence(
    conn,
    *,
    governance_root: Path,
    batch_key: str,
    expected_plan_sha: str,
    actor: str,
) -> dict[str, object]:
    batch_key = _batch_key(batch_key)
    expected_plan_sha = _sha256_text(
        expected_plan_sha, "expected_plan_sha"
    )
    path = _plan_path(governance_root, batch_key, expected_plan_sha)
    if not path.is_file():
        raise EvidenceManifestError(f"canonical plan does not exist: {path}")
    report = json.loads(path.read_text())
    if report.get("manifest_sha") != expected_plan_sha or report.get("batch_key") != batch_key:
        raise EvidenceManifestError("plan filename and canonical content disagree")
    validated = validate_manifest(report["manifest"])
    if sha256(validated["manifest"]) != expected_plan_sha:
        raise EvidenceManifestError("plan manifest hash has changed")
    actor = _text(actor, "actor")
    if actor == validated["manifest"]["prepared_by"]:
        raise EvidenceManifestError("manifest approval requires a different actor from the preparer")
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, plan_sha, approved_by, status FROM iv_damage_evidence_batches WHERE batch_key = %s FOR UPDATE", (batch_key,))
        existing = cursor.fetchone()
        if existing:
            if existing[1] != expected_plan_sha:
                raise EvidenceManifestError("batch_key is locked to a different manifest hash")
            conn.commit()
            return {"batch_id": int(existing[0]), "batch_key": batch_key, "status": existing[3], "approved_by": existing[2], "idempotent": True}
        cursor.execute("""
            INSERT INTO iv_damage_evidence_batches (
                batch_key, manifest_version, plan_sha, manifest, plan_report,
                prepared_by, prepared_at, approved_by, approved_at, status
            ) VALUES (%s, 1, %s, %s, %s, %s, %s, %s, clock_timestamp(), 'approved')
            RETURNING id
        """, (batch_key, expected_plan_sha, Json(validated["manifest"]), Json(dict(report)),
              validated["manifest"]["prepared_by"], _timestamp(validated["manifest"]["prepared_at"], "prepared_at"), actor))
        batch_id = int(cursor.fetchone()[0])
        ordered = sorted(validated["items"], key=lambda item: (ITEM_ORDER[item["item_type"]], item["item_key"]))
        for order, item in enumerate(ordered):
            cursor.execute("""
                INSERT INTO iv_damage_evidence_batch_items
                    (batch_id, item_key, item_type, item_order, payload_sha)
                VALUES (%s, %s, %s, %s, %s)
            """, (batch_id, item["item_key"], item["item_type"], order, sha256(item["payload"])))
    conn.commit()
    return {"batch_id": batch_id, "batch_key": batch_key, "status": "approved", "approved_by": actor, "idempotent": False}


def _register_session(conn, item: Mapping[str, object]) -> int:
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, physical_device_key, stress_type, campaign_key, run_key, stress_condition_key, stress_features, identity_source FROM iv_damage_stress_sessions WHERE stress_session_key = %s", (item["stress_session_key"],))
        existing = cursor.fetchone()
        expected = (item["physical_device_key"], item["stress_type"], item["campaign_key"], item["run_key"], item["stress_condition_key"], dict(item["stress_features"]), item.get("identity_source", "campaign_registry"))
        if existing:
            if tuple(existing[1:6]) + (dict(existing[6]), existing[7]) != expected:
                raise EvidenceManifestError("stress session key is bound to different evidence")
            return int(existing[0])
        cursor.execute("""
            INSERT INTO iv_damage_stress_sessions (
                stress_session_key, physical_device_key, stress_type, campaign_key,
                run_key, stress_condition_key, stress_features, started_at, ended_at,
                identity_source, reviewed_by, reviewed_at, review_reason
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                CASE WHEN %s = 'manual_review' THEN clock_timestamp() END,%s)
            RETURNING id
        """, (item["stress_session_key"], item["physical_device_key"], item["stress_type"],
              item["campaign_key"], item["run_key"], item["stress_condition_key"], Json(item["stress_features"]),
              _timestamp(item["started_at"], "started_at") if item.get("started_at") else None,
              _timestamp(item["ended_at"], "ended_at") if item.get("ended_at") else None,
              item.get("identity_source", "campaign_registry"), item.get("reviewed_by"),
              item.get("identity_source", "campaign_registry"), item.get("review_reason")))
        return int(cursor.fetchone()[0])


def _result_ids(conn, batch_id: int) -> dict[str, int]:
    with conn.cursor() as cursor:
        cursor.execute("SELECT item_key, result_identity FROM iv_damage_evidence_batch_items WHERE batch_id = %s AND status = 'applied'", (batch_id,))
        return {key: int(next(iter(identity.values()))) for key, identity in cursor.fetchall() if identity}


def _apply_item(conn, manifest: Mapping[str, object], item_type: str, item: Mapping[str, object], ids: Mapping[str, int], source) -> dict[str, int]:
    if item_type == "acquisition":
        values = {key: value for key, value in item.items() if key not in {"item_key", "source_relation", "source_checksum"}}
        values["measured_at"] = _timestamp(values["measured_at"], "measured_at")
        record = register_acquisition(conn, AcquisitionSpec(**values))
        return {"acquisition_id": record.id}
    if item_type == "stress_session":
        return {"stress_session_id": _register_session(conn, item)}
    if item_type == "observation":
        config = ExtractionConfig(**dict(manifest["extraction_config"]))
        acquisition, points = load_acquisition_sweep_points(conn, item["acquisition_key"])
        context = ObservationContext(
            metadata_id=acquisition.metadata_id,
            measurement_protocol_id=acquisition.measurement_protocol_id,
            replicate_group_key=item["replicate_group_key"], measured_at=acquisition.measured_at,
            source_fingerprint={
                "source_relation": item["source_relation"], "source_checksum": item["source_checksum"],
                "source_row_ids": item.get("source_row_ids", [acquisition.metadata_id]),
                "acquisition_id": acquisition.id, "acquisition_point_payload_hash": acquisition.point_payload_hash,
                "source_provenance": source,
            },
        )
        operation = extract_and_persist_vth if config.target_type == "delta_vth_v" else extract_and_persist_rdson
        observation_id, _ = operation(conn, points=points, config=config, context=context)
        return {"observation_id": observation_id}
    values = {key: value for key, value in item.items() if key not in {"item_key", "pre_observation_keys", "post_observation_keys"}}
    values["pre_observation_ids"] = [ids[key] for key in item["pre_observation_keys"]]
    values["post_observation_ids"] = [ids[key] for key in item["post_observation_keys"]]
    response_id, _ = materialize_response_unit(conn, ResponseUnitSpec(**values))
    return {"response_unit_id": response_id}


@contextmanager
def _application_lock(conn, batch_key: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT pg_try_advisory_lock(hashtextextended(%s, 0))
            """,
            (batch_key,),
        )
        acquired = bool(cursor.fetchone()[0])
    conn.commit()
    if not acquired:
        raise EvidenceManifestError(
            f"evidence batch {batch_key!r} is already being applied"
        )
    try:
        yield
    finally:
        conn.rollback()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_advisory_unlock(hashtextextended(%s, 0))
                """,
                (batch_key,),
            )
            cursor.fetchone()
        conn.commit()


def apply_evidence(
    conn,
    *,
    batch_key: str,
    expected_plan_sha: str,
    actor: str,
    source_provenance: Mapping[str, object],
) -> dict[str, object]:
    batch_key = _batch_key(batch_key)
    expected_plan_sha = _sha256_text(
        expected_plan_sha, "expected_plan_sha"
    )
    with _application_lock(conn, batch_key):
        return _apply_evidence_locked(
            conn,
            batch_key=batch_key,
            expected_plan_sha=expected_plan_sha,
            actor=actor,
            source_provenance=source_provenance,
        )


def _apply_evidence_locked(
    conn,
    *,
    batch_key: str,
    expected_plan_sha: str,
    actor: str,
    source_provenance: Mapping[str, object],
) -> dict[str, object]:
    batch_key = _batch_key(batch_key)
    expected_plan_sha = _sha256_text(
        expected_plan_sha, "expected_plan_sha"
    )
    actor = _text(actor, "actor")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, plan_sha, manifest, status
            FROM iv_damage_evidence_batches
            WHERE batch_key = %s
            FOR UPDATE
            """,
            (batch_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise EvidenceManifestError("evidence batch is not approved")
        batch_id, plan_sha, manifest, status = (
            int(row[0]),
            row[1],
            dict(row[2]),
            row[3],
        )
        if plan_sha != expected_plan_sha:
            raise EvidenceManifestError(
                "expected plan hash does not match approved batch"
            )
        if status == "applied":
            conn.commit()
            return {
                "batch_id": batch_id,
                "status": "applied",
                "idempotent": True,
            }
        cursor.execute(
            """
            UPDATE iv_damage_evidence_batches
            SET status = 'applying', last_error = NULL
            WHERE id = %s
            """,
            (batch_id,),
        )
    conn.commit()

    active_ledger_id: int | None = None
    active_item_key: str | None = None
    try:
        validated = validate_manifest(manifest)
        if sha256(validated["manifest"]) != plan_sha:
            raise EvidenceManifestError(
                "approved manifest no longer matches its plan hash"
            )
        payloads = {
            item["item_key"]: item["payload"]
            for item in validated["items"]
        }
        item_types = {
            item["item_key"]: item["item_type"]
            for item in validated["items"]
        }
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, item_key, item_type, payload_sha, status
                FROM iv_damage_evidence_batch_items
                WHERE batch_id = %s
                ORDER BY item_order
                """,
                (batch_id,),
            )
            ledger = cursor.fetchall()
        if {row[1] for row in ledger} != set(payloads):
            raise EvidenceManifestError(
                "approved item ledger does not match the manifest"
            )

        for (
            ledger_id,
            item_key,
            item_type,
            payload_sha,
            item_status,
        ) in ledger:
            if item_status == "applied":
                continue
            active_ledger_id = int(ledger_id)
            active_item_key = item_key
            item = payloads[item_key]
            if item_types[item_key] != item_type:
                raise EvidenceManifestError(
                    f"approved item type changed for {item_key}"
                )
            if sha256(item) != payload_sha:
                raise EvidenceManifestError(
                    f"approved payload hash changed for {item_key}"
                )
            result = _apply_item(
                conn,
                manifest,
                item_type,
                item,
                _result_ids(conn, batch_id),
                source_provenance,
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE iv_damage_evidence_batch_items
                    SET status = 'applied', result_identity = %s,
                        last_error = NULL,
                        attempt_count = attempt_count + 1,
                        applied_at = clock_timestamp()
                    WHERE id = %s
                    """,
                    (Json(result), ledger_id),
                )
            conn.commit()
            active_ledger_id = None
            active_item_key = None

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*)
                FROM iv_damage_evidence_batch_items
                WHERE batch_id = %s AND status <> 'applied'
                """,
                (batch_id,),
            )
            if cursor.fetchone()[0]:
                raise EvidenceManifestError(
                    "batch cannot finalize while items remain unapplied"
                )
            cursor.execute(
                """
                UPDATE iv_damage_evidence_batches
                SET status = 'applied', applied_by = %s,
                    applied_at = clock_timestamp(), last_error = NULL
                WHERE id = %s
                """,
                (actor, batch_id),
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        message = (
            f"{active_item_key}: {exc}"
            if active_item_key is not None
            else str(exc)
        )
        with conn.cursor() as cursor:
            if active_ledger_id is not None:
                cursor.execute(
                    """
                    UPDATE iv_damage_evidence_batch_items
                    SET status = 'failed', last_error = %s,
                        attempt_count = attempt_count + 1,
                        result_identity = NULL, applied_at = NULL
                    WHERE id = %s AND status <> 'applied'
                    """,
                    (str(exc), active_ledger_id),
                )
            cursor.execute(
                """
                UPDATE iv_damage_evidence_batches
                SET status = 'failed', last_error = %s
                WHERE id = %s AND status <> 'applied'
                """,
                (message, batch_id),
            )
        conn.commit()
        raise

    return {
        "batch_id": batch_id,
        "status": "applied",
        "idempotent": False,
    }

def evidence_status(conn, batch_key: str) -> dict[str, object]:
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, batch_key, plan_sha, prepared_by, approved_by, applied_by, status, last_error, created_at, approved_at, applied_at FROM iv_damage_evidence_batches WHERE batch_key=%s", (batch_key,))
        row = cursor.fetchone()
        if row is None:
            raise EvidenceManifestError("evidence batch does not exist")
        cursor.execute("SELECT item_type, status, count(*) FROM iv_damage_evidence_batch_items WHERE batch_id=%s GROUP BY item_type,status ORDER BY item_type,status", (row[0],))
        counts = [{"item_type": item_type, "status": status, "count": count} for item_type, status, count in cursor.fetchall()]
    return {"batch_id": int(row[0]), "batch_key": row[1], "plan_sha": row[2], "prepared_by": row[3], "approved_by": row[4], "applied_by": row[5], "status": row[6], "last_error": row[7], "created_at": row[8], "approved_at": row[9], "applied_at": row[10], "item_counts": counts}
