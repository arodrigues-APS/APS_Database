"""Persistence and immutable artifacts for retrospective IV research."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from uuid import uuid4

import joblib

from aps.ml.iv_damage_research_contracts import ArtifactIdentity


class ResearchOperationError(RuntimeError):
    """A research mutation conflicts with frozen evidence."""


REQUIRED_RELATIONS = (
    "iv_damage_research_snapshots",
    "iv_damage_research_curve_pairs",
    "iv_damage_research_model_runs",
    "iv_damage_research_scalar_predictions",
    "iv_damage_research_curve_predictions",
)


def require_schema(conn) -> None:
    cursor = conn.cursor()
    try:
        missing = []
        for relation in REQUIRED_RELATIONS:
            cursor.execute("SELECT to_regclass(%s)", (f"public.{relation}",))
            if cursor.fetchone()[0] is None:
                missing.append(relation)
        if missing:
            raise ResearchOperationError(
                "research schema is incomplete; apply migration 046 (missing: " + ", ".join(missing) + ")"
            )
    finally:
        cursor.close()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_contained_path(path: Path, root: Path) -> Path:
    destination = path.resolve()
    resolved_root = root.resolve()
    if destination == resolved_root or not destination.is_relative_to(resolved_root):
        raise ResearchOperationError(f"artifact path must be a file below configured root {resolved_root}")
    return destination


def save_artifact_immutable(payload: object, path: Path, *, root: Path) -> ArtifactIdentity:
    destination = require_contained_path(path, root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        joblib.dump(
            {"format_version": "iv-damage-research-artifact-v1", "payload": payload},
            temporary,
            compress=3,
        )
        checksum = sha256_file(temporary)
        try:
            os.link(temporary, destination)
        except FileExistsError:
            existing_checksum = sha256_file(destination)
            if existing_checksum != checksum:
                raise ResearchOperationError(
                    f"research artifact replay conflicts with immutable content: {destination}"
                )
    finally:
        if temporary.exists():
            temporary.unlink()
    return ArtifactIdentity(destination, checksum)


def load_artifact_verified(identity: ArtifactIdentity, *, root: Path) -> object:
    path = require_contained_path(identity.path, root)
    if not path.is_file() or sha256_file(path) != identity.checksum:
        raise ResearchOperationError("research artifact is missing or checksum verification failed")
    record = joblib.load(path)
    if not isinstance(record, dict) or record.get("format_version") != "iv-damage-research-artifact-v1":
        raise ResearchOperationError("unsupported research artifact format")
    return record["payload"]


def status(conn) -> dict[str, object]:
    require_schema(conn)
    cursor = conn.cursor()
    try:
        counts = {}
        for relation in REQUIRED_RELATIONS:
            cursor.execute(f"SELECT count(*) FROM {relation}")
            counts[relation] = int(cursor.fetchone()[0])
        cursor.execute("SELECT count(*) FROM iv_damage_decision_eligible_prediction_view")
        counts["certified_decision_eligible_predictions"] = int(cursor.fetchone()[0])
        return {
            "claim_class": "retrospective_research",
            "decision_eligible": False,
            "counts": counts,
        }
    finally:
        cursor.close()
