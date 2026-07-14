#!/usr/bin/env python3
"""Export the complete v1/v2/v3 winner union used by the interactive viewer.

One row represents a unique (irradiation target, candidate) pair selected as
rank 1 by at least one official method.  Every row carries the candidate's rank
and denominator under each method when that rank is genuinely available.
Candidates outside v2's materialized top-10 (and therefore outside official
v3) remain present with explicit eligibility/availability reasons.

The exporter adds immutable contract/build provenance.  It never invents truth
labels or candidate destruction boundaries: absent evidence stays absent.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from psycopg2.extras import RealDictCursor

from aps.db_config import get_connection
from aps.paths import OUT_ROOT, REPO_ROOT
from aps.provenance import collect_source_provenance

OUT_DIR = OUT_ROOT / "avalanche_irrad_pilot"
OUTPUT_CSV = OUT_DIR / "proxy_method_comparison_union.csv"
CONTRACT_VERSION = "aps-proxy-method-comparison-v1"

ARRAY_COLUMNS = (
    "v1_proxy_claim_blockers",
    "v2_proxy_claim_blockers",
)

QUERY = """
    SELECT *
    FROM stress_proxy_method_comparison_union_view
    ORDER BY device_type, target_stress_record_key,
             picked_by_v1 DESC, picked_by_v2 DESC, picked_by_v3 DESC,
             candidate_stress_record_key
"""


def _flatten_array(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(item) for item in value)
    return str(value)


def _validate(frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    pair = ["target_stress_record_key", "candidate_stress_record_key"]
    if frame.duplicated(pair).any():
        raise RuntimeError("comparison contract contains duplicate target/candidate rows")

    for method in ("v1", "v2", "v3"):
        picked = frame[f"picked_by_{method}"].fillna(False).astype(bool)
        winner_key = frame[f"{method}_winner_key"].fillna("").astype(str)
        by_target = frame.assign(_picked=picked, _has=winner_key.ne("")) \
            .groupby("target_stress_record_key", dropna=False)[["_picked", "_has"]].sum()
        expected = by_target["_has"].gt(0)
        bad = by_target.loc[expected & by_target["_picked"].ne(1)]
        if not bad.empty:
            raise RuntimeError(f"{method} winner-union invariant failed for {len(bad)} targets")

        pct_col = f"{method}_rank_percentile"
        pct = pd.to_numeric(frame[pct_col], errors="coerce").dropna()
        if ((pct <= 0.0) | (pct > 100.0)).any():
            raise RuntimeError(f"{method} rank percentile is outside (0, 100]")

    v3 = frame.loc[frame["v3_rank_available"].fillna(False).astype(bool)].copy()
    if not v3.empty:
        total = pd.to_numeric(v3["v3_component_weighted_sq_total"], errors="coerce")
        distance = pd.to_numeric(v3["v3_combined_vector_distance"], errors="coerce")
        mismatch = (total - distance.pow(2)).abs()
        if mismatch.gt(1e-9).any():
            raise RuntimeError(
                "v3 component invariant failed: weighted component sum does not "
                "match combined_vector_distance^2"
            )


def export(
    conn,
    out_path: Path = OUTPUT_CSV,
    *,
    generated_at: datetime | None = None,
    git_revision: str | None = None,
    git_dirty: bool | None = None,
    source_fingerprint: str | None = None,
    git_available: bool | None = None,
) -> Path:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY)
        rows = [dict(row) for row in cur.fetchall()]
    frame = pd.DataFrame(rows)
    _validate(frame)

    for column in ARRAY_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].apply(_flatten_array)

    source = collect_source_provenance(REPO_ROOT)
    if git_revision is None:
        git_revision = source.code_sha
    if git_dirty is None:
        git_dirty = source.dirty
    if source_fingerprint is None:
        source_fingerprint = source.fingerprint
    if git_available is None:
        git_available = source.git_available
    timestamp = generated_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    frame.insert(0, "comparison_contract_version", CONTRACT_VERSION)
    frame.insert(1, "export_generated_at_utc", timestamp.astimezone(timezone.utc).isoformat())
    frame.insert(2, "source_git_revision", git_revision or "unknown")
    frame.insert(3, "source_git_dirty", git_dirty)
    frame.insert(4, "source_fingerprint", source_fingerprint)
    frame.insert(5, "source_git_available", git_available)
    frame.insert(6, "v3_official_scope", "screening-only rerank of v2 top-10")
    frame.insert(7, "evidence_policy", "missing boundary/truth remains missing; no synthetic labels")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=OUTPUT_CSV)
    args = parser.parse_args()
    with get_connection() as conn:
        path = export(conn, out_path=args.out)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
