#!/usr/bin/env python3
"""Derive a prioritized truth-label curation shortlist from the queue CSV.

Pure CSV -> CSV: reads the export_proxy_truth_curation_queue.py output and
narrows it to the rows most worth a human's first pass. Never touches the
database, so this cannot write proxy_truth_labels -- it only helps a curator
decide what to look at first.

Usage:
    python3 data_processing_scripts/export_proxy_truth_curation_shortlist.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

OUT_DIR = Path(__file__).resolve().parent.parent / "out" / "mechanistic_energy_proxy_calibration"
QUEUE_CSV = OUT_DIR / "proxy_truth_curation_queue.csv"
OUTPUT_CSV = OUT_DIR / "proxy_truth_curation_shortlist.csv"

# Reasons worth a human's first pass: strong automatic evidence with no truth
# label yet, or a same-device measured-damage anchor not already caught above.
SHORTLIST_REASONS = (
    "needs_truth_label_for_claim_candidate",
    "same_device_measured_damage_anchor",
)

SHORTLIST_COLUMNS = [
    "curation_priority",
    "curation_priority_reason",
    "suggested_label_basis",
    "target_stress_record_key",
    "candidate_stress_record_key",
    "device_type",
    "target_event_type",
    "candidate_source",
    "match_scope",
    "damage_evidence_class",
    "critical_severity_overlap_class",
    "terminal_energy_overlap_class",
    "measured_sign_mismatch_axis_count",
    "prediction_sign_mismatch_axis_count",
    "signature_claim_quality_v1",
    "proxy_claim_status_v1",
    "proxy_claim_status",
]


def build_shortlist(queue: pd.DataFrame) -> pd.DataFrame:
    """Filter to the priority reasons above, same-device rows surfaced first."""
    shortlist = queue[queue["curation_priority_reason"].isin(SHORTLIST_REASONS)].copy()
    shortlist["_same_device_first"] = (shortlist["match_scope"] != "same_device").astype(int)
    shortlist = shortlist.sort_values(
        by=["_same_device_first", "curation_priority", "device_type",
            "target_stress_record_key"],
        kind="stable",
    )
    columns = [c for c in SHORTLIST_COLUMNS if c in shortlist.columns]
    return shortlist[columns].reset_index(drop=True)


def export(queue_path: Path = QUEUE_CSV, out_path: Path = OUTPUT_CSV) -> Path:
    if not queue_path.exists():
        raise SystemExit(
            f"{queue_path} not found. Run "
            "export_proxy_truth_curation_queue.py first."
        )
    queue = pd.read_csv(queue_path)
    shortlist = build_shortlist(queue)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shortlist.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--queue", type=Path, default=QUEUE_CSV)
    ap.add_argument("--out", type=Path, default=OUTPUT_CSV)
    args = ap.parse_args()
    path = export(queue_path=args.queue, out_path=args.out)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
