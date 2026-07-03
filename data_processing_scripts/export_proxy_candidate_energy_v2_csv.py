#!/usr/bin/env python3
"""Export stress_proxy_candidate_energy_v2 to CSV for the interactive viewer.

The Phase-5 ``v2 severity overlap`` tab in
create_interactive_damage_signature_viewer.py reads this CSV.  The viewer treats
it as optional, so this is a separate live-DB step: run it after applying
schema/028 with the Phase-5 band columns, then rebuild the viewer offline.

Array columns (energy_v2_blockers / energy_v2_notes / proxy claim blockers)
are flattened to a ``"; "``-joined string so the CSV round-trips cleanly into
the Plotly hover.

Usage:
    python3 data_processing_scripts/export_proxy_candidate_energy_v2_csv.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from psycopg2.extras import RealDictCursor

try:
    from db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - package import path
    from data_processing_scripts.db_config import get_connection

OUT_DIR = Path(__file__).resolve().parent.parent / "out" / "avalanche_irrad_pilot"
OUTPUT_CSV = OUT_DIR / "proxy_candidate_energy_v2.csv"

# Only the columns the viewer's v2 payload consumes (keeps the CSV small).
COLUMNS = [
    "target_stress_record_key",
    "candidate_stress_record_key",
    "device_type",
    "target_event_type",
    "target_ion_species",
    "candidate_source",
    "match_scope",
    "target_mechanistic_regime",
    "candidate_mechanistic_regime",
    "regime_match_class",
    "candidate_rank_v1",
    "candidate_status_v1",
    "proxy_claim_status_v1",
    "proxy_claim_basis_v1",
    "proxy_claim_blockers_v1",
    "proxy_claim_summary_v1",
    "decision_safe_rank_v1",
    "signature_claim_quality_v1",
    "target_energy_comparability_class",
    "candidate_energy_comparability_class",
    "mechanistic_energy_candidate_rank",
    "mechanistic_energy_candidate_status",
    "proxy_claim_status",
    "proxy_claim_basis",
    "proxy_claim_blockers",
    "proxy_claim_summary",
    "truth_validation_status",
    "truth_label",
    "truth_label_basis",
    "truth_reviewer",
    "truth_review_date",
    "candidate_failure_fraction_overlap_class",
    "critical_severity_overlap_class_kosier_context",
    "terminal_energy_overlap_class",
    "timescale_overlap_class",
    "power_rate_overlap_class",
    "cumulative_exposure_overlap_class",
    "localization_mismatch_log10",
    "target_severity_low",
    "target_severity_high",
    "target_severity_point_ratio",
    "candidate_failure_fraction_low",
    "candidate_failure_fraction_point",
    "candidate_failure_fraction_high",
    "candidate_failure_fraction_basis",
    "candidate_failure_fraction_gate_usable",
    "candidate_severity_low_kosier_context",
    "candidate_severity_high_kosier_context",
    "candidate_severity_point_ratio_kosier_context",
    "damage_evidence_class",
    "measured_sign_mismatch_axis_count",
    "prediction_sign_mismatch_axis_count",
    "energy_v2_blockers",
    "energy_v2_notes",
]
ARRAY_COLUMNS = (
    "energy_v2_blockers",
    "energy_v2_notes",
    "proxy_claim_blockers",
    "proxy_claim_blockers_v1",
)


def _flatten_array(value: object) -> str:
    """Postgres text[] (psycopg2 -> list) -> '; '-joined string ('' for NULL)."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def export(conn, out_path: Path = OUTPUT_CSV, top_n: int = 3) -> Path:
    """Write the top-``top_n`` v2 candidates per target to ``out_path``."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT {', '.join(COLUMNS)} "
            "FROM stress_proxy_candidate_energy_v2 "
            "WHERE mechanistic_energy_candidate_rank <= %s "
            "ORDER BY device_type, target_stress_record_key, "
            "mechanistic_energy_candidate_rank",
            (top_n,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows, columns=COLUMNS)
    for col in ARRAY_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_flatten_array)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=OUTPUT_CSV)
    ap.add_argument("--top-n", type=int, default=3,
                    help="Keep the top-N v2 candidates per target (default 3).")
    args = ap.parse_args()
    with get_connection() as conn:
        path = export(conn, out_path=args.out, top_n=args.top_n)
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
