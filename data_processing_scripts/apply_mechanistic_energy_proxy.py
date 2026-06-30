#!/usr/bin/env python3
"""
Apply schema/028 and run the Phase-1 mechanistic energy-proxy checks.

Phase 1 is feature-correctness only: this creates
``stress_energy_equivalence_settings`` and the ``stress_energy_equivalence_features``
view, then verifies population and the target/candidate severity separation
invariant.  It does NOT touch the v1 ``stress_proxy_candidate_view`` and does
NOT build any dashboard.

``stress_energy_equivalence_features`` depends on ``stress_test_context_view``
(schema/025), which must already be live.  Re-run create_proxy_readiness_dashboard.py
or extract_single_event_effects.py first if 025 is stale.

Usage:
    python3 data_processing_scripts/apply_mechanistic_energy_proxy.py
    python3 data_processing_scripts/apply_mechanistic_energy_proxy.py --validate-only
"""

from __future__ import annotations

import argparse
from pathlib import Path

from psycopg2.extras import RealDictCursor

try:
    from db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - package import path
    from data_processing_scripts.db_config import get_connection

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schema"
# 025 is re-applied first because it now defines stress_proxy_candidate_ranked_view,
# which 028's v2 view reads from.  Both are idempotent.  Assumes the upstream
# pipeline schemas (022/027) and the damage_equivalence_* views are already live.
SCHEMA_PATHS = (
    SCHEMA_DIR / "025_proxy_readiness_waveforms.sql",
    SCHEMA_DIR / "028_mechanistic_energy_proxy.sql",
)


def apply_schema(conn) -> None:
    with conn.cursor() as cur:
        for path in SCHEMA_PATHS:
            cur.execute(path.read_text())
            print(f"Applied {path.name}")
    conn.commit()


def _print_rows(title, rows):
    print(f"\n{title}")
    if not rows:
        print("  (no rows)")
        return
    headers = list(rows[0].keys())
    print("  " + " | ".join(headers))
    for row in rows:
        print("  " + " | ".join(str(row[h]) for h in headers))


REQUIRED_OBJECTS = (
    "stress_energy_equivalence_settings",
    "stress_energy_equivalence_features",
    "stress_regime_compatibility",
    "stress_proxy_candidate_energy_v2",
)


def required_objects_present(conn) -> list[str]:
    """Return the names of any required schema-028 objects that are missing."""
    with conn.cursor() as cur:
        missing = []
        for name in REQUIRED_OBJECTS:
            cur.execute("SELECT to_regclass(%s)", (name,))
            if cur.fetchone()[0] is None:
                missing.append(name)
        return missing


def validate(conn) -> bool:
    missing = required_objects_present(conn)
    if missing:
        print(
            "\nschema 028 not applied (missing: "
            + ", ".join(missing)
            + ").\nRun without --validate-only to apply it, e.g.\n"
            "    python3 data_processing_scripts/apply_mechanistic_energy_proxy.py"
        )
        return False
    ok = True
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                source,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE mechanistic_regime IS NOT NULL) AS with_regime,
                COUNT(*) FILTER (WHERE terminal_areal_energy_bulk_j_cm2 IS NOT NULL)
                    AS with_terminal_areal_energy,
                COUNT(*) FILTER (WHERE track_core_energy_density_j_cm3 IS NOT NULL)
                    AS with_track_core_density,
                COUNT(*) FILTER (WHERE se_depletion_ratio_to_seb IS NOT NULL)
                    AS with_seb_ratio
            FROM stress_energy_equivalence_features
            GROUP BY source
            ORDER BY source
            """
        )
        _print_rows("Coverage by source:", cur.fetchall())

        # Severity separation invariant: candidates never carry a stored-field
        # depletion ratio; targets never carry a candidate terminal ratio.
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE source IN ('sc', 'avalanche')
                      AND se_depletion_ratio_to_seb IS NOT NULL
                ) AS candidate_with_depletion_ratio,
                COUNT(*) FILTER (
                    WHERE source = 'irradiation'
                      AND terminal_ratio_to_seb_critical IS NOT NULL
                ) AS target_with_terminal_ratio
            FROM stress_energy_equivalence_features
            """
        )
        sep = cur.fetchone()
        _print_rows("Severity separation invariant (both must be 0):", [sep])
        if sep["candidate_with_depletion_ratio"] or sep["target_with_terminal_ratio"]:
            ok = False
            print("  FAIL: target/candidate severity columns are leaking across sources")

        cur.execute(
            """
            SELECT source, mechanistic_regime, COUNT(*) AS rows
            FROM stress_energy_equivalence_features
            GROUP BY source, mechanistic_regime
            ORDER BY source, rows DESC
            """
        )
        _print_rows("Mechanistic regime distribution:", cur.fetchall())

        # Proton SEB must split by measured collapse, not collapse into one label.
        cur.execute(
            """
            SELECT mechanistic_regime, COUNT(*) AS rows
            FROM stress_energy_equivalence_features
            WHERE source = 'irradiation'
              AND UPPER(COALESCE(event_type, '')) = 'SEB'
              AND (LOWER(COALESCE(ion_species, '')) LIKE '%proton%'
                   OR LOWER(TRIM(COALESCE(ion_species, ''))) IN ('p','h','h+'))
            GROUP BY mechanistic_regime
            ORDER BY rows DESC
            """
        )
        _print_rows("Proton SEB regime split:", cur.fetchall())

        # Phase 2: regime-compatibility priors are seeded and every target has
        # an 'any' fallback so no (target, candidate) pair is unresolved.
        cur.execute(
            """
            SELECT
                COUNT(*) AS rules,
                COUNT(DISTINCT target_regime) AS target_regimes,
                COUNT(*) FILTER (WHERE candidate_regime = 'any') AS any_fallbacks
            FROM stress_regime_compatibility
            """
        )
        compat = cur.fetchone()
        _print_rows("Regime-compatibility priors (Phase 2):", [compat])
        if compat["any_fallbacks"] != compat["target_regimes"]:
            ok = False
            print("  FAIL: some target regime lacks an 'any' fallback rule")

        # Phase 3: v2 candidate screening.
        cur.execute(
            """
            SELECT mechanistic_energy_candidate_status AS status, COUNT(*) AS rows
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
            GROUP BY mechanistic_energy_candidate_status
            ORDER BY rows DESC
            """
        )
        _print_rows("v2 rank-1 status distribution (Phase 3):", cur.fetchall())

        # Is the cross-device dominance a coverage gap (targets with no
        # same-device electrical candidate) rather than a ranking artifact?
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT target_stress_record_key) AS targets,
                COUNT(DISTINCT target_stress_record_key)
                    FILTER (WHERE match_scope = 'same_device') AS with_same_device_candidate,
                COUNT(DISTINCT target_stress_record_key)
                    FILTER (WHERE match_scope = 'same_device') * 100
                    / NULLIF(COUNT(DISTINCT target_stress_record_key), 0) AS pct_same_device
            FROM stress_proxy_candidate_energy_v2
            """
        )
        _print_rows("Same-device candidate coverage (cross-device gap check):", cur.fetchall())

        cur.execute(
            """
            SELECT match_scope, COUNT(*) AS rows
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
            GROUP BY match_scope
            ORDER BY rows DESC
            """
        )
        _print_rows("v2 rank-1 by match scope:", cur.fetchall())

        # Where does v2 pick a different candidate source than v1 at rank 1?
        cur.execute(
            """
            SELECT
                v2.target_event_type,
                v2.target_mechanistic_regime,
                v1.candidate_source AS v1_source,
                v2.candidate_source AS v2_source,
                COUNT(*) AS rows
            FROM stress_proxy_candidate_view v1
            JOIN stress_proxy_candidate_energy_v2 v2
              ON v2.target_stress_record_key = v1.target_stress_record_key
             AND v2.mechanistic_energy_candidate_rank = 1
            WHERE v1.candidate_rank = 1
              AND v1.candidate_source IS DISTINCT FROM v2.candidate_source
            GROUP BY 1, 2, 3, 4
            ORDER BY rows DESC
            LIMIT 20
            """
        )
        _print_rows("v1 -> v2 rank-1 source shifts:", cur.fetchall())

    print("\nPhase 1-3 checks:", "PASS" if ok else "FAIL")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--validate-only", action="store_true",
                    help="Skip applying schema/028; only run the checks.")
    args = ap.parse_args()

    conn = get_connection()
    try:
        if not args.validate_only:
            apply_schema(conn)
        ok = validate(conn)
    finally:
        conn.close()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
