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
    SCHEMA_DIR / "029_proxy_viz_support.sql",
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
    "stress_candidate_destruction_boundary_energy_view",
    "stress_regime_compatibility",
    "stress_proxy_candidate_energy_v2",
    "stress_proxy_combined_ranker_settings",
    "stress_proxy_candidate_combined_v3",
    "stress_proxy_concordance_enrichment_view",
    "proxy_truth_labels",
)


def required_objects_present(conn) -> list[str]:
    """Return the names of any required mechanistic-proxy objects that are missing.

    Since 2026-07-02 the settings + regime tables are created by schema/025
    (shared v1/v2 prior layer); the feature/candidate views and truth-label
    table remain schema/028's.  The apply order (025 then 028) is unchanged,
    so one missing-object list still covers both.
    """
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
            "\nmechanistic energy-proxy schema not applied (missing: "
            + ", ".join(missing)
            + ").\nRun without --validate-only to apply 025+028, e.g.\n"
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
        print(
            "  Note: compare this rank-1 status distribution before/after the "
            "ceiling fix; MIXED/unknown targets should cap again, while "
            "measured/waveform v2 statuses become reachable when evidence exists."
        )


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

        # Phase A/E: target severity and candidate own-threshold fractions are
        # exposed and coherent.  The target point ratio must lie inside the
        # widened Kosier band; the candidate point must lie inside its measured
        # electrical destruction-boundary fraction band whenever that band exists.
        cur.execute(
            """
            SELECT
                COUNT(*) AS rank1_rows,
                COUNT(*) FILTER (
                    WHERE target_severity_point_ratio IS NOT NULL
                      AND target_severity_low IS NOT NULL
                      AND target_severity_high IS NOT NULL
                      AND NOT (target_severity_point_ratio
                               BETWEEN target_severity_low AND target_severity_high)
                ) AS target_point_outside_band,
                COUNT(*) FILTER (
                    WHERE candidate_failure_fraction_point IS NOT NULL
                      AND candidate_failure_fraction_low IS NOT NULL
                      AND candidate_failure_fraction_high IS NOT NULL
                      AND NOT (candidate_failure_fraction_point
                               BETWEEN candidate_failure_fraction_low
                                   AND candidate_failure_fraction_high)
                ) AS candidate_point_outside_band,
                COUNT(*) FILTER (
                    WHERE candidate_failure_fraction_overlap_class = 'missing_interval'
                      AND NOT (energy_v2_blockers @> ARRAY[
                          'candidate_failure_fraction_missing'
                      ]::text[])
                ) AS missing_fraction_without_blocker
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
            """
        )
        band = cur.fetchone()
        _print_rows("Own-threshold fraction band coherence (all must be 0):", [band])
        if (band["target_point_outside_band"]
                or band["candidate_point_outside_band"]
                or band["missing_fraction_without_blocker"]):
            ok = False
            print("  FAIL: own-threshold severity columns or blockers are incoherent")

        cur.execute(
            """
            SELECT
                boundary_scope,
                COUNT(*) AS cells,
                COUNT(*) FILTER (WHERE boundary_usable) AS usable_cells,
                COUNT(*) FILTER (WHERE boundary_inverted) AS inverted_cells
            FROM stress_candidate_destruction_boundary_energy_view
            GROUP BY boundary_scope
            ORDER BY boundary_scope
            """
        )
        _print_rows("Candidate destruction-boundary cells:", cur.fetchall())

        cur.execute(
            """
            SELECT
                COUNT(*) AS v2_rows,
                COUNT(*) FILTER (WHERE candidate_failure_fraction_gate_usable)
                    AS usable_boundary_rows,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE candidate_failure_fraction_gate_usable)
                    / NULLIF(COUNT(*), 0),
                    1
                ) AS pct_usable_boundary_rows,
                COUNT(*) FILTER (WHERE mechanistic_energy_candidate_rank = 1)
                    AS rank1_rows,
                COUNT(*) FILTER (
                    WHERE mechanistic_energy_candidate_rank = 1
                      AND candidate_failure_fraction_gate_usable
                ) AS rank1_usable_boundary_rows,
                ROUND(
                    100.0 * COUNT(*) FILTER (
                        WHERE mechanistic_energy_candidate_rank = 1
                          AND candidate_failure_fraction_gate_usable
                    ) / NULLIF(
                        COUNT(*) FILTER (WHERE mechanistic_energy_candidate_rank = 1),
                        0
                    ),
                    1
                ) AS pct_rank1_usable_boundary_rows
            FROM stress_proxy_candidate_energy_v2
            """
        )
        _print_rows("v2 row-level own-threshold boundary coverage:", cur.fetchall())

        cur.execute(
            """
            SELECT
                candidate_mechanistic_regime,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE candidate_failure_fraction_gate_usable)
                    AS usable_boundary_rows,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE candidate_failure_fraction_gate_usable)
                    / NULLIF(COUNT(*), 0),
                    1
                ) AS pct_usable,
                COUNT(*) FILTER (WHERE mechanistic_energy_candidate_rank = 1)
                    AS rank1_rows,
                COUNT(*) FILTER (
                    WHERE mechanistic_energy_candidate_rank = 1
                      AND candidate_failure_fraction_gate_usable
                ) AS rank1_usable_rows,
                ROUND(
                    100.0 * COUNT(*) FILTER (
                        WHERE mechanistic_energy_candidate_rank = 1
                          AND candidate_failure_fraction_gate_usable
                    ) / NULLIF(
                        COUNT(*) FILTER (WHERE mechanistic_energy_candidate_rank = 1),
                        0
                    ),
                    1
                ) AS pct_rank1_usable
            FROM stress_proxy_candidate_energy_v2
            GROUP BY candidate_mechanistic_regime
            ORDER BY rows DESC
            """
        )
        _print_rows("v2 boundary coverage by candidate regime:", cur.fetchall())

        regression_checks = []
        cur.execute(
            """
            SELECT COUNT(*) AS unflagged_avalanche_rank1
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
              AND target_mechanistic_regime = 'selci_gate_coupled'
              AND candidate_source = 'avalanche'
              AND mechanistic_energy_candidate_status
                  IS DISTINCT FROM 'mechanistic_regime_mismatch'
            """
        )
        row = cur.fetchone()
        regression_checks.append({
            "name": "selci_no_unflagged_avalanche_rank1",
            "passed": (row["unflagged_avalanche_rank1"] or 0) == 0,
            **row,
        })
        cur.execute(
            """
            SELECT COUNT(*) AS avalanche_rank1_with_same_device_sc
            FROM stress_proxy_candidate_energy_v2 v2
            WHERE v2.mechanistic_energy_candidate_rank = 1
              AND v2.target_mechanistic_regime = 'proton_low_collapse_seb'
              AND v2.candidate_source = 'avalanche'
              AND EXISTS (
                  SELECT 1
                  FROM stress_proxy_candidate_ranked_view alt
                  WHERE alt.target_stress_record_key = v2.target_stress_record_key
                    AND alt.candidate_source = 'sc'
                    AND alt.match_scope = 'same_device'
              )
            """
        )
        row = cur.fetchone()
        regression_checks.append({
            "name": "proton_seb_sc_preferred_when_available",
            "passed": (row["avalanche_rank1_with_same_device_sc"] or 0) == 0,
            **row,
        })
        cur.execute(
            """
            SELECT COUNT(*) AS localization_blocked_rows
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
              AND EXISTS (
                  SELECT 1 FROM unnest(COALESCE(energy_v2_blockers, ARRAY[]::text[])) b
                  WHERE b LIKE 'localization%'
              )
            """
        )
        row = cur.fetchone()
        regression_checks.append({
            "name": "localization_never_blocks",
            "passed": (row["localization_blocked_rows"] or 0) == 0,
            **row,
        })
        cur.execute(
            """
            SELECT COUNT(*) AS first_order_measured_capped
            FROM stress_proxy_candidate_energy_v2
            WHERE regime_match_class IN ('first_order_analog', 'secondary_analog')
              AND measured_comparability_status IN ('strong', 'usable')
              AND mechanistic_energy_candidate_status = 'mechanistic_analog_questionable'
            """
        )
        row = cur.fetchone()
        regression_checks.append({
            "name": "first_order_measured_not_capped",
            "passed": (row["first_order_measured_capped"] or 0) == 0,
            **row,
        })
        # Each check carries its own detail column, so render per-row instead
        # of _print_rows (which assumes homogeneous keys across rows).
        print("\nv2 regression checks (all must pass):")
        for chk in regression_checks:
            detail = ", ".join(
                f"{k}={v}" for k, v in chk.items() if k not in ("name", "passed")
            )
            print(f"  {'PASS' if chk['passed'] else 'FAIL'} {chk['name']} ({detail})")
        if any(not r["passed"] for r in regression_checks):
            ok = False
            print("  FAIL: one or more v2 regression checks failed")

        # Phase 4: curated truth-label coverage.  These are the human-labeled
        # pairs the v2 calibrator scores against.  An empty table is the
        # expected initial state (curation is a live step) and is NOT a failure;
        # it just means the calibrator's hit-rate fails closed until seeded.
        cur.execute(
            """
            SELECT
                label,
                label_basis,
                COUNT(*) AS rows,
                COUNT(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1 FROM stress_proxy_candidate_energy_v2 v2
                        WHERE v2.target_stress_record_key = t.target_stress_record_key
                          AND v2.candidate_stress_record_key = t.candidate_stress_record_key
                    )
                ) AS resolve_to_v2_pair
            FROM proxy_truth_labels t
            GROUP BY label, label_basis
            ORDER BY rows DESC
            """
        )
        truth_rows = cur.fetchall()
        _print_rows("Phase 4 curated truth-label coverage:", truth_rows)
        if not truth_rows:
            print("  (no curated truth labels yet — calibrator hit-rate will fail closed)")
        else:
            unresolved = sum(
                r["rows"] - r["resolve_to_v2_pair"] for r in truth_rows
            )
            if unresolved:
                print(
                    f"  NOTE: {unresolved} labeled pair(s) do not resolve to a v2"
                    " candidate row (target/candidate key not in the top-10 pool"
                    " or stale key)."
                )

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
