#!/usr/bin/env python3
"""Audit / calibrate the v2 mechanistic energy proxy ranking.

This is the v2 companion to ``calibrate_proxy_distance.py``.  It is deliberately
**not** a grid search: v2 is a staged-status, per-axis-overlap ranking
(``stress_proxy_candidate_energy_v2``), not a weighted Euclidean distance, so
there are no continuous weights to tune.  Tuning a high-dimensional score
against the sparse auto-derived rank-1 output is the explicitly prohibited
outcome (it looks precise and is not falsifiable).

Instead this harness is **read-only** and answers two questions:

1. *Does v2 change proxy choices in interpretable ways?*  It emits the rank-shift,
   status-transition, proton-SEB split, SELC-I/II coverage, same-device
   coverage, localization-context, and top-blocker tables from the Phase-4
   validation manifest.
2. *Does v2 retrieve the curated truth pairs?*  It scores v2's top-1 / top-3 /
   not-blocked rates against ``proxy_truth_labels``.  When no curated labels
   exist (the expected initial state) the truth-hit metrics **fail closed** with
   ``no curated truth labels`` rather than borrowing the sparse auto-truth set.

The DB loaders are isolated from the pure metric/render functions so the latter
are unit-testable offline (see tests/test_mechanistic_energy_calibration.py).

Usage:
    python3 data_processing_scripts/calibrate_mechanistic_energy_proxy.py
    python3 data_processing_scripts/calibrate_mechanistic_energy_proxy.py --out-dir /tmp/cal
"""

from __future__ import annotations

import argparse
import json
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
OUT_DIR = REPO_ROOT / "out" / "mechanistic_energy_proxy_calibration"


# --------------------------------------------------------------------------- #
# Pure metric helpers (no DB) — unit-tested offline.
# --------------------------------------------------------------------------- #

def rate(numerator: int, denominator: int) -> float | None:
    """Hit fraction, or None when there is nothing to divide by (fail closed)."""
    if denominator <= 0:
        return None
    return numerator / denominator


def _has_blocker(blockers: Any) -> bool:
    """True when a v2 row carries any blocker (text[] -> list, or NULL/empty)."""
    if not blockers:
        return False
    return len(blockers) > 0


# Labels seeded by script from damage_equivalence_match_view strong/usable rows
# are QUARANTINED from the headline truth-hit rates: both rankers already sort
# measured-damage matches first, so scoring auto-seeded labels there would be
# self-confirming (~100% top-1 by construction).  They remain useful for the
# miss decomposition and not_equivalent violations and are reported in their
# own group.  Contract: the auto-seeder writes label_basis =
# 'measured_post_iv_auto'; the reviewer sentinel is defense-in-depth in case a
# seeder ever writes the human basis by mistake.
AUTO_SEEDED_LABEL_BASES = {"measured_post_iv_auto"}
AUTO_SEED_REVIEWER_SENTINEL = "auto_seed"


def is_auto_seeded_label(row: dict[str, Any]) -> bool:
    """True when a truth-label row was seeded by script rather than curated."""
    if row.get("label_basis") in AUTO_SEEDED_LABEL_BASES:
        return True
    return (row.get("reviewer") or "").strip().lower() == AUTO_SEED_REVIEWER_SENTINEL


def compute_truth_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Score curated truth labels against v2's ranking.

    ``rows`` is one row per curated label, each with ``label`` and the v2 rank
    of that exact (target, candidate) pair (``v2_rank``, NULL when the pair is
    outside v2's top-10 pool) plus its ``blockers``.

    - ``equivalent`` labels assert the candidate *should* rank high: they drive
      the top-1 / top-3 / not-blocked hit rates.
    - ``not_equivalent`` labels assert the candidate should *not* win: a rank-1
      appearance is a violation (false positive).
    - ``uncertain`` labels are counted but never scored.

    Every rate is None when its denominator is zero, so an empty (or
    all-uncertain) label set fails closed instead of reporting a fake 0% or
    100%.
    """
    equivalent = [r for r in rows if r.get("label") == "equivalent"]
    not_equivalent = [r for r in rows if r.get("label") == "not_equivalent"]
    uncertain = [r for r in rows if r.get("label") == "uncertain"]

    evaluable = len(equivalent)
    top1_hits = sum(1 for r in equivalent if r.get("v2_rank") == 1)
    top3_hits = sum(
        1 for r in equivalent
        if r.get("v2_rank") is not None and r["v2_rank"] <= 3
    )
    not_blocked_hits = sum(
        1 for r in equivalent
        if r.get("v2_rank") is not None and not _has_blocker(r.get("blockers"))
    )

    # Split equivalent misses (no v2 rank in the top-10 view) by whether the
    # pair is in the candidate pool at all.  out_of_top10 is a ranking issue;
    # not_in_pool is a data-coverage gap (the candidate is not a candidate for
    # that target); unknown_pool means in_candidate_pool was not supplied.
    eq_misses = [r for r in equivalent if r.get("v2_rank") is None]
    miss_not_in_pool = sum(1 for r in eq_misses if r.get("in_candidate_pool") is False)
    miss_out_of_top10 = sum(1 for r in eq_misses if r.get("in_candidate_pool") is True)
    miss_unknown_pool = sum(1 for r in eq_misses if r.get("in_candidate_pool") is None)

    ne_evaluable = len(not_equivalent)
    ne_rank1_violations = sum(1 for r in not_equivalent if r.get("v2_rank") == 1)

    return {
        "labels_total": len(rows),
        "equivalent_labels": evaluable,
        "not_equivalent_labels": ne_evaluable,
        "uncertain_labels": len(uncertain),
        "top1_hits": top1_hits,
        "top1_rate": rate(top1_hits, evaluable),
        "top3_hits": top3_hits,
        "top3_rate": rate(top3_hits, evaluable),
        "not_blocked_hits": not_blocked_hits,
        "not_blocked_rate": rate(not_blocked_hits, evaluable),
        "miss_not_in_pool": miss_not_in_pool,
        "miss_out_of_top10": miss_out_of_top10,
        "miss_unknown_pool": miss_unknown_pool,
        "not_equivalent_rank1_violations": ne_rank1_violations,
        "not_equivalent_rank1_rate": rate(ne_rank1_violations, ne_evaluable),
        "fail_closed": evaluable == 0,
    }


def compute_truth_metrics_by_basis(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Truth metrics for the curated set, per ``label_basis``, and quarantined.

    A hit on a ``measured_post_iv`` label is empirical-anchor evidence; a
    ``pilot`` hit is weak.  Reporting them separately keeps the strength of the
    evidence visible instead of averaging it away.

    Auto-seeded labels (``is_auto_seeded_label``) are QUARANTINED: the ``all``
    headline group and the per-basis groups cover human-curated rows only, and
    auto rows land in a single ``auto_seeded`` group.  Rationale: v1 and v2
    already rank measured-damage matches first, so an auto-seeded label derived
    from those same matches sits at rank-1 by construction — scoring it in the
    headline would convert fail-closed into fake-open.  A label set that is
    ONLY auto-seeded therefore still fails closed.  The ``all`` key always
    exists; per-basis keys appear only for bases actually present;
    ``auto_seeded`` appears only when auto rows exist.
    """
    curated = [r for r in rows if not is_auto_seeded_label(r)]
    auto = [r for r in rows if is_auto_seeded_label(r)]
    out = {"all": compute_truth_metrics(curated)}
    for basis in sorted({r.get("label_basis") for r in curated if r.get("label_basis")}):
        out[basis] = compute_truth_metrics(
            [r for r in curated if r.get("label_basis") == basis]
        )
    if auto:
        out["auto_seeded"] = compute_truth_metrics(auto)
    return out


def fmt_rate(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


# --------------------------------------------------------------------------- #
# Report rendering (pure) — unit-tested offline.
# --------------------------------------------------------------------------- #

def render_table(headers: list[str], rows: list[dict[str, Any]]) -> str:
    """Render rows as a GitHub-flavored markdown table (or a no-rows note)."""
    if not rows:
        return "_(no rows)_"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append(
            "| " + " | ".join(_cell(row.get(h)) for h in headers) + " |"
        )
    return "\n".join(lines)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value) if value else ""
    return str(value)


def render_regression_checks(regression_checks: list[dict[str, Any]]) -> list[str]:
    """Render the §4.3 invariant checks as a PASS/FAIL list."""
    out = ["## Regression checks (§4.3 invariants)", ""]
    if not regression_checks:
        return out + ["_(not evaluated — no DB connection)_", ""]
    for chk in regression_checks:
        status = "PASS" if chk["passed"] else "FAIL"
        detail = ", ".join(
            f"{k}={v}" for k, v in chk.items() if k not in ("name", "passed")
        )
        out.append(f"- **{status}** `{chk['name']}` ({detail})")
    return out + [""]


def render_report(sections: dict[str, list[dict[str, Any]]],
                  truth_by_basis: dict[str, dict[str, Any]],
                  regression_checks: list[dict[str, Any]],
                  generated_at: str,
                  concordance: dict[str, Any] | None = None) -> str:
    """Assemble the full markdown audit report from the manifest sections."""
    truth_metrics = truth_by_basis["all"]
    out: list[str] = [
        "# Mechanistic energy proxy (v2) calibration audit",
        "",
        f"Generated: {generated_at}",
        "",
        "Read-only audit of `stress_proxy_candidate_energy_v2`. v2 is a staged",
        "ranking, not a fitted score, so this harness reports interpretability",
        "and truth-retrieval, not tuned weights.",
        "",
    ]
    out += render_regression_checks(regression_checks)
    if concordance is not None:
        out += render_concordance(concordance)
    out += ["## Truth-hit rate (curated `proxy_truth_labels`)", ""]
    if truth_metrics["fail_closed"]:
        out += [
            "**No curated truth labels — failing closed.** Seed real",
            "(target, candidate) pairs into `proxy_truth_labels` before reading",
            "any hit rate. The auto-truth set is intentionally not substituted.",
            "",
        ]
    out += [
        f"- equivalent labels scored: {truth_metrics['equivalent_labels']}",
        f"- top-1: {fmt_rate(truth_metrics['top1_rate'])} "
        f"({truth_metrics['top1_hits']} hits)",
        f"- top-3: {fmt_rate(truth_metrics['top3_rate'])} "
        f"({truth_metrics['top3_hits']} hits)",
        f"- not-blocked: {fmt_rate(truth_metrics['not_blocked_rate'])} "
        f"({truth_metrics['not_blocked_hits']} hits)",
        f"- equivalent misses: out-of-top-10={truth_metrics['miss_out_of_top10']}, "
        f"not-in-pool={truth_metrics['miss_not_in_pool']}, "
        f"unknown-pool={truth_metrics['miss_unknown_pool']}",
        f"- not_equivalent rank-1 violations: "
        f"{fmt_rate(truth_metrics['not_equivalent_rank1_rate'])} "
        f"({truth_metrics['not_equivalent_rank1_violations']} of "
        f"{truth_metrics['not_equivalent_labels']})",
        f"- uncertain labels (unscored): {truth_metrics['uncertain_labels']}",
        "",
    ]
    auto_metrics = truth_by_basis.get("auto_seeded")
    if auto_metrics:
        out += [
            f"- auto-seeded labels (quarantined): {auto_metrics['labels_total']} — "
            "excluded from the headline rates above because both rankers already "
            "sort measured-damage matches first, so scoring script-seeded copies "
            "of those matches would be self-confirming. Their value is the miss "
            "decomposition and not_equivalent violations; see the by-basis table.",
            "",
        ]

    basis_keys = [k for k in truth_by_basis if k != "all"]
    if basis_keys:
        basis_rows = [
            {
                "label_basis": b,
                "equivalent": truth_by_basis[b]["equivalent_labels"],
                "top1": fmt_rate(truth_by_basis[b]["top1_rate"]),
                "top3": fmt_rate(truth_by_basis[b]["top3_rate"]),
                "not_blocked": fmt_rate(truth_by_basis[b]["not_blocked_rate"]),
            }
            for b in basis_keys
        ]
        out += [
            "### By label basis", "",
            render_table(
                ["label_basis", "equivalent", "top1", "top3", "not_blocked"],
                basis_rows,
            ),
            "",
        ]

    titles = {
        "rank_shifts": ("v1 -> v2 rank-1 source shifts",
                        ["target_event_type", "target_mechanistic_regime",
                         "v1_source", "v2_source", "rows"]),
        "status_transitions": ("v1 -> v2 rank-1 status transitions",
                               ["candidate_status_v1",
                                "mechanistic_energy_candidate_status", "rows"]),
        "proton_seb_split": ("Proton SEB rank-1 split",
                             ["target_mechanistic_regime", "v2_source", "rows"]),
        "selci_reconfirmation": ("SELC-I rank-1 re-confirmation",
                                 ["v2_source", "mechanistic_energy_candidate_status",
                                  "rows"]),
        "selcii_coverage": ("SELC-II cumulative coverage",
                            ["candidate_mechanistic_regime",
                             "mechanistic_energy_candidate_status", "rows"]),
        "same_device_coverage": ("Same-device coverage (rank-1 by match scope)",
                                 ["match_scope", "rows"]),
        "localization_context": ("Localization mismatch context (rank-1)",
                                 ["localization_mismatch_class", "rows",
                                  "incorrectly_blocked_rows"]),
        "top_blockers": ("Top v2 rank-1 blockers",
                         ["blocker", "rows"]),
    }
    for key, (title, headers) in titles.items():
        out += [f"## {title}", "", render_table(headers, sections.get(key, [])), ""]
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# DB loaders (read-only).  Each returns plain dict rows.
# --------------------------------------------------------------------------- #

def _fetch(conn, sql: str) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]


def load_rank_shifts(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
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
    """)


def load_status_transitions(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
        SELECT
            candidate_status_v1,
            mechanistic_energy_candidate_status,
            COUNT(*) AS rows
        FROM stress_proxy_candidate_energy_v2
        WHERE mechanistic_energy_candidate_rank = 1
        GROUP BY 1, 2
        ORDER BY rows DESC
    """)


def load_proton_seb_split(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
        SELECT
            target_mechanistic_regime,
            candidate_source AS v2_source,
            COUNT(*) AS rows
        FROM stress_proxy_candidate_energy_v2
        WHERE mechanistic_energy_candidate_rank = 1
          AND UPPER(COALESCE(target_event_type, '')) = 'SEB'
          AND (LOWER(COALESCE(target_ion_species, '')) LIKE '%proton%'
               OR LOWER(TRIM(COALESCE(target_ion_species, ''))) IN ('p','h','h+'))
        GROUP BY 1, 2
        ORDER BY rows DESC
    """)


def load_selci_reconfirmation(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
        SELECT
            candidate_source AS v2_source,
            mechanistic_energy_candidate_status,
            COUNT(*) AS rows
        FROM stress_proxy_candidate_energy_v2
        WHERE mechanistic_energy_candidate_rank = 1
          AND target_mechanistic_regime = 'selci_gate_coupled'
        GROUP BY 1, 2
        ORDER BY rows DESC
    """)


def load_selcii_coverage(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
        SELECT
            candidate_mechanistic_regime,
            mechanistic_energy_candidate_status,
            COUNT(*) AS rows
        FROM stress_proxy_candidate_energy_v2
        WHERE mechanistic_energy_candidate_rank = 1
          AND target_mechanistic_regime = 'selcii_drain_source_cumulative'
        GROUP BY 1, 2
        ORDER BY rows DESC
    """)


def load_same_device_coverage(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
        SELECT match_scope, COUNT(*) AS rows
        FROM stress_proxy_candidate_energy_v2
        WHERE mechanistic_energy_candidate_rank = 1
        GROUP BY match_scope
        ORDER BY rows DESC
    """)


def load_localization_context(conn) -> list[dict[str, Any]]:
    # Localization mismatch is a context NOTE, not a blocker.  incorrectly_blocked_rows
    # must stay 0 — a non-zero value means a localization class leaked into the
    # blocker array, which would gate everything (ion track vs bulk is always huge).
    return _fetch(conn, """
        SELECT
            CASE
                WHEN localization_mismatch_log10 IS NULL THEN 'missing'
                WHEN ABS(localization_mismatch_log10) > 4.0 THEN 'extreme'
                WHEN ABS(localization_mismatch_log10) > 2.0 THEN 'large'
                WHEN ABS(localization_mismatch_log10) > 1.0 THEN 'moderate'
                ELSE 'comparable'
            END AS localization_mismatch_class,
            COUNT(*) AS rows,
            COUNT(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(COALESCE(energy_v2_blockers, ARRAY[]::text[])) b
                    WHERE b LIKE 'localization%'
                )
            ) AS incorrectly_blocked_rows
        FROM stress_proxy_candidate_energy_v2
        WHERE mechanistic_energy_candidate_rank = 1
        GROUP BY 1
        ORDER BY rows DESC
    """)


def load_v2_regression_checks(conn) -> list[dict[str, Any]]:
    """Codify the §4.3 re-confirmation items as PASS/FAIL assertions.

    These turn known-good v2 behavior into hard checks so a future schema edit
    that regresses them fails loudly (mirrors load_regression_checks in the v1
    calibrate_proxy_distance.py).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. A SELC-I target must not get an avalanche rank-1 candidate unless it
        #    is explicitly flagged a regime mismatch.  The seeded priors make
        #    every (selci_gate_coupled, avalanche_*) pair 'mechanism_mismatch',
        #    which becomes status 'mechanistic_regime_mismatch'.
        cur.execute("""
            SELECT COUNT(*) AS unflagged_avalanche_rank1
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
              AND target_mechanistic_regime = 'selci_gate_coupled'
              AND candidate_source = 'avalanche'
              AND mechanistic_energy_candidate_status
                  IS DISTINCT FROM 'mechanistic_regime_mismatch'
        """)
        selci = dict(cur.fetchone())

        # 2. Proton-SEB endpoint assertion (replaces the old
        #    seb_source_shifts_present check 2026-07-02).  The old check
        #    asserted v1->v2 rank-1 source SHIFTS exist, which is only true
        #    while v1 and v2 carry different mechanism priors; once v1's path
        #    penalty derives from the unified stress_regime_compatibility
        #    table, the shifts legitimately vanish and a shift-based check
        #    fails by design.  The stable invariant is the ENDPOINT: a
        #    low-collapse proton-SEB target must never get an avalanche rank-1
        #    while a same-device SC candidate exists in the pool (the seeded
        #    prior makes SC first-order and avalanche a mismatch there).
        #    proton_high_field_seb is excluded: avalanche is a legitimate
        #    secondary analog for it.
        cur.execute("""
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
        """)
        seb = dict(cur.fetchone())

        # 3. Localization mismatch is a context NOTE, never a blocker (invariant
        #    #6).  Any localization token in energy_v2_blockers is a regression.
        cur.execute("""
            SELECT COUNT(*) AS localization_blocked_rows
            FROM stress_proxy_candidate_energy_v2
            WHERE mechanistic_energy_candidate_rank = 1
              AND EXISTS (
                  SELECT 1 FROM unnest(COALESCE(energy_v2_blockers, ARRAY[]::text[])) b
                  WHERE b LIKE 'localization%'
              )
        """)
        loc = dict(cur.fetchone())

        # 4. A seeded first-order/secondary analog row with a NULL ceiling means
        #    no cap.  Missing rule rows default to analog_questionable, but
        #    explicit first-order measured evidence must stay reachable.
        cur.execute("""
            SELECT COUNT(*) AS first_order_measured_capped
            FROM stress_proxy_candidate_energy_v2
            WHERE regime_match_class IN ('first_order_analog', 'secondary_analog')
              AND measured_comparability_status IN ('strong', 'usable')
              AND mechanistic_energy_candidate_status = 'mechanistic_analog_questionable'
        """)
        first_order = dict(cur.fetchone())

    return [
        {
            "name": "selci_no_unflagged_avalanche_rank1",
            "passed": (selci["unflagged_avalanche_rank1"] or 0) == 0,
            **selci,
        },
        {
            "name": "proton_seb_sc_preferred_when_available",
            "passed": (seb["avalanche_rank1_with_same_device_sc"] or 0) == 0,
            **seb,
        },
        {
            "name": "localization_never_blocks",
            "passed": (loc["localization_blocked_rows"] or 0) == 0,
            **loc,
        },
        {
            "name": "first_order_measured_not_capped",
            "passed": (first_order["first_order_measured_capped"] or 0) == 0,
            **first_order,
        },
    ]


def load_top_blockers(conn) -> list[dict[str, Any]]:
    return _fetch(conn, """
        SELECT blocker, COUNT(*) AS rows
        FROM stress_proxy_candidate_energy_v2,
             LATERAL unnest(COALESCE(energy_v2_blockers, ARRAY[]::text[])) AS blocker
        WHERE mechanistic_energy_candidate_rank = 1
        GROUP BY blocker
        ORDER BY rows DESC
        LIMIT 20
    """)


def load_truth_label_rows(conn) -> list[dict[str, Any]]:
    """One row per curated label, with the v2 rank/blockers of that exact pair.

    Two LEFT JOINs:
    - ``stress_proxy_candidate_energy_v2`` (top-10 capped) gives ``v2_rank`` /
      ``blockers``; NULL ``v2_rank`` means the pair is not in v2's top-10.
    - ``stress_proxy_candidate_ranked_view`` (uncapped pool) gives
      ``in_candidate_pool``, so compute_truth_metrics can tell an out-of-top-10
      miss (ranking issue) from a not-in-pool miss (data-coverage gap).
    """
    return _fetch(conn, """
        SELECT
            t.target_stress_record_key,
            t.candidate_stress_record_key,
            t.label,
            t.label_basis,
            t.reviewer,
            v2.mechanistic_energy_candidate_rank AS v2_rank,
            v2.energy_v2_blockers AS blockers,
            (r.target_stress_record_key IS NOT NULL) AS in_candidate_pool
        FROM proxy_truth_labels t
        LEFT JOIN stress_proxy_candidate_energy_v2 v2
          ON v2.target_stress_record_key = t.target_stress_record_key
         AND v2.candidate_stress_record_key = t.candidate_stress_record_key
        LEFT JOIN stress_proxy_candidate_ranked_view r
          ON r.target_stress_record_key = t.target_stress_record_key
         AND r.candidate_stress_record_key = t.candidate_stress_record_key
    """)


# Per-target rank-1 pick by each method.  rank_pick = v1's published
# prior+mask pick (candidate_rank = 1); dssig_pick = v1's pick ranked by
# signature_axis_distance alone (energy-free and prior-free, the headline
# comparator); energy_blended_pick = the explicit energy_blended_control_distance
# control that preserves the energy-circularity diagnostic; v2_pick = the
# energy proxy's pick.
_CONCORDANCE_PICKS_CTE = """
    WITH ranked AS (
        SELECT
            r.target_stress_record_key AS t,
            r.candidate_stress_record_key AS c,
            r.candidate_rank AS rank_pick_rank,
            r.damage_signature_evidence_class AS evidence,
            ROW_NUMBER() OVER (
                PARTITION BY r.target_stress_record_key
                ORDER BY r.signature_axis_distance ASC NULLS LAST
            ) AS dssig_rank,
            ROW_NUMBER() OVER (
                PARTITION BY r.target_stress_record_key
                ORDER BY r.energy_blended_control_distance ASC NULLS LAST
            ) AS energy_blended_rank,
            v2.mechanistic_energy_candidate_rank AS v2_rank,
            v2.match_scope AS scope
        FROM stress_proxy_candidate_ranked_view r
        LEFT JOIN stress_proxy_candidate_energy_v2 v2
            ON v2.target_stress_record_key = r.target_stress_record_key
           AND v2.candidate_stress_record_key = r.candidate_stress_record_key
    ),
    picks AS (
        SELECT
            t,
            MAX(c) FILTER (WHERE v2_rank = 1) AS v2_pick,
            MAX(c) FILTER (WHERE rank_pick_rank = 1) AS rank_pick,
            MAX(c) FILTER (WHERE dssig_rank = 1) AS dssig_pick,
            MAX(c) FILTER (WHERE energy_blended_rank = 1) AS energy_blended_pick,
            MAX(scope) FILTER (WHERE v2_rank = 1) AS scope,
            MAX(evidence) FILTER (WHERE v2_rank = 1) AS v2_pick_evidence
        FROM ranked
        GROUP BY t
    )
"""


def load_concordance(conn) -> dict[str, Any]:
    """v1 (damage-signature) vs v2 (energy) rank-1 agreement, with the energy
    ablation that separates independent corroboration from shared-energy
    circularity.
    """
    summary = _fetch(conn, _CONCORDANCE_PICKS_CTE + """
        SELECT
            COUNT(*) AS targets,
            COUNT(*) FILTER (WHERE v2_pick = rank_pick) AS v2_eq_v1_rank,
            COUNT(*) FILTER (WHERE v2_pick = dssig_pick) AS v2_eq_v1_damagesig,
            COUNT(*) FILTER (WHERE v2_pick = energy_blended_pick) AS v2_eq_v1_energy_blended
        FROM picks
        WHERE v2_pick IS NOT NULL
    """)[0]
    by_scope = _fetch(conn, _CONCORDANCE_PICKS_CTE + """
        SELECT
            scope,
            COUNT(*) AS targets,
            COUNT(*) FILTER (WHERE v2_pick = rank_pick) AS rank_agree,
            COUNT(*) FILTER (WHERE v2_pick = dssig_pick) AS prior_free_agree,
            COUNT(*) FILTER (WHERE v2_pick = energy_blended_pick) AS energy_blended_agree
        FROM picks
        WHERE v2_pick IS NOT NULL
        GROUP BY scope
        ORDER BY targets DESC
    """)
    # Curation queue: targets where the energy proxy and the independent
    # damage-signature ranking disagree, grouped by the v2 pick's evidence class.
    # Disagreements with measured/strong evidence are the highest-value pairs to
    # curate into proxy_truth_labels.
    curation_queue = _fetch(conn, _CONCORDANCE_PICKS_CTE + """
        SELECT
            COALESCE(v2_pick_evidence, '(none)') AS v2_pick_evidence_class,
            COUNT(*) AS disagreement_targets
        FROM picks
        WHERE v2_pick IS NOT NULL
          AND v2_pick IS DISTINCT FROM dssig_pick
        GROUP BY v2_pick_evidence
        ORDER BY disagreement_targets DESC
    """)
    enrichment = _fetch(conn, """
        SELECT
            COUNT(*) AS targets,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY v2_pick_dssig_percentile
            ) AS median_dssig_percentile,
            COUNT(*) FILTER (WHERE v2_pick_dssig_percentile <= 10.0)
                AS best_decile_targets,
            COUNT(*) FILTER (WHERE source_conflict) AS source_conflict_targets,
            COUNT(*) FILTER (WHERE c2m0080120d_avalanche_vs_sc_conflict)
                AS c2m0080120d_avalanche_vs_sc_conflicts
        FROM stress_proxy_concordance_enrichment_view
        WHERE v2_pick_dssig_percentile IS NOT NULL
    """)[0]
    enrichment_by_scope = _fetch(conn, """
        SELECT
            v2_match_scope AS scope,
            COUNT(*) AS targets,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY v2_pick_dssig_percentile
            ) AS median_dssig_percentile,
            COUNT(*) FILTER (WHERE v2_pick_dssig_percentile <= 10.0)
                AS best_decile_targets,
            COUNT(*) FILTER (WHERE source_conflict) AS source_conflict_targets
        FROM stress_proxy_concordance_enrichment_view
        WHERE v2_pick_dssig_percentile IS NOT NULL
        GROUP BY v2_match_scope
        ORDER BY targets DESC
    """)
    return {
        "summary": summary,
        "by_scope": by_scope,
        "curation_queue": curation_queue,
        "enrichment": enrichment,
        "enrichment_by_scope": enrichment_by_scope,
    }


def render_concordance(conc: dict[str, Any]) -> list[str]:
    """Markdown for the cross-method concordance + energy-ablation section."""
    s = conc.get("summary") or {}
    targets = s.get("targets") or 0
    rank = s.get("v2_eq_v1_rank") or 0
    prior_free = s.get("v2_eq_v1_damagesig") or 0
    energy_blended = s.get("v2_eq_v1_energy_blended") or 0
    enrichment = conc.get("enrichment") or {}
    enrichment_targets = enrichment.get("targets") or 0
    best_decile = enrichment.get("best_decile_targets") or 0
    median_percentile = enrichment.get("median_dssig_percentile")
    median_text = "n/a" if median_percentile is None else f"{float(median_percentile):.1f}th percentile"
    out = [
        "## Cross-method concordance (v1 damage-signature vs v2 energy)",
        "",
        "Rank-1 agreement between the energy proxy and v1 is reported three "
        "ways: the current v1 prior+mask rank, the prior-free signature-axis "
        "rank (headline), and an explicit energy-blended distance control.",
        "",
        f"- targets compared: {targets}",
        f"- v2 == v1 **prior+mask rank-1**: "
        f"{rank} ({fmt_rate(rate(rank, targets))})",
        f"- v2 == v1 **prior-free signature-axis rank-1** (headline): "
        f"{prior_free} ({fmt_rate(rate(prior_free, targets))})",
        f"- v2 == v1 **energy-blended distance rank-1**: "
        f"{energy_blended} ({fmt_rate(rate(energy_blended, targets))})",
        f"- enrichment headline: v2 picks sit at median {median_text} "
        f"of v1's prior-free signature ordering; "
        f"{best_decile} ({fmt_rate(rate(best_decile, enrichment_targets))}) "
        f"are in the best decile",
        f"- source conflicts flagged for curation: "
        f"{enrichment.get('source_conflict_targets') or 0}; "
        f"C2M0080120D avalanche-vs-SC focus rows: "
        f"{enrichment.get('c2m0080120d_avalanche_vs_sc_conflicts') or 0}",
        "",
        "The prior-free enrichment statistic is the durable comparator.  Exact "
        "rank-1 agreement is expected to be low after v1/v2 separation; the "
        "question is whether v2 picks concentrate near the top of v1's "
        "energy-free signature ordering.  The energy-blended "
        "pick is retained as a circularity diagnostic: a large gap between it "
        "and the headline rate means shared energy terms, not independent "
        "damage-signature corroboration, are driving agreement.",
        "",
        "Caveat: this apply re-baselines all three rates because prior "
        "unification, mask ranking, and energy removal from v1's published rank "
        "landed together.  Do not compare these rates to the earlier 32.0% / "
        "13.4% figures; record them as a fresh baseline.",
        "",
        "### Independent agreement by match scope",
        "",
        render_table(
            ["scope", "targets", "rank_agree", "prior_free_agree", "energy_blended_agree"],
            conc.get("by_scope") or [],
        ),
        "",
        "### Enrichment by match scope",
        "",
        render_table(
            ["scope", "targets", "median_dssig_percentile", "best_decile_targets", "source_conflict_targets"],
            conc.get("enrichment_by_scope") or [],
        ),
        "",
        "### Disagreement curation queue (energy vs prior-free signature axes)",
        "",
        "Targets where the two methods disagree at rank-1, by the v2 pick's "
        "evidence class. Rows with measured/strong evidence are the priority "
        "pairs to adjudicate into `proxy_truth_labels`.",
        "",
        render_table(
            ["v2_pick_evidence_class", "disagreement_targets"],
            conc.get("curation_queue") or [],
        ),
        "",
    ]
    return out


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def collect_sections(conn) -> dict[str, list[dict[str, Any]]]:
    return {
        "rank_shifts": load_rank_shifts(conn),
        "status_transitions": load_status_transitions(conn),
        "proton_seb_split": load_proton_seb_split(conn),
        "selci_reconfirmation": load_selci_reconfirmation(conn),
        "selcii_coverage": load_selcii_coverage(conn),
        "same_device_coverage": load_same_device_coverage(conn),
        "localization_context": load_localization_context(conn),
        "top_blockers": load_top_blockers(conn),
    }


def write_outputs(out_dir: Path,
                  sections: dict[str, list[dict[str, Any]]],
                  truth_by_basis: dict[str, dict[str, Any]],
                  regression_checks: list[dict[str, Any]],
                  concordance: dict[str, Any] | None = None) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    report = render_report(sections, truth_by_basis, regression_checks,
                           generated_at, concordance=concordance)
    (out_dir / "report.md").write_text(report)
    payload = {
        "generated_at": generated_at,
        "truth_metrics": truth_by_basis,
        "regression_checks": regression_checks,
        "concordance": concordance,
        "sections": sections,
        "caveat": "Read-only v2 audit; not fitted constants. Data coverage, "
                  "not method, is the binding constraint.",
    }
    (out_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str)
    )
    return generated_at


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir", type=Path, default=OUT_DIR,
        help="Directory for report.md and results.json.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with get_connection() as conn:
        sections = collect_sections(conn)
        truth_by_basis = compute_truth_metrics_by_basis(load_truth_label_rows(conn))
        regression_checks = load_v2_regression_checks(conn)
        concordance = load_concordance(conn)

    write_outputs(args.out_dir, sections, truth_by_basis, regression_checks,
                  concordance=concordance)
    truth_metrics = truth_by_basis["all"]
    print(f"Wrote {args.out_dir / 'report.md'}")
    print(f"Wrote {args.out_dir / 'results.json'}")
    if truth_metrics["fail_closed"]:
        print("Truth-hit: no curated truth labels (failing closed).")
    else:
        print(
            "Truth-hit: "
            f"top1={fmt_rate(truth_metrics['top1_rate'])}, "
            f"top3={fmt_rate(truth_metrics['top3_rate'])}, "
            f"not_blocked={fmt_rate(truth_metrics['not_blocked_rate'])}"
        )

    cs = concordance.get("summary") or {}
    ct = cs.get("targets") or 0
    print(
        "Concordance (v2==v1 rank-1): "
        f"prior_mask={fmt_rate(rate(cs.get('v2_eq_v1_rank') or 0, ct))}, "
        f"prior_free={fmt_rate(rate(cs.get('v2_eq_v1_damagesig') or 0, ct))}, "
        f"energy_blended={fmt_rate(rate(cs.get('v2_eq_v1_energy_blended') or 0, ct))}"
    )

    failed = [c["name"] for c in regression_checks if not c["passed"]]
    for chk in regression_checks:
        print(f"Regression {'PASS' if chk['passed'] else 'FAIL'}: {chk['name']}")
    if failed:
        print(f"FAIL: {len(failed)} regression check(s) failed: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
