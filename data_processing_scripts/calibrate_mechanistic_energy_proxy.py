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
    """Truth metrics for the whole set and per ``label_basis`` group.

    A hit on a ``measured_post_iv`` label is empirical-anchor evidence; a
    ``pilot`` hit is weak.  Reporting them separately keeps the strength of the
    evidence visible instead of averaging it away.  The ``all`` key always
    exists; per-basis keys appear only for bases actually present.
    """
    out = {"all": compute_truth_metrics(rows)}
    for basis in sorted({r.get("label_basis") for r in rows if r.get("label_basis")}):
        out[basis] = compute_truth_metrics(
            [r for r in rows if r.get("label_basis") == basis]
        )
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

        # 2. SEB targets should still show interpretable v1->v2 rank-1 source
        #    shifts (the intended SEB signal, not a regression).
        cur.execute("""
            SELECT COUNT(*) AS seb_source_shifts
            FROM stress_proxy_candidate_view v1
            JOIN stress_proxy_candidate_energy_v2 v2
              ON v2.target_stress_record_key = v1.target_stress_record_key
             AND v2.mechanistic_energy_candidate_rank = 1
            WHERE v1.candidate_rank = 1
              AND UPPER(COALESCE(v2.target_event_type, '')) = 'SEB'
              AND v1.candidate_source IS DISTINCT FROM v2.candidate_source
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

    return [
        {
            "name": "selci_no_unflagged_avalanche_rank1",
            "passed": (selci["unflagged_avalanche_rank1"] or 0) == 0,
            **selci,
        },
        {
            "name": "seb_source_shifts_present",
            "passed": (seb["seb_source_shifts"] or 0) > 0,
            **seb,
        },
        {
            "name": "localization_never_blocks",
            "passed": (loc["localization_blocked_rows"] or 0) == 0,
            **loc,
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


# Per-target rank-1 pick by each method.  combined_pick = v1's published pick
# (energy-BLENDED combined distance); dssig_pick = v1's pick ranked by
# damage_signature_distance ALONE (energy-FREE, the independent comparator);
# v2_pick = the energy proxy's pick.
_CONCORDANCE_PICKS_CTE = """
    WITH ranked AS (
        SELECT
            r.target_stress_record_key AS t,
            r.candidate_stress_record_key AS c,
            r.candidate_rank AS combined_rank,
            r.damage_signature_evidence_class AS evidence,
            ROW_NUMBER() OVER (
                PARTITION BY r.target_stress_record_key
                ORDER BY r.damage_signature_distance ASC NULLS LAST
            ) AS dssig_rank,
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
            MAX(c) FILTER (WHERE combined_rank = 1) AS combined_pick,
            MAX(c) FILTER (WHERE dssig_rank = 1) AS dssig_pick,
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
            COUNT(*) FILTER (WHERE v2_pick = combined_pick) AS v2_eq_v1_combined,
            COUNT(*) FILTER (WHERE v2_pick = dssig_pick) AS v2_eq_v1_damagesig
        FROM picks
        WHERE v2_pick IS NOT NULL
    """)[0]
    by_scope = _fetch(conn, _CONCORDANCE_PICKS_CTE + """
        SELECT
            scope,
            COUNT(*) AS targets,
            COUNT(*) FILTER (WHERE v2_pick = dssig_pick) AS independent_agree,
            COUNT(*) FILTER (WHERE v2_pick = combined_pick) AS blended_agree
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
    return {"summary": summary, "by_scope": by_scope, "curation_queue": curation_queue}


def render_concordance(conc: dict[str, Any]) -> list[str]:
    """Markdown for the cross-method concordance + energy-ablation section."""
    s = conc.get("summary") or {}
    targets = s.get("targets") or 0
    blended = s.get("v2_eq_v1_combined") or 0
    independent = s.get("v2_eq_v1_damagesig") or 0
    out = [
        "## Cross-method concordance (v1 damage-signature vs v2 energy)",
        "",
        "Rank-1 agreement between the energy proxy and v1, measured two ways. The "
        "gap is the energy ablation: v1's *combined* score already contains an "
        "energy term, so agreement with it is partly circular; agreement with v1's "
        "*damage-signature-only* ranking is the independent corroboration.",
        "",
        f"- targets compared: {targets}",
        f"- v2 == v1 **combined** rank-1 (energy-blended, circular): "
        f"{blended} ({fmt_rate(rate(blended, targets))})",
        f"- v2 == v1 **damage-signature-only** rank-1 (energy-free, independent): "
        f"{independent} ({fmt_rate(rate(independent, targets))})",
        "",
        "If the independent rate is far below the blended rate, the apparent "
        "agreement was driven by shared energy, not cross-method corroboration.",
        "",
        "### Independent agreement by match scope",
        "",
        render_table(
            ["scope", "targets", "independent_agree", "blended_agree"],
            conc.get("by_scope") or [],
        ),
        "",
        "### Disagreement curation queue (energy vs independent damage-signature)",
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
        f"blended={fmt_rate(rate(cs.get('v2_eq_v1_combined') or 0, ct))}, "
        f"independent={fmt_rate(rate(cs.get('v2_eq_v1_damagesig') or 0, ct))} "
        "(gap = shared-energy circularity)"
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
