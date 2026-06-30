import unittest

from data_processing_scripts.calibrate_mechanistic_energy_proxy import (
    compute_truth_metrics,
    compute_truth_metrics_by_basis,
    render_regression_checks,
    render_report,
    render_table,
    _has_blocker,
)


class TruthMetricTests(unittest.TestCase):
    def test_empty_labels_fail_closed(self):
        m = compute_truth_metrics([])
        self.assertTrue(m["fail_closed"])
        self.assertIsNone(m["top1_rate"])
        self.assertIsNone(m["top3_rate"])
        self.assertIsNone(m["not_blocked_rate"])
        self.assertEqual(m["equivalent_labels"], 0)

    def test_all_uncertain_fails_closed(self):
        rows = [{"label": "uncertain", "v2_rank": 1, "blockers": None}]
        m = compute_truth_metrics(rows)
        self.assertTrue(m["fail_closed"])
        self.assertEqual(m["uncertain_labels"], 1)
        self.assertIsNone(m["top1_rate"])

    def test_equivalent_top1_and_top3(self):
        rows = [
            {"label": "equivalent", "v2_rank": 1, "blockers": None},
            {"label": "equivalent", "v2_rank": 3, "blockers": None},
            {"label": "equivalent", "v2_rank": None, "blockers": None},  # outside pool
        ]
        m = compute_truth_metrics(rows)
        self.assertFalse(m["fail_closed"])
        self.assertEqual(m["equivalent_labels"], 3)
        self.assertEqual(m["top1_hits"], 1)
        self.assertEqual(m["top3_hits"], 2)
        self.assertAlmostEqual(m["top1_rate"], 1 / 3)
        self.assertAlmostEqual(m["top3_rate"], 2 / 3)

    def test_not_blocked_excludes_blocked_and_missing(self):
        rows = [
            {"label": "equivalent", "v2_rank": 1, "blockers": None},
            {"label": "equivalent", "v2_rank": 2, "blockers": ["mechanism_mismatch"]},
            {"label": "equivalent", "v2_rank": None, "blockers": None},
        ]
        m = compute_truth_metrics(rows)
        # Only the rank-1, blocker-free row counts as not-blocked.
        self.assertEqual(m["not_blocked_hits"], 1)
        self.assertAlmostEqual(m["not_blocked_rate"], 1 / 3)

    def test_not_equivalent_rank1_is_violation(self):
        rows = [
            {"label": "not_equivalent", "v2_rank": 1, "blockers": None},
            {"label": "not_equivalent", "v2_rank": 5, "blockers": None},
        ]
        m = compute_truth_metrics(rows)
        self.assertEqual(m["not_equivalent_labels"], 2)
        self.assertEqual(m["not_equivalent_rank1_violations"], 1)
        self.assertAlmostEqual(m["not_equivalent_rank1_rate"], 0.5)
        # not_equivalent labels never count as equivalent evaluable cases.
        self.assertEqual(m["equivalent_labels"], 0)
        self.assertTrue(m["fail_closed"])

    def test_miss_split_by_candidate_pool(self):
        rows = [
            {"label": "equivalent", "v2_rank": None, "in_candidate_pool": False},  # coverage gap
            {"label": "equivalent", "v2_rank": None, "in_candidate_pool": True},   # ranked 11+
            {"label": "equivalent", "v2_rank": None},                              # unknown pool
            {"label": "equivalent", "v2_rank": 1, "in_candidate_pool": True},      # hit, not a miss
        ]
        m = compute_truth_metrics(rows)
        self.assertEqual(m["miss_not_in_pool"], 1)
        self.assertEqual(m["miss_out_of_top10"], 1)
        self.assertEqual(m["miss_unknown_pool"], 1)
        self.assertEqual(m["top1_hits"], 1)


class ByBasisTests(unittest.TestCase):
    def test_groups_present_bases_plus_all(self):
        rows = [
            {"label": "equivalent", "v2_rank": 1, "label_basis": "measured_post_iv"},
            {"label": "equivalent", "v2_rank": 5, "label_basis": "pilot"},
        ]
        by_basis = compute_truth_metrics_by_basis(rows)
        self.assertEqual(set(by_basis), {"all", "measured_post_iv", "pilot"})
        # measured group is a perfect top-1; pilot group misses top-1.
        self.assertAlmostEqual(by_basis["measured_post_iv"]["top1_rate"], 1.0)
        self.assertAlmostEqual(by_basis["pilot"]["top1_rate"], 0.0)
        self.assertAlmostEqual(by_basis["all"]["top1_rate"], 0.5)

    def test_empty_yields_only_all(self):
        by_basis = compute_truth_metrics_by_basis([])
        self.assertEqual(set(by_basis), {"all"})
        self.assertTrue(by_basis["all"]["fail_closed"])


class BlockerHelperTests(unittest.TestCase):
    def test_none_and_empty_are_not_blocked(self):
        self.assertFalse(_has_blocker(None))
        self.assertFalse(_has_blocker([]))

    def test_nonempty_list_is_blocked(self):
        self.assertTrue(_has_blocker(["x"]))


class RenderTests(unittest.TestCase):
    def test_render_table_no_rows(self):
        self.assertEqual(render_table(["a", "b"], []), "_(no rows)_")

    def test_render_table_formats_list_cell(self):
        out = render_table(["blockers"], [{"blockers": ["a", "b"]}])
        self.assertIn("a, b", out)

    def test_render_report_failclosed_message(self):
        by_basis = compute_truth_metrics_by_basis([])
        report = render_report({}, by_basis, [], "2026-06-30T00:00:00Z")
        self.assertIn("No curated truth labels", report)
        self.assertIn("failing closed", report.lower())

    def test_render_report_includes_all_sections(self):
        by_basis = compute_truth_metrics_by_basis([])
        report = render_report({}, by_basis, [], "2026-06-30T00:00:00Z")
        for title in (
            "Regression checks",
            "rank-1 source shifts",
            "status transitions",
            "Proton SEB rank-1 split",
            "SELC-I rank-1 re-confirmation",
            "SELC-II cumulative coverage",
            "Same-device coverage",
            "Localization mismatch context",
            "Top v2 rank-1 blockers",
        ):
            self.assertIn(title, report)

    def test_render_report_includes_by_basis_table(self):
        rows = [{"label": "equivalent", "v2_rank": 1, "label_basis": "measured_post_iv"}]
        by_basis = compute_truth_metrics_by_basis(rows)
        report = render_report({}, by_basis, [], "2026-06-30T00:00:00Z")
        self.assertIn("By label basis", report)
        self.assertIn("measured_post_iv", report)

    def test_render_regression_checks_pass_and_fail(self):
        checks = [
            {"name": "selci_no_unflagged_avalanche_rank1", "passed": True,
             "unflagged_avalanche_rank1": 0},
            {"name": "localization_never_blocks", "passed": False,
             "localization_blocked_rows": 3},
        ]
        out = "\n".join(render_regression_checks(checks))
        self.assertIn("**PASS** `selci_no_unflagged_avalanche_rank1`", out)
        self.assertIn("**FAIL** `localization_never_blocks`", out)
        self.assertIn("localization_blocked_rows=3", out)

    def test_render_regression_checks_empty(self):
        out = "\n".join(render_regression_checks([]))
        self.assertIn("not evaluated", out)


if __name__ == "__main__":
    unittest.main()
