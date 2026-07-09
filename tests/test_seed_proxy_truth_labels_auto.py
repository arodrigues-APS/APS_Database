import unittest

from aps.seeds.seed_proxy_truth_labels_auto import (
    AUTO_LABEL_BASIS,
    AUTO_REVIEWER,
    build_seed_sql,
)


class SeedProxyTruthLabelsAutoTests(unittest.TestCase):
    def test_sql_uses_quarantined_basis_and_reviewer_contract(self):
        sql = build_seed_sql()

        self.assertIn("measured_match_scope = 'exact_condition'", sql)
        self.assertIn("measured_comparability_status IN ('strong', 'usable')", sql)
        self.assertIn("COALESCE(r.measured_sign_mismatch_axis_count, 0) = 0", sql)
        self.assertIn("ON CONFLICT (target_stress_record_key, candidate_stress_record_key)", sql)
        self.assertIn("DO NOTHING", sql)
        self.assertEqual(AUTO_LABEL_BASIS, "measured_post_iv_auto")
        self.assertEqual(AUTO_REVIEWER, "auto_seed")

    def test_dry_run_sql_has_no_insert(self):
        sql = build_seed_sql(dry_run=True)

        self.assertIn("WITH eligible AS", sql)
        self.assertNotIn("INSERT INTO proxy_truth_labels", sql)


if __name__ == "__main__":
    unittest.main()
