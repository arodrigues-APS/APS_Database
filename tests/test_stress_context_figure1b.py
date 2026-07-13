import os
import unittest

import pytest

from aps.db_config import get_connection


pytestmark = pytest.mark.production_smoke


class StressContextFigure1BTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("APS_RUN_PRODUCTION_SMOKE") != "1":
            raise unittest.SkipTest(
                "set APS_RUN_PRODUCTION_SMOKE=1 to run live database checks"
            )
        cls.conn = get_connection()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def fetchone(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def test_repetitive_sequence_effective_time_scales_by_pulse_count(self):
        mismatch_count, scaled_count = self.fetchone(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE ABS(
                        effective_stress_time_s
                        - pulse_count_in_sequence * stress_duration_s
                    ) > GREATEST(
                        1e-18,
                        ABS(pulse_count_in_sequence * stress_duration_s) * 1e-12
                    )
                ) AS mismatch_count,
                COUNT(*) AS scaled_count
            FROM stress_test_context_view
            WHERE pulse_count_in_sequence > 1
              AND stress_duration_s IS NOT NULL
            """
        )

        self.assertEqual(mismatch_count, 0)
        self.assertGreaterEqual(scaled_count, 1000)

    def test_figure1b_time_basis_is_populated_for_effective_time(self):
        non_null_effective, missing_basis, scaled_basis = self.fetchone(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE effective_stress_time_s IS NOT NULL
                ) AS non_null_effective,
                COUNT(*) FILTER (
                    WHERE effective_stress_time_s IS NOT NULL
                      AND figure1b_time_basis IS NULL
                ) AS missing_basis,
                COUNT(*) FILTER (
                    WHERE figure1b_time_basis = 'repetitive_sequence_scaled'
                ) AS scaled_basis
            FROM stress_test_context_view
            """
        )

        self.assertGreaterEqual(non_null_effective, 2000)
        self.assertEqual(missing_basis, 0)
        self.assertGreaterEqual(scaled_basis, 1000)

    def test_figure1b_chart_filter_excludes_known_avalanche_artifact_family(self):
        unfiltered_count, artifact_count, chart_count = self.fetchone(
            """
            SELECT
                COUNT(*) AS unfiltered_count,
                COUNT(*) FILTER (
                    WHERE source = 'avalanche' AND normalized_vds > 1.60
                ) AS artifact_count,
                COUNT(*) FILTER (
                    WHERE NOT (source = 'avalanche' AND normalized_vds > 1.60)
                ) AS chart_count
            FROM stress_test_context_view
            WHERE normalized_vds IS NOT NULL
              AND stress_duration_s IS NOT NULL
            """
        )

        self.assertGreaterEqual(unfiltered_count, 2000)
        self.assertGreaterEqual(artifact_count, 1)
        self.assertGreaterEqual(chart_count, 1000)
        self.assertLess(chart_count, unfiltered_count)

    def test_destructive_marker_rows_are_plottable(self):
        marker_count, irradiation_marker_count = self.fetchone(
            """
            SELECT
                COUNT(*) AS marker_count,
                COUNT(*) FILTER (WHERE source = 'irradiation')
                    AS irradiation_marker_count
            FROM stress_test_context_view
            WHERE normalized_vds IS NOT NULL
              AND stress_duration_s IS NOT NULL
              AND response_reversibility = 'destructive_or_catastrophic'
              AND NOT (source = 'avalanche' AND normalized_vds > 1.60)
            """
        )

        self.assertGreaterEqual(marker_count, 10)
        self.assertGreaterEqual(irradiation_marker_count, 10)

    def test_destruction_boundary_rollup_has_destructive_lower_bound(self):
        boundary_rows, destructive_boundary_rows, destructive_count = self.fetchone(
            """
            SELECT
                COUNT(*) AS boundary_rows,
                COUNT(*) FILTER (
                    WHERE min_destructive_normalized_vds IS NOT NULL
                ) AS destructive_boundary_rows,
                COALESCE(SUM(destructive_count), 0) AS destructive_count
            FROM stress_destruction_boundary_view
            """
        )

        self.assertGreaterEqual(boundary_rows, 1)
        self.assertGreaterEqual(destructive_boundary_rows, 1)
        self.assertGreaterEqual(destructive_count, 1)

    def test_plottable_baseline_counts_remain_in_expected_ranges(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    source,
                    COUNT(*) FILTER (
                        WHERE normalized_vds IS NOT NULL
                          AND stress_duration_s IS NOT NULL
                    ) AS has_both,
                    COUNT(*) FILTER (
                        WHERE normalized_vds IS NOT NULL
                          AND stress_duration_s IS NOT NULL
                          AND pulse_count_in_sequence IS NOT NULL
                    ) AS sequenced_with_both
                FROM stress_test_context_view
                GROUP BY source
                """
            )
            rows = {source: (has_both, sequenced)
                    for source, has_both, sequenced in cur.fetchall()}

        self.assertGreaterEqual(rows.get('avalanche', (0, 0))[0], 1000)
        self.assertGreaterEqual(rows.get('avalanche', (0, 0))[1], 1000)
        self.assertGreaterEqual(rows.get('irradiation', (0, 0))[0], 900)
        self.assertGreaterEqual(rows.get('sc', (0, 0))[0], 20)


if __name__ == "__main__":
    unittest.main()
