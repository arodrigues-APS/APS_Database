import math
import unittest

from aps.proxy.mechanistic_energy_proxy import (
    EnergyEquivalenceSettings,
    combine_log_sigmas,
    depletion_ratio_interval,
    intervals_overlap,
    log_interval,
    overlap_class,
    terminal_ratio_interval,
)


class LogIntervalTests(unittest.TestCase):
    def test_multiplicative_band(self):
        low, high = log_interval(2.0, 0.5)
        self.assertAlmostEqual(low, 2.0 / math.exp(0.5))
        self.assertAlmostEqual(high, 2.0 * math.exp(0.5))

    def test_zero_sigma_is_point(self):
        low, high = log_interval(3.0, 0.0)
        self.assertAlmostEqual(low, 3.0)
        self.assertAlmostEqual(high, 3.0)

    def test_invalid_inputs(self):
        self.assertEqual(log_interval(None, 0.5), (None, None))
        self.assertEqual(log_interval(-1.0, 0.5), (None, None))
        self.assertEqual(log_interval(2.0, None), (None, None))

    def test_combine_log_sigmas(self):
        self.assertAlmostEqual(combine_log_sigmas(0.3, 0.4), 0.5)
        self.assertIsNone(combine_log_sigmas(None, None))


class RatioIntervalTests(unittest.TestCase):
    def setUp(self):
        self.settings = EnergyEquivalenceSettings()

    def test_depletion_ratio_uses_half_doping_sigma(self):
        # U_stored ∝ √N, so the ratio band is half the doping log-sigma.
        low, high = depletion_ratio_interval(
            2.0, "rated_voltage_reachthrough_active_sic_thickness_estimate",
            self.settings,
        )
        factor = math.exp(0.5 * self.settings.doping_log_sigma_estimated)
        self.assertAlmostEqual(low, 2.0 / factor)
        self.assertAlmostEqual(high, 2.0 * factor)

    def test_measured_doping_band_is_narrower(self):
        _, est_high = depletion_ratio_interval(
            2.0, "reachthrough_estimate", self.settings)
        _, meas_high = depletion_ratio_interval(
            2.0, "kosier_2026_table_i_measured_epi_doping", self.settings)
        self.assertLess(meas_high, est_high)

    def test_terminal_ratio_combines_energy_and_area_sigma(self):
        low, high = terminal_ratio_interval(
            3.0, "integrated_event_vds_id", 0.9, self.settings)
        combined = math.sqrt(
            self.settings.terminal_energy_log_sigma_integrated ** 2
            + self.settings.active_area_log_sigma_measured ** 2
        )
        self.assertAlmostEqual(low, 3.0 / math.exp(combined))
        self.assertAlmostEqual(high, 3.0 * math.exp(combined))

    def test_commanded_energy_widens_band(self):
        _, integrated_high = terminal_ratio_interval(
            3.0, "integrated_event_vds_id", 0.9, self.settings)
        _, commanded_high = terminal_ratio_interval(
            3.0, "commanded_or_stored", 0.9, self.settings)
        self.assertGreater(commanded_high, integrated_high)

    def test_estimated_geometry_widens_band(self):
        _, measured_high = terminal_ratio_interval(
            3.0, "integrated_event_vds_id", 0.9, self.settings)
        _, estimated_high = terminal_ratio_interval(
            3.0, "integrated_event_vds_id", 0.1, self.settings)
        self.assertGreater(estimated_high, measured_high)


class OverlapTests(unittest.TestCase):
    def test_intervals_overlap_basic(self):
        self.assertTrue(intervals_overlap(1.0, 2.0, 1.5, 3.0))
        self.assertFalse(intervals_overlap(1.0, 2.0, 2.5, 3.0))
        self.assertIsNone(intervals_overlap(1.0, None, 2.0, 3.0))

    def test_strong_overlap(self):
        self.assertEqual(overlap_class(1.0, 2.0, 1.0, 2.0), "strong_overlap")

    def test_partial_overlap(self):
        self.assertEqual(overlap_class(1.0, 2.0, 1.8, 3.0), "partial_overlap")

    def test_near_miss(self):
        self.assertEqual(overlap_class(1.0, 2.0, 2.5, 3.0), "near_miss")

    def test_far_miss(self):
        self.assertEqual(overlap_class(1.0, 2.0, 5.0, 6.0), "far_miss")

    def test_missing_interval(self):
        self.assertEqual(overlap_class(1.0, None, 2.0, 3.0), "missing_interval")


if __name__ == "__main__":
    unittest.main()
