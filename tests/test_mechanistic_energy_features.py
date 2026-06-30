import math
import unittest

from data_processing_scripts.mechanistic_energy_proxy import (
    SEB_CRITICAL_J_CM2,
    SELC_CRITICAL_J_CM2,
    SIC_DENSITY_MG_CM3,
    MEV_TO_J,
    EnergyEquivalenceSettings,
    active_area_cm2,
    critical_ratio,
    terminal_areal_energy_j_cm2,
    track_core_energy_density_from_deposited,
    track_core_energy_density_from_let,
    track_core_volume_cm3,
)


class GeometryTests(unittest.TestCase):
    def test_active_area_from_volume_and_thickness(self):
        # 1e-3 cm^3 over a 10 um layer -> 1.0 cm^2.
        self.assertAlmostEqual(active_area_cm2(1e-3, 10.0), 1.0)

    def test_active_area_invalid(self):
        self.assertIsNone(active_area_cm2(None, 10.0))
        self.assertIsNone(active_area_cm2(1e-3, 0.0))
        self.assertIsNone(active_area_cm2(-1.0, 10.0))


class TerminalArealEnergyTests(unittest.TestCase):
    def test_areal_energy(self):
        self.assertAlmostEqual(terminal_areal_energy_j_cm2(0.5, 1.0), 0.5)

    def test_areal_energy_invalid(self):
        self.assertIsNone(terminal_areal_energy_j_cm2(0.5, None))
        self.assertIsNone(terminal_areal_energy_j_cm2(0.0, 1.0))

    def test_critical_ratio_to_seb_and_selc(self):
        areal = 0.5
        self.assertAlmostEqual(
            critical_ratio(areal, SEB_CRITICAL_J_CM2), areal / 207e-6)
        self.assertAlmostEqual(
            critical_ratio(areal, SELC_CRITICAL_J_CM2), areal / 60e-6)

    def test_selc_ratio_exceeds_seb_ratio(self):
        # The SELC critical energy is smaller, so the same loading is a larger
        # multiple of it.
        areal = 1e-3
        self.assertGreater(
            critical_ratio(areal, SELC_CRITICAL_J_CM2),
            critical_ratio(areal, SEB_CRITICAL_J_CM2),
        )

    def test_critical_ratio_invalid(self):
        self.assertIsNone(critical_ratio(None, SEB_CRITICAL_J_CM2))
        self.assertIsNone(critical_ratio(0.5, 0.0))


class TrackCoreTests(unittest.TestCase):
    def test_core_volume(self):
        # r = 0.1 um, L = 10 um.
        expected = math.pi * (0.1e-4) ** 2 * (10.0e-4)
        self.assertAlmostEqual(track_core_volume_cm3(0.1, 10.0), expected)

    def test_density_from_deposited(self):
        volume = math.pi * (0.1e-4) ** 2 * (10.0e-4)
        self.assertAlmostEqual(
            track_core_energy_density_from_deposited(1e-9, 0.1, 10.0),
            1e-9 / volume,
        )

    def test_density_from_let_matches_formula(self):
        let = 10.0
        radius_um = 0.1
        core_area = math.pi * (radius_um * 1e-4) ** 2
        expected = let * SIC_DENSITY_MG_CM3 * MEV_TO_J / core_area
        self.assertAlmostEqual(
            track_core_energy_density_from_let(let, radius_um), expected)

    def test_density_from_let_is_path_length_independent(self):
        # The LET-based density does not depend on path length by construction.
        a = track_core_energy_density_from_let(10.0, 0.1)
        b = track_core_energy_density_from_let(10.0, 0.1)
        self.assertEqual(a, b)

    def test_smaller_radius_gives_higher_density(self):
        tight = track_core_energy_density_from_let(10.0, 0.05)
        wide = track_core_energy_density_from_let(10.0, 0.5)
        self.assertGreater(tight, wide)

    def test_invalid_inputs(self):
        self.assertIsNone(track_core_volume_cm3(0.0, 10.0))
        self.assertIsNone(track_core_energy_density_from_let(None, 0.1))
        self.assertIsNone(track_core_energy_density_from_deposited(1e-9, 0.1, None))


class SettingsTests(unittest.TestCase):
    def test_defaults(self):
        s = EnergyEquivalenceSettings()
        self.assertEqual(s.setting_name, "default")
        self.assertEqual(s.collapse_hard_threshold, 0.5)
        self.assertEqual(s.default_track_core_radius_um, 0.1)
        # The reachthrough doping estimate is known to run high, so its band is
        # wider than the measured band.
        self.assertGreater(
            s.doping_log_sigma_estimated, s.doping_log_sigma_measured)
        self.assertIn("doping_log_sigma_estimated", s.to_dict())


if __name__ == "__main__":
    unittest.main()
