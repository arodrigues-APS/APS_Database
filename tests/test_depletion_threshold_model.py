import unittest

from data_processing_scripts.depletion_threshold_model import (
    KOSIER_2026_SEB_CRITICAL_J_CM2,
    KOSIER_2026_SELC_CRITICAL_J_CM2,
    ELEMENTARY_CHARGE_C,
    critical_voltage_for_areal_energy,
    depletion_width_um,
    net_doping_from_reachthrough,
    peak_field_v_cm,
    sic_permittivity_f_per_cm,
    stored_depletion_energy_areal_j_cm2,
)


class DepletionThresholdModelTests(unittest.TestCase):
    def test_reachthrough_doping_round_trip_matches_slab_energy(self):
        voltage_v = 1200.0
        width_um = 10.0
        eps = sic_permittivity_f_per_cm()
        doping = net_doping_from_reachthrough(voltage_v, width_um)

        expected_doping = (
            2.0 * eps * voltage_v
            / (ELEMENTARY_CHARGE_C * (width_um * 1e-4) ** 2)
        )
        self.assertAlmostEqual(doping, expected_doping)

        stored = stored_depletion_energy_areal_j_cm2(voltage_v, doping)
        slab_equivalent = (2.0 / 3.0) * eps * voltage_v ** 2 / (width_um * 1e-4)
        self.assertAlmostEqual(stored, slab_equivalent)

    def test_critical_voltage_round_trip(self):
        doping = net_doping_from_reachthrough(1200.0, 10.0)
        stored = stored_depletion_energy_areal_j_cm2(800.0, doping)
        critical = critical_voltage_for_areal_energy(stored, doping)

        self.assertAlmostEqual(critical, 800.0)

    def test_seb_threshold_exceeds_selc_threshold_for_same_epi(self):
        doping = net_doping_from_reachthrough(1200.0, 10.0)
        seb_v = critical_voltage_for_areal_energy(
            KOSIER_2026_SEB_CRITICAL_J_CM2, doping)
        selc_v = critical_voltage_for_areal_energy(
            KOSIER_2026_SELC_CRITICAL_J_CM2, doping)

        self.assertGreater(seb_v, selc_v)
        self.assertGreater(seb_v, 0.0)
        self.assertGreater(selc_v, 0.0)

    def test_depletion_width_and_peak_field_are_finite(self):
        doping = net_doping_from_reachthrough(1200.0, 10.0)

        self.assertAlmostEqual(depletion_width_um(1200.0, doping), 10.0)
        self.assertGreater(peak_field_v_cm(1200.0, doping), 0.0)

    def test_invalid_inputs_return_none(self):
        self.assertIsNone(net_doping_from_reachthrough(None, 10.0))
        self.assertIsNone(stored_depletion_energy_areal_j_cm2(100.0, None))
        self.assertIsNone(critical_voltage_for_areal_energy(0.0, 1e16))
        self.assertIsNone(depletion_width_um(-1.0, 1e16))


if __name__ == "__main__":
    unittest.main()
