import unittest

from data_processing_scripts.seed_radiation_dose_foundation import (
    KOSIER_TABLE_I_DOPING_BASIS,
    sic_active_layer_defaults,
    voltage_class_from_rating,
)


class SeedRadiationDoseFoundationTests(unittest.TestCase):
    def test_kosier_table_i_classes_seed_measured_drift_inputs(self):
        cases = {
            1200.0: (1200, 10.0, 8.0e15),
            1700.0: (1700, 12.0, 7.0e15),
            3300.0: (3300, 30.0, 3.0e15),
            4500.0: (4500, 40.0, 2.0e15),
            6500.0: (6500, 70.0, 1.3e15),
            10000.0: (10000, 110.0, 0.6e15),
        }

        for voltage, (voltage_class, thickness_um, doping_cm3) in cases.items():
            with self.subTest(voltage=voltage):
                self.assertEqual(voltage_class_from_rating(voltage), voltage_class)
                seeded = sic_active_layer_defaults(voltage)
                self.assertEqual(seeded[0], thickness_um)
                self.assertIn("kosier_2026_table_i", seeded[1])
                self.assertEqual(seeded[2], doping_cm3)
                self.assertEqual(seeded[3], KOSIER_TABLE_I_DOPING_BASIS)

    def test_out_of_scope_low_voltage_classes_do_not_seed_table_i_doping(self):
        self.assertEqual(voltage_class_from_rating(650.0), 650)
        self.assertEqual(sic_active_layer_defaults(650.0), (
            6.0,
            "650v_class_active_sic_estimate_out_of_kosier_table_i_scope",
            None,
            None,
        ))
        self.assertEqual(voltage_class_from_rating(900.0), 900)
        self.assertEqual(sic_active_layer_defaults(900.0), (
            8.0,
            "900v_class_active_sic_estimate_out_of_kosier_table_i_scope",
            None,
            None,
        ))

    def test_unknown_voltage_keeps_existing_1200v_like_thickness_default(self):
        self.assertIsNone(voltage_class_from_rating(None))
        self.assertEqual(sic_active_layer_defaults(None), (
            10.0,
            "unknown_voltage_default_1200v_like",
            None,
            None,
        ))


if __name__ == "__main__":
    unittest.main()
