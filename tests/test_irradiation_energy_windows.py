import unittest

from aps.enrich.irradiation_energy_windows import infer_energy_window


def point(index, time_s, vds=100.0, id_drain=1e-6, fluence=None):
    return {
        "point_index": index,
        "time_val": time_s,
        "v_drain": vds,
        "i_drain": id_drain,
        "i_gate": 0.0,
        "fluence": fluence,
    }


class IrradiationEnergyWindowTests(unittest.TestCase):
    def test_active_fluence_window_excludes_pre_and_post_idle_rows(self):
        points = [
            point(0, 0.0, fluence=0.0),
            point(1, 1.0, fluence=0.0),
            point(2, 2.0, fluence=5.0),
            point(3, 3.0, fluence=10.0),
            point(4, 4.0, fluence=10.0),
        ]

        window, flags = infer_energy_window(points, metadata_id=101)

        self.assertEqual(window["active_window_basis"], "fluence_positive_delta")
        self.assertEqual(window["energy_censored_reason"], "none")
        self.assertTrue(window["energy_is_comparable"])
        self.assertEqual(window["active_start_s"], 1.0)
        self.assertEqual(window["active_end_s"], 3.0)
        self.assertEqual(window["energy_start_s"], 1.0)
        self.assertEqual(window["energy_end_s"], 3.0)
        self.assertFalse(flags[0]["is_active_beam"])
        self.assertTrue(flags[1]["is_energy_integrable"])
        self.assertTrue(flags[2]["is_energy_integrable"])
        self.assertTrue(flags[3]["is_energy_integrable"])
        self.assertFalse(flags[4]["is_energy_integrable"])
        self.assertEqual(flags[4]["exclusion_reason"], "outside_active_window")

    def test_metadata_compliance_censors_mid_event(self):
        points = [
            point(0, 0.0, id_drain=1e-6, fluence=0.0),
            point(1, 1.0, id_drain=2e-6, fluence=5.0),
            point(2, 2.0, id_drain=9.95e-3, fluence=10.0),
            point(3, 3.0, id_drain=1.1e-2, fluence=15.0),
        ]

        window, flags = infer_energy_window(
            points, metadata_id=102, compliance_ch1=0.01)

        self.assertEqual(window["energy_censored_reason"], "current_compliance")
        self.assertEqual(window["compliance_source"], "metadata")
        self.assertEqual(window["failure_time_s"], 2.0)
        self.assertEqual(window["energy_end_s"], 2.0)
        self.assertFalse(window["energy_is_comparable"])
        self.assertFalse(flags[3]["is_energy_integrable"])
        self.assertEqual(flags[3]["exclusion_reason"], "post_failure_or_compliance")

    def test_catastrophic_trace_abort_uses_heuristic_censoring(self):
        points = [
            point(0, 0.0, id_drain=1e-6, fluence=0.0),
            point(1, 1.0, id_drain=2e-6, fluence=5.0),
            point(2, 2.0, id_drain=1.2e-2, fluence=10.0),
            point(3, 3.0, id_drain=2.0e-2, fluence=15.0),
        ]

        window, flags = infer_energy_window(points, metadata_id=103)

        self.assertEqual(window["energy_censored_reason"],
                         "heuristic_current_plateau")
        self.assertEqual(window["compliance_source"], "heuristic")
        self.assertEqual(window["failure_time_s"], 2.0)
        self.assertEqual(window["energy_end_s"], 2.0)
        self.assertFalse(window["energy_is_comparable"])
        self.assertFalse(flags[3]["is_energy_integrable"])

    def test_no_fluence_column_does_not_assume_full_file_active_energy(self):
        points = [
            point(0, 0.0),
            point(1, 1.0),
            point(2, 2.0),
        ]

        window, flags = infer_energy_window(points, metadata_id=104)

        self.assertEqual(window["active_window_basis"], "unknown_no_fluence")
        self.assertEqual(window["energy_censored_reason"], "active_window_unknown")
        self.assertIsNone(window["energy_start_s"])
        self.assertIsNone(window["energy_end_s"])
        self.assertFalse(window["energy_is_comparable"])
        self.assertTrue(all(not row["is_energy_integrable"] for row in flags))

    def test_decreasing_fluence_marks_reset_artifact_uncertain(self):
        points = [
            point(0, 0.0, fluence=0.0),
            point(1, 1.0, fluence=5.0),
            point(2, 2.0, fluence=1.0),
            point(3, 3.0, fluence=6.0),
        ]

        window, flags = infer_energy_window(points, metadata_id=105)

        self.assertEqual(window["active_window_basis"], "fluence_reset_artifact")
        self.assertEqual(window["energy_censored_reason"], "fluence_reset_artifact")
        self.assertIsNone(window["energy_start_s"])
        self.assertIsNone(window["energy_end_s"])
        self.assertFalse(window["energy_is_comparable"])
        self.assertTrue(any(row["is_active_beam"] for row in flags))
        self.assertTrue(all(not row["is_energy_integrable"] for row in flags))


if __name__ == "__main__":
    unittest.main()
