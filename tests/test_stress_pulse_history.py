import unittest

from data_processing_scripts.extract_stress_pulse_history import (
    build_history_rows,
    parse_avalanche_pulse_index,
    parse_sc_pulse_index,
)


class StressPulseHistoryTests(unittest.TestCase):
    def test_avalanche_vg_counter_matches_ingestion_semantics(self):
        pulse_index, basis = parse_avalanche_pulse_index(
            "RP10_1.4J_Vg-1000001.h5"
        )

        self.assertEqual(pulse_index, 1)
        self.assertEqual(basis, "avalanche_filename_vg_counter")

    def test_avalanche_metadata_counter_takes_precedence(self):
        pulse_index, basis = parse_avalanche_pulse_index(
            "C2M0080120D_C100002.h5",
            metadata_index=42,
        )

        self.assertEqual(pulse_index, 42)
        self.assertEqual(basis, "avalanche_shot_index_metadata")

    def test_sc_pulse_counter_patterns_are_supported(self):
        self.assertEqual(
            parse_sc_pulse_index("sample_pulse12.csv"),
            (12, "sc_filename_pulse_counter"),
        )
        self.assertEqual(
            parse_sc_pulse_index("device/3_after800V10us/capture.csv"),
            (3, "sc_filename_after_counter"),
        )

    def test_history_groups_by_physical_sample_and_accumulates_energy(self):
        rows = [
            {
                "id": 2,
                "data_source": "avalanche",
                "device_type": "C2M0080120D",
                "device_id": "C10",
                "sample_group": "C10",
                "filename": "C10_1.2J_Vg-1000001.h5",
                "avalanche_shot_index": 1,
                "avalanche_energy_j": 1.2,
            },
            {
                "id": 1,
                "data_source": "avalanche",
                "device_type": "C2M0080120D",
                "device_id": "C10",
                "sample_group": "C10",
                "filename": "C10_1.0J_Vg-1000000.h5",
                "avalanche_shot_index": 0,
                "avalanche_energy_j": 1.0,
            },
        ]

        history = build_history_rows(rows)

        self.assertEqual([row.metadata_id for row in history], [1, 2])
        self.assertEqual([row.pulse_count_in_sequence for row in history], [1, 2])
        self.assertEqual(history[0].sequence_key, history[1].sequence_key)
        self.assertAlmostEqual(history[0].cumulative_energy_j, 1.0)
        self.assertAlmostEqual(history[1].cumulative_energy_j, 2.2)

    def test_sc_rows_without_explicit_counter_are_not_inferred(self):
        rows = [
            {
                "id": 10,
                "data_source": "sc_ruggedness",
                "measurement_category": "SC_Waveform",
                "device_type": "C2M0080120D",
                "device_id": "IMC31",
                "sample_group": "IMC31",
                "filename": "600V8us.csv",
                "sc_voltage_v": 600.0,
                "sc_duration_us": 8.0,
                "sc_sequence_num": None,
            }
        ]

        self.assertEqual(build_history_rows(rows), [])


if __name__ == "__main__":
    unittest.main()
