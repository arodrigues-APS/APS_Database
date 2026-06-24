import unittest

from data_processing_scripts.calibrate_proxy_distance import (
    DistanceSettings,
    score_row,
)


def default_settings():
    return DistanceSettings(
        setting_name="default",
        description="unit test defaults",
        max_energy_log_delta=5.0,
        collapse_delta_scale=0.25,
        gate_delta_scale=0.20,
        normalized_vds_delta_scale=0.15,
        energy_log_weight=1.0,
        same_path_penalty=0.0,
        path_unknown_penalty=0.25,
        path_mismatch_penalty=0.75,
        duration_log_weight=0.01,
        best_damage_distance_fallback=2.50,
        energy_out_of_range_log_delta=4.0,
        damage_signature_mismatch_distance=2.50,
        measured_exact_waveform_max=1.75,
        predicted_waveform_max=1.75,
        device_run_waveform_max=2.25,
        weak_waveform_max=3.00,
        waveform_only_max=1.25,
        high_confidence_combined_max=1.50,
    )


def calibration_row(normalized_vds_delta):
    return {
        "target_match_tier": "energy_censored_damage_signature_only",
        "log_energy_delta": None,
        "collapse_delta": 0.05,
        "gate_delta": 0.02,
        "normalized_vds_delta": normalized_vds_delta,
        "duration_log_delta": 0.0,
        "path_penalty": 0.15,
        "damage_distance": 0.25,
        "damage_comparability_status": "usable",
        "mechanism_status_ceiling": None,
        "target_energy_floor_j": None,
        "candidate_energy_j": 1.0,
    }


class ProxyDistanceCalibrationTests(unittest.TestCase):
    def test_large_normalized_vds_delta_blocks_otherwise_close_candidate(self):
        scored = score_row(calibration_row(0.70), default_settings())

        self.assertEqual(scored["damage_signature_axes_used"], 3)
        self.assertGreater(scored["damage_signature_distance"], 2.50)
        self.assertEqual(scored["candidate_status"], "damage_signature_mismatch")

    def test_omitted_avalanche_vds_axis_preserves_damage_candidate(self):
        scored = score_row(calibration_row(None), default_settings())

        self.assertEqual(scored["damage_signature_axes_used"], 2)
        self.assertLess(scored["damage_signature_distance"], 2.50)
        self.assertEqual(scored["candidate_status"], "measured_damage_candidate")


if __name__ == "__main__":
    unittest.main()
