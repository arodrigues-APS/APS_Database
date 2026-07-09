import unittest

from aps.proxy.calibrate_proxy_distance import (
    DistanceSettings,
    ranked_candidate_items,
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
        energy_log_weight=0.0,
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

    def test_energy_delta_does_not_change_waveform_status(self):
        row = calibration_row(None)
        row["target_match_tier"] = "energy_comparable"
        row["log_energy_delta"] = 10.0

        scored = score_row(row, default_settings())

        self.assertEqual(scored["candidate_status"], "measured_damage_candidate")

    def test_signature_axis_distance_excludes_path_penalty(self):
        scored = score_row(calibration_row(None), default_settings())

        self.assertLess(
            scored["signature_axis_distance"],
            scored["damage_signature_distance"],
        )

    def test_mask_aware_rank_does_not_compare_raw_distance_across_masks(self):
        low_distance_collapse_only = (
            {"candidate_source": "avalanche", "candidate_stress_record_key": "a"},
            {
                "damage_signature_axis_mask": "collapse",
                "signature_axis_distance": 0.01,
                "waveform_distance": 0.01,
                "candidate_rank_penalty": 0,
                "candidate_status_priority": 1,
                "mechanism_preference": 2,
                "damage_signature_axes_used": 1,
            },
        )
        higher_distance_richer_mask = (
            {"candidate_source": "sc", "candidate_stress_record_key": "b"},
            {
                "damage_signature_axis_mask": "collapse+gate",
                "signature_axis_distance": 0.50,
                "waveform_distance": 0.50,
                "candidate_rank_penalty": 0,
                "candidate_status_priority": 1,
                "mechanism_preference": 1,
                "damage_signature_axes_used": 2,
            },
        )

        ranked = ranked_candidate_items([
            low_distance_collapse_only,
            higher_distance_richer_mask,
        ])

        self.assertEqual(ranked[0][0]["candidate_stress_record_key"], "b")
        self.assertEqual(ranked[0][1]["damage_signature_mask_rank"], 1)
        self.assertEqual(ranked[1][1]["damage_signature_mask_rank"], 1)

    def test_axes_richness_breaks_cross_mask_source_tie(self):
        collapse_only = (
            {"candidate_source": "avalanche", "candidate_stress_record_key": "a"},
            {
                "damage_signature_axis_mask": "collapse",
                "signature_axis_distance": 0.10,
                "waveform_distance": 0.10,
                "candidate_rank_penalty": 0,
                "candidate_status_priority": 1,
                "mechanism_preference": 1,
                "damage_signature_axes_used": 1,
            },
        )
        richer_sc = (
            {"candidate_source": "sc", "candidate_stress_record_key": "b"},
            {
                "damage_signature_axis_mask": "collapse+normalized_vds",
                "signature_axis_distance": 0.10,
                "waveform_distance": 0.10,
                "candidate_rank_penalty": 0,
                "candidate_status_priority": 1,
                "mechanism_preference": 1,
                "damage_signature_axes_used": 2,
            },
        )

        ranked = ranked_candidate_items([collapse_only, richer_sc])
        self.assertEqual(ranked[0][0]["candidate_stress_record_key"], "b")

        ranked_reversed = ranked_candidate_items([richer_sc, collapse_only])
        self.assertEqual(ranked_reversed[0][0]["candidate_stress_record_key"], "b")

    def test_within_one_axis_mask_distance_ranks_first(self):
        far = (
            {"candidate_source": "sc", "candidate_stress_record_key": "far"},
            {
                "damage_signature_axis_mask": "collapse+gate",
                "signature_axis_distance": 0.50,
                "waveform_distance": 0.50,
                "candidate_rank_penalty": 0,
                "candidate_status_priority": 1,
                "mechanism_preference": 1,
            },
        )
        close = (
            {"candidate_source": "sc", "candidate_stress_record_key": "close"},
            {
                "damage_signature_axis_mask": "collapse+gate",
                "signature_axis_distance": 0.10,
                "waveform_distance": 0.10,
                "candidate_rank_penalty": 0,
                "candidate_status_priority": 1,
                "mechanism_preference": 1,
            },
        )

        ranked = ranked_candidate_items([far, close])

        self.assertEqual(ranked[0][0]["candidate_stress_record_key"], "close")
        self.assertEqual(ranked[0][1]["damage_signature_mask_rank"], 1)
        self.assertEqual(ranked[1][1]["damage_signature_mask_rank"], 2)


if __name__ == "__main__":
    unittest.main()
