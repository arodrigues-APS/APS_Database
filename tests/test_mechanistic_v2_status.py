import unittest

from aps.proxy.mechanistic_energy_proxy import (
    cumulative_exposure_overlap_class,
    localization_mismatch_class,
    mechanistic_energy_candidate_status,
    mechanistic_status_priority,
    terminal_energy_overlap_class,
)


def status(**kw):
    base = dict(
        regime_match_class_value="first_order_analog",
        regime_status_ceiling=None,
        match_scope="same_device",
        target_has_energy_context=True,
        measured_comparability_status="strong",
        prediction_comparability_status=None,
        target_regime="heavy_ion_hard_collapse_seb",
        candidate_pulse_count=None,
        energy_rankable=True,
    )
    base.update(kw)
    return mechanistic_energy_candidate_status(**base)


class OverlapClassTests(unittest.TestCase):
    def test_terminal_energy_overlap(self):
        self.assertEqual(terminal_energy_overlap_class(0.0), "strong_overlap")
        self.assertEqual(terminal_energy_overlap_class(1.0), "partial_overlap")
        self.assertEqual(terminal_energy_overlap_class(2.0), "near_miss")
        self.assertEqual(terminal_energy_overlap_class(5.0), "far_miss")
        self.assertEqual(terminal_energy_overlap_class(None), "missing_interval")
        # symmetric in sign
        self.assertEqual(terminal_energy_overlap_class(-1.0), "partial_overlap")

    def test_localization_mismatch(self):
        self.assertEqual(localization_mismatch_class(0.5), "comparable")
        self.assertEqual(localization_mismatch_class(1.5), "moderate_localized_vs_bulk")
        self.assertEqual(localization_mismatch_class(3.0), "large_localized_vs_bulk")
        self.assertEqual(localization_mismatch_class(-5.0), "extreme_localized_vs_bulk")
        self.assertEqual(localization_mismatch_class(None), "missing")

    def test_cumulative_exposure(self):
        self.assertEqual(
            cumulative_exposure_overlap_class("selcii_drain_source_cumulative", 5),
            "cumulative_present")
        self.assertEqual(
            cumulative_exposure_overlap_class("selci_gate_coupled", 1),
            "cumulative_missing")
        self.assertEqual(
            cumulative_exposure_overlap_class("selci_gate_coupled", None),
            "cumulative_missing")
        self.assertEqual(
            cumulative_exposure_overlap_class("heavy_ion_hard_collapse_seb", 5),
            "not_applicable")


class StatusTests(unittest.TestCase):
    def test_mismatch_ranked_as_mismatch_even_with_strong_damage(self):
        self.assertEqual(
            status(regime_match_class_value="mechanism_mismatch",
                   measured_comparability_status="strong"),
            "mechanistic_regime_mismatch")

    def test_cross_device(self):
        self.assertEqual(status(match_scope="cross_device"),
                         "mechanistic_cross_device_screening_only")

    def test_missing_energy_context(self):
        self.assertEqual(status(target_has_energy_context=False),
                         "mechanistic_missing_energy_context")

    def test_missing_damage_context(self):
        self.assertEqual(
            status(measured_comparability_status=None,
                   prediction_comparability_status=None),
            "mechanistic_missing_damage_context")

    def test_measured_first_order(self):
        self.assertEqual(status(), "mechanistic_measured_candidate")

    def test_predicted(self):
        self.assertEqual(
            status(measured_comparability_status=None,
                   prediction_comparability_status="usable"),
            "mechanistic_predicted_candidate")

    def test_ceiling_caps_to_questionable(self):
        self.assertEqual(
            status(regime_status_ceiling="analog_questionable",
                   target_regime="proton_low_collapse_seb"),
            "mechanistic_analog_questionable")

    def test_ceiling_cumulative_pair_is_cumulative_candidate(self):
        self.assertEqual(
            status(regime_status_ceiling="analog_questionable",
                   target_regime="selcii_drain_source_cumulative",
                   candidate_pulse_count=5,
                   measured_comparability_status="weak"),
            "mechanistic_cumulative_candidate")

    def test_cumulative_requires_damage_context(self):
        # A cumulative pair with no damage evidence is still missing_damage_context
        # (post-IV stays the anchor).
        self.assertEqual(
            status(regime_status_ceiling="analog_questionable",
                   target_regime="selcii_drain_source_cumulative",
                   candidate_pulse_count=5,
                   measured_comparability_status=None,
                   prediction_comparability_status=None),
            "mechanistic_missing_damage_context")

    def test_no_ceiling_cumulative_candidate(self):
        self.assertEqual(
            status(regime_status_ceiling=None,
                   target_regime="selci_gate_coupled",
                   candidate_pulse_count=3,
                   measured_comparability_status="weak"),
            "mechanistic_cumulative_candidate")

    def test_energy_screening_only(self):
        self.assertEqual(
            status(measured_comparability_status="weak",
                   candidate_pulse_count=None,
                   energy_rankable=True),
            "mechanistic_energy_screening_only")

    def test_inspect_manually(self):
        self.assertEqual(
            status(measured_comparability_status="weak",
                   candidate_pulse_count=None,
                   energy_rankable=False),
            "mechanistic_inspect_manually")


class PriorityTests(unittest.TestCase):
    def test_priority_order(self):
        self.assertEqual(mechanistic_status_priority("mechanistic_measured_candidate"), 1)
        self.assertEqual(mechanistic_status_priority("mechanistic_regime_mismatch"), 8)
        self.assertLess(
            mechanistic_status_priority("mechanistic_measured_candidate"),
            mechanistic_status_priority("mechanistic_energy_screening_only"),
        )
        self.assertEqual(mechanistic_status_priority("unknown_status"), 9)


if __name__ == "__main__":
    unittest.main()
