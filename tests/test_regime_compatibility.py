import unittest

from data_processing_scripts.mechanistic_energy_proxy import (
    CANDIDATE_REGIMES,
    TARGET_REGIMES,
    _REGIME_COMPATIBILITY,
    regime_match_class,
)


class RegimeMatchTests(unittest.TestCase):
    def test_proton_seb_prefers_short_circuit(self):
        match = regime_match_class("proton_low_collapse_seb", "sc_low_collapse")
        self.assertEqual(match.match_class, "first_order_analog")
        self.assertEqual(match.preference, 1)
        self.assertIsNone(match.status_ceiling)

    def test_proton_seb_rejects_hard_avalanche(self):
        match = regime_match_class(
            "proton_low_collapse_seb", "avalanche_hard_collapse")
        self.assertEqual(match.match_class, "mechanism_mismatch")
        self.assertEqual(match.status_ceiling, "analog_questionable")

    def test_heavy_ion_seb_prefers_avalanche(self):
        match = regime_match_class(
            "heavy_ion_hard_collapse_seb", "avalanche_hard_collapse")
        self.assertEqual(match.match_class, "first_order_analog")
        self.assertEqual(match.preference, 1)

    def test_proxy_family_split_is_opposite_for_proton_vs_heavy_ion_seb(self):
        # The core Phase-2 guard: SEB does not map to one proxy family.  Proton
        # SEB must prefer SC over avalanche; heavy-ion SEB the reverse.
        proton_sc = regime_match_class("proton_low_collapse_seb", "sc_low_collapse")
        proton_av = regime_match_class(
            "proton_low_collapse_seb", "avalanche_hard_collapse")
        self.assertLess(proton_sc.preference, proton_av.preference)

        heavy_av = regime_match_class(
            "heavy_ion_hard_collapse_seb", "avalanche_hard_collapse")
        heavy_sc = regime_match_class(
            "heavy_ion_hard_collapse_seb", "sc_low_collapse")
        self.assertLess(heavy_av.preference, heavy_sc.preference)

    def test_cumulative_targets_capped_questionable(self):
        for target in ("selci_gate_coupled", "selcii_drain_source_cumulative"):
            match = regime_match_class(target, "repetitive_sc_cumulative")
            self.assertEqual(match.match_class, "cumulative_analog")
            self.assertEqual(match.status_ceiling, "analog_questionable")

    def test_selci_treats_avalanche_as_mismatch(self):
        # SELC-I is gate-coupled; avalanche (drain-source) does not stress the
        # gate, so it is a mechanism mismatch regardless of repetition.
        for candidate in (
            "avalanche_hard_collapse",
            "avalanche_noncatastrophic",
            "repetitive_avalanche_cumulative",
        ):
            match = regime_match_class("selci_gate_coupled", candidate)
            self.assertEqual(match.match_class, "mechanism_mismatch", candidate)

    def test_selci_prefers_short_circuit_over_avalanche(self):
        sc = regime_match_class("selci_gate_coupled", "sc_low_collapse")
        av = regime_match_class("selci_gate_coupled", "repetitive_avalanche_cumulative")
        self.assertNotEqual(sc.match_class, "mechanism_mismatch")
        self.assertLess(sc.preference, av.preference)

    def test_selcii_keeps_avalanche_as_cumulative_analog(self):
        # SELC-II is drain-source cumulative; avalanche stays a valid analog.
        match = regime_match_class(
            "selcii_drain_source_cumulative", "repetitive_avalanche_cumulative")
        self.assertEqual(match.match_class, "cumulative_analog")

    def test_any_fallback(self):
        # An unseeded candidate regime falls to the target's 'any' rule.
        match = regime_match_class(
            "selcii_drain_source_cumulative", "avalanche_hard_collapse")
        self.assertEqual(match.match_class, "analog_questionable")
        self.assertEqual(match.status_ceiling, "analog_questionable")

    def test_unseeded_target_uses_global_default(self):
        match = regime_match_class("unknown_single_event", "sc_low_collapse")
        self.assertEqual(match.match_class, "analog_questionable")
        self.assertEqual(match.status_ceiling, "analog_questionable")

    def test_every_target_resolves(self):
        for target in TARGET_REGIMES:
            for candidate in CANDIDATE_REGIMES:
                match = regime_match_class(target, candidate)
                self.assertIsNotNone(match.match_class, (target, candidate))
                self.assertGreaterEqual(match.preference, 1)


class RegimeTableConsistencyTests(unittest.TestCase):
    def test_table_targets_are_known_target_regimes(self):
        for target in _REGIME_COMPATIBILITY:
            self.assertIn(target, TARGET_REGIMES, target)

    def test_table_candidates_are_known_or_any(self):
        for target, rules in _REGIME_COMPATIBILITY.items():
            for candidate in rules:
                self.assertTrue(
                    candidate == "any" or candidate in CANDIDATE_REGIMES,
                    (target, candidate),
                )

    def test_every_target_has_an_any_fallback(self):
        for target, rules in _REGIME_COMPATIBILITY.items():
            self.assertIn("any", rules, target)


class PathPenaltyTests(unittest.TestCase):
    """The v1-scale path_penalty carried on every regime rule (2026-07-02) so
    the Phase-C v1 flip is a source swap.  Mapping mirrors the v1 constants:
    first_order 0.15, secondary 0.25, gate/cumulative analogs 0.50,
    questionable/mismatch 0.75."""

    _EXPECTED_BY_CLASS = {
        "first_order_analog": 0.15,
        "secondary_analog": 0.25,
        "gate_coupled_analog": 0.50,
        "cumulative_analog": 0.50,
        "analog_questionable": 0.75,
        "mechanism_mismatch": 0.75,
    }

    def test_every_rule_carries_the_class_mapped_penalty(self):
        for target, rules in _REGIME_COMPATIBILITY.items():
            for candidate, rule in rules.items():
                self.assertIsNotNone(rule.path_penalty, (target, candidate))
                self.assertEqual(
                    rule.path_penalty,
                    self._EXPECTED_BY_CLASS[rule.match_class],
                    (target, candidate, rule.match_class),
                )

    def test_first_order_analogs_get_the_lowest_penalty(self):
        proton = regime_match_class("proton_low_collapse_seb", "sc_low_collapse")
        heavy = regime_match_class(
            "heavy_ion_hard_collapse_seb", "avalanche_hard_collapse")
        self.assertEqual(proton.path_penalty, 0.15)
        self.assertEqual(heavy.path_penalty, 0.15)

    def test_default_fallback_penalty(self):
        match = regime_match_class("unseeded_regime", "whatever")
        self.assertEqual(match.path_penalty, 0.75)


if __name__ == "__main__":
    unittest.main()
