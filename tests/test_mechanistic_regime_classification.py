import unittest

from aps.proxy.mechanistic_energy_proxy import (
    CANDIDATE_REGIMES,
    TARGET_REGIMES,
    classify_mechanistic_regime,
    is_proton,
)


class IsProtonTests(unittest.TestCase):
    def test_aliases(self):
        for alias in ("p", "Proton", " PROTONS ", "H+", "h1", "1H"):
            self.assertTrue(is_proton(alias), alias)

    def test_substring(self):
        self.assertTrue(is_proton("200 MeV proton"))

    def test_non_proton(self):
        for ion in ("Au", "Xe", "Ar", "", None, "gold"):
            self.assertFalse(is_proton(ion), ion)


class IrradiationRegimeTests(unittest.TestCase):
    def test_proton_low_collapse_seb(self):
        regime = classify_mechanistic_regime(
            "irradiation", event_type="SEB", ion_species="proton",
            vds_collapse_fraction=0.0,
        )
        self.assertEqual(regime, "proton_low_collapse_seb")

    def test_proton_high_field_seb(self):
        regime = classify_mechanistic_regime(
            "irradiation", event_type="SEB", ion_species="proton",
            vds_collapse_fraction=0.9,
        )
        self.assertEqual(regime, "proton_high_field_seb")

    def test_heavy_ion_hard_collapse_seb(self):
        regime = classify_mechanistic_regime(
            "irradiation", event_type="SEB", ion_species="Au",
            vds_collapse_fraction=0.99,
        )
        self.assertEqual(regime, "heavy_ion_hard_collapse_seb")

    def test_heavy_ion_seb_without_collapse_is_unknown(self):
        regime = classify_mechanistic_regime(
            "irradiation", event_type="SEB", ion_species="Au",
            vds_collapse_fraction=None,
        )
        self.assertEqual(regime, "unknown_single_event")

    def test_selc_and_mixed(self):
        self.assertEqual(
            classify_mechanistic_regime("irradiation", event_type="SELCI"),
            "selci_gate_coupled",
        )
        self.assertEqual(
            classify_mechanistic_regime("irradiation", event_type="SELCII"),
            "selcii_drain_source_cumulative",
        )
        self.assertEqual(
            classify_mechanistic_regime("irradiation", event_type="MIXED"),
            "mixed_single_event",
        )

    def test_proton_non_see_is_tid_dd(self):
        regime = classify_mechanistic_regime(
            "irradiation", event_type="UNKNOWN", ion_species="proton",
        )
        self.assertEqual(regime, "tid_dd_cumulative")

    def test_seb_label_alone_does_not_collapse_to_one_family(self):
        # The cardinal rule: a low-collapse proton SEB and a heavy-ion SEB must
        # not receive the same regime just because both are labeled SEB.
        proton = classify_mechanistic_regime(
            "irradiation", event_type="SEB", ion_species="proton",
            vds_collapse_fraction=0.0,
        )
        heavy = classify_mechanistic_regime(
            "irradiation", event_type="SEB", ion_species="Xe",
            vds_collapse_fraction=0.97,
        )
        self.assertNotEqual(proton, heavy)


class ElectricalRegimeTests(unittest.TestCase):
    def test_repetitive_avalanche(self):
        regime = classify_mechanistic_regime(
            "avalanche", vds_collapse_fraction=0.99, pulse_count_in_sequence=5,
        )
        self.assertEqual(regime, "repetitive_avalanche_cumulative")

    def test_avalanche_hard_collapse(self):
        regime = classify_mechanistic_regime(
            "avalanche", vds_collapse_fraction=0.99, pulse_count_in_sequence=1,
        )
        self.assertEqual(regime, "avalanche_hard_collapse")

    def test_avalanche_catastrophic_without_collapse(self):
        regime = classify_mechanistic_regime(
            "avalanche", vds_collapse_fraction=None, is_catastrophic=True,
        )
        self.assertEqual(regime, "avalanche_hard_collapse")

    def test_avalanche_noncatastrophic(self):
        regime = classify_mechanistic_regime(
            "avalanche", vds_collapse_fraction=0.1,
        )
        self.assertEqual(regime, "avalanche_noncatastrophic")

    def test_sc_regimes(self):
        self.assertEqual(
            classify_mechanistic_regime("sc", vds_collapse_fraction=0.0),
            "sc_low_collapse",
        )
        self.assertEqual(
            classify_mechanistic_regime("sc", vds_collapse_fraction=0.9),
            "sc_high_power_short_pulse",
        )
        self.assertEqual(
            classify_mechanistic_regime(
                "sc", vds_collapse_fraction=0.9, pulse_count_in_sequence=3),
            "repetitive_sc_cumulative",
        )

    def test_unknown_source(self):
        self.assertEqual(
            classify_mechanistic_regime("mystery"), "unknown_electrical_proxy")


class RegimeVocabularyTests(unittest.TestCase):
    def test_target_and_candidate_vocabularies_disjoint(self):
        self.assertEqual(TARGET_REGIMES & CANDIDATE_REGIMES, set())

    def test_irradiation_always_target_regime(self):
        for event in ("SEB", "SELCI", "SELCII", "MIXED", "UNKNOWN", None):
            regime = classify_mechanistic_regime(
                "irradiation", event_type=event, ion_species="Au",
                vds_collapse_fraction=0.99,
            )
            self.assertIn(regime, TARGET_REGIMES, event)

    def test_electrical_always_candidate_regime(self):
        for source in ("sc", "avalanche"):
            regime = classify_mechanistic_regime(
                source, vds_collapse_fraction=0.5)
            self.assertIn(regime, CANDIDATE_REGIMES, source)


if __name__ == "__main__":
    unittest.main()
