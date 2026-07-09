"""Destruction-boundary interval + candidate failure-fraction helpers (R1 prep).

These pure functions in aps/proxy/mechanistic_energy_proxy.py are
the spec the future boundary SQL view must mirror.  They pin the review's
degenerate-case rules (2026-07-02): inverted brackets are emitted and flagged,
never discarded; thin cells and one-sided/missing brackets fall to
missing_interval; destructive energy is documented as right-censored; the
failure fraction refuses to gate across energy-basis families or for
repetitive candidate regimes.
"""

import math
import unittest

from aps.proxy.mechanistic_energy_proxy import (
    BOUNDARY_MIN_DESTRUCTIVE_COUNT,
    BOUNDARY_MIN_SURVIVED_COUNT,
    BOUNDARY_UNKNOWN_OUTCOME_NOTE,
    REPETITIVE_CANDIDATE_REGIMES,
    WU_2024_AVALANCHE_FAILURE_TJ_BAND_K,
    WU_2024_SIC_INTRINSIC_LIMIT_K,
    WU_2024_TCRIT_AL_MELT_K,
    candidate_failure_fraction,
    destruction_boundary_interval,
    energy_basis_family,
    survived_evidence,
)


def _boundary(max_survived=1.0, min_destructive=2.0, survived=5, destructive=5,
              **kwargs):
    return destruction_boundary_interval(
        max_survived, min_destructive, survived, destructive, **kwargs)


class BoundaryIntervalTests(unittest.TestCase):
    def test_normal_bracket(self):
        b = _boundary()
        self.assertEqual((b["low_j"], b["high_j"]), (1.0, 2.0))
        self.assertFalse(b["inverted"])
        self.assertTrue(b["usable"])
        self.assertEqual(b["blockers"], [])
        self.assertIn("destructive_energy_right_censored_lower_bound", b["notes"])

    def test_inverted_bracket_is_flagged_not_discarded(self):
        # Unit-to-unit spread: a survivor above another unit's failure energy.
        b = _boundary(max_survived=3.0, min_destructive=2.0)
        self.assertEqual((b["low_j"], b["high_j"]), (2.0, 3.0))
        self.assertTrue(b["inverted"])
        self.assertTrue(b["usable"])
        self.assertIn(
            "destruction_boundary_brackets_inverted_unit_spread", b["notes"])

    def test_one_sided_survived_only(self):
        b = _boundary(min_destructive=None)
        self.assertIsNone(b["low_j"])
        self.assertIsNone(b["high_j"])
        self.assertFalse(b["usable"])
        self.assertIn("destruction_boundary_one_sided_survived_only", b["blockers"])
        # No destructive bound -> no censoring note.
        self.assertNotIn(
            "destructive_energy_right_censored_lower_bound", b["notes"])

    def test_one_sided_destructive_only(self):
        b = _boundary(max_survived=None)
        self.assertFalse(b["usable"])
        self.assertIn(
            "destruction_boundary_one_sided_destructive_only", b["blockers"])
        self.assertIn("destructive_energy_right_censored_lower_bound", b["notes"])

    def test_missing_both(self):
        b = _boundary(max_survived=None, min_destructive=None)
        self.assertFalse(b["usable"])
        self.assertEqual(b["blockers"], ["destruction_boundary_missing"])

    def test_insufficient_counts_block_gating_but_keep_bracket(self):
        b = _boundary(survived=BOUNDARY_MIN_SURVIVED_COUNT - 1)
        self.assertEqual((b["low_j"], b["high_j"]), (1.0, 2.0))
        self.assertFalse(b["usable"])
        self.assertIn(
            "destruction_boundary_insufficient_survived_count", b["blockers"])
        b = _boundary(destructive=BOUNDARY_MIN_DESTRUCTIVE_COUNT - 1)
        self.assertFalse(b["usable"])
        self.assertIn(
            "destruction_boundary_insufficient_destructive_count", b["blockers"])

    def test_nonpositive_energies_treated_missing(self):
        b = _boundary(max_survived=0.0, min_destructive=-1.0)
        self.assertFalse(b["usable"])
        self.assertIn("destruction_boundary_missing", b["blockers"])

    def test_degenerate_equal_bounds(self):
        b = _boundary(max_survived=2.0, min_destructive=2.0)
        self.assertEqual((b["low_j"], b["high_j"]), (2.0, 2.0))
        self.assertFalse(b["inverted"])
        self.assertTrue(b["usable"])

    def test_unknown_outcome_count_is_visible_note_not_bracket_side(self):
        b = _boundary(max_survived=1.0, min_destructive=4.0,
                      survived=1, destructive=3, unknown_outcome_count=2)
        self.assertEqual((b["low_j"], b["high_j"]), (1.0, 4.0))
        self.assertIn(BOUNDARY_UNKNOWN_OUTCOME_NOTE, b["notes"])
        self.assertIn("destruction_boundary_insufficient_survived_count",
                      b["blockers"])
        self.assertFalse(b["usable"])


class SurvivedEvidenceTests(unittest.TestCase):
    def test_post_iv_measured_is_survived_evidence(self):
        self.assertTrue(survived_evidence("post_iv_measured", None))

    def test_nonfail_avalanche_outcome_is_survived_evidence(self):
        self.assertTrue(survived_evidence("unknown_no_post_iv", "pass"))
        self.assertTrue(survived_evidence("unknown_no_post_iv", "survived"))

    def test_unknown_and_latent_values_are_not_survived_without_outcome(self):
        self.assertFalse(survived_evidence("unknown_no_post_iv", None))
        self.assertFalse(survived_evidence("potentially_reversible_or_latent", None))

    def test_fail_outcome_is_not_survived_evidence(self):
        self.assertFalse(survived_evidence("unknown_no_post_iv", "FAIL catastrophic"))

    def test_destructive_row_is_never_survived_evidence(self):
        # Contradictory metadata (catastrophic flag + non-fail outcome string)
        # must not put one row on BOTH bracket sides.
        self.assertFalse(survived_evidence("destructive_or_catastrophic", "pass"))
        self.assertFalse(survived_evidence("destructive_or_catastrophic", None))


class FailureFractionTests(unittest.TestCase):
    def test_point_fraction_uses_geometric_mean(self):
        b = _boundary(max_survived=1.0, min_destructive=4.0)
        f = candidate_failure_fraction(4.0, b)
        self.assertAlmostEqual(f["fraction_point"], 4.0 / math.sqrt(4.0))
        self.assertAlmostEqual(f["fraction_low"], 1.0)   # energy / high end
        self.assertAlmostEqual(f["fraction_high"], 4.0)  # energy / low end
        self.assertTrue(f["usable"])

    def test_unusable_boundary_propagates(self):
        b = _boundary(min_destructive=None)
        f = candidate_failure_fraction(4.0, b)
        self.assertIsNone(f["fraction_point"])
        self.assertFalse(f["usable"])
        self.assertIn("destruction_boundary_one_sided_survived_only", f["blockers"])

    def test_basis_family_mismatch_displays_but_never_gates(self):
        b = _boundary()
        f = candidate_failure_fraction(
            1.5, b,
            candidate_energy_basis="integrated_event_vds_id",
            boundary_energy_basis="commanded_or_stored",
        )
        # Fraction stays visible for screening, but must not gate/rank.
        self.assertIsNotNone(f["fraction_point"])
        self.assertFalse(f["usable"])
        self.assertIn("boundary_energy_basis_family_mismatch", f["blockers"])

    def test_matching_basis_families_gate(self):
        b = _boundary()
        f = candidate_failure_fraction(
            1.5, b,
            candidate_energy_basis="integrated_event_vds_id",
            boundary_energy_basis="integrated_file_vds_id",
        )
        self.assertTrue(f["usable"])

    def test_repetitive_candidate_regime_excluded(self):
        b = _boundary()
        for regime in REPETITIVE_CANDIDATE_REGIMES:
            f = candidate_failure_fraction(1.5, b, candidate_regime=regime)
            self.assertFalse(f["usable"], regime)
            self.assertIn("boundary_repetitive_regime_excluded", f["blockers"])
        # Single-pulse regimes pass through.
        f = candidate_failure_fraction(
            1.5, b, candidate_regime="avalanche_hard_collapse")
        self.assertTrue(f["usable"])

    def test_missing_candidate_energy(self):
        b = _boundary()
        f = candidate_failure_fraction(None, b)
        self.assertIsNone(f["fraction_point"])
        self.assertFalse(f["usable"])
        self.assertIn("candidate_energy_missing", f["blockers"])

    def test_censoring_note_travels_with_fraction(self):
        b = _boundary()
        f = candidate_failure_fraction(1.5, b)
        self.assertIn("destructive_energy_right_censored_lower_bound", f["notes"])


class EnergyBasisFamilyTests(unittest.TestCase):
    def test_families(self):
        self.assertEqual(energy_basis_family("integrated_event_vds_id"), "integrated")
        self.assertEqual(energy_basis_family("integrated_file_vds_id"), "integrated")
        self.assertEqual(energy_basis_family("commanded_or_stored"), "commanded_or_stored")
        self.assertEqual(
            energy_basis_family("proxy_event_rectangular"), "proxy")
        self.assertEqual(energy_basis_family(""), "missing")
        self.assertEqual(energy_basis_family(None), "missing")
        self.assertEqual(energy_basis_family("something_else"), "other")

    def test_excluded_proxy_variant_is_proxy_family(self):
        self.assertEqual(
            energy_basis_family("proxy_event_rectangular_excluded"), "proxy")


class WuConstantsTests(unittest.TestCase):
    def test_pinned_temperatures_are_ordered(self):
        # T_CRIT (Al melt) sits below the 4H-SiC intrinsic limit; the surveyed
        # avalanche-failure junction-T band is ascending and brackets T_CRIT
        # from below at its low end.
        self.assertLess(WU_2024_TCRIT_AL_MELT_K, WU_2024_SIC_INTRINSIC_LIMIT_K)
        low, high = WU_2024_AVALANCHE_FAILURE_TJ_BAND_K
        self.assertLess(low, high)
        self.assertLess(low, WU_2024_TCRIT_AL_MELT_K)
        self.assertLess(WU_2024_TCRIT_AL_MELT_K, high)


if __name__ == "__main__":
    unittest.main()
