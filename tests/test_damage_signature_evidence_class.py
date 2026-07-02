"""Tests for the damage-signature evidence-coverage model.

These assert the pure-Python helper that mirrors the `distances`/`coverage`
CTEs in schema/025_proxy_readiness_waveforms.sql.  Keep both in sync.
"""

import math
import unittest

from data_processing_scripts.calibrate_proxy_distance import (
    damage_signature_evidence,
    proxy_claim,
    signature_claim_quality,
)


def row(collapse=None, gate=None, norm=None):
    return {
        "collapse_delta": collapse,
        "gate_delta": gate,
        "normalized_vds_delta": norm,
    }


def claim_row(**overrides):
    base = {
        "candidate_status": "waveform_only_candidate",
        "match_scope": "same_device",
        "damage_evidence_tier": "waveform_only",
        "measured_comparability_status": None,
        "measured_match_scope": None,
        "measured_sign_mismatch_axis_count": 0,
        "prediction_sign_mismatch_axis_count": 0,
        "damage_signature_axes_used": 2,
        "damage_signature_evidence_class": "collapse_bias_signature",
        "has_collapse_overlap": True,
        "has_normalized_vds_overlap": True,
        "candidate_source": "sc",
        "target_match_tier": "energy_comparable",
        "mechanism_status_ceiling": None,
        "candidate_blockers": [],
    }
    base.update(overrides)
    return base


class DamageSignatureEvidenceTest(unittest.TestCase):
    def test_full_signature(self):
        ev = damage_signature_evidence(row(collapse=0.1, gate=0.2, norm=0.3))
        self.assertEqual(ev["damage_signature_evidence_class"], "full_signature")
        self.assertEqual(ev["damage_signature_evidence_tier"], 1)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 1.0)
        self.assertEqual(ev["damage_signature_axis_mask"], "collapse+gate+normalized_vds")
        self.assertEqual(ev["damage_signature_missing_axes"], [])
        self.assertTrue(ev["has_collapse_overlap"])
        self.assertTrue(ev["has_gate_overlap"])
        self.assertTrue(ev["has_normalized_vds_overlap"])

    def test_collapse_bias_signature(self):
        ev = damage_signature_evidence(row(collapse=0.1, norm=0.3))
        self.assertEqual(
            ev["damage_signature_evidence_class"], "collapse_bias_signature"
        )
        self.assertEqual(ev["damage_signature_evidence_tier"], 2)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 0.80)
        self.assertEqual(ev["damage_signature_missing_axes"], ["gate_delta"])
        self.assertEqual(ev["damage_signature_axis_mask"], "collapse+normalized_vds")

    def test_collapse_gate_signature(self):
        ev = damage_signature_evidence(row(collapse=0.1, gate=0.2))
        self.assertEqual(
            ev["damage_signature_evidence_class"], "collapse_gate_signature"
        )
        self.assertEqual(ev["damage_signature_evidence_tier"], 3)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 0.65)

    def test_collapse_only_signature(self):
        ev = damage_signature_evidence(row(collapse=0.1))
        self.assertEqual(
            ev["damage_signature_evidence_class"], "collapse_only_signature"
        )
        self.assertEqual(ev["damage_signature_evidence_tier"], 4)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 0.45)
        self.assertEqual(
            sorted(ev["damage_signature_missing_axes"]),
            ["gate_delta", "normalized_vds_delta"],
        )

    def test_bias_only_signature(self):
        ev = damage_signature_evidence(row(norm=0.3))
        self.assertEqual(ev["damage_signature_evidence_class"], "bias_only_signature")
        self.assertEqual(ev["damage_signature_evidence_tier"], 6)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 0.35)

    def test_gate_only_signature(self):
        ev = damage_signature_evidence(row(gate=0.2))
        self.assertEqual(ev["damage_signature_evidence_class"], "gate_only_signature")
        self.assertEqual(ev["damage_signature_evidence_tier"], 5)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 0.20)

    def test_no_signature_overlap(self):
        ev = damage_signature_evidence(row())
        self.assertEqual(ev["damage_signature_evidence_class"], "no_signature_overlap")
        self.assertEqual(ev["damage_signature_evidence_tier"], 9)
        self.assertAlmostEqual(ev["damage_signature_coverage_score"], 0.0)
        self.assertEqual(ev["damage_signature_axis_mask"], "none")
        self.assertIsNone(ev["coverage_adjusted_damage_signature_distance"])

    def test_coverage_adjusted_never_below_raw(self):
        # Penalty >= 0, so adjusted >= raw for every class with a raw distance.
        raw = 0.5
        for r in (
            row(collapse=0.1, gate=0.2, norm=0.3),
            row(collapse=0.1, norm=0.3),
            row(collapse=0.1, gate=0.2),
            row(collapse=0.1),
            row(gate=0.2),
            row(norm=0.3),
        ):
            ev = damage_signature_evidence(r, raw)
            self.assertGreaterEqual(
                ev["coverage_adjusted_damage_signature_distance"], raw
            )

    def test_coverage_adjusted_value_collapse_only(self):
        ev = damage_signature_evidence(row(collapse=0.1), 0.5)
        self.assertAlmostEqual(
            ev["coverage_adjusted_damage_signature_distance"],
            math.sqrt(0.5 ** 2 + 0.40 ** 2),
        )

    def test_full_signature_adjusted_equals_raw(self):
        ev = damage_signature_evidence(row(collapse=0.1, gate=0.2, norm=0.3), 0.5)
        self.assertAlmostEqual(
            ev["coverage_adjusted_damage_signature_distance"], 0.5
        )

    def test_avalanche_clamp_caps_at_collapse_only(self):
        # Avalanche normalized_vds_delta is NULL by design, so the richest an
        # avalanche row can reach (without gate, which is also absent today) is
        # collapse_only_signature with coverage 0.45 -- never collapse_bias.
        ev = damage_signature_evidence(row(collapse=0.1, gate=None, norm=None))
        self.assertEqual(
            ev["damage_signature_evidence_class"], "collapse_only_signature"
        )
        self.assertLessEqual(ev["damage_signature_coverage_score"], 0.45)


class ProxyClaimTest(unittest.TestCase):
    def test_validation_candidate_requires_same_device_exact_measured_support(self):
        claim = proxy_claim(claim_row(
            candidate_status="measured_damage_candidate",
            damage_evidence_tier="measured_damage",
            measured_comparability_status="usable",
            measured_match_scope="exact_condition",
            damage_signature_axes_used=2,
        ))
        self.assertEqual(claim["proxy_claim_status"], "validation_candidate")
        self.assertEqual(claim["proxy_claim_basis"], "same_device_measured_post_iv")

    def test_cross_device_rows_are_screening_only(self):
        claim = proxy_claim(claim_row(
            match_scope="cross_device",
            candidate_status="measured_damage_candidate",
            damage_evidence_tier="measured_damage",
            measured_comparability_status="usable",
            measured_match_scope="exact_condition",
        ))
        self.assertEqual(claim["proxy_claim_status"], "screening_only")
        self.assertIn("cross_device_screening_only", claim["proxy_claim_blockers"])

    def test_sign_mismatch_demotes_measured_row_to_screening(self):
        claim = proxy_claim(claim_row(
            candidate_status="measured_damage_candidate",
            damage_evidence_tier="measured_damage",
            measured_comparability_status="usable",
            measured_match_scope="exact_condition",
            measured_sign_mismatch_axis_count=1,
        ))
        self.assertEqual(claim["proxy_claim_status"], "screening_only")
        self.assertIn("measured_damage_sign_mismatch", claim["proxy_claim_blockers"])

    def test_one_axis_same_device_measured_row_needs_curation(self):
        claim = proxy_claim(claim_row(
            candidate_status="weak_measured_candidate",
            damage_evidence_tier="measured_damage",
            measured_comparability_status="weak",
            measured_match_scope="device_run_best_damage",
            damage_signature_axes_used=1,
            damage_signature_evidence_class="collapse_only_signature",
            has_normalized_vds_overlap=False,
        ))
        self.assertEqual(claim["proxy_claim_status"], "curation_candidate")
        self.assertIn(
            "insufficient_signature_axes_for_validation",
            claim["proxy_claim_blockers"],
        )

    def test_hard_mismatch_is_blocked(self):
        claim = proxy_claim(claim_row(candidate_status="damage_signature_mismatch"))
        self.assertEqual(claim["proxy_claim_status"], "blocked")

    def test_avalanche_axis_exclusion_is_explicit(self):
        quality = signature_claim_quality(claim_row(
            candidate_source="avalanche",
            damage_signature_evidence_class="collapse_only_signature",
            has_normalized_vds_overlap=False,
        ))
        self.assertEqual(quality, "axis_excluded")


if __name__ == "__main__":
    unittest.main()
