"""Tests for the damage-signature evidence-coverage model.

These assert the pure-Python helper that mirrors the `distances`/`coverage`
CTEs in schema/025_proxy_readiness_waveforms.sql.  Keep both in sync.
"""

import math
import unittest

from data_processing_scripts.calibrate_proxy_distance import (
    damage_signature_evidence,
)


def row(collapse=None, gate=None, norm=None):
    return {
        "collapse_delta": collapse,
        "gate_delta": gate,
        "normalized_vds_delta": norm,
    }


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


if __name__ == "__main__":
    unittest.main()
