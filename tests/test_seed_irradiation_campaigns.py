import sys
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "data_processing_scripts"))
sys.modules.setdefault("psycopg2", types.SimpleNamespace(connect=None))

from data_processing_scripts import seed_irradiation_campaigns as seed


def existing_run(**overrides):
    row = {
        "id": 1,
        "ion_species": "proton",
        "beam_energy_mev": 200.0,
        "let_surface": None,
        "let_bragg_peak": None,
        "range_um": None,
        "beam_type": "broad_beam",
        "notes": "seed note",
    }
    row.update(overrides)
    return row


def seed_run(**overrides):
    row = seed._run_seed_record(
        "proton", 200.0, None, None, None, "broad_beam", "seed note"
    )
    row.update(overrides)
    return row


class SeedIrradiationCampaignRunActionTests(unittest.TestCase):
    def test_insert_when_run_is_missing(self):
        plan = seed._compute_run_actions(None, seed_run(let_surface=0.1))

        self.assertTrue(plan["insert"])
        self.assertEqual(plan["actions"], [{"action": "insert"}])

    def test_blank_protected_numeric_is_filled(self):
        plan = seed._compute_run_actions(
            existing_run(let_surface=None),
            seed_run(let_surface=0.00374262719938149),
        )

        self.assertEqual(
            plan["numeric_updates"],
            {"let_surface": 0.00374262719938149},
        )
        self.assertEqual(plan["conflicts"], [])
        self.assertEqual(plan["actions"][0]["action"], "fill_blank")

    def test_close_numeric_values_are_noops(self):
        plan = seed._compute_run_actions(
            existing_run(let_surface=1.0),
            seed_run(let_surface=1.0 + 5e-10),
        )

        self.assertEqual(plan["numeric_updates"], {})
        self.assertEqual(plan["conflicts"], [])
        self.assertEqual(plan["actions"], [{"action": "noop"}])

    def test_different_non_null_numeric_conflicts_and_keeps_db(self):
        plan = seed._compute_run_actions(
            existing_run(let_surface=0.2),
            seed_run(let_surface=0.1),
        )

        self.assertEqual(plan["numeric_updates"], {})
        self.assertEqual(len(plan["conflicts"]), 1)
        self.assertEqual(plan["conflicts"][0]["field"], "let_surface")
        self.assertFalse(plan["conflicts"][0]["accepted"])

    def test_accept_seed_conflicts_overwrites_protected_numeric(self):
        plan = seed._compute_run_actions(
            existing_run(let_surface=0.2),
            seed_run(let_surface=0.1),
            accept_seed_conflicts=True,
        )

        self.assertEqual(plan["numeric_updates"], {"let_surface": 0.1})
        self.assertEqual(len(plan["conflicts"]), 1)
        self.assertTrue(plan["conflicts"][0]["accepted"])

    def test_seed_null_does_not_clear_curated_numeric(self):
        plan = seed._compute_run_actions(
            existing_run(let_surface=0.2),
            seed_run(let_surface=None),
        )

        self.assertEqual(plan["numeric_updates"], {})
        self.assertEqual(plan["conflicts"], [])
        self.assertEqual(plan["actions"], [{"action": "noop"}])

    def test_code_owned_fields_update_from_seed(self):
        plan = seed._compute_run_actions(
            existing_run(beam_type="old", notes="old note"),
            seed_run(beam_type="micro_beam", notes="new note"),
        )

        self.assertEqual(
            plan["code_updates"],
            {"beam_type": "micro_beam", "notes": "new note"},
        )
        self.assertEqual(
            [action["action"] for action in plan["actions"]],
            ["code_owned_update", "code_owned_update"],
        )

    def test_identity_fields_are_not_update_actions(self):
        plan = seed._compute_run_actions(
            existing_run(ion_species="old", beam_energy_mev=1.0),
            seed_run(ion_species="new", beam_energy_mev=2.0),
        )

        self.assertEqual(plan["numeric_updates"], {})
        self.assertEqual(plan["code_updates"], {})
        self.assertEqual(plan["conflicts"], [])
        self.assertEqual(plan["actions"], [{"action": "noop"}])

    def test_legacy_cleanup_can_suppress_one_pass_conflict(self):
        plan = seed._compute_run_actions(
            existing_run(let_surface=0.2),
            seed_run(let_surface=0.1),
            suppress_conflicts=True,
        )

        self.assertEqual(plan["numeric_updates"], {})
        self.assertEqual(plan["conflicts"], [])
        self.assertEqual(plan["actions"][0]["action"], "noop")
        self.assertEqual(
            plan["actions"][0]["reason"],
            "legacy_cleanup_suppressed_conflict",
        )


if __name__ == "__main__":
    unittest.main()
