import math
import unittest

from aps.enrich.radiation_stress_dose import (
    FluenceContext,
    LayerSpec,
    StoppingPoint,
    StoppingTable,
    calculate_context_components,
    delete_existing,
    dose_gy_from_fluence_and_let,
    fetch_stopping_tables,
    integrate_layer_deposition,
    normalize_mass_stopping_power,
    particle_count_for_layer,
    parse_scopes,
    RadiationContext,
)


def layer(name, order, thickness_um, area_cm2=0.02):
    return LayerSpec(
        layer_id=order + 1,
        layer_order=order,
        layer_name=name,
        material_key="sic_derived",
        density_g_cm3=3.21,
        thickness_um=thickness_um,
        exposed_area_cm2=area_cm2,
        area_basis="synthetic_test_area",
        coverage_fraction=0.5,
        incidence_angle_deg=0.0,
        confidence=0.8,
        provenance="unit test",
    )


def stopping_table():
    return StoppingTable(
        table_id=10,
        particle="proton",
        material_key="sic_derived",
        source_name="unit_test",
        source_version="v1",
        source_unit="MeV cm2/mg",
        canonical_unit="MeV cm2/mg",
        points=(
            StoppingPoint(
                energy_mev=1.0,
                electronic_mev_cm2_mg=0.20,
                nuclear_mev_cm2_mg=0.01,
                total_mev_cm2_mg=0.21,
                csda_range_um=100.0,
            ),
            StoppingPoint(
                energy_mev=10.0,
                electronic_mev_cm2_mg=0.05,
                nuclear_mev_cm2_mg=0.001,
                total_mev_cm2_mg=0.051,
                csda_range_um=1000.0,
            ),
        ),
    )


class RadiationStressDoseTests(unittest.TestCase):
    def test_pstar_mev_cm2_per_g_normalizes_to_repo_let_unit(self):
        self.assertAlmostEqual(
            normalize_mass_stopping_power(50.0, "MeV cm2/g"),
            0.05,
        )
        self.assertAlmostEqual(
            normalize_mass_stopping_power(0.05, "MeV cm2/mg"),
            0.05,
        )

    def test_thin_layer_dose_constant_uses_mev_cm2_per_mg(self):
        # 1e10 cm-2 at LET=0.1 MeV cm2/mg -> 160.2176634 Gy.
        self.assertAlmostEqual(
            dose_gy_from_fluence_and_let(1e10, 0.1),
            160.2176634,
        )

    def test_layer_propagation_carries_residual_energy_forward(self):
        table = stopping_table()
        first = layer("passivation", 0, 10.0)
        second = layer("drift", 1, 5000.0)

        first_dep = integrate_layer_deposition(1.0, first, table, max_steps=20)
        second_dep = integrate_layer_deposition(
            first_dep.energy_out_mev, second, table, max_steps=200)

        self.assertGreater(first_dep.energy_out_mev, 0.0)
        self.assertLess(first_dep.energy_out_mev, first_dep.energy_in_mev)
        self.assertTrue(second_dep.stopped_in_layer)
        self.assertEqual(second_dep.energy_out_mev, 0.0)
        self.assertLess(second_dep.range_margin_um, 0.0)

    def test_particle_count_keeps_fluence_area_and_single_particle_distinct(self):
        geom = layer("drift", 0, 10.0, area_cm2=0.25)
        file_fluence = FluenceContext(
            dose_scope="file",
            fluence_basis="file_fluence_span",
            fluence_delta_cm2=1e6,
        )
        single_particle = FluenceContext(
            dose_scope="single_particle",
            fluence_basis="single_particle_see",
            particle_count_override=1.0,
        )

        self.assertEqual(particle_count_for_layer(file_fluence, geom), 125000.0)
        self.assertEqual(particle_count_for_layer(single_particle, geom), 1.0)


    def test_default_scopes_include_campaign_basis(self):
        self.assertEqual(
            parse_scopes(None),
            ["campaign", "event_window", "file", "single_particle"],
        )


    def test_fetch_stopping_tables_normalizes_source_units(self):
        class Cursor:
            def execute(self, sql):
                self.sql = sql

            def fetchall(self):
                return [{
                    "table_id": 9,
                    "particle": "proton",
                    "material_key": "sic_derived",
                    "source_name": "NIST PSTAR derived",
                    "source_version": "SRD 124",
                    "source_unit": "MeV cm2/g",
                    "canonical_unit": "MeV cm2/mg",
                    "energy_mev": 1.0,
                    "electronic_stopping_mev_cm2_mg": None,
                    "nuclear_stopping_mev_cm2_mg": None,
                    "total_stopping_mev_cm2_mg": None,
                    "electronic_stopping_source": 50.0,
                    "nuclear_stopping_source": 1.0,
                    "total_stopping_source": 51.0,
                    "csda_range_um": 12.0,
                }]

        tables = fetch_stopping_tables(Cursor())
        point = tables[("proton", "sic_derived")].points[0]
        self.assertAlmostEqual(point.electronic_mev_cm2_mg, 0.05)
        self.assertAlmostEqual(point.nuclear_mev_cm2_mg, 0.001)
        self.assertAlmostEqual(point.total_mev_cm2_mg, 0.051)

    def test_delete_existing_can_be_limited_to_campaign(self):
        class Cursor:
            def execute(self, sql, params):
                self.sql = sql
                self.params = params

        cur = Cursor()
        delete_existing(cur, ["file", "campaign"], campaign="Padova_Proton")

        self.assertIn("USING irradiation_campaigns", cur.sql)
        self.assertIn("campaign_name", cur.sql)
        self.assertEqual(cur.params[2:], ("Padova_Proton", "Padova_Proton"))

    def test_context_components_store_electronic_and_nuclear_separately(self):
        ctx = RadiationContext(
            dose_scope="event_window",
            metadata_id=1,
            event_id=2,
            irrad_campaign_id=3,
            irrad_run_id=4,
            device_type="TESTPART",
            device_id="D1",
            particle="proton",
            ion_species="proton",
            beam_energy_mev=1.0,
            fluence=FluenceContext(
                dose_scope="event_window",
                fluence_basis="event_window_fluence_delta",
                fluence_start_cm2=10.0,
                fluence_end_cm2=20.0,
                fluence_delta_cm2=10.0,
            ),
        )
        rows = calculate_context_components(
            ctx,
            [layer("drift", 0, 10.0, area_cm2=0.1)],
            {("proton", "sic_derived"): stopping_table()},
            settings={"max_layer_steps": 20},
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["calculation_status"], "calculated")
        self.assertIsNotNone(row["radiation_deposited_energy_electronic_j"])
        self.assertIsNotNone(row["radiation_deposited_energy_nuclear_j"])
        self.assertGreater(
            row["radiation_deposited_energy_electronic_j"],
            row["radiation_deposited_energy_nuclear_j"],
        )
        self.assertEqual(row["fluence_basis"], "event_window_fluence_delta")
        self.assertEqual(row["energy_in_mev"], 1.0)
        self.assertGreater(row["energy_out_mev"], 0.0)


if __name__ == "__main__":
    unittest.main()
