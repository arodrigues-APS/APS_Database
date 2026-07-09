#!/usr/bin/env python3
"""
Seed first-pass radiation dose/deposition inputs.

The radiation dose calculator needs two source tables before it can populate
`radiation_stress_dose_components`:

  1. `device_material_layers` for material geometry and modeled mass.
  2. `radiation_stopping_power_tables` plus points for LET/stopping power.

This seed intentionally starts with a narrow, traceable model:

  - one SiC active drift/epitaxial layer per known device type;
  - heavy-ion SiC LET/range points copied from `irradiation_runs`;
  - proton SiC stopping points estimated from NIST PSTAR silicon and
    graphite tables by SiC mass-fraction Bragg additivity.

It does not claim a full package/passivation/metallization stack. Those
layers should be added later from device cross-sections or SRIM/PSTAR runs.
"""

from __future__ import annotations

import argparse
import math
import re
from collections import defaultdict
from dataclasses import dataclass

from psycopg2.extras import RealDictCursor

try:
    from aps.common import apply_schema
    from aps.db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - exercised by package imports
    from aps.common import apply_schema
    from aps.db_config import get_connection


SCRIPT_NAME = "seed_radiation_dose_foundation.py"
SEED_VERSION = "seed_foundation_v2"
SIC_DENSITY_G_CM3 = 3.21
SIC_MATERIAL_KEY = "sic"
SIC_MATERIAL_NAME = "Silicon carbide active-region estimate"
DEFAULT_CURRENT_DENSITY_A_CM2 = 400.0
DEFAULT_UNKNOWN_AREA_CM2 = 0.09
KOSIER_TABLE_I_DOPING_BASIS = (
    "kosier_2026_table_i_measured_epi_doping_by_voltage_class"
)


ACTIVE_SIC_CLASS_ESTIMATES = {
    650: {
        "thickness_um": 6.0,
        "thickness_basis": "650v_class_active_sic_estimate_out_of_kosier_table_i_scope",
        "net_doping_cm3": None,
        "net_doping_basis": None,
    },
    900: {
        "thickness_um": 8.0,
        "thickness_basis": "900v_class_active_sic_estimate_out_of_kosier_table_i_scope",
        "net_doping_cm3": None,
        "net_doping_basis": None,
    },
    1200: {
        "thickness_um": 10.0,
        "thickness_basis": "kosier_2026_table_i_device_1_1200v_w_epi",
        "net_doping_cm3": 8.0e15,
        "net_doping_basis": KOSIER_TABLE_I_DOPING_BASIS,
    },
    1700: {
        "thickness_um": 12.0,
        "thickness_basis": "kosier_2026_table_i_device_3_1700v_w_epi",
        "net_doping_cm3": 7.0e15,
        "net_doping_basis": KOSIER_TABLE_I_DOPING_BASIS,
    },
    3300: {
        "thickness_um": 30.0,
        "thickness_basis": "kosier_2026_table_i_device_4_3300v_w_epi",
        "net_doping_cm3": 3.0e15,
        "net_doping_basis": KOSIER_TABLE_I_DOPING_BASIS,
    },
    4500: {
        "thickness_um": 40.0,
        "thickness_basis": "kosier_2026_table_i_device_5_4500v_vbr_w_epi",
        "net_doping_cm3": 2.0e15,
        "net_doping_basis": KOSIER_TABLE_I_DOPING_BASIS,
    },
    6500: {
        "thickness_um": 70.0,
        "thickness_basis": "kosier_2026_table_i_device_6_6500v_w_epi",
        "net_doping_cm3": 1.3e15,
        "net_doping_basis": KOSIER_TABLE_I_DOPING_BASIS,
    },
    10000: {
        "thickness_um": 110.0,
        "thickness_basis": "kosier_2026_table_i_device_7_10000v_w_epi",
        "net_doping_cm3": 0.6e15,
        "net_doping_basis": KOSIER_TABLE_I_DOPING_BASIS,
    },
}


@dataclass(frozen=True)
class ProtonPstarPoint:
    energy_mev: float
    electronic_g: float
    nuclear_g: float
    total_g: float
    csda_range_g_cm2: float

    @property
    def electronic_mg(self) -> float:
        return self.electronic_g / 1000.0

    @property
    def nuclear_mg(self) -> float:
        return self.nuclear_g / 1000.0

    @property
    def total_mg(self) -> float:
        return self.total_g / 1000.0

    @property
    def csda_range_um(self) -> float:
        return self.csda_range_g_cm2 / SIC_DENSITY_G_CM3 * 10000.0


# NIST PSTAR text endpoint values requested on 2026-06-09 for proton energies
# 1, 3, and 200 MeV. PSTAR has silicon and graphite, not SiC. Values below are
# SiC mass-fraction Bragg-additive estimates using Si/(Si+C)=0.70044 and
# C/(Si+C)=0.29956. Source PSTAR units are MeV cm2/g and g/cm2.
PROTON_SIC_PSTAR_POINTS = (
    ProtonPstarPoint(
        energy_mev=1.0,
        electronic_g=191.53573628620956,
        nuclear_g=0.1447606436970808,
        total_g=191.66569151920993,
        csda_range_g_cm2=0.0035099857219458063,
    ),
    ProtonPstarPoint(
        energy_mev=3.0,
        electronic_g=90.30699287967778,
        nuclear_g=0.05484934084022297,
        total_g=90.37197049617798,
        csda_range_g_cm2=0.020194130909181603,
    ),
    ProtonPstarPoint(
        energy_mev=200.0,
        electronic_g=3.7426271993814924,
        nuclear_g=0.0011047838838801392,
        total_g=3.7436271993814927,
        csda_range_g_cm2=31.390508647886975,
    ),
)


def finite_float(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def first_number(value):
    if value is None:
        return None
    match = re.search(r"[0-9]+(?:[.][0-9]+)?", str(value))
    return float(match.group(0)) if match else None


def voltage_from_device(row):
    voltage = first_number(row.get("voltage_rating"))
    if voltage:
        return voltage
    part = (row.get("device_type") or "").upper()
    for candidate in (10000, 6500, 4500, 3300, 1700, 1200, 900, 650):
        if str(candidate) in part:
            return float(candidate)
    for label, value in (
        ("10KV", 10000.0),
        ("6.5KV", 6500.0),
        ("4.5KV", 4500.0),
        ("3.3KV", 3300.0),
        ("1.7KV", 1700.0),
    ):
        if label in part:
            return value
    return None


def voltage_class_from_rating(voltage_rating_v):
    """Return the nominal voltage class used for active-layer defaults."""
    voltage = finite_float(voltage_rating_v)
    if voltage is None:
        return None
    for upper_bound, voltage_class in (
        (775.0, 650),
        (1050.0, 900),
        (1450.0, 1200),
        (1900.0, 1700),
        (3900.0, 3300),
        (5500.0, 4500),
        (8000.0, 6500),
        (11000.0, 10000),
    ):
        if voltage <= upper_bound:
            return voltage_class
    return None


def sic_active_layer_defaults(voltage_rating_v):
    """Return active SiC thickness and optional measured epi doping defaults."""
    voltage = finite_float(voltage_rating_v)
    if voltage is None:
        return 10.0, "unknown_voltage_default_1200v_like", None, None

    voltage_class = voltage_class_from_rating(voltage)
    estimate = ACTIVE_SIC_CLASS_ESTIMATES.get(voltage_class)
    if estimate:
        return (
            estimate["thickness_um"],
            estimate["thickness_basis"],
            estimate["net_doping_cm3"],
            estimate["net_doping_basis"],
        )

    return (
        110.0,
        "above_10000v_active_sic_estimate_from_kosier_table_i_upper_class",
        None,
        None,
    )


def sic_thickness_um(voltage_rating_v):
    """Return an active drift/epi thickness estimate by blocking class."""
    thickness_um, thickness_basis, _, _ = sic_active_layer_defaults(
        voltage_rating_v)
    return thickness_um, thickness_basis


def exposed_area_estimate(row):
    """
    Estimate irradiated active area for energy scaling.

    Dose for a uniform fluence slab is independent of this area because the
    area appears in both particle count and layer mass. Deposited energy in J
    does scale with it, so every estimate is marked via area_basis/confidence.
    """
    part = (row.get("device_type") or "").upper()
    if "3X3" in part:
        return 0.09, "part_number_dimension_3x3mm", 0.55
    if "5X5" in part:
        return 0.25, "part_number_dimension_5x5mm", 0.55

    current = first_number(row.get("current_rating_a"))
    if current and current > 0.0:
        area = current / DEFAULT_CURRENT_DENSITY_A_CM2
        area = min(max(area, 0.01), 0.30)
        return (
            area,
            f"current_rating_estimate_{DEFAULT_CURRENT_DENSITY_A_CM2:g}_a_cm2",
            0.35,
        )

    if "1.7KV" in part or "1700" in part:
        return 0.10, "generic_1700v_sic_die_area_estimate", 0.20
    return DEFAULT_UNKNOWN_AREA_CM2, "generic_sic_die_area_estimate", 0.20


def load_device_rows(cur):
    cur.execute("""
        WITH device_keys AS (
            SELECT part_number AS device_type
            FROM device_library
            UNION
            SELECT DISTINCT device_type
            FROM baselines_metadata
            WHERE measurement_category = 'Irradiation'
              AND device_type IS NOT NULL
        )
        SELECT
            dk.device_type,
            dl.device_category,
            dl.manufacturer,
            dl.voltage_rating,
            dl.current_rating_a,
            dl.package_type
        FROM device_keys dk
        LEFT JOIN device_library dl ON dl.part_number = dk.device_type
        WHERE dk.device_type IS NOT NULL
        ORDER BY dk.device_type
    """)
    return list(cur.fetchall())


def seed_device_material_layers(cur):
    rows = load_device_rows(cur)
    upserted = skipped = 0
    for row in rows:
        voltage = voltage_from_device(row)
        (
            thickness_um,
            thickness_basis,
            net_doping_cm3,
            net_doping_basis,
        ) = sic_active_layer_defaults(voltage)
        area_cm2, area_basis, area_confidence = exposed_area_estimate(row)
        confidence = min(0.60, max(0.20, area_confidence))
        doping_note = (
            f"net_doping_basis={net_doping_basis}"
            if net_doping_basis else
            "net_doping_basis=unseeded_out_of_kosier_table_i_scope"
        )
        notes = (
            "First-pass single-layer SiC active-region model. "
            f"thickness_basis={thickness_basis}; {doping_note}; "
            f"area_basis={area_basis}. "
            "Replace or extend with measured die geometry and package stack "
            "when available."
        )
        provenance = (
            f"{SCRIPT_NAME}:{SEED_VERSION}; voltage/current/dimension "
            "heuristics from device_library and part-number labels; Kosier "
            "TDMR 2026 Table I W_EPI/N by voltage class where applicable"
        )
        cur.execute(
            """
            INSERT INTO device_material_layers
                (device_type, layer_order, layer_name, material_key,
                 density_g_cm3, thickness_um, net_doping_cm3,
                 net_doping_basis, exposed_area_cm2, area_basis,
                 coverage_fraction, incidence_angle_deg, confidence,
                 provenance, notes)
            VALUES
                (%s, 0, 'sic_active_drift_region', %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 1.0, 0.0, %s, %s, %s)
            ON CONFLICT (device_type, layer_order, layer_name)
            DO UPDATE SET
                material_key = EXCLUDED.material_key,
                density_g_cm3 = EXCLUDED.density_g_cm3,
                thickness_um = EXCLUDED.thickness_um,
                net_doping_cm3 = EXCLUDED.net_doping_cm3,
                net_doping_basis = EXCLUDED.net_doping_basis,
                exposed_area_cm2 = EXCLUDED.exposed_area_cm2,
                area_basis = EXCLUDED.area_basis,
                coverage_fraction = EXCLUDED.coverage_fraction,
                incidence_angle_deg = EXCLUDED.incidence_angle_deg,
                confidence = EXCLUDED.confidence,
                provenance = EXCLUDED.provenance,
                notes = EXCLUDED.notes
            WHERE device_material_layers.provenance IS NULL
               OR device_material_layers.provenance LIKE %s
            """,
            (
                row["device_type"],
                SIC_MATERIAL_KEY,
                SIC_DENSITY_G_CM3,
                thickness_um,
                net_doping_cm3,
                net_doping_basis,
                area_cm2,
                area_basis,
                confidence,
                provenance,
                notes,
                f"{SCRIPT_NAME}:%",
            ),
        )
        if cur.rowcount:
            upserted += 1
        else:
            skipped += 1
    return upserted, skipped, len(rows)


def canonical_particle(ion_species):
    ion = (ion_species or "").strip().lower()
    if ion in {"p", "proton", "protons", "h", "h+"}:
        return "proton"
    return ion or None


def get_or_create_stopping_table(cur, particle, source_name, source_url,
                                 source_version, source_unit,
                                 canonical_unit, derivation_method,
                                 provenance, notes):
    cur.execute(
        """
        SELECT id
        FROM radiation_stopping_power_tables
        WHERE particle = %s
          AND material_key = %s
          AND source_name = %s
          AND COALESCE(source_version, '') = %s
        """,
        (particle, SIC_MATERIAL_KEY, source_name, source_version),
    )
    row = cur.fetchone()
    if row:
        table_id = row["id"]
        cur.execute(
            """
            UPDATE radiation_stopping_power_tables
            SET material_name = %s,
                material_density_g_cm3 = %s,
                source_url = %s,
                source_material_name = %s,
                source_unit = %s,
                canonical_unit = %s,
                derivation_method = %s,
                provenance = %s,
                notes = %s
            WHERE id = %s
            """,
            (
                SIC_MATERIAL_NAME,
                SIC_DENSITY_G_CM3,
                source_url,
                "SiC",
                source_unit,
                canonical_unit,
                derivation_method,
                provenance,
                notes,
                table_id,
            ),
        )
        return table_id, False

    cur.execute(
        """
        INSERT INTO radiation_stopping_power_tables
            (particle, material_key, material_name, material_density_g_cm3,
             source_name, source_url, source_version, source_material_name,
             source_unit, canonical_unit, derivation_method, provenance, notes)
        VALUES
            (%s, %s, %s, %s,
             %s, %s, %s, %s,
             %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            particle,
            SIC_MATERIAL_KEY,
            SIC_MATERIAL_NAME,
            SIC_DENSITY_G_CM3,
            source_name,
            source_url,
            source_version,
            "SiC",
            source_unit,
            canonical_unit,
            derivation_method,
            provenance,
            notes,
        ),
    )
    return cur.fetchone()["id"], True


def replace_stopping_points(cur, table_id, points):
    cur.execute(
        "DELETE FROM radiation_stopping_power_points WHERE table_id = %s",
        (table_id,),
    )
    for point in points:
        cur.execute(
            """
            INSERT INTO radiation_stopping_power_points
                (table_id, energy_mev,
                 electronic_stopping_mev_cm2_mg,
                 nuclear_stopping_mev_cm2_mg,
                 total_stopping_mev_cm2_mg,
                 electronic_stopping_source,
                 nuclear_stopping_source,
                 total_stopping_source,
                 csda_range_g_cm2,
                 csda_range_um,
                 provenance)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                table_id,
                point["energy_mev"],
                point["electronic_mg"],
                point["nuclear_mg"],
                point["total_mg"],
                point.get("electronic_source"),
                point.get("nuclear_source"),
                point.get("total_source"),
                point.get("csda_range_g_cm2"),
                point.get("csda_range_um"),
                point["provenance"],
            ),
        )


def seed_proton_stopping_table(cur):
    provenance = (
        f"{SCRIPT_NAME}:{SEED_VERSION}; NIST PSTAR silicon and graphite "
        "values requested 2026-06-09; SiC mass-fraction Bragg additivity"
    )
    table_id, created = get_or_create_stopping_table(
        cur,
        particle="proton",
        source_name="nist_pstar_bragg_additive_sic",
        source_url="https://physics.nist.gov/PhysRefData/Star/Text/PSTAR-t.html",
        source_version=SEED_VERSION,
        source_unit="MeV cm2/g",
        canonical_unit="MeV cm2/mg",
        derivation_method=(
            "Mass-fraction Bragg additivity from NIST PSTAR silicon and "
            "graphite proton stopping powers; canonical values divided by "
            "1000 from MeV cm2/g to MeV cm2/mg."
        ),
        provenance=provenance,
        notes=(
            "PSTAR does not list SiC directly in this endpoint. Use as a "
            "traceable proton fallback until a dedicated SiC PSTAR/SRIM table "
            "is available."
        ),
    )
    points = []
    for point in PROTON_SIC_PSTAR_POINTS:
        points.append({
            "energy_mev": point.energy_mev,
            "electronic_mg": point.electronic_mg,
            "nuclear_mg": point.nuclear_mg,
            "total_mg": point.total_mg,
            "electronic_source": point.electronic_g,
            "nuclear_source": point.nuclear_g,
            "total_source": point.total_g,
            "csda_range_g_cm2": point.csda_range_g_cm2,
            "csda_range_um": point.csda_range_um,
            "provenance": provenance,
        })
    replace_stopping_points(cur, table_id, points)
    return 1, len(points), created


def load_heavy_ion_points(cur):
    cur.execute("""
        SELECT
            lower(btrim(ir.ion_species)) AS ion_species,
            ir.beam_energy_mev,
            AVG(ir.let_surface) AS let_surface,
            AVG(ir.range_um) AS range_um,
            STRING_AGG(DISTINCT ic.campaign_name, ', ' ORDER BY ic.campaign_name)
                AS campaigns,
            STRING_AGG(DISTINCT NULLIF(ir.notes, ''), ' | ' ORDER BY NULLIF(ir.notes, ''))
                AS notes
        FROM irradiation_runs ir
        JOIN irradiation_campaigns ic ON ic.id = ir.campaign_id
        WHERE ir.beam_energy_mev IS NOT NULL
          AND ir.let_surface IS NOT NULL
          AND lower(btrim(ir.ion_species)) NOT IN ('p', 'h', 'h+', 'proton', 'protons')
        GROUP BY lower(btrim(ir.ion_species)), ir.beam_energy_mev
        ORDER BY lower(btrim(ir.ion_species)), ir.beam_energy_mev
    """)
    by_particle = defaultdict(list)
    for row in cur.fetchall():
        particle = canonical_particle(row["ion_species"])
        if not particle:
            continue
        let_surface = finite_float(row["let_surface"])
        energy_mev = finite_float(row["beam_energy_mev"])
        if let_surface is None or energy_mev is None:
            continue
        provenance = (
            f"{SCRIPT_NAME}:{SEED_VERSION}; irradiation_runs.let_surface "
            f"for campaigns {row['campaigns']}"
        )
        by_particle[particle].append({
            "energy_mev": energy_mev,
            "electronic_mg": let_surface,
            "nuclear_mg": 0.0,
            "total_mg": let_surface,
            "electronic_source": let_surface,
            "nuclear_source": 0.0,
            "total_source": let_surface,
            "csda_range_g_cm2": None,
            "csda_range_um": finite_float(row["range_um"]),
            "provenance": provenance,
            "notes": row["notes"],
        })
    return by_particle


def seed_heavy_ion_stopping_tables(cur):
    by_particle = load_heavy_ion_points(cur)
    tables = point_count = created_count = 0
    for particle, points in sorted(by_particle.items()):
        campaigns = sorted({
            point["provenance"].split("campaigns ", 1)[1]
            for point in points
            if "campaigns " in point["provenance"]
        })
        notes = " | ".join(
            sorted({point.get("notes") or "" for point in points if point.get("notes")})
        ) or None
        provenance = (
            f"{SCRIPT_NAME}:{SEED_VERSION}; run-level LET/range from "
            f"irradiation_runs for particle={particle}"
        )
        table_id, created = get_or_create_stopping_table(
            cur,
            particle=particle,
            source_name="irradiation_runs_let_surface_sic",
            source_url=None,
            source_version=SEED_VERSION,
            source_unit="MeV cm2/mg",
            canonical_unit="MeV cm2/mg",
            derivation_method=(
                "Copies irradiation_runs.let_surface into electronic and "
                "total stopping fields for a first-pass active SiC LET model; "
                "nuclear stopping is set to 0 until SRIM/material-specific "
                "heavy-ion tables are seeded."
            ),
            provenance=provenance,
            notes=(
                "Campaigns: " + "; ".join(campaigns) + ". "
                "Original run notes: " + notes
                if notes else
                "Campaigns: " + "; ".join(campaigns)
            ),
        )
        replace_stopping_points(cur, table_id, points)
        tables += 1
        point_count += len(points)
        created_count += 1 if created else 0
    return tables, point_count, created_count


def print_table_counts(cur):
    cur.execute("""
        SELECT 'device_material_layers' AS table_name, COUNT(*) FROM device_material_layers
        UNION ALL
        SELECT 'radiation_stopping_power_tables', COUNT(*) FROM radiation_stopping_power_tables
        UNION ALL
        SELECT 'radiation_stopping_power_points', COUNT(*) FROM radiation_stopping_power_points
        UNION ALL
        SELECT 'radiation_stress_dose_components', COUNT(*) FROM radiation_stress_dose_components
        ORDER BY table_name
    """)
    print("\nCurrent radiation dose/deposition table state:")
    for row in cur.fetchall():
        print(f"  {row['table_name']:35s} {row['count']}")


def main():
    parser = argparse.ArgumentParser(
        description="Seed first-pass radiation dose/deposition foundation tables."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build seed rows and summarize, then roll back table changes.",
    )
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = False
    try:
        apply_schema(conn, include_pipeline={"027_radiation_stress_dose.sql"})
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            layer_upserted, layer_skipped, layer_candidates = (
                seed_device_material_layers(cur)
            )
            proton_tables, proton_points, proton_created = (
                seed_proton_stopping_table(cur)
            )
            ion_tables, ion_points, ion_created = seed_heavy_ion_stopping_tables(cur)

            if args.dry_run:
                conn.rollback()
            else:
                conn.commit()

            print("\nRadiation dose/deposition foundation seed")
            print(f"  mode:                  {'dry-run' if args.dry_run else 'applied'}")
            print(f"  device candidates:     {layer_candidates}")
            print(f"  material layers:       {layer_upserted} upserted")
            if layer_skipped:
                print(f"                         {layer_skipped} preserved user-managed rows")
            print(f"  proton tables:         {proton_tables} ({proton_points} points)")
            print(f"                         {1 if proton_created else 0} newly created")
            print(f"  heavy-ion tables:      {ion_tables} ({ion_points} points)")
            print(f"                         {ion_created} newly created")
            print_table_counts(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
