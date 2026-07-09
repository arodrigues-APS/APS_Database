#!/usr/bin/env python3
"""
Calculate radiation deposited energy and dose by device material layer.

This is intentionally separate from electrical terminal energy.  Existing
waveform features integrate Vds * Id at the terminals; this script estimates
radiation energy deposited in material layers from fluence, stopping power, and
geometry provenance.
"""

from __future__ import annotations

import argparse
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from time import perf_counter

try:
    from psycopg2.extras import Json, RealDictCursor, execute_values
except ImportError:  # pragma: no cover - production environments install it
    Json = None
    RealDictCursor = None
    execute_values = None

try:
    from aps.common import apply_schema
    from aps.db_config import get_connection
except ImportError:  # pragma: no cover - package import path for tests
    from aps.common import apply_schema
    from aps.db_config import get_connection


MEV_TO_J = 1.602176634e-13
GY_PER_FLUENCE_LET = 1.602176634e-7
CALCULATION_SOURCE = "radiation_stress_dose.py"
DEFAULT_SETTINGS = {
    "max_layer_steps": 200,
    "default_particle_for_proton_aliases": "proton",
}


@dataclass(frozen=True)
class StoppingPoint:
    energy_mev: float
    electronic_mev_cm2_mg: float | None = None
    nuclear_mev_cm2_mg: float | None = None
    total_mev_cm2_mg: float | None = None
    csda_range_um: float | None = None


@dataclass(frozen=True)
class StoppingTable:
    table_id: int | None
    particle: str
    material_key: str
    source_name: str | None
    source_version: str | None
    source_unit: str | None
    canonical_unit: str | None
    points: tuple[StoppingPoint, ...]


@dataclass(frozen=True)
class LayerSpec:
    layer_id: int | None
    layer_order: int
    layer_name: str
    material_key: str
    density_g_cm3: float
    thickness_um: float
    exposed_area_cm2: float | None = None
    area_basis: str | None = None
    coverage_fraction: float = 1.0
    incidence_angle_deg: float = 0.0
    confidence: float = 0.0
    provenance: str | None = None

    @property
    def effective_thickness_um(self) -> float:
        angle_rad = math.radians(self.incidence_angle_deg)
        cos_angle = math.cos(angle_rad)
        if cos_angle <= 0.0:
            raise ValueError("incidence_angle_deg must be less than 90")
        return self.thickness_um / cos_angle

    @property
    def areal_density_mg_cm2(self) -> float:
        # density [g/cm3] * thickness [um] * 1e-4 [cm/um] * 1000 [mg/g]
        return self.density_g_cm3 * self.effective_thickness_um * 0.1

    @property
    def layer_mass_kg(self) -> float | None:
        if self.exposed_area_cm2 is None:
            return None
        thickness_cm = self.thickness_um * 1e-4
        grams = (
            self.density_g_cm3
            * thickness_cm
            * self.exposed_area_cm2
            * self.coverage_fraction
        )
        return grams * 1e-3


@dataclass(frozen=True)
class FluenceContext:
    dose_scope: str
    fluence_basis: str
    fluence_start_cm2: float | None = None
    fluence_end_cm2: float | None = None
    fluence_delta_cm2: float | None = None
    fluence_at_meas_cm2: float | None = None
    particle_count_override: float | None = None


@dataclass(frozen=True)
class RadiationContext:
    dose_scope: str
    metadata_id: int | None
    event_id: int | None
    irrad_campaign_id: int | None
    irrad_run_id: int | None
    device_type: str | None
    device_id: str | None
    particle: str | None
    ion_species: str | None
    beam_energy_mev: float | None
    fluence: FluenceContext


@dataclass(frozen=True)
class LayerDeposition:
    energy_in_mev: float | None
    energy_out_mev: float | None
    stopped_in_layer: bool
    range_margin_um: float | None
    electronic_stopping_mev_cm2_mg: float | None
    nuclear_stopping_mev_cm2_mg: float | None
    total_stopping_mev_cm2_mg: float | None
    deposited_electronic_mev_per_particle: float | None
    deposited_nuclear_mev_per_particle: float | None
    deposited_total_mev_per_particle: float | None


def finite_float(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def canonical_particle(ion_species):
    ion = (ion_species or "").strip().lower()
    if ion in {"p", "proton", "protons", "h", "h+"}:
        return "proton"
    return ion or None


def normalize_mass_stopping_power(value, source_unit):
    """
    Convert mass stopping power to canonical MeV cm2/mg.

    NIST PSTAR reports MeV cm2/g.  The irradiation-run LET convention in this
    repo is MeV cm2/mg, so PSTAR values are divided by 1000.
    """
    value = finite_float(value)
    if value is None:
        return None
    unit = (source_unit or "").replace("^", "").replace(" ", "").lower()
    if unit in {"mevcm2/g", "mev*cm2/g", "mevcm^2/g"}:
        return value / 1000.0
    if unit in {"mevcm2/mg", "mev*cm2/mg", "mevcm^2/mg"}:
        return value
    raise ValueError(f"Unsupported stopping-power unit: {source_unit!r}")


def range_g_cm2_to_um(range_g_cm2, density_g_cm3):
    range_g_cm2 = finite_float(range_g_cm2)
    density_g_cm3 = finite_float(density_g_cm3)
    if range_g_cm2 is None or density_g_cm3 is None or density_g_cm3 <= 0.0:
        return None
    return range_g_cm2 / density_g_cm3 * 10000.0


def dose_gy_from_fluence_and_let(fluence_cm2, let_mev_cm2_mg):
    fluence_cm2 = finite_float(fluence_cm2)
    let_mev_cm2_mg = finite_float(let_mev_cm2_mg)
    if fluence_cm2 is None or let_mev_cm2_mg is None:
        return None
    return fluence_cm2 * let_mev_cm2_mg * GY_PER_FLUENCE_LET


def _interpolate_value(points, energy_mev, attr):
    values = [
        (p.energy_mev, getattr(p, attr))
        for p in points
        if p.energy_mev is not None and getattr(p, attr) is not None
    ]
    values = sorted(values)
    if not values:
        return None
    if energy_mev <= values[0][0]:
        return values[0][1]
    if energy_mev >= values[-1][0]:
        return values[-1][1]
    for (e0, v0), (e1, v1) in zip(values, values[1:]):
        if e0 <= energy_mev <= e1:
            if e0 > 0.0 and e1 > 0.0 and v0 > 0.0 and v1 > 0.0:
                frac = (
                    math.log(energy_mev) - math.log(e0)
                ) / (math.log(e1) - math.log(e0))
                return math.exp(math.log(v0) + frac * (math.log(v1) - math.log(v0)))
            frac = (energy_mev - e0) / (e1 - e0)
            return v0 + frac * (v1 - v0)
    return values[-1][1]


def interpolate_stopping(table, energy_mev):
    points = table.points if isinstance(table, StoppingTable) else tuple(table)
    electronic = _interpolate_value(points, energy_mev, "electronic_mev_cm2_mg")
    nuclear = _interpolate_value(points, energy_mev, "nuclear_mev_cm2_mg")
    total = _interpolate_value(points, energy_mev, "total_mev_cm2_mg")
    if total is None:
        parts = [v for v in (electronic, nuclear) if v is not None]
        total = sum(parts) if parts else None
    return electronic, nuclear, total


def interpolate_range_um(table, energy_mev):
    points = table.points if isinstance(table, StoppingTable) else tuple(table)
    return _interpolate_value(points, energy_mev, "csda_range_um")


def integrate_layer_deposition(energy_in_mev, layer, stopping_table,
                               max_steps=200):
    energy_in_mev = finite_float(energy_in_mev)
    if energy_in_mev is None or energy_in_mev <= 0.0:
        return LayerDeposition(
            energy_in_mev=energy_in_mev,
            energy_out_mev=energy_in_mev,
            stopped_in_layer=True,
            range_margin_um=None,
            electronic_stopping_mev_cm2_mg=None,
            nuclear_stopping_mev_cm2_mg=None,
            total_stopping_mev_cm2_mg=None,
            deposited_electronic_mev_per_particle=0.0,
            deposited_nuclear_mev_per_particle=0.0,
            deposited_total_mev_per_particle=0.0,
        )

    steps = max(1, int(max_steps))
    step_areal_density = layer.areal_density_mg_cm2 / steps
    energy = energy_in_mev
    deposited_e = 0.0
    deposited_n = 0.0
    deposited_total = 0.0
    stopped = False

    e_stop0, n_stop0, total_stop0 = interpolate_stopping(stopping_table, energy)
    for _ in range(steps):
        e_stop, n_stop, total_stop = interpolate_stopping(stopping_table, energy)
        if total_stop is None or total_stop <= 0.0:
            break
        loss_total = total_stop * step_areal_density
        loss_e = (e_stop or 0.0) * step_areal_density
        loss_n = (n_stop or 0.0) * step_areal_density
        if loss_total >= energy:
            scale = energy / loss_total if loss_total > 0.0 else 0.0
            deposited_e += loss_e * scale
            deposited_n += loss_n * scale
            deposited_total += energy
            energy = 0.0
            stopped = True
            break
        deposited_e += loss_e
        deposited_n += loss_n
        deposited_total += loss_total
        energy -= loss_total

    range_um = interpolate_range_um(stopping_table, energy_in_mev)
    range_margin_um = (
        range_um - layer.effective_thickness_um
        if range_um is not None else None
    )
    return LayerDeposition(
        energy_in_mev=energy_in_mev,
        energy_out_mev=energy,
        stopped_in_layer=stopped,
        range_margin_um=range_margin_um,
        electronic_stopping_mev_cm2_mg=e_stop0,
        nuclear_stopping_mev_cm2_mg=n_stop0,
        total_stopping_mev_cm2_mg=total_stop0,
        deposited_electronic_mev_per_particle=deposited_e,
        deposited_nuclear_mev_per_particle=deposited_n,
        deposited_total_mev_per_particle=deposited_total,
    )


def particle_count_for_layer(fluence, layer):
    if fluence.particle_count_override is not None:
        return finite_float(fluence.particle_count_override)
    fluence_delta = finite_float(fluence.fluence_delta_cm2)
    if fluence_delta is None or layer.exposed_area_cm2 is None:
        return None
    return fluence_delta * layer.exposed_area_cm2 * layer.coverage_fraction


def build_component_row(context, layer, stopping_table, deposition, settings,
                        status, quality_flags):
    particle_count = particle_count_for_layer(context.fluence, layer)
    layer_mass_kg = layer.layer_mass_kg

    e_j = n_j = total_j = None
    e_dose = n_dose = total_dose = None
    if particle_count is not None:
        e_j = (
            deposition.deposited_electronic_mev_per_particle
            * particle_count * MEV_TO_J
            if deposition.deposited_electronic_mev_per_particle is not None
            else None
        )
        n_j = (
            deposition.deposited_nuclear_mev_per_particle
            * particle_count * MEV_TO_J
            if deposition.deposited_nuclear_mev_per_particle is not None
            else None
        )
        total_j = (
            deposition.deposited_total_mev_per_particle
            * particle_count * MEV_TO_J
            if deposition.deposited_total_mev_per_particle is not None
            else None
        )
    if layer_mass_kg and layer_mass_kg > 0.0:
        e_dose = e_j / layer_mass_kg if e_j is not None else None
        n_dose = n_j / layer_mass_kg if n_j is not None else None
        total_dose = total_j / layer_mass_kg if total_j is not None else None

    if stopping_table is None:
        table_id = source_name = source_version = source_unit = canonical_unit = None
    else:
        table_id = stopping_table.table_id
        source_name = stopping_table.source_name
        source_version = stopping_table.source_version
        source_unit = stopping_table.source_unit
        canonical_unit = stopping_table.canonical_unit

    return {
        "dose_scope": context.dose_scope,
        "metadata_id": context.metadata_id,
        "event_id": context.event_id,
        "irrad_campaign_id": context.irrad_campaign_id,
        "irrad_run_id": context.irrad_run_id,
        "device_type": context.device_type,
        "device_id": context.device_id,
        "particle": context.particle,
        "ion_species": context.ion_species,
        "beam_energy_mev": context.beam_energy_mev,
        "layer_id": layer.layer_id,
        "layer_order": layer.layer_order,
        "layer_name": layer.layer_name,
        "material_key": layer.material_key,
        "material_density_g_cm3": layer.density_g_cm3,
        "thickness_um": layer.thickness_um,
        "effective_thickness_um": layer.effective_thickness_um,
        "exposed_area_cm2": layer.exposed_area_cm2,
        "area_basis": layer.area_basis,
        "coverage_fraction": layer.coverage_fraction,
        "incidence_angle_deg": layer.incidence_angle_deg,
        "geometry_confidence": layer.confidence,
        "geometry_provenance": layer.provenance,
        "layer_mass_kg": layer_mass_kg,
        "fluence_basis": context.fluence.fluence_basis,
        "fluence_start_cm2": context.fluence.fluence_start_cm2,
        "fluence_end_cm2": context.fluence.fluence_end_cm2,
        "fluence_delta_cm2": context.fluence.fluence_delta_cm2,
        "fluence_at_meas_cm2": context.fluence.fluence_at_meas_cm2,
        "particle_count_estimate": particle_count,
        "energy_in_mev": deposition.energy_in_mev,
        "energy_out_mev": deposition.energy_out_mev,
        "stopped_in_layer": deposition.stopped_in_layer,
        "range_margin_um": deposition.range_margin_um,
        "electronic_stopping_mev_cm2_mg": deposition.electronic_stopping_mev_cm2_mg,
        "nuclear_stopping_mev_cm2_mg": deposition.nuclear_stopping_mev_cm2_mg,
        "total_stopping_mev_cm2_mg": deposition.total_stopping_mev_cm2_mg,
        "stopping_power_table_id": table_id,
        "stopping_power_source_name": source_name,
        "stopping_power_source_version": source_version,
        "stopping_power_source_unit": source_unit,
        "stopping_power_canonical_unit": canonical_unit,
        "deposited_energy_electronic_mev_per_particle": (
            deposition.deposited_electronic_mev_per_particle
        ),
        "deposited_energy_nuclear_mev_per_particle": (
            deposition.deposited_nuclear_mev_per_particle
        ),
        "deposited_energy_total_mev_per_particle": (
            deposition.deposited_total_mev_per_particle
        ),
        "radiation_deposited_energy_electronic_j": e_j,
        "radiation_deposited_energy_nuclear_j": n_j,
        "radiation_deposited_energy_total_j": total_j,
        "radiation_dose_electronic_gy": e_dose,
        "radiation_dose_nuclear_gy": n_dose,
        "radiation_dose_total_gy": total_dose,
        "radiation_energy_basis": (
            "layer_residual_energy_integrated_stopping_power"
        ),
        "calculation_status": status,
        "quality_flags": quality_flags,
        "settings": settings,
        "calculation_source": CALCULATION_SOURCE,
    }


def calculate_context_components(context, layers, stopping_tables, settings=None):
    settings = {**DEFAULT_SETTINGS, **(settings or {})}
    quality = []
    if context.beam_energy_mev is None:
        quality.append("missing_beam_energy")
    if not context.particle:
        quality.append("missing_particle")
    if not layers:
        quality.append("missing_device_material_layers")
    if quality:
        return []

    energy = float(context.beam_energy_mev)
    rows = []
    for layer in sorted(layers, key=lambda item: item.layer_order):
        table = stopping_tables.get((context.particle, layer.material_key))
        layer_quality = list(quality)
        if table is None:
            layer_quality.append("missing_stopping_power_table")
            deposition = LayerDeposition(
                energy_in_mev=energy,
                energy_out_mev=energy,
                stopped_in_layer=False,
                range_margin_um=None,
                electronic_stopping_mev_cm2_mg=None,
                nuclear_stopping_mev_cm2_mg=None,
                total_stopping_mev_cm2_mg=None,
                deposited_electronic_mev_per_particle=None,
                deposited_nuclear_mev_per_particle=None,
                deposited_total_mev_per_particle=None,
            )
            rows.append(build_component_row(
                context, layer, None, deposition, settings, "blocked",
                layer_quality,
            ))
            continue

        deposition = integrate_layer_deposition(
            energy,
            layer,
            table,
            max_steps=settings["max_layer_steps"],
        )
        particle_count = particle_count_for_layer(context.fluence, layer)
        if particle_count is None:
            layer_quality.append("missing_fluence_area_particle_count")
            status = "calculated_per_particle_only"
        elif layer.layer_mass_kg is None:
            layer_quality.append("missing_layer_mass_for_dose")
            status = "calculated_energy_only"
        else:
            status = "calculated"
        if table.source_unit and table.source_unit != table.canonical_unit:
            layer_quality.append("stopping_power_unit_normalized")
        if deposition.stopped_in_layer:
            layer_quality.append("particle_stopped_in_layer")
        rows.append(build_component_row(
            context, layer, table, deposition, settings, status, layer_quality,
        ))
        energy = deposition.energy_out_mev or 0.0
        if deposition.stopped_in_layer:
            break
    return rows


def _row_get(row, key):
    return row[key] if isinstance(row, dict) else getattr(row, key)


def fetch_stopping_tables(cur):
    cur.execute("""
        SELECT
            t.id AS table_id,
            lower(t.particle) AS particle,
            t.material_key,
            t.source_name,
            t.source_version,
            t.source_unit,
            t.canonical_unit,
            p.energy_mev,
            p.electronic_stopping_mev_cm2_mg,
            p.nuclear_stopping_mev_cm2_mg,
            p.total_stopping_mev_cm2_mg,
            p.electronic_stopping_source,
            p.nuclear_stopping_source,
            p.total_stopping_source,
            COALESCE(
                p.csda_range_um,
                CASE
                    WHEN p.csda_range_g_cm2 IS NOT NULL
                     AND t.material_density_g_cm3 IS NOT NULL
                     AND t.material_density_g_cm3 > 0.0
                    THEN p.csda_range_g_cm2 / t.material_density_g_cm3 * 10000.0
                END
            ) AS csda_range_um
        FROM radiation_stopping_power_tables t
        JOIN radiation_stopping_power_points p ON p.table_id = t.id
        ORDER BY t.id, p.energy_mev
    """)
    grouped = {}
    meta = {}
    for row in cur.fetchall():
        key = (row["particle"], row["material_key"])
        source_unit = row["source_unit"]
        electronic = finite_float(row["electronic_stopping_mev_cm2_mg"])
        nuclear = finite_float(row["nuclear_stopping_mev_cm2_mg"])
        total = finite_float(row["total_stopping_mev_cm2_mg"])
        if electronic is None:
            electronic = normalize_mass_stopping_power(
                row["electronic_stopping_source"], source_unit)
        if nuclear is None:
            nuclear = normalize_mass_stopping_power(
                row["nuclear_stopping_source"], source_unit)
        if total is None:
            total = normalize_mass_stopping_power(
                row["total_stopping_source"], source_unit)
        grouped.setdefault(key, []).append(StoppingPoint(
            energy_mev=float(row["energy_mev"]),
            electronic_mev_cm2_mg=electronic,
            nuclear_mev_cm2_mg=nuclear,
            total_mev_cm2_mg=total,
            csda_range_um=finite_float(row["csda_range_um"]),
        ))
        meta[key] = row
    return {
        key: StoppingTable(
            table_id=meta[key]["table_id"],
            particle=key[0],
            material_key=key[1],
            source_name=meta[key]["source_name"],
            source_version=meta[key]["source_version"],
            source_unit=meta[key]["source_unit"],
            canonical_unit=meta[key]["canonical_unit"],
            points=tuple(points),
        )
        for key, points in grouped.items()
    }


def fetch_layers(cur):
    cur.execute("""
        SELECT *
        FROM device_material_layers
        ORDER BY device_type NULLS LAST, layer_order, layer_name
    """)
    by_device = defaultdict(list)
    for row in cur.fetchall():
        layer = LayerSpec(
            layer_id=row["id"],
            layer_order=row["layer_order"],
            layer_name=row["layer_name"],
            material_key=row["material_key"],
            density_g_cm3=float(row["density_g_cm3"]),
            thickness_um=float(row["thickness_um"]),
            exposed_area_cm2=finite_float(row["exposed_area_cm2"]),
            area_basis=row["area_basis"],
            coverage_fraction=float(row["coverage_fraction"]),
            incidence_angle_deg=float(row["incidence_angle_deg"]),
            confidence=float(row["confidence"]),
            provenance=row["provenance"],
        )
        by_device[row["device_type"]].append(layer)
    return by_device


def _fluence_delta(start, end):
    start = finite_float(start)
    end = finite_float(end)
    if start is None or end is None:
        return None
    return max(end - start, 0.0)


def fetch_campaign_contexts(cur, campaign=None, limit=None):
    params = []
    where = [
        "md.irrad_campaign_id IS NOT NULL",
        "md.measurement_category = 'Irradiation'",
    ]
    if campaign:
        where.append("(ic.campaign_name = %s OR ic.folder_name = %s)")
        params.extend([campaign, campaign])
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    cur.execute(f"""
        SELECT
            'campaign'::text AS dose_scope,
            NULL::integer AS metadata_id,
            NULL::integer AS event_id,
            md.irrad_campaign_id,
            md.irrad_run_id,
            md.device_type,
            md.device_id,
            ir.ion_species,
            ir.beam_energy_mev,
            NULL::double precision AS fluence_start_cm2,
            MAX(md.fluence_at_meas) AS fluence_end_cm2,
            MAX(md.fluence_at_meas) AS fluence_delta_cm2,
            MAX(md.fluence_at_meas) AS fluence_at_meas_cm2,
            CASE
                WHEN MAX(md.fluence_at_meas) IS NOT NULL
                    THEN 'campaign_max_cumulative_fluence_at_meas'
                ELSE 'missing_campaign_fluence'
            END AS fluence_basis
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
        LEFT JOIN irradiation_runs ir ON ir.id = md.irrad_run_id
        WHERE {' AND '.join(where)}
        GROUP BY
            md.irrad_campaign_id, md.irrad_run_id, md.device_type,
            md.device_id, ir.ion_species, ir.beam_energy_mev
        ORDER BY md.irrad_campaign_id, md.irrad_run_id, md.device_type, md.device_id
        {limit_sql}
    """, params)
    return [_context_from_row(row) for row in cur.fetchall()]


def fetch_file_contexts(cur, campaign=None, limit=None):
    params = []
    where = [
        "md.irrad_campaign_id IS NOT NULL",
        "md.measurement_category = 'Irradiation'",
    ]
    if campaign:
        where.append("(ic.campaign_name = %s OR ic.folder_name = %s)")
        params.extend([campaign, campaign])
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    cur.execute(f"""
        SELECT
            'file'::text AS dose_scope,
            md.id AS metadata_id,
            NULL::integer AS event_id,
            md.irrad_campaign_id,
            md.irrad_run_id,
            md.device_type,
            md.device_id,
            ir.ion_species,
            ir.beam_energy_mev,
            NULL::double precision AS fluence_start_cm2,
            COALESCE(s.fluence_stop, s.fluence_max, md.fluence_at_meas)
                AS fluence_end_cm2,
            COALESCE(s.fluence_span, md.fluence_at_meas)
                AS fluence_delta_cm2,
            md.fluence_at_meas AS fluence_at_meas_cm2,
            CASE
                WHEN s.fluence_span IS NOT NULL THEN 'file_fluence_span'
                WHEN md.fluence_at_meas IS NOT NULL THEN 'file_max_cumulative_fluence_at_meas'
                ELSE 'missing_file_fluence'
            END AS fluence_basis
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
        LEFT JOIN irradiation_runs ir ON ir.id = md.irrad_run_id
        LEFT JOIN irradiation_single_event_file_summary s ON s.metadata_id = md.id
        WHERE {' AND '.join(where)}
        ORDER BY md.id
        {limit_sql}
    """, params)
    return [_context_from_row(row) for row in cur.fetchall()]


def fetch_event_contexts(cur, scope, campaign=None, limit=None):
    params = []
    where = [
        "md.irrad_campaign_id IS NOT NULL",
        "md.measurement_category = 'Irradiation'",
    ]
    if campaign:
        where.append("(ic.campaign_name = %s OR ic.folder_name = %s)")
        params.extend([campaign, campaign])
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    if scope == "event_window":
        fluence_cols = """
            e.fluence_start AS fluence_start_cm2,
            e.fluence_end AS fluence_end_cm2,
            CASE
                WHEN e.fluence_start IS NOT NULL AND e.fluence_end IS NOT NULL
                    THEN GREATEST(e.fluence_end - e.fluence_start, 0.0)
            END AS fluence_delta_cm2,
            md.fluence_at_meas AS fluence_at_meas_cm2,
            CASE
                WHEN e.fluence_start IS NOT NULL AND e.fluence_end IS NOT NULL
                    THEN 'event_window_fluence_delta'
                ELSE 'missing_event_window_fluence'
            END AS fluence_basis,
            NULL::double precision AS particle_count_override
        """
    elif scope == "single_particle":
        fluence_cols = """
            e.fluence_start AS fluence_start_cm2,
            e.fluence_end AS fluence_end_cm2,
            NULL::double precision AS fluence_delta_cm2,
            md.fluence_at_meas AS fluence_at_meas_cm2,
            'single_particle_see'::text AS fluence_basis,
            1.0::double precision AS particle_count_override
        """
    else:
        raise ValueError(f"Unsupported event scope: {scope}")

    cur.execute(f"""
        SELECT
            %s::text AS dose_scope,
            md.id AS metadata_id,
            e.id AS event_id,
            md.irrad_campaign_id,
            md.irrad_run_id,
            md.device_type,
            md.device_id,
            ir.ion_species,
            ir.beam_energy_mev,
            {fluence_cols}
        FROM irradiation_single_events e
        JOIN baselines_metadata md ON md.id = e.metadata_id
        JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
        LEFT JOIN irradiation_runs ir ON ir.id = md.irrad_run_id
        WHERE {' AND '.join(where)}
        ORDER BY md.id, e.id
        {limit_sql}
    """, [scope, *params])
    return [_context_from_row(row) for row in cur.fetchall()]


def _context_from_row(row):
    particle = canonical_particle(row["ion_species"])
    fluence = FluenceContext(
        dose_scope=row["dose_scope"],
        fluence_basis=row["fluence_basis"],
        fluence_start_cm2=finite_float(row["fluence_start_cm2"]),
        fluence_end_cm2=finite_float(row["fluence_end_cm2"]),
        fluence_delta_cm2=finite_float(row["fluence_delta_cm2"]),
        fluence_at_meas_cm2=finite_float(row["fluence_at_meas_cm2"]),
        particle_count_override=finite_float(row.get("particle_count_override")),
    )
    return RadiationContext(
        dose_scope=row["dose_scope"],
        metadata_id=row["metadata_id"],
        event_id=row["event_id"],
        irrad_campaign_id=row["irrad_campaign_id"],
        irrad_run_id=row["irrad_run_id"],
        device_type=row["device_type"],
        device_id=row["device_id"],
        particle=particle,
        ion_species=row["ion_species"],
        beam_energy_mev=finite_float(row["beam_energy_mev"]),
        fluence=fluence,
    )


COMPONENT_COLUMNS = (
    "dose_scope", "metadata_id", "event_id", "irrad_campaign_id",
    "irrad_run_id", "device_type", "device_id", "particle", "ion_species",
    "beam_energy_mev", "layer_id", "layer_order", "layer_name",
    "material_key", "material_density_g_cm3", "thickness_um",
    "effective_thickness_um", "exposed_area_cm2", "area_basis",
    "coverage_fraction", "incidence_angle_deg", "geometry_confidence",
    "geometry_provenance", "layer_mass_kg", "fluence_basis",
    "fluence_start_cm2", "fluence_end_cm2", "fluence_delta_cm2",
    "fluence_at_meas_cm2", "particle_count_estimate", "energy_in_mev",
    "energy_out_mev", "stopped_in_layer", "range_margin_um",
    "electronic_stopping_mev_cm2_mg", "nuclear_stopping_mev_cm2_mg",
    "total_stopping_mev_cm2_mg", "stopping_power_table_id",
    "stopping_power_source_name", "stopping_power_source_version",
    "stopping_power_source_unit", "stopping_power_canonical_unit",
    "deposited_energy_electronic_mev_per_particle",
    "deposited_energy_nuclear_mev_per_particle",
    "deposited_energy_total_mev_per_particle",
    "radiation_deposited_energy_electronic_j",
    "radiation_deposited_energy_nuclear_j",
    "radiation_deposited_energy_total_j",
    "radiation_dose_electronic_gy", "radiation_dose_nuclear_gy",
    "radiation_dose_total_gy", "radiation_energy_basis",
    "calculation_status", "quality_flags", "settings", "calculation_source",
)


def insert_components(cur, rows):
    if not rows:
        return
    values = []
    for row in rows:
        values.append(tuple(
            Json(row[col]) if col == "settings" and Json is not None else row[col]
            for col in COMPONENT_COLUMNS
        ))
    execute_values(cur, f"""
        INSERT INTO radiation_stress_dose_components
            ({', '.join(COMPONENT_COLUMNS)})
        VALUES %s
    """, values, page_size=1000)


def delete_existing(cur, scopes, campaign=None):
    if campaign:
        cur.execute("""
            DELETE FROM radiation_stress_dose_components c
            USING irradiation_campaigns ic
            WHERE c.calculation_source = %s
              AND c.dose_scope = ANY(%s)
              AND c.irrad_campaign_id = ic.id
              AND (ic.campaign_name = %s OR ic.folder_name = %s)
        """, (CALCULATION_SOURCE, list(scopes), campaign, campaign))
    else:
        cur.execute("""
            DELETE FROM radiation_stress_dose_components
            WHERE calculation_source = %s
              AND dose_scope = ANY(%s)
        """, (CALCULATION_SOURCE, list(scopes)))


def load_contexts(cur, scopes, campaign=None, limit=None):
    contexts = []
    if "campaign" in scopes:
        contexts.extend(fetch_campaign_contexts(
            cur, campaign=campaign, limit=limit))
    if "file" in scopes:
        contexts.extend(fetch_file_contexts(cur, campaign=campaign, limit=limit))
    if "event_window" in scopes:
        contexts.extend(fetch_event_contexts(
            cur, "event_window", campaign=campaign, limit=limit))
    if "single_particle" in scopes:
        contexts.extend(fetch_event_contexts(
            cur, "single_particle", campaign=campaign, limit=limit))
    return contexts


def calculate_rows(contexts, layers_by_device, stopping_tables, settings):
    rows = []
    skipped = Counter()
    for context in contexts:
        layers = layers_by_device.get(context.device_type)
        if not layers:
            layers = layers_by_device.get(None, [])
        if not layers:
            skipped["missing_device_material_layers"] += 1
            continue
        built = calculate_context_components(
            context, layers, stopping_tables, settings=settings)
        rows.extend(built)
        if not built:
            skipped["no_component_rows"] += 1
    return rows, skipped


def parse_scopes(raw):
    allowed = {"campaign", "file", "event_window", "single_particle"}
    scopes = set(raw or [
        "campaign", "file", "event_window", "single_particle"
    ])
    bad = sorted(scopes - allowed)
    if bad:
        raise ValueError(f"Unsupported dose scope(s): {', '.join(bad)}")
    return sorted(scopes)


def main():
    ap = argparse.ArgumentParser(
        description="Calculate radiation deposited energy/dose by material layer."
    )
    ap.add_argument("--campaign", help="Filter by campaign_name or folder_name")
    ap.add_argument("--scope", action="append",
                    choices=["campaign", "file", "event_window", "single_particle"],
                    help="Dose scope to calculate; can be repeated")
    ap.add_argument("--limit", type=int, help="Limit contexts per scope")
    ap.add_argument("--dry-run", action="store_true",
                    help="Calculate and summarize without writing")
    args = ap.parse_args()

    scopes = parse_scopes(args.scope)
    t0 = perf_counter()
    conn = get_connection()
    conn.autocommit = False
    try:
        apply_schema(conn, include_pipeline={
            "022_irradiation_single_events.sql",
            "027_radiation_stress_dose.sql",
        })
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            stopping_tables = fetch_stopping_tables(cur)
            layers_by_device = fetch_layers(cur)
            contexts = load_contexts(
                cur, scopes, campaign=args.campaign, limit=args.limit)
            rows, skipped = calculate_rows(
                contexts, layers_by_device, stopping_tables, DEFAULT_SETTINGS)
            status_counts = Counter(row["calculation_status"] for row in rows)
            if args.dry_run:
                conn.rollback()
            else:
                delete_existing(cur, scopes, campaign=args.campaign)
                insert_components(cur, rows)
                conn.commit()

        elapsed = perf_counter() - t0
        print("\nRadiation stress-dose calculation")
        print(f"  mode:              {'dry-run' if args.dry_run else 'applied'}")
        print(f"  scopes:            {', '.join(scopes)}")
        print(f"  contexts:          {len(contexts)}")
        print(f"  component rows:    {len(rows)}")
        print(f"  stopping tables:   {len(stopping_tables)}")
        print(f"  layer device keys: {len(layers_by_device)}")
        print("  status:")
        for status, count in sorted(status_counts.items()):
            print(f"    {status:32s} {count}")
        if skipped:
            print("  skipped:")
            for reason, count in sorted(skipped.items()):
                print(f"    {reason:32s} {count}")
        print(f"  elapsed:           {elapsed:.1f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
