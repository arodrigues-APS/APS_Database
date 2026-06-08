-- Radiation stopping-power and deposited-dose model.
-- Owned by data_processing_scripts/radiation_stress_dose.py.
-- apply_schema: pipeline-owned
--
-- This layer is separate from electrical terminal energy. It stores radiation
-- deposited energy and dose by material layer, with explicit fluence,
-- geometry, stopping-power, and provenance bases.

CREATE TABLE IF NOT EXISTS radiation_stopping_power_tables (
    id                         SERIAL PRIMARY KEY,
    particle                   TEXT NOT NULL,
    material_key               TEXT NOT NULL,
    material_name              TEXT NOT NULL,
    material_density_g_cm3     DOUBLE PRECISION,
    source_name                TEXT NOT NULL,
    source_url                 TEXT,
    source_version             TEXT,
    source_material_name       TEXT,
    source_unit                TEXT NOT NULL,
    canonical_unit             TEXT NOT NULL DEFAULT 'MeV cm2/mg',
    derivation_method          TEXT,
    provenance                 TEXT,
    notes                      TEXT,
    created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE radiation_stopping_power_tables
    ADD COLUMN IF NOT EXISTS material_density_g_cm3 DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS source_url TEXT,
    ADD COLUMN IF NOT EXISTS source_version TEXT,
    ADD COLUMN IF NOT EXISTS source_material_name TEXT,
    ADD COLUMN IF NOT EXISTS source_unit TEXT NOT NULL DEFAULT 'MeV cm2/mg',
    ADD COLUMN IF NOT EXISTS canonical_unit TEXT NOT NULL DEFAULT 'MeV cm2/mg',
    ADD COLUMN IF NOT EXISTS derivation_method TEXT,
    ADD COLUMN IF NOT EXISTS provenance TEXT,
    ADD COLUMN IF NOT EXISTS notes TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_rad_stop_tables_particle_material
    ON radiation_stopping_power_tables(particle, material_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rad_stop_tables_source
    ON radiation_stopping_power_tables(
        particle,
        material_key,
        source_name,
        COALESCE(source_version, '')
    );


CREATE TABLE IF NOT EXISTS radiation_stopping_power_points (
    id                                     BIGSERIAL PRIMARY KEY,
    table_id                               INTEGER NOT NULL
                                           REFERENCES radiation_stopping_power_tables(id)
                                           ON DELETE CASCADE,
    energy_mev                             DOUBLE PRECISION NOT NULL,

    electronic_stopping_mev_cm2_mg         DOUBLE PRECISION,
    nuclear_stopping_mev_cm2_mg            DOUBLE PRECISION,
    total_stopping_mev_cm2_mg              DOUBLE PRECISION,

    electronic_stopping_source             DOUBLE PRECISION,
    nuclear_stopping_source                DOUBLE PRECISION,
    total_stopping_source                  DOUBLE PRECISION,

    csda_range_g_cm2                       DOUBLE PRECISION,
    projected_range_g_cm2                  DOUBLE PRECISION,
    csda_range_um                          DOUBLE PRECISION,
    projected_range_um                     DOUBLE PRECISION,

    provenance                             TEXT,
    created_at                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, energy_mev),
    CHECK (energy_mev > 0.0)
);

ALTER TABLE radiation_stopping_power_points
    ADD COLUMN IF NOT EXISTS electronic_stopping_mev_cm2_mg DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS nuclear_stopping_mev_cm2_mg DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS total_stopping_mev_cm2_mg DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS electronic_stopping_source DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS nuclear_stopping_source DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS total_stopping_source DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS csda_range_g_cm2 DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS projected_range_g_cm2 DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS csda_range_um DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS projected_range_um DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS provenance TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_rad_stop_points_table_energy
    ON radiation_stopping_power_points(table_id, energy_mev);


CREATE TABLE IF NOT EXISTS device_material_layers (
    id                         SERIAL PRIMARY KEY,
    device_type                TEXT,
    layer_order                INTEGER NOT NULL,
    layer_name                 TEXT NOT NULL,
    material_key               TEXT NOT NULL,
    density_g_cm3              DOUBLE PRECISION NOT NULL,
    thickness_um               DOUBLE PRECISION NOT NULL,
    exposed_area_cm2           DOUBLE PRECISION,
    area_basis                 TEXT,
    coverage_fraction          DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    incidence_angle_deg        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    confidence                 DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    provenance                 TEXT,
    notes                      TEXT,
    created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (device_type, layer_order, layer_name),
    CHECK (layer_order >= 0),
    CHECK (density_g_cm3 > 0.0),
    CHECK (thickness_um > 0.0),
    CHECK (exposed_area_cm2 IS NULL OR exposed_area_cm2 > 0.0),
    CHECK (coverage_fraction >= 0.0 AND coverage_fraction <= 1.0),
    CHECK (incidence_angle_deg >= 0.0 AND incidence_angle_deg < 90.0),
    CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

ALTER TABLE device_material_layers
    ADD COLUMN IF NOT EXISTS device_type TEXT,
    ADD COLUMN IF NOT EXISTS layer_order INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS layer_name TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS material_key TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS density_g_cm3 DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS thickness_um DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS exposed_area_cm2 DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS area_basis TEXT,
    ADD COLUMN IF NOT EXISTS coverage_fraction DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS incidence_angle_deg DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS provenance TEXT,
    ADD COLUMN IF NOT EXISTS notes TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_device_material_layers_device
    ON device_material_layers(device_type);
CREATE INDEX IF NOT EXISTS idx_device_material_layers_material
    ON device_material_layers(material_key);


CREATE TABLE IF NOT EXISTS radiation_stress_dose_components (
    id                                      BIGSERIAL PRIMARY KEY,
    dose_scope                              TEXT NOT NULL CHECK (
                                                dose_scope IN (
                                                    'file',
                                                    'event_window',
                                                    'campaign',
                                                    'single_particle'
                                                )
                                            ),
    metadata_id                             INTEGER REFERENCES baselines_metadata(id)
                                            ON DELETE CASCADE,
    event_id                                INTEGER REFERENCES irradiation_single_events(id)
                                            ON DELETE CASCADE,
    irrad_campaign_id                       INTEGER REFERENCES irradiation_campaigns(id)
                                            ON DELETE SET NULL,
    irrad_run_id                            INTEGER REFERENCES irradiation_runs(id)
                                            ON DELETE SET NULL,

    device_type                             TEXT,
    device_id                               TEXT,
    particle                                TEXT,
    ion_species                             TEXT,
    beam_energy_mev                         DOUBLE PRECISION,

    layer_id                                INTEGER REFERENCES device_material_layers(id)
                                            ON DELETE SET NULL,
    layer_order                             INTEGER,
    layer_name                              TEXT,
    material_key                            TEXT,
    material_density_g_cm3                  DOUBLE PRECISION,
    thickness_um                            DOUBLE PRECISION,
    effective_thickness_um                  DOUBLE PRECISION,
    exposed_area_cm2                        DOUBLE PRECISION,
    area_basis                              TEXT,
    coverage_fraction                       DOUBLE PRECISION,
    incidence_angle_deg                     DOUBLE PRECISION,
    geometry_confidence                     DOUBLE PRECISION,
    geometry_provenance                     TEXT,
    layer_mass_kg                           DOUBLE PRECISION,

    fluence_basis                           TEXT NOT NULL,
    fluence_start_cm2                       DOUBLE PRECISION,
    fluence_end_cm2                         DOUBLE PRECISION,
    fluence_delta_cm2                       DOUBLE PRECISION,
    fluence_at_meas_cm2                     DOUBLE PRECISION,
    particle_count_estimate                 DOUBLE PRECISION,

    energy_in_mev                           DOUBLE PRECISION,
    energy_out_mev                          DOUBLE PRECISION,
    stopped_in_layer                        BOOLEAN NOT NULL DEFAULT FALSE,
    range_margin_um                         DOUBLE PRECISION,

    electronic_stopping_mev_cm2_mg          DOUBLE PRECISION,
    nuclear_stopping_mev_cm2_mg             DOUBLE PRECISION,
    total_stopping_mev_cm2_mg               DOUBLE PRECISION,
    stopping_power_table_id                 INTEGER REFERENCES radiation_stopping_power_tables(id)
                                            ON DELETE SET NULL,
    stopping_power_source_name              TEXT,
    stopping_power_source_version           TEXT,
    stopping_power_source_unit              TEXT,
    stopping_power_canonical_unit           TEXT,

    deposited_energy_electronic_mev_per_particle DOUBLE PRECISION,
    deposited_energy_nuclear_mev_per_particle    DOUBLE PRECISION,
    deposited_energy_total_mev_per_particle      DOUBLE PRECISION,
    radiation_deposited_energy_electronic_j      DOUBLE PRECISION,
    radiation_deposited_energy_nuclear_j         DOUBLE PRECISION,
    radiation_deposited_energy_total_j           DOUBLE PRECISION,
    radiation_dose_electronic_gy                 DOUBLE PRECISION,
    radiation_dose_nuclear_gy                    DOUBLE PRECISION,
    radiation_dose_total_gy                      DOUBLE PRECISION,

    radiation_energy_basis                 TEXT NOT NULL,
    calculation_status                     TEXT NOT NULL,
    quality_flags                          TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    settings                               JSONB DEFAULT '{}'::jsonb,
    calculation_source                     TEXT NOT NULL DEFAULT 'radiation_stress_dose.py',
    updated_at                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rad_dose_components_scope
    ON radiation_stress_dose_components(dose_scope);
CREATE INDEX IF NOT EXISTS idx_rad_dose_components_metadata
    ON radiation_stress_dose_components(metadata_id);
CREATE INDEX IF NOT EXISTS idx_rad_dose_components_event
    ON radiation_stress_dose_components(event_id);
CREATE INDEX IF NOT EXISTS idx_rad_dose_components_run
    ON radiation_stress_dose_components(irrad_run_id);
CREATE INDEX IF NOT EXISTS idx_rad_dose_components_device
    ON radiation_stress_dose_components(device_type);
CREATE INDEX IF NOT EXISTS idx_rad_dose_components_status
    ON radiation_stress_dose_components(calculation_status);


CREATE OR REPLACE VIEW radiation_stress_dose_summary_view AS
SELECT
    dose_scope,
    metadata_id,
    event_id,
    irrad_campaign_id,
    irrad_run_id,
    device_type,
    device_id,
    particle,
    ion_species,
    beam_energy_mev,
    COUNT(*) AS layer_count,
    COUNT(*) FILTER (WHERE calculation_status = 'calculated') AS calculated_layer_count,
    SUM(layer_mass_kg) AS modeled_mass_kg,
    SUM(radiation_deposited_energy_electronic_j)
        AS radiation_deposited_energy_electronic_j,
    SUM(radiation_deposited_energy_nuclear_j)
        AS radiation_deposited_energy_nuclear_j,
    SUM(radiation_deposited_energy_total_j)
        AS radiation_deposited_energy_total_j,
    SUM(radiation_deposited_energy_electronic_j)
        AS radiation_deposited_energy_j,
    CASE
        WHEN SUM(layer_mass_kg) > 0.0
         AND SUM(radiation_deposited_energy_electronic_j) IS NOT NULL
        THEN SUM(radiation_deposited_energy_electronic_j) / SUM(layer_mass_kg)
    END AS radiation_dose_electronic_gy,
    CASE
        WHEN SUM(layer_mass_kg) > 0.0
         AND SUM(radiation_deposited_energy_nuclear_j) IS NOT NULL
        THEN SUM(radiation_deposited_energy_nuclear_j) / SUM(layer_mass_kg)
    END AS radiation_dose_nuclear_gy,
    CASE
        WHEN SUM(layer_mass_kg) > 0.0
         AND SUM(radiation_deposited_energy_total_j) IS NOT NULL
        THEN SUM(radiation_deposited_energy_total_j) / SUM(layer_mass_kg)
    END AS radiation_dose_total_gy,
    CASE
        WHEN SUM(layer_mass_kg) > 0.0
         AND SUM(radiation_deposited_energy_electronic_j) IS NOT NULL
        THEN SUM(radiation_deposited_energy_electronic_j) / SUM(layer_mass_kg)
    END AS radiation_dose_gy,
    CASE
        WHEN SUM(layer_mass_kg) > 0.0
         AND SUM(radiation_deposited_energy_total_j) IS NOT NULL
        THEN SUM(radiation_deposited_energy_total_j) / SUM(layer_mass_kg)
    END AS radiation_total_dose_gy,
    MIN(energy_in_mev) AS min_energy_in_mev,
    MIN(energy_out_mev) AS min_energy_out_mev,
    BOOL_OR(stopped_in_layer) AS stopped_in_any_layer,
    MIN(range_margin_um) AS min_range_margin_um,
    STRING_AGG(DISTINCT fluence_basis, ', ' ORDER BY fluence_basis)
        AS fluence_basis,
    STRING_AGG(DISTINCT radiation_energy_basis, ', ' ORDER BY radiation_energy_basis)
        AS radiation_energy_basis,
    ARRAY_AGG(DISTINCT calculation_status ORDER BY calculation_status)
        AS calculation_statuses,
    MAX(updated_at) AS updated_at
FROM radiation_stress_dose_components
GROUP BY
    dose_scope, metadata_id, event_id, irrad_campaign_id, irrad_run_id,
    device_type, device_id, particle, ion_species, beam_energy_mev;
