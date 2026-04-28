#!/usr/bin/env python3
"""
Seed irradiation campaign tables and create SQL views for the dashboard.

Creates (all idempotent — safe to re-run):
  1. irradiation_campaigns table  (facility-level metadata)
  2. irradiation_runs table       (per-ion/energy within a campaign)
  3. experiment_campaign_map table
  4. irrad_campaign_id, irrad_run_id, irrad_role on baselines_metadata
  5. Known campaigns, runs, and experiment-to-campaign mappings
  6. Backfills baselines_metadata from experiment_campaign_map
  7. Three SQL views:
       irradiation_view
       irradiation_degradation_summary
       irradiation_campaign_overview

Usage:
    python3 seed_irradiation_campaigns.py
"""

import sys

try:
    import psycopg2
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


# ── Known Campaigns (facility-level) ─────────────────────────────────────────
# (campaign_name, folder_name, facility, beam_type, notes)

CAMPAIGNS = [
    ("GSI_March_2025",  "GSIMarch2025Au",
     "GSI Darmstadt",   "micro_beam",
     "Heavy-ion microbeam, March 2025 at GSI; local folder is Au 1162 MeV"),
    ("Padova_Proton",   None,
     "LNL Padova",      "broad_beam",
     "1 MeV and 3 MeV proton irradiation at INFN-LNL Legnaro"),
    ("GSI_Ca_2022",     "2022_01_06_GSI_Ca",
     "GSI Darmstadt",   "micro_beam",
     "Calcium UNILAC microbeam, January 2022 campaign at GSI"),
    ("D2019_Proton",    None,
     None,              None,
     "Proton irradiation test campaign, 2019"),
    ("PSI_Proton_2022", "2022_30_08_PSI",
     "PSI Villigen",    "broad_beam",
     "Proton irradiation, August 2022 at PSI"),
    ("UCL_Ions_2023",   "2023_03_08_UCL_ions",
     "UCL",             "broad_beam",
     "Heavy-ion broad beam, March 2023 at UCL (HIF)"),
    ("ANSTO_Microbeam_2024", "ANSTO_23_01_2024_06_02_2024",
     "ANSTO",           "micro_beam",
     "Heavy-ion microbeam, Jan-Feb 2024 at ANSTO (SIRIUS)"),
    ("RADEF_2023",      "21_RADEF Test Campaign 2023",
     "RADEF",           "broad_beam",
     "Heavy-ion broad beam, June 2023 at RADEF (Jyvaskyla)"),
]


# ── Known Runs (per-ion/energy within a campaign) ────────────────────────────
# Source: local PDFs under ~/APS_Database/relevant papers/0_Literature
# (campaign_name, ion_species, beam_energy_mev, let_surface, let_bragg_peak,
#  range_um, beam_type_override, notes)
# let values in MeV·cm²/mg, range in µm
# beam_type_override: if set, overrides campaign-level beam_type for this run

RUNS = [
    # ── UCL / RADEF broad-beam (Martinella et al. 2024, Table I) ────────
    # Paper reports MeV/amu; beam_energy_mev stores total kinetic energy.
    ("UCL_Ions_2023", "Fe", 940.5, 14.53, None, 138.7, None,
     "57Fe20+, 16.5 MeV/amu. Source: Martinella et al. 2024 Table I."),
    ("UCL_Ions_2023", "Ni", 581.74, 20.4, None, 66.9, None,
     "58Ni18+, 10.03 MeV/amu. UCL broad-beam frame ~2 x 2 cm2. Source: Martinella et al. 2024 Table I; Ba thesis."),
    ("UCL_Ions_2023", "Kr", 764.4, 32.4, None, 60.9, None,
     "84Kr25+, 9.1 MeV/amu. UCL broad-beam frame ~2 x 2 cm2. Source: Martinella et al. 2024 Table I; Ba thesis."),
    ("UCL_Ions_2023", "Xe", 992.0, 62.5, None, 65.6, None,
     "124Xe35+, 8.0 MeV/amu. UCL broad-beam frame ~2 x 2 cm2; thesis reports Xe fluxes of 1e3 and 1e4 cm-2s-1 at 350 V. Source: Martinella et al. 2024 Table I; Ba thesis."),
    ("RADEF_2023", "Fe", 940.5, 14.53, None, 138.7, None,
     "57Fe20+, 16.5 MeV/amu. Source: Martinella et al. 2024 Table I."),
    ("RADEF_2023", "Ni", 581.74, 20.4, None, 66.9, None,
     "58Ni18+, 10.03 MeV/amu. Source: Martinella et al. 2024 Table I."),
    ("RADEF_2023", "Kr", 764.4, 32.4, None, 60.9, None,
     "84Kr25+, 9.1 MeV/amu. Source: Martinella et al. 2024 Table I."),
    ("RADEF_2023", "Xe", 992.0, 62.5, None, 65.6, None,
     "124Xe35+, 8.0 MeV/amu. Source: Martinella et al. 2024 Table I."),

    # ── ANSTO / GSI microbeam (Martinella et al. 2025, Table I) ─────────
    ("ANSTO_Microbeam_2024", "C",   6.0,  5.56,  None,  3.79, None,
     "Short-range C. Source: Martinella et al. 2025 Table I."),
    ("ANSTO_Microbeam_2024", "C",  12.0,  4.85,  None,  7.38, None,
     "Short-range C. Source: Martinella et al. 2025 Table I."),
    ("ANSTO_Microbeam_2024", "C",  36.0,  3.124, None, 26.9,  None,
     "Long-range C. Source: Martinella et al. 2025 Table I."),
    ("ANSTO_Microbeam_2024", "Cl", 36.0, 19.23, None,  7.29, None,
     "Short-range Cl. Source: Martinella et al. 2025 Table I."),
    ("ANSTO_Microbeam_2024", "Ni", 62.0, 32.34, None,  9.65, None,
     "Short-range Ni. Source: Martinella et al. 2025 Table I."),

    # ── GSI March 2025 / GSI microbeam ─────────────────────────────────
    ("GSI_March_2025", "Au", 1162.0, 97.1, None, 45.35, "micro_beam",
     "Au25+, 5.9 MeV/amu. GSI microbeam; thesis reports ~500 nm focus, ~55 x 50 um2 planar scan frame, and ~1600 ions/run. Sources: GSI March 2025 page; Martinella et al. 2025 Table I; Ba thesis."),
    ("GSI_March_2025", "Ar",  344.0, 11.07, None, 72.4, "micro_beam",
     "Source: Martinella et al. 2025 Table I; verify local folder before assigning."),

    # ── GSI Ca 2022 (Martinella et al. 2024, Table II) ─────────────────
    # Ca-40 at 8.6 MeV/amu = 344 MeV total.
    ("GSI_Ca_2022", "Ca", 344.0, 13.5, None, 60.4, "micro_beam",
     "UNILAC microbeam at GSI. 8.6 MeV/amu; thesis reports ~500 nm focus, ~15 x 10 um2 trench scan frame, and ~1600 ions/run. Source: Martinella et al. 2024 Table II; Ba thesis."),

    # ── Proton campaigns ────────────────────────────────────────────────
    # Padova: 1 and 3 MeV protons at CN accelerator, INFN-LNL Legnaro.
    ("Padova_Proton",   "proton", 1.0,  None, None, 7.0, None,
     "1 MeV protons at INFN-LNL CN accelerator. MOSFET range ~7 um after top metallization; wafer/DLTS pieces ~10.8 um. Flux 7.9e9 cm-2s-1; max MOSFET fluence 6e12 cm-2 (~20.9 Mrad); dose factor 3.49e-6 rad per cm-2. Source: NSREC2025_TNS_FINAL."),
    ("Padova_Proton",   "proton", 3.0,  None, None, 57.0, None,
     "3 MeV protons at INFN-LNL CN accelerator. MOSFET range ~57 um after top metallization; wafer/DLTS pieces ~62 um. Reported fluxes include 7.9e9, ~2e10, and 3.46e10 cm-2s-1 depending device/campaign; max fluence 6.72e13 cm-2 (planar/NSREC) or 2.7e13 cm-2 (trench); dose factor ~1.47e-6 to 1.49e-6 rad per cm-2."),
    ("D2019_Proton",    "proton", None, None, None, None, None, None),
    ("PSI_Proton_2022", "proton", 200.0, None, None, None, None,
     "200 MeV protons at PSI/PIF; 5 cm diameter flatness area, normal incidence, target fluence 1e11 cm-2. Source: Martinella et al. 2023, DOI 10.1109/TNS.2023.3267144."),
]


# Historical bad rows seeded before the paper tables were rechecked.
# Move referenced metadata onto the corrected canonical row before deleting
# stale rows, so existing assignments survive the cleanup.
LEGACY_RUN_ALIASES = [
    ("UCL_Ions_2023", "Fe", None, "Fe", 940.5),
    ("UCL_Ions_2023", "Ni", None, "Ni", 581.74),
    ("UCL_Ions_2023", "Kr", 9.53, "Kr", 764.4),
    ("UCL_Ions_2023", "Xe", 14.53, "Xe", 992.0),
    ("RADEF_2023", "Fe", None, "Fe", 940.5),
    ("RADEF_2023", "Kr", 9.53, "Kr", 764.4),
    ("RADEF_2023", "Xe", 14.53, "Xe", 992.0),
    ("PSI_Proton_2022", "proton", None, "proton", 200.0),
]

STALE_RUNS = [
    ("UCL_Ions_2023", "N", 16.5),
    ("UCL_Ions_2023", "Ne", 10.65),
    ("RADEF_2023", "N", 16.5),
    ("RADEF_2023", "Ne", 10.65),
]


# (experiment_name_in_baselines_metadata, campaign_name, role)
# role is 'pre_irrad' or 'post_irrad'
EXPERIMENT_MAPPINGS = [
    ("MOSFET_preCharact_GSI_March25",
     "GSI_March_2025", "pre_irrad"),
    ("MOSFET_Blocking_preCharact_GSI_March25",
     "GSI_March_2025", "pre_irrad"),
    ("MOSFET_2ndWolf_proton_irrad",
     "Padova_Proton", "post_irrad"),
    ("D2019_ProtonTestPreIV",
     "D2019_Proton", "pre_irrad"),
]


# ── Table DDL ────────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
-- Campaigns: one row per test campaign at a facility
CREATE TABLE IF NOT EXISTS irradiation_campaigns (
    id               SERIAL PRIMARY KEY,
    campaign_name    TEXT NOT NULL UNIQUE,
    folder_name      TEXT,
    facility         TEXT,
    beam_type        TEXT,
    date_start       DATE,
    date_end         DATE,
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Runs: one row per ion species + energy used within a campaign
CREATE TABLE IF NOT EXISTS irradiation_runs (
    id               SERIAL PRIMARY KEY,
    campaign_id      INTEGER NOT NULL REFERENCES irradiation_campaigns(id) ON DELETE CASCADE,
    ion_species      TEXT NOT NULL,
    beam_energy_mev  DOUBLE PRECISION,
    let_surface      DOUBLE PRECISION,
    let_bragg_peak   DOUBLE PRECISION,
    range_um         DOUBLE PRECISION,
    beam_type        TEXT,
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, ion_species, beam_energy_mev)
);

CREATE TABLE IF NOT EXISTS experiment_campaign_map (
    id           SERIAL PRIMARY KEY,
    experiment   TEXT NOT NULL UNIQUE,
    campaign_id  INTEGER REFERENCES irradiation_campaigns(id) ON DELETE CASCADE,
    role         TEXT NOT NULL DEFAULT 'post_irrad'
);

-- Add columns to irradiation_campaigns (idempotent, for existing DBs)
DO $$ BEGIN
    ALTER TABLE irradiation_campaigns ADD COLUMN folder_name TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- Drop legacy columns from irradiation_campaigns if they exist
-- (ion_species, beam_energy_mev, fluence_range, let_mev_cm2_mg moved to runs)
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN ion_species;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN beam_energy_mev;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN fluence_range;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN let_mev_cm2_mg;
EXCEPTION WHEN undefined_column THEN NULL; END $$;

-- Add irradiation columns to baselines_metadata (idempotent)
DO $$ BEGIN
    ALTER TABLE baselines_metadata
        ADD COLUMN irrad_campaign_id INTEGER REFERENCES irradiation_campaigns(id);
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN irrad_run_id INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- Add FK constraint for irrad_run_id if not present
DO $$ BEGIN
    ALTER TABLE baselines_metadata
        ADD CONSTRAINT baselines_metadata_irrad_run_id_fkey
        FOREIGN KEY (irrad_run_id) REFERENCES irradiation_runs(id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN irrad_role TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN fluence_at_meas DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'baselines_metadata'
          AND column_name = 'fluence'
    ) THEN
        EXECUTE 'UPDATE baselines_metadata
                 SET fluence_at_meas = fluence
                 WHERE fluence_at_meas IS NULL
                   AND fluence IS NOT NULL';
    END IF;
END $$;

DO $$ BEGIN
    ALTER TABLE baselines_measurements ADD COLUMN fluence DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_campaign
    ON baselines_metadata(irrad_campaign_id);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_run
    ON baselines_metadata(irrad_run_id);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_role
    ON baselines_metadata(irrad_role);
"""


# ── SQL Views ────────────────────────────────────────────────────────────────

VIEWS_SQL = """
-- ── irradiation_view ────────────────────────────────────────────────────────
-- Main denormalized view joining measurements → metadata → campaigns → runs.
-- irradiation_runs is LEFT JOINed so measurements without a run assignment
-- still appear (with NULL ion/energy fields).
DROP VIEW IF EXISTS irradiation_view CASCADE;
CREATE VIEW irradiation_view AS
SELECT
    m.id               AS measurement_id,
    md.id              AS metadata_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.measurement_type,
    md.measurement_category,
    md.filename,
    md.irrad_role      AS test_condition,
    ic.campaign_name,
    ic.facility,
    COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
    ir.ion_species,
    ir.beam_energy_mev,
    ir.let_surface     AS let_mev_cm2_mg,
    ir.let_bragg_peak,
    ir.range_um,
    -- Human-readable series label
    COALESCE(ir.ion_species, '?') || ' ' ||
        COALESCE(ir.beam_energy_mev::text, '?') || ' MeV ' ||
        COALESCE(ir.beam_type, ic.beam_type, '') AS irrad_condition_label,
    md.sweep_start, md.sweep_stop, md.sweep_points,
    md.bias_value, md.drain_bias_value,
    md.fluence_at_meas,
    m.point_index,
    m.step_index,
    -- Keithley 9.9E37 overflow sentinel → NULL
    CASE WHEN m.v_gate  IS NOT NULL AND ABS(m.v_gate)  < 1e30
         THEN m.v_gate  ELSE NULL END AS v_gate,
    CASE WHEN m.i_gate  IS NOT NULL AND ABS(m.i_gate)  < 1e30
         THEN m.i_gate  ELSE NULL END AS i_gate,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN m.v_drain ELSE NULL END AS v_drain,
    CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
         THEN m.i_drain ELSE NULL END AS i_drain,
    -- 0.01 V bin resolution
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2)::double precision ELSE NULL END AS v_gate_bin,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 2)::double precision
        ELSE NULL
    END AS v_drain_bin,
    CASE
        WHEN m.v_gate IS NULL OR ABS(m.v_gate) >= 1e30 THEN NULL
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant', 'Bodydiode')
        THEN ROUND(m.v_gate::numeric, 0)::double precision
        ELSE ROUND(m.v_gate::numeric, 1)::double precision
    END AS v_gate_plot_bin,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 1)::double precision
        WHEN m.v_drain IS NULL OR ABS(m.v_drain) >= 1e30 THEN NULL
        WHEN md.measurement_category = 'Blocking'
        THEN ROUND(m.v_drain::numeric, 0)::double precision
        ELSE ROUND(m.v_drain::numeric, 1)::double precision
    END AS v_drain_plot_bin,
    m.rds, m.bv, m.time_val
FROM baselines_measurements m
JOIN baselines_metadata     md ON m.metadata_id         = md.id
JOIN irradiation_campaigns  ic ON md.irrad_campaign_id  = ic.id
LEFT JOIN irradiation_runs  ir ON md.irrad_run_id       = ir.id
WHERE md.irrad_campaign_id IS NOT NULL;


-- ── irradiation_degradation_summary ─────────────────────────────────────────
DROP VIEW IF EXISTS irradiation_degradation_summary CASCADE;
CREATE VIEW irradiation_degradation_summary AS
SELECT
    md.device_type,
    md.manufacturer,
    md.device_id,
    md.irrad_role      AS test_condition,
    ic.campaign_name,
    ic.facility,
    ir.ion_species,
    ir.beam_energy_mev,
    ir.let_surface     AS let_mev_cm2_mg,
    COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
    COALESCE(ir.ion_species, '?') || ' ' ||
        COALESCE(ir.beam_energy_mev::text, '?') || ' MeV' AS irrad_condition_label,
    md.measurement_category,
    ROUND(m.v_gate::numeric, 2)::double precision  AS v_gate_bin,
    ROUND(m.v_drain::numeric, 2)::double precision AS v_drain_bin,
    CASE
        WHEN m.v_gate IS NULL OR ABS(m.v_gate) >= 1e30 THEN NULL
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant', 'Bodydiode')
        THEN ROUND(m.v_gate::numeric, 0)::double precision
        ELSE ROUND(m.v_gate::numeric, 1)::double precision
    END AS v_gate_plot_bin,
    CASE
        WHEN m.v_drain IS NULL OR ABS(m.v_drain) >= 1e30 THEN NULL
        WHEN md.measurement_category = 'Blocking'
        THEN ROUND(m.v_drain::numeric, 0)::double precision
        ELSE ROUND(m.v_drain::numeric, 1)::double precision
    END AS v_drain_plot_bin,
    AVG(m.i_drain)              AS avg_i_drain,
    AVG(m.i_gate)               AS avg_i_gate,
    AVG(ABS(m.i_drain))         AS avg_abs_i_drain,
    COUNT(*)                    AS n_points
FROM baselines_measurements m
JOIN baselines_metadata     md ON m.metadata_id        = md.id
JOIN irradiation_campaigns  ic ON md.irrad_campaign_id = ic.id
LEFT JOIN irradiation_runs  ir ON md.irrad_run_id      = ir.id
WHERE md.irrad_campaign_id IS NOT NULL
  AND (m.v_gate  IS NULL OR ABS(m.v_gate)  < 1e30)
  AND (m.i_drain IS NULL OR ABS(m.i_drain) < 1e30)
  AND (m.v_drain IS NULL OR ABS(m.v_drain) < 1e30)
  -- gate-swept categories must have a non-NULL v_gate so v_gate_bin is valid
  AND NOT (md.measurement_category IN ('IdVg','Vth','Igss','Subthreshold')
           AND m.v_gate IS NULL)
  -- drain-swept categories must have a non-NULL v_drain so v_drain_bin is valid
  AND NOT (md.measurement_category IN ('IdVd','Blocking','3rd_Quadrant')
           AND m.v_drain IS NULL)
GROUP BY
    md.device_type, md.manufacturer, md.device_id,
    md.irrad_role, ic.campaign_name, ic.facility,
    ir.ion_species, ir.beam_energy_mev, ir.let_surface,
    COALESCE(ir.beam_type, ic.beam_type),
    md.measurement_category,
    ROUND(m.v_gate::numeric, 2)::double precision,
    ROUND(m.v_drain::numeric, 2)::double precision,
    CASE
        WHEN m.v_gate IS NULL OR ABS(m.v_gate) >= 1e30 THEN NULL
        WHEN md.measurement_category IN ('IdVd', '3rd_Quadrant', 'Bodydiode')
        THEN ROUND(m.v_gate::numeric, 0)::double precision
        ELSE ROUND(m.v_gate::numeric, 1)::double precision
    END,
    CASE
        WHEN m.v_drain IS NULL OR ABS(m.v_drain) >= 1e30 THEN NULL
        WHEN md.measurement_category = 'Blocking'
        THEN ROUND(m.v_drain::numeric, 0)::double precision
        ELSE ROUND(m.v_drain::numeric, 1)::double precision
    END;


-- ── irradiation_waveform_view ───────────────────────────────────────────────
-- Time-domain monitoring captures (measurement_category = 'Irradiation').
-- 3-col files: time / Vds / Id.  7-col files also have Vgs / Igs.
DROP VIEW IF EXISTS irradiation_waveform_view CASCADE;
CREATE VIEW irradiation_waveform_view AS
SELECT
    m.id               AS measurement_id,
    md.id              AS metadata_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.measurement_type,
    md.filename,
    md.irrad_role      AS test_condition,
    ic.campaign_name,
    ic.facility,
    COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
    ir.ion_species,
    ir.beam_energy_mev,
    ir.let_surface     AS let_mev_cm2_mg,
    COALESCE(ir.ion_species, '?') || ' ' ||
        COALESCE(ir.beam_energy_mev::text, '?') || ' MeV ' ||
        COALESCE(ir.beam_type, ic.beam_type, '') AS irrad_condition_label,
    -- Round to 1-second bins so Superset aggregates ~1600 time steps
    -- instead of the ~335 K near-unique raw timestamps.
    FLOOR(m.time_val)::double precision AS time_val,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN m.v_drain ELSE NULL END AS vds,
    CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
         THEN m.i_drain ELSE NULL END AS id_drain,
    CASE WHEN m.v_gate  IS NOT NULL AND ABS(m.v_gate)  < 1e30
         THEN m.v_gate  ELSE NULL END AS vgs,
    CASE WHEN m.i_gate  IS NOT NULL AND ABS(m.i_gate)  < 1e30
         THEN m.i_gate  ELSE NULL END AS igs,
    m.fluence,
    md.fluence_at_meas,
    m.point_index
FROM baselines_measurements m
JOIN baselines_metadata     md ON m.metadata_id         = md.id
JOIN irradiation_campaigns  ic ON md.irrad_campaign_id  = ic.id
LEFT JOIN irradiation_runs  ir ON md.irrad_run_id       = ir.id
WHERE md.irrad_campaign_id IS NOT NULL
  AND md.measurement_category = 'Irradiation'
  AND m.time_val IS NOT NULL;


-- ── irradiation_campaign_overview ───────────────────────────────────────────
DROP VIEW IF EXISTS irradiation_campaign_overview CASCADE;
CREATE VIEW irradiation_campaign_overview AS
SELECT
    ic.campaign_name,
    ic.facility,
    COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
    ir.ion_species,
    ir.beam_energy_mev,
    ir.let_surface     AS let_mev_cm2_mg,
    md.device_type,
    md.manufacturer,
    md.irrad_role      AS test_condition,
    md.measurement_category,
    COUNT(DISTINCT md.device_id) AS n_devices,
    COUNT(DISTINCT md.id)        AS n_files,
    SUM(md.num_points)           AS n_points
FROM baselines_metadata    md
JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
LEFT JOIN irradiation_runs ir ON md.irrad_run_id      = ir.id
WHERE md.irrad_campaign_id IS NOT NULL
GROUP BY
    ic.campaign_name, ic.facility,
    COALESCE(ir.beam_type, ic.beam_type),
    ir.ion_species, ir.beam_energy_mev, ir.let_surface,
    md.device_type, md.manufacturer,
    md.irrad_role, md.measurement_category;
"""


# ── Backfill ─────────────────────────────────────────────────────────────────

BACKFILL_SQL = """
UPDATE baselines_metadata md
SET irrad_campaign_id = ecm.campaign_id,
    irrad_role        = ecm.role
FROM experiment_campaign_map ecm
WHERE md.experiment = ecm.experiment
  AND (md.irrad_campaign_id IS DISTINCT FROM ecm.campaign_id
       OR md.irrad_role IS DISTINCT FROM ecm.role);
"""


DEDUP_RUNS_SQL = """
WITH ranked AS (
    SELECT
        id,
        MIN(id) OVER (
            PARTITION BY campaign_id, lower(btrim(ion_species)), beam_energy_mev
        ) AS keep_id
    FROM irradiation_runs
)
UPDATE baselines_metadata md
SET irrad_run_id = ranked.keep_id
FROM ranked
WHERE md.irrad_run_id = ranked.id
  AND ranked.id <> ranked.keep_id;

WITH ranked AS (
    SELECT
        id,
        MIN(id) OVER (
            PARTITION BY campaign_id, lower(btrim(ion_species)), beam_energy_mev
        ) AS keep_id
    FROM irradiation_runs
)
DELETE FROM irradiation_runs ir
USING ranked
WHERE ir.id = ranked.id
  AND ranked.id <> ranked.keep_id;

DO $$ BEGIN
    ALTER TABLE irradiation_runs
        ADD CONSTRAINT irradiation_run_ion_not_blank
        CHECK (btrim(ion_species) <> '');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_irradiation_runs_energy_norm
    ON irradiation_runs (campaign_id, lower(btrim(ion_species)), beam_energy_mev)
    WHERE beam_energy_mev IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_irradiation_runs_null_energy_norm
    ON irradiation_runs (campaign_id, lower(btrim(ion_species)))
    WHERE beam_energy_mev IS NULL;
"""


def _campaign_id(cur, campaign_name):
    cur.execute(
        "SELECT id FROM irradiation_campaigns WHERE campaign_name = %s",
        (campaign_name,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _find_run(cur, campaign_id, ion_species, beam_energy_mev):
    cur.execute("""
        SELECT id
        FROM irradiation_runs
        WHERE campaign_id = %s
          AND lower(btrim(ion_species)) = lower(btrim(%s))
          AND beam_energy_mev IS NOT DISTINCT FROM %s
        ORDER BY id
        LIMIT 1
    """, (campaign_id, ion_species, beam_energy_mev))
    row = cur.fetchone()
    return row[0] if row else None


def _update_run(cur, run_id, ion_species, beam_energy_mev, let_surface,
                let_bragg_peak, range_um, beam_type, notes):
    cur.execute("""
        UPDATE irradiation_runs
        SET ion_species     = %s,
            beam_energy_mev = %s,
            let_surface     = %s,
            let_bragg_peak  = %s,
            range_um        = %s,
            beam_type       = %s,
            notes           = %s
        WHERE id = %s
    """, (ion_species, beam_energy_mev, let_surface, let_bragg_peak,
          range_um, beam_type, notes, run_id))


def _upsert_run(cur, campaign_id, ion_species, beam_energy_mev, let_surface,
                let_bragg_peak, range_um, beam_type, notes):
    run_id = _find_run(cur, campaign_id, ion_species, beam_energy_mev)
    if run_id:
        _update_run(cur, run_id, ion_species, beam_energy_mev, let_surface,
                    let_bragg_peak, range_um, beam_type, notes)
        return False

    cur.execute("""
        INSERT INTO irradiation_runs
            (campaign_id, ion_species, beam_energy_mev,
             let_surface, let_bragg_peak, range_um,
             beam_type, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (campaign_id, ion_species, beam_energy_mev, let_surface,
          let_bragg_peak, range_um, beam_type, notes))
    return True


def _move_legacy_run(cur, campaign_id, old_ion, old_energy,
                     new_ion, new_energy):
    old_id = _find_run(cur, campaign_id, old_ion, old_energy)
    if not old_id:
        return False

    new_id = _find_run(cur, campaign_id, new_ion, new_energy)
    if new_id and new_id != old_id:
        cur.execute("""
            UPDATE baselines_metadata
            SET irrad_run_id = %s
            WHERE irrad_run_id = %s
        """, (new_id, old_id))
        cur.execute("DELETE FROM irradiation_runs WHERE id = %s", (old_id,))
        return True

    cur.execute("""
        UPDATE irradiation_runs
        SET ion_species = %s,
            beam_energy_mev = %s
        WHERE id = %s
    """, (new_ion, new_energy, old_id))
    return True


def _delete_stale_run_if_unreferenced(cur, campaign_id, ion_species,
                                      beam_energy_mev):
    run_id = _find_run(cur, campaign_id, ion_species, beam_energy_mev)
    if not run_id:
        return False
    cur.execute(
        "SELECT COUNT(*) FROM baselines_metadata WHERE irrad_run_id = %s",
        (run_id,),
    )
    n_refs = cur.fetchone()[0]
    if n_refs:
        print(
            f"   WARNING: keeping stale run {ion_species} "
            f"{beam_energy_mev} MeV because {n_refs} metadata rows use it"
        )
        return False
    cur.execute("DELETE FROM irradiation_runs WHERE id = %s", (run_id,))
    return True


def cleanup_legacy_runs(cur):
    moved = 0
    deleted = 0
    for campaign_name, old_ion, old_energy, new_ion, new_energy in LEGACY_RUN_ALIASES:
        campaign_id = _campaign_id(cur, campaign_name)
        if campaign_id and _move_legacy_run(
                cur, campaign_id, old_ion, old_energy, new_ion, new_energy):
            moved += 1

    for campaign_name, ion_species, beam_energy_mev in STALE_RUNS:
        campaign_id = _campaign_id(cur, campaign_name)
        if campaign_id and _delete_stale_run_if_unreferenced(
                cur, campaign_id, ion_species, beam_energy_mev):
            deleted += 1
    return moved, deleted


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Seeding irradiation campaign tables")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 70)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # 1. Create / migrate tables
    print("\n1. Creating tables and migrating schema...")
    cur.execute(CREATE_TABLES_SQL)
    cur.execute(DEDUP_RUNS_SQL)
    conn.commit()
    print("   OK")

    # 2. Seed campaigns
    print("\n2. Seeding irradiation campaigns...")
    inserted_c = 0
    skipped_c = 0
    for (campaign_name, folder_name, facility, beam_type, notes) in CAMPAIGNS:
        cur.execute("""
            INSERT INTO irradiation_campaigns
                (campaign_name, folder_name, facility, beam_type, notes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (campaign_name)
            DO UPDATE SET folder_name = COALESCE(
                              irradiation_campaigns.folder_name,
                              EXCLUDED.folder_name),
                          facility = COALESCE(
                              EXCLUDED.facility,
                              irradiation_campaigns.facility),
                          beam_type = COALESCE(
                              EXCLUDED.beam_type,
                              irradiation_campaigns.beam_type),
                          notes = COALESCE(
                              EXCLUDED.notes,
                              irradiation_campaigns.notes)
        """, (campaign_name, folder_name, facility, beam_type, notes))
        if cur.rowcount:
            inserted_c += 1
            print(f"   + {campaign_name}")
        else:
            skipped_c += 1
            print(f"   = {campaign_name} (already exists)")
    conn.commit()
    print(f"   Inserted/updated: {inserted_c}, Skipped: {skipped_c}")

    print("\n3. Cleaning legacy irradiation runs...")
    moved, deleted = cleanup_legacy_runs(cur)
    conn.commit()
    print(f"   Corrected aliases: {moved}, Deleted stale rows: {deleted}")

    # 3. Seed irradiation runs
    print("\n4. Seeding irradiation runs...")
    inserted_r = 0
    skipped_r = 0
    for (campaign_name, ion_species, beam_energy_mev, let_surface,
         let_bragg_peak, range_um, beam_type_override, notes) in RUNS:
        campaign_id = _campaign_id(cur, campaign_name)
        if not campaign_id:
            print(f"   WARNING: campaign '{campaign_name}' not found, skipping")
            continue
        inserted = _upsert_run(cur, campaign_id, ion_species, beam_energy_mev,
                               let_surface, let_bragg_peak, range_um,
                               beam_type_override, notes)
        label = f"{ion_species} {beam_energy_mev or '?'} MeV"
        if inserted:
            inserted_r += 1
            print(f"   + {campaign_name} / {label}")
        else:
            skipped_r += 1
            print(f"   = {campaign_name} / {label} (already exists)")
    conn.commit()
    print(f"   Inserted/updated: {inserted_r}, Skipped: {skipped_r}")

    # 4. Seed experiment mappings
    print("\n5. Seeding experiment -> campaign mappings...")
    inserted_m = 0
    skipped_m = 0
    for experiment, campaign_name, role in EXPERIMENT_MAPPINGS:
        cur.execute(
            "SELECT id FROM irradiation_campaigns WHERE campaign_name = %s",
            (campaign_name,)
        )
        row = cur.fetchone()
        if not row:
            print(f"   WARNING: campaign '{campaign_name}' not found, skipping")
            continue
        campaign_id = row[0]
        cur.execute("""
            INSERT INTO experiment_campaign_map (experiment, campaign_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (experiment) DO NOTHING
        """, (experiment, campaign_id, role))
        if cur.rowcount:
            inserted_m += 1
            print(f"   + {experiment} -> {campaign_name} ({role})")
        else:
            skipped_m += 1
            print(f"   = {experiment} (already mapped)")
    conn.commit()
    print(f"   Inserted: {inserted_m}, Skipped: {skipped_m}")

    # 5. Backfill baselines_metadata
    print("\n6. Backfilling baselines_metadata from experiment_campaign_map...")
    cur.execute(BACKFILL_SQL)
    n_linked = cur.rowcount
    conn.commit()
    if n_linked:
        print(f"   Linked {n_linked} metadata rows to irradiation campaigns")
    else:
        print("   No rows updated (all already up to date)")

    # 6. Show summary
    cur.execute("""
        SELECT ic.campaign_name, md.irrad_role, COUNT(DISTINCT md.id) AS n_files
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
        GROUP BY ic.campaign_name, md.irrad_role
        ORDER BY ic.campaign_name, md.irrad_role
    """)
    rows = cur.fetchall()
    if rows:
        print("\n   Linked metadata summary:")
        for campaign_name, role, n_files in rows:
            print(f"     {campaign_name} / {role}: {n_files} files")

    cur.execute("""
        SELECT ic.campaign_name, COUNT(*) AS n_runs,
               string_agg(ir.ion_species || ' ' ||
                          COALESCE(ir.beam_energy_mev::text, '?') || ' MeV',
                          ', ' ORDER BY ir.ion_species) AS ions
        FROM irradiation_runs ir
        JOIN irradiation_campaigns ic ON ir.campaign_id = ic.id
        GROUP BY ic.campaign_name
        ORDER BY ic.campaign_name
    """)
    rows = cur.fetchall()
    if rows:
        print("\n   Runs per campaign:")
        for campaign_name, n_runs, ions in rows:
            print(f"     {campaign_name}: {n_runs} runs ({ions})")

    # 7. Create views
    print("\n7. Creating SQL views...")
    cur.execute(VIEWS_SQL)
    conn.commit()
    print("   Created: irradiation_view")
    print("   Created: irradiation_degradation_summary")
    print("   Created: irradiation_waveform_view")
    print("   Created: irradiation_campaign_overview")

    cur.close()
    conn.close()

    print("\n" + "=" * 70)
    print("Done!")
    print()
    print("Next steps:")
    print("  1. Add experiment mappings via the web UI (/irradiation)")
    print("     and click 'Sync to Metadata' to propagate.")
    print("  2. Assign irradiation runs to individual measurements")
    print("     via the web UI to enable per-measurement ion linkage.")
    print("=" * 70)


if __name__ == "__main__":
    main()
