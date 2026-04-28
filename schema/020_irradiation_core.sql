-- Core irradiation-admin tables used by Flask /irradiation and ingestion.
-- Kept idempotent so apply_schema() can be safely run at startup.

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

DO $$ BEGIN
    ALTER TABLE irradiation_campaigns ADD COLUMN folder_name TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE irradiation_campaigns ADD COLUMN beam_type TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE experiment_campaign_map ADD COLUMN role TEXT NOT NULL DEFAULT 'post_irrad';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- Drop legacy campaign-level irradiation columns if they exist.
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN ion_species;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN beam_energy_mev;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN fluence_range;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE irradiation_campaigns DROP COLUMN let_mev_cm2_mg;
EXCEPTION WHEN undefined_column THEN NULL; END $$;

-- Link imported measurement metadata to curated campaigns/runs.
DO $$ BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        ALTER TABLE baselines_metadata
            ADD COLUMN irrad_campaign_id INTEGER REFERENCES irradiation_campaigns(id);
    END IF;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        ALTER TABLE baselines_metadata ADD COLUMN irrad_run_id INTEGER;
    END IF;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        ALTER TABLE baselines_metadata
            ADD CONSTRAINT baselines_metadata_irrad_run_id_fkey
            FOREIGN KEY (irrad_run_id) REFERENCES irradiation_runs(id);
    END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        ALTER TABLE baselines_metadata ADD COLUMN irrad_role TEXT;
    END IF;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        ALTER TABLE baselines_metadata ADD COLUMN fluence_at_meas DOUBLE PRECISION;
    END IF;
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
    IF to_regclass('public.baselines_measurements') IS NOT NULL THEN
        ALTER TABLE baselines_measurements ADD COLUMN fluence DOUBLE PRECISION;
    END IF;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$
BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_campaign
            ON baselines_metadata(irrad_campaign_id);
        CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_run
            ON baselines_metadata(irrad_run_id);
        CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_role
            ON baselines_metadata(irrad_role);
    END IF;
END $$;
