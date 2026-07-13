-- Forward migration: Flask avalanche administration schema.
--
-- This DDL formerly executed inside every /avalanche request. Application
-- requests must verify prepared schema, never mutate it. Existing databases
-- adopt historical files through 026 and execute this migration normally.

CREATE TABLE IF NOT EXISTS avalanche_campaigns (
    id                 SERIAL PRIMARY KEY,
    folder_path        TEXT NOT NULL UNIQUE,
    campaign_name      TEXT NOT NULL,
    inductance_mh      DOUBLE PRECISION,
    temperature_c      DOUBLE PRECISION,
    device_part_number TEXT,
    outcome_default    TEXT DEFAULT 'unknown',
    notes              TEXT
);

DO $migration$
BEGIN
    IF to_regclass('public.baselines_metadata') IS NOT NULL THEN
        ALTER TABLE baselines_metadata
            ADD COLUMN IF NOT EXISTS avalanche_family TEXT;
        ALTER TABLE baselines_metadata
            ADD COLUMN IF NOT EXISTS avalanche_inductance_mh DOUBLE PRECISION;
        ALTER TABLE baselines_metadata
            ADD COLUMN IF NOT EXISTS avalanche_temperature_c DOUBLE PRECISION;
    END IF;
END
$migration$;
