-- Deduplicate irradiation runs and prevent repeat NULL-energy rows.
-- PostgreSQL UNIQUE constraints treat NULL values as distinct, which allowed
-- repeated "same ion, energy unknown" rows every time the seed script ran.

DO $$
BEGIN
    IF to_regclass('public.irradiation_runs') IS NULL THEN
        RETURN;
    END IF;

    UPDATE irradiation_runs
    SET ion_species = regexp_replace(btrim(ion_species), '\s+', ' ', 'g')
    WHERE ion_species <> regexp_replace(btrim(ion_species), '\s+', ' ', 'g');

    IF to_regclass('public.baselines_metadata') IS NOT NULL
       AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'baselines_metadata'
              AND column_name = 'irrad_run_id'
       ) THEN
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY campaign_id,
                                 lower(btrim(ion_species)),
                                 beam_energy_mev
                ) AS keep_id
            FROM irradiation_runs
        )
        UPDATE baselines_metadata md
        SET irrad_run_id = ranked.keep_id
        FROM ranked
        WHERE md.irrad_run_id = ranked.id
          AND ranked.id <> ranked.keep_id;
    END IF;

    WITH ranked AS (
        SELECT
            id,
            MIN(id) OVER (
                PARTITION BY campaign_id,
                             lower(btrim(ion_species)),
                             beam_energy_mev
            ) AS keep_id
        FROM irradiation_runs
    )
    DELETE FROM irradiation_runs ir
    USING ranked
    WHERE ir.id = ranked.id
      AND ranked.id <> ranked.keep_id;
END $$;

DO $$
BEGIN
    IF to_regclass('public.irradiation_runs') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE irradiation_runs
        ADD CONSTRAINT irradiation_run_ion_not_blank
        CHECK (btrim(ion_species) <> '');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_irradiation_runs_energy_norm
    ON irradiation_runs (campaign_id, lower(btrim(ion_species)), beam_energy_mev)
    WHERE beam_energy_mev IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_irradiation_runs_null_energy_norm
    ON irradiation_runs (campaign_id, lower(btrim(ion_species)))
    WHERE beam_energy_mev IS NULL;
