-- Deduplicate irradiation campaigns by normalized name and enforce
-- case/whitespace-insensitive uniqueness to prevent duplicate UI entries.
DO $$
BEGIN
    IF to_regclass('public.irradiation_campaigns') IS NULL THEN
        RETURN;
    END IF;

    UPDATE irradiation_campaigns
    SET campaign_name = regexp_replace(btrim(campaign_name), '\s+', ' ', 'g')
    WHERE campaign_name <> regexp_replace(btrim(campaign_name), '\s+', ' ', 'g');

    UPDATE irradiation_campaigns
    SET folder_name = NULL
    WHERE folder_name IS NOT NULL AND btrim(folder_name) = '';

    -- Move baselines_metadata.irrad_run_id away from run rows that would
    -- conflict after campaign merge (same ion + energy in kept campaign).
    IF to_regclass('public.irradiation_runs') IS NOT NULL
       AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'baselines_metadata'
              AND column_name = 'irrad_run_id'
       ) THEN
        WITH campaign_dedup AS (
            SELECT
                id,
                MIN(id) OVER (PARTITION BY lower(btrim(campaign_name))) AS keep_id
            FROM irradiation_campaigns
        ),
        run_conflicts AS (
            SELECT
                r_dup.id  AS dup_run_id,
                r_keep.id AS keep_run_id
            FROM irradiation_runs r_dup
            JOIN campaign_dedup cd
              ON r_dup.campaign_id = cd.id
             AND cd.id <> cd.keep_id
            JOIN irradiation_runs r_keep
              ON r_keep.campaign_id = cd.keep_id
             AND r_keep.ion_species = r_dup.ion_species
             AND r_keep.beam_energy_mev IS NOT DISTINCT FROM r_dup.beam_energy_mev
        )
        UPDATE baselines_metadata md
        SET irrad_run_id = rc.keep_run_id
        FROM run_conflicts rc
        WHERE md.irrad_run_id = rc.dup_run_id;
    END IF;

    IF to_regclass('public.irradiation_runs') IS NOT NULL THEN
        WITH campaign_dedup AS (
            SELECT
                id,
                MIN(id) OVER (PARTITION BY lower(btrim(campaign_name))) AS keep_id
            FROM irradiation_campaigns
        ),
        run_conflicts AS (
            SELECT
                r_dup.id AS dup_run_id
            FROM irradiation_runs r_dup
            JOIN campaign_dedup cd
              ON r_dup.campaign_id = cd.id
             AND cd.id <> cd.keep_id
            JOIN irradiation_runs r_keep
              ON r_keep.campaign_id = cd.keep_id
             AND r_keep.ion_species = r_dup.ion_species
             AND r_keep.beam_energy_mev IS NOT DISTINCT FROM r_dup.beam_energy_mev
        )
        DELETE FROM irradiation_runs ir
        USING run_conflicts rc
        WHERE ir.id = rc.dup_run_id;

        WITH dedup AS (
            SELECT
                id,
                MIN(id) OVER (PARTITION BY lower(btrim(campaign_name))) AS keep_id
            FROM irradiation_campaigns
        )
        UPDATE irradiation_runs ir
        SET campaign_id = d.keep_id
        FROM dedup d
        WHERE ir.campaign_id = d.id
          AND d.id <> d.keep_id;
    END IF;

    WITH dedup AS (
        SELECT
            id,
            MIN(id) OVER (PARTITION BY lower(btrim(campaign_name))) AS keep_id
        FROM irradiation_campaigns
    )
    UPDATE experiment_campaign_map ecm
    SET campaign_id = d.keep_id
    FROM dedup d
    WHERE ecm.campaign_id = d.id
      AND d.id <> d.keep_id;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'baselines_metadata'
          AND column_name = 'irrad_campaign_id'
    ) THEN
        WITH dedup AS (
            SELECT
                id,
                MIN(id) OVER (PARTITION BY lower(btrim(campaign_name))) AS keep_id
            FROM irradiation_campaigns
        )
        UPDATE baselines_metadata md
        SET irrad_campaign_id = d.keep_id
        FROM dedup d
        WHERE md.irrad_campaign_id = d.id
          AND d.id <> d.keep_id;
    END IF;

    DELETE FROM irradiation_campaigns ic
    USING (
        SELECT
            id,
            MIN(id) OVER (PARTITION BY lower(btrim(campaign_name))) AS keep_id
        FROM irradiation_campaigns
    ) d
    WHERE ic.id = d.id
      AND d.id <> d.keep_id;
END $$;

DO $$
BEGIN
    IF to_regclass('public.irradiation_campaigns') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE irradiation_campaigns
        ADD CONSTRAINT irradiation_campaign_name_not_blank
        CHECK (btrim(campaign_name) <> '');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    IF to_regclass('public.irradiation_campaigns') IS NULL THEN
        RETURN;
    END IF;
    CREATE UNIQUE INDEX IF NOT EXISTS uq_irradiation_campaign_name_norm
    ON irradiation_campaigns ((lower(btrim(campaign_name))));
END $$;
