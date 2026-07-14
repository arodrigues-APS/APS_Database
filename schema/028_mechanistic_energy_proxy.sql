-- Mechanistic energy-proxy feature layer (Phase 1 — feature correctness).
-- apply_schema: pipeline-owned
--
-- Owned by data_processing_scripts/create_proxy_readiness_dashboard.py and
-- applied right after schema/025_proxy_readiness_waveforms.sql, which this
-- depends on (stress_test_context_view).
--
-- This layer is intentionally descriptive.  It builds a per-record energy
-- feature vector that fixes the irradiation energy-density localization
-- semantics and adds critical-energy severity with a strict target/candidate
-- name separation.  It does NOT rank candidates and does NOT change the v1
-- stress_proxy_candidate_view.  The Python source-of-truth and the unit tests
-- live in data_processing_scripts/mechanistic_energy_proxy.py.
--
-- See docs/mechanistic_energy_proxy_rollout_plan_2026-06-26.md.

-- Both v2 relations started as plain VIEWs and are now MATERIALIZED (see
-- perf notes below), so the live object can be either kind.  A hard-coded
-- DROP VIEW raises wrong_object_type when the object is materialized (IF
-- EXISTS only covers absence, not the wrong kind) and would abort every
-- reapply, so look the kind up in pg_class and drop what is actually there.
DO $$
DECLARE
    obj_name text;
    obj_kind "char";
BEGIN
    FOREACH obj_name IN ARRAY ARRAY[
        'stress_proxy_candidate_combined_v3',
        'stress_proxy_candidate_energy_v2',
        'stress_candidate_destruction_boundary_energy_view',
        'stress_energy_equivalence_features'
    ] LOOP
        SELECT c.relkind INTO obj_kind
        FROM pg_class c
        WHERE c.oid = to_regclass(obj_name);
        IF obj_kind = 'v' THEN
            EXECUTE format('DROP VIEW %I CASCADE', obj_name);
        ELSIF obj_kind = 'm' THEN
            EXECUTE format('DROP MATERIALIZED VIEW %I CASCADE', obj_name);
        ELSIF obj_kind IS NOT NULL THEN
            RAISE EXCEPTION
                '% is relkind %, not a view/materialized view; refusing to drop',
                obj_name, obj_kind;
        END IF;
    END LOOP;
END $$;

-- stress_energy_equivalence_settings and stress_regime_compatibility moved to
-- schema/025_proxy_readiness_waveforms.sql (2026-07-02): the regime layer is
-- now shared by BOTH rankers — stress_test_context_view (025) computes the
-- measured `mechanistic_regime` label, and v1 ranking now reads
-- stress_regime_compatibility.path_penalty.  This file consumes the
-- tables (base CTE below CROSS JOINs the settings; the v2 view LATERAL-joins
-- the regime priors) but no longer defines them.  Apply order is unchanged:
-- 025 always runs before 028.


-- Phase 4: curated truth labels for calibrating the v2 mechanistic ranking.
--
-- This is the human-labeled supplement to the sparse auto-truth source
-- (damage_equivalence_match_view strong/usable rows).  It exists because the
-- Phase-3 live run proved the binding constraint is DATA, not method: tuning a
-- high-dimensional score against the auto-derived rank-1 output would look
-- precise and not be falsifiable.  Real (target, candidate) pairs are curated
-- against the live DB from known-good cases (e.g. the C2M0080120D
-- avalanche/proton pilot) before any weight/threshold tuning.
--
-- Keys are text stress_record_key values (schema/025, built by string concat),
-- NOT numeric ids.  label_basis records how strong the evidence is so the
-- calibrator can weight measured post-IV labels above expert/pilot judgement.
--
-- 'measured_post_iv_auto' is the QUARANTINED basis for labels seeded by
-- script from damage_equivalence_match_view strong/usable rows (the planned
-- auto-seeder MUST use it, never 'measured_post_iv').  Rationale: both
-- rankers already sort measured-damage matches first, so scoring auto-seeded
-- labels in the headline truth-hit metrics would be self-confirming — the
-- calibrator excludes this basis from headline rates and reports it
-- separately, and the v2 claim layer's equality test on 'measured_post_iv'
-- means auto labels can never mark a pair 'validated'.  Validation stays a
-- human act.
CREATE TABLE IF NOT EXISTS proxy_truth_labels (
    target_stress_record_key     text NOT NULL,
    candidate_stress_record_key  text NOT NULL,
    label                        text NOT NULL,
    label_basis                  text NOT NULL,
    reviewer                     text,
    review_date                  date,
    notes                        text,
    PRIMARY KEY (target_stress_record_key, candidate_stress_record_key),
    CHECK (label IN ('equivalent', 'not_equivalent', 'uncertain')),
    CONSTRAINT proxy_truth_labels_label_basis_check CHECK (
        label_basis IN ('measured_post_iv', 'expert', 'pilot',
                        'measured_post_iv_auto'))
);

-- Live databases created before 2026-07-02 carry the 3-value label_basis
-- CHECK under the same auto-generated name; swap it for the 4-value one.
-- Idempotent: drop-if-exists then re-add.
DO $$ BEGIN
    ALTER TABLE proxy_truth_labels
        DROP CONSTRAINT IF EXISTS proxy_truth_labels_label_basis_check;
    ALTER TABLE proxy_truth_labels
        ADD CONSTRAINT proxy_truth_labels_label_basis_check CHECK (
            label_basis IN ('measured_post_iv', 'expert', 'pilot',
                            'measured_post_iv_auto'));
END $$;

-- Idempotent seed/upsert.  Intentionally a no-op until real live keys are added
-- as UNION ALL SELECT rows below: the keys must come from the target database,
-- so curation is a live step (the assistant verifies SQL offline only).  The
-- WHERE FALSE seed guarantees copying this block never inserts placeholder
-- labels.  Mirrors the stress_regime_compatibility / settings upsert pattern.
WITH seed (
    target_stress_record_key,
    candidate_stress_record_key,
    label,
    label_basis,
    reviewer,
    review_date,
    notes
) AS (
    SELECT
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::date,
        NULL::text
    WHERE FALSE
    -- UNION ALL SELECT
    --     'LIVE_TARGET_STRESS_RECORD_KEY',
    --     'LIVE_CANDIDATE_STRESS_RECORD_KEY',
    --     'equivalent',
    --     'pilot',
    --     'aps',
    --     DATE '2026-06-30',
    --     'C2M0080120D avalanche/proton pilot; curated from live DB keys'
)
INSERT INTO proxy_truth_labels
    (target_stress_record_key, candidate_stress_record_key,
     label, label_basis, reviewer, review_date, notes)
SELECT
    target_stress_record_key,
    candidate_stress_record_key,
    label,
    label_basis,
    reviewer,
    review_date,
    notes
FROM seed
ON CONFLICT (target_stress_record_key, candidate_stress_record_key) DO UPDATE SET
    label = EXCLUDED.label,
    label_basis = EXCLUDED.label_basis,
    reviewer = EXCLUDED.reviewer,
    review_date = EXCLUDED.review_date,
    notes = EXCLUDED.notes;


CREATE TABLE IF NOT EXISTS stress_proxy_combined_ranker_settings (
    setting_name text PRIMARY KEY,
    description text NOT NULL,
    signature_axis_weight double precision NOT NULL,
    duration_weight double precision NOT NULL,
    log_energy_weight double precision NOT NULL,
    failure_fraction_weight double precision NOT NULL,
    post_iv_damage_weight double precision NOT NULL,
    regime_path_weight double precision NOT NULL,
    coverage_gap_weight double precision NOT NULL,
    CHECK (signature_axis_weight >= 0.0),
    CHECK (duration_weight >= 0.0),
    CHECK (log_energy_weight >= 0.0),
    CHECK (failure_fraction_weight >= 0.0),
    CHECK (post_iv_damage_weight >= 0.0),
    CHECK (regime_path_weight >= 0.0),
    CHECK (coverage_gap_weight >= 0.0)
);

INSERT INTO stress_proxy_combined_ranker_settings (
    setting_name, description, signature_axis_weight, duration_weight,
    log_energy_weight, failure_fraction_weight, post_iv_damage_weight,
    regime_path_weight, coverage_gap_weight
) VALUES (
    'screening_default',
    '2026-07-06 screening-only combined vector weights; uncalibrated pending curated truth labels.',
    1.0, 0.10, 1.0, 1.0, 0.50, 0.25, 0.25
)
ON CONFLICT (setting_name) DO UPDATE SET
    description = EXCLUDED.description,
    signature_axis_weight = EXCLUDED.signature_axis_weight,
    duration_weight = EXCLUDED.duration_weight,
    log_energy_weight = EXCLUDED.log_energy_weight,
    failure_fraction_weight = EXCLUDED.failure_fraction_weight,
    post_iv_damage_weight = EXCLUDED.post_iv_damage_weight,
    regime_path_weight = EXCLUDED.regime_path_weight,
    coverage_gap_weight = EXCLUDED.coverage_gap_weight;


-- Materialized (not a plain view): stress_proxy_candidate_energy_v2 below
-- joins this view to itself twice (once for target, once for candidate)
-- across all 1M+ rows of stress_proxy_candidate_ranked_view.  As a plain
-- view that meant recomputing the CROSS JOIN + dose-summary aggregation
-- from stress_test_context_view on every dashboard query; Superset timed
-- out at 60s (EXPLAIN cost ~8.3M for the rank-1 filter alone).  Materializing
-- this 3k-row layer collapses each join to an indexed lookup.
CREATE MATERIALIZED VIEW stress_energy_equivalence_features AS
WITH base AS (
    SELECT
        s.*,
        cfg.setting_name,
        cfg.default_track_core_radius_um,
        cfg.track_core_radius_low_um,
        cfg.track_core_radius_high_um,
        cfg.collapse_hard_threshold,
        cfg.terminal_energy_log_sigma_integrated,
        cfg.terminal_energy_log_sigma_commanded,
        cfg.terminal_energy_log_sigma_censored,
        cfg.active_area_log_sigma_measured,
        cfg.active_area_log_sigma_estimated,
        cfg.doping_log_sigma_measured,
        cfg.doping_log_sigma_estimated,
        cfg.geometry_confidence_measured_min,
        -- Kosier critical areal energies (J/cm^2), authoritative.
        207e-6::double precision AS seb_critical_j_cm2,
        60e-6::double precision  AS selc_critical_j_cm2,
        1.602176634e-13::double precision AS mev_to_j,
        3210.0::double precision AS sic_density_mg_cm3,
        -- is_proton / collapse_high / cumulative and the mechanistic_regime
        -- label they fed are no longer computed here: stress_test_context_view
        -- (schema/025) now carries mechanistic_regime as the single shared
        -- source, and s.* passes it through this view unchanged.
        -- Single-particle deposited energy sourced DIRECTLY from the dose
        -- summary, not the rollup's coalesced scope.  stress_test_context_view
        -- prefers event_window when both scopes exist, which would otherwise
        -- hide the per-particle dose behind the file-window dose for ~80% of
        -- events and force track-core density onto the surface-LET fallback.
        sp.single_particle_total_j AS single_particle_deposited_energy_j_raw
    FROM stress_test_context_view s
    CROSS JOIN stress_energy_equivalence_settings cfg
    LEFT JOIN (
        SELECT metadata_id, event_id,
               SUM(radiation_deposited_energy_total_j) AS single_particle_total_j
        FROM radiation_stress_dose_summary_view
        WHERE dose_scope = 'single_particle'
        GROUP BY metadata_id, event_id
    ) sp
      ON sp.metadata_id IS NOT DISTINCT FROM s.metadata_id
     AND sp.event_id IS NOT DISTINCT FROM s.event_id
    WHERE cfg.setting_name = 'default'
),
derived AS (
    SELECT
        b.*,
        -- Electrically-active cross-section from active volume / thickness.
        CASE
            WHEN b.energy_density_active_volume_cm3 IS NOT NULL
             AND b.energy_density_active_volume_cm3 > 0.0
             AND b.se_depletion_active_thickness_um IS NOT NULL
             AND b.se_depletion_active_thickness_um > 0.0
            THEN b.energy_density_active_volume_cm3
                 / (b.se_depletion_active_thickness_um * 1e-4)
        END AS active_area_cm2,
        -- mechanistic_regime arrives via b.* from stress_test_context_view
        -- (schema/025) — the shared single source for both rankers.
        -- Doping log-sigma for target depletion-ratio bands.
        CASE
            WHEN LOWER(COALESCE(b.se_depletion_net_doping_basis, '')) LIKE '%estimate%'
              OR LOWER(COALESCE(b.se_depletion_net_doping_basis, '')) LIKE '%reachthrough%'
            THEN b.doping_log_sigma_estimated
            ELSE b.doping_log_sigma_measured
        END AS doping_log_sigma,
        -- Terminal-energy log-sigma for candidate ratio bands.
        CASE
            WHEN LOWER(COALESCE(b.electrical_terminal_energy_basis, '')) LIKE '%commanded_or_stored%'
                THEN b.terminal_energy_log_sigma_commanded
            WHEN LOWER(COALESCE(b.electrical_terminal_energy_basis, '')) LIKE '%proxy%'
                THEN b.terminal_energy_log_sigma_censored
            WHEN LOWER(COALESCE(b.electrical_terminal_energy_basis, '')) LIKE '%integrated%'
                THEN b.terminal_energy_log_sigma_integrated
            ELSE b.terminal_energy_log_sigma_censored
        END AS terminal_energy_log_sigma,
        CASE
            WHEN b.energy_density_geometry_confidence IS NOT NULL
             AND b.energy_density_geometry_confidence >= b.geometry_confidence_measured_min
                THEN b.active_area_log_sigma_measured
            ELSE b.active_area_log_sigma_estimated
        END AS active_area_log_sigma
    FROM base b
),
computed AS (
    SELECT
        d.*,
        -- Candidate (SC / avalanche) bulk terminal areal-energy loading.
        CASE
            WHEN d.source IN ('sc', 'avalanche')
             AND d.electrical_terminal_energy_j IS NOT NULL
             AND d.electrical_terminal_energy_j > 0.0
             AND d.active_area_cm2 IS NOT NULL
             AND d.active_area_cm2 > 0.0
            THEN d.electrical_terminal_energy_j / d.active_area_cm2
        END AS terminal_areal_energy_bulk_j_cm2,
        CASE
            WHEN d.source IN ('sc', 'avalanche')
             AND d.electrical_terminal_energy_j IS NOT NULL
             AND d.electrical_terminal_energy_j > 0.0
             AND d.energy_density_active_volume_cm3 IS NOT NULL
             AND d.energy_density_active_volume_cm3 > 0.0
            THEN d.electrical_terminal_energy_j / d.energy_density_active_volume_cm3
        END AS terminal_energy_density_bulk_j_cm3,
        CASE
            WHEN d.source IN ('sc', 'avalanche')
             AND d.peak_abs_power_w IS NOT NULL
             AND d.energy_density_active_volume_cm3 IS NOT NULL
             AND d.energy_density_active_volume_cm3 > 0.0
            THEN d.peak_abs_power_w / d.energy_density_active_volume_cm3
        END AS peak_power_density_bulk_w_cm3,
        -- Irradiation bulk-equivalent density: the existing full-volume number,
        -- explicitly renamed so it can never be misread as a track-core density.
        CASE
            WHEN d.source = 'irradiation'
             AND d.energy_density_basis = 'ion_track_deposited_energy_over_active_volume'
            THEN d.stress_energy_density_j_cm3
        END AS radiation_deposited_bulk_equivalent_density_j_cm3,
        -- Per-particle deposited energy, taken directly from the single_particle
        -- dose scope regardless of which scope the rollup coalesced into.
        CASE
            WHEN d.source = 'irradiation'
            THEN d.single_particle_deposited_energy_j_raw
        END AS single_particle_deposited_energy_j
    FROM derived d
),
localized AS (
    SELECT
        c.*,
        -- Track-core density via per-particle deposited energy over core volume.
        CASE
            WHEN c.single_particle_deposited_energy_j IS NOT NULL
             AND c.single_particle_deposited_energy_j > 0.0
             AND c.se_depletion_active_thickness_um IS NOT NULL
             AND c.se_depletion_active_thickness_um > 0.0
            THEN c.single_particle_deposited_energy_j
                 / (PI() * POWER(c.default_track_core_radius_um * 1e-4, 2)
                    * (c.se_depletion_active_thickness_um * 1e-4))
        END AS track_core_density_from_sp_j_cm3,
        -- Track-core density via surface LET * density over core area.
        CASE
            WHEN c.source = 'irradiation'
             AND c.let_surface IS NOT NULL
             AND c.let_surface > 0.0
            THEN c.let_surface * c.sic_density_mg_cm3 * c.mev_to_j
                 / (PI() * POWER(c.default_track_core_radius_um * 1e-4, 2))
        END AS track_core_density_from_let_j_cm3
    FROM computed c
)
SELECT
    l.stress_record_key,
    l.source,
    l.device_type,
    l.voltage_class,
    l.technology_class,
    l.event_type,
    l.path_type,
    l.ion_species,
    l.let_surface AS let_surface_mev_cm2_mg,
    l.is_catastrophic,
    l.vds_collapse_fraction,
    l.gate_delta_fraction,
    l.normalized_vds,
    l.mechanistic_regime,

    -- Electrical terminal energy (passthrough).
    l.electrical_terminal_energy_j,
    l.electrical_terminal_energy_basis AS terminal_energy_basis,

    -- Candidate-side severity (SC / avalanche).  These are bulk terminal
    -- areal-energy ratios, NOT stored depletion-field ratios.
    l.active_area_cm2,
    l.terminal_energy_density_bulk_j_cm3,
    l.terminal_areal_energy_bulk_j_cm2,
    l.terminal_areal_energy_bulk_j_cm2 / l.seb_critical_j_cm2
        AS terminal_ratio_to_seb_critical,
    l.terminal_areal_energy_bulk_j_cm2 / l.selc_critical_j_cm2
        AS terminal_ratio_to_selc_critical,
    CASE
        WHEN l.terminal_areal_energy_bulk_j_cm2 IS NOT NULL THEN
            (l.terminal_areal_energy_bulk_j_cm2 / l.seb_critical_j_cm2)
            / EXP(SQRT(POWER(l.terminal_energy_log_sigma, 2)
                      + POWER(l.active_area_log_sigma, 2)))
    END AS terminal_ratio_to_seb_lower,
    CASE
        WHEN l.terminal_areal_energy_bulk_j_cm2 IS NOT NULL THEN
            (l.terminal_areal_energy_bulk_j_cm2 / l.seb_critical_j_cm2)
            * EXP(SQRT(POWER(l.terminal_energy_log_sigma, 2)
                      + POWER(l.active_area_log_sigma, 2)))
    END AS terminal_ratio_to_seb_upper,
    CASE
        WHEN l.terminal_areal_energy_bulk_j_cm2 IS NOT NULL THEN
            (l.terminal_areal_energy_bulk_j_cm2 / l.selc_critical_j_cm2)
            / EXP(SQRT(POWER(l.terminal_energy_log_sigma, 2)
                      + POWER(l.active_area_log_sigma, 2)))
    END AS terminal_ratio_to_selc_lower,
    CASE
        WHEN l.terminal_areal_energy_bulk_j_cm2 IS NOT NULL THEN
            (l.terminal_areal_energy_bulk_j_cm2 / l.selc_critical_j_cm2)
            * EXP(SQRT(POWER(l.terminal_energy_log_sigma, 2)
                      + POWER(l.active_area_log_sigma, 2)))
    END AS terminal_ratio_to_selc_upper,
    CASE
        WHEN l.source IN ('sc', 'avalanche')
             AND l.terminal_areal_energy_bulk_j_cm2 IS NOT NULL
        THEN 'bulk_terminal_areal_energy_over_active_area'
    END AS terminal_ratio_basis,

    -- Radiation deposition (passthrough) + localization-aware densities.
    l.radiation_deposited_energy_total_j,
    l.radiation_deposited_energy_electronic_j,
    l.radiation_deposited_energy_nuclear_j,
    l.radiation_dose_total_gy,
    l.radiation_dose_scope,
    l.radiation_deposited_bulk_equivalent_density_j_cm3,
    CASE
        WHEN l.radiation_deposited_bulk_equivalent_density_j_cm3 IS NOT NULL
        THEN 'deposited_energy_over_full_active_volume_bulk_equivalent'
    END AS bulk_equivalent_density_basis,
    l.single_particle_deposited_energy_j,
    l.default_track_core_radius_um AS track_core_radius_um,
    l.track_core_radius_low_um,
    l.track_core_radius_high_um,
    COALESCE(l.track_core_density_from_sp_j_cm3, l.track_core_density_from_let_j_cm3)
        AS track_core_energy_density_j_cm3,
    -- Density ∝ 1/r^2 for both bases, so a wider core gives the lower bound.
    COALESCE(l.track_core_density_from_sp_j_cm3, l.track_core_density_from_let_j_cm3)
        * POWER(l.default_track_core_radius_um / l.track_core_radius_high_um, 2)
        AS track_core_energy_density_lower_j_cm3,
    COALESCE(l.track_core_density_from_sp_j_cm3, l.track_core_density_from_let_j_cm3)
        * POWER(l.default_track_core_radius_um / l.track_core_radius_low_um, 2)
        AS track_core_energy_density_upper_j_cm3,
    CASE
        WHEN l.track_core_density_from_sp_j_cm3 IS NOT NULL
            THEN 'single_particle_deposited_over_core_volume'
        WHEN l.track_core_density_from_let_j_cm3 IS NOT NULL
            THEN 'surface_let_over_core_area'
    END AS localization_basis,

    -- Target-side severity (irradiation): stored depletion field energy vs
    -- Kosier critical areal energies, with doping-driven bands.
    l.se_depletion_stored_energy_j_cm2,
    l.se_depletion_ratio_to_seb,
    l.se_depletion_ratio_to_selc,
    CASE WHEN l.se_depletion_ratio_to_seb IS NOT NULL
         THEN l.se_depletion_ratio_to_seb / EXP(0.5 * l.doping_log_sigma) END
        AS se_depletion_ratio_lower_to_seb,
    CASE WHEN l.se_depletion_ratio_to_seb IS NOT NULL
         THEN l.se_depletion_ratio_to_seb * EXP(0.5 * l.doping_log_sigma) END
        AS se_depletion_ratio_upper_to_seb,
    CASE WHEN l.se_depletion_ratio_to_selc IS NOT NULL
         THEN l.se_depletion_ratio_to_selc / EXP(0.5 * l.doping_log_sigma) END
        AS se_depletion_ratio_lower_to_selc,
    CASE WHEN l.se_depletion_ratio_to_selc IS NOT NULL
         THEN l.se_depletion_ratio_to_selc * EXP(0.5 * l.doping_log_sigma) END
        AS se_depletion_ratio_upper_to_selc,
    l.se_depletion_model_quality,
    l.se_depletion_net_doping_basis,
    CASE
        WHEN l.se_depletion_ratio_to_seb IS NOT NULL
        THEN 'stored_field_energy_band_from_doping_log_sigma'
    END AS se_depletion_ratio_interval_basis,

    -- Rate / cumulative descriptors (passthrough).
    l.effective_stress_time_s,
    l.peak_abs_power_w,
    l.average_terminal_power_w,
    l.peak_power_density_bulk_w_cm3,
    l.cumulative_pulse_energy_j,
    l.pulse_count_in_sequence,

    -- Visible blockers (fail-closed; describes what is missing or assumed).
    ARRAY_REMOVE(ARRAY[
        CASE WHEN l.source IN ('sc', 'avalanche') AND l.active_area_cm2 IS NULL
             THEN 'candidate_missing_active_area' END,
        CASE WHEN l.source IN ('sc', 'avalanche')
              AND (LOWER(COALESCE(l.electrical_terminal_energy_basis, '')) LIKE '%proxy%'
                OR LOWER(COALESCE(l.electrical_terminal_energy_basis, '')) LIKE '%commanded_or_stored%')
             THEN 'candidate_terminal_energy_censored_or_proxy' END,
        CASE WHEN l.source = 'irradiation' AND l.se_depletion_ratio_to_seb IS NULL
             THEN 'target_missing_depletion_ratio' END,
        CASE WHEN l.source = 'irradiation'
              AND (LOWER(COALESCE(l.se_depletion_net_doping_basis, '')) LIKE '%estimate%'
                OR LOWER(COALESCE(l.se_depletion_net_doping_basis, '')) LIKE '%reachthrough%')
             THEN 'target_depletion_ratio_estimated_doping' END,
        CASE WHEN l.source = 'irradiation'
              AND COALESCE(l.track_core_density_from_sp_j_cm3,
                           l.track_core_density_from_let_j_cm3) IS NULL
             THEN 'target_missing_track_core_density' END,
        CASE WHEN l.source = 'irradiation'
              AND COALESCE(l.track_core_density_from_sp_j_cm3,
                           l.track_core_density_from_let_j_cm3) IS NOT NULL
             THEN 'target_track_radius_assumed' END,
        CASE WHEN l.source = 'irradiation'
              AND l.track_core_density_from_sp_j_cm3 IS NULL
              AND l.track_core_density_from_let_j_cm3 IS NOT NULL
             THEN 'target_track_core_density_from_surface_let_only' END,
        CASE WHEN l.source = 'irradiation'
              AND UPPER(COALESCE(l.event_type, '')) = 'SEB'
              AND l.vds_collapse_fraction IS NULL
             THEN 'target_collapse_unknown' END,
        CASE WHEN l.mechanistic_regime IN ('unknown_single_event', 'unknown_electrical_proxy')
             THEN 'mechanistic_regime_unresolved' END
    ], NULL)::text[] AS feature_blockers,
    l.setting_name
FROM localized l;

CREATE UNIQUE INDEX idx_stress_energy_equivalence_features_key
    ON stress_energy_equivalence_features(stress_record_key);


CREATE OR REPLACE FUNCTION mech_energy_basis_family(basis text)
RETURNS text AS $$
    SELECT CASE
        WHEN TRIM(COALESCE(basis, '')) = '' THEN 'missing'
        WHEN LOWER(basis) LIKE '%commanded_or_stored%' THEN 'commanded_or_stored'
        WHEN LOWER(basis) LIKE '%proxy%' THEN 'proxy'
        WHEN LOWER(basis) LIKE '%integrated%' THEN 'integrated'
        ELSE 'other'
    END
$$ LANGUAGE sql IMMUTABLE;


-- Candidate-side electrical destruction boundary.  This is the SQL mirror of
-- destruction_boundary_interval() / candidate_failure_fraction() in
-- data_processing_scripts/mechanistic_energy_proxy.py: bracket inversions are
-- emitted and flagged, one-sided or under-count cells are unusable, and only
-- single-pulse SC/avalanche regimes contribute to per-pulse boundaries.
CREATE MATERIALIZED VIEW stress_candidate_destruction_boundary_energy_view AS
WITH source_rows AS (
    SELECT
        s.device_type,
        s.voltage_class,
        s.source,
        s.mechanistic_regime,
        s.test_timescale_class,
        s.electrical_terminal_energy_j AS boundary_energy_j,
        s.electrical_terminal_energy_basis AS boundary_energy_basis,
        mech_energy_basis_family(s.electrical_terminal_energy_basis)
            AS boundary_energy_basis_family,
        (s.response_reversibility = 'destructive_or_catastrophic')
            AS destructive,
        -- Positive survival evidence only; a destructive row is never survived
        -- evidence even when contradictory metadata pairs it with a non-fail
        -- outcome string (mirrors survived_evidence() in
        -- mechanistic_energy_proxy.py).
        (s.response_reversibility <> 'destructive_or_catastrophic'
         AND (s.response_reversibility = 'post_iv_measured'
              OR (s.avalanche_outcome IS NOT NULL
                  AND LOWER(s.avalanche_outcome) NOT LIKE '%fail%')))
            AS survived
    FROM stress_test_context_view s
    WHERE s.source IN ('sc', 'avalanche')
      AND s.device_type IS NOT NULL
      AND s.electrical_terminal_energy_j IS NOT NULL
      AND s.electrical_terminal_energy_j > 0.0
      AND s.mechanistic_regime NOT IN (
            'repetitive_avalanche_cumulative',
            'repetitive_sc_cumulative'
      )
),
same_basis_counts AS (
    SELECT
        device_type,
        source,
        test_timescale_class,
        boundary_energy_basis,
        boundary_energy_basis_family,
        COUNT(*) AS basis_rows
    FROM source_rows
    GROUP BY device_type, source, test_timescale_class,
             boundary_energy_basis, boundary_energy_basis_family
),
same_basis AS (
    SELECT DISTINCT ON (device_type, source, test_timescale_class)
        device_type,
        source,
        test_timescale_class,
        boundary_energy_basis,
        boundary_energy_basis_family,
        basis_rows
    FROM same_basis_counts
    ORDER BY device_type, source, test_timescale_class,
             basis_rows DESC, boundary_energy_basis
),
same_cells AS (
    SELECT
        'same_device'::text AS boundary_scope,
        r.device_type,
        MIN(r.voltage_class) AS voltage_class,
        r.source,
        r.test_timescale_class,
        MAX(r.boundary_energy_j) FILTER (WHERE r.survived)
            AS max_survived_energy_j,
        MIN(r.boundary_energy_j) FILTER (WHERE r.destructive)
            AS min_destructive_energy_j,
        COUNT(*) FILTER (WHERE r.survived) AS survived_count,
        COUNT(*) FILTER (WHERE r.destructive) AS destructive_count,
        COUNT(*) FILTER (WHERE NOT r.destructive AND NOT r.survived)
            AS unknown_outcome_count,
        COUNT(*) AS record_count,
        b.boundary_energy_basis,
        b.boundary_energy_basis_family,
        b.basis_rows AS boundary_energy_basis_rows
    FROM source_rows r
    JOIN same_basis b
      ON b.device_type IS NOT DISTINCT FROM r.device_type
     AND b.source = r.source
     AND b.test_timescale_class IS NOT DISTINCT FROM r.test_timescale_class
     AND r.boundary_energy_basis_family = b.boundary_energy_basis_family
    GROUP BY r.device_type, r.source,
             r.test_timescale_class, b.boundary_energy_basis,
             b.boundary_energy_basis_family, b.basis_rows
),
voltage_basis_counts AS (
    SELECT
        voltage_class,
        source,
        test_timescale_class,
        boundary_energy_basis,
        boundary_energy_basis_family,
        COUNT(*) AS basis_rows
    FROM source_rows
    WHERE voltage_class IS NOT NULL
    GROUP BY voltage_class, source, test_timescale_class,
             boundary_energy_basis, boundary_energy_basis_family
),
voltage_basis AS (
    SELECT DISTINCT ON (voltage_class, source, test_timescale_class)
        voltage_class,
        source,
        test_timescale_class,
        boundary_energy_basis,
        boundary_energy_basis_family,
        basis_rows
    FROM voltage_basis_counts
    ORDER BY voltage_class, source, test_timescale_class,
             basis_rows DESC, boundary_energy_basis
),
voltage_cells AS (
    SELECT
        r.voltage_class,
        r.source,
        r.test_timescale_class,
        MAX(r.boundary_energy_j) FILTER (WHERE r.survived)
            AS max_survived_energy_j,
        MIN(r.boundary_energy_j) FILTER (WHERE r.destructive)
            AS min_destructive_energy_j,
        COUNT(*) FILTER (WHERE r.survived) AS survived_count,
        COUNT(*) FILTER (WHERE r.destructive) AS destructive_count,
        COUNT(*) FILTER (WHERE NOT r.destructive AND NOT r.survived)
            AS unknown_outcome_count,
        COUNT(*) AS record_count,
        b.boundary_energy_basis,
        b.boundary_energy_basis_family,
        b.basis_rows AS boundary_energy_basis_rows
    FROM source_rows r
    JOIN voltage_basis b
      ON b.voltage_class IS NOT DISTINCT FROM r.voltage_class
     AND b.source = r.source
     AND b.test_timescale_class IS NOT DISTINCT FROM r.test_timescale_class
     AND r.boundary_energy_basis_family = b.boundary_energy_basis_family
    WHERE r.voltage_class IS NOT NULL
    GROUP BY r.voltage_class, r.source,
             r.test_timescale_class, b.boundary_energy_basis,
             b.boundary_energy_basis_family, b.basis_rows
),
candidate_groups AS (
    SELECT DISTINCT
        device_type,
        voltage_class,
        source,
        test_timescale_class
    FROM source_rows
),
raw_cells AS (
    SELECT
        boundary_scope,
        device_type,
        voltage_class,
        source,
        test_timescale_class,
        max_survived_energy_j,
        min_destructive_energy_j,
        survived_count,
        destructive_count,
        unknown_outcome_count,
        record_count,
        boundary_energy_basis,
        boundary_energy_basis_family,
        boundary_energy_basis_rows
    FROM same_cells

    UNION ALL

    SELECT
        'voltage_class_fallback'::text AS boundary_scope,
        cg.device_type,
        vc.voltage_class,
        vc.source,
        vc.test_timescale_class,
        vc.max_survived_energy_j,
        vc.min_destructive_energy_j,
        vc.survived_count,
        vc.destructive_count,
        vc.unknown_outcome_count,
        vc.record_count,
        vc.boundary_energy_basis,
        vc.boundary_energy_basis_family,
        vc.boundary_energy_basis_rows
    FROM candidate_groups cg
    JOIN voltage_cells vc
      ON vc.voltage_class IS NOT DISTINCT FROM cg.voltage_class
     AND vc.source = cg.source
     AND vc.test_timescale_class IS NOT DISTINCT FROM cg.test_timescale_class
    WHERE NOT EXISTS (
        SELECT 1
        FROM same_cells sc
        WHERE sc.device_type IS NOT DISTINCT FROM cg.device_type
          AND sc.source = cg.source
          AND sc.test_timescale_class IS NOT DISTINCT FROM cg.test_timescale_class
          AND sc.max_survived_energy_j IS NOT NULL
          AND sc.min_destructive_energy_j IS NOT NULL
          AND sc.survived_count >= 3
          AND sc.destructive_count >= 3
    )
),
bounded AS (
    SELECT
        r.*,
        CASE
            WHEN r.max_survived_energy_j IS NOT NULL
             AND r.max_survived_energy_j > 0.0
             AND r.min_destructive_energy_j IS NOT NULL
             AND r.min_destructive_energy_j > 0.0
                THEN LEAST(r.max_survived_energy_j, r.min_destructive_energy_j)
        END AS boundary_low_j,
        CASE
            WHEN r.max_survived_energy_j IS NOT NULL
             AND r.max_survived_energy_j > 0.0
             AND r.min_destructive_energy_j IS NOT NULL
             AND r.min_destructive_energy_j > 0.0
                THEN GREATEST(r.max_survived_energy_j, r.min_destructive_energy_j)
        END AS boundary_high_j,
        (r.max_survived_energy_j IS NOT NULL
         AND r.min_destructive_energy_j IS NOT NULL
         AND r.max_survived_energy_j > r.min_destructive_energy_j)
            AS boundary_inverted
    FROM raw_cells r
),
classified AS (
    SELECT
        b.*,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN b.max_survived_energy_j IS NULL
                   AND b.min_destructive_energy_j IS NULL
                 THEN 'destruction_boundary_missing' END,
            CASE WHEN b.max_survived_energy_j IS NULL
                   AND b.min_destructive_energy_j IS NOT NULL
                 THEN 'destruction_boundary_one_sided_destructive_only' END,
            CASE WHEN b.max_survived_energy_j IS NOT NULL
                   AND b.min_destructive_energy_j IS NULL
                 THEN 'destruction_boundary_one_sided_survived_only' END,
            CASE WHEN b.max_survived_energy_j IS NOT NULL
                   AND b.survived_count < 3
                 THEN 'destruction_boundary_insufficient_survived_count' END,
            CASE WHEN b.min_destructive_energy_j IS NOT NULL
                   AND b.destructive_count < 3
                 THEN 'destruction_boundary_insufficient_destructive_count' END
        ], NULL)::text[] AS boundary_blockers,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN b.boundary_inverted
                 THEN 'destruction_boundary_brackets_inverted_unit_spread' END,
            CASE WHEN b.min_destructive_energy_j IS NOT NULL
                 THEN 'destructive_energy_right_censored_lower_bound' END,
            CASE WHEN b.unknown_outcome_count > 0
                 THEN 'unknown_outcome_rows_excluded_from_bracket' END
        ], NULL)::text[] AS boundary_notes
    FROM bounded b
)
SELECT
    boundary_scope,
    device_type,
    voltage_class,
    source,
    test_timescale_class,
    max_survived_energy_j,
    min_destructive_energy_j,
    survived_count,
    destructive_count,
    unknown_outcome_count,
    record_count,
    boundary_low_j,
    boundary_high_j,
    boundary_inverted,
    boundary_energy_basis,
    boundary_energy_basis_family,
    boundary_energy_basis_rows,
    boundary_blockers,
    boundary_notes,
    (boundary_low_j IS NOT NULL
     AND boundary_high_j IS NOT NULL
     AND CARDINALITY(boundary_blockers) = 0) AS boundary_usable
FROM classified;

CREATE INDEX idx_candidate_destruction_boundary_energy_join
    ON stress_candidate_destruction_boundary_energy_view(
        device_type, source, test_timescale_class
    );
CREATE INDEX idx_candidate_destruction_boundary_energy_voltage
    ON stress_candidate_destruction_boundary_energy_view(
        voltage_class, source, test_timescale_class
    );


-- ── Phase 3: v2 candidate screening (parallel to v1; v1 left unchanged) ──────
--
-- Mirrors the pure functions in data_processing_scripts/mechanistic_energy_proxy.py.
-- mech_overlap_class is the SQL twin of overlap_class(): strong/partial/near/far/
-- missing, where "strong" means the overlap spans >= half the narrower interval
-- and a disjoint pair is "near" if the gap is no wider than the narrower interval.
CREATE OR REPLACE FUNCTION mech_overlap_class(
    low1 double precision, high1 double precision,
    low2 double precision, high2 double precision)
RETURNS text AS $$
    SELECT CASE
        WHEN low1 IS NULL OR high1 IS NULL OR low2 IS NULL OR high2 IS NULL
            THEN 'missing_interval'
        WHEN high1 < low1 OR high2 < low2 THEN 'missing_interval'
        WHEN low1 <= high2 AND low2 <= high1 THEN
            CASE
                WHEN LEAST(high1 - low1, high2 - low2) <= 0.0 THEN 'strong_overlap'
                WHEN (LEAST(high1, high2) - GREATEST(low1, low2))
                     / LEAST(high1 - low1, high2 - low2) >= 0.5
                    THEN 'strong_overlap'
                ELSE 'partial_overlap'
            END
        ELSE
            CASE
                WHEN LEAST(high1 - low1, high2 - low2) <= 0.0 THEN 'far_miss'
                WHEN (GREATEST(low1, low2) - LEAST(high1, high2))
                     <= LEAST(high1 - low1, high2 - low2)
                    THEN 'near_miss'
                ELSE 'far_miss'
            END
    END
$$ LANGUAGE sql IMMUTABLE;


-- Materialized for the same reason as stress_energy_equivalence_features
-- above: this view ranks over the full uncapped 1M+ row v1 pool per target
-- before capping to rank <= 10, and every dashboard chart queried the raw
-- view directly (Superset dataset = view, no query-time LIMIT pushdown).
-- The materialized result is bounded to <= 10 rows per target (~1300
-- targets), so chart queries hit an indexed few-thousand-row table instead.
CREATE MATERIALIZED VIEW stress_proxy_candidate_energy_v2 AS
WITH paired AS (
    SELECT
        v1.target_stress_record_key,
        v1.candidate_stress_record_key,
        'irradiation'::text AS target_source,
        v1.candidate_source,
        v1.device_type,
        v1.candidate_device_type,
        v1.candidate_voltage_class,
        v1.candidate_timescale_class,
        v1.target_event_type,
        v1.target_ion_species,
        v1.match_scope,
        v1.candidate_rank   AS candidate_rank_v1,
        v1.waveform_rank,
        v1.waveform_rankable,
        v1.energy_rankable,
        v1.candidate_energy_missing,
        v1.candidate_status AS candidate_status_v1,
        v1.proxy_claim_status AS proxy_claim_status_v1,
        v1.proxy_claim_basis AS proxy_claim_basis_v1,
        v1.proxy_claim_blockers AS proxy_claim_blockers_v1,
        v1.proxy_claim_summary AS proxy_claim_summary_v1,
        v1.decision_safe_rank AS decision_safe_rank_v1,
        v1.signature_claim_quality AS signature_claim_quality_v1,
        v1.target_energy_comparability_class,
        v1.candidate_energy_comparability_class,
        v1.waveform_distance AS waveform_distance_v1,
        v1.combined_screening_distance AS combined_screening_distance_v1,
        v1.energy_blended_control_distance,
        v1.collapse_delta,
        v1.gate_delta,
        v1.normalized_vds_delta,
        v1.duration_log_delta,
        v1.path_penalty,
        v1.damage_signature_axes_used,
        v1.damage_signature_coverage_score,
        v1.best_damage_distance,
        v1.damage_evidence_tier,
        v1.measured_comparability_status,
        v1.measured_sign_mismatch_axis_count,
        v1.prediction_comparability_status,
        v1.prediction_sign_mismatch_axis_count,
        v1.log_energy_delta,
        v1.log_energy_delta_dex,
        v1.signature_axis_distance,
        v1.damage_signature_distance,
        tf.mechanistic_regime AS target_mechanistic_regime,
        cf.mechanistic_regime AS candidate_mechanistic_regime,
        tf.electrical_terminal_energy_j AS target_terminal_energy_j,
        tf.se_depletion_ratio_to_seb,
        tf.track_core_energy_density_j_cm3,
        tf.effective_stress_time_s AS target_effective_stress_time_s,
        tf.feature_blockers AS target_feature_blockers,
        cf.electrical_terminal_energy_j AS candidate_terminal_energy_j,
        cf.terminal_energy_basis AS candidate_terminal_energy_basis,
        mech_energy_basis_family(cf.terminal_energy_basis)
            AS candidate_terminal_energy_basis_family,
        cf.terminal_energy_density_bulk_j_cm3 AS candidate_bulk_energy_density_j_cm3,
        cb.boundary_scope AS candidate_boundary_scope,
        cb.boundary_low_j AS candidate_boundary_low_j,
        cb.boundary_high_j AS candidate_boundary_high_j,
        cb.boundary_usable AS candidate_boundary_usable,
        cb.boundary_energy_basis AS candidate_boundary_energy_basis,
        cb.boundary_energy_basis_family AS candidate_boundary_energy_basis_family,
        cb.boundary_blockers AS candidate_boundary_blockers,
        cb.boundary_notes AS candidate_boundary_notes,
        CASE
            WHEN cf.electrical_terminal_energy_j IS NOT NULL
             AND cf.electrical_terminal_energy_j > 0.0
             AND cb.boundary_low_j IS NOT NULL
             AND cb.boundary_low_j > 0.0
             AND cb.boundary_high_j IS NOT NULL
             AND cb.boundary_high_j > 0.0
                THEN cf.electrical_terminal_energy_j / cb.boundary_high_j
        END AS candidate_failure_fraction_low,
        CASE
            WHEN cf.electrical_terminal_energy_j IS NOT NULL
             AND cf.electrical_terminal_energy_j > 0.0
             AND cb.boundary_low_j IS NOT NULL
             AND cb.boundary_low_j > 0.0
             AND cb.boundary_high_j IS NOT NULL
             AND cb.boundary_high_j > 0.0
                THEN cf.electrical_terminal_energy_j
                     / SQRT(cb.boundary_low_j * cb.boundary_high_j)
        END AS candidate_failure_fraction_point,
        CASE
            WHEN cf.electrical_terminal_energy_j IS NOT NULL
             AND cf.electrical_terminal_energy_j > 0.0
             AND cb.boundary_low_j IS NOT NULL
             AND cb.boundary_low_j > 0.0
             AND cb.boundary_high_j IS NOT NULL
             AND cb.boundary_high_j > 0.0
                THEN cf.electrical_terminal_energy_j / cb.boundary_low_j
        END AS candidate_failure_fraction_high,
        CASE
            WHEN cb.boundary_scope IS NOT NULL
                THEN cb.boundary_scope || ':' || cb.boundary_energy_basis
        END AS candidate_failure_fraction_basis,
        tl.label AS truth_label,
        tl.label_basis AS truth_label_basis,
        tl.reviewer AS truth_reviewer,
        tl.review_date AS truth_review_date,
        tl.notes AS truth_label_notes,
        CASE
            WHEN tl.label = 'equivalent'
             AND tl.label_basis = 'measured_post_iv'
                THEN 'validated_by_curated_measured_post_iv'
            WHEN tl.label = 'equivalent'
                THEN 'curated_equivalent_non_measured'
            WHEN tl.label = 'not_equivalent'
                THEN 'curated_not_equivalent'
            WHEN tl.label = 'uncertain'
                THEN 'curated_uncertain'
            ELSE 'no_curated_truth'
        END AS truth_validation_status,
        cf.effective_stress_time_s AS candidate_effective_stress_time_s,
        cf.pulse_count_in_sequence AS candidate_pulse_count_in_sequence,
        cf.feature_blockers AS candidate_feature_blockers,
        (tf.stress_record_key IS NOT NULL
         AND (tf.se_depletion_ratio_to_seb IS NOT NULL
              OR tf.electrical_terminal_energy_j IS NOT NULL)) AS target_has_energy_context,
        rc.match_class    AS regime_match_class,
        rc.status_ceiling AS regime_status_ceiling,
        rc.preference     AS regime_preference,
        rc.rationale      AS regime_rationale,
        -- Target severity stays on the Kosier stored-field axis, but its
        -- uncertainty now folds in the published threshold spread (SEB
        -- 175/207/230 uJ/cm2, SELC 44/60/68 uJ/cm2) rather than treating
        -- the nominal denominator as exact.
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN tf.se_depletion_ratio_lower_to_selc * (60.0 / 68.0)
             ELSE tf.se_depletion_ratio_lower_to_seb * (207.0 / 230.0)
        END AS target_severity_low,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN tf.se_depletion_ratio_upper_to_selc * (60.0 / 44.0)
             ELSE tf.se_depletion_ratio_upper_to_seb * (207.0 / 175.0)
        END AS target_severity_high,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN tf.se_depletion_ratio_to_selc
             ELSE tf.se_depletion_ratio_to_seb END AS target_severity_point_ratio,
        -- Legacy candidate Kosier-denominator ratios stay visible only as
        -- context; they no longer rank or gate claims.
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN cf.terminal_ratio_to_selc_lower
             ELSE cf.terminal_ratio_to_seb_lower
        END AS candidate_severity_low_kosier_context,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN cf.terminal_ratio_to_selc_upper
             ELSE cf.terminal_ratio_to_seb_upper
        END AS candidate_severity_high_kosier_context,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN cf.terminal_ratio_to_selc_critical
             ELSE cf.terminal_ratio_to_seb_critical
        END AS candidate_severity_point_ratio_kosier_context
    FROM stress_proxy_candidate_ranked_view v1
    LEFT JOIN stress_energy_equivalence_features tf
        ON tf.stress_record_key = v1.target_stress_record_key
    LEFT JOIN stress_energy_equivalence_features cf
        ON cf.stress_record_key = v1.candidate_stress_record_key
    LEFT JOIN LATERAL (
        SELECT b.*
        FROM stress_candidate_destruction_boundary_energy_view b
        WHERE b.source = v1.candidate_source
          AND b.test_timescale_class IS NOT DISTINCT FROM v1.candidate_timescale_class
          AND (
                (b.boundary_scope = 'same_device'
                 AND b.device_type IS NOT DISTINCT FROM v1.candidate_device_type)
             OR (b.boundary_scope = 'voltage_class_fallback'
                 AND b.voltage_class IS NOT DISTINCT FROM v1.candidate_voltage_class)
          )
        ORDER BY
            CASE
                WHEN b.boundary_scope = 'same_device' AND b.boundary_usable THEN 0
                WHEN b.boundary_scope = 'voltage_class_fallback' THEN 1
                WHEN b.boundary_scope = 'same_device' THEN 2
                ELSE 3
            END
        LIMIT 1
    ) cb ON TRUE
    LEFT JOIN proxy_truth_labels tl
        ON tl.target_stress_record_key = v1.target_stress_record_key
       AND tl.candidate_stress_record_key = v1.candidate_stress_record_key
    LEFT JOIN LATERAL (
        SELECT rc.match_class, rc.status_ceiling, rc.preference, rc.rationale
        FROM stress_regime_compatibility rc
        WHERE rc.target_regime = tf.mechanistic_regime
          AND (rc.candidate_regime = cf.mechanistic_regime OR rc.candidate_regime = 'any')
        ORDER BY CASE WHEN rc.candidate_regime = cf.mechanistic_regime THEN 0 ELSE 1 END
        LIMIT 1
    ) rc ON TRUE
),
classed AS (
    SELECT
        q.*,
        CASE
            WHEN q.candidate_failure_fraction_gate_usable THEN
                mech_overlap_class(
                    LN(NULLIF(q.target_severity_low, 0.0)),
                    LN(NULLIF(q.target_severity_high, 0.0)),
                    LN(NULLIF(q.candidate_failure_fraction_low, 0.0)),
                    LN(NULLIF(q.candidate_failure_fraction_high, 0.0))
                )
            ELSE 'missing_interval'
        END AS candidate_failure_fraction_overlap_class,
        CASE
            WHEN q.log_energy_delta IS NULL THEN 'missing_interval'
            WHEN ABS(q.log_energy_delta) <= 0.5 THEN 'strong_overlap'
            WHEN ABS(q.log_energy_delta) <= 1.5 THEN 'partial_overlap'
            WHEN ABS(q.log_energy_delta) <= 3.0 THEN 'near_miss'
            ELSE 'far_miss'
        END AS terminal_energy_overlap_class,
        CASE
            WHEN q.target_effective_stress_time_s IS NOT NULL
             AND q.target_effective_stress_time_s > 0.0
             AND q.candidate_effective_stress_time_s IS NOT NULL
             AND q.candidate_effective_stress_time_s > 0.0 THEN
                CASE
                    WHEN ABS(LN(q.candidate_effective_stress_time_s)
                             - LN(q.target_effective_stress_time_s)) <= 1.0 THEN 'strong_overlap'
                    WHEN ABS(LN(q.candidate_effective_stress_time_s)
                             - LN(q.target_effective_stress_time_s)) <= 3.0 THEN 'partial_overlap'
                    WHEN ABS(LN(q.candidate_effective_stress_time_s)
                             - LN(q.target_effective_stress_time_s)) <= 5.0 THEN 'near_miss'
                    ELSE 'far_miss'
                END
            ELSE 'missing_interval'
        END AS timescale_overlap_class,
        CASE
            WHEN q.target_mechanistic_regime IN (
                    'selci_gate_coupled', 'selcii_drain_source_cumulative', 'tid_dd_cumulative')
                THEN CASE WHEN COALESCE(q.candidate_pulse_count_in_sequence, 0) > 1
                          THEN 'cumulative_present' ELSE 'cumulative_missing' END
            ELSE 'not_applicable'
        END AS cumulative_exposure_overlap_class,
        q.damage_evidence_tier AS damage_evidence_class,
        (q.target_mechanistic_regime IN (
            'selci_gate_coupled', 'selcii_drain_source_cumulative', 'tid_dd_cumulative')
         AND COALESCE(q.candidate_pulse_count_in_sequence, 0) > 1) AS cumulative_pair
    FROM (
        SELECT
            p.*,
            COALESCE(p.regime_match_class, 'analog_questionable') AS regime_match_class_final,
            CASE WHEN p.regime_match_class IS NULL THEN 'analog_questionable'
                 ELSE p.regime_status_ceiling END AS regime_status_ceiling_final,
            COALESCE(p.regime_preference, 3) AS regime_preference_final,
            -- Legacy context only: candidate bulk-terminal/Kosier denominator.
            mech_overlap_class(
                LN(NULLIF(p.target_severity_low, 0.0)),
                LN(NULLIF(p.target_severity_high, 0.0)),
                LN(NULLIF(p.candidate_severity_low_kosier_context, 0.0)),
                LN(NULLIF(p.candidate_severity_high_kosier_context, 0.0))
            ) AS critical_severity_overlap_class_kosier_context,
            CASE
                WHEN p.candidate_bulk_energy_density_j_cm3 IS NULL
                  OR p.candidate_bulk_energy_density_j_cm3 <= 0.0
                  OR p.track_core_energy_density_j_cm3 IS NULL
                  OR p.track_core_energy_density_j_cm3 <= 0.0 THEN NULL
                ELSE LOG(p.candidate_bulk_energy_density_j_cm3)
                   - LOG(p.track_core_energy_density_j_cm3)
            END AS localization_mismatch_log10,
            (
                p.candidate_failure_fraction_point IS NOT NULL
                AND COALESCE(p.candidate_boundary_usable, FALSE)
                AND p.candidate_terminal_energy_basis_family
                    = p.candidate_boundary_energy_basis_family
                AND COALESCE(
                    p.candidate_mechanistic_regime NOT IN (
                        'repetitive_avalanche_cumulative',
                        'repetitive_sc_cumulative'
                    ),
                    FALSE
                )
            ) AS candidate_failure_fraction_gate_usable
        FROM paired p
    ) q
),
statused AS (
    SELECT
        c.*,
        CASE
            WHEN c.regime_match_class_final = 'mechanism_mismatch'
                THEN 'mechanistic_regime_mismatch'
            WHEN c.match_scope = 'cross_device'
                THEN 'mechanistic_cross_device_screening_only'
            WHEN NOT c.target_has_energy_context
                THEN 'mechanistic_missing_energy_context'
            WHEN c.measured_comparability_status IS NULL
             AND c.prediction_comparability_status IS NULL
                THEN 'mechanistic_missing_damage_context'
            WHEN c.regime_status_ceiling_final = 'analog_questionable' THEN
                CASE WHEN c.cumulative_pair THEN 'mechanistic_cumulative_candidate'
                     ELSE 'mechanistic_analog_questionable' END
            WHEN c.measured_comparability_status IN ('strong', 'usable')
                THEN 'mechanistic_measured_candidate'
            WHEN c.prediction_comparability_status IN ('strong', 'usable')
                THEN 'mechanistic_predicted_candidate'
            WHEN c.cumulative_pair
                THEN 'mechanistic_cumulative_candidate'
            WHEN c.energy_rankable
                THEN 'mechanistic_energy_screening_only'
            ELSE 'mechanistic_inspect_manually'
        END AS mechanistic_energy_candidate_status
    FROM classed c
),
finalized AS (
    SELECT
        s.*,
        CASE s.mechanistic_energy_candidate_status
            WHEN 'mechanistic_measured_candidate' THEN 1
            WHEN 'mechanistic_predicted_candidate' THEN 2
            WHEN 'mechanistic_cumulative_candidate' THEN 3
            WHEN 'mechanistic_energy_screening_only' THEN 4
            WHEN 'mechanistic_analog_questionable' THEN 5
            WHEN 'mechanistic_cross_device_screening_only' THEN 6
            WHEN 'mechanistic_missing_damage_context' THEN 6
            WHEN 'mechanistic_missing_energy_context' THEN 6
            WHEN 'mechanistic_inspect_manually' THEN 7
            WHEN 'mechanistic_regime_mismatch' THEN 8
            ELSE 9
        END AS mechanistic_energy_status_priority,
        CASE
            WHEN s.mechanistic_energy_candidate_status IN (
                'mechanistic_measured_candidate', 'mechanistic_predicted_candidate',
                'mechanistic_cumulative_candidate')
                THEN 'primary_or_cumulative_candidate'
            WHEN s.mechanistic_energy_candidate_status IN (
                'mechanistic_energy_screening_only', 'mechanistic_analog_questionable')
                THEN 'weak_or_questionable'
            WHEN s.mechanistic_energy_candidate_status = 'mechanistic_regime_mismatch'
                THEN 'regime_mismatch_last'
            ELSE 'screening_or_blocked'
        END AS mechanistic_energy_screening_bucket,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN s.regime_match_class_final = 'mechanism_mismatch' THEN 'regime_mismatch' END,
            CASE WHEN s.match_scope = 'cross_device' THEN 'cross_device_screening_only' END,
            CASE WHEN s.measured_comparability_status IS NULL
                  AND s.prediction_comparability_status IS NULL THEN 'damage_context_missing' END,
            CASE WHEN s.measured_comparability_status IS NULL
                  AND s.prediction_comparability_status IS NOT NULL THEN 'post_iv_predicted_only' END,
            CASE WHEN s.candidate_failure_fraction_overlap_class = 'far_miss'
                 THEN 'candidate_failure_fraction_far_miss' END,
            CASE WHEN NOT COALESCE(s.candidate_failure_fraction_gate_usable, FALSE)
                 THEN 'candidate_failure_fraction_missing' END,
            CASE WHEN s.candidate_boundary_scope IS NULL
                 THEN 'candidate_failure_fraction_missing_boundary' END,
            CASE WHEN s.candidate_boundary_energy_basis_family IS NOT NULL
                   AND s.candidate_terminal_energy_basis_family
                       IS DISTINCT FROM s.candidate_boundary_energy_basis_family
                 THEN 'boundary_energy_basis_family_mismatch' END,
            CASE WHEN s.candidate_mechanistic_regime IN (
                    'repetitive_avalanche_cumulative',
                    'repetitive_sc_cumulative')
                 THEN 'boundary_repetitive_regime_excluded' END,
            CASE WHEN s.cumulative_exposure_overlap_class = 'cumulative_missing'
                 THEN 'candidate_missing_cumulative_energy' END,
            CASE WHEN NOT s.target_has_energy_context THEN 'target_missing_energy_context' END
        ], NULL)::text[]
        || COALESCE(s.candidate_boundary_blockers, ARRAY[]::text[])
        || COALESCE(s.target_feature_blockers, ARRAY[]::text[])
        || COALESCE(s.candidate_feature_blockers, ARRAY[]::text[]) AS energy_v2_blockers,
        ARRAY_REMOVE(ARRAY[
            'candidate_failure_fraction_is_screening_descriptor_only',
            CASE WHEN s.localization_mismatch_log10 IS NOT NULL THEN
                'localization_mismatch_' ||
                CASE
                    WHEN ABS(s.localization_mismatch_log10) > 4.0 THEN 'extreme_localized_vs_bulk'
                    WHEN ABS(s.localization_mismatch_log10) > 2.0 THEN 'large_localized_vs_bulk'
                    WHEN ABS(s.localization_mismatch_log10) > 1.0 THEN 'moderate_localized_vs_bulk'
                    ELSE 'comparable'
                END
            END,
            'timescale_basis_duration_proxy',
            CASE WHEN UPPER(COALESCE(s.target_event_type, '')) IN ('SELCI', 'SELCII')
                   AND s.candidate_failure_fraction_gate_usable
                 THEN 'target_threshold_is_selc_onset_candidate_threshold_is_destruction' END,
            CASE WHEN s.regime_rationale IS NOT NULL THEN 'regime: ' || s.regime_rationale END
        ], NULL)::text[]
        || COALESCE(s.candidate_boundary_notes, ARRAY[]::text[])
        AS energy_v2_notes
    FROM statused s
),
claimed AS (
    SELECT
        f.*,
        (
            COALESCE(f.proxy_claim_blockers_v1, ARRAY[]::text[])
            || COALESCE(f.energy_v2_blockers, ARRAY[]::text[])
            || ARRAY_REMOVE(ARRAY[
                CASE WHEN f.truth_label = 'not_equivalent'
                     THEN 'curated_not_equivalent' END,
                CASE WHEN f.truth_label = 'equivalent'
                       AND COALESCE(f.truth_label_basis, '') <> 'measured_post_iv'
                     THEN 'curated_equivalent_non_measured_not_validation' END,
                CASE WHEN f.truth_label = 'uncertain'
                     THEN 'curated_truth_uncertain' END,
                CASE WHEN f.truth_label IS NULL
                     THEN 'no_curated_truth_label' END,
                CASE WHEN COALESCE(f.measured_sign_mismatch_axis_count, 0) > 0
                     THEN 'measured_damage_sign_mismatch' END,
                CASE WHEN COALESCE(f.prediction_sign_mismatch_axis_count, 0) > 0
                     THEN 'predicted_damage_sign_mismatch' END
            ], NULL)::text[]
        ) AS proxy_claim_blockers,
        CASE
            WHEN f.truth_label = 'equivalent'
             AND f.truth_label_basis = 'measured_post_iv'
                THEN 'validated'
            WHEN f.truth_label = 'not_equivalent'
              OR f.mechanistic_energy_candidate_status = 'mechanistic_regime_mismatch'
                THEN 'blocked'
            WHEN f.truth_label = 'equivalent'
                THEN 'curation_candidate'
            WHEN f.proxy_claim_status_v1 = 'validation_candidate'
             AND f.match_scope = 'same_device'
             AND f.mechanistic_energy_candidate_status NOT IN (
                    'mechanistic_cross_device_screening_only',
                    'mechanistic_missing_energy_context',
                    'mechanistic_regime_mismatch'
                 )
             AND f.candidate_failure_fraction_overlap_class <> 'far_miss'
             AND COALESCE(f.measured_sign_mismatch_axis_count, 0) = 0
                THEN 'validation_candidate'
            WHEN f.truth_label = 'uncertain'
              OR f.proxy_claim_status_v1 IN ('validation_candidate', 'curation_candidate')
              OR (f.match_scope = 'same_device'
                  AND f.damage_evidence_class = 'measured_damage')
                THEN 'curation_candidate'
            ELSE 'screening_only'
        END AS proxy_claim_status,
        CASE
            WHEN f.truth_label = 'equivalent'
             AND f.truth_label_basis = 'measured_post_iv'
                THEN 'curated_truth_measured_post_iv'
            WHEN f.truth_label = 'equivalent'
                THEN 'curated_truth_non_measured'
            WHEN f.truth_label = 'not_equivalent'
                THEN 'curated_truth_rejection'
            WHEN f.mechanistic_energy_candidate_status = 'mechanistic_regime_mismatch'
                THEN 'mechanistic_regime_mismatch'
            WHEN f.proxy_claim_status_v1 = 'validation_candidate'
             AND f.match_scope = 'same_device'
             AND f.candidate_failure_fraction_overlap_class <> 'far_miss'
                THEN 'same_device_measured_post_iv_plus_energy_screen'
            WHEN f.truth_label = 'uncertain'
                THEN 'curated_uncertain_needs_review'
            WHEN f.proxy_claim_status_v1 IN ('validation_candidate', 'curation_candidate')
              OR (f.match_scope = 'same_device'
                  AND f.damage_evidence_class = 'measured_damage')
                THEN 'same_device_needs_truth_curation'
            WHEN f.match_scope = 'cross_device' THEN 'cross_device_screening'
            WHEN f.damage_evidence_class = 'waveform_only' THEN 'waveform_only'
            ELSE 'screening_evidence_only'
        END AS proxy_claim_basis,
        CASE
            WHEN f.truth_label = 'equivalent'
             AND f.truth_label_basis = 'measured_post_iv'
                THEN 'Curated measured post-IV truth label validates this proxy pair.'
            WHEN f.truth_label = 'not_equivalent'
              OR f.mechanistic_energy_candidate_status = 'mechanistic_regime_mismatch'
                THEN 'Required evidence rejects or blocks this proxy pair.'
            WHEN f.truth_label = 'equivalent'
                THEN 'Curated equivalent label is not measured post-IV; keep as curation evidence.'
            WHEN f.proxy_claim_status_v1 = 'validation_candidate'
             AND f.match_scope = 'same_device'
             AND f.candidate_failure_fraction_overlap_class <> 'far_miss'
                THEN 'Same-device measured evidence and mechanistic screening support validation review.'
            WHEN f.truth_label = 'uncertain'
              OR f.proxy_claim_status_v1 IN ('validation_candidate', 'curation_candidate')
              OR (f.match_scope = 'same_device'
                  AND f.damage_evidence_class = 'measured_damage')
                THEN 'Promising row for human truth-label curation; not validated yet.'
            ELSE 'Mechanistic-energy row is useful for visual screening only.'
        END AS proxy_claim_summary
    FROM finalized f
),
ranked2 AS (
    -- Rank over the FULL same-/cross-device pool (uncapped v1 input) so the
    -- mechanistic prior can surface a candidate v1 buried below rank 10, then
    -- emit v2's own top 10 to keep the output bounded.
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY f.target_stress_record_key
            ORDER BY
                CASE f.match_scope WHEN 'same_device' THEN 0 ELSE 1 END,
                f.mechanistic_energy_status_priority,
                CASE f.regime_match_class_final
                    WHEN 'mechanism_mismatch' THEN 2
                    WHEN 'analog_questionable' THEN 1
                    ELSE 0
                END,
                CASE f.candidate_failure_fraction_overlap_class
                    WHEN 'strong_overlap' THEN 0
                    WHEN 'partial_overlap' THEN 1
                    WHEN 'near_miss' THEN 2
                    WHEN 'far_miss' THEN 3
                    ELSE 4
                END,
                CASE f.terminal_energy_overlap_class
                    WHEN 'strong_overlap' THEN 0
                    WHEN 'partial_overlap' THEN 1
                    WHEN 'near_miss' THEN 2
                    WHEN 'far_miss' THEN 3
                    ELSE 4
                END,
                CASE
                    WHEN f.candidate_failure_fraction_point IS NOT NULL
                     AND f.candidate_failure_fraction_point > 0.0
                     AND f.target_severity_point_ratio IS NOT NULL
                     AND f.target_severity_point_ratio > 0.0
                        THEN ABS(LN(f.candidate_failure_fraction_point)
                                 - LN(f.target_severity_point_ratio))
                    ELSE ABS(f.log_energy_delta)
                END ASC NULLS LAST,
                CASE f.cumulative_exposure_overlap_class
                    WHEN 'cumulative_present' THEN 0
                    WHEN 'not_applicable' THEN 1
                    WHEN 'cumulative_missing' THEN 2
                    ELSE 3
                END,
                f.candidate_stress_record_key
        ) AS mechanistic_energy_candidate_rank
    FROM claimed f
    -- Pure v2 denominator: censored/missing-energy targets are intentionally
    -- excluded from the energy-rank surface rather than retained with a NULL
    -- energy rank. v1 remains the surface for waveform-only/censored cases.
    WHERE f.energy_rankable
)
SELECT
    target_stress_record_key,
    candidate_stress_record_key,
    target_source,
    candidate_source,
    device_type,
    target_event_type,
    target_ion_species,
    candidate_device_type,
    candidate_voltage_class,
    candidate_timescale_class,
    match_scope,
    candidate_rank_v1,
    waveform_rank,
    waveform_rankable,
    energy_rankable,
    candidate_energy_missing,
    candidate_status_v1,
    proxy_claim_status_v1,
    proxy_claim_basis_v1,
    proxy_claim_blockers_v1,
    proxy_claim_summary_v1,
    decision_safe_rank_v1,
    signature_claim_quality_v1,
    target_energy_comparability_class,
    candidate_energy_comparability_class,
    waveform_distance_v1,
    combined_screening_distance_v1,
    energy_blended_control_distance,
    best_damage_distance,
    collapse_delta,
    gate_delta,
    normalized_vds_delta,
    duration_log_delta,
    path_penalty,
    damage_signature_axes_used,
    damage_signature_coverage_score,
    log_energy_delta,
    log_energy_delta_dex,
    signature_axis_distance,
    damage_signature_distance,
    target_mechanistic_regime,
    candidate_mechanistic_regime,
    regime_match_class_final AS regime_match_class,
    regime_status_ceiling_final AS regime_status_ceiling,
    regime_preference_final AS regime_preference,
    regime_rationale,
    terminal_energy_overlap_class,
    candidate_failure_fraction_overlap_class,
    critical_severity_overlap_class_kosier_context,
    localization_mismatch_log10,
    timescale_overlap_class,
    timescale_overlap_class AS power_rate_overlap_class,
    cumulative_exposure_overlap_class,
    damage_evidence_class,
    -- Post-IV comparability statuses (from v1) exported so the regression
    -- check first_order_measured_not_capped and the curation exports can read
    -- them without re-joining the ranked view.
    measured_comparability_status,
    prediction_comparability_status,
    measured_sign_mismatch_axis_count,
    prediction_sign_mismatch_axis_count,
    truth_label,
    truth_label_basis,
    truth_reviewer,
    truth_review_date,
    truth_label_notes,
    truth_validation_status,
    proxy_claim_status,
    proxy_claim_basis,
    proxy_claim_blockers,
    proxy_claim_summary,
    -- Target severity remains stored-field/Kosier. Candidate severity for
    -- ranking/gating is the candidate's fraction of its own measured electrical
    -- destruction boundary; legacy Kosier candidate ratios are context only.
    target_severity_low,
    target_severity_high,
    target_severity_point_ratio,
    candidate_failure_fraction_low,
    candidate_failure_fraction_point,
    candidate_failure_fraction_high,
    candidate_failure_fraction_basis,
    candidate_failure_fraction_gate_usable,
    candidate_boundary_scope,
    candidate_boundary_low_j,
    candidate_boundary_high_j,
    candidate_boundary_energy_basis,
    candidate_severity_low_kosier_context,
    candidate_severity_high_kosier_context,
    candidate_severity_point_ratio_kosier_context,
    mechanistic_energy_screening_bucket,
    mechanistic_energy_candidate_status,
    mechanistic_energy_status_priority,
    mechanistic_energy_candidate_rank,
    mechanistic_energy_candidate_rank AS energy_rank,
    energy_v2_blockers,
    energy_v2_notes
FROM ranked2
WHERE mechanistic_energy_candidate_rank <= 10;

CREATE INDEX idx_stress_proxy_candidate_energy_v2_target_rank
    ON stress_proxy_candidate_energy_v2(target_stress_record_key, mechanistic_energy_candidate_rank);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_rank
    ON stress_proxy_candidate_energy_v2(mechanistic_energy_candidate_rank);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_device
    ON stress_proxy_candidate_energy_v2(device_type);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_regime
    ON stress_proxy_candidate_energy_v2(target_mechanistic_regime);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_status
    ON stress_proxy_candidate_energy_v2(mechanistic_energy_candidate_status);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_claim_status
    ON stress_proxy_candidate_energy_v2(proxy_claim_status);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_match_scope
    ON stress_proxy_candidate_energy_v2(match_scope);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_overlap
    ON stress_proxy_candidate_energy_v2(candidate_failure_fraction_overlap_class);
CREATE INDEX idx_stress_proxy_candidate_energy_v2_source
    ON stress_proxy_candidate_energy_v2(candidate_source);

-- v3 combined screening vector.  This is intentionally not a validation claim:
-- weights are settings-driven and uncalibrated until curated truth labels are
-- dense enough to support calibration. The first implementation is deliberately
-- over v2's energy-rank top-10 materialized surface, so waveform-strong but
-- energy-weak/missing rows are outside this v3 denominator until a full-pool
-- v3 refactor is justified.
CREATE MATERIALIZED VIEW stress_proxy_candidate_combined_v3 AS
WITH vector_base AS (
    SELECT
        v2.*,
        cfg.setting_name AS combined_ranker_setting_name,
        cfg.description AS combined_ranker_description,
        cfg.signature_axis_weight,
        cfg.duration_weight,
        cfg.log_energy_weight,
        cfg.failure_fraction_weight,
        cfg.post_iv_damage_weight,
        cfg.regime_path_weight,
        cfg.coverage_gap_weight,
        CASE v2.candidate_failure_fraction_overlap_class
            WHEN 'strong_overlap' THEN 0.0
            WHEN 'partial_overlap' THEN 1.0
            WHEN 'near_miss' THEN 2.0
            WHEN 'far_miss' THEN 3.0
            ELSE 4.0
        END AS failure_fraction_overlap_score,
        CASE v2.terminal_energy_overlap_class
            WHEN 'strong_overlap' THEN 0.0
            WHEN 'partial_overlap' THEN 1.0
            WHEN 'near_miss' THEN 2.0
            WHEN 'far_miss' THEN 3.0
            ELSE 4.0
        END AS terminal_energy_overlap_score,
        CASE
            WHEN v2.candidate_failure_fraction_point IS NOT NULL
             AND v2.candidate_failure_fraction_point > 0.0
             AND v2.target_severity_point_ratio IS NOT NULL
             AND v2.target_severity_point_ratio > 0.0
                THEN ABS(LN(v2.candidate_failure_fraction_point)
                         - LN(v2.target_severity_point_ratio))
        END AS failure_fraction_log_delta,
        GREATEST(0.0, 1.0 - COALESCE(v2.damage_signature_coverage_score, 0.0))
            AS damage_signature_coverage_gap
    FROM stress_proxy_candidate_energy_v2 v2
    CROSS JOIN stress_proxy_combined_ranker_settings cfg
    WHERE cfg.setting_name = 'screening_default'
), scored AS (
    SELECT
        b.*,
        SQRT(
            b.signature_axis_weight * POWER(COALESCE(b.signature_axis_distance, 3.0), 2)
          + b.duration_weight * POWER(COALESCE(b.duration_log_delta, 1.0), 2)
          + b.log_energy_weight * POWER(COALESCE(ABS(b.log_energy_delta), 5.0), 2)
          -- Missing candidate destruction-boundary evidence must not silently
          -- reuse terminal-energy overlap as a second, apparently independent
          -- feature. The explicit missing-interval score is conservative and
          -- remains visible through the basis/imputation columns emitted below.
          + b.failure_fraction_weight * POWER(COALESCE(b.failure_fraction_log_delta,
                                                       b.failure_fraction_overlap_score), 2)
          + b.post_iv_damage_weight * POWER(COALESCE(b.best_damage_distance, 2.50), 2)
          + b.regime_path_weight * POWER(COALESCE(b.path_penalty, 0.75), 2)
          + b.coverage_gap_weight * POWER(b.damage_signature_coverage_gap, 2)
        ) AS combined_vector_distance
    FROM vector_base b
), ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.target_stress_record_key
            ORDER BY
                CASE s.match_scope WHEN 'same_device' THEN 0 ELSE 1 END,
                CASE s.proxy_claim_status
                    WHEN 'validated' THEN 0
                    WHEN 'validation_candidate' THEN 1
                    WHEN 'curation_candidate' THEN 2
                    WHEN 'screening_only' THEN 3
                    WHEN 'blocked' THEN 4
                    ELSE 5
                END,
                s.mechanistic_energy_status_priority,
                CASE s.regime_match_class
                    WHEN 'mechanism_mismatch' THEN 2
                    WHEN 'analog_questionable' THEN 1
                    ELSE 0
                END,
                s.combined_vector_distance ASC NULLS LAST,
                s.energy_rank ASC NULLS LAST,
                s.waveform_rank ASC NULLS LAST,
                s.candidate_stress_record_key
        ) AS combined_rank
    FROM scored s
)
SELECT
    target_stress_record_key,
    candidate_stress_record_key,
    target_source,
    candidate_source,
    device_type,
    target_event_type,
    target_ion_species,
    candidate_device_type,
    candidate_voltage_class,
    candidate_timescale_class,
    match_scope,
    waveform_rank,
    energy_rank,
    combined_rank,
    combined_vector_distance,
    combined_ranker_setting_name,
    combined_ranker_description,
    signature_axis_weight,
    duration_weight,
    log_energy_weight,
    failure_fraction_weight,
    post_iv_damage_weight,
    regime_path_weight,
    coverage_gap_weight,
    waveform_rankable,
    energy_rankable,
    candidate_energy_missing,
    signature_axis_distance,
    collapse_delta,
    gate_delta,
    normalized_vds_delta,
    duration_log_delta,
    log_energy_delta,
    log_energy_delta_dex,
    terminal_energy_overlap_class,
    terminal_energy_overlap_score,
    target_severity_point_ratio,
    candidate_failure_fraction_point,
    failure_fraction_log_delta,
    candidate_failure_fraction_overlap_class,
    failure_fraction_overlap_score,
    (failure_fraction_log_delta IS NULL) AS failure_fraction_component_imputed,
    CASE
        WHEN failure_fraction_log_delta IS NOT NULL
            THEN 'own_candidate_destruction_boundary'
        ELSE 'explicit_missing_boundary_penalty'
    END AS failure_fraction_component_basis,
    best_damage_distance,
    damage_signature_axes_used,
    damage_signature_coverage_score,
    damage_signature_coverage_gap,
    regime_match_class,
    regime_status_ceiling,
    regime_preference,
    path_penalty,
    proxy_claim_status,
    proxy_claim_basis,
    proxy_claim_blockers,
    proxy_claim_summary,
    mechanistic_energy_candidate_status,
    mechanistic_energy_status_priority,
    mechanistic_energy_screening_bucket,
    candidate_rank_v1,
    mechanistic_energy_candidate_rank,
    candidate_status_v1,
    proxy_claim_status_v1,
    combined_screening_distance_v1,
    waveform_distance_v1,
    damage_signature_distance,
    damage_evidence_class,
    truth_label,
    truth_label_basis,
    truth_validation_status,
    energy_v2_blockers,
    energy_v2_notes
FROM ranked
WHERE combined_rank <= 10;

CREATE INDEX idx_stress_proxy_candidate_combined_v3_target_rank
    ON stress_proxy_candidate_combined_v3(target_stress_record_key, combined_rank);
CREATE INDEX idx_stress_proxy_candidate_combined_v3_rank
    ON stress_proxy_candidate_combined_v3(combined_rank);
CREATE INDEX idx_stress_proxy_candidate_combined_v3_device
    ON stress_proxy_candidate_combined_v3(device_type);
CREATE INDEX idx_stress_proxy_candidate_combined_v3_status
    ON stress_proxy_candidate_combined_v3(proxy_claim_status);
CREATE INDEX idx_stress_proxy_candidate_combined_v3_match_scope
    ON stress_proxy_candidate_combined_v3(match_scope);
