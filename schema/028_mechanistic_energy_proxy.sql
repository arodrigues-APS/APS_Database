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

DROP VIEW IF EXISTS stress_proxy_candidate_energy_v2 CASCADE;
DROP VIEW IF EXISTS stress_energy_equivalence_features CASCADE;

CREATE TABLE IF NOT EXISTS stress_energy_equivalence_settings (
    setting_name                          text PRIMARY KEY,
    description                           text,
    default_track_core_radius_um          double precision NOT NULL,
    track_core_radius_low_um              double precision NOT NULL,
    track_core_radius_high_um             double precision NOT NULL,
    collapse_hard_threshold               double precision NOT NULL,
    terminal_energy_log_sigma_integrated  double precision NOT NULL,
    terminal_energy_log_sigma_commanded   double precision NOT NULL,
    terminal_energy_log_sigma_censored    double precision NOT NULL,
    active_area_log_sigma_measured        double precision NOT NULL,
    active_area_log_sigma_estimated       double precision NOT NULL,
    doping_log_sigma_measured             double precision NOT NULL,
    doping_log_sigma_estimated            double precision NOT NULL,
    geometry_confidence_measured_min      double precision NOT NULL,
    same_regime_required_for_primary      boolean NOT NULL DEFAULT FALSE
);

-- First-pass screening assumptions (NOT fitted constants — the measured truth
-- set is far too small to fit).  Mirrors EnergyEquivalenceSettings defaults.
INSERT INTO stress_energy_equivalence_settings (
    setting_name, description,
    default_track_core_radius_um, track_core_radius_low_um, track_core_radius_high_um,
    collapse_hard_threshold,
    terminal_energy_log_sigma_integrated, terminal_energy_log_sigma_commanded,
    terminal_energy_log_sigma_censored,
    active_area_log_sigma_measured, active_area_log_sigma_estimated,
    doping_log_sigma_measured, doping_log_sigma_estimated,
    geometry_confidence_measured_min, same_regime_required_for_primary
)
VALUES (
    'default',
    'Phase 1 mechanistic energy-proxy screening assumptions; see rollout plan open questions.',
    0.1, 0.05, 0.5,
    0.5,
    0.20, 0.41, 0.69,
    0.20, 0.69,
    0.20, 0.47,
    0.5, FALSE
)
ON CONFLICT (setting_name) DO UPDATE SET
    description = EXCLUDED.description,
    default_track_core_radius_um = EXCLUDED.default_track_core_radius_um,
    track_core_radius_low_um = EXCLUDED.track_core_radius_low_um,
    track_core_radius_high_um = EXCLUDED.track_core_radius_high_um,
    collapse_hard_threshold = EXCLUDED.collapse_hard_threshold,
    terminal_energy_log_sigma_integrated = EXCLUDED.terminal_energy_log_sigma_integrated,
    terminal_energy_log_sigma_commanded = EXCLUDED.terminal_energy_log_sigma_commanded,
    terminal_energy_log_sigma_censored = EXCLUDED.terminal_energy_log_sigma_censored,
    active_area_log_sigma_measured = EXCLUDED.active_area_log_sigma_measured,
    active_area_log_sigma_estimated = EXCLUDED.active_area_log_sigma_estimated,
    doping_log_sigma_measured = EXCLUDED.doping_log_sigma_measured,
    doping_log_sigma_estimated = EXCLUDED.doping_log_sigma_estimated,
    geometry_confidence_measured_min = EXCLUDED.geometry_confidence_measured_min,
    same_regime_required_for_primary = EXCLUDED.same_regime_required_for_primary;


-- Phase 2: regime-compatibility priors (consumed by no ranker yet).  The
-- regime-granular successor to stress_mechanism_compatibility, keyed on
-- *measured* regime, not the event-type label.  Mirrors _REGIME_COMPATIBILITY
-- in data_processing_scripts/mechanistic_energy_proxy.py.  A candidate_regime
-- of 'any' is the per-target fallback.  Lower preference ranks first.
CREATE TABLE IF NOT EXISTS stress_regime_compatibility (
    target_regime     text NOT NULL,
    candidate_regime  text NOT NULL,
    match_class       text NOT NULL,
    status_ceiling    text,
    preference        integer NOT NULL,
    rationale         text,
    PRIMARY KEY (target_regime, candidate_regime)
);

INSERT INTO stress_regime_compatibility
    (target_regime, candidate_regime, match_class, status_ceiling, preference, rationale)
VALUES
    ('heavy_ion_hard_collapse_seb', 'avalanche_hard_collapse', 'first_order_analog', NULL, 1,
     'Hard-collapse heavy-ion SEB matches inductive avalanche field-collapse burnout.'),
    ('heavy_ion_hard_collapse_seb', 'sc_high_power_short_pulse', 'secondary_analog', NULL, 2,
     'Short-circuit high-power pulse shares thermal runaway with a less direct topology.'),
    ('heavy_ion_hard_collapse_seb', 'repetitive_avalanche_cumulative', 'secondary_analog', NULL, 2,
     'Repetitive avalanche reaches similar collapse but is a multi-pulse stimulus.'),
    ('heavy_ion_hard_collapse_seb', 'sc_low_collapse', 'mechanism_mismatch', 'analog_questionable', 4,
     'Low-collapse SC does not match a hard-collapse heavy-ion SEB.'),
    ('heavy_ion_hard_collapse_seb', 'any', 'analog_questionable', 'analog_questionable', 3,
     'No collapse-matched electrical analog seeded for this heavy-ion SEB.'),

    ('proton_low_collapse_seb', 'sc_low_collapse', 'first_order_analog', NULL, 1,
     'Low-collapse proton SEB matches short-circuit low-collapse stress (proton diagnostic).'),
    ('proton_low_collapse_seb', 'sc_high_power_short_pulse', 'secondary_analog', NULL, 2,
     'Short-circuit candidate; collapse is higher than the near-zero proton SEB target.'),
    ('proton_low_collapse_seb', 'avalanche_hard_collapse', 'mechanism_mismatch', 'analog_questionable', 4,
     'Avalanche hard collapse does not match near-zero proton SEB collapse.'),
    ('proton_low_collapse_seb', 'any', 'analog_questionable', 'analog_questionable', 3,
     'Weak analog for low-collapse proton SEB.'),

    ('proton_high_field_seb', 'avalanche_hard_collapse', 'secondary_analog', NULL, 2,
     'High-field proton SEB with collapse; avalanche is a partial field-collapse analog.'),
    ('proton_high_field_seb', 'sc_high_power_short_pulse', 'secondary_analog', NULL, 2,
     'Short-circuit high-power pulse is a partial analog for high-field proton SEB.'),
    ('proton_high_field_seb', 'any', 'analog_questionable', 'analog_questionable', 3,
     'Inspect high-field proton SEB manually.'),

    -- SELC-I is gate-oxide leakage: short-circuit stresses the gate, avalanche
    -- (drain-source UIS) does not, so avalanche is a mechanism mismatch here
    -- regardless of repetition.  Mirrors _REGIME_COMPATIBILITY in the Python module.
    ('selci_gate_coupled', 'repetitive_sc_cumulative', 'cumulative_analog', 'analog_questionable', 1,
     'Repetitive SC gate stress is the gate-coupled, cumulative analog for SELC-I leakage.'),
    ('selci_gate_coupled', 'sc_high_power_short_pulse', 'gate_coupled_analog', 'analog_questionable', 1,
     'Short-circuit stresses the gate oxide implicated in SELC-I leakage.'),
    ('selci_gate_coupled', 'sc_low_collapse', 'gate_coupled_analog', 'analog_questionable', 1,
     'Short-circuit stresses the gate oxide implicated in SELC-I leakage.'),
    ('selci_gate_coupled', 'repetitive_avalanche_cumulative', 'mechanism_mismatch', 'analog_questionable', 4,
     'Repetitive avalanche is a drain-source stress with no gate-oxide coupling for SELC-I.'),
    ('selci_gate_coupled', 'avalanche_hard_collapse', 'mechanism_mismatch', 'analog_questionable', 4,
     'Avalanche (drain-source UIS) does not stress the gate oxide implicated in SELC-I.'),
    ('selci_gate_coupled', 'avalanche_noncatastrophic', 'mechanism_mismatch', 'analog_questionable', 4,
     'Avalanche (drain-source UIS) does not stress the gate oxide implicated in SELC-I.'),
    ('selci_gate_coupled', 'any', 'analog_questionable', 'analog_questionable', 3,
     'SELC-I needs gate-coupled (short-circuit) evidence before any strong status.'),

    ('selcii_drain_source_cumulative', 'repetitive_sc_cumulative', 'cumulative_analog', 'analog_questionable', 2,
     'Cumulative drain-source leakage weakly tracked by repetitive electrical overstress.'),
    ('selcii_drain_source_cumulative', 'repetitive_avalanche_cumulative', 'cumulative_analog', 'analog_questionable', 2,
     'Cumulative drain-source leakage weakly tracked by repetitive avalanche overstress.'),
    ('selcii_drain_source_cumulative', 'any', 'analog_questionable', 'analog_questionable', 3,
     'SELC-II is cumulative defect leakage without a strong single-pulse analog.')
ON CONFLICT (target_regime, candidate_regime) DO UPDATE SET
    match_class = EXCLUDED.match_class,
    status_ceiling = EXCLUDED.status_ceiling,
    preference = EXCLUDED.preference,
    rationale = EXCLUDED.rationale;


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
    CHECK (label_basis IN ('measured_post_iv', 'expert', 'pilot'))
);

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


CREATE VIEW stress_energy_equivalence_features AS
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
        (LOWER(TRIM(COALESCE(s.ion_species, ''))) IN
            ('p', 'proton', 'protons', 'h', 'h+', 'h1', '1h')
         OR LOWER(COALESCE(s.ion_species, '')) LIKE '%proton%') AS is_proton,
        (s.vds_collapse_fraction IS NOT NULL
         AND s.vds_collapse_fraction >= cfg.collapse_hard_threshold) AS collapse_high,
        (s.pulse_count_in_sequence IS NOT NULL
         AND s.pulse_count_in_sequence > 1) AS cumulative,
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
        -- Measured-regime label (mirrors classify_mechanistic_regime).
        CASE
            WHEN b.source = 'irradiation' THEN
                CASE
                    WHEN UPPER(COALESCE(b.event_type, '')) = 'SEB' THEN
                        CASE
                            WHEN b.is_proton AND b.collapse_high THEN 'proton_high_field_seb'
                            WHEN b.is_proton THEN 'proton_low_collapse_seb'
                            WHEN b.collapse_high THEN 'heavy_ion_hard_collapse_seb'
                            ELSE 'unknown_single_event'
                        END
                    WHEN UPPER(COALESCE(b.event_type, '')) = 'SELCI' THEN 'selci_gate_coupled'
                    WHEN UPPER(COALESCE(b.event_type, '')) = 'SELCII' THEN 'selcii_drain_source_cumulative'
                    WHEN UPPER(COALESCE(b.event_type, '')) = 'MIXED' THEN 'mixed_single_event'
                    WHEN b.is_proton THEN 'tid_dd_cumulative'
                    ELSE 'unknown_single_event'
                END
            WHEN b.source = 'avalanche' THEN
                CASE
                    WHEN b.cumulative THEN 'repetitive_avalanche_cumulative'
                    WHEN b.collapse_high OR COALESCE(b.is_catastrophic, FALSE)
                        THEN 'avalanche_hard_collapse'
                    ELSE 'avalanche_noncatastrophic'
                END
            WHEN b.source = 'sc' THEN
                CASE
                    WHEN b.cumulative THEN 'repetitive_sc_cumulative'
                    WHEN b.collapse_high THEN 'sc_high_power_short_pulse'
                    ELSE 'sc_low_collapse'
                END
            ELSE 'unknown_electrical_proxy'
        END AS mechanistic_regime,
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


CREATE VIEW stress_proxy_candidate_energy_v2 AS
WITH paired AS (
    SELECT
        v1.target_stress_record_key,
        v1.candidate_stress_record_key,
        'irradiation'::text AS target_source,
        v1.candidate_source,
        v1.device_type,
        v1.target_event_type,
        v1.target_ion_species,
        v1.match_scope,
        v1.candidate_rank   AS candidate_rank_v1,
        v1.candidate_status AS candidate_status_v1,
        v1.waveform_distance AS waveform_distance_v1,
        v1.combined_screening_distance AS combined_screening_distance_v1,
        v1.best_damage_distance,
        v1.damage_evidence_tier,
        v1.measured_comparability_status,
        v1.prediction_comparability_status,
        v1.log_energy_delta,
        tf.mechanistic_regime AS target_mechanistic_regime,
        cf.mechanistic_regime AS candidate_mechanistic_regime,
        tf.electrical_terminal_energy_j AS target_terminal_energy_j,
        tf.se_depletion_ratio_to_seb,
        tf.track_core_energy_density_j_cm3,
        tf.effective_stress_time_s AS target_effective_stress_time_s,
        tf.feature_blockers AS target_feature_blockers,
        cf.terminal_energy_density_bulk_j_cm3 AS candidate_bulk_energy_density_j_cm3,
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
        -- Severity axis matched to the target threshold (SEB vs SELC), in log
        -- space because the ratios span orders of magnitude.
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN tf.se_depletion_ratio_lower_to_selc
             ELSE tf.se_depletion_ratio_lower_to_seb END AS target_severity_low,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN tf.se_depletion_ratio_upper_to_selc
             ELSE tf.se_depletion_ratio_upper_to_seb END AS target_severity_high,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN cf.terminal_ratio_to_selc_lower
             ELSE cf.terminal_ratio_to_seb_lower END AS candidate_severity_low,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN cf.terminal_ratio_to_selc_upper
             ELSE cf.terminal_ratio_to_seb_upper END AS candidate_severity_high,
        -- Axis-matched nominal (point) ratios for the band chart.  Same
        -- target/candidate separation as the bands: target uses its stored
        -- depletion ratio, candidate uses its bulk terminal-critical ratio.
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN tf.se_depletion_ratio_to_selc
             ELSE tf.se_depletion_ratio_to_seb END AS target_severity_point_ratio,
        CASE WHEN UPPER(COALESCE(v1.target_event_type, '')) IN ('SELCI', 'SELCII')
             THEN cf.terminal_ratio_to_selc_critical
             ELSE cf.terminal_ratio_to_seb_critical END AS candidate_severity_point_ratio
    FROM stress_proxy_candidate_ranked_view v1
    LEFT JOIN stress_energy_equivalence_features tf
        ON tf.stress_record_key = v1.target_stress_record_key
    LEFT JOIN stress_energy_equivalence_features cf
        ON cf.stress_record_key = v1.candidate_stress_record_key
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
        p.*,
        COALESCE(p.regime_match_class, 'analog_questionable') AS regime_match_class_final,
        COALESCE(p.regime_status_ceiling, 'analog_questionable') AS regime_status_ceiling_final,
        COALESCE(p.regime_preference, 3) AS regime_preference_final,
        -- Per-axis overlap descriptors (visible, not a blended score).
        mech_overlap_class(
            LN(NULLIF(p.target_severity_low, 0.0)),
            LN(NULLIF(p.target_severity_high, 0.0)),
            LN(NULLIF(p.candidate_severity_low, 0.0)),
            LN(NULLIF(p.candidate_severity_high, 0.0))
        ) AS critical_severity_overlap_class,
        CASE
            WHEN p.log_energy_delta IS NULL THEN 'missing_interval'
            WHEN ABS(p.log_energy_delta) <= 0.5 THEN 'strong_overlap'
            WHEN ABS(p.log_energy_delta) <= 1.5 THEN 'partial_overlap'
            WHEN ABS(p.log_energy_delta) <= 3.0 THEN 'near_miss'
            ELSE 'far_miss'
        END AS terminal_energy_overlap_class,
        CASE
            WHEN p.candidate_bulk_energy_density_j_cm3 IS NULL
              OR p.candidate_bulk_energy_density_j_cm3 <= 0.0
              OR p.track_core_energy_density_j_cm3 IS NULL
              OR p.track_core_energy_density_j_cm3 <= 0.0 THEN NULL
            ELSE LOG(p.candidate_bulk_energy_density_j_cm3)
               - LOG(p.track_core_energy_density_j_cm3)
        END AS localization_mismatch_log10,
        CASE
            WHEN p.target_effective_stress_time_s IS NOT NULL
             AND p.target_effective_stress_time_s > 0.0
             AND p.candidate_effective_stress_time_s IS NOT NULL
             AND p.candidate_effective_stress_time_s > 0.0 THEN
                CASE
                    WHEN ABS(LN(p.candidate_effective_stress_time_s)
                             - LN(p.target_effective_stress_time_s)) <= 1.0 THEN 'strong_overlap'
                    WHEN ABS(LN(p.candidate_effective_stress_time_s)
                             - LN(p.target_effective_stress_time_s)) <= 3.0 THEN 'partial_overlap'
                    WHEN ABS(LN(p.candidate_effective_stress_time_s)
                             - LN(p.target_effective_stress_time_s)) <= 5.0 THEN 'near_miss'
                    ELSE 'far_miss'
                END
            ELSE 'missing_interval'
        END AS power_rate_overlap_class,
        CASE
            WHEN p.target_mechanistic_regime IN (
                    'selci_gate_coupled', 'selcii_drain_source_cumulative', 'tid_dd_cumulative')
                THEN CASE WHEN COALESCE(p.candidate_pulse_count_in_sequence, 0) > 1
                          THEN 'cumulative_present' ELSE 'cumulative_missing' END
            ELSE 'not_applicable'
        END AS cumulative_exposure_overlap_class,
        p.damage_evidence_tier AS damage_evidence_class,
        (p.target_mechanistic_regime IN (
            'selci_gate_coupled', 'selcii_drain_source_cumulative', 'tid_dd_cumulative')
         AND COALESCE(p.candidate_pulse_count_in_sequence, 0) > 1) AS cumulative_pair
    FROM paired p
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
            WHEN c.waveform_distance_v1 IS NOT NULL
                THEN 'mechanistic_waveform_candidate'
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
            WHEN 'mechanistic_waveform_candidate' THEN 4
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
                'mechanistic_waveform_candidate', 'mechanistic_analog_questionable')
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
            CASE WHEN s.critical_severity_overlap_class = 'far_miss' THEN 'severity_intervals_far_miss' END,
            CASE WHEN s.cumulative_exposure_overlap_class = 'cumulative_missing'
                 THEN 'candidate_missing_cumulative_energy' END,
            CASE WHEN NOT s.target_has_energy_context THEN 'target_missing_energy_context' END
        ], NULL)::text[]
        || COALESCE(s.target_feature_blockers, ARRAY[]::text[])
        || COALESCE(s.candidate_feature_blockers, ARRAY[]::text[]) AS energy_v2_blockers,
        ARRAY_REMOVE(ARRAY[
            'critical_severity_is_screening_descriptor_only',
            CASE WHEN s.localization_mismatch_log10 IS NOT NULL THEN
                'localization_mismatch_' ||
                CASE
                    WHEN ABS(s.localization_mismatch_log10) > 4.0 THEN 'extreme_localized_vs_bulk'
                    WHEN ABS(s.localization_mismatch_log10) > 2.0 THEN 'large_localized_vs_bulk'
                    WHEN ABS(s.localization_mismatch_log10) > 1.0 THEN 'moderate_localized_vs_bulk'
                    ELSE 'comparable'
                END
            END,
            'power_rate_basis_timescale_proxy',
            CASE WHEN s.regime_rationale IS NOT NULL THEN 'regime: ' || s.regime_rationale END
        ], NULL)::text[] AS energy_v2_notes
    FROM statused s
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
                f.regime_preference_final,
                CASE f.critical_severity_overlap_class
                    WHEN 'strong_overlap' THEN 0
                    WHEN 'partial_overlap' THEN 1
                    WHEN 'near_miss' THEN 2
                    WHEN 'far_miss' THEN 3
                    ELSE 4
                END,
                f.best_damage_distance ASC NULLS LAST,
                CASE f.terminal_energy_overlap_class
                    WHEN 'strong_overlap' THEN 0
                    WHEN 'partial_overlap' THEN 1
                    WHEN 'near_miss' THEN 2
                    WHEN 'far_miss' THEN 3
                    ELSE 4
                END,
                f.candidate_rank_v1 ASC NULLS LAST,
                f.candidate_stress_record_key
        ) AS mechanistic_energy_candidate_rank
    FROM finalized f
)
SELECT
    target_stress_record_key,
    candidate_stress_record_key,
    target_source,
    candidate_source,
    device_type,
    target_event_type,
    target_ion_species,
    match_scope,
    candidate_rank_v1,
    candidate_status_v1,
    waveform_distance_v1,
    combined_screening_distance_v1,
    target_mechanistic_regime,
    candidate_mechanistic_regime,
    regime_match_class_final AS regime_match_class,
    regime_status_ceiling_final AS regime_status_ceiling,
    regime_preference_final AS regime_preference,
    regime_rationale,
    terminal_energy_overlap_class,
    critical_severity_overlap_class,
    localization_mismatch_log10,
    power_rate_overlap_class,
    cumulative_exposure_overlap_class,
    damage_evidence_class,
    -- Severity bands + axis-matched point ratios for the Phase-5 interval chart.
    -- Target vs candidate stay in separate columns (separation invariant #1):
    -- target_* are stored depletion ratios, candidate_* are bulk terminal ratios.
    target_severity_low,
    target_severity_high,
    target_severity_point_ratio,
    candidate_severity_low,
    candidate_severity_high,
    candidate_severity_point_ratio,
    mechanistic_energy_screening_bucket,
    mechanistic_energy_candidate_status,
    mechanistic_energy_status_priority,
    mechanistic_energy_candidate_rank,
    energy_v2_blockers,
    energy_v2_notes
FROM ranked2
WHERE mechanistic_energy_candidate_rank <= 10;
