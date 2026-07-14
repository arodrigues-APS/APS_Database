-- Proxy visualization support views for dashboard/viewer concordance.
-- apply_schema: pipeline-owned

DROP MATERIALIZED VIEW IF EXISTS stress_proxy_method_comparison_union_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_proxy_concordance_enrichment_view CASCADE;

CREATE MATERIALIZED VIEW stress_proxy_concordance_enrichment_view AS
WITH ranked AS (
    SELECT
        r.target_stress_record_key,
        r.candidate_stress_record_key,
        r.device_type,
        r.target_event_type,
        r.target_ion_species,
        r.candidate_source,
        r.match_scope,
        r.proxy_claim_status AS v1_proxy_claim_status,
        r.proxy_claim_basis AS v1_proxy_claim_basis,
        r.proxy_claim_blockers AS v1_proxy_claim_blockers,
        r.proxy_claim_summary AS v1_proxy_claim_summary,
        r.signature_claim_quality AS v1_signature_claim_quality,
        r.signature_axis_distance,
        r.energy_blended_control_distance,
        r.candidate_rank AS waveform_rank,
        ROW_NUMBER() OVER (
            PARTITION BY r.target_stress_record_key
            ORDER BY r.signature_axis_distance ASC NULLS LAST,
                     r.candidate_stress_record_key
        ) AS dssig_rank,
        COUNT(*) FILTER (WHERE r.signature_axis_distance IS NOT NULL) OVER (
            PARTITION BY r.target_stress_record_key
        ) AS dssig_pool_size,
        ROW_NUMBER() OVER (
            PARTITION BY r.target_stress_record_key
            ORDER BY r.energy_blended_control_distance ASC NULLS LAST,
                     r.candidate_stress_record_key
        ) AS energy_blended_rank
    FROM stress_proxy_candidate_ranked_view r
), v2_rank1 AS (
    SELECT *
    FROM stress_proxy_candidate_energy_v2
    WHERE mechanistic_energy_candidate_rank = 1
), picked AS (
    SELECT
        v2.target_stress_record_key,
        v2.device_type,
        v2.target_event_type,
        v2.target_ion_species,
        v2.candidate_stress_record_key AS v2_pick_key,
        v2.candidate_source AS v2_pick_source,
        v2.match_scope AS v2_match_scope,
        v2.mechanistic_energy_candidate_status AS v2_candidate_status,
        v2.proxy_claim_status AS v2_proxy_claim_status,
        v2.proxy_claim_basis AS v2_proxy_claim_basis,
        v2.proxy_claim_blockers AS v2_proxy_claim_blockers,
        v2.proxy_claim_summary AS v2_proxy_claim_summary,
        v2.truth_validation_status,
        v2.truth_label,
        v2.truth_label_basis,
        v2.candidate_failure_fraction_overlap_class,
        v2.terminal_energy_overlap_class,
        rv2.signature_axis_distance AS v2_pick_signature_axis_distance,
        CASE WHEN rv2.signature_axis_distance IS NOT NULL THEN rv2.dssig_rank END
            AS v2_pick_dssig_rank,
        rv2.dssig_pool_size,
        CASE
            WHEN rv2.signature_axis_distance IS NOT NULL
             AND rv2.dssig_pool_size > 0
                THEN 100.0 * rv2.dssig_rank::double precision / rv2.dssig_pool_size
        END AS v2_pick_dssig_percentile,
        CASE
            WHEN rv2.signature_axis_distance IS NOT NULL
             AND rv2.dssig_pool_size > 0
                THEN rv2.dssig_rank::double precision / rv2.dssig_pool_size
        END AS v2_pick_dssig_percentile_fraction,
        CASE
            WHEN rv2.signature_axis_distance IS NOT NULL
             AND rv2.dssig_pool_size > 0
                THEN LEAST(10, WIDTH_BUCKET(
                    100.0 * rv2.dssig_rank::double precision / rv2.dssig_pool_size,
                    0.0, 100.0, 10
                ))
        END AS v2_pick_dssig_decile,
        rv1.candidate_stress_record_key AS v1_signature_pick_key,
        rv1.candidate_source AS v1_signature_pick_source,
        rv1.match_scope AS v1_signature_match_scope,
        rv1.v1_proxy_claim_status AS v1_signature_proxy_claim_status,
        rv1.v1_proxy_claim_basis AS v1_signature_proxy_claim_basis,
        rv1.v1_proxy_claim_blockers AS v1_signature_proxy_claim_blockers,
        rv1.v1_proxy_claim_summary AS v1_signature_proxy_claim_summary,
        rv1.v1_signature_claim_quality,
        rrank.candidate_stress_record_key AS v1_waveform_pick_key,
        eblend.candidate_stress_record_key AS energy_blended_pick_key
    FROM v2_rank1 v2
    LEFT JOIN ranked rv2
        ON rv2.target_stress_record_key = v2.target_stress_record_key
       AND rv2.candidate_stress_record_key = v2.candidate_stress_record_key
    LEFT JOIN ranked rv1
        ON rv1.target_stress_record_key = v2.target_stress_record_key
       AND rv1.dssig_rank = 1
    LEFT JOIN ranked rrank
        ON rrank.target_stress_record_key = v2.target_stress_record_key
       AND rrank.waveform_rank = 1
    LEFT JOIN ranked eblend
        ON eblend.target_stress_record_key = v2.target_stress_record_key
       AND eblend.energy_blended_rank = 1
)
SELECT
    *,
    (v2_pick_key = v1_waveform_pick_key) AS strict_waveform_rank1_agreement,
    (v2_pick_key = v1_signature_pick_key) AS prior_free_signature_rank1_agreement,
    (v2_pick_key = energy_blended_pick_key) AS energy_blended_control_agreement,
    (v2_pick_source IS DISTINCT FROM v1_signature_pick_source) AS source_conflict,
    (
        device_type = 'C2M0080120D'
        AND v2_match_scope = 'same_device'
        AND v2_pick_source = 'avalanche'
        AND v1_signature_pick_source = 'sc'
    ) AS c2m0080120d_avalanche_vs_sc_conflict,
    CASE
        WHEN v2_match_scope = 'same_device'
         AND v2_pick_source IS DISTINCT FROM v1_signature_pick_source
            THEN 0
        WHEN v2_pick_source IS DISTINCT FROM v1_signature_pick_source
            THEN 1
        ELSE 2
    END AS conflict_priority,
    (
        v2_match_scope = 'same_device'
        AND v2_pick_source IS DISTINCT FROM v1_signature_pick_source
    ) AS same_device_source_conflict,
    CASE
        WHEN v2_pick_dssig_percentile IS NULL THEN 'missing_signature_rank'
        WHEN v2_pick_dssig_percentile <= 10.0 THEN 'best_decile'
        WHEN v2_pick_dssig_percentile <= 25.0 THEN 'best_quartile'
        WHEN v2_pick_dssig_percentile <= 50.0 THEN 'top_half'
        ELSE 'bottom_half'
    END AS enrichment_band
FROM picked;

CREATE INDEX idx_proxy_concordance_enrichment_target
    ON stress_proxy_concordance_enrichment_view(target_stress_record_key);
CREATE INDEX idx_proxy_concordance_enrichment_device
    ON stress_proxy_concordance_enrichment_view(device_type);
CREATE INDEX idx_proxy_concordance_enrichment_scope
    ON stress_proxy_concordance_enrichment_view(v2_match_scope);
CREATE INDEX idx_proxy_concordance_enrichment_conflict
    ON stress_proxy_concordance_enrichment_view(
        c2m0080120d_avalanche_vs_sc_conflict,
        source_conflict,
        conflict_priority
    );

-- Complete winner-union contract for the interactive v1 -> v2 -> v3
-- comparator.  Unlike the old concordance export, this surface always carries
-- the official winner from every method.  A v1 winner that falls outside v2's
-- materialized top-10 is retained with an explicit unavailable rank reason;
-- it is never misclassified as "matches neither".
CREATE MATERIALIZED VIEW stress_proxy_method_comparison_union_view AS
WITH v1_pool AS (
    SELECT
        target_stress_record_key,
        COUNT(*) AS v1_pool_size,
        COUNT(*) FILTER (WHERE energy_rankable) AS v2_pool_size
    FROM stress_proxy_candidate_ranked_view
    GROUP BY target_stress_record_key
),
v3_pool AS (
    SELECT target_stress_record_key, COUNT(*) AS v3_official_pool_size
    FROM stress_proxy_candidate_energy_v2
    GROUP BY target_stress_record_key
),
v1_winner AS (
    SELECT *
    FROM stress_proxy_candidate_ranked_view
    WHERE candidate_rank = 1
),
v2_winner AS (
    SELECT *
    FROM stress_proxy_candidate_energy_v2
    WHERE energy_rank = 1
),
v3_winner AS (
    SELECT *
    FROM stress_proxy_candidate_combined_v3
    WHERE combined_rank = 1
),
v3_runner_up AS (
    SELECT target_stress_record_key, candidate_stress_record_key,
           combined_vector_distance
    FROM stress_proxy_candidate_combined_v3
    WHERE combined_rank = 2
),
winner_keys AS (
    SELECT target_stress_record_key, candidate_stress_record_key,
           TRUE AS picked_by_v1, FALSE AS picked_by_v2, FALSE AS picked_by_v3
    FROM v1_winner
    UNION ALL
    SELECT target_stress_record_key, candidate_stress_record_key,
           FALSE, TRUE, FALSE
    FROM v2_winner
    UNION ALL
    SELECT target_stress_record_key, candidate_stress_record_key,
           FALSE, FALSE, TRUE
    FROM v3_winner
),
winner_union AS (
    SELECT
        target_stress_record_key,
        candidate_stress_record_key,
        BOOL_OR(picked_by_v1) AS picked_by_v1,
        BOOL_OR(picked_by_v2) AS picked_by_v2,
        BOOL_OR(picked_by_v3) AS picked_by_v3
    FROM winner_keys
    GROUP BY target_stress_record_key, candidate_stress_record_key
),
comparison_base AS (
    SELECT
        u.target_stress_record_key,
        u.candidate_stress_record_key,
        r.device_type,
        r.target_device_label,
        r.target_event_type,
        r.target_ion_species,
        r.target_filename,
        r.candidate_source,
        r.candidate_device_type,
        r.candidate_device_label,
        r.candidate_filename,
        r.match_scope,
        u.picked_by_v1,
        u.picked_by_v2,
        u.picked_by_v3,
        (u.picked_by_v1::integer + u.picked_by_v2::integer
            + u.picked_by_v3::integer) AS picked_by_method_count,
        v1w.candidate_stress_record_key AS v1_winner_key,
        v2w.candidate_stress_record_key AS v2_winner_key,
        v3w.candidate_stress_record_key AS v3_winner_key,
        CASE
            WHEN v1w.candidate_stress_record_key IS NULL THEN 'v1_winner_unavailable'
            WHEN v2w.candidate_stress_record_key IS NULL THEN 'v2_winner_unavailable'
            WHEN v3w.candidate_stress_record_key IS NULL THEN 'v3_winner_unavailable'
            WHEN v1w.candidate_stress_record_key = v2w.candidate_stress_record_key
             AND v2w.candidate_stress_record_key = v3w.candidate_stress_record_key
                THEN 'all_three_same'
            WHEN v3w.candidate_stress_record_key = v1w.candidate_stress_record_key
             AND v3w.candidate_stress_record_key IS DISTINCT FROM v2w.candidate_stress_record_key
                THEN 'v3_follows_v1'
            WHEN v3w.candidate_stress_record_key = v2w.candidate_stress_record_key
             AND v3w.candidate_stress_record_key IS DISTINCT FROM v1w.candidate_stress_record_key
                THEN 'v3_follows_v2'
            ELSE 'v3_selects_third_candidate'
        END AS transition_class,
        CASE WHEN v1w.candidate_stress_record_key = v2w.candidate_stress_record_key
             THEN 'same_winner' ELSE 'changed_winner' END AS v1_to_v2_transition,
        CASE WHEN v2w.candidate_stress_record_key = v3w.candidate_stress_record_key
             THEN 'same_winner' ELSE 'changed_winner' END AS v2_to_v3_transition,

        -- v1: production lexicographic rank over the complete uncapped pool.
        r.candidate_rank AS v1_rank,
        p.v1_pool_size,
        100.0 * r.candidate_rank::double precision / NULLIF(p.v1_pool_size, 0)
            AS v1_rank_percentile,
        TRUE AS v1_eligible,
        TRUE AS v1_rank_available,
        NULL::text AS v1_rank_unavailable_reason,
        'production_lexicographic_status_mask_signature'::text AS v1_rank_basis,
        r.candidate_status AS v1_candidate_status,
        r.candidate_status_priority AS v1_candidate_status_priority,
        r.damage_signature_axis_mask AS v1_signature_axis_mask,
        r.damage_signature_mask_rank AS v1_signature_mask_rank,
        r.damage_signature_axes_used AS v1_signature_axes_used,
        r.signature_axis_distance AS v1_signature_axis_distance,
        r.duration_log_delta AS v1_duration_log_delta,
        r.best_damage_distance AS v1_post_iv_damage_distance,
        r.damage_evidence_tier AS v1_damage_evidence_tier,
        r.signature_claim_quality AS v1_signature_claim_quality,
        r.proxy_claim_status AS v1_proxy_claim_status,
        r.proxy_claim_basis AS v1_proxy_claim_basis,
        r.proxy_claim_blockers AS v1_proxy_claim_blockers,
        CASE
            WHEN u.picked_by_v1 THEN 'winner'
            WHEN r.match_scope IS DISTINCT FROM v1w.match_scope THEN 'match_scope'
            WHEN r.candidate_status_priority IS DISTINCT FROM v1w.candidate_status_priority
                THEN 'candidate_status_priority'
            WHEN r.damage_signature_mask_rank IS DISTINCT FROM v1w.damage_signature_mask_rank
                THEN 'within_evidence_mask_signature_rank'
            WHEN r.damage_signature_axes_used IS DISTINCT FROM v1w.damage_signature_axes_used
                THEN 'signature_axis_coverage'
            ELSE 'source_or_candidate_identity_tiebreaker'
        END AS v1_first_losing_criterion,

        -- v2: the official rank is available only on the materialized top-10.
        v2.energy_rank AS v2_rank,
        p.v2_pool_size,
        CASE WHEN v2.energy_rank IS NOT NULL
             THEN 100.0 * v2.energy_rank::double precision / NULLIF(p.v2_pool_size, 0)
        END AS v2_rank_percentile,
        COALESCE(r.energy_rankable, FALSE) AS v2_eligible,
        (v2.energy_rank IS NOT NULL) AS v2_rank_available,
        CASE
            WHEN NOT COALESCE(r.energy_rankable, FALSE) THEN 'not_energy_rankable'
            WHEN v2.energy_rank IS NULL THEN 'outside_materialized_v2_top10'
        END AS v2_rank_unavailable_reason,
        'mechanistic_energy_lexicographic_full_pool_top10_materialized'::text
            AS v2_rank_basis,
        v2.mechanistic_energy_candidate_status AS v2_candidate_status,
        v2.mechanistic_energy_status_priority AS v2_candidate_status_priority,
        v2.regime_match_class AS v2_regime_match_class,
        v2.candidate_failure_fraction_overlap_class AS v2_failure_overlap_class,
        v2.terminal_energy_overlap_class AS v2_terminal_overlap_class,
        v2.candidate_failure_fraction_point AS v2_failure_fraction_point,
        v2.target_severity_point_ratio AS v2_target_severity_point,
        v2.log_energy_delta AS v2_log_energy_delta,
        v2.log_energy_delta_dex AS v2_log_energy_delta_dex,
        v2.cumulative_exposure_overlap_class AS v2_cumulative_overlap_class,
        v2.candidate_failure_fraction_gate_usable AS candidate_boundary_usable,
        v2.candidate_failure_fraction_basis AS candidate_boundary_basis,
        v2.proxy_claim_status AS v2_proxy_claim_status,
        v2.proxy_claim_basis AS v2_proxy_claim_basis,
        v2.proxy_claim_blockers AS v2_proxy_claim_blockers,
        CASE
            WHEN NOT COALESCE(r.energy_rankable, FALSE) THEN 'not_energy_rankable'
            WHEN v2.energy_rank IS NULL THEN 'rank_not_materialized_outside_top10'
            WHEN u.picked_by_v2 THEN 'winner'
            WHEN v2.match_scope IS DISTINCT FROM v2w.match_scope THEN 'match_scope'
            WHEN v2.mechanistic_energy_status_priority IS DISTINCT FROM
                 v2w.mechanistic_energy_status_priority THEN 'candidate_status_priority'
            WHEN v2.regime_match_class IS DISTINCT FROM v2w.regime_match_class
                THEN 'mechanistic_regime_class'
            WHEN v2.candidate_failure_fraction_overlap_class IS DISTINCT FROM
                 v2w.candidate_failure_fraction_overlap_class
                THEN 'candidate_own_boundary_overlap_class'
            WHEN v2.terminal_energy_overlap_class IS DISTINCT FROM
                 v2w.terminal_energy_overlap_class THEN 'terminal_energy_overlap_class'
            WHEN v2.cumulative_exposure_overlap_class IS DISTINCT FROM
                 v2w.cumulative_exposure_overlap_class THEN 'cumulative_exposure_class'
            ELSE 'severity_residual_or_candidate_identity_tiebreaker'
        END AS v2_first_losing_criterion,

        -- v3: official screening-only rerank of v2's materialized top-10.
        v3.combined_rank AS v3_rank,
        vp.v3_official_pool_size AS v3_pool_size,
        CASE WHEN v3.combined_rank IS NOT NULL
             THEN 100.0 * v3.combined_rank::double precision
                    / NULLIF(vp.v3_official_pool_size, 0)
        END AS v3_rank_percentile,
        (v2.energy_rank IS NOT NULL) AS v3_eligible,
        (v3.combined_rank IS NOT NULL) AS v3_rank_available,
        CASE
            WHEN v2.energy_rank IS NULL THEN 'outside_official_v2_top10_shortlist'
            WHEN v3.combined_rank IS NULL THEN 'v3_rank_not_materialized'
        END AS v3_rank_unavailable_reason,
        'official_screening_only_v2_top10_reranker'::text AS v3_rank_basis,
        CASE WHEN v3.combined_rank IS NOT NULL THEN 'official'
             ELSE 'not_scored_outside_official_shortlist' END AS v3_score_scope,
        v3.combined_vector_distance AS v3_combined_vector_distance,
        v3.combined_ranker_setting_name AS v3_ranker_setting_name,
        v3.combined_ranker_description AS v3_ranker_description,
        v3.signature_axis_weight AS v3_signature_axis_weight,
        v3.duration_weight AS v3_duration_weight,
        v3.log_energy_weight AS v3_log_energy_weight,
        v3.failure_fraction_weight AS v3_failure_fraction_weight,
        v3.post_iv_damage_weight AS v3_post_iv_damage_weight,
        v3.regime_path_weight AS v3_regime_path_weight,
        v3.coverage_gap_weight AS v3_coverage_gap_weight,
        v3.failure_fraction_component_imputed AS v3_failure_boundary_imputed,
        v3.failure_fraction_component_basis AS v3_failure_component_basis,
        v3.failure_fraction_overlap_score AS v3_failure_fraction_overlap_score,
        (v3.signature_axis_distance IS NULL) AS v3_signature_imputed,
        (v3.duration_log_delta IS NULL) AS v3_duration_imputed,
        (v3.log_energy_delta IS NULL) AS v3_terminal_energy_imputed,
        (v3.best_damage_distance IS NULL) AS v3_post_iv_imputed,
        v3.signature_axis_distance AS v3_signature_axis_distance,
        v3.duration_log_delta AS v3_duration_log_delta,
        v3.log_energy_delta AS v3_log_energy_delta,
        v3.failure_fraction_log_delta AS v3_failure_fraction_log_delta,
        v3.best_damage_distance AS v3_post_iv_damage_distance,
        v3.path_penalty AS v3_path_penalty,
        v3.damage_signature_coverage_gap AS v3_coverage_gap,
        CASE
            WHEN v3.combined_rank IS NULL THEN 'not_in_official_shortlist'
            WHEN u.picked_by_v3 THEN 'winner'
            WHEN v3.match_scope IS DISTINCT FROM v3w.match_scope THEN 'match_scope'
            WHEN v3.proxy_claim_status IS DISTINCT FROM v3w.proxy_claim_status
                THEN 'proxy_claim_status'
            WHEN v3.mechanistic_energy_status_priority IS DISTINCT FROM
                 v3w.mechanistic_energy_status_priority THEN 'candidate_status_priority'
            WHEN v3.regime_match_class IS DISTINCT FROM v3w.regime_match_class
                THEN 'mechanistic_regime_class'
            ELSE 'combined_vector_distance_or_tiebreaker'
        END AS v3_first_losing_criterion,
        v3w.combined_vector_distance AS v3_winner_distance,
        v3r2.candidate_stress_record_key AS v3_runner_up_key,
        v3r2.combined_vector_distance AS v3_runner_up_distance,
        v3r2.combined_vector_distance - v3w.combined_vector_distance
            AS v3_rank1_margin,

        tl.label AS truth_label,
        tl.label_basis AS truth_label_basis,
        tl.reviewer AS truth_reviewer,
        tl.review_date AS truth_review_date,
        tl.notes AS truth_notes,
        CASE
            WHEN tl.label = 'equivalent' AND tl.label_basis = 'measured_post_iv'
                THEN 'validated_by_curated_measured_post_iv'
            WHEN tl.label IS NOT NULL THEN 'curated_' || tl.label
            ELSE 'no_curated_truth'
        END AS truth_validation_status
    FROM winner_union u
    JOIN v1_pool p
      ON p.target_stress_record_key = u.target_stress_record_key
    JOIN stress_proxy_candidate_ranked_view r
      ON r.target_stress_record_key = u.target_stress_record_key
     AND r.candidate_stress_record_key = u.candidate_stress_record_key
    LEFT JOIN v3_pool vp
      ON vp.target_stress_record_key = u.target_stress_record_key
    LEFT JOIN v1_winner v1w
      ON v1w.target_stress_record_key = u.target_stress_record_key
    LEFT JOIN v2_winner v2w
      ON v2w.target_stress_record_key = u.target_stress_record_key
    LEFT JOIN v3_winner v3w
      ON v3w.target_stress_record_key = u.target_stress_record_key
    LEFT JOIN v3_runner_up v3r2
      ON v3r2.target_stress_record_key = u.target_stress_record_key
    LEFT JOIN stress_proxy_candidate_energy_v2 v2
      ON v2.target_stress_record_key = u.target_stress_record_key
     AND v2.candidate_stress_record_key = u.candidate_stress_record_key
    LEFT JOIN stress_proxy_candidate_combined_v3 v3
      ON v3.target_stress_record_key = u.target_stress_record_key
     AND v3.candidate_stress_record_key = u.candidate_stress_record_key
    LEFT JOIN proxy_truth_labels tl
      ON tl.target_stress_record_key = u.target_stress_record_key
     AND tl.candidate_stress_record_key = u.candidate_stress_record_key
),
component_terms AS (
    SELECT
        b.*,
        CASE WHEN b.v3_rank_available THEN
            b.v3_signature_axis_weight * POWER(COALESCE(b.v3_signature_axis_distance, 3.0), 2)
        END AS v3_signature_component_weighted_sq,
        CASE WHEN b.v3_rank_available THEN
            b.v3_duration_weight * POWER(COALESCE(b.v3_duration_log_delta, 1.0), 2)
        END AS v3_duration_component_weighted_sq,
        CASE WHEN b.v3_rank_available THEN
            b.v3_log_energy_weight * POWER(COALESCE(ABS(b.v3_log_energy_delta), 5.0), 2)
        END AS v3_log_energy_component_weighted_sq,
        CASE WHEN b.v3_rank_available THEN
            b.v3_failure_fraction_weight * POWER(
                COALESCE(
                    b.v3_failure_fraction_log_delta,
                    b.v3_failure_fraction_overlap_score
                ), 2)
        END AS v3_failure_fraction_component_weighted_sq,
        CASE WHEN b.v3_rank_available THEN
            b.v3_post_iv_damage_weight * POWER(COALESCE(b.v3_post_iv_damage_distance, 2.5), 2)
        END AS v3_post_iv_component_weighted_sq,
        CASE WHEN b.v3_rank_available THEN
            b.v3_regime_path_weight * POWER(COALESCE(b.v3_path_penalty, 0.75), 2)
        END AS v3_regime_component_weighted_sq,
        CASE WHEN b.v3_rank_available THEN
            b.v3_coverage_gap_weight * POWER(COALESCE(b.v3_coverage_gap, 1.0), 2)
        END AS v3_coverage_component_weighted_sq
    FROM comparison_base b
),
component_totals AS (
    SELECT
        c.*,
        c.v3_signature_component_weighted_sq
          + c.v3_duration_component_weighted_sq
          + c.v3_log_energy_component_weighted_sq
          + c.v3_failure_fraction_component_weighted_sq
          + c.v3_post_iv_component_weighted_sq
          + c.v3_regime_component_weighted_sq
          + c.v3_coverage_component_weighted_sq AS v3_component_weighted_sq_total
    FROM component_terms c
)
SELECT
    t.*,
    t.v3_signature_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_signature_component_share,
    t.v3_duration_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_duration_component_share,
    t.v3_log_energy_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_log_energy_component_share,
    t.v3_failure_fraction_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_failure_fraction_component_share,
    t.v3_post_iv_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_post_iv_component_share,
    t.v3_regime_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_regime_component_share,
    t.v3_coverage_component_weighted_sq / NULLIF(t.v3_component_weighted_sq_total, 0)
        AS v3_coverage_component_share
FROM component_totals t;

CREATE UNIQUE INDEX idx_proxy_method_comparison_union_pair
    ON stress_proxy_method_comparison_union_view(
        target_stress_record_key, candidate_stress_record_key
    );
CREATE INDEX idx_proxy_method_comparison_union_target
    ON stress_proxy_method_comparison_union_view(target_stress_record_key);
CREATE INDEX idx_proxy_method_comparison_union_device
    ON stress_proxy_method_comparison_union_view(device_type);
CREATE INDEX idx_proxy_method_comparison_union_transition
    ON stress_proxy_method_comparison_union_view(transition_class);
