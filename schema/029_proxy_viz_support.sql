-- Proxy visualization support views for dashboard/viewer concordance.
-- apply_schema: pipeline-owned

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
