-- V3 decision-eligible adapter for damage-equivalence and other consumers.
-- This migration is additive.  It does not alter or drop the legacy V2 views.

CREATE VIEW iv_damage_equivalence_input_view AS
SELECT
    prediction.id AS prediction_id,
    prediction.model_run_id,
    prediction.model_version,
    prediction.algorithm,
    prediction.request_key,
    prediction.physical_device_key,
    prediction.device_type,
    prediction.stress_type,
    prediction.target_type,
    prediction.pre_value,
    prediction.reference_policy,
    prediction.stress_features,
    NULLIF(prediction.stress_features->>'ion_species', '') AS ion_species,
    (prediction.stress_features->>'beam_energy_mev')::double precision AS beam_energy_mev,
    (prediction.stress_features->>'let_surface')::double precision AS let_surface,
    (prediction.stress_features->>'range_um')::double precision AS range_um,
    (prediction.stress_features->>'fluence_or_dose')::double precision AS fluence_or_dose,
    prediction.predicted_response,
    prediction.predicted_response_lower,
    prediction.predicted_response_upper,
    prediction.predicted_post_value,
    prediction.predicted_post_lower,
    prediction.predicted_post_upper,
    CASE
      WHEN prediction.target_type = 'delta_vth_v'
        THEN prediction.predicted_response
      WHEN prediction.target_type = 'log_rdson_ratio'
        THEN prediction.predicted_post_value - prediction.pre_value
    END AS physical_damage_delta,
    CASE
      WHEN prediction.target_type = 'delta_vth_v'
        THEN prediction.predicted_response_lower
      WHEN prediction.target_type = 'log_rdson_ratio'
        THEN prediction.predicted_post_lower - prediction.pre_value
    END AS physical_damage_lower,
    CASE
      WHEN prediction.target_type = 'delta_vth_v'
        THEN prediction.predicted_response_upper
      WHEN prediction.target_type = 'log_rdson_ratio'
        THEN prediction.predicted_post_upper - prediction.pre_value
    END AS physical_damage_upper,
    prediction.ood_score,
    prediction.activated_at,
    prediction.created_at
FROM iv_damage_decision_eligible_prediction_view prediction;

COMMENT ON VIEW iv_damage_equivalence_input_view IS
    'Only active-release, same-device, in-domain, validation-gated V3 predictions. '
    'This is the sole prospective prediction boundary for equivalence consumers.';

CREATE VIEW iv_damage_equivalence_fingerprint_view AS
SELECT
    model_run_id,
    model_version,
    algorithm,
    device_type,
    stress_type,
    stress_features,
    ion_species,
    beam_energy_mev,
    let_surface,
    range_um,
    fluence_or_dose,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY physical_damage_delta)
        FILTER (WHERE target_type = 'delta_vth_v') AS dvth_v,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY physical_damage_lower)
        FILTER (WHERE target_type = 'delta_vth_v') AS dvth_lower_v,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY physical_damage_upper)
        FILTER (WHERE target_type = 'delta_vth_v') AS dvth_upper_v,
    COUNT(*) FILTER (WHERE target_type = 'delta_vth_v') AS dvth_prediction_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY physical_damage_delta)
        FILTER (WHERE target_type = 'log_rdson_ratio') AS drdson_mohm,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY physical_damage_lower)
        FILTER (WHERE target_type = 'log_rdson_ratio') AS drdson_lower_mohm,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY physical_damage_upper)
        FILTER (WHERE target_type = 'log_rdson_ratio') AS drdson_upper_mohm,
    COUNT(*) FILTER (WHERE target_type = 'log_rdson_ratio') AS drdson_prediction_count,
    COUNT(DISTINCT physical_device_key) AS independent_physical_devices,
    COUNT(*) AS prediction_count,
    MIN(activated_at) AS active_release_since,
    MAX(created_at) AS latest_prediction_at,
    'v3_active_release_same_device'::text AS prediction_evidence_basis
FROM iv_damage_equivalence_input_view
WHERE stress_type = 'irradiation'
GROUP BY model_run_id, model_version, algorithm, device_type, stress_type,
         stress_features, ion_species, beam_energy_mev, let_surface, range_um,
         fluence_or_dose;

COMMENT ON VIEW iv_damage_equivalence_fingerprint_view IS
    'Aggregated irradiation fingerprints derived exclusively from the canonical '
    'V3 decision-eligible prediction view; prediction_count is not an independent-device count.';
