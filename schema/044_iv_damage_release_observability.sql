-- Close the remaining scalar certification bypass and expose the evidence
-- needed to interpret scalar release and monitoring state in Superset.

CREATE FUNCTION iv_damage_validate_scalar_development_attestation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.release_status <> 'candidate'
       OR NEW.validation_metrics->>'external_certification' IS DISTINCT FROM 'not_accessed'
       OR jsonb_typeof(NEW.validation_metrics->'development_gate_eligible') IS DISTINCT FROM 'boolean'
       OR jsonb_typeof(NEW.validation_metrics->'development_gate_checks') IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION
            'scalar model must enter as an attested development candidate with external certification not accessed';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_scalar_development_attestation_guard
BEFORE INSERT ON iv_damage_model_runs
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_scalar_development_attestation();

CREATE OR REPLACE VIEW iv_damage_release_gate_check_view AS
SELECT
    model.id AS model_run_id,
    model.model_version,
    model.stress_type,
    model.target_type,
    CASE model.target_type WHEN 'delta_vth_v' THEN 'V' ELSE 'ln(ratio)' END AS response_unit,
    model.release_status,
    policy.policy_version,
    policy.approved AS policy_approved,
    selection.id IS NOT NULL AS candidate_selected,
    certification.id IS NOT NULL AS external_certification_present,
    COALESCE(certification.passed, FALSE) AS external_certification_passed,
    certification.certified_at,
    model.validation_metrics -> 'development_gate_checks' AS development_gate_checks,
    certification.gate_checks AS certification_gate_checks,
    EXISTS (
        SELECT 1 FROM iv_damage_model_deployments deployment
        WHERE deployment.model_run_id = model.id
          AND deployment.deployment_mode = 'shadow' AND deployment.active
    ) AS active_shadow,
    EXISTS (
        SELECT 1 FROM iv_damage_model_releases release
        WHERE release.model_run_id = model.id AND release.active
    ) AS active_decision_release,
    policy.requirements AS acceptance_requirements,
    monitoring.passed AS latest_monitoring_passed,
    monitoring.assessed_at AS latest_monitoring_at,
    monitoring.checks AS latest_monitoring_checks,
    monitoring.metrics AS latest_monitoring_metrics
FROM iv_damage_model_runs model
JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
LEFT JOIN iv_damage_model_selections selection ON selection.model_run_id = model.id
LEFT JOIN iv_damage_external_certifications certification ON certification.model_run_id = model.id
LEFT JOIN LATERAL (
    SELECT assessment.passed, assessment.assessed_at,
           assessment.checks, assessment.metrics
    FROM iv_damage_monitoring_assessments assessment
    WHERE assessment.model_run_id = model.id
    ORDER BY assessment.assessed_at DESC, assessment.id DESC
    LIMIT 1
) monitoring ON TRUE;

CREATE OR REPLACE VIEW iv_damage_temporal_monitoring_view AS
SELECT
    date_trunc('week', prediction.created_at) AS monitoring_week,
    prediction.model_run_id,
    model.model_version,
    request.stress_type,
    request.target_type,
    CASE request.target_type WHEN 'delta_vth_v' THEN 'V' ELSE 'ln(ratio)' END AS response_unit,
    COUNT(*) AS predictions,
    COUNT(*) FILTER (WHERE outcome.id IS NOT NULL) AS matched_outcomes,
    COUNT(*) FILTER (WHERE NOT prediction.in_domain) AS abstentions,
    AVG(abs(outcome.observed_response - prediction.predicted_response))
        FILTER (WHERE outcome.id IS NOT NULL) AS mae,
    AVG(outcome.observed_response - prediction.predicted_response)
        FILTER (WHERE outcome.id IS NOT NULL) AS bias,
    AVG((outcome.observed_response BETWEEN prediction.predicted_response_lower
        AND prediction.predicted_response_upper)::integer)
        FILTER (WHERE outcome.id IS NOT NULL) AS interval_coverage,
    prediction.deployment_mode
FROM iv_damage_predictions prediction
JOIN iv_damage_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
LEFT JOIN iv_damage_prediction_outcomes outcome ON outcome.prediction_id = prediction.id
GROUP BY date_trunc('week', prediction.created_at), prediction.model_run_id,
         model.model_version, request.stress_type, request.target_type,
         prediction.deployment_mode;

COMMENT ON FUNCTION iv_damage_validate_scalar_development_attestation() IS
    'Prevents legacy combined train/evaluate paths from bypassing sealed external certification.';
COMMENT ON VIEW iv_damage_release_gate_check_view IS
    'Scalar release state with governing thresholds and latest prospective assessment.';
COMMENT ON VIEW iv_damage_temporal_monitoring_view IS
    'Weekly prospective scalar performance separated by shadow versus decision deployment mode.';
