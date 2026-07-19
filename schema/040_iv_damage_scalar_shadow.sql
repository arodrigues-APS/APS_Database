-- Allow prospective scalar scoring in shadow without weakening decision use.

ALTER TABLE iv_damage_predictions
    ADD COLUMN deployment_mode TEXT NOT NULL DEFAULT 'decision'
        CHECK (deployment_mode IN ('shadow', 'decision')),
    ADD CONSTRAINT iv_damage_shadow_prediction_not_decision_ck
        CHECK (deployment_mode <> 'shadow' OR NOT decision_eligible);

CREATE OR REPLACE FUNCTION iv_damage_validate_prediction()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    request_stress TEXT;
    request_target TEXT;
    request_pre DOUBLE PRECISION;
    request_protocol TEXT;
    request_status TEXT;
    model_stress TEXT;
    model_target TEXT;
    model_status TEXT;
    model_domain JSONB;
    policy_approved BOOLEAN;
    development_gate_eligible BOOLEAN;
    deployment_time TIMESTAMPTZ;
BEGIN
    SELECT request.stress_type, request.target_type, request.pre_value,
           request.measurement_protocol_id, request.request_status
    INTO request_stress, request_target, request_pre, request_protocol, request_status
    FROM iv_damage_prediction_requests request
    WHERE request.id = NEW.request_id FOR SHARE;
    SELECT model.stress_type, model.target_type, model.release_status,
           model.released_domain, policy.approved,
           model.validation_metrics @> '{"development_gate_eligible": true}'::jsonb
    INTO model_stress, model_target, model_status, model_domain,
         policy_approved, development_gate_eligible
    FROM iv_damage_model_runs model
    JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
    WHERE model.id = NEW.model_run_id;

    IF NEW.deployment_mode = 'shadow' THEN
        SELECT activated_at INTO deployment_time
        FROM iv_damage_model_deployments
        WHERE model_run_id = NEW.model_run_id AND deployment_mode = 'shadow' AND active
        ORDER BY activated_at DESC, id DESC LIMIT 1 FOR SHARE;
    ELSE
        SELECT activated_at INTO deployment_time
        FROM iv_damage_model_releases
        WHERE model_run_id = NEW.model_run_id AND stress_type = request_stress
          AND target_type = request_target AND active
        ORDER BY activated_at DESC, id DESC LIMIT 1 FOR SHARE;
    END IF;

    IF request_status IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION 'only a pending request can be scored';
    END IF;
    IF request_stress IS DISTINCT FROM model_stress
       OR request_target IS DISTINCT FROM model_target THEN
        RAISE EXCEPTION 'prediction request and model domains do not match';
    END IF;
    IF deployment_time IS NULL OR NEW.created_at < deployment_time THEN
        RAISE EXCEPTION 'prediction requires a currently active matching deployment';
    END IF;
    IF NEW.deployment_mode = 'shadow' AND (
        model_status <> 'shadow' OR NEW.decision_eligible
        OR NEW.evidence_status = 'decision_eligible'
    ) THEN
        RAISE EXCEPTION 'shadow prediction must be screening-only and non-decision';
    END IF;
    IF NEW.deployment_mode = 'decision' AND model_status <> 'released' THEN
        RAISE EXCEPTION 'decision prediction requires released lifecycle state';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM iv_damage_external_certifications certification
        WHERE certification.model_run_id = NEW.model_run_id AND certification.passed
    ) THEN
        RAISE EXCEPTION 'prediction requires passed external certification';
    END IF;
    IF NEW.decision_eligible AND (
        NOT policy_approved OR NOT development_gate_eligible
        OR jsonb_typeof(model_domain->'measurement_protocol_ids') IS DISTINCT FROM 'array'
        OR NOT (model_domain->'measurement_protocol_ids' ? request_protocol)
    ) THEN
        RAISE EXCEPTION 'decision-eligible prediction protocol is outside released domain';
    END IF;
    IF request_target = 'log_rdson_ratio' AND (
        (NEW.predicted_post_value IS NOT NULL AND NEW.predicted_post_value <= 0.0)
        OR (NEW.predicted_post_lower IS NOT NULL AND NEW.predicted_post_lower <= 0.0)
        OR (NEW.predicted_post_upper IS NOT NULL AND NEW.predicted_post_upper <= 0.0)
    ) THEN
        RAISE EXCEPTION 'Rds(on) post predictions must remain positive';
    END IF;
    IF request_target = 'delta_vth_v' AND (
        (NEW.predicted_response IS NOT NULL AND NEW.predicted_post_value IS NOT NULL
         AND abs(NEW.predicted_post_value - request_pre - NEW.predicted_response)
             > 1e-10 * greatest(1.0, abs(NEW.predicted_post_value), abs(request_pre), abs(NEW.predicted_response)))
        OR (NEW.predicted_response_lower IS NOT NULL AND NEW.predicted_post_lower IS NOT NULL
            AND abs(NEW.predicted_post_lower - request_pre - NEW.predicted_response_lower)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_post_lower), abs(request_pre), abs(NEW.predicted_response_lower)))
        OR (NEW.predicted_response_upper IS NOT NULL AND NEW.predicted_post_upper IS NOT NULL
            AND abs(NEW.predicted_post_upper - request_pre - NEW.predicted_response_upper)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_post_upper), abs(request_pre), abs(NEW.predicted_response_upper)))
    ) THEN
        RAISE EXCEPTION 'threshold-voltage response and post predictions are inconsistent';
    END IF;
    IF request_target = 'log_rdson_ratio' AND (
        (NEW.predicted_response IS NOT NULL AND NEW.predicted_post_value IS NOT NULL
         AND abs((ln(NEW.predicted_post_value) - ln(request_pre)) - NEW.predicted_response)
             > 1e-10 * greatest(1.0, abs(NEW.predicted_response)))
        OR (NEW.predicted_response_lower IS NOT NULL AND NEW.predicted_post_lower IS NOT NULL
            AND abs((ln(NEW.predicted_post_lower) - ln(request_pre)) - NEW.predicted_response_lower)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_response_lower)))
        OR (NEW.predicted_response_upper IS NOT NULL AND NEW.predicted_post_upper IS NOT NULL
            AND abs((ln(NEW.predicted_post_upper) - ln(request_pre)) - NEW.predicted_response_upper)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_response_upper)))
    ) THEN
        RAISE EXCEPTION 'Rds(on) response and post predictions are inconsistent';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE VIEW iv_damage_prediction_monitoring_view AS
SELECT
    prediction.id AS prediction_id,
    prediction.model_run_id,
    prediction.request_id,
    request.request_key,
    request.stress_type,
    request.target_type,
    request.device_type,
    prediction.support_status,
    prediction.evidence_status,
    prediction.in_domain,
    prediction.decision_eligible,
    prediction.ood_score,
    prediction.ood_threshold,
    prediction.predicted_response,
    prediction.predicted_response_lower,
    prediction.predicted_response_upper,
    outcome.observed_response,
    CASE WHEN outcome.observed_response IS NOT NULL
         THEN prediction.predicted_response - outcome.observed_response END AS residual,
    CASE WHEN outcome.observed_response IS NOT NULL
         THEN abs(prediction.predicted_response - outcome.observed_response) END AS abs_residual,
    CASE WHEN outcome.observed_response IS NOT NULL
              AND prediction.predicted_response_lower IS NOT NULL
              AND prediction.predicted_response_upper IS NOT NULL
         THEN outcome.observed_response BETWEEN prediction.predicted_response_lower
                                              AND prediction.predicted_response_upper END AS interval_hit,
    prediction.created_at,
    outcome.matched_at,
    model.model_version,
    prediction.deployment_mode
FROM iv_damage_predictions prediction
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
JOIN iv_damage_model_runs model ON model.id = prediction.model_run_id
LEFT JOIN iv_damage_prediction_outcomes outcome ON outcome.prediction_id = prediction.id;
