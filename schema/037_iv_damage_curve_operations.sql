-- Prospective monitoring and promotion records for learned full curves.

ALTER TABLE iv_damage_curve_validation_results
    DROP CONSTRAINT iv_damage_curve_validation_results_point_count_check;
ALTER TABLE iv_damage_curve_validation_results
    ADD CONSTRAINT iv_damage_curve_validation_results_point_count_check
    CHECK (point_count >= 0);

CREATE TABLE iv_damage_curve_monitoring_assessments (
    id BIGSERIAL PRIMARY KEY,
    curve_model_run_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_model_runs(id) ON DELETE RESTRICT,
    deployment_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_model_deployments(id) ON DELETE RESTRICT,
    assessment_kind TEXT NOT NULL
        CHECK (assessment_kind IN ('shadow_promotion', 'released_monitoring')),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    policy JSONB NOT NULL,
    metrics JSONB NOT NULL,
    checks JSONB NOT NULL,
    passed BOOLEAN NOT NULL,
    assessed_by TEXT NOT NULL,
    assessed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (window_end > window_start)
);

CREATE FUNCTION iv_damage_validate_curve_deployment_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    model_record iv_damage_curve_model_runs%ROWTYPE;
BEGIN
    SELECT * INTO model_record FROM iv_damage_curve_model_runs
    WHERE id = NEW.curve_model_run_id;
    IF NOT FOUND
       OR model_record.stress_type <> NEW.stress_type
       OR model_record.curve_family <> NEW.curve_family
       OR model_record.measurement_protocol_id <> NEW.measurement_protocol_id THEN
        RAISE EXCEPTION 'curve deployment does not match model domain';
    END IF;
    IF NEW.deployment_mode = 'shadow' AND model_record.release_status NOT IN ('validated', 'shadow') THEN
        RAISE EXCEPTION 'shadow curve deployment requires validated model';
    END IF;
    IF NEW.deployment_mode = 'decision' AND model_record.release_status <> 'released' THEN
        RAISE EXCEPTION 'decision curve deployment requires released model';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM iv_damage_curve_external_certifications certification
        WHERE certification.curve_model_run_id = NEW.curve_model_run_id
          AND certification.passed
    ) THEN
        RAISE EXCEPTION 'curve deployment requires passed external certification';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_deployment_insert_guard
BEFORE INSERT ON iv_damage_curve_model_deployments
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_curve_deployment_insert();

CREATE FUNCTION iv_damage_guard_curve_deployment_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'curve deployment history cannot be deleted';
    END IF;
    IF to_jsonb(NEW) - ARRAY[
        'active', 'deactivated_by', 'deactivated_at', 'deactivation_reason'
    ]::text[] IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
        'active', 'deactivated_by', 'deactivated_at', 'deactivation_reason'
    ]::text[] OR NOT OLD.active OR NEW.active THEN
        RAISE EXCEPTION 'curve deployment may only transition once from active to inactive';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_deployment_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_curve_model_deployments
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_deployment_lifecycle();

CREATE VIEW iv_damage_curve_temporal_monitoring_view AS
SELECT
    date_trunc('week', prediction.created_at) AS monitoring_week,
    prediction.curve_model_run_id,
    model.model_version,
    request.stress_type,
    request.curve_family,
    request.measurement_protocol_id,
    prediction.deployment_mode,
    COUNT(*) AS curve_predictions,
    COUNT(*) FILTER (WHERE outcome.id IS NOT NULL) AS matched_outcomes,
    COUNT(*) FILTER (WHERE NOT prediction.in_domain) AS abstentions,
    AVG(outcome.mae_a) AS mean_curve_mae_a,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY outcome.max_abs_error_a) AS p90_max_abs_error_a,
    AVG(outcome.simultaneous_band_hit::integer) AS simultaneous_band_coverage
FROM iv_damage_curve_predictions prediction
JOIN iv_damage_curve_prediction_requests request ON request.id = prediction.request_id
JOIN iv_damage_curve_model_runs model ON model.id = prediction.curve_model_run_id
LEFT JOIN iv_damage_curve_prediction_outcomes outcome
  ON outcome.curve_prediction_id = prediction.id
GROUP BY date_trunc('week', prediction.created_at), prediction.curve_model_run_id,
         model.model_version, request.stress_type, request.curve_family,
         request.measurement_protocol_id, prediction.deployment_mode;

CREATE VIEW iv_damage_curve_release_gate_view AS
SELECT
    model.id AS curve_model_run_id,
    model.model_version,
    model.stress_type,
    model.curve_family,
    model.measurement_protocol_id,
    'V'::text AS x_unit,
    'A'::text AS current_unit,
    model.release_status,
    model.validation_metrics -> 'development_gate_checks' AS development_gate_checks,
    selection.id IS NOT NULL AS selected,
    certification.passed AS external_certification_passed,
    certification.gate_checks AS external_gate_checks,
    EXISTS (
        SELECT 1 FROM iv_damage_curve_model_deployments deployment
        WHERE deployment.curve_model_run_id = model.id
          AND deployment.deployment_mode = 'shadow' AND deployment.active
    ) AS active_shadow,
    EXISTS (
        SELECT 1 FROM iv_damage_curve_model_deployments deployment
        WHERE deployment.curve_model_run_id = model.id
          AND deployment.deployment_mode = 'decision' AND deployment.active
    ) AS active_decision,
    monitoring.passed AS latest_monitoring_passed,
    monitoring.assessed_at AS latest_monitoring_at
FROM iv_damage_curve_model_runs model
LEFT JOIN iv_damage_curve_model_selections selection
  ON selection.curve_model_run_id = model.id
LEFT JOIN iv_damage_curve_external_certifications certification
  ON certification.curve_model_run_id = model.id
LEFT JOIN LATERAL (
    SELECT passed, assessed_at
    FROM iv_damage_curve_monitoring_assessments assessment
    WHERE assessment.curve_model_run_id = model.id
    ORDER BY assessed_at DESC, id DESC LIMIT 1
) monitoring ON TRUE;
