-- Certification is necessary but not sufficient for first decision release:
-- require a passed prospective shadow assessment at the database boundary.

CREATE OR REPLACE FUNCTION iv_damage_validate_release_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    model_stress TEXT;
    model_target TEXT;
    model_status TEXT;
BEGIN
    SELECT stress_type, target_type, release_status
      INTO model_stress, model_target, model_status
      FROM iv_damage_model_runs WHERE id = NEW.model_run_id;
    IF model_stress IS DISTINCT FROM NEW.stress_type
       OR model_target IS DISTINCT FROM NEW.target_type THEN
        RAISE EXCEPTION 'release and model domains do not match';
    END IF;
    IF NOT NEW.active OR model_status NOT IN ('shadow', 'retired') THEN
        RAISE EXCEPTION 'first release requires monitored shadow; rollback requires retired model';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM iv_damage_external_certifications certification
        WHERE certification.model_run_id = NEW.model_run_id AND certification.passed
    ) THEN
        RAISE EXCEPTION 'release activation requires passed sealed external certification';
    END IF;
    IF model_status = 'shadow' AND NOT EXISTS (
        SELECT 1
        FROM iv_damage_model_deployments deployment
        JOIN iv_damage_monitoring_assessments assessment
          ON assessment.deployment_id = deployment.id
         AND assessment.model_run_id = deployment.model_run_id
         AND assessment.assessment_kind = 'shadow_promotion'
         AND assessment.passed
        WHERE deployment.model_run_id = NEW.model_run_id
          AND deployment.deployment_mode = 'shadow'
          AND deployment.active
    ) THEN
        RAISE EXCEPTION 'first release requires a passed prospective shadow assessment';
    END IF;
    IF model_status = 'retired' AND NOT EXISTS (
        SELECT 1 FROM iv_damage_model_releases history
        WHERE history.model_run_id = NEW.model_run_id
    ) THEN
        RAISE EXCEPTION 'rollback target must have prior release history';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION iv_damage_validate_curve_decision_deployment()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deployment_mode = 'decision' AND NOT EXISTS (
        SELECT 1
        FROM iv_damage_curve_model_deployments shadow
        JOIN iv_damage_curve_monitoring_assessments assessment
          ON assessment.deployment_id = shadow.id
         AND assessment.curve_model_run_id = shadow.curve_model_run_id
         AND assessment.assessment_kind = 'shadow_promotion'
         AND assessment.passed
        WHERE shadow.curve_model_run_id = NEW.curve_model_run_id
          AND shadow.deployment_mode = 'shadow'
          AND NOT shadow.active
    ) THEN
        RAISE EXCEPTION 'curve decision deployment requires passed prospective shadow assessment';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_decision_promotion_guard
BEFORE INSERT ON iv_damage_curve_model_deployments
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_curve_decision_deployment();
