-- Fail-closed lifecycle guards for the selection/certification objects added
-- by migration 035.  Kept separate so 035 remains reviewable as a data-model
-- migration and this migration remains reviewable as an authorization layer.

CREATE TABLE iv_damage_curve_projection_validations (
    id BIGSERIAL PRIMARY KEY,
    projection_method_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_projection_methods(id) ON DELETE RESTRICT,
    curve_response_pair_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_response_pairs(id) ON DELETE RESTRICT,
    dataset_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    split_scheme TEXT NOT NULL,
    split_role TEXT NOT NULL CHECK (split_role IN ('grouped_test', 'external_test')),
    evaluation_kind TEXT NOT NULL CHECK (evaluation_kind IN ('development_cv', 'external_certification')),
    mae_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(mae_a) AND mae_a >= 0),
    max_abs_error_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(max_abs_error_a) AND max_abs_error_a >= 0),
    normalized_rmse DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(normalized_rmse) AND normalized_rmse >= 0),
    simultaneous_band_hit BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (projection_method_id, curve_response_pair_id, split_scheme)
);

CREATE TABLE iv_damage_curve_projection_certifications (
    id BIGSERIAL PRIMARY KEY,
    projection_method_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_curve_projection_methods(id) ON DELETE RESTRICT,
    dataset_snapshot_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    metrics JSONB NOT NULL,
    gate_checks JSONB NOT NULL,
    passed BOOLEAN NOT NULL,
    certified_by TEXT NOT NULL,
    certified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION iv_damage_guard_model_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'model runs cannot be deleted';
    END IF;
    IF to_jsonb(NEW) - ARRAY[
        'release_status', 'validated_at', 'released_at', 'retired_at'
    ]::text[] IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
        'release_status', 'validated_at', 'released_at', 'retired_at'
    ]::text[] THEN
        RAISE EXCEPTION 'model identity, development evidence, configuration, and artifact are immutable';
    END IF;
    IF NOT (
        (OLD.release_status = 'candidate' AND NEW.release_status IN ('validated', 'failed'))
        OR (OLD.release_status = 'validated' AND NEW.release_status IN ('shadow', 'released', 'failed'))
        OR (OLD.release_status = 'shadow' AND NEW.release_status IN ('released', 'retired', 'failed'))
        OR (OLD.release_status = 'released' AND NEW.release_status = 'retired')
        OR (OLD.release_status = 'retired' AND NEW.release_status = 'released')
    ) THEN
        RAISE EXCEPTION 'invalid model lifecycle transition: % to %',
            OLD.release_status, NEW.release_status;
    END IF;
    IF NEW.release_status IN ('validated', 'shadow', 'released') AND NOT EXISTS (
        SELECT 1 FROM iv_damage_external_certifications certification
        WHERE certification.model_run_id = NEW.id AND certification.passed
    ) THEN
        RAISE EXCEPTION 'validated/released model requires passed sealed external certification';
    END IF;
    IF NEW.release_status = 'released' AND (
        NEW.released_at IS NULL
        OR jsonb_typeof(NEW.released_domain->'measurement_protocol_ids') IS DISTINCT FROM 'array'
        OR jsonb_array_length(NEW.released_domain->'measurement_protocol_ids') = 0
        OR NEW.released_domain->>'stress_type' IS DISTINCT FROM NEW.stress_type
        OR NEW.released_domain->>'target_type' IS DISTINCT FROM NEW.target_type
    ) THEN
        RAISE EXCEPTION 'released model requires a canonical nonempty released domain';
    END IF;
    IF NEW.release_status = 'retired' AND NEW.retired_at IS NULL THEN
        RAISE EXCEPTION 'retired model requires retired_at';
    END IF;
    RETURN NEW;
END
$$;

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
    IF NOT NEW.active OR model_status NOT IN ('validated', 'shadow', 'retired') THEN
        RAISE EXCEPTION 'release activation requires an active event for a certified model';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM iv_damage_external_certifications certification
        WHERE certification.model_run_id = NEW.model_run_id AND certification.passed
    ) THEN
        RAISE EXCEPTION 'release activation requires passed sealed external certification';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION iv_damage_validate_deployment_insert()
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
        RAISE EXCEPTION 'deployment and model domains do not match';
    END IF;
    IF NEW.deployment_mode = 'shadow' AND model_status NOT IN ('validated', 'shadow') THEN
        RAISE EXCEPTION 'shadow deployment requires a certified validated model';
    END IF;
    IF NEW.deployment_mode = 'decision' AND model_status <> 'released' THEN
        RAISE EXCEPTION 'decision deployment requires a released model';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM iv_damage_external_certifications certification
        WHERE certification.model_run_id = NEW.model_run_id AND certification.passed
    ) THEN
        RAISE EXCEPTION 'deployment requires passed sealed external certification';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_deployment_insert_guard
BEFORE INSERT ON iv_damage_model_deployments
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_deployment_insert();

CREATE FUNCTION iv_damage_guard_deployment_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'deployment history cannot be deleted';
    END IF;
    IF to_jsonb(NEW) - ARRAY[
        'active', 'deactivated_by', 'deactivated_at', 'deactivation_reason'
    ]::text[] IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
        'active', 'deactivated_by', 'deactivated_at', 'deactivation_reason'
    ]::text[] OR NOT OLD.active OR NEW.active THEN
        RAISE EXCEPTION 'deployment may only transition once from active to inactive';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_deployment_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_model_deployments
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_deployment_lifecycle();

CREATE FUNCTION iv_damage_guard_curve_prediction_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deployment_mode = 'shadow' AND NEW.decision_eligible THEN
        RAISE EXCEPTION 'shadow curve predictions can never be decision eligible';
    END IF;
    IF NEW.decision_eligible AND NOT EXISTS (
        SELECT 1
        FROM iv_damage_curve_model_deployments deployment
        JOIN iv_damage_curve_external_certifications certification
          ON certification.curve_model_run_id = deployment.curve_model_run_id
         AND certification.passed
        WHERE deployment.curve_model_run_id = NEW.curve_model_run_id
          AND deployment.deployment_mode = 'decision' AND deployment.active
    ) THEN
        RAISE EXCEPTION 'decision-eligible curve prediction requires active certified decision deployment';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_prediction_insert_guard
BEFORE INSERT ON iv_damage_curve_predictions
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_prediction_insert();

CREATE VIEW iv_damage_curve_validation_summary_view AS
SELECT
    validation.curve_model_run_id,
    model.model_version,
    model.stress_type,
    model.curve_family,
    model.measurement_protocol_id,
    validation.split_scheme,
    validation.split_role,
    validation.evaluation_kind,
    validation.support_status,
    COUNT(*) AS independent_curves,
    COUNT(DISTINCT validation.physical_device_key) AS physical_devices,
    AVG(validation.mae_a) AS mean_curve_mae_a,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY validation.max_abs_error_a) AS median_max_abs_error_a,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY validation.max_abs_error_a) AS p90_max_abs_error_a,
    AVG(validation.normalized_rmse) AS mean_normalized_rmse,
    AVG(validation.simultaneous_band_hit::integer) AS simultaneous_band_coverage
FROM iv_damage_curve_validation_results validation
JOIN iv_damage_curve_model_runs model ON model.id = validation.curve_model_run_id
GROUP BY validation.curve_model_run_id, model.model_version, model.stress_type,
         model.curve_family, model.measurement_protocol_id, validation.split_scheme,
         validation.split_role, validation.evaluation_kind, validation.support_status;

COMMENT ON TABLE iv_damage_curve_projection_certifications IS
    'A scalar-to-curve projection method needs curve-level external evidence; scalar certification alone is insufficient.';
