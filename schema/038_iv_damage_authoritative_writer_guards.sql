-- Compatibility guards force all evidence writers, including older CLI code,
-- through the authoritative identities introduced in migration 035.

CREATE OR REPLACE FUNCTION iv_damage_validate_observation_acquisition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    acquisition iv_damage_acquisitions%ROWTYPE;
BEGIN
    IF NEW.acquisition_id IS NULL THEN
        SELECT * INTO acquisition
        FROM iv_damage_acquisitions WHERE metadata_id = NEW.metadata_id;
        NEW.acquisition_id := acquisition.id;
    ELSE
        SELECT * INTO acquisition
        FROM iv_damage_acquisitions WHERE id = NEW.acquisition_id;
    END IF;
    IF acquisition.id IS NULL
       OR acquisition.metadata_id <> NEW.metadata_id
       OR acquisition.measurement_protocol_id <> NEW.measurement_protocol_id
       OR acquisition.measured_at <> NEW.measured_at
       OR NEW.source_fingerprint->>'acquisition_point_payload_hash'
            IS DISTINCT FROM acquisition.point_payload_hash THEN
        RAISE EXCEPTION USING MESSAGE =
            'metric observation must be extracted from the registered authoritative acquisition payload';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION iv_damage_bind_response_session()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    session_record iv_damage_stress_sessions%ROWTYPE;
BEGIN
    SELECT * INTO session_record
    FROM iv_damage_stress_sessions
    WHERE stress_session_key = NEW.stress_session_key;
    IF NOT FOUND
       OR session_record.physical_device_key <> NEW.physical_device_key
       OR session_record.stress_type <> NEW.stress_type
       OR session_record.campaign_key <> NEW.campaign_key
       OR session_record.run_key <> NEW.run_key
       OR session_record.stress_condition_key
            IS DISTINCT FROM NEW.stress_features->>'stress_condition_key'
       OR session_record.stress_features IS DISTINCT FROM NEW.stress_features THEN
        RAISE EXCEPTION USING MESSAGE =
            'response unit requires a matching pre-registered authoritative stress session';
    END IF;
    NEW.stress_session_id := session_record.id;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_a_bind_response_session
BEFORE INSERT ON iv_damage_response_units
FOR EACH ROW EXECUTE FUNCTION iv_damage_bind_response_session();

CREATE FUNCTION iv_damage_link_response_observations()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO iv_damage_response_observations (
        response_unit_id, observation_id, observation_role
    )
    SELECT NEW.id, observation_id, 'pre'
    FROM unnest(NEW.pre_observation_ids) observation_id;
    INSERT INTO iv_damage_response_observations (
        response_unit_id, observation_id, observation_role
    )
    SELECT NEW.id, observation_id, 'post'
    FROM unnest(NEW.post_observation_ids) observation_id;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_response_observation_linker
AFTER INSERT ON iv_damage_response_units
FOR EACH ROW EXECUTE FUNCTION iv_damage_link_response_observations();

CREATE FUNCTION iv_damage_validate_curve_model_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    policy_stress TEXT;
    policy_target TEXT;
BEGIN
    SELECT stress_type, target_type INTO policy_stress, policy_target
    FROM iv_damage_acceptance_policies WHERE id = NEW.acceptance_policy_id;
    IF policy_stress IS DISTINCT FROM NEW.stress_type
       OR (NEW.curve_family = 'IdVg' AND policy_target <> 'delta_vth_v')
       OR (NEW.curve_family = 'IdVd' AND policy_target <> 'log_rdson_ratio') THEN
        RAISE EXCEPTION 'curve model and acceptance-policy physical domains do not match';
    END IF;
    IF NEW.release_status <> 'candidate' THEN
        RAISE EXCEPTION 'curve model must enter lifecycle as candidate';
    END IF;
    IF NOT (NEW.validation_metrics @> '{"external_certification": "not_accessed"}'::jsonb) THEN
        RAISE EXCEPTION 'curve candidate must attest that external outcomes were not accessed';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_model_insert_guard
BEFORE INSERT ON iv_damage_curve_model_runs
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_curve_model_insert();

CREATE FUNCTION iv_damage_guard_curve_model_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'curve model runs cannot be deleted';
    END IF;
    IF to_jsonb(NEW) - ARRAY[
        'release_status', 'validated_at', 'released_at', 'retired_at'
    ]::text[] IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
        'release_status', 'validated_at', 'released_at', 'retired_at'
    ]::text[] THEN
        RAISE EXCEPTION 'curve model identity, development evidence, configuration, and artifact are immutable';
    END IF;
    IF NOT (
        (OLD.release_status = 'candidate' AND NEW.release_status IN ('validated', 'failed'))
        OR (OLD.release_status = 'validated' AND NEW.release_status = 'shadow')
        OR (OLD.release_status = 'shadow' AND NEW.release_status IN ('released', 'failed'))
        OR (OLD.release_status = 'released' AND NEW.release_status = 'retired')
    ) THEN
        RAISE EXCEPTION 'invalid curve model lifecycle transition: % to %',
            OLD.release_status, NEW.release_status;
    END IF;
    IF NEW.release_status IN ('validated', 'shadow', 'released') AND NOT EXISTS (
        SELECT 1 FROM iv_damage_curve_external_certifications certification
        WHERE certification.curve_model_run_id = NEW.id AND certification.passed
    ) THEN
        RAISE EXCEPTION 'curve lifecycle requires passed external certification';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_model_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_curve_model_runs
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_model_lifecycle();

CREATE FUNCTION iv_damage_guard_curve_scientific_record()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END
$$;

CREATE TRIGGER iv_damage_curve_members_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_snapshot_members
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_scientific_record();
CREATE TRIGGER iv_damage_curve_validation_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_validation_results
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_scientific_record();
CREATE TRIGGER iv_damage_curve_predictions_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_predictions
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_scientific_record();
CREATE TRIGGER iv_damage_curve_prediction_points_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_prediction_points
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_scientific_record();
CREATE TRIGGER iv_damage_curve_outcomes_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_prediction_outcomes
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_curve_scientific_record();
