-- Harden the prospective V3 damage-prediction boundary.
--
-- 032 and 033 are already deployed and remain immutable.  This forward-only
-- migration adds the missing database invariants without changing legacy V2
-- objects.  V3 operational tables were intentionally empty when this
-- migration was introduced, so authoritative measurement timestamps are not
-- synthesized from ingestion timestamps.

CREATE FUNCTION iv_damage_is_finite(value DOUBLE PRECISION)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT value IS NULL
        OR value NOT IN (
            'NaN'::double precision,
            'Infinity'::double precision,
            '-Infinity'::double precision
        )
$$;

CREATE FUNCTION iv_damage_try_float8(value TEXT)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    converted DOUBLE PRECISION;
BEGIN
    IF btrim(value) = '' THEN
        RETURN NULL;
    END IF;
    converted := value::double precision;
    IF NOT iv_damage_is_finite(converted) THEN
        RETURN NULL;
    END IF;
    RETURN converted;
EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range THEN
        RETURN NULL;
END
$$;

ALTER TABLE iv_damage_metric_observations
    ADD COLUMN measured_at TIMESTAMPTZ NOT NULL,
    ADD CONSTRAINT iv_damage_observation_method_target_ck CHECK (
        (metric_name = 'vth_v' AND unit = 'V')
        OR (metric_name = 'rdson_mohm' AND unit = 'mohm')
    ),
    ADD CONSTRAINT iv_damage_observation_value_finite_ck CHECK (
        iv_damage_is_finite(value)
        AND iv_damage_is_finite(uncertainty)
    ),
    ADD CONSTRAINT iv_damage_observation_usable_value_ck CHECK (
        quality_status <> 'usable' OR value IS NOT NULL
    ),
    ADD CONSTRAINT iv_damage_observation_chronology_ck CHECK (
        measured_at <= extracted_at
    );

ALTER TABLE iv_damage_extraction_methods
    ADD COLUMN approved_by TEXT,
    ADD COLUMN approved_at TIMESTAMPTZ,
    ADD CONSTRAINT iv_damage_extraction_metric_target_ck CHECK (
        (metric_name = 'vth_v' AND target_type = 'delta_vth_v')
        OR (metric_name = 'rdson_mohm' AND target_type = 'log_rdson_ratio')
    ),
    ADD CONSTRAINT iv_damage_extraction_approval_audit_ck CHECK (
        NOT approved OR (
            approved_by IS NOT NULL
            AND btrim(approved_by) <> ''
            AND approved_at IS NOT NULL
        )
    );

ALTER TABLE iv_damage_response_units
    ADD COLUMN pre_measured_at TIMESTAMPTZ NOT NULL,
    ADD COLUMN post_measured_at TIMESTAMPTZ NOT NULL,
    ADD CONSTRAINT iv_damage_response_time_order_ck CHECK (
        post_measured_at > pre_measured_at
    ),
    ADD CONSTRAINT iv_damage_response_observation_sets_ck CHECK (
        cardinality(pre_observation_ids) > 0
        AND cardinality(post_observation_ids) > 0
        AND array_position(pre_observation_ids, NULL) IS NULL
        AND array_position(post_observation_ids, NULL) IS NULL
        AND NOT (pre_observation_ids && post_observation_ids)
    ),
    ADD CONSTRAINT iv_damage_response_values_finite_ck CHECK (
        iv_damage_is_finite(pre_value)
        AND iv_damage_is_finite(pre_uncertainty)
        AND iv_damage_is_finite(post_value)
        AND iv_damage_is_finite(post_uncertainty)
        AND iv_damage_is_finite(response_value)
        AND iv_damage_is_finite(response_uncertainty)
    ),
    ADD CONSTRAINT iv_damage_response_target_values_ck CHECK (
        (
            target_type = 'delta_vth_v'
            AND abs(response_value - (post_value - pre_value))
                <= 1e-10 * greatest(1.0, abs(response_value), abs(post_value), abs(pre_value))
        )
        OR (
            target_type = 'log_rdson_ratio'
            AND pre_value > 0.0
            AND post_value > 0.0
            AND abs(response_value - (ln(post_value) - ln(pre_value)))
                <= 1e-10 * greatest(1.0, abs(response_value))
        )
    );

CREATE TABLE iv_damage_dataset_snapshot_members (
    id BIGSERIAL PRIMARY KEY,
    dataset_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    response_unit_id BIGINT NOT NULL
        REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    frozen_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT iv_damage_snapshot_member_identity_uq
        UNIQUE (dataset_snapshot_id, response_unit_id),
    CONSTRAINT iv_damage_snapshot_member_hash_uq
        UNIQUE (dataset_snapshot_id, payload_hash),
    CONSTRAINT iv_damage_snapshot_member_payload_ck CHECK (
        jsonb_typeof(frozen_payload) = 'object'
        AND frozen_payload ? 'unit_key'
        AND frozen_payload ? 'stress_type'
        AND frozen_payload ? 'target_type'
        AND frozen_payload ? 'response_value'
    ),
    CONSTRAINT iv_damage_snapshot_member_hash_ck CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    )
);

CREATE INDEX iv_damage_snapshot_members_response_idx
    ON iv_damage_dataset_snapshot_members (response_unit_id, dataset_snapshot_id);

ALTER TABLE iv_damage_split_assignments
    ADD CONSTRAINT iv_damage_split_frozen_member_fk
    FOREIGN KEY (dataset_snapshot_id, response_unit_id)
    REFERENCES iv_damage_dataset_snapshot_members (
        dataset_snapshot_id, response_unit_id
    ) ON DELETE RESTRICT;

ALTER TABLE iv_damage_prediction_requests
    ADD CONSTRAINT iv_damage_request_values_finite_ck CHECK (
        iv_damage_is_finite(pre_value)
        AND iv_damage_is_finite(pre_uncertainty)
        AND iv_damage_is_finite(requested_prediction_horizon_s)
        AND (
            requested_prediction_horizon_s IS NULL
            OR requested_prediction_horizon_s > 0.0
        )
    ),
    ADD CONSTRAINT iv_damage_request_target_value_ck CHECK (
        target_type <> 'log_rdson_ratio' OR pre_value > 0.0
    ),
    ADD CONSTRAINT iv_damage_request_horizon_ck CHECK (
        stress_type <> 'irradiation'
        OR (
            requested_prediction_horizon_s IS NOT NULL
            AND iv_damage_try_float8(stress_features->>'post_measurement_delay_s')
                IS NOT NULL
            AND iv_damage_try_float8(stress_features->>'post_measurement_delay_s')
                = requested_prediction_horizon_s
        )
    );

ALTER TABLE iv_damage_validation_results
    ADD CONSTRAINT iv_damage_validation_values_finite_ck CHECK (
        iv_damage_is_finite(observed_value)
        AND iv_damage_is_finite(predicted_value)
        AND iv_damage_is_finite(predicted_lower)
        AND iv_damage_is_finite(predicted_upper)
        AND iv_damage_is_finite(residual)
        AND iv_damage_is_finite(abs_residual)
        AND iv_damage_is_finite(ood_score)
    ),
    ADD CONSTRAINT iv_damage_validation_interval_ck CHECK (
        (predicted_lower IS NULL AND predicted_upper IS NULL)
        OR (
            predicted_lower IS NOT NULL
            AND predicted_upper IS NOT NULL
            AND predicted_lower <= predicted_upper
            AND (predicted_value IS NULL OR predicted_value BETWEEN predicted_lower AND predicted_upper)
        )
    ),
    ADD CONSTRAINT iv_damage_validation_residual_ck CHECK (
        (predicted_value IS NULL AND residual IS NULL AND abs_residual IS NULL)
        OR (
            predicted_value IS NOT NULL
            AND residual IS NOT NULL
            AND abs_residual IS NOT NULL
            AND abs_residual >= 0.0
            AND abs(abs_residual - abs(residual))
                <= 1e-10 * greatest(1.0, abs(abs_residual), abs(residual))
        )
    );

ALTER TABLE iv_damage_predictions
    ADD CONSTRAINT iv_damage_prediction_values_finite_ck CHECK (
        iv_damage_is_finite(predicted_response)
        AND iv_damage_is_finite(predicted_response_lower)
        AND iv_damage_is_finite(predicted_response_upper)
        AND iv_damage_is_finite(predicted_post_value)
        AND iv_damage_is_finite(predicted_post_lower)
        AND iv_damage_is_finite(predicted_post_upper)
        AND iv_damage_is_finite(ood_score)
        AND iv_damage_is_finite(ood_threshold)
    ),
    ADD CONSTRAINT iv_damage_prediction_response_interval_ck CHECK (
        (predicted_response_lower IS NULL AND predicted_response_upper IS NULL)
        OR (
            predicted_response_lower IS NOT NULL
            AND predicted_response_upper IS NOT NULL
            AND predicted_response_lower <= predicted_response_upper
            AND (
                predicted_response IS NULL
                OR predicted_response BETWEEN predicted_response_lower AND predicted_response_upper
            )
        )
    ),
    ADD CONSTRAINT iv_damage_prediction_post_interval_ck CHECK (
        (predicted_post_lower IS NULL AND predicted_post_upper IS NULL)
        OR (
            predicted_post_lower IS NOT NULL
            AND predicted_post_upper IS NOT NULL
            AND predicted_post_lower <= predicted_post_upper
            AND (
                predicted_post_value IS NULL
                OR predicted_post_value BETWEEN predicted_post_lower AND predicted_post_upper
            )
        )
    ),
    ADD CONSTRAINT iv_damage_prediction_decision_outputs_ck CHECK (
        NOT decision_eligible OR (
            predicted_response IS NOT NULL
            AND predicted_response_lower IS NOT NULL
            AND predicted_response_upper IS NOT NULL
            AND predicted_post_value IS NOT NULL
            AND predicted_post_lower IS NOT NULL
            AND predicted_post_upper IS NOT NULL
        )
    ),
    ADD CONSTRAINT iv_damage_prediction_ood_ck CHECK (
        (ood_score IS NULL OR ood_score >= 0.0)
        AND (ood_threshold IS NULL OR ood_threshold >= 0.0)
    );

ALTER TABLE iv_damage_prediction_outcomes
    ADD COLUMN prediction_id BIGINT NOT NULL
        REFERENCES iv_damage_predictions(id) ON DELETE RESTRICT,
    ADD CONSTRAINT iv_damage_outcome_prediction_uq UNIQUE (prediction_id),
    ADD CONSTRAINT iv_damage_outcome_value_finite_ck CHECK (
        iv_damage_is_finite(observed_response)
    );

ALTER TABLE iv_damage_model_releases
    ADD COLUMN deactivated_by TEXT,
    ADD COLUMN deactivation_reason TEXT,
    ADD COLUMN deactivation_kind TEXT,
    ADD CONSTRAINT iv_damage_release_deactivation_kind_ck CHECK (
        deactivation_kind IS NULL
        OR deactivation_kind IN ('superseded', 'rollback', 'emergency')
    ),
    ADD CONSTRAINT iv_damage_release_lifecycle_audit_ck CHECK (
        activated_at IS NOT NULL
        AND btrim(activated_by) <> ''
        AND (
            (
                active
                AND deactivated_at IS NULL
                AND deactivated_by IS NULL
                AND deactivation_reason IS NULL
                AND deactivation_kind IS NULL
            )
            OR (
                NOT active
                AND deactivated_at IS NOT NULL
                AND deactivated_at >= activated_at
                AND deactivated_by IS NOT NULL
                AND btrim(deactivated_by) <> ''
                AND deactivation_reason IS NOT NULL
                AND btrim(deactivation_reason) <> ''
                AND deactivation_kind IS NOT NULL
            )
        )
    );

-- Reject generic UPDATE/DELETE operations on append-only scientific evidence.
CREATE FUNCTION iv_damage_reject_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is not permitted', TG_TABLE_NAME, TG_OP;
END
$$;

CREATE TRIGGER iv_damage_observations_immutable
BEFORE UPDATE OR DELETE ON iv_damage_metric_observations
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_responses_immutable
BEFORE UPDATE OR DELETE ON iv_damage_response_units
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_snapshots_immutable
BEFORE UPDATE OR DELETE ON iv_damage_dataset_snapshots
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_snapshot_members_immutable
BEFORE UPDATE OR DELETE ON iv_damage_dataset_snapshot_members
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_splits_immutable
BEFORE UPDATE OR DELETE ON iv_damage_split_assignments
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_validation_immutable
BEFORE UPDATE OR DELETE ON iv_damage_validation_results
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_predictions_immutable
BEFORE UPDATE OR DELETE ON iv_damage_predictions
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

CREATE TRIGGER iv_damage_outcomes_immutable
BEFORE UPDATE OR DELETE ON iv_damage_prediction_outcomes
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_mutation();

-- Method and policy identity/configuration is frozen, while approval is a
-- deliberate, auditable, one-way lifecycle transition.
CREATE FUNCTION iv_damage_guard_method_approval()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.approved THEN
            RAISE EXCEPTION 'new extraction methods must enter as unapproved';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'iv_damage_extraction_methods is append-only';
    END IF;
    IF to_jsonb(NEW) - ARRAY['approved', 'approved_by', 'approved_at']::text[]
       IS DISTINCT FROM to_jsonb(OLD) - ARRAY['approved', 'approved_by', 'approved_at']::text[] THEN
        RAISE EXCEPTION 'extraction method identity and configuration are immutable';
    END IF;
    IF OLD.approved OR NOT NEW.approved
       OR NEW.approved_by IS NULL OR btrim(NEW.approved_by) = ''
       OR NEW.approved_at IS NULL THEN
        RAISE EXCEPTION 'method approval requires one false-to-true transition and approval audit';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_method_approval_guard
BEFORE INSERT OR UPDATE OR DELETE ON iv_damage_extraction_methods
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_method_approval();

CREATE FUNCTION iv_damage_guard_policy_approval()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.approved THEN
            RAISE EXCEPTION 'new acceptance policies must enter as unapproved';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'iv_damage_acceptance_policies is append-only';
    END IF;
    IF to_jsonb(NEW) - ARRAY['approved', 'approved_by', 'approved_at']::text[]
       IS DISTINCT FROM to_jsonb(OLD) - ARRAY['approved', 'approved_by', 'approved_at']::text[] THEN
        RAISE EXCEPTION 'acceptance policy identity and requirements are immutable';
    END IF;
    IF OLD.approved OR NOT NEW.approved
       OR NEW.approved_by IS NULL OR btrim(NEW.approved_by) = ''
       OR NEW.approved_at IS NULL THEN
        RAISE EXCEPTION 'policy approval requires one false-to-true transition and approval audit';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_policy_approval_guard
BEFORE INSERT OR UPDATE OR DELETE ON iv_damage_acceptance_policies
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_policy_approval();

-- Observation insertions must use an approved method whose metric/target
-- contract agrees with the observation unit.
CREATE FUNCTION iv_damage_validate_observation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    method_metric TEXT;
    method_approved BOOLEAN;
BEGIN
    SELECT metric_name, approved
    INTO method_metric, method_approved
    FROM iv_damage_extraction_methods
    WHERE id = NEW.extraction_method_id;

    IF NOT FOUND OR NOT method_approved THEN
        RAISE EXCEPTION 'metric observation requires an approved extraction method';
    END IF;
    IF method_metric <> NEW.metric_name THEN
        RAISE EXCEPTION 'observation metric does not match extraction method';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_observation_insert_guard
BEFORE INSERT ON iv_damage_metric_observations
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_observation();

-- Arrays cannot carry foreign keys.  Validate every frozen observation link,
-- its metric/protocol, and the conservative aggregate acquisition timestamps.
CREATE FUNCTION iv_damage_validate_response_unit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_count INTEGER;
    found_count INTEGER;
    distinct_count INTEGER;
    method_count INTEGER;
    protocol_ok BOOLEAN;
    metric_ok BOOLEAN;
    quality_ok BOOLEAN;
    usable_quality_ok BOOLEAN;
    observed_pre_time TIMESTAMPTZ;
    observed_post_time TIMESTAMPTZ;
BEGIN
    expected_count := cardinality(NEW.pre_observation_ids)
        + cardinality(NEW.post_observation_ids);

    WITH expected AS (
        SELECT id, 'pre'::text AS role FROM unnest(NEW.pre_observation_ids) AS id
        UNION ALL
        SELECT id, 'post'::text AS role FROM unnest(NEW.post_observation_ids) AS id
    )
    SELECT count(*), count(DISTINCT observation.id),
           count(DISTINCT observation.extraction_method_id),
           bool_and(observation.measurement_protocol_id = NEW.measurement_protocol_id),
           bool_and(observation.metric_name = CASE NEW.target_type
               WHEN 'delta_vth_v' THEN 'vth_v' ELSE 'rdson_mohm' END),
           bool_and(observation.quality_status <> 'invalid'),
           bool_and(observation.quality_status = 'usable'),
           max(observation.measured_at) FILTER (WHERE expected.role = 'pre'),
           min(observation.measured_at) FILTER (WHERE expected.role = 'post')
    INTO found_count, distinct_count, method_count,
         protocol_ok, metric_ok, quality_ok, usable_quality_ok,
         observed_pre_time, observed_post_time
    FROM expected
    JOIN iv_damage_metric_observations observation ON observation.id = expected.id;

    IF found_count <> expected_count OR distinct_count <> expected_count THEN
        RAISE EXCEPTION 'response observation ids must exist and be unique';
    END IF;
    IF method_count <> 1 THEN
        RAISE EXCEPTION 'response observations must share one extraction method/configuration';
    END IF;
    IF NOT protocol_ok OR NOT metric_ok OR NOT quality_ok
       OR (NEW.quality_status = 'usable' AND NOT usable_quality_ok) THEN
        RAISE EXCEPTION 'response observations violate protocol, metric, or quality contract';
    END IF;
    IF NEW.pre_measured_at IS DISTINCT FROM observed_pre_time
       OR NEW.post_measured_at IS DISTINCT FROM observed_post_time THEN
        RAISE EXCEPTION 'response timestamps must equal max(pre) and min(post) measured_at';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_response_insert_guard
BEFORE INSERT ON iv_damage_response_units
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_response_unit();

CREATE FUNCTION iv_damage_validate_validation_result()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    model_snapshot_id BIGINT;
    model_stress TEXT;
    model_target TEXT;
    response_stress TEXT;
    response_target TEXT;
    authoritative_response DOUBLE PRECISION;
BEGIN
    SELECT dataset_snapshot_id, stress_type, target_type
    INTO model_snapshot_id, model_stress, model_target
    FROM iv_damage_model_runs
    WHERE id = NEW.model_run_id;
    SELECT response.stress_type, response.target_type, response.response_value
    INTO response_stress, response_target, authoritative_response
    FROM iv_damage_response_units response
    WHERE response.id = NEW.response_unit_id;

    IF model_snapshot_id IS NULL OR response_stress IS NULL THEN
        RAISE EXCEPTION 'validation model or response unit does not exist';
    END IF;
    IF model_stress IS DISTINCT FROM response_stress
       OR model_target IS DISTINCT FROM response_target THEN
        RAISE EXCEPTION 'validation response does not match model domain';
    END IF;
    IF NEW.observed_value IS DISTINCT FROM authoritative_response THEN
        RAISE EXCEPTION 'validation observation must equal the frozen response value';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM iv_damage_split_assignments assignment
        WHERE assignment.dataset_snapshot_id = model_snapshot_id
          AND assignment.response_unit_id = NEW.response_unit_id
          AND assignment.split_scheme = NEW.split_scheme
    ) THEN
        RAISE EXCEPTION 'validation response is not assigned in the model snapshot';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_validation_insert_guard
BEFORE INSERT ON iv_damage_validation_results
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_validation_result();

-- Members may be accumulated only while a snapshot is being constructed.
-- Once any split or model exists, its frozen population is sealed.
CREATE FUNCTION iv_damage_validate_snapshot_member()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_rows INTEGER;
    current_rows INTEGER;
BEGIN
    SELECT row_count INTO expected_rows
    FROM iv_damage_dataset_snapshots
    WHERE id = NEW.dataset_snapshot_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dataset snapshot does not exist';
    END IF;
    IF EXISTS (
        SELECT 1 FROM iv_damage_split_assignments
        WHERE dataset_snapshot_id = NEW.dataset_snapshot_id
    ) OR EXISTS (
        SELECT 1 FROM iv_damage_model_runs
        WHERE dataset_snapshot_id = NEW.dataset_snapshot_id
    ) THEN
        RAISE EXCEPTION 'dataset snapshot population is already sealed';
    END IF;
    SELECT count(*) INTO current_rows
    FROM iv_damage_dataset_snapshot_members
    WHERE dataset_snapshot_id = NEW.dataset_snapshot_id;
    IF current_rows >= expected_rows THEN
        RAISE EXCEPTION 'snapshot members exceed declared row_count';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_snapshot_member_insert_guard
BEFORE INSERT ON iv_damage_dataset_snapshot_members
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_snapshot_member();

-- Requests retain immutable prospective inputs; only their terminal status can
-- advance from pending.
CREATE FUNCTION iv_damage_guard_request_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'prediction requests cannot be deleted';
    END IF;
    IF to_jsonb(NEW) - ARRAY['request_status']::text[]
       IS DISTINCT FROM to_jsonb(OLD) - ARRAY['request_status']::text[] THEN
        RAISE EXCEPTION 'prospective prediction request inputs are immutable';
    END IF;
    IF OLD.request_status <> 'pending'
       OR NEW.request_status NOT IN ('scored', 'invalid', 'cancelled') THEN
        RAISE EXCEPTION 'invalid prediction request status transition';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_request_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_prediction_requests
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_request_lifecycle();

CREATE FUNCTION iv_damage_validate_model_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    policy_stress TEXT;
    policy_target TEXT;
    policy_approved BOOLEAN;
    member_count INTEGER;
    snapshot_rows INTEGER;
    snapshot_stress TEXT;
    snapshot_target TEXT;
BEGIN
    IF NEW.release_status IN ('shadow', 'released', 'retired') THEN
        RAISE EXCEPTION 'model must enter release lifecycle as candidate, validated, or failed';
    END IF;
    SELECT stress_type, target_type, approved
    INTO policy_stress, policy_target, policy_approved
    FROM iv_damage_acceptance_policies
    WHERE id = NEW.acceptance_policy_id;
    IF policy_stress IS DISTINCT FROM NEW.stress_type
       OR policy_target IS DISTINCT FROM NEW.target_type THEN
        RAISE EXCEPTION 'model and acceptance policy domains do not match';
    END IF;

    SELECT row_count, domain_summary->>'stress_type', domain_summary->>'target_type'
    INTO snapshot_rows, snapshot_stress, snapshot_target
    FROM iv_damage_dataset_snapshots
    WHERE id = NEW.dataset_snapshot_id;
    SELECT count(*) INTO member_count
    FROM iv_damage_dataset_snapshot_members
    WHERE dataset_snapshot_id = NEW.dataset_snapshot_id;
    IF snapshot_rows IS NULL OR member_count <> snapshot_rows THEN
        RAISE EXCEPTION 'model snapshot does not contain its declared frozen population';
    END IF;
    IF snapshot_stress IS DISTINCT FROM NEW.stress_type
       OR snapshot_target IS DISTINCT FROM NEW.target_type THEN
        RAISE EXCEPTION 'model and dataset snapshot domains do not match';
    END IF;
    IF NEW.release_status IN ('validated', 'shadow') AND (
        NOT policy_approved
        OR NOT (NEW.validation_metrics @> '{"release_gate_eligible": true}'::jsonb)
    ) THEN
        RAISE EXCEPTION 'validated model requires approved policy and persisted release gate';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_model_insert_guard
BEFORE INSERT ON iv_damage_model_runs
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_model_insert();

CREATE FUNCTION iv_damage_guard_model_lifecycle()
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
        RAISE EXCEPTION 'model identity, evidence, configuration, and artifact are immutable';
    END IF;
    IF NOT (
        (OLD.release_status = 'candidate' AND NEW.release_status IN ('validated', 'shadow', 'failed'))
        OR (OLD.release_status = 'validated' AND NEW.release_status IN ('shadow', 'released', 'failed'))
        OR (OLD.release_status = 'shadow' AND NEW.release_status IN ('released', 'retired', 'failed'))
        OR (OLD.release_status = 'released' AND NEW.release_status = 'retired')
        OR (OLD.release_status = 'retired' AND NEW.release_status = 'released')
    ) THEN
        RAISE EXCEPTION 'invalid model lifecycle transition: % to %',
            OLD.release_status, NEW.release_status;
    END IF;
    IF NEW.release_status = 'released' AND (
        NEW.released_at IS NULL
        OR jsonb_typeof(NEW.released_domain->'measurement_protocol_ids')
            IS DISTINCT FROM 'array'
        OR jsonb_array_length(NEW.released_domain->'measurement_protocol_ids') = 0
        OR NEW.released_domain->>'stress_type' IS DISTINCT FROM NEW.stress_type
        OR NEW.released_domain->>'target_type' IS DISTINCT FROM NEW.target_type
    ) THEN
        RAISE EXCEPTION 'released model requires a canonical nonempty released domain';
    END IF;
    IF NEW.release_status IN ('validated', 'shadow', 'released') AND (
        NOT (NEW.validation_metrics @> '{"release_gate_eligible": true}'::jsonb)
        OR NOT EXISTS (
            SELECT 1 FROM iv_damage_acceptance_policies policy
            WHERE policy.id = NEW.acceptance_policy_id AND policy.approved
        )
    ) THEN
        RAISE EXCEPTION 'validated/released model requires approved policy and persisted release gate';
    END IF;
    IF NEW.release_status = 'retired' AND NEW.retired_at IS NULL THEN
        RAISE EXCEPTION 'retired model requires retired_at';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_model_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_model_runs
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_model_lifecycle();

-- A release event is bound to its model domain.  Deactivation is the only
-- permitted mutation and carries enough audit data for emergency shutdown.
CREATE FUNCTION iv_damage_validate_release_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    model_stress TEXT;
    model_target TEXT;
    model_status TEXT;
    policy_approved BOOLEAN;
    release_gate_eligible BOOLEAN;
BEGIN
    SELECT model.stress_type, model.target_type, model.release_status,
           policy.approved,
           model.validation_metrics @> '{"release_gate_eligible": true}'::jsonb
    INTO model_stress, model_target, model_status,
         policy_approved, release_gate_eligible
    FROM iv_damage_model_runs model
    JOIN iv_damage_acceptance_policies policy
      ON policy.id = model.acceptance_policy_id
    WHERE model.id = NEW.model_run_id;
    IF model_stress IS DISTINCT FROM NEW.stress_type
       OR model_target IS DISTINCT FROM NEW.target_type THEN
        RAISE EXCEPTION 'release and model domains do not match';
    END IF;
    IF NOT NEW.active THEN
        RAISE EXCEPTION 'release history is created by activation, then deactivated';
    END IF;
    IF model_status NOT IN ('validated', 'shadow', 'retired') THEN
        RAISE EXCEPTION 'release activation requires a validated, shadow, or rollback model';
    END IF;
    IF NOT policy_approved OR NOT release_gate_eligible THEN
        RAISE EXCEPTION 'release activation requires approved policy and persisted release gate';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_release_insert_guard
BEFORE INSERT ON iv_damage_model_releases
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_release_insert();

CREATE FUNCTION iv_damage_guard_release_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'model release history cannot be deleted';
    END IF;
    IF to_jsonb(NEW) - ARRAY[
        'active', 'deactivated_at', 'deactivated_by',
        'deactivation_reason', 'deactivation_kind'
    ]::text[] IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
        'active', 'deactivated_at', 'deactivated_by',
        'deactivation_reason', 'deactivation_kind'
    ]::text[] THEN
        RAISE EXCEPTION 'release identity and activation audit are immutable';
    END IF;
    IF NOT OLD.active OR NEW.active OR NEW.deactivated_at IS NULL
       OR NEW.deactivated_by IS NULL OR btrim(NEW.deactivated_by) = ''
       OR NEW.deactivation_reason IS NULL OR btrim(NEW.deactivation_reason) = ''
       OR NEW.deactivation_kind IS NULL THEN
        RAISE EXCEPTION 'release update must be one audited active-to-inactive transition';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_release_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_model_releases
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_release_lifecycle();

-- A persisted prediction is tied to a request/model domain and must have been
-- scored while that model had an active release.  Target-specific output
-- semantics are checked here because CHECK constraints cannot inspect request.
CREATE FUNCTION iv_damage_validate_prediction()
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
    release_gate_eligible BOOLEAN;
    release_time TIMESTAMPTZ;
BEGIN
    SELECT request.stress_type, request.target_type, request.pre_value,
           request.measurement_protocol_id, request.request_status
    INTO request_stress, request_target, request_pre, request_protocol, request_status
    FROM iv_damage_prediction_requests request
    WHERE request.id = NEW.request_id
    FOR SHARE;
    SELECT model.stress_type, model.target_type, model.release_status,
           model.released_domain, policy.approved,
           model.validation_metrics @> '{"release_gate_eligible": true}'::jsonb
    INTO model_stress, model_target, model_status, model_domain,
         policy_approved, release_gate_eligible
    FROM iv_damage_model_runs model
    JOIN iv_damage_acceptance_policies policy
      ON policy.id = model.acceptance_policy_id
    WHERE model.id = NEW.model_run_id;
    SELECT release.activated_at INTO release_time
    FROM iv_damage_model_releases release
    WHERE release.model_run_id = NEW.model_run_id
      AND release.stress_type = request_stress
      AND release.target_type = request_target
      AND release.active
    ORDER BY release.activated_at DESC, release.id DESC
    LIMIT 1
    FOR SHARE OF release;

    IF request_status IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION 'only a pending request can be scored';
    END IF;
    IF request_stress IS DISTINCT FROM model_stress
       OR request_target IS DISTINCT FROM model_target THEN
        RAISE EXCEPTION 'prediction request and model domains do not match';
    END IF;
    IF model_status <> 'released' THEN
        RAISE EXCEPTION 'prediction model is not in released lifecycle state';
    END IF;
    IF release_time IS NULL OR NEW.created_at < release_time THEN
        RAISE EXCEPTION 'prediction requires a currently active model release';
    END IF;
    IF NEW.decision_eligible AND (
        NOT policy_approved
        OR NOT release_gate_eligible
        OR
        jsonb_typeof(model_domain->'measurement_protocol_ids')
            IS DISTINCT FROM 'array'
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
             > 1e-10 * greatest(1.0, abs(NEW.predicted_post_value), abs(request_pre),
                                abs(NEW.predicted_response)))
        OR (NEW.predicted_response_lower IS NOT NULL AND NEW.predicted_post_lower IS NOT NULL
            AND abs(NEW.predicted_post_lower - request_pre - NEW.predicted_response_lower)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_post_lower), abs(request_pre),
                                   abs(NEW.predicted_response_lower)))
        OR (NEW.predicted_response_upper IS NOT NULL AND NEW.predicted_post_upper IS NOT NULL
            AND abs(NEW.predicted_post_upper - request_pre - NEW.predicted_response_upper)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_post_upper), abs(request_pre),
                                   abs(NEW.predicted_response_upper)))
    ) THEN
        RAISE EXCEPTION 'threshold-voltage response and post predictions are inconsistent';
    END IF;
    IF request_target = 'log_rdson_ratio' AND (
        (NEW.predicted_response IS NOT NULL AND NEW.predicted_post_value IS NOT NULL
         AND abs((ln(NEW.predicted_post_value) - ln(request_pre)) - NEW.predicted_response)
             > 1e-10 * greatest(1.0, abs(NEW.predicted_response)))
        OR (NEW.predicted_response_lower IS NOT NULL AND NEW.predicted_post_lower IS NOT NULL
            AND abs((ln(NEW.predicted_post_lower) - ln(request_pre))
                    - NEW.predicted_response_lower)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_response_lower)))
        OR (NEW.predicted_response_upper IS NOT NULL AND NEW.predicted_post_upper IS NOT NULL
            AND abs((ln(NEW.predicted_post_upper) - ln(request_pre))
                    - NEW.predicted_response_upper)
                > 1e-10 * greatest(1.0, abs(NEW.predicted_response_upper)))
    ) THEN
        RAISE EXCEPTION 'Rds(on) response and post predictions are inconsistent';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_prediction_insert_guard
BEFORE INSERT ON iv_damage_predictions
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_prediction();

-- Outcome chronology is based on acquisition time, never response ingestion
-- time.  An outcome is tied to the exact prediction it evaluates.
CREATE FUNCTION iv_damage_validate_outcome()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    facts RECORD;
BEGIN
    SELECT prediction.request_id AS prediction_request_id,
           prediction.created_at AS predicted_at,
           request.physical_device_key AS request_device,
           request.measurement_protocol_id AS request_protocol,
           request.stress_type AS request_stress,
           request.target_type AS request_target,
           request.stress_features AS request_features,
           response.physical_device_key AS response_device,
           response.measurement_protocol_id AS response_protocol,
           response.stress_type AS response_stress,
           response.target_type AS response_target,
           response.stress_features AS response_features,
           response.post_measured_at,
           response.response_value
    INTO facts
    FROM iv_damage_predictions prediction
    JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
    CROSS JOIN iv_damage_response_units response
    WHERE prediction.id = NEW.prediction_id
      AND response.id = NEW.response_unit_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'prediction or response unit does not exist';
    END IF;
    IF NEW.request_id IS DISTINCT FROM facts.prediction_request_id THEN
        RAISE EXCEPTION 'outcome request does not own prediction';
    END IF;
    IF facts.request_device IS DISTINCT FROM facts.response_device
       OR facts.request_protocol IS DISTINCT FROM facts.response_protocol
       OR facts.request_stress IS DISTINCT FROM facts.response_stress
       OR facts.request_target IS DISTINCT FROM facts.response_target
       OR NOT (facts.response_features @> facts.request_features) THEN
        RAISE EXCEPTION 'outcome response does not match prediction domain';
    END IF;
    IF facts.post_measured_at <= facts.predicted_at THEN
        RAISE EXCEPTION 'outcome must be acquired after the prediction';
    END IF;
    IF NEW.matched_at < facts.post_measured_at THEN
        RAISE EXCEPTION 'outcome cannot be matched before it was measured';
    END IF;
    IF NEW.observed_response IS DISTINCT FROM facts.response_value THEN
        RAISE EXCEPTION 'outcome value must equal the authoritative response unit';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_outcome_insert_guard
BEFORE INSERT ON iv_damage_prediction_outcomes
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_outcome();

-- Reconcile the canonical boundary.  Explicit domain predicates defend in
-- depth; protocol membership and activation time prevent unsupported or stale
-- predictions from becoming decision eligible.
CREATE OR REPLACE VIEW iv_damage_decision_eligible_prediction_view AS
SELECT
    prediction.*,
    request.request_key,
    request.physical_device_key,
    request.device_type,
    request.measurement_protocol_id,
    request.stress_type,
    request.target_type,
    request.pre_value,
    request.pre_uncertainty,
    request.reference_policy,
    request.stress_features,
    model.model_version,
    model.algorithm,
    model.released_domain,
    release.activated_at
FROM iv_damage_predictions prediction
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
JOIN iv_damage_model_runs model
  ON model.id = prediction.model_run_id
 AND model.stress_type = request.stress_type
 AND model.target_type = request.target_type
JOIN iv_damage_acceptance_policies policy
  ON policy.id = model.acceptance_policy_id
 AND policy.approved
JOIN iv_damage_model_releases release
  ON release.model_run_id = model.id
 AND release.stress_type = model.stress_type
 AND release.target_type = model.target_type
 AND release.active
 AND release.deactivated_at IS NULL
WHERE model.release_status = 'released'
  AND model.validation_metrics @> '{"release_gate_eligible": true}'::jsonb
  AND prediction.created_at >= release.activated_at
  AND prediction.decision_eligible
  AND prediction.evidence_status = 'decision_eligible'
  AND prediction.support_status = 'in_domain'
  AND prediction.in_domain
  AND prediction.validation_gate_passed
  AND request.reference_policy = 'same_device'
  AND jsonb_typeof(model.released_domain->'measurement_protocol_ids') = 'array'
  AND model.released_domain->'measurement_protocol_ids' ? request.measurement_protocol_id;

CREATE OR REPLACE VIEW iv_damage_prediction_monitoring_view AS
SELECT
    prediction.id AS prediction_id,
    prediction.model_run_id,
    request.id AS request_id,
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
                                              AND prediction.predicted_response_upper END
        AS interval_hit,
    prediction.created_at,
    outcome.matched_at,
    model.model_version
FROM iv_damage_predictions prediction
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
JOIN iv_damage_model_runs model ON model.id = prediction.model_run_id
LEFT JOIN iv_damage_prediction_outcomes outcome ON outcome.prediction_id = prediction.id;

CREATE OR REPLACE VIEW iv_damage_validation_summary_view AS
SELECT
    result.model_run_id,
    result.split_scheme,
    result.split_role,
    unit.stress_type,
    unit.target_type,
    unit.device_type,
    unit.ion_species,
    result.support_status,
    count(*) AS independent_units,
    count(DISTINCT unit.physical_device_key) AS physical_devices,
    count(DISTINCT unit.campaign_key) AS campaigns,
    avg(result.abs_residual) FILTER (WHERE result.abs_residual IS NOT NULL)
        AS mean_abs_error,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY result.abs_residual)
        FILTER (WHERE result.abs_residual IS NOT NULL) AS median_abs_error,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY result.abs_residual)
        FILTER (WHERE result.abs_residual IS NOT NULL) AS p90_abs_error,
    avg(result.residual) FILTER (WHERE result.residual IS NOT NULL) AS mean_bias,
    avg(result.interval_hit::integer) FILTER (WHERE result.interval_hit IS NOT NULL)
        AS interval_coverage,
    model.model_version
FROM iv_damage_validation_results result
JOIN iv_damage_response_units unit ON unit.id = result.response_unit_id
JOIN iv_damage_model_runs model ON model.id = result.model_run_id
GROUP BY result.model_run_id, result.split_scheme, result.split_role,
         unit.stress_type, unit.target_type, unit.device_type, unit.ion_species,
         result.support_status, model.model_version;

CREATE VIEW iv_damage_prediction_backlog_view AS
SELECT
    request.id AS request_id,
    request.request_key,
    request.physical_device_key,
    request.device_type,
    request.measurement_protocol_id,
    request.stress_type,
    request.target_type,
    request.request_status,
    request.requested_prediction_horizon_s,
    request.request_source,
    request.created_at,
    clock_timestamp() - request.created_at AS request_age
FROM iv_damage_prediction_requests request
WHERE request.request_status = 'pending'
  AND NOT EXISTS (
      SELECT 1 FROM iv_damage_predictions prediction
      WHERE prediction.request_id = request.id
  );

-- Bad or missing feature strings now produce NULL instead of aborting every
-- downstream dashboard/equivalence query.
CREATE OR REPLACE VIEW iv_damage_equivalence_input_view AS
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
    iv_damage_try_float8(prediction.stress_features->>'beam_energy_mev') AS beam_energy_mev,
    iv_damage_try_float8(prediction.stress_features->>'let_surface') AS let_surface,
    iv_damage_try_float8(prediction.stress_features->>'range_um') AS range_um,
    iv_damage_try_float8(prediction.stress_features->>'fluence_or_dose') AS fluence_or_dose,
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

COMMENT ON TABLE iv_damage_dataset_snapshot_members IS
    'Frozen canonical response-unit payloads. Training reads these values, not mutable live evidence.';
COMMENT ON COLUMN iv_damage_metric_observations.measured_at IS
    'Authoritative acquisition timestamp; never an ingestion/extraction timestamp.';
COMMENT ON COLUMN iv_damage_response_units.pre_measured_at IS
    'Latest authoritative acquisition time among the aggregate pre observations.';
COMMENT ON COLUMN iv_damage_response_units.post_measured_at IS
    'Earliest authoritative acquisition time among the aggregate post observations.';
COMMENT ON COLUMN iv_damage_prediction_outcomes.prediction_id IS
    'Exact immutable prediction evaluated by this prospectively acquired outcome.';
COMMENT ON VIEW iv_damage_decision_eligible_prediction_view IS
    'Canonical active-release V3 boundary with request/model/release domain, protocol, chronology, support, and validation gates.';
