-- Certified V3 lifecycle and governed full-curve prediction.
--
-- Forward-only extension of migrations 032--034.  Scalar damage prediction,
-- deterministic curve projection, and learned functional-curve prediction are
-- deliberately separate claim classes.  A release in one class never implies
-- certification of another.

-- ---------------------------------------------------------------------------
-- Authoritative acquisition and stress-session identity
-- ---------------------------------------------------------------------------

CREATE TABLE iv_damage_acquisitions (
    id BIGSERIAL PRIMARY KEY,
    acquisition_key TEXT NOT NULL UNIQUE,
    metadata_id INTEGER NOT NULL UNIQUE
        REFERENCES baselines_metadata(id) ON DELETE RESTRICT,
    physical_device_key TEXT NOT NULL,
    device_type TEXT NOT NULL,
    manufacturer TEXT,
    measurement_protocol_id TEXT NOT NULL,
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    measured_at TIMESTAMPTZ NOT NULL,
    source_file_hash TEXT NOT NULL,
    point_payload_hash TEXT NOT NULL CHECK (point_payload_hash ~ '^[0-9a-f]{64}$'),
    point_count INTEGER NOT NULL CHECK (point_count > 0),
    identity_source TEXT NOT NULL
        CHECK (identity_source IN ('metadata_exact', 'manual_review')),
    identity_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        identity_source <> 'manual_review'
        OR (reviewed_by IS NOT NULL AND btrim(reviewed_by) <> '' AND reviewed_at IS NOT NULL)
    )
);

CREATE INDEX iv_damage_acquisitions_device_idx
    ON iv_damage_acquisitions (physical_device_key, measured_at, curve_family);
CREATE INDEX iv_damage_acquisitions_protocol_idx
    ON iv_damage_acquisitions (measurement_protocol_id, curve_family);

CREATE TABLE iv_damage_acquisition_identity_reviews (
    id BIGSERIAL PRIMARY KEY,
    acquisition_id BIGINT NOT NULL REFERENCES iv_damage_acquisitions(id) ON DELETE RESTRICT,
    metadata_device_id TEXT,
    asserted_physical_device_key TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('accepted', 'rejected')),
    reason TEXT NOT NULL,
    reviewed_by TEXT NOT NULL,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_stress_sessions (
    id BIGSERIAL PRIMARY KEY,
    stress_session_key TEXT NOT NULL UNIQUE,
    physical_device_key TEXT NOT NULL,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    campaign_key TEXT NOT NULL,
    run_key TEXT NOT NULL,
    stress_condition_key TEXT NOT NULL,
    stress_features JSONB NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    identity_source TEXT NOT NULL
        CHECK (identity_source IN ('campaign_registry', 'manual_review')),
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (physical_device_key, campaign_key, run_key, stress_condition_key),
    CHECK (started_at IS NULL OR ended_at IS NULL OR ended_at >= started_at),
    CHECK (
        identity_source <> 'manual_review'
        OR (reviewed_by IS NOT NULL AND btrim(reviewed_by) <> '' AND reviewed_at IS NOT NULL)
    )
);

ALTER TABLE iv_damage_metric_observations
    ADD COLUMN acquisition_id BIGINT REFERENCES iv_damage_acquisitions(id) ON DELETE RESTRICT;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM iv_damage_metric_observations WHERE acquisition_id IS NULL) THEN
        RAISE EXCEPTION USING MESSAGE =
            'migration 035 refuses unbound metric observations; register authoritative acquisitions first';
    END IF;
END
$$;

ALTER TABLE iv_damage_metric_observations
    ALTER COLUMN acquisition_id SET NOT NULL,
    ADD CONSTRAINT iv_damage_observation_acquisition_uq
        UNIQUE (acquisition_id, extraction_method_id, metric_name);

ALTER TABLE iv_damage_response_units
    ADD COLUMN stress_session_id BIGINT REFERENCES iv_damage_stress_sessions(id) ON DELETE RESTRICT;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM iv_damage_response_units WHERE stress_session_id IS NULL) THEN
        RAISE EXCEPTION USING MESSAGE =
            'migration 035 refuses response units without an authoritative stress session';
    END IF;
END
$$;

ALTER TABLE iv_damage_response_units
    ALTER COLUMN stress_session_id SET NOT NULL,
    ADD CONSTRAINT iv_damage_response_session_uq
        UNIQUE (stress_session_id, target_type);

CREATE TABLE iv_damage_response_observations (
    response_unit_id BIGINT NOT NULL
        REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    observation_id BIGINT NOT NULL
        REFERENCES iv_damage_metric_observations(id) ON DELETE RESTRICT,
    observation_role TEXT NOT NULL CHECK (observation_role IN ('pre', 'post')),
    PRIMARY KEY (response_unit_id, observation_id)
);

CREATE FUNCTION iv_damage_validate_observation_acquisition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    acquisition iv_damage_acquisitions%ROWTYPE;
BEGIN
    SELECT * INTO acquisition FROM iv_damage_acquisitions WHERE id = NEW.acquisition_id;
    IF NOT FOUND
       OR acquisition.metadata_id <> NEW.metadata_id
       OR acquisition.measurement_protocol_id <> NEW.measurement_protocol_id
       OR acquisition.measured_at <> NEW.measured_at THEN
        RAISE EXCEPTION USING MESSAGE =
            'metric observation identity/protocol/time must match its authoritative acquisition';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_observation_acquisition_guard
BEFORE INSERT ON iv_damage_metric_observations
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_observation_acquisition();

CREATE FUNCTION iv_damage_validate_response_observation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    response_device TEXT;
    response_protocol TEXT;
    observation_device TEXT;
    observation_protocol TEXT;
BEGIN
    SELECT response.physical_device_key, response.measurement_protocol_id
      INTO response_device, response_protocol
      FROM iv_damage_response_units response WHERE response.id = NEW.response_unit_id;
    SELECT acquisition.physical_device_key, acquisition.measurement_protocol_id
      INTO observation_device, observation_protocol
      FROM iv_damage_metric_observations observation
      JOIN iv_damage_acquisitions acquisition ON acquisition.id = observation.acquisition_id
     WHERE observation.id = NEW.observation_id;
    IF response_device IS NULL OR observation_device IS NULL
       OR response_device <> observation_device
       OR response_protocol <> observation_protocol THEN
        RAISE EXCEPTION USING MESSAGE =
            'response observation must match authoritative device and protocol';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_response_observation_guard
BEFORE INSERT ON iv_damage_response_observations
FOR EACH ROW EXECUTE FUNCTION iv_damage_validate_response_observation();

-- ---------------------------------------------------------------------------
-- Development, sealed external certification, and shadow promotion
-- ---------------------------------------------------------------------------

ALTER TABLE iv_damage_split_assignments
    DROP CONSTRAINT iv_damage_split_assignments_split_role_check;
ALTER TABLE iv_damage_split_assignments
    ADD CONSTRAINT iv_damage_split_assignments_split_role_check
    CHECK (split_role IN ('train', 'calibration', 'external_test', 'grouped_test'));

ALTER TABLE iv_damage_validation_results
    DROP CONSTRAINT iv_damage_validation_results_split_role_check;
ALTER TABLE iv_damage_validation_results
    ADD CONSTRAINT iv_damage_validation_results_split_role_check
    CHECK (split_role IN ('train', 'calibration', 'external_test', 'grouped_test')),
    ADD COLUMN evaluation_kind TEXT NOT NULL DEFAULT 'development_cv'
        CHECK (evaluation_kind IN (
            'development_cv', 'development_calibration', 'external_certification',
            'prospective_shadow', 'prospective_released'
        ));

UPDATE iv_damage_split_assignments
SET split_role = 'grouped_test'
WHERE split_scheme <> 'frozen_release' AND split_role = 'train';

CREATE TABLE iv_damage_model_selections (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    dataset_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    selection_protocol JSONB NOT NULL,
    selected_by TEXT NOT NULL,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dataset_snapshot_id, stress_type, target_type)
);

CREATE TABLE iv_damage_external_certifications (
    id BIGSERIAL PRIMARY KEY,
    selection_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_model_selections(id) ON DELETE RESTRICT,
    model_run_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    dataset_snapshot_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    evaluation_protocol JSONB NOT NULL,
    metrics JSONB NOT NULL,
    subgroup_metrics JSONB NOT NULL,
    gate_checks JSONB NOT NULL,
    passed BOOLEAN NOT NULL,
    certified_by TEXT NOT NULL,
    certified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_model_deployments (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    deployment_mode TEXT NOT NULL CHECK (deployment_mode IN ('shadow', 'decision')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    activated_by TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deactivated_by TEXT,
    deactivated_at TIMESTAMPTZ,
    deactivation_reason TEXT,
    CHECK (active OR (deactivated_by IS NOT NULL AND deactivated_at IS NOT NULL))
);

CREATE UNIQUE INDEX iv_damage_active_deployment_uq
    ON iv_damage_model_deployments (stress_type, target_type, deployment_mode)
    WHERE active;

CREATE TABLE iv_damage_monitoring_assessments (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    deployment_id BIGINT NOT NULL REFERENCES iv_damage_model_deployments(id) ON DELETE RESTRICT,
    assessment_kind TEXT NOT NULL CHECK (assessment_kind IN ('shadow_promotion', 'released_monitoring')),
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

CREATE FUNCTION iv_damage_certification_lifecycle_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION USING MESSAGE = 'selection and certification records are immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_selection_immutable
BEFORE UPDATE OR DELETE ON iv_damage_model_selections
FOR EACH ROW EXECUTE FUNCTION iv_damage_certification_lifecycle_guard();
CREATE TRIGGER iv_damage_certification_immutable
BEFORE UPDATE OR DELETE ON iv_damage_external_certifications
FOR EACH ROW EXECUTE FUNCTION iv_damage_certification_lifecycle_guard();

-- ---------------------------------------------------------------------------
-- Immutable measured curves and paired curve truth
-- ---------------------------------------------------------------------------

CREATE TABLE iv_damage_curve_snapshots (
    id BIGSERIAL PRIMARY KEY,
    curve_snapshot_key TEXT NOT NULL UNIQUE,
    acquisition_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_acquisitions(id) ON DELETE RESTRICT,
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    measurement_protocol_id TEXT NOT NULL,
    x_unit TEXT NOT NULL CHECK (x_unit = 'V'),
    current_unit TEXT NOT NULL CHECK (current_unit = 'A'),
    point_count INTEGER NOT NULL CHECK (point_count >= 3),
    point_payload_hash TEXT NOT NULL CHECK (point_payload_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_curve_snapshot_points (
    curve_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_snapshots(id) ON DELETE RESTRICT,
    point_index INTEGER NOT NULL CHECK (point_index >= 0),
    x_value DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(x_value)),
    i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(i_drain_a)),
    PRIMARY KEY (curve_snapshot_id, point_index),
    UNIQUE (curve_snapshot_id, x_value)
);

CREATE TABLE iv_damage_curve_response_pairs (
    id BIGSERIAL PRIMARY KEY,
    pair_key TEXT NOT NULL UNIQUE,
    response_unit_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    pre_curve_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_snapshots(id) ON DELETE RESTRICT,
    post_curve_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_snapshots(id) ON DELETE RESTRICT,
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    measurement_protocol_id TEXT NOT NULL,
    quality_status TEXT NOT NULL CHECK (quality_status IN ('usable', 'screening_only', 'invalid')),
    quality_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (pre_curve_snapshot_id <> post_curve_snapshot_id)
);

CREATE TABLE iv_damage_curve_snapshot_members (
    dataset_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    curve_response_pair_id BIGINT NOT NULL
        REFERENCES iv_damage_curve_response_pairs(id) ON DELETE RESTRICT,
    response_unit_id BIGINT NOT NULL
        REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    frozen_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (dataset_snapshot_id, curve_response_pair_id),
    UNIQUE (dataset_snapshot_id, response_unit_id)
);

CREATE FUNCTION iv_damage_curve_pair_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    response_device TEXT;
    response_protocol TEXT;
    pre_device TEXT;
    post_device TEXT;
    pre_protocol TEXT;
    post_protocol TEXT;
    pre_family TEXT;
    post_family TEXT;
    pre_time TIMESTAMPTZ;
    post_time TIMESTAMPTZ;
BEGIN
    SELECT physical_device_key, measurement_protocol_id
      INTO response_device, response_protocol
      FROM iv_damage_response_units WHERE id = NEW.response_unit_id;
    SELECT acquisition.physical_device_key, curve.measurement_protocol_id,
           curve.curve_family, acquisition.measured_at
      INTO pre_device, pre_protocol, pre_family, pre_time
      FROM iv_damage_curve_snapshots curve
      JOIN iv_damage_acquisitions acquisition ON acquisition.id = curve.acquisition_id
     WHERE curve.id = NEW.pre_curve_snapshot_id;
    SELECT acquisition.physical_device_key, curve.measurement_protocol_id,
           curve.curve_family, acquisition.measured_at
      INTO post_device, post_protocol, post_family, post_time
      FROM iv_damage_curve_snapshots curve
      JOIN iv_damage_acquisitions acquisition ON acquisition.id = curve.acquisition_id
     WHERE curve.id = NEW.post_curve_snapshot_id;
    IF response_device IS NULL OR response_device <> pre_device OR pre_device <> post_device
       OR response_protocol <> pre_protocol OR pre_protocol <> post_protocol
       OR NEW.measurement_protocol_id <> pre_protocol
       OR NEW.curve_family <> pre_family OR pre_family <> post_family
       OR post_time <= pre_time THEN
        RAISE EXCEPTION USING MESSAGE =
            'curve pair must match response device/protocol/family and authoritative chronology';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_curve_pair_identity_guard
BEFORE INSERT ON iv_damage_curve_response_pairs
FOR EACH ROW EXECUTE FUNCTION iv_damage_curve_pair_guard();

CREATE TRIGGER iv_damage_acquisition_immutable
BEFORE UPDATE OR DELETE ON iv_damage_acquisitions
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_evidence_mutation();
CREATE TRIGGER iv_damage_curve_snapshot_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_snapshots
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_evidence_mutation();
CREATE TRIGGER iv_damage_curve_point_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_snapshot_points
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_evidence_mutation();
CREATE TRIGGER iv_damage_curve_pair_immutable
BEFORE UPDATE OR DELETE ON iv_damage_curve_response_pairs
FOR EACH ROW EXECUTE FUNCTION iv_damage_reject_evidence_mutation();

-- ---------------------------------------------------------------------------
-- Derived scalar-to-curve projections
-- ---------------------------------------------------------------------------

CREATE TABLE iv_damage_curve_projection_methods (
    id BIGSERIAL PRIMARY KEY,
    method_version TEXT NOT NULL UNIQUE,
    projection_kind TEXT NOT NULL
        CHECK (projection_kind IN ('rigid_vth_shift', 'linear_rdson_scale')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    configuration JSONB NOT NULL,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (projection_kind = 'rigid_vth_shift' AND target_type = 'delta_vth_v' AND curve_family = 'IdVg')
        OR
        (projection_kind = 'linear_rdson_scale' AND target_type = 'log_rdson_ratio' AND curve_family = 'IdVd')
    ),
    CHECK (NOT approved OR (approved_by IS NOT NULL AND approved_at IS NOT NULL))
);

CREATE TABLE iv_damage_curve_projections (
    id BIGSERIAL PRIMARY KEY,
    prediction_id BIGINT NOT NULL REFERENCES iv_damage_predictions(id) ON DELETE RESTRICT,
    pre_curve_snapshot_id BIGINT NOT NULL REFERENCES iv_damage_curve_snapshots(id) ON DELETE RESTRICT,
    projection_method_id BIGINT NOT NULL REFERENCES iv_damage_curve_projection_methods(id) ON DELETE RESTRICT,
    projection_status TEXT NOT NULL CHECK (projection_status IN ('projected', 'abstained', 'invalid')),
    evidence_status TEXT NOT NULL CHECK (evidence_status IN (
        'decision_eligible', 'screening_only', 'out_of_domain', 'insufficient_evidence', 'invalid_input'
    )),
    decision_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (prediction_id, pre_curve_snapshot_id, projection_method_id),
    CHECK (NOT decision_eligible OR (projection_status = 'projected' AND evidence_status = 'decision_eligible'))
);

CREATE TABLE iv_damage_curve_projection_points (
    curve_projection_id BIGINT NOT NULL REFERENCES iv_damage_curve_projections(id) ON DELETE RESTRICT,
    point_index INTEGER NOT NULL,
    x_value_v DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(x_value_v)),
    pre_i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(pre_i_drain_a)),
    predicted_i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(predicted_i_drain_a)),
    predicted_lower_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(predicted_lower_a)),
    predicted_upper_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(predicted_upper_a)),
    PRIMARY KEY (curve_projection_id, point_index),
    CHECK (predicted_lower_a <= predicted_i_drain_a AND predicted_i_drain_a <= predicted_upper_a)
);

-- ---------------------------------------------------------------------------
-- True functional full-curve model registry and predictions
-- ---------------------------------------------------------------------------

CREATE TABLE iv_damage_curve_model_runs (
    id BIGSERIAL PRIMARY KEY,
    model_version TEXT NOT NULL UNIQUE,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    measurement_protocol_id TEXT NOT NULL,
    dataset_snapshot_id BIGINT NOT NULL REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    acceptance_policy_id BIGINT NOT NULL REFERENCES iv_damage_acceptance_policies(id) ON DELETE RESTRICT,
    algorithm TEXT NOT NULL,
    grid_spec JSONB NOT NULL,
    model_config JSONB NOT NULL,
    released_domain JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    artifact_path TEXT NOT NULL,
    artifact_checksum TEXT NOT NULL,
    code_sha TEXT NOT NULL,
    environment_fingerprint JSONB NOT NULL,
    release_status TEXT NOT NULL DEFAULT 'candidate'
        CHECK (release_status IN ('candidate', 'selected', 'validated', 'shadow', 'released', 'retired', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    validated_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ
);

CREATE TABLE iv_damage_curve_validation_results (
    id BIGSERIAL PRIMARY KEY,
    curve_model_run_id BIGINT NOT NULL REFERENCES iv_damage_curve_model_runs(id) ON DELETE RESTRICT,
    curve_response_pair_id BIGINT NOT NULL REFERENCES iv_damage_curve_response_pairs(id) ON DELETE RESTRICT,
    split_scheme TEXT NOT NULL,
    split_role TEXT NOT NULL CHECK (split_role IN ('grouped_test', 'external_test')),
    evaluation_kind TEXT NOT NULL CHECK (evaluation_kind IN ('development_cv', 'external_certification')),
    physical_device_key TEXT NOT NULL,
    point_count INTEGER NOT NULL CHECK (point_count > 0),
    mae_a DOUBLE PRECISION,
    max_abs_error_a DOUBLE PRECISION,
    normalized_rmse DOUBLE PRECISION,
    simultaneous_band_hit BOOLEAN,
    support_status TEXT NOT NULL CHECK (support_status IN ('in_domain', 'out_of_domain', 'insufficient_evidence', 'invalid_input')),
    ood_score DOUBLE PRECISION,
    reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (curve_model_run_id, curve_response_pair_id, split_scheme)
);

CREATE TABLE iv_damage_curve_model_selections (
    id BIGSERIAL PRIMARY KEY,
    curve_model_run_id BIGINT NOT NULL UNIQUE REFERENCES iv_damage_curve_model_runs(id) ON DELETE RESTRICT,
    dataset_snapshot_id BIGINT NOT NULL REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    selection_protocol JSONB NOT NULL,
    selected_by TEXT NOT NULL,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dataset_snapshot_id, curve_model_run_id)
);

CREATE TABLE iv_damage_curve_external_certifications (
    id BIGSERIAL PRIMARY KEY,
    selection_id BIGINT NOT NULL UNIQUE REFERENCES iv_damage_curve_model_selections(id) ON DELETE RESTRICT,
    curve_model_run_id BIGINT NOT NULL UNIQUE REFERENCES iv_damage_curve_model_runs(id) ON DELETE RESTRICT,
    dataset_snapshot_id BIGINT NOT NULL UNIQUE REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    metrics JSONB NOT NULL,
    gate_checks JSONB NOT NULL,
    passed BOOLEAN NOT NULL,
    certified_by TEXT NOT NULL,
    certified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_curve_model_deployments (
    id BIGSERIAL PRIMARY KEY,
    curve_model_run_id BIGINT NOT NULL REFERENCES iv_damage_curve_model_runs(id) ON DELETE RESTRICT,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    measurement_protocol_id TEXT NOT NULL,
    deployment_mode TEXT NOT NULL CHECK (deployment_mode IN ('shadow', 'decision')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    activated_by TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deactivated_by TEXT,
    deactivated_at TIMESTAMPTZ,
    deactivation_reason TEXT
);

CREATE UNIQUE INDEX iv_damage_curve_active_deployment_uq
    ON iv_damage_curve_model_deployments (
        stress_type, curve_family, measurement_protocol_id, deployment_mode
    ) WHERE active;

CREATE TABLE iv_damage_curve_prediction_requests (
    id BIGSERIAL PRIMARY KEY,
    request_key TEXT NOT NULL UNIQUE,
    pre_curve_snapshot_id BIGINT NOT NULL REFERENCES iv_damage_curve_snapshots(id) ON DELETE RESTRICT,
    physical_device_key TEXT NOT NULL,
    device_type TEXT NOT NULL,
    manufacturer TEXT,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    measurement_protocol_id TEXT NOT NULL,
    stress_features JSONB NOT NULL,
    prediction_horizon_s DOUBLE PRECISION,
    request_source TEXT NOT NULL,
    requested_by TEXT,
    request_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (request_status IN ('pending', 'scored', 'invalid', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_curve_predictions (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL REFERENCES iv_damage_curve_prediction_requests(id) ON DELETE RESTRICT,
    curve_model_run_id BIGINT NOT NULL REFERENCES iv_damage_curve_model_runs(id) ON DELETE RESTRICT,
    deployment_mode TEXT NOT NULL CHECK (deployment_mode IN ('shadow', 'decision')),
    support_status TEXT NOT NULL CHECK (support_status IN ('in_domain', 'out_of_domain', 'insufficient_evidence', 'invalid_input')),
    evidence_status TEXT NOT NULL CHECK (evidence_status IN ('decision_eligible', 'screening_only', 'out_of_domain', 'insufficient_evidence', 'invalid_input')),
    in_domain BOOLEAN NOT NULL,
    certification_gate_passed BOOLEAN NOT NULL,
    decision_eligible BOOLEAN NOT NULL,
    ood_score DOUBLE PRECISION,
    ood_threshold DOUBLE PRECISION,
    reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (request_id, curve_model_run_id),
    CHECK (
        NOT decision_eligible OR (
            deployment_mode = 'decision' AND support_status = 'in_domain'
            AND evidence_status = 'decision_eligible' AND in_domain
            AND certification_gate_passed
        )
    )
);

CREATE TABLE iv_damage_curve_prediction_points (
    curve_prediction_id BIGINT NOT NULL REFERENCES iv_damage_curve_predictions(id) ON DELETE RESTRICT,
    point_index INTEGER NOT NULL,
    x_value_v DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(x_value_v)),
    pre_i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(pre_i_drain_a)),
    predicted_i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(predicted_i_drain_a)),
    predicted_lower_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(predicted_lower_a)),
    predicted_upper_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(predicted_upper_a)),
    PRIMARY KEY (curve_prediction_id, point_index),
    CHECK (predicted_lower_a <= predicted_i_drain_a AND predicted_i_drain_a <= predicted_upper_a)
);

CREATE TABLE iv_damage_curve_prediction_outcomes (
    id BIGSERIAL PRIMARY KEY,
    curve_prediction_id BIGINT NOT NULL UNIQUE REFERENCES iv_damage_curve_predictions(id) ON DELETE RESTRICT,
    post_curve_snapshot_id BIGINT NOT NULL REFERENCES iv_damage_curve_snapshots(id) ON DELETE RESTRICT,
    mae_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(mae_a) AND mae_a >= 0),
    max_abs_error_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(max_abs_error_a) AND max_abs_error_a >= 0),
    simultaneous_band_hit BOOLEAN NOT NULL,
    match_method TEXT NOT NULL,
    reviewed_by TEXT NOT NULL,
    matched_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ---------------------------------------------------------------------------
-- Dashboard contracts: explicit units, gates, certification, and time windows
-- ---------------------------------------------------------------------------

CREATE VIEW iv_damage_release_gate_check_view AS
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
    ) AS active_decision_release
FROM iv_damage_model_runs model
JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
LEFT JOIN iv_damage_model_selections selection ON selection.model_run_id = model.id
LEFT JOIN iv_damage_external_certifications certification ON certification.model_run_id = model.id;

CREATE VIEW iv_damage_temporal_monitoring_view AS
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
        FILTER (WHERE outcome.id IS NOT NULL) AS interval_coverage
FROM iv_damage_predictions prediction
JOIN iv_damage_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
LEFT JOIN iv_damage_prediction_outcomes outcome ON outcome.prediction_id = prediction.id
GROUP BY date_trunc('week', prediction.created_at), prediction.model_run_id,
         model.model_version, request.stress_type, request.target_type;

CREATE VIEW iv_damage_curve_prediction_view AS
SELECT
    prediction.id AS curve_prediction_id,
    request.request_key,
    model.model_version,
    request.stress_type,
    request.curve_family,
    request.measurement_protocol_id,
    prediction.deployment_mode,
    prediction.support_status,
    prediction.evidence_status,
    prediction.decision_eligible,
    prediction.ood_score,
    prediction.ood_threshold,
    point.point_index,
    point.x_value_v,
    point.pre_i_drain_a,
    point.predicted_i_drain_a,
    point.predicted_lower_a,
    point.predicted_upper_a,
    prediction.created_at
FROM iv_damage_curve_predictions prediction
JOIN iv_damage_curve_prediction_requests request ON request.id = prediction.request_id
JOIN iv_damage_curve_model_runs model ON model.id = prediction.curve_model_run_id
JOIN iv_damage_curve_prediction_points point ON point.curve_prediction_id = prediction.id;

CREATE VIEW iv_damage_curve_model_card_view AS
SELECT
    model.id,
    model.model_version,
    model.stress_type,
    model.curve_family,
    model.measurement_protocol_id,
    model.algorithm,
    model.release_status,
    model.validation_metrics,
    certification.passed AS external_certification_passed,
    certification.certified_at,
    model.created_at,
    model.validated_at,
    model.released_at
FROM iv_damage_curve_model_runs model
LEFT JOIN iv_damage_curve_external_certifications certification
  ON certification.curve_model_run_id = model.id;

COMMENT ON TABLE iv_damage_external_certifications IS
    'One-time sealed external certification. UNIQUE(dataset_snapshot_id) prevents holdout reuse.';
COMMENT ON TABLE iv_damage_curve_model_runs IS
    'Independent functional full-curve claim; never implied by scalar model release or deterministic projection.';
COMMENT ON VIEW iv_damage_curve_prediction_view IS
    'Long-form ampere-valued functional curve predictions for protocol-specific dashboard overlays.';
