-- V3 prospective parametric-damage prediction foundation.
-- Forward migration: append-only operational tables, immutable after apply.
-- V2 iv_physical_* tables remain legacy/exploratory and are not mutated here.

CREATE TABLE iv_damage_extraction_methods (
    id BIGSERIAL PRIMARY KEY,
    method_version TEXT NOT NULL,
    config_version TEXT NOT NULL,
    metric_name TEXT NOT NULL CHECK (metric_name IN ('vth_v', 'rdson_mohm')),
    target_type TEXT NOT NULL
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    configuration JSONB NOT NULL,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (method_version, config_version, metric_name)
);

CREATE TABLE iv_damage_metric_observations (
    id BIGSERIAL PRIMARY KEY,
    observation_key TEXT NOT NULL UNIQUE,
    metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE RESTRICT,
    extraction_method_id BIGINT NOT NULL
        REFERENCES iv_damage_extraction_methods(id) ON DELETE RESTRICT,
    measurement_protocol_id TEXT NOT NULL,
    metric_name TEXT NOT NULL CHECK (metric_name IN ('vth_v', 'rdson_mohm')),
    value DOUBLE PRECISION,
    unit TEXT NOT NULL CHECK (unit IN ('V', 'mohm')),
    uncertainty DOUBLE PRECISION CHECK (uncertainty IS NULL OR uncertainty >= 0.0),
    accepted_point_count INTEGER NOT NULL DEFAULT 0 CHECK (accepted_point_count >= 0),
    replicate_group_key TEXT NOT NULL,
    quality_status TEXT NOT NULL
        CHECK (quality_status IN ('usable', 'screening_only', 'invalid')),
    quality_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (metadata_id, extraction_method_id, metric_name)
);

CREATE INDEX iv_damage_metric_observations_replicate_idx
    ON iv_damage_metric_observations (replicate_group_key, metric_name);
CREATE INDEX iv_damage_metric_observations_quality_idx
    ON iv_damage_metric_observations (quality_status, metric_name);

CREATE TABLE iv_damage_response_units (
    id BIGSERIAL PRIMARY KEY,
    unit_key TEXT NOT NULL UNIQUE,
    physical_device_key TEXT NOT NULL,
    stress_session_key TEXT NOT NULL,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    device_type TEXT NOT NULL,
    manufacturer TEXT,
    measurement_protocol_id TEXT NOT NULL,
    campaign_key TEXT NOT NULL,
    run_key TEXT NOT NULL,
    ion_species TEXT,
    pre_observation_ids BIGINT[] NOT NULL,
    post_observation_ids BIGINT[] NOT NULL,
    pre_value DOUBLE PRECISION NOT NULL,
    pre_uncertainty DOUBLE PRECISION CHECK (pre_uncertainty IS NULL OR pre_uncertainty >= 0.0),
    post_value DOUBLE PRECISION NOT NULL,
    post_uncertainty DOUBLE PRECISION CHECK (post_uncertainty IS NULL OR post_uncertainty >= 0.0),
    response_value DOUBLE PRECISION NOT NULL,
    response_uncertainty DOUBLE PRECISION
        CHECK (response_uncertainty IS NULL OR response_uncertainty >= 0.0),
    pre_replicate_count INTEGER NOT NULL CHECK (pre_replicate_count > 0),
    post_replicate_count INTEGER NOT NULL CHECK (post_replicate_count > 0),
    reference_policy TEXT NOT NULL
        CHECK (reference_policy IN ('same_device', 'library_screening')),
    baseline_reference_group_key TEXT,
    stress_features JSONB NOT NULL,
    required_features_complete BOOLEAN NOT NULL DEFAULT FALSE,
    quality_status TEXT NOT NULL
        CHECK (quality_status IN ('usable', 'screening_only', 'invalid')),
    quality_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (physical_device_key, stress_session_key, target_type)
);

CREATE INDEX iv_damage_response_units_domain_idx
    ON iv_damage_response_units (stress_type, target_type, device_type, ion_species);
CREATE INDEX iv_damage_response_units_campaign_idx
    ON iv_damage_response_units (campaign_key, run_key, physical_device_key);
CREATE INDEX iv_damage_response_units_quality_idx
    ON iv_damage_response_units (quality_status, required_features_complete);

CREATE TABLE iv_damage_acceptance_policies (
    id BIGSERIAL PRIMARY KEY,
    policy_version TEXT NOT NULL UNIQUE,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    requirements JSONB NOT NULL,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (NOT approved OR (approved_by IS NOT NULL AND approved_at IS NOT NULL))
);

CREATE TABLE iv_damage_dataset_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_version TEXT NOT NULL UNIQUE,
    snapshot_hash TEXT NOT NULL UNIQUE,
    extraction_method_versions JSONB NOT NULL,
    source_query TEXT NOT NULL,
    source_code_sha TEXT NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    independent_group_count INTEGER NOT NULL CHECK (independent_group_count >= 0),
    domain_summary JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_split_assignments (
    id BIGSERIAL PRIMARY KEY,
    dataset_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    response_unit_id BIGINT NOT NULL
        REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    split_scheme TEXT NOT NULL,
    fold_number INTEGER,
    split_role TEXT NOT NULL CHECK (split_role IN ('train', 'calibration', 'external_test')),
    group_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dataset_snapshot_id, response_unit_id, split_scheme)
);

CREATE INDEX iv_damage_split_assignments_group_idx
    ON iv_damage_split_assignments (dataset_snapshot_id, split_scheme, group_key);

CREATE TABLE iv_damage_model_runs (
    id BIGSERIAL PRIMARY KEY,
    model_version TEXT NOT NULL UNIQUE,
    model_name TEXT NOT NULL,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    dataset_snapshot_id BIGINT NOT NULL
        REFERENCES iv_damage_dataset_snapshots(id) ON DELETE RESTRICT,
    acceptance_policy_id BIGINT NOT NULL
        REFERENCES iv_damage_acceptance_policies(id) ON DELETE RESTRICT,
    algorithm TEXT NOT NULL,
    feature_schema JSONB NOT NULL,
    model_config JSONB NOT NULL,
    released_domain JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    code_sha TEXT NOT NULL,
    environment_fingerprint JSONB NOT NULL,
    artifact_path TEXT NOT NULL,
    artifact_checksum TEXT NOT NULL,
    release_status TEXT NOT NULL DEFAULT 'candidate'
        CHECK (release_status IN ('candidate', 'validated', 'shadow', 'released', 'retired', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    validated_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,
    CHECK (release_status <> 'released' OR released_at IS NOT NULL)
);

CREATE INDEX iv_damage_model_runs_domain_idx
    ON iv_damage_model_runs (stress_type, target_type, release_status, created_at DESC);

CREATE TABLE iv_damage_validation_results (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    response_unit_id BIGINT NOT NULL REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    split_scheme TEXT NOT NULL,
    fold_number INTEGER,
    split_role TEXT NOT NULL CHECK (split_role IN ('train', 'calibration', 'external_test')),
    group_key TEXT NOT NULL,
    observed_value DOUBLE PRECISION NOT NULL,
    predicted_value DOUBLE PRECISION,
    predicted_lower DOUBLE PRECISION,
    predicted_upper DOUBLE PRECISION,
    baseline_predictions JSONB NOT NULL DEFAULT '{}'::jsonb,
    residual DOUBLE PRECISION,
    abs_residual DOUBLE PRECISION,
    interval_hit BOOLEAN,
    support_status TEXT NOT NULL
        CHECK (support_status IN ('in_domain', 'out_of_domain', 'insufficient_evidence', 'invalid_input')),
    ood_score DOUBLE PRECISION,
    support_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_run_id, response_unit_id, split_scheme)
);

CREATE INDEX iv_damage_validation_results_model_idx
    ON iv_damage_validation_results (model_run_id, split_scheme, split_role, support_status);

CREATE TABLE iv_damage_prediction_requests (
    id BIGSERIAL PRIMARY KEY,
    request_key TEXT NOT NULL UNIQUE,
    physical_device_key TEXT NOT NULL,
    device_type TEXT NOT NULL,
    manufacturer TEXT,
    measurement_protocol_id TEXT NOT NULL,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    pre_value DOUBLE PRECISION NOT NULL,
    pre_uncertainty DOUBLE PRECISION CHECK (pre_uncertainty IS NULL OR pre_uncertainty >= 0.0),
    reference_policy TEXT NOT NULL
        CHECK (reference_policy IN ('same_device', 'library_screening')),
    stress_features JSONB NOT NULL,
    requested_prediction_horizon_s DOUBLE PRECISION
        CHECK (requested_prediction_horizon_s IS NULL OR requested_prediction_horizon_s >= 0.0),
    request_source TEXT NOT NULL,
    requested_by TEXT,
    request_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (request_status IN ('pending', 'scored', 'invalid', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX iv_damage_prediction_requests_pending_idx
    ON iv_damage_prediction_requests (request_status, stress_type, target_type, created_at);

CREATE TABLE iv_damage_predictions (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL REFERENCES iv_damage_prediction_requests(id) ON DELETE RESTRICT,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    predicted_response DOUBLE PRECISION,
    predicted_response_lower DOUBLE PRECISION,
    predicted_response_upper DOUBLE PRECISION,
    predicted_post_value DOUBLE PRECISION,
    predicted_post_lower DOUBLE PRECISION,
    predicted_post_upper DOUBLE PRECISION,
    support_status TEXT NOT NULL
        CHECK (support_status IN ('in_domain', 'out_of_domain', 'insufficient_evidence', 'invalid_input')),
    evidence_status TEXT NOT NULL
        CHECK (evidence_status IN ('decision_eligible', 'screening_only', 'out_of_domain', 'insufficient_evidence', 'invalid_input')),
    in_domain BOOLEAN NOT NULL DEFAULT FALSE,
    validation_gate_passed BOOLEAN NOT NULL DEFAULT FALSE,
    decision_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    ood_score DOUBLE PRECISION,
    ood_threshold DOUBLE PRECISION,
    reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    feature_completeness JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (request_id, model_run_id),
    CHECK (
        NOT decision_eligible OR (
            evidence_status = 'decision_eligible'
            AND support_status = 'in_domain'
            AND in_domain
            AND validation_gate_passed
        )
    )
);

CREATE TABLE iv_damage_prediction_outcomes (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_prediction_requests(id) ON DELETE RESTRICT,
    response_unit_id BIGINT NOT NULL UNIQUE
        REFERENCES iv_damage_response_units(id) ON DELETE RESTRICT,
    observed_response DOUBLE PRECISION NOT NULL,
    matched_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    match_method TEXT NOT NULL,
    reviewed_by TEXT,
    review_notes TEXT
);

CREATE TABLE iv_damage_model_releases (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_model_runs(id) ON DELETE RESTRICT,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    target_type TEXT NOT NULL
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    activated_at TIMESTAMPTZ,
    deactivated_at TIMESTAMPTZ,
    activated_by TEXT NOT NULL,
    release_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (NOT active OR activated_at IS NOT NULL)
);

CREATE UNIQUE INDEX iv_damage_model_releases_one_active_domain_idx
    ON iv_damage_model_releases (stress_type, target_type)
    WHERE active;

CREATE VIEW iv_damage_decision_eligible_prediction_view AS
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
JOIN iv_damage_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_model_releases release
  ON release.model_run_id = model.id
 AND release.stress_type = request.stress_type
 AND release.target_type = request.target_type
 AND release.active
WHERE model.release_status = 'released'
  AND prediction.decision_eligible
  AND prediction.evidence_status = 'decision_eligible'
  AND prediction.support_status = 'in_domain'
  AND prediction.in_domain
  AND prediction.validation_gate_passed
  AND request.reference_policy = 'same_device';

CREATE VIEW iv_damage_model_card_view AS
SELECT
    model.*,
    policy.policy_version,
    policy.approved AS acceptance_policy_approved,
    policy.requirements AS acceptance_requirements,
    snapshot.snapshot_version,
    snapshot.snapshot_hash,
    snapshot.row_count AS snapshot_rows,
    snapshot.independent_group_count,
    EXISTS (
        SELECT 1 FROM iv_damage_model_releases release
        WHERE release.model_run_id = model.id AND release.active
    ) AS is_active_release
FROM iv_damage_model_runs model
JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
JOIN iv_damage_dataset_snapshots snapshot ON snapshot.id = model.dataset_snapshot_id;

CREATE VIEW iv_damage_validation_summary_view AS
SELECT
    result.model_run_id,
    result.split_scheme,
    result.split_role,
    unit.stress_type,
    unit.target_type,
    unit.device_type,
    unit.ion_species,
    result.support_status,
    COUNT(*) AS independent_units,
    COUNT(DISTINCT unit.physical_device_key) AS physical_devices,
    COUNT(DISTINCT unit.campaign_key) AS campaigns,
    AVG(result.abs_residual) FILTER (WHERE result.abs_residual IS NOT NULL) AS mean_abs_error,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY result.abs_residual)
        FILTER (WHERE result.abs_residual IS NOT NULL) AS median_abs_error,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY result.abs_residual)
        FILTER (WHERE result.abs_residual IS NOT NULL) AS p90_abs_error,
    AVG(result.residual) FILTER (WHERE result.residual IS NOT NULL) AS mean_bias,
    AVG(result.interval_hit::integer) FILTER (WHERE result.interval_hit IS NOT NULL)
        AS interval_coverage
FROM iv_damage_validation_results result
JOIN iv_damage_response_units unit ON unit.id = result.response_unit_id
GROUP BY result.model_run_id, result.split_scheme, result.split_role,
         unit.stress_type, unit.target_type, unit.device_type, unit.ion_species,
         result.support_status;

CREATE VIEW iv_damage_prediction_monitoring_view AS
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
         THEN ABS(prediction.predicted_response - outcome.observed_response) END AS abs_residual,
    CASE WHEN outcome.observed_response IS NOT NULL
              AND prediction.predicted_response_lower IS NOT NULL
              AND prediction.predicted_response_upper IS NOT NULL
         THEN outcome.observed_response BETWEEN prediction.predicted_response_lower
                                              AND prediction.predicted_response_upper END
        AS interval_hit,
    prediction.created_at,
    outcome.matched_at
FROM iv_damage_predictions prediction
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
LEFT JOIN iv_damage_prediction_outcomes outcome ON outcome.request_id = request.id;
