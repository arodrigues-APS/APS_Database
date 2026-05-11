-- V2 post-stress IV physical degradation prediction tables.
-- Owned by data_processing_scripts/ml_post_iv_physical_prediction.py.
-- apply_schema: pipeline-owned
--
-- This schema intentionally does not mutate the legacy iv_prediction_* tables.
-- V2 predicts physical response parameters first; curve reconstruction is
-- reserved for later validation-gated work.

CREATE TABLE IF NOT EXISTS iv_physical_curve_features (
    id BIGSERIAL PRIMARY KEY,
    metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    measurement_category TEXT NOT NULL,
    measurement_type TEXT,
    filename TEXT,
    csv_path TEXT,
    metadata_created_at TIMESTAMP,
    bias_value DOUBLE PRECISION,
    drain_bias_value DOUBLE PRECISION,
    sweep_start DOUBLE PRECISION,
    sweep_stop DOUBLE PRECISION,
    sweep_points INTEGER,
    step_num INTEGER,
    step_start DOUBLE PRECISION,
    step_stop DOUBLE PRECISION,
    experiment TEXT,
    data_source TEXT,
    test_condition TEXT,
    irrad_role TEXT,
    device_id TEXT,
    sample_group TEXT,
    physical_device_key TEXT,
    device_type TEXT,
    manufacturer TEXT,
    voltage_rating_v DOUBLE PRECISION,
    rdson_rating_mohm DOUBLE PRECISION,
    current_rating_a DOUBLE PRECISION,
    package_type TEXT,
    vth_v DOUBLE PRECISION,
    rdson_mohm DOUBLE PRECISION,
    bvdss_v DOUBLE PRECISION,
    vsd_v DOUBLE PRECISION,
    gate_params_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    sc_voltage_v DOUBLE PRECISION,
    sc_duration_us DOUBLE PRECISION,
    sc_vgs_on_v DOUBLE PRECISION,
    sc_vgs_off_v DOUBLE PRECISION,
    sc_condition_label TEXT,
    sc_sequence_num INTEGER,
    irrad_campaign_id INTEGER REFERENCES irradiation_campaigns(id),
    irrad_run_id INTEGER REFERENCES irradiation_runs(id),
    ion_species TEXT,
    beam_energy_mev DOUBLE PRECISION,
    let_surface DOUBLE PRECISION,
    let_bragg_peak DOUBLE PRECISION,
    range_um DOUBLE PRECISION,
    beam_type TEXT,
    fluence_at_meas DOUBLE PRECISION,
    promotion_decision TEXT,
    is_likely_irradiated BOOLEAN,
    quality_status TEXT NOT NULL DEFAULT 'usable'
        CHECK (quality_status IN ('usable', 'excluded', 'missing_metric', 'out_of_scope')),
    quality_flags TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metadata_id, curve_family, target_type)
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_features_meta
    ON iv_physical_curve_features(metadata_id);
CREATE INDEX IF NOT EXISTS idx_iv_phys_features_target
    ON iv_physical_curve_features(target_type, curve_family);
CREATE INDEX IF NOT EXISTS idx_iv_phys_features_device
    ON iv_physical_curve_features(device_type, physical_device_key);
CREATE INDEX IF NOT EXISTS idx_iv_phys_features_source
    ON iv_physical_curve_features(data_source, test_condition, irrad_role);
CREATE INDEX IF NOT EXISTS idx_iv_phys_features_quality
    ON iv_physical_curve_features(quality_status);

DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN metadata_created_at TIMESTAMP;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN bias_value DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN drain_bias_value DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN sweep_start DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN sweep_stop DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN sweep_points INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN step_num INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN step_start DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_features ADD COLUMN step_stop DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS iv_physical_response_pairs (
    id BIGSERIAL PRIMARY KEY,
    pair_key TEXT NOT NULL UNIQUE,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    pairing_method TEXT NOT NULL,
    reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad'
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    pre_feature_id BIGINT NOT NULL REFERENCES iv_physical_curve_features(id) ON DELETE CASCADE,
    post_feature_id BIGINT NOT NULL REFERENCES iv_physical_curve_features(id) ON DELETE CASCADE,
    pre_metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    post_metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    physical_device_key TEXT NOT NULL,
    split_group TEXT NOT NULL,
    same_physical_device BOOLEAN NOT NULL DEFAULT TRUE,
    device_type TEXT NOT NULL,
    manufacturer TEXT,
    voltage_rating_v DOUBLE PRECISION,
    rdson_rating_mohm DOUBLE PRECISION,
    current_rating_a DOUBLE PRECISION,
    package_type TEXT,
    pre_vth_v DOUBLE PRECISION,
    post_vth_v DOUBLE PRECISION,
    pre_rdson_mohm DOUBLE PRECISION,
    post_rdson_mohm DOUBLE PRECISION,
    delta_vth_v DOUBLE PRECISION,
    log_rdson_ratio DOUBLE PRECISION,
    sc_voltage_v DOUBLE PRECISION,
    sc_duration_us DOUBLE PRECISION,
    sc_vgs_on_v DOUBLE PRECISION,
    sc_vgs_off_v DOUBLE PRECISION,
    sc_condition_label TEXT,
    sc_sequence_num INTEGER,
    irrad_campaign_id INTEGER REFERENCES irradiation_campaigns(id),
    irrad_run_id INTEGER REFERENCES irradiation_runs(id),
    ion_species TEXT,
    beam_energy_mev DOUBLE PRECISION,
    let_surface DOUBLE PRECISION,
    let_bragg_peak DOUBLE PRECISION,
    range_um DOUBLE PRECISION,
    beam_type TEXT,
    fluence_at_meas DOUBLE PRECISION,
    baseline_reference_count INTEGER,
    baseline_reference_spread DOUBLE PRECISION,
    baseline_reference_method TEXT,
    library_reference_group_key TEXT,
    quality_status TEXT NOT NULL DEFAULT 'usable'
        CHECK (quality_status IN ('usable', 'excluded', 'unsupported')),
    quality_flags TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_pairs_target
    ON iv_physical_response_pairs(target_type, curve_family);
CREATE INDEX IF NOT EXISTS idx_iv_phys_pairs_stress
    ON iv_physical_response_pairs(stress_type);
CREATE INDEX IF NOT EXISTS idx_iv_phys_pairs_device
    ON iv_physical_response_pairs(device_type, physical_device_key);
CREATE INDEX IF NOT EXISTS idx_iv_phys_pairs_split
    ON iv_physical_response_pairs(split_group);
CREATE INDEX IF NOT EXISTS idx_iv_phys_pairs_quality
    ON iv_physical_response_pairs(quality_status);

DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs
        ADD COLUMN reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs
        ADD CONSTRAINT iv_phys_pairs_reference_tier_check
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs
        ADD COLUMN same_physical_device BOOLEAN NOT NULL DEFAULT TRUE;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs ADD COLUMN baseline_reference_count INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs ADD COLUMN baseline_reference_spread DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs ADD COLUMN baseline_reference_method TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_response_pairs ADD COLUMN library_reference_group_key TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

UPDATE iv_physical_response_pairs
SET baseline_reference_count = COALESCE(baseline_reference_count, 1),
    baseline_reference_spread = COALESCE(baseline_reference_spread, 0.0),
    baseline_reference_method = COALESCE(baseline_reference_method, 'strict_same_physical_device')
WHERE reference_tier = 'strict_pre_irrad';

CREATE INDEX IF NOT EXISTS idx_iv_phys_pairs_reference_tier
    ON iv_physical_response_pairs(reference_tier);

CREATE TABLE IF NOT EXISTS iv_physical_model_runs (
    id SERIAL PRIMARY KEY,
    model_name TEXT NOT NULL DEFAULT 'post_iv_physical_prediction',
    model_version TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    artifact_path TEXT,
    target_stress_types TEXT[] NOT NULL DEFAULT ARRAY['sc', 'irradiation']::text[],
    curve_families TEXT[] NOT NULL DEFAULT ARRAY['IdVg', 'IdVd']::text[],
    train_pairs INTEGER,
    validation_pairs INTEGER,
    supported_validation_pairs INTEGER,
    unsupported_validation_pairs INTEGER,
    model_status TEXT NOT NULL DEFAULT 'pending_validation'
        CHECK (model_status IN ('pending_validation', 'usable', 'weak_validation', 'unsupported')),
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    feature_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_model_runs_trained_at
    ON iv_physical_model_runs(trained_at DESC);
CREATE INDEX IF NOT EXISTS idx_iv_phys_model_runs_status
    ON iv_physical_model_runs(model_status);

CREATE TABLE IF NOT EXISTS iv_physical_validation_residuals (
    id BIGSERIAL PRIMARY KEY,
    model_run_id INTEGER NOT NULL REFERENCES iv_physical_model_runs(id) ON DELETE CASCADE,
    validation_mode TEXT NOT NULL DEFAULT 'within_condition'
        CHECK (validation_mode IN ('within_condition', 'leave_condition')),
    pair_id BIGINT NOT NULL REFERENCES iv_physical_response_pairs(id) ON DELETE CASCADE,
    pair_key TEXT NOT NULL,
    split_group TEXT NOT NULL,
    reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad'
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine')),
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    observed_value DOUBLE PRECISION,
    predicted_value DOUBLE PRECISION,
    predicted_p10 DOUBLE PRECISION,
    predicted_p90 DOUBLE PRECISION,
    residual DOUBLE PRECISION,
    abs_residual DOUBLE PRECISION,
    device_type TEXT,
    manufacturer TEXT,
    physical_device_key TEXT,
    sc_voltage_v DOUBLE PRECISION,
    sc_duration_us DOUBLE PRECISION,
    irrad_run_id INTEGER REFERENCES irradiation_runs(id),
    ion_species TEXT,
    beam_energy_mev DOUBLE PRECISION,
    let_surface DOUBLE PRECISION,
    let_bragg_peak DOUBLE PRECISION,
    range_um DOUBLE PRECISION,
    fluence_at_meas DOUBLE PRECISION,
    donor_pair_keys TEXT[],
    donor_count INTEGER,
    donor_distance DOUBLE PRECISION,
    support_status TEXT NOT NULL,
    unsupported_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_validation_model
    ON iv_physical_validation_residuals(model_run_id);
CREATE INDEX IF NOT EXISTS idx_iv_phys_validation_target
    ON iv_physical_validation_residuals(target_type, curve_family);
CREATE INDEX IF NOT EXISTS idx_iv_phys_validation_status
    ON iv_physical_validation_residuals(support_status);

DO $$ BEGIN
    ALTER TABLE iv_physical_validation_residuals
        ADD COLUMN validation_mode TEXT NOT NULL DEFAULT 'within_condition';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE iv_physical_validation_residuals
        ADD CONSTRAINT iv_phys_validation_mode_check
        CHECK (validation_mode IN ('within_condition', 'leave_condition'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_iv_phys_validation_mode
    ON iv_physical_validation_residuals(validation_mode);

DO $$ BEGIN
    ALTER TABLE iv_physical_validation_residuals
        ADD COLUMN reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_validation_residuals
        ADD CONSTRAINT iv_phys_validation_reference_tier_check
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_iv_phys_validation_reference_tier
    ON iv_physical_validation_residuals(reference_tier);

-- Reserved for later gated parameter prediction. The V1 workflow does not
-- write rows here; it exists so downstream work has a stable ownership boundary.
CREATE TABLE IF NOT EXISTS iv_physical_parameter_predictions (
    id BIGSERIAL PRIMARY KEY,
    model_run_id INTEGER NOT NULL REFERENCES iv_physical_model_runs(id) ON DELETE CASCADE,
    pair_id BIGINT REFERENCES iv_physical_response_pairs(id) ON DELETE SET NULL,
    pair_key TEXT,
    source_feature_id BIGINT REFERENCES iv_physical_curve_features(id) ON DELETE SET NULL,
    post_feature_id BIGINT REFERENCES iv_physical_curve_features(id) ON DELETE SET NULL,
    source_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL,
    post_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL,
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    predicted_value DOUBLE PRECISION,
    predicted_p10 DOUBLE PRECISION,
    predicted_p90 DOUBLE PRECISION,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad'
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine')),
    donor_pair_keys TEXT[],
    donor_count INTEGER,
    donor_distance DOUBLE PRECISION,
    support_status TEXT NOT NULL,
    unsupported_reason TEXT,
    sc_voltage_v DOUBLE PRECISION,
    sc_duration_us DOUBLE PRECISION,
    sc_condition_label TEXT,
    irrad_run_id INTEGER REFERENCES irradiation_runs(id),
    ion_species TEXT,
    beam_energy_mev DOUBLE PRECISION,
    let_surface DOUBLE PRECISION,
    let_bragg_peak DOUBLE PRECISION,
    range_um DOUBLE PRECISION,
    beam_type TEXT,
    fluence_at_meas DOUBLE PRECISION,
    validation_mode_used TEXT,
    validation_gate_pass BOOLEAN,
    validation_supported_fraction DOUBLE PRECISION,
    validation_supported_pairs INTEGER,
    validation_total_pairs INTEGER,
    baseline_reference_count INTEGER,
    baseline_reference_spread DOUBLE PRECISION,
    baseline_reference_method TEXT,
    confidence_level TEXT NOT NULL DEFAULT 'unsupported'
        CHECK (confidence_level IN ('strong', 'weak', 'unsupported')),
    confidence_score DOUBLE PRECISION,
    confidence_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    physics_flags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_param_pred_model
    ON iv_physical_parameter_predictions(model_run_id);

DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN pair_id BIGINT REFERENCES iv_physical_response_pairs(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN pair_key TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN post_feature_id BIGINT REFERENCES iv_physical_curve_features(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN source_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN post_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions
        ADD COLUMN reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions
        ADD CONSTRAINT iv_phys_param_reference_tier_check
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN donor_distance DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN unsupported_reason TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN sc_voltage_v DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN sc_duration_us DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN sc_condition_label TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN irrad_run_id INTEGER REFERENCES irradiation_runs(id);
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN ion_species TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN beam_energy_mev DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN let_surface DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN let_bragg_peak DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN range_um DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN beam_type TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN fluence_at_meas DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN validation_mode_used TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN validation_gate_pass BOOLEAN;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN validation_supported_fraction DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN validation_supported_pairs INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN validation_total_pairs INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN baseline_reference_count INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN baseline_reference_spread DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN baseline_reference_method TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions
        ADD COLUMN confidence_level TEXT NOT NULL DEFAULT 'unsupported';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions
        ADD CONSTRAINT iv_phys_param_confidence_level_check
        CHECK (confidence_level IN ('strong', 'weak', 'unsupported'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions ADD COLUMN confidence_score DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_parameter_predictions
        ADD COLUMN confidence_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[];
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_iv_phys_param_pred_pair
    ON iv_physical_parameter_predictions(pair_id);
CREATE INDEX IF NOT EXISTS idx_iv_phys_param_pred_confidence
    ON iv_physical_parameter_predictions(confidence_level);
CREATE INDEX IF NOT EXISTS idx_iv_phys_param_pred_reference_tier
    ON iv_physical_parameter_predictions(reference_tier);

-- V2 confidence-labeled constrained curve reconstruction.
-- Blocking and 3rd_Quadrant are intentionally outside the V1 prediction scope.
CREATE TABLE IF NOT EXISTS iv_physical_curve_points (
    id BIGSERIAL PRIMARY KEY,
    parameter_prediction_id BIGINT NOT NULL
        REFERENCES iv_physical_parameter_predictions(id) ON DELETE CASCADE,
    model_run_id INTEGER NOT NULL REFERENCES iv_physical_model_runs(id) ON DELETE CASCADE,
    source_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL,
    source_feature_id BIGINT REFERENCES iv_physical_curve_features(id) ON DELETE SET NULL,
    pair_id BIGINT REFERENCES iv_physical_response_pairs(id) ON DELETE SET NULL,
    target_type TEXT NOT NULL DEFAULT 'delta_vth_v'
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad'
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine')),
    x_axis_name TEXT NOT NULL,
    x_value DOUBLE PRECISION NOT NULL,
    source_x_value DOUBLE PRECISION,
    predicted_x_value DOUBLE PRECISION,
    bias_axis_name TEXT,
    bias_value DOUBLE PRECISION,
    point_index INTEGER,
    pristine_i_drain DOUBLE PRECISION,
    predicted_post_i_drain DOUBLE PRECISION,
    predicted_parameter_value DOUBLE PRECISION,
    predicted_parameter_p10 DOUBLE PRECISION,
    predicted_parameter_p90 DOUBLE PRECISION,
    donor_pair_keys TEXT[],
    donor_count INTEGER,
    donor_distance DOUBLE PRECISION,
    support_status TEXT,
    unsupported_reason TEXT,
    confidence_level TEXT NOT NULL DEFAULT 'weak'
        CHECK (confidence_level IN ('strong', 'weak', 'unsupported')),
    confidence_score DOUBLE PRECISION,
    confidence_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    physics_flags TEXT[],
    prediction_status TEXT NOT NULL DEFAULT 'ok',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_model
    ON iv_physical_curve_points(model_run_id);
CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_source
    ON iv_physical_curve_points(source_metadata_id);

DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN source_feature_id BIGINT REFERENCES iv_physical_curve_features(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN pair_id BIGINT REFERENCES iv_physical_response_pairs(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD COLUMN target_type TEXT NOT NULL DEFAULT 'delta_vth_v';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD CONSTRAINT iv_phys_curve_target_type_check
        CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD COLUMN reference_tier TEXT NOT NULL DEFAULT 'strict_pre_irrad';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD CONSTRAINT iv_phys_curve_reference_tier_check
        CHECK (reference_tier IN ('strict_pre_irrad', 'library_pristine'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN source_x_value DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN predicted_x_value DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN predicted_parameter_value DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN predicted_parameter_p10 DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN predicted_parameter_p90 DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN donor_pair_keys TEXT[];
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN donor_count INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN donor_distance DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN support_status TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN unsupported_reason TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD COLUMN confidence_level TEXT NOT NULL DEFAULT 'weak';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD CONSTRAINT iv_phys_curve_confidence_level_check
        CHECK (confidence_level IN ('strong', 'weak', 'unsupported'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points ADD COLUMN confidence_score DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_physical_curve_points
        ADD COLUMN confidence_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[];
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_param
    ON iv_physical_curve_points(parameter_prediction_id);
CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_confidence
    ON iv_physical_curve_points(confidence_level);
CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_reference_tier
    ON iv_physical_curve_points(reference_tier);

-- Superset dashboard views.
-- These views flatten validation metrics and support diagnostics into shapes
-- Apache Superset can chart directly.

DROP VIEW IF EXISTS iv_physical_prediction_quality_flag_view CASCADE;
DROP VIEW IF EXISTS iv_physical_prediction_feature_coverage_view CASCADE;
DROP VIEW IF EXISTS iv_physical_prediction_pair_coverage_view CASCADE;
DROP VIEW IF EXISTS iv_physical_prediction_support_summary_view CASCADE;
DROP VIEW IF EXISTS iv_physical_prediction_validation_view CASCADE;
DROP VIEW IF EXISTS iv_physical_prediction_model_summary_view CASCADE;
DROP VIEW IF EXISTS iv_physical_curve_shape_plot_view CASCADE;
DROP VIEW IF EXISTS iv_physical_curve_prediction_view CASCADE;
DROP VIEW IF EXISTS iv_physical_parameter_prediction_summary_view CASCADE;
DROP VIEW IF EXISTS iv_physical_parameter_prediction_view CASCADE;

CREATE VIEW iv_physical_prediction_model_summary_view AS
WITH latest_validated AS (
    SELECT MAX(id) AS latest_validated_model_run_id
    FROM iv_physical_model_runs
    WHERE validation_pairs IS NOT NULL
)
SELECT
    mr.id AS model_run_id,
    mr.model_name,
    mr.model_version,
    mr.algorithm,
    mr.trained_at,
    mr.artifact_path,
    mr.model_status,
    mr.train_pairs,
    mr.validation_pairs,
    mr.supported_validation_pairs,
    mr.unsupported_validation_pairs,
    t.validation_mode,
    t.validation_label,
    t.reference_tier,
    t.stress_type,
    t.stress_target_key,
    t.curve_family,
    t.target_type,
    t.target_label,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'validation_pairs'], '')::integer
        AS target_validation_pairs,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'supported_validation_pairs'], '')::integer
        AS target_supported_validation_pairs,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'unsupported_validation_pairs'], '')::integer
        AS target_unsupported_validation_pairs,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'median_abs_residual'], '')::double precision
        AS median_abs_residual,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'p90_abs_residual'], '')::double precision
        AS p90_abs_residual,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'gate', 'min_supported_validation_pairs'], '')::integer
        AS gate_min_supported_validation_pairs,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'gate', 'median_abs_residual_max'], '')::double precision
        AS gate_median_abs_residual_max,
    NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'gate', 'p90_abs_residual_max'], '')::double precision
        AS gate_p90_abs_residual_max,
    COALESCE(NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'gate_pass'], '')::boolean, false)
        AS gate_pass,
    COALESCE(NULLIF(mr.metrics #>> ARRAY['curve_reconstruction_enabled'], '')::boolean, false)
        AS curve_reconstruction_enabled,
    (mr.id = (SELECT MAX(id) FROM iv_physical_model_runs)) AS is_latest_model_run,
    (mr.id = lv.latest_validated_model_run_id) AS is_latest_validated_model_run,
    (mr.metrics #> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key]) IS NOT NULL
        AS is_intended_stress_target,
    CASE
      WHEN (mr.metrics #> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key]) IS NULL
        THEN 'not_evaluated'
      WHEN COALESCE(NULLIF(mr.metrics #>> ARRAY['validation_modes', t.validation_mode, 'reference_stress_targets', t.stress_target_key, 'gate_pass'], '')::boolean, false)
        THEN 'gate_pass'
      WHEN mr.validation_pairs IS NULL
        THEN 'not_validated'
      ELSE 'gate_fail'
    END AS target_gate_status
FROM iv_physical_model_runs mr
LEFT JOIN latest_validated lv ON true
CROSS JOIN (
    VALUES
      ('within_condition'::text, 'Within-condition validation'::text, 'strict_pre_irrad'::text, 'sc'::text,
       'strict_pre_irrad|sc|delta_vth_v'::text, 'IdVg'::text, 'delta_vth_v'::text,
       'IdVg / delta Vth'::text),
      ('within_condition'::text, 'Within-condition validation'::text, 'strict_pre_irrad'::text, 'irradiation'::text,
       'strict_pre_irrad|irradiation|delta_vth_v'::text, 'IdVg'::text, 'delta_vth_v'::text,
       'IdVg / delta Vth'::text),
      ('within_condition'::text, 'Within-condition validation'::text, 'strict_pre_irrad'::text, 'sc'::text,
       'strict_pre_irrad|sc|log_rdson_ratio'::text, 'IdVd'::text, 'log_rdson_ratio'::text,
       'IdVd / log Rds(on) ratio'::text),
      ('within_condition'::text, 'Within-condition validation'::text, 'strict_pre_irrad'::text, 'irradiation'::text,
       'strict_pre_irrad|irradiation|log_rdson_ratio'::text, 'IdVd'::text, 'log_rdson_ratio'::text,
       'IdVd / log Rds(on) ratio'::text),
      ('within_condition'::text, 'Within-condition validation'::text, 'library_pristine'::text, 'irradiation'::text,
       'library_pristine|irradiation|delta_vth_v'::text, 'IdVg'::text, 'delta_vth_v'::text,
       'IdVg / delta Vth'::text),
      ('within_condition'::text, 'Within-condition validation'::text, 'library_pristine'::text, 'irradiation'::text,
       'library_pristine|irradiation|log_rdson_ratio'::text, 'IdVd'::text, 'log_rdson_ratio'::text,
       'IdVd / log Rds(on) ratio'::text),
      ('leave_condition'::text, 'Leave-condition validation'::text, 'strict_pre_irrad'::text, 'sc'::text,
       'strict_pre_irrad|sc|delta_vth_v'::text, 'IdVg'::text, 'delta_vth_v'::text,
       'IdVg / delta Vth'::text),
      ('leave_condition'::text, 'Leave-condition validation'::text, 'strict_pre_irrad'::text, 'irradiation'::text,
       'strict_pre_irrad|irradiation|delta_vth_v'::text, 'IdVg'::text, 'delta_vth_v'::text,
       'IdVg / delta Vth'::text),
      ('leave_condition'::text, 'Leave-condition validation'::text, 'strict_pre_irrad'::text, 'sc'::text,
       'strict_pre_irrad|sc|log_rdson_ratio'::text, 'IdVd'::text, 'log_rdson_ratio'::text,
       'IdVd / log Rds(on) ratio'::text),
      ('leave_condition'::text, 'Leave-condition validation'::text, 'strict_pre_irrad'::text, 'irradiation'::text,
       'strict_pre_irrad|irradiation|log_rdson_ratio'::text, 'IdVd'::text, 'log_rdson_ratio'::text,
       'IdVd / log Rds(on) ratio'::text),
      ('leave_condition'::text, 'Leave-condition validation'::text, 'library_pristine'::text, 'irradiation'::text,
       'library_pristine|irradiation|delta_vth_v'::text, 'IdVg'::text, 'delta_vth_v'::text,
       'IdVg / delta Vth'::text),
      ('leave_condition'::text, 'Leave-condition validation'::text, 'library_pristine'::text, 'irradiation'::text,
       'library_pristine|irradiation|log_rdson_ratio'::text, 'IdVd'::text, 'log_rdson_ratio'::text,
       'IdVd / log Rds(on) ratio'::text)
) AS t(validation_mode, validation_label, reference_tier, stress_type, stress_target_key,
       curve_family, target_type, target_label);

CREATE VIEW iv_physical_prediction_validation_view AS
WITH latest_validated AS (
    SELECT MAX(id) AS latest_validated_model_run_id
    FROM iv_physical_model_runs
    WHERE validation_pairs IS NOT NULL
)
SELECT
    vr.id AS validation_id,
    vr.model_run_id,
    vr.validation_mode,
    CASE
      WHEN vr.validation_mode = 'within_condition' THEN 'Within-condition validation'
      WHEN vr.validation_mode = 'leave_condition' THEN 'Leave-condition validation'
      ELSE vr.validation_mode
    END AS validation_label,
    mr.model_version,
    mr.algorithm,
    mr.trained_at,
    mr.model_status,
    (mr.id = (SELECT MAX(id) FROM iv_physical_model_runs)) AS is_latest_model_run,
    (mr.id = lv.latest_validated_model_run_id) AS is_latest_validated_model_run,
    vr.pair_id,
    vr.pair_key,
    vr.split_group,
    vr.reference_tier,
    vr.stress_type,
    vr.curve_family,
    vr.target_type,
    CASE
      WHEN vr.target_type = 'delta_vth_v' THEN 'IdVg / delta Vth'
      WHEN vr.target_type = 'log_rdson_ratio' THEN 'IdVd / log Rds(on) ratio'
      ELSE vr.target_type
    END AS target_label,
    vr.observed_value,
    vr.predicted_value,
    vr.predicted_p10,
    vr.predicted_p90,
    vr.residual,
    vr.abs_residual,
    vr.predicted_p90 - vr.predicted_p10 AS prediction_interval_width,
    CASE
      WHEN vr.predicted_value IS NULL THEN NULL
      WHEN vr.predicted_p10 IS NULL OR vr.predicted_p90 IS NULL THEN NULL
      WHEN vr.observed_value BETWEEN LEAST(vr.predicted_p10, vr.predicted_p90)
                                AND GREATEST(vr.predicted_p10, vr.predicted_p90)
        THEN true
      ELSE false
    END AS observed_within_prediction_interval,
    CASE
      WHEN vr.target_type = 'delta_vth_v' THEN 0.5
      WHEN vr.target_type = 'log_rdson_ratio' THEN 0.25
    END AS gate_median_abs_residual_max,
    CASE
      WHEN vr.target_type = 'delta_vth_v' THEN 2.0
      WHEN vr.target_type = 'log_rdson_ratio' THEN 0.75
    END AS gate_p90_abs_residual_max,
    CASE
      WHEN vr.support_status <> 'ok' THEN 'unsupported'
      WHEN vr.target_type = 'delta_vth_v' AND vr.abs_residual <= 0.5 THEN 'within_median_gate'
      WHEN vr.target_type = 'delta_vth_v' AND vr.abs_residual <= 2.0 THEN 'within_p90_gate'
      WHEN vr.target_type = 'log_rdson_ratio' AND vr.abs_residual <= 0.25 THEN 'within_median_gate'
      WHEN vr.target_type = 'log_rdson_ratio' AND vr.abs_residual <= 0.75 THEN 'within_p90_gate'
      ELSE 'outside_gate'
    END AS residual_gate_band,
    vr.device_type,
    vr.manufacturer,
    vr.physical_device_key,
    vr.sc_voltage_v,
    vr.sc_duration_us,
    vr.irrad_run_id,
    vr.ion_species,
    vr.beam_energy_mev,
    vr.let_surface,
    vr.let_bragg_peak,
    vr.range_um,
    vr.fluence_at_meas,
    vr.donor_pair_keys,
    vr.donor_count,
    vr.donor_distance,
    vr.support_status,
    COALESCE(vr.unsupported_reason, 'supported') AS support_reason,
    vr.created_at
FROM iv_physical_validation_residuals vr
JOIN iv_physical_model_runs mr ON mr.id = vr.model_run_id
LEFT JOIN latest_validated lv ON true;

CREATE VIEW iv_physical_prediction_support_summary_view AS
SELECT
    model_run_id,
    model_version,
    model_status,
    is_latest_model_run,
    is_latest_validated_model_run,
    validation_mode,
    validation_label,
    reference_tier,
    target_type,
    target_label,
    curve_family,
    stress_type,
    device_type,
    support_status,
    support_reason,
    COUNT(*) AS n_validation_pairs,
    COUNT(*) FILTER (WHERE predicted_value IS NOT NULL) AS n_numeric_predictions,
    AVG(abs_residual) FILTER (WHERE abs_residual IS NOT NULL) AS mean_abs_residual,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY abs_residual)
        FILTER (WHERE abs_residual IS NOT NULL) AS median_abs_residual,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY abs_residual)
        FILTER (WHERE abs_residual IS NOT NULL) AS p90_abs_residual,
    AVG(donor_count) FILTER (WHERE donor_count IS NOT NULL) AS mean_donor_count,
    AVG(donor_distance) FILTER (WHERE donor_distance IS NOT NULL) AS mean_donor_distance
FROM iv_physical_prediction_validation_view
GROUP BY
    model_run_id, model_version, model_status, is_latest_model_run,
    is_latest_validated_model_run, validation_mode, validation_label,
    reference_tier, target_type, target_label, curve_family, stress_type, device_type,
    support_status, support_reason;

CREATE VIEW iv_physical_prediction_pair_coverage_view AS
SELECT
    reference_tier,
    pairing_method,
    target_type,
    CASE
      WHEN target_type = 'delta_vth_v' THEN 'IdVg / delta Vth'
      WHEN target_type = 'log_rdson_ratio' THEN 'IdVd / log Rds(on) ratio'
      ELSE target_type
    END AS target_label,
    curve_family,
    stress_type,
    device_type,
    manufacturer,
    quality_status,
    same_physical_device,
    COUNT(*) AS n_pairs,
    COUNT(DISTINCT split_group) AS n_split_groups,
    COUNT(DISTINCT physical_device_key) AS n_physical_devices,
    COUNT(DISTINCT pre_metadata_id) AS n_pre_files,
    COUNT(DISTINCT post_metadata_id) AS n_post_files,
    MIN(sc_voltage_v) AS min_sc_voltage_v,
    MAX(sc_voltage_v) AS max_sc_voltage_v,
    MIN(sc_duration_us) AS min_sc_duration_us,
    MAX(sc_duration_us) AS max_sc_duration_us,
    COUNT(DISTINCT irrad_run_id) FILTER (WHERE irrad_run_id IS NOT NULL)
        AS n_irrad_runs,
    COUNT(DISTINCT ion_species) FILTER (WHERE ion_species IS NOT NULL)
        AS n_ion_species,
    AVG(baseline_reference_count) FILTER (WHERE baseline_reference_count IS NOT NULL)
        AS mean_baseline_reference_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY baseline_reference_spread)
        FILTER (WHERE baseline_reference_spread IS NOT NULL) AS median_baseline_reference_spread,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY baseline_reference_spread)
        FILTER (WHERE baseline_reference_spread IS NOT NULL) AS p90_baseline_reference_spread,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delta_vth_v)
        FILTER (WHERE delta_vth_v IS NOT NULL) AS median_delta_vth_v,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY log_rdson_ratio)
        FILTER (WHERE log_rdson_ratio IS NOT NULL) AS median_log_rdson_ratio
FROM iv_physical_response_pairs
GROUP BY reference_tier, pairing_method, target_type, curve_family, stress_type,
         device_type, manufacturer, quality_status, same_physical_device;

CREATE VIEW iv_physical_prediction_feature_coverage_view AS
SELECT
    data_source,
    COALESCE(test_condition, irrad_role, '<none>') AS stress_condition,
    measurement_category,
    curve_family,
    target_type,
    CASE
      WHEN target_type = 'delta_vth_v' THEN 'IdVg / delta Vth'
      WHEN target_type = 'log_rdson_ratio' THEN 'IdVd / log Rds(on) ratio'
      ELSE target_type
    END AS target_label,
    device_type,
    manufacturer,
    quality_status,
    COUNT(*) AS n_features,
    COUNT(DISTINCT metadata_id) AS n_files,
    COUNT(DISTINCT physical_device_key) AS n_physical_devices,
    COUNT(*) FILTER (WHERE vth_v IS NOT NULL) AS n_with_vth,
    COUNT(*) FILTER (WHERE rdson_mohm IS NOT NULL) AS n_with_rdson,
    COUNT(*) FILTER (WHERE quality_status = 'usable') AS n_usable
FROM iv_physical_curve_features
GROUP BY data_source, COALESCE(test_condition, irrad_role, '<none>'),
         measurement_category,
         curve_family, target_type, device_type, manufacturer, quality_status;

CREATE VIEW iv_physical_prediction_quality_flag_view AS
SELECT
    'feature'::text AS record_type,
    NULL::integer AS model_run_id,
    NULL::text AS validation_mode,
    NULL::text AS validation_label,
    NULL::text AS reference_tier,
    f.target_type,
    CASE
      WHEN f.target_type = 'delta_vth_v' THEN 'IdVg / delta Vth'
      WHEN f.target_type = 'log_rdson_ratio' THEN 'IdVd / log Rds(on) ratio'
      ELSE f.target_type
    END AS target_label,
    f.curve_family,
    f.data_source,
    COALESCE(f.test_condition, f.irrad_role, '<none>') AS stress_condition,
    f.device_type,
    f.quality_status,
    flag AS reason,
    COUNT(*) AS n_records
FROM iv_physical_curve_features f
CROSS JOIN LATERAL unnest(f.quality_flags) AS flag
GROUP BY f.target_type, f.curve_family, f.data_source,
         COALESCE(f.test_condition, f.irrad_role, '<none>'),
         f.device_type, f.quality_status, flag
UNION ALL
SELECT
    'validation'::text AS record_type,
    model_run_id,
    validation_mode,
    validation_label,
    reference_tier,
    target_type,
    target_label,
    curve_family,
    stress_type AS data_source,
    support_status AS stress_condition,
    device_type,
    support_status AS quality_status,
    support_reason AS reason,
    COUNT(*) AS n_records
FROM iv_physical_prediction_validation_view
WHERE support_status <> 'ok'
GROUP BY model_run_id, validation_mode, validation_label, reference_tier, target_type, target_label, curve_family,
         stress_type, support_status, device_type, support_reason;

CREATE VIEW iv_physical_parameter_prediction_view AS
SELECT
    pp.id AS parameter_prediction_id,
    pp.model_run_id,
    mr.model_version,
    mr.algorithm,
    mr.trained_at,
    mr.model_status,
    (mr.id = (SELECT MAX(id) FROM iv_physical_model_runs)) AS is_latest_model_run,
    pp.pair_id,
    pp.pair_key,
    pp.source_feature_id,
    pp.post_feature_id,
    pp.source_metadata_id,
    pp.post_metadata_id,
    pp.target_type,
    CASE
      WHEN pp.target_type = 'delta_vth_v' THEN 'IdVg / delta Vth'
      WHEN pp.target_type = 'log_rdson_ratio' THEN 'IdVd / log Rds(on) ratio'
      ELSE pp.target_type
    END AS target_label,
    pp.curve_family,
    pp.stress_type,
    pp.reference_tier,
    pp.predicted_value,
    pp.predicted_p10,
    pp.predicted_p90,
    pp.predicted_p90 - pp.predicted_p10 AS prediction_interval_width,
    pp.validation_mode_used,
    pp.validation_gate_pass,
    pp.validation_supported_fraction,
    pp.validation_supported_pairs,
    pp.validation_total_pairs,
    pp.baseline_reference_count,
    pp.baseline_reference_spread,
    pp.baseline_reference_method,
    pp.donor_pair_keys,
    pp.donor_count,
    pp.donor_distance,
    pp.support_status,
    COALESCE(pp.unsupported_reason, 'supported') AS support_reason,
    pp.confidence_level,
    pp.confidence_score,
    pp.confidence_reasons,
    pp.sc_voltage_v,
    pp.sc_duration_us,
    pp.sc_condition_label,
    pp.irrad_run_id,
    pp.ion_species,
    pp.beam_energy_mev,
    pp.let_surface,
    pp.let_bragg_peak,
    pp.range_um,
    pp.beam_type,
    pp.fluence_at_meas,
    sf.device_type,
    sf.manufacturer,
    sf.physical_device_key,
    pp.physics_flags,
    pp.created_at
FROM iv_physical_parameter_predictions pp
JOIN iv_physical_model_runs mr ON mr.id = pp.model_run_id
LEFT JOIN iv_physical_curve_features sf ON sf.id = pp.source_feature_id;

CREATE VIEW iv_physical_curve_prediction_view AS
SELECT
    cp.id AS curve_point_id,
    cp.parameter_prediction_id,
    cp.model_run_id,
    pp.model_version,
    pp.algorithm,
    pp.trained_at,
    pp.model_status,
    pp.is_latest_model_run,
    cp.pair_id,
    pp.pair_key,
    cp.source_metadata_id,
    cp.source_feature_id,
    pp.post_metadata_id,
    pp.post_feature_id,
    cp.target_type,
    pp.target_label,
    cp.curve_family,
    pp.stress_type,
    cp.reference_tier,
    pp.device_type,
    pp.manufacturer,
    pp.physical_device_key,
    cp.x_axis_name,
    cp.x_value,
    cp.source_x_value,
    cp.predicted_x_value,
    cp.bias_axis_name,
    cp.bias_value,
    cp.point_index,
    cp.pristine_i_drain,
    cp.predicted_post_i_drain,
    cp.predicted_parameter_value,
    cp.predicted_parameter_p10,
    cp.predicted_parameter_p90,
    cp.donor_pair_keys,
    cp.donor_count,
    cp.donor_distance,
    cp.support_status,
    COALESCE(cp.unsupported_reason, 'supported') AS support_reason,
    cp.confidence_level,
    cp.confidence_score,
    cp.confidence_reasons,
    cp.physics_flags,
    cp.prediction_status,
    pp.validation_mode_used,
    pp.validation_gate_pass,
    pp.validation_supported_fraction,
    pp.validation_supported_pairs,
    pp.validation_total_pairs,
    pp.baseline_reference_count,
    pp.baseline_reference_spread,
    pp.irrad_run_id,
    pp.ion_species,
    pp.beam_energy_mev,
    pp.let_surface,
    pp.let_bragg_peak,
    pp.range_um,
    pp.fluence_at_meas,
    cp.created_at
FROM iv_physical_curve_points cp
JOIN iv_physical_parameter_prediction_view pp
  ON pp.parameter_prediction_id = cp.parameter_prediction_id;

CREATE VIEW iv_physical_curve_shape_plot_view AS
WITH supported_curve_points AS (
    SELECT
        cv.*,
        ROUND(cv.bias_value::numeric, 2)::double precision AS bias_value_rounded
    FROM iv_physical_curve_prediction_view cv
    WHERE cv.support_status = 'ok'
      AND COALESCE(cv.confidence_level, 'unsupported') <> 'unsupported'
      AND cv.source_x_value IS NOT NULL
      AND cv.predicted_x_value IS NOT NULL
      AND cv.pristine_i_drain IS NOT NULL
      AND cv.predicted_post_i_drain IS NOT NULL
),
pair_scores AS (
    SELECT
        model_run_id,
        validation_mode_used,
        reference_tier,
        stress_type,
        target_type,
        pair_key,
        MAX(confidence_score) AS max_confidence_score,
        MAX(donor_count) AS max_donor_count,
        MIN(donor_distance) AS min_donor_distance
    FROM supported_curve_points
    GROUP BY
        model_run_id,
        validation_mode_used,
        reference_tier,
        stress_type,
        target_type,
        pair_key
),
ranked_pairs AS (
    SELECT
        ps.*,
        DENSE_RANK() OVER (
            PARTITION BY
                model_run_id,
                validation_mode_used,
                reference_tier,
                stress_type,
                target_type
            ORDER BY
                max_confidence_score DESC NULLS LAST,
                max_donor_count DESC NULLS LAST,
                min_donor_distance ASC NULLS LAST,
                pair_key ASC
        ) AS plot_pair_rank
    FROM pair_scores ps
)
SELECT
    cv.curve_point_id,
    cv.parameter_prediction_id,
    cv.model_run_id,
    cv.model_version,
    cv.algorithm,
    cv.trained_at,
    cv.model_status,
    cv.is_latest_model_run,
    cv.pair_id,
    cv.pair_key,
    cv.source_metadata_id,
    cv.source_feature_id,
    cv.post_metadata_id,
    cv.post_feature_id,
    cv.target_type,
    cv.target_label,
    cv.curve_family,
    cv.stress_type,
    cv.reference_tier,
    cv.device_type,
    cv.manufacturer,
    cv.physical_device_key,
    cv.x_axis_name,
    cv.bias_axis_name,
    cv.bias_value,
    cv.bias_value_rounded,
    cv.point_index,
    role.curve_role,
    role.curve_role_order,
    role.plot_x_value,
    role.plot_i_drain,
    CONCAT_WS(
        ' | ',
        cv.pair_key,
        role.curve_role_label,
        cv.confidence_level,
        COALESCE(cv.bias_axis_name, 'bias') || '=' || COALESCE(cv.bias_value_rounded::text, 'NA')
    ) AS plot_series_label,
    cv.source_x_value,
    cv.predicted_x_value,
    cv.pristine_i_drain,
    cv.predicted_post_i_drain,
    cv.predicted_parameter_value,
    cv.predicted_parameter_p10,
    cv.predicted_parameter_p90,
    cv.donor_pair_keys,
    cv.donor_count,
    cv.donor_distance,
    cv.support_status,
    cv.support_reason,
    cv.confidence_level,
    cv.confidence_score,
    cv.confidence_reasons,
    cv.physics_flags,
    cv.prediction_status,
    cv.validation_mode_used,
    cv.validation_gate_pass,
    cv.validation_supported_fraction,
    cv.validation_supported_pairs,
    cv.validation_total_pairs,
    cv.baseline_reference_count,
    cv.baseline_reference_spread,
    cv.irrad_run_id,
    cv.ion_species,
    cv.beam_energy_mev,
    cv.let_surface,
    cv.let_bragg_peak,
    cv.range_um,
    cv.fluence_at_meas,
    rp.plot_pair_rank,
    cv.created_at
FROM supported_curve_points cv
JOIN ranked_pairs rp
  ON rp.model_run_id = cv.model_run_id
 AND rp.validation_mode_used IS NOT DISTINCT FROM cv.validation_mode_used
 AND rp.reference_tier IS NOT DISTINCT FROM cv.reference_tier
 AND rp.stress_type IS NOT DISTINCT FROM cv.stress_type
 AND rp.target_type IS NOT DISTINCT FROM cv.target_type
 AND rp.pair_key IS NOT DISTINCT FROM cv.pair_key
CROSS JOIN LATERAL (
    VALUES
        (
            'source_reference',
            1,
            'Source reference',
            cv.source_x_value,
            cv.pristine_i_drain
        ),
        (
            'predicted_post',
            2,
            'Predicted post',
            cv.predicted_x_value,
            cv.predicted_post_i_drain
        )
) AS role(curve_role, curve_role_order, curve_role_label, plot_x_value, plot_i_drain)
WHERE role.plot_x_value IS NOT NULL
  AND role.plot_i_drain IS NOT NULL;

CREATE VIEW iv_physical_parameter_prediction_summary_view AS
WITH curve_counts AS (
    SELECT
        parameter_prediction_id,
        COUNT(*) AS n_curve_points
    FROM iv_physical_curve_points
    GROUP BY parameter_prediction_id
)
SELECT
    pp.model_run_id,
    pp.model_version,
    pp.algorithm,
    pp.trained_at,
    pp.model_status,
    pp.is_latest_model_run,
    pp.validation_mode_used,
    pp.reference_tier,
    pp.stress_type,
    pp.target_type,
    pp.target_label,
    pp.curve_family,
    pp.device_type,
    pp.manufacturer,
    pp.confidence_level,
    pp.support_status,
    pp.support_reason,
    pp.validation_gate_pass,
    pp.validation_supported_fraction,
    pp.validation_supported_pairs,
    pp.validation_total_pairs,
    pp.baseline_reference_method,
    COUNT(*) AS n_parameter_predictions,
    COUNT(*) FILTER (WHERE pp.predicted_value IS NOT NULL) AS n_numeric_predictions,
    COUNT(*) FILTER (WHERE pp.confidence_level = 'unsupported') AS n_unsupported_predictions,
    COUNT(*) FILTER (WHERE COALESCE(cc.n_curve_points, 0) > 0) AS n_parameters_with_curves,
    COALESCE(SUM(cc.n_curve_points), 0)::bigint AS n_curve_points,
    AVG(pp.confidence_score) AS avg_confidence_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pp.confidence_score)
        FILTER (WHERE pp.confidence_score IS NOT NULL) AS median_confidence_score,
    AVG(pp.donor_count) AS avg_donor_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pp.donor_distance)
        FILTER (WHERE pp.donor_distance IS NOT NULL) AS median_donor_distance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pp.baseline_reference_spread)
        FILTER (WHERE pp.baseline_reference_spread IS NOT NULL) AS median_baseline_reference_spread,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY pp.baseline_reference_spread)
        FILTER (WHERE pp.baseline_reference_spread IS NOT NULL) AS p90_baseline_reference_spread,
    MIN(pp.created_at) AS first_created_at,
    MAX(pp.created_at) AS last_created_at
FROM iv_physical_parameter_prediction_view pp
LEFT JOIN curve_counts cc
  ON cc.parameter_prediction_id = pp.parameter_prediction_id
GROUP BY
    pp.model_run_id,
    pp.model_version,
    pp.algorithm,
    pp.trained_at,
    pp.model_status,
    pp.is_latest_model_run,
    pp.validation_mode_used,
    pp.reference_tier,
    pp.stress_type,
    pp.target_type,
    pp.target_label,
    pp.curve_family,
    pp.device_type,
    pp.manufacturer,
    pp.confidence_level,
    pp.support_status,
    pp.support_reason,
    pp.validation_gate_pass,
    pp.validation_supported_fraction,
    pp.validation_supported_pairs,
    pp.validation_total_pairs,
    pp.baseline_reference_method;
