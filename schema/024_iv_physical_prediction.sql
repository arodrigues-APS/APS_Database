-- V2 post-stress IV physical degradation prediction tables.
-- Owned by data_processing_scripts/ml_post_iv_physical_prediction.py.
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

CREATE TABLE IF NOT EXISTS iv_physical_response_pairs (
    id BIGSERIAL PRIMARY KEY,
    pair_key TEXT NOT NULL UNIQUE,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    pairing_method TEXT NOT NULL,
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    pre_feature_id BIGINT NOT NULL REFERENCES iv_physical_curve_features(id) ON DELETE CASCADE,
    post_feature_id BIGINT NOT NULL REFERENCES iv_physical_curve_features(id) ON DELETE CASCADE,
    pre_metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    post_metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    physical_device_key TEXT NOT NULL,
    split_group TEXT NOT NULL,
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
    pair_id BIGINT NOT NULL REFERENCES iv_physical_response_pairs(id) ON DELETE CASCADE,
    pair_key TEXT NOT NULL,
    split_group TEXT NOT NULL,
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

-- Reserved for later gated parameter prediction. The V1 workflow does not
-- write rows here; it exists so downstream work has a stable ownership boundary.
CREATE TABLE IF NOT EXISTS iv_physical_parameter_predictions (
    id BIGSERIAL PRIMARY KEY,
    model_run_id INTEGER NOT NULL REFERENCES iv_physical_model_runs(id) ON DELETE CASCADE,
    source_feature_id BIGINT REFERENCES iv_physical_curve_features(id) ON DELETE SET NULL,
    target_type TEXT NOT NULL CHECK (target_type IN ('delta_vth_v', 'log_rdson_ratio')),
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    predicted_value DOUBLE PRECISION,
    predicted_p10 DOUBLE PRECISION,
    predicted_p90 DOUBLE PRECISION,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    donor_pair_keys TEXT[],
    donor_count INTEGER,
    support_status TEXT NOT NULL,
    physics_flags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_param_pred_model
    ON iv_physical_parameter_predictions(model_run_id);

-- Reserved for later validation-gated constrained curve reconstruction.
-- Blocking and 3rd_Quadrant are intentionally outside the V1 prediction scope.
CREATE TABLE IF NOT EXISTS iv_physical_curve_points (
    id BIGSERIAL PRIMARY KEY,
    parameter_prediction_id BIGINT NOT NULL
        REFERENCES iv_physical_parameter_predictions(id) ON DELETE CASCADE,
    model_run_id INTEGER NOT NULL REFERENCES iv_physical_model_runs(id) ON DELETE CASCADE,
    source_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL,
    curve_family TEXT NOT NULL CHECK (curve_family IN ('IdVg', 'IdVd')),
    x_axis_name TEXT NOT NULL,
    x_value DOUBLE PRECISION NOT NULL,
    bias_axis_name TEXT,
    bias_value DOUBLE PRECISION,
    point_index INTEGER,
    pristine_i_drain DOUBLE PRECISION,
    predicted_post_i_drain DOUBLE PRECISION,
    physics_flags TEXT[],
    prediction_status TEXT NOT NULL DEFAULT 'ok',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_model
    ON iv_physical_curve_points(model_run_id);
CREATE INDEX IF NOT EXISTS idx_iv_phys_curve_points_source
    ON iv_physical_curve_points(source_metadata_id);
