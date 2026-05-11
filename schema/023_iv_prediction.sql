-- Post-stress IV curve prediction tables.
-- Views are owned by data_processing_scripts/ml_post_iv_prediction.py.
-- apply_schema: pipeline-owned

CREATE TABLE IF NOT EXISTS iv_prediction_pair_grid (
    id BIGSERIAL PRIMARY KEY,
    pair_key TEXT NOT NULL,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    pairing_method TEXT NOT NULL,
    physical_device_key TEXT,
    pre_metadata_ids INTEGER[],
    post_metadata_ids INTEGER[],
    device_type TEXT,
    manufacturer TEXT,
    voltage_rating_v DOUBLE PRECISION,
    rdson_mohm DOUBLE PRECISION,
    current_rating_a DOUBLE PRECISION,
    package_type TEXT,
    measurement_category TEXT NOT NULL,
    x_axis_name TEXT NOT NULL,
    x_value DOUBLE PRECISION NOT NULL,
    bias_axis_name TEXT,
    bias_value DOUBLE PRECISION,
    pre_i_drain DOUBLE PRECISION,
    post_i_drain DOUBLE PRECISION,
    pre_slog_i_drain DOUBLE PRECISION,
    post_slog_i_drain DOUBLE PRECISION,
    delta_slog_i_drain DOUBLE PRECISION,
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
    split_group TEXT NOT NULL,
    quality_flags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_pair_grid_category
    ON iv_prediction_pair_grid(measurement_category);
CREATE INDEX IF NOT EXISTS idx_iv_pair_grid_stress
    ON iv_prediction_pair_grid(stress_type);
CREATE INDEX IF NOT EXISTS idx_iv_pair_grid_device
    ON iv_prediction_pair_grid(device_type);
CREATE INDEX IF NOT EXISTS idx_iv_pair_grid_pair_key
    ON iv_prediction_pair_grid(pair_key);

CREATE TABLE IF NOT EXISTS iv_prediction_model_runs (
    id SERIAL PRIMARY KEY,
    model_name TEXT NOT NULL DEFAULT 'post_iv_prediction',
    model_version TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    artifact_path TEXT,
    target_stress_types TEXT[] NOT NULL DEFAULT ARRAY['sc', 'irradiation'],
    curve_categories TEXT[] NOT NULL DEFAULT ARRAY['IdVg', 'IdVd', '3rd_Quadrant', 'Blocking'],
    train_rows INTEGER,
    validation_rows INTEGER,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    feature_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_iv_model_runs_trained_at
    ON iv_prediction_model_runs(trained_at DESC);

CREATE TABLE IF NOT EXISTS iv_prediction_batches (
    id SERIAL PRIMARY KEY,
    model_run_id INTEGER NOT NULL REFERENCES iv_prediction_model_runs(id) ON DELETE CASCADE,
    batch_label TEXT,
    target_stress_type TEXT NOT NULL CHECK (target_stress_type IN ('sc', 'irradiation')),
    target_condition_label TEXT,
    target_irrad_run_id INTEGER REFERENCES irradiation_runs(id),
    target_ion_species TEXT,
    target_beam_energy_mev DOUBLE PRECISION,
    target_let_surface DOUBLE PRECISION,
    target_let_bragg_peak DOUBLE PRECISION,
    target_range_um DOUBLE PRECISION,
    target_beam_type TEXT,
    target_fluence_at_meas DOUBLE PRECISION,
    target_sc_voltage_v DOUBLE PRECISION,
    target_sc_duration_us DOUBLE PRECISION,
    target_sc_condition_label TEXT,
    candidate_mode TEXT NOT NULL DEFAULT 'pristine-only',
    filters JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$ BEGIN
    ALTER TABLE iv_prediction_batches ADD COLUMN target_let_bragg_peak DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_batches ADD COLUMN target_range_um DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_iv_prediction_batches_model
    ON iv_prediction_batches(model_run_id);
CREATE INDEX IF NOT EXISTS idx_iv_prediction_batches_target
    ON iv_prediction_batches(target_stress_type, target_condition_label);

CREATE TABLE IF NOT EXISTS iv_prediction_points (
    id BIGSERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL REFERENCES iv_prediction_batches(id) ON DELETE CASCADE,
    model_run_id INTEGER NOT NULL REFERENCES iv_prediction_model_runs(id) ON DELETE CASCADE,
    source_metadata_id INTEGER REFERENCES baselines_metadata(id) ON DELETE SET NULL,
    device_id TEXT,
    sample_group TEXT,
    device_type TEXT,
    manufacturer TEXT,
    voltage_rating_v DOUBLE PRECISION,
    rdson_mohm DOUBLE PRECISION,
    current_rating_a DOUBLE PRECISION,
    package_type TEXT,
    measurement_category TEXT NOT NULL,
    output_kind TEXT NOT NULL DEFAULT 'curve',
    x_axis_name TEXT NOT NULL,
    x_value DOUBLE PRECISION NOT NULL,
    bias_axis_name TEXT,
    bias_value DOUBLE PRECISION,
    point_index INTEGER,
    pristine_i_drain DOUBLE PRECISION,
    predicted_post_i_drain DOUBLE PRECISION,
    predicted_post_i_drain_p10 DOUBLE PRECISION,
    predicted_post_i_drain_p90 DOUBLE PRECISION,
    raw_predicted_post_i_drain DOUBLE PRECISION,
    raw_predicted_post_i_drain_p10 DOUBLE PRECISION,
    raw_predicted_post_i_drain_p90 DOUBLE PRECISION,
    predicted_delta_slog_i_drain DOUBLE PRECISION,
    target_stress_type TEXT NOT NULL CHECK (target_stress_type IN ('sc', 'irradiation')),
    target_condition_label TEXT,
    target_irrad_run_id INTEGER REFERENCES irradiation_runs(id),
    target_ion_species TEXT,
    target_beam_energy_mev DOUBLE PRECISION,
    target_let_surface DOUBLE PRECISION,
    target_let_bragg_peak DOUBLE PRECISION,
    target_range_um DOUBLE PRECISION,
    target_beam_type TEXT,
    target_fluence_at_meas DOUBLE PRECISION,
    target_sc_voltage_v DOUBLE PRECISION,
    target_sc_duration_us DOUBLE PRECISION,
    target_sc_condition_label TEXT,
    donor_pair_keys TEXT[],
    donor_count INTEGER,
    donor_distance DOUBLE PRECISION,
    support_status TEXT,
    support_fraction DOUBLE PRECISION,
    donor_shape_distance DOUBLE PRECISION,
    uncertainty_reason TEXT,
    prediction_status TEXT NOT NULL DEFAULT 'ok',
    physics_flags TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN target_let_bragg_peak DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN target_range_um DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN donor_pair_keys TEXT[];
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN donor_count INTEGER;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN donor_distance DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN raw_predicted_post_i_drain DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN raw_predicted_post_i_drain_p10 DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN raw_predicted_post_i_drain_p90 DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN support_status TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN support_fraction DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN donor_shape_distance DOUBLE PRECISION;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE iv_prediction_points ADD COLUMN uncertainty_reason TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_iv_prediction_points_batch
    ON iv_prediction_points(batch_id);
CREATE INDEX IF NOT EXISTS idx_iv_prediction_points_model
    ON iv_prediction_points(model_run_id);
CREATE INDEX IF NOT EXISTS idx_iv_prediction_points_device
    ON iv_prediction_points(device_type, device_id);
CREATE INDEX IF NOT EXISTS idx_iv_prediction_points_category
    ON iv_prediction_points(measurement_category);
CREATE INDEX IF NOT EXISTS idx_iv_prediction_points_target
    ON iv_prediction_points(target_stress_type, target_condition_label);

CREATE TABLE IF NOT EXISTS iv_prediction_validation_residuals (
    id BIGSERIAL PRIMARY KEY,
    model_run_id INTEGER NOT NULL REFERENCES iv_prediction_model_runs(id) ON DELETE CASCADE,
    pair_key TEXT NOT NULL,
    split_group TEXT NOT NULL,
    stress_type TEXT NOT NULL CHECK (stress_type IN ('sc', 'irradiation')),
    device_type TEXT,
    manufacturer TEXT,
    measurement_category TEXT NOT NULL,
    x_value DOUBLE PRECISION NOT NULL,
    bias_value DOUBLE PRECISION,
    observed_post_i_drain DOUBLE PRECISION,
    predicted_post_i_drain DOUBLE PRECISION,
    residual_slog_i_drain DOUBLE PRECISION,
    residual_abs_i_drain DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_iv_validation_model
    ON iv_prediction_validation_residuals(model_run_id);
CREATE INDEX IF NOT EXISTS idx_iv_validation_category
    ON iv_prediction_validation_residuals(measurement_category);
