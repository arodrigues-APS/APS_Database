-- Single-event effect extraction outputs for irradiation monitor waveforms.
-- Populated by data_processing_scripts/extract_single_event_effects.py.

CREATE TABLE IF NOT EXISTS irradiation_single_event_file_summary (
    metadata_id                 INTEGER PRIMARY KEY
                                REFERENCES baselines_metadata(id)
                                ON DELETE CASCADE,
    detector_version            TEXT NOT NULL,
    analyzed_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status                      TEXT NOT NULL,
    skip_reason                 TEXT,

    n_points                    INTEGER,
    n_valid_id                  INTEGER,
    n_valid_ig                  INTEGER,
    has_gate_current            BOOLEAN,
    has_fluence                 BOOLEAN,

    time_start                  DOUBLE PRECISION,
    time_stop                   DOUBLE PRECISION,
    duration_s                  DOUBLE PRECISION,
    fluence_start               DOUBLE PRECISION,
    fluence_stop                DOUBLE PRECISION,
    fluence_min                 DOUBLE PRECISION,
    fluence_max                 DOUBLE PRECISION,
    fluence_span                DOUBLE PRECISION,

    vds_initial_v               DOUBLE PRECISION,
    vds_final_v                 DOUBLE PRECISION,
    vds_min_v                   DOUBLE PRECISION,
    vds_max_v                   DOUBLE PRECISION,
    vds_span_v                  DOUBLE PRECISION,

    id_initial_a                DOUBLE PRECISION,
    id_final_a                  DOUBLE PRECISION,
    id_max_abs_a                DOUBLE PRECISION,
    ig_initial_a                DOUBLE PRECISION,
    ig_final_a                  DOUBLE PRECISION,
    ig_max_abs_a                DOUBLE PRECISION,

    id_noise_sigma_a            DOUBLE PRECISION,
    ig_noise_sigma_a            DOUBLE PRECISION,
    id_step_threshold_a         DOUBLE PRECISION,
    ig_step_threshold_a         DOUBLE PRECISION,

    event_count_total           INTEGER DEFAULT 0,
    seb_count                   INTEGER DEFAULT 0,
    selc_i_count                INTEGER DEFAULT 0,
    selc_ii_count               INTEGER DEFAULT 0,
    mixed_count                 INTEGER DEFAULT 0,
    unknown_count               INTEGER DEFAULT 0,
    dominant_event_type         TEXT,

    event_rate_per_s            DOUBLE PRECISION,
    event_rate_per_fluence      DOUBLE PRECISION,
    event_rate_per_1e5_fluence  DOUBLE PRECISION,

    settings                    JSONB DEFAULT '{}'::jsonb,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS irradiation_single_events (
    id                          SERIAL PRIMARY KEY,
    metadata_id                 INTEGER NOT NULL
                                REFERENCES irradiation_single_event_file_summary(metadata_id)
                                ON DELETE CASCADE,
    event_index                 INTEGER NOT NULL,
    event_type                  TEXT NOT NULL,
    confidence                  DOUBLE PRECISION,

    point_index_start           INTEGER,
    point_index_peak            INTEGER,
    point_index_end             INTEGER,
    cluster_width_points        INTEGER,

    time_start                  DOUBLE PRECISION,
    time_peak                   DOUBLE PRECISION,
    time_end                    DOUBLE PRECISION,
    fluence_start               DOUBLE PRECISION,
    fluence_peak                DOUBLE PRECISION,
    fluence_end                 DOUBLE PRECISION,

    vds_before_v                DOUBLE PRECISION,
    vds_after_v                 DOUBLE PRECISION,
    vds_delta_v                 DOUBLE PRECISION,

    id_before_a                 DOUBLE PRECISION,
    id_after_a                  DOUBLE PRECISION,
    ig_before_a                 DOUBLE PRECISION,
    ig_after_a                  DOUBLE PRECISION,

    delta_id_abs_a              DOUBLE PRECISION,
    delta_ig_abs_a              DOUBLE PRECISION,
    delta_id_signed_a           DOUBLE PRECISION,
    delta_ig_signed_a           DOUBLE PRECISION,
    id_slope_a_per_s            DOUBLE PRECISION,
    ig_slope_a_per_s            DOUBLE PRECISION,
    id_to_ig_delta_ratio        DOUBLE PRECISION,
    residual_id_minus_ig_a      DOUBLE PRECISION,

    id_threshold_a              DOUBLE PRECISION,
    ig_threshold_a              DOUBLE PRECISION,
    evidence                    JSONB DEFAULT '{}'::jsonb,

    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metadata_id, event_index)
);

CREATE INDEX IF NOT EXISTS idx_irr_single_events_metadata
    ON irradiation_single_events(metadata_id);
CREATE INDEX IF NOT EXISTS idx_irr_single_events_type
    ON irradiation_single_events(event_type);
CREATE INDEX IF NOT EXISTS idx_irr_single_events_time
    ON irradiation_single_events(time_peak);
CREATE INDEX IF NOT EXISTS idx_irr_single_events_fluence
    ON irradiation_single_events(fluence_peak);
CREATE INDEX IF NOT EXISTS idx_irr_single_event_summary_status
    ON irradiation_single_event_file_summary(status);
