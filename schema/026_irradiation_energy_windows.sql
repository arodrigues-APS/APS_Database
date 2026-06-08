-- Derived active-beam and energy-integration windows for irradiation monitor
-- waveforms.  Raw rows remain in baselines_measurements; this table records
-- the auditable window/censoring decision used by energy calculations.

CREATE TABLE IF NOT EXISTS irradiation_waveform_windows (
    metadata_id                 INTEGER PRIMARY KEY
                                REFERENCES baselines_metadata(id)
                                ON DELETE CASCADE,

    active_start_s              DOUBLE PRECISION,
    active_end_s                DOUBLE PRECISION,
    energy_start_s              DOUBLE PRECISION,
    energy_end_s                DOUBLE PRECISION,

    active_window_basis         TEXT NOT NULL DEFAULT 'not_analyzed',
    active_window_confidence    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    energy_censored_reason      TEXT NOT NULL DEFAULT 'not_analyzed',
    compliance_source           TEXT,
    compliance_current_a        DOUBLE PRECISION,
    failure_time_s              DOUBLE PRECISION,
    energy_is_comparable        BOOLEAN NOT NULL DEFAULT FALSE,

    settings                    JSONB DEFAULT '{}'::jsonb,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE irradiation_waveform_windows
    ADD COLUMN IF NOT EXISTS active_start_s DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS active_end_s DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS energy_start_s DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS energy_end_s DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS active_window_basis TEXT NOT NULL DEFAULT 'not_analyzed',
    ADD COLUMN IF NOT EXISTS active_window_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS energy_censored_reason TEXT NOT NULL DEFAULT 'not_analyzed',
    ADD COLUMN IF NOT EXISTS compliance_source TEXT,
    ADD COLUMN IF NOT EXISTS compliance_current_a DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS failure_time_s DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS energy_is_comparable BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS settings JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_irrad_waveform_windows_basis
    ON irradiation_waveform_windows(active_window_basis);
CREATE INDEX IF NOT EXISTS idx_irrad_waveform_windows_censor
    ON irradiation_waveform_windows(energy_censored_reason);
CREATE INDEX IF NOT EXISTS idx_irrad_waveform_windows_comparable
    ON irradiation_waveform_windows(energy_is_comparable);


CREATE TABLE IF NOT EXISTS irradiation_waveform_point_flags (
    metadata_id                 INTEGER NOT NULL
                                REFERENCES baselines_metadata(id)
                                ON DELETE CASCADE,
    point_index                 INTEGER NOT NULL,
    is_active_beam              BOOLEAN NOT NULL DEFAULT FALSE,
    is_pre_failure              BOOLEAN NOT NULL DEFAULT TRUE,
    is_energy_integrable        BOOLEAN NOT NULL DEFAULT FALSE,
    exclusion_reason            TEXT,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metadata_id, point_index)
);

ALTER TABLE irradiation_waveform_point_flags
    ADD COLUMN IF NOT EXISTS is_active_beam BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_pre_failure BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS is_energy_integrable BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS exclusion_reason TEXT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_irrad_waveform_point_flags_integrable
    ON irradiation_waveform_point_flags(metadata_id, is_energy_integrable);
CREATE INDEX IF NOT EXISTS idx_irrad_waveform_point_flags_reason
    ON irradiation_waveform_point_flags(exclusion_reason);
