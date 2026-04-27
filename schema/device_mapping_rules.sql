-- Rules that map file-path / chip-ID patterns to a device_library entry.
-- Retires the hardcoded _EXPERIMENT_RULES (ingestion_baselines.py),
-- DEVICE_DIR_MAP (ingestion_sc.py), and CHIP_ID_TO_DEVICE
-- (ingestion_irradiation.py) once the ingestion scripts are swapped to
-- consult this table via common.match_device().
--
-- Precedence (applied in common.match_device):
--   1. WHERE scope IN (caller_scope, 'all')
--   2. ORDER BY priority DESC, LENGTH(pattern) DESC
--   3. First pattern that matches the path wins.
-- `device_library` must already exist before this file is applied — it
-- is created by ingestion_baselines.py or seed_device_library.py.

CREATE TABLE IF NOT EXISTS device_mapping_rules (
    id                    SERIAL PRIMARY KEY,
    pattern               TEXT NOT NULL,
    pattern_type          TEXT NOT NULL DEFAULT 'substring'
                          CHECK (pattern_type IN ('substring', 'regex')),
    scope                 TEXT NOT NULL DEFAULT 'all'
                          CHECK (scope IN ('all', 'baselines', 'sc',
                                           'irradiation', 'avalanche')),
    priority              INTEGER NOT NULL DEFAULT 100,
    part_number           TEXT NOT NULL
                          REFERENCES device_library(part_number)
                          ON UPDATE CASCADE,
    source_reference      TEXT,
    notes                 TEXT,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pattern, pattern_type, scope)
);

CREATE INDEX IF NOT EXISTS idx_device_mapping_rules_scope_priority
    ON device_mapping_rules(scope, priority DESC, LENGTH(pattern) DESC);
