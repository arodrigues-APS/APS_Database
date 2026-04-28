-- Core device catalog edited from Flask /devices.
-- This table must exist before device_mapping_rules.sql (FK on part_number).

CREATE TABLE IF NOT EXISTS device_library (
    id                SERIAL PRIMARY KEY,
    part_number       TEXT NOT NULL UNIQUE,
    device_category   TEXT,
    manufacturer      TEXT,
    voltage_rating    TEXT,
    rdson_mohm        TEXT,
    current_rating_a  TEXT,
    package_type      TEXT,
    notes             TEXT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
