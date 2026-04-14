#!/usr/bin/env python3
"""
Seed irradiation campaign tables and create SQL views for the dashboard.

Creates (all idempotent — safe to re-run):
  1. irradiation_campaigns table
  2. experiment_campaign_map table
  3. irrad_campaign_id + irrad_role columns on baselines_metadata
  4. Known campaigns and experiment-to-campaign mappings
  5. Backfills baselines_metadata from experiment_campaign_map
  6. Three SQL views:
       irradiation_view
       irradiation_degradation_summary
       irradiation_campaign_overview

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 seed_irradiation_campaigns.py
"""

import sys

try:
    import psycopg2
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


# ── Known Campaigns ───────────────────────────────────────────────────────────
# (campaign_name, facility, ion_species, beam_energy_mev, beam_type,
#  fluence_range, let_mev_cm2_mg, notes)

CAMPAIGNS = [
    ("GSI_March_2025_Au",
     "GSI Darmstadt", "Au", 1162.0, "broad_beam",
     None, None,
     "Gold ions at 1162 MeV, March 2025 campaign at GSI"),
    ("Padova_Proton",
     "LNL Padova", "proton", None, "broad_beam",
     None, None,
     "Proton irradiation, Wolfspeed 2nd generation, Jan 2024"),
    ("GSI_Ca_2022",
     "GSI Darmstadt", "Ca", None, "broad_beam",
     None, None,
     "Calcium ions, January 2022 campaign at GSI"),
    ("D2019_Proton",
     None, "proton", None, None,
     None, None,
     "Proton irradiation test campaign, 2019"),
    ("PSI_Proton_2022",
     "PSI Villigen", "proton", None, "broad_beam",
     None, None,
     "Proton irradiation, August 2022 at PSI"),
    ("UCL_Ions_2023",
     "UCL", "Fe", None, "broad_beam",
     None, None,
     "Heavy ion irradiation, March 2023 at UCL"),
]

# (experiment_name_in_baselines_metadata, campaign_name, role)
# role is 'pre_irrad' or 'post_irrad'
EXPERIMENT_MAPPINGS = [
    ("MOSFET_preCharact_GSI_March25",
     "GSI_March_2025_Au", "pre_irrad"),
    ("MOSFET_Blocking_preCharact_GSI_March25",
     "GSI_March_2025_Au", "pre_irrad"),
    ("MOSFET_2ndWolf_proton_irrad",
     "Padova_Proton", "post_irrad"),
    ("D2019_ProtonTestPreIV",
     "D2019_Proton", "pre_irrad"),
]


# ── Table DDL ─────────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS irradiation_campaigns (
    id               SERIAL PRIMARY KEY,
    campaign_name    TEXT NOT NULL UNIQUE,
    facility         TEXT,
    ion_species      TEXT NOT NULL,
    beam_energy_mev  DOUBLE PRECISION,
    beam_type        TEXT,
    fluence_range    TEXT,
    let_mev_cm2_mg   DOUBLE PRECISION,
    date_start       DATE,
    date_end         DATE,
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS experiment_campaign_map (
    id           SERIAL PRIMARY KEY,
    experiment   TEXT NOT NULL UNIQUE,
    campaign_id  INTEGER REFERENCES irradiation_campaigns(id) ON DELETE CASCADE,
    role         TEXT NOT NULL DEFAULT 'post_irrad'
);

-- Add irradiation columns to baselines_metadata (idempotent)
DO $$ BEGIN
    ALTER TABLE baselines_metadata
        ADD COLUMN irrad_campaign_id INTEGER REFERENCES irradiation_campaigns(id);
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE baselines_metadata ADD COLUMN irrad_role TEXT;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_campaign
    ON baselines_metadata(irrad_campaign_id);
CREATE INDEX IF NOT EXISTS idx_baselines_meta_irrad_role
    ON baselines_metadata(irrad_role);
"""


# ── SQL Views ─────────────────────────────────────────────────────────────────

VIEWS_SQL = """
-- ── irradiation_view ────────────────────────────────────────────────────────
-- Main denormalized view. All IV data linked to an irradiation campaign.
-- Mirrors sc_ruggedness_view but JOINs to irradiation_campaigns instead of
-- filtering by data_source.
DROP VIEW IF EXISTS irradiation_view CASCADE;
CREATE VIEW irradiation_view AS
SELECT
    m.id               AS measurement_id,
    md.id              AS metadata_id,
    md.experiment,
    md.device_id,
    md.device_type,
    md.manufacturer,
    md.measurement_type,
    md.measurement_category,
    md.filename,
    md.irrad_role      AS test_condition,
    ic.campaign_name,
    ic.ion_species,
    ic.beam_energy_mev,
    ic.beam_type,
    ic.facility,
    ic.fluence_range,
    ic.let_mev_cm2_mg,
    -- Human-readable series label: "Au 1162.0 MeV broad_beam"
    COALESCE(ic.ion_species, '?') || ' ' ||
        COALESCE(ic.beam_energy_mev::text, '?') || ' MeV ' ||
        COALESCE(ic.beam_type, '') AS irrad_condition_label,
    md.sweep_start, md.sweep_stop, md.sweep_points,
    md.bias_value, md.drain_bias_value,
    m.point_index,
    m.step_index,
    -- Keithley 9.9E37 overflow sentinel → NULL
    CASE WHEN m.v_gate  IS NOT NULL AND ABS(m.v_gate)  < 1e30
         THEN m.v_gate  ELSE NULL END AS v_gate,
    CASE WHEN m.i_gate  IS NOT NULL AND ABS(m.i_gate)  < 1e30
         THEN m.i_gate  ELSE NULL END AS i_gate,
    CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
         THEN m.v_drain ELSE NULL END AS v_drain,
    CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
         THEN m.i_drain ELSE NULL END AS i_drain,
    -- 0.01 V bin resolution (matches baselines_view convention)
    CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
         THEN ROUND(m.v_gate::numeric, 2) ELSE NULL END AS v_gate_bin,
    CASE
        WHEN md.measurement_category IN ('IdVg', 'Vth')
             AND md.drain_bias_value IS NOT NULL
        THEN ROUND(md.drain_bias_value::numeric, 2)
        WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
        THEN ROUND(m.v_drain::numeric, 2)
        ELSE NULL
    END AS v_drain_bin,
    m.rds, m.bv, m.time_val
FROM baselines_measurements m
JOIN baselines_metadata     md ON m.metadata_id         = md.id
JOIN irradiation_campaigns  ic ON md.irrad_campaign_id  = ic.id
WHERE md.irrad_campaign_id IS NOT NULL;


-- ── irradiation_degradation_summary ─────────────────────────────────────────
-- Aggregated per voltage-bin per condition. Used by the cross-campaign tab.
-- Mirrors sc_degradation_summary.
DROP VIEW IF EXISTS irradiation_degradation_summary CASCADE;
CREATE VIEW irradiation_degradation_summary AS
SELECT
    md.device_type,
    md.manufacturer,
    md.device_id,
    md.irrad_role      AS test_condition,
    ic.campaign_name,
    ic.ion_species,
    ic.beam_energy_mev,
    ic.beam_type,
    COALESCE(ic.ion_species, '?') || ' ' ||
        COALESCE(ic.beam_energy_mev::text, '?') || ' MeV' AS irrad_condition_label,
    md.measurement_category,
    ROUND(m.v_gate::numeric, 2) AS v_gate_bin,
    AVG(m.i_drain)              AS avg_i_drain,
    AVG(m.i_gate)               AS avg_i_gate,
    AVG(ABS(m.i_drain))         AS avg_abs_i_drain,
    COUNT(*)                    AS n_points
FROM baselines_measurements m
JOIN baselines_metadata     md ON m.metadata_id        = md.id
JOIN irradiation_campaigns  ic ON md.irrad_campaign_id = ic.id
WHERE md.irrad_campaign_id IS NOT NULL
  AND (m.v_gate  IS NULL OR ABS(m.v_gate)  < 1e30)
  AND (m.i_drain IS NULL OR ABS(m.i_drain) < 1e30)
GROUP BY
    md.device_type, md.manufacturer, md.device_id,
    md.irrad_role, ic.campaign_name, ic.ion_species,
    ic.beam_energy_mev, ic.beam_type,
    md.measurement_category,
    ROUND(m.v_gate::numeric, 2);


-- ── irradiation_campaign_overview ───────────────────────────────────────────
-- Summary counts per campaign × device type × condition. Used by overview tab.
DROP VIEW IF EXISTS irradiation_campaign_overview CASCADE;
CREATE VIEW irradiation_campaign_overview AS
SELECT
    ic.campaign_name,
    ic.ion_species,
    ic.beam_energy_mev,
    ic.beam_type,
    ic.facility,
    md.device_type,
    md.manufacturer,
    md.irrad_role      AS test_condition,
    md.measurement_category,
    COUNT(DISTINCT md.device_id) AS n_devices,
    COUNT(DISTINCT md.id)        AS n_files,
    SUM(md.num_points)           AS n_points
FROM baselines_metadata    md
JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
WHERE md.irrad_campaign_id IS NOT NULL
GROUP BY
    ic.campaign_name, ic.ion_species, ic.beam_energy_mev,
    ic.beam_type, ic.facility,
    md.device_type, md.manufacturer,
    md.irrad_role, md.measurement_category;
"""


# ── Backfill ──────────────────────────────────────────────────────────────────

BACKFILL_SQL = """
UPDATE baselines_metadata md
SET irrad_campaign_id = ecm.campaign_id,
    irrad_role        = ecm.role
FROM experiment_campaign_map ecm
WHERE md.experiment = ecm.experiment
  AND (md.irrad_campaign_id IS DISTINCT FROM ecm.campaign_id
       OR md.irrad_role IS DISTINCT FROM ecm.role);
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Seeding irradiation campaign tables")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 70)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # 1. Create tables + add columns
    print("\n1. Creating tables and adding columns...")
    cur.execute(CREATE_TABLES_SQL)
    conn.commit()
    print("   OK")

    # 2. Seed campaigns
    print("\n2. Seeding irradiation campaigns...")
    inserted_c = 0
    skipped_c = 0
    for (campaign_name, facility, ion_species, beam_energy_mev,
         beam_type, fluence_range, let_mev_cm2_mg, notes) in CAMPAIGNS:
        cur.execute("""
            INSERT INTO irradiation_campaigns
                (campaign_name, facility, ion_species, beam_energy_mev,
                 beam_type, fluence_range, let_mev_cm2_mg, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (campaign_name) DO NOTHING
        """, (campaign_name, facility, ion_species, beam_energy_mev,
              beam_type, fluence_range, let_mev_cm2_mg, notes))
        if cur.rowcount:
            inserted_c += 1
            print(f"   + {campaign_name}")
        else:
            skipped_c += 1
            print(f"   = {campaign_name} (already exists)")
    conn.commit()
    print(f"   Inserted: {inserted_c}, Skipped: {skipped_c}")

    # 3. Seed experiment mappings
    print("\n3. Seeding experiment → campaign mappings...")
    inserted_m = 0
    skipped_m = 0
    for experiment, campaign_name, role in EXPERIMENT_MAPPINGS:
        cur.execute(
            "SELECT id FROM irradiation_campaigns WHERE campaign_name = %s",
            (campaign_name,)
        )
        row = cur.fetchone()
        if not row:
            print(f"   WARNING: campaign '{campaign_name}' not found, skipping")
            continue
        campaign_id = row[0]
        cur.execute("""
            INSERT INTO experiment_campaign_map (experiment, campaign_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (experiment) DO NOTHING
        """, (experiment, campaign_id, role))
        if cur.rowcount:
            inserted_m += 1
            print(f"   + {experiment} → {campaign_name} ({role})")
        else:
            skipped_m += 1
            print(f"   = {experiment} (already mapped)")
    conn.commit()
    print(f"   Inserted: {inserted_m}, Skipped: {skipped_m}")

    # 4. Backfill baselines_metadata
    print("\n4. Backfilling baselines_metadata from experiment_campaign_map...")
    cur.execute(BACKFILL_SQL)
    n_linked = cur.rowcount
    conn.commit()
    if n_linked:
        print(f"   Linked {n_linked} metadata rows to irradiation campaigns")
    else:
        print("   No rows updated (all already up to date, or no matching experiments)")

    # 5. Show linked counts
    cur.execute("""
        SELECT ic.campaign_name, md.irrad_role, COUNT(DISTINCT md.id) AS n_files
        FROM baselines_metadata md
        JOIN irradiation_campaigns ic ON md.irrad_campaign_id = ic.id
        GROUP BY ic.campaign_name, md.irrad_role
        ORDER BY ic.campaign_name, md.irrad_role
    """)
    rows = cur.fetchall()
    if rows:
        print("\n   Linked metadata summary:")
        for campaign_name, role, n_files in rows:
            print(f"     {campaign_name} / {role}: {n_files} files")

    # 6. Create views
    print("\n5. Creating SQL views...")
    cur.execute(VIEWS_SQL)
    conn.commit()
    print("   Created: irradiation_view")
    print("   Created: irradiation_degradation_summary")
    print("   Created: irradiation_campaign_overview")

    cur.close()
    conn.close()

    print("\n" + "=" * 70)
    print("Done!")
    print()
    print("Next steps:")
    print("  1. Add more experiment mappings via the web UI (/irradiation)")
    print("     and click 'Sync to Metadata' to propagate.")
    print("  2. Run: python3 create_irradiation_dashboard.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
