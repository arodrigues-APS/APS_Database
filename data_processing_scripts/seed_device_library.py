#!/usr/bin/env python3
"""
Seed the device_library table with the initial set of known devices.

This script is idempotent — it uses INSERT ... ON CONFLICT DO NOTHING
so it can be re-run safely without duplicating rows.

After seeding, admins can add / edit / remove devices through
Superset SQL Lab (or any PostgreSQL client).

Usage:
    source /home/apsadmin/py3/bin/activate
    python3 seed_device_library.py
"""

import sys

try:
    import psycopg2
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

# ── DB Connection ────────────────────────────────────────────────────────────
from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


# ── Device Catalogue ─────────────────────────────────────────────────────────
# Columns: (part_number, device_category, manufacturer,
#           voltage_rating, rdson_mohm, current_rating_a, package_type, notes)

DEVICES = [
    # ── Wolfspeed bare-die Diodes ────────────────────────────────────────
    ("CPW4-1200-S010B", "Diode", "Wolfspeed",
     "1200 V", None, "33", "bare_die", None),
    ("CPW4-1200-S020B", "Diode", "Wolfspeed",
     "1200 V", None, "91", "bare_die", None),
    ("CPW5-1200-Z050B", "Diode", "Wolfspeed",
     "1200 V", None, "50", "bare_die", None),
    ("CPW5-1700-Z025A", "Diode", "Wolfspeed",
     "1700 V", None, "25", "bare_die", None),
    ("CPW5-1700-Z005A", "Diode", "Wolfspeed",
     "1700 V", None, "5", "bare_die", None),

    # ── Wolfspeed bare-die MOSFETs (Gen 2) ──────────────────────────────
    ("CPM2-1200-0025A", "MOSFET", "Wolfspeed",
     "1200 V", "25", None, "bare_die", None),
    ("CPM2-1200-0040A", "MOSFET", "Wolfspeed",
     "1200 V", "40", None, "bare_die", None),
    ("CPM2-1200-0040B", "MOSFET", "Wolfspeed",
     "1200 V", "40", None, "bare_die", None),
    ("CPM2-1200-0080A", "MOSFET", "Wolfspeed",
     "1200 V", "80", None, "bare_die", None),
    ("CPM2-1200-0080B", "MOSFET", "Wolfspeed",
     "1200 V", "80", None, "bare_die", None),
    ("CPM2-1200-0160A", "MOSFET", "Wolfspeed",
     "1200 V", "160", None, "bare_die", None),
    ("CPM2-1200-0160B", "MOSFET", "Wolfspeed",
     "1200 V", "160", None, "bare_die", None),

    # ── Wolfspeed bare-die MOSFETs (Gen 3) ──────────────────────────────
    ("CPM3-0650-0015A", "MOSFET", "Wolfspeed",
     "650 V", "15", None, "bare_die", None),
    ("CPM3-1200-0060A", "MOSFET", "Wolfspeed",
     "1200 V", "60", None, "bare_die", None),
    ("CPM3-0900-0065B", "MOSFET", "Wolfspeed",
     "900 V", "65", None, "bare_die", None),
    ("CPM3-1200-0032A", "MOSFET", "Wolfspeed",
     "1200 V", "32", None, "bare_die", None),
    ("CPM3-1200-0075A", "MOSFET", "Wolfspeed",
     "1200 V", "75", None, "bare_die", None),

    # ── Wolfspeed packaged TO-247 (Gen 2) ────────────────────────────────
    ("C2M0080120D", "MOSFET", "Wolfspeed",
     "1200 V", "80", None, "TO-247", None),
    ("C2M0025120D", "MOSFET", "Wolfspeed",
     "1200 V", "25", None, "TO-247", None),
    ("C2M0280120D", "MOSFET", "Wolfspeed",
     "1200 V", "280", None, "TO-247", None),

    # ── Wolfspeed packaged TO-247 (Gen 3) ────────────────────────────────
    ("C3M0075120D", "MOSFET", "Wolfspeed",
     "1200 V", "75", None, "TO-247", None),
    ("C3M0065090D", "MOSFET", "Wolfspeed",
     "900 V", "65", None, "TO-247", None),

    # ── Wolfspeed home-made TO packages ──────────────────────────────────
    # (same part numbers as bare-die, different package_type)
    # These are already covered by the bare_die entries above — the
    # part_number will match regardless.  Add separate entries only if
    # the researcher wants distinct rows per package variant.

    # ── Infineon bare dies ───────────────────────────────────────────────
    ("IFX-3x3mm", "MOSFET", "Infineon",
     None, None, None, "bare_die", "Unknown specs"),
    ("IFX-5x5mm", "MOSFET", "Infineon",
     None, None, None, "bare_die", "Unknown specs; one has a bondwire"),

    # ── Infineon packaged ────────────────────────────────────────────────
    ("IMW120R090M1H", "MOSFET", "Infineon",
     "1200 V", "90", None, "TO-247", None),

    # ── Rohm packaged ────────────────────────────────────────────────────
    ("SCT3030AL", "MOSFET", "Rohm",
     "1200 V", "30", None, "TO-247", None),
    ("SCT2080KE", "MOSFET", "Rohm",
     "1200 V", "80", None, "TO-247", "Trench SiC MOSFET"),
    ("SCT3080AL", "MOSFET", "Rohm",
     "1200 V", "80", None, "TO-247", "Planar SiC MOSFET"),

    # ── Infineon packaged (additional) ──────────────────────────────────
    ("IMW120R060M1H", "MOSFET", "Infineon",
     "1200 V", "60", None, "TO-247", None),

    # ── Littlefuse packaged ─────────────────────────────────────────────
    ("LSIC1MO120E0080", "MOSFET", "Littlefuse",
     "1200 V", "80", None, "TO-247", None),

    # ── STMicroelectronics packaged ─────────────────────────────────────
    ("SCTW35N65G2V", "MOSFET", "STMicroelectronics",
     "650 V", "55", None, "TO-247", "Gen 1 SiC MOSFET"),
]


def main():
    print("=" * 70)
    print("Seeding device_library table")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 70)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # Ensure the table exists (same DDL as ingestion_baselines.py)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_library (
            id SERIAL PRIMARY KEY,
            part_number  TEXT NOT NULL UNIQUE,
            device_category TEXT,
            manufacturer TEXT,
            voltage_rating TEXT,
            rdson_mohm   TEXT,
            current_rating_a TEXT,
            package_type TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    inserted = 0
    skipped = 0
    for row in DEVICES:
        cur.execute("""
            INSERT INTO device_library
                (part_number, device_category, manufacturer,
                 voltage_rating, rdson_mohm, current_rating_a,
                 package_type, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (part_number) DO NOTHING
        """, row)
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM device_library")
    total = cur.fetchone()[0]

    print(f"\n  Inserted: {inserted}")
    print(f"  Skipped (already exist): {skipped}")
    print(f"  Total devices in table: {total}")

    print("\nTo manage devices, use Superset SQL Lab:")
    print("  -- View all devices:")
    print("  SELECT * FROM device_library ORDER BY manufacturer, part_number;")
    print()
    print("  -- Add a new device:")
    print("  INSERT INTO device_library (part_number, device_category, manufacturer,")
    print("    voltage_rating, rdson_mohm, package_type)")
    print("  VALUES ('NEW-PART-NUMBER', 'MOSFET', 'Manufacturer',")
    print("    '1200 V', '80', 'bare_die');")
    print()
    print("  -- Remove a device:")
    print("  DELETE FROM device_library WHERE part_number = 'OLD-PART';")
    print("=" * 70)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
