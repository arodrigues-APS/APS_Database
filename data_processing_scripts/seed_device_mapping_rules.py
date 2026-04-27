#!/usr/bin/env python3
"""
Seed device_mapping_rules from the three legacy hardcoded dicts that will
be retired in Phase 1.7:

  - _EXPERIMENT_RULES  (ingestion_baselines.py:120-125) -> scope='baselines'
  - DEVICE_DIR_MAP     (ingestion_sc.py:79-106)         -> scope='sc'
  - CHIP_ID_TO_DEVICE  (ingestion_irradiation.py:108-158) -> scope='irradiation'

Idempotent via UNIQUE (pattern, pattern_type, scope) + ON CONFLICT DO NOTHING.
Safe to re-run.  Rules whose part_number isn't in device_library are
skipped with a warning — add those devices via the Flask UI (or extend
seed_device_library.py) and re-run.

Phase 1.3 matcher note (recorded here so it isn't lost):

  The legacy scripts apply rules in different orders relative to the
  device_library substring fallback.  The Phase 1.3 common.match_device()
  MUST preserve that per-scope ordering or parity will silently break:

    baselines:  library-substring -> library-prefix -> rules
    sc:         rules -> library-substring
    irradiation rules -> library-substring

  Example of the trap: a path like Cree_80mOhm/IMW120R090M1H_DUT1_IdVg.csv
  in baselines scope.  Legacy Pass 1 finds IMW120R090M1H in the library
  and returns it — _EXPERIMENT_RULES never runs.  If the new matcher
  checks rules first, the CREE_80MOHM substring rule (priority 100)
  fires and returns C2M0080120D (Wolfspeed).  Silent mis-assignment.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "data_processing_scripts"))

from db_config import get_connection
from common import apply_schema


# ── Baselines scope ─────────────────────────────────────────────────────────
# From _EXPERIMENT_RULES plus the CREE_80MOHM filename-override sub-rule.
# Tuple: (pattern, pattern_type, part_number, priority, notes)
#
# The CREE_80MOHM overrides must outrank the default rule (priority 200 >
# 100) so that filenames beginning with I<digit> or containing
# INFINEON/ROHM resolve to the correct non-Wolfspeed part.
#
# Regex patterns are matched against the uppercased path with
# re.IGNORECASE (so case doesn't matter either way).  The [/\\] character
# class handles posix and windows-style separators.

BASELINES_RULES = [
    # CREE_80MOHM overrides — filename-level disambiguation inside the
    # mixed-manufacturer Cree_80mOhm experiment folder.
    (r"CREE_80MOHM.*[/\\]I\d", "regex",     "IMW120R090M1H", 200,
     "Cree_80mOhm override: filename starts with I<digit> -> Infineon"),
    (r"CREE_80MOHM.*INFINEON", "regex",     "IMW120R090M1H", 200,
     "Cree_80mOhm override: filename contains INFINEON -> Infineon"),
    (r"CREE_80MOHM.*ROHM",     "regex",     "SCT3030AL",     200,
     "Cree_80mOhm override: filename contains ROHM -> Rohm"),

    # Default experiment rules — matched only if no override fires first.
    ("ROHM_30MOHM",     "substring", "SCT3030AL",     100, None),
    ("INFINEON_90MOHM", "substring", "IMW120R090M1H", 100, None),
    ("CREE_25MOHM",     "substring", "C2M0025120D",   100, None),
    ("CREE_80MOHM",     "substring", "C2M0080120D",   100, None),
]


# ── SC scope ────────────────────────────────────────────────────────────────
# DEVICE_DIR_MAP keys become path-component regex patterns so a dir name
# like "LF" doesn't substring-match arbitrary filenames.  Entries with
# part_number=None in the legacy dict were "skip this folder" markers;
# omitted here because a miss in the rules table produces the same
# no-match outcome.
#
# Tuple: (dir_name, part_number)

SC_LEGACY_DIRS = [
    # ForDataAnalysis
    ("C2M0080120D",         "C2M0080120D"),
    ("C3M0075120D",         "C3M0075120D"),
    ("C2M_160mohm",         "C2M0280120D"),
    ("IMW120R060",          "IMW120R060M1H"),
    ("SCT2080",             "SCT2080KE"),
    ("SCT3080",             "SCT3080AL"),
    ("LF",                  "LSIC1MO120E0080"),
    ("STM",                 "SCTW35N65G2V"),
    ("STMGen2",             "SCTW35N65G2V"),
    # curvetracermeasurements
    ("CREE3Pin2G",          "C2M0080120D"),
    ("CREE3Pin3G",          "C3M0075120D"),
    ("CREE4Pin3G",          "C3M0075120D"),
    ("Infineon3Pin",        "IMW120R090M1H"),
    ("infineon4Pin",        "IMW120R060M1H"),
    ("RohmPlanar",          "SCT3030AL"),
    ("RohmTrench",          "SCT2080KE"),
    ("Littlefuse",          "LSIC1MO120E0080"),
    ("STMicroelectronic",   "SCTW35N65G2V"),
    ("STMicroGen2",         "SCTW35N65G2V"),
]


def _sc_pattern(dirname):
    """
    Anchor a directory name to parts[0] of a root-stripped path.

    The SC caller strips the SC_ROOT prefix before calling match_device,
    so the path looks like "SCT2080/DUT1/IdVg.csv".  Anchoring at '^'
    reproduces legacy DEVICE_DIR_MAP's parts[0]-exact-match semantics;
    using `(^|/)...` would over-match (e.g. pick up 'Rohmplanar' nested
    inside the legacy no-device folder 'hightemperatureLeakagecurrent').
    """
    import re as _re
    return rf"^{_re.escape(dirname)}(/|$)"


# ── Irradiation scope ───────────────────────────────────────────────────────
# Chip IDs substring-matched against the uppercased path.  The legacy
# matcher runs against the extracted chip_id, but the chip always appears
# verbatim in the filename so path-substring is equivalent.  None-marked
# entries in the legacy dict (category-only catch-alls) are omitted —
# they were "no commercial part, just a category hint" markers that
# don't translate to the rules table.
#
# Tuple: (chip_id, part_number)

IRRADIATION_RULES = [
    # Wolfspeed Gen 3 MOSFETs
    ("CPM312000075A", "CPM3-1200-0075A"),
    ("CPM3120075A",   "CPM3-1200-0075A"),
    ("CM312000075A",  "CPM3-1200-0075A"),
    # Wolfspeed Gen 2 MOSFETs
    ("CPM212000080A", "CPM2-1200-0080A"),
    # Wolfspeed packaged MOSFETs (self-mapping — rule still wins over
    # library substring fallback and produces the same result)
    ("C2M0080120D",   "C2M0080120D"),
    ("C2M0025120D",   "C2M0025120D"),
    ("C3M0075120D",   "C3M0075120D"),
    # Typo correction — observed 2 rows in baselines_metadata with
    # chip_id 'C2M004012D' (missing one zero); the canonical Wolfspeed
    # part is C2M0040120D (1200 V / 40 mΩ).  Legacy left these rows
    # with device_type='C2M004012D', manufacturer=NULL.
    ("C2M004012D",    "C2M0040120D"),
    # Wolfspeed Diodes
    ("CPW51700Z050A",  "CPW5-1700-Z050A"),
    ("CPW517000Z050B", "CPW5-1700-Z050B"),
    ("CPW412000010B",  "CPW4-1200-S010B"),
    ("CPW41200010B",   "CPW4-1200-S010B"),
    ("CPW4-1200-010B", "CPW4-1200-S010B"),
    ("CPW412000020B",  "CPW4-1200-S020B"),
    ("CPW41200S020B",  "CPW4-1200-S020B"),
    ("CPW41200S010B",  "CPW4-1200-S010B"),
    ("CPW41700",       "CPW5-1700-Z050A"),
    ("CPW41700b",      "CPW5-1700-Z050A"),
    # Wolfspeed Diodes — generic ANSTO naming; target parts are
    # placeholder library rows (specific commercial part unknown).
    ("Cree_diode",       "Cree-Diode"),
    ("Cree_diode_1.7kV", "Cree-Diode-1.7kV"),
    ("Cree_diode_1200V", "Cree-Diode-1200V"),
    # Infineon
    ("IFX Trench",    "IFX-Trench"),
    ("IFX Trnech",    "IFX-Trench"),
    ("IFXDiode3x3",   "IFX-Diode-3x3"),
    ("IFX Diode 3x3", "IFX-Diode-3x3"),
    # "12M1H090" is a research-campaign nickname; the name decodes as
    # Infineon 1200V / M1H process / 90 mΩ == IMW120R090M1H, so we
    # collapse the alias here rather than adding a standalone library row.
    ("12M1H090",      "IMW120R090M1H"),
    # Rohm
    ("SCT3030KL",     "SCT3030KL"),
    # Infineon Trench (UCL naming)
    ("Trench",        "IFX-Trench"),
    # VU reference
    ("VU_MOSFET_uncoated", "VU-MOSFET-uncoated"),
]

# Legacy's match_device_from_library in ingestion_irradiation.py falls
# through to a BIDIRECTIONAL library substring test (chip_id in part OR
# part in chip_id).  For generic chip_ids 'diode' / 'Diode' that test
# accidentally matches any library entry containing "Diode" — and in
# the current library that's the three Cree-Diode* placeholders, of
# which Cree-Diode-1200V happens to be returned by the LENGTH-DESC
# iteration.  Lock that observed mapping in explicitly; legacy's choice
# was non-deterministic under library row reordering (same-length
# tie-break is physical-row-order).
IRRADIATION_GENERIC_DIODE_RULES = [
    ("Diode", "Cree-Diode-1200V"),
    ("diode", "Cree-Diode-1200V"),
]


def _load_known_parts(cur):
    cur.execute("SELECT part_number FROM device_library")
    return {row[0] for row in cur.fetchall()}


def _insert(cur, pattern, pattern_type, scope, priority, part_number,
            source_reference, notes, known_parts, skipped):
    if part_number not in known_parts:
        skipped.append((scope, pattern_type, pattern, part_number))
        return 0
    cur.execute(
        """
        INSERT INTO device_mapping_rules
            (pattern, pattern_type, scope, priority, part_number,
             source_reference, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pattern, pattern_type, scope) DO NOTHING
        """,
        (pattern, pattern_type, scope, priority, part_number,
         source_reference, notes),
    )
    return cur.rowcount


def main():
    conn = get_connection()
    try:
        apply_schema(conn)
        cur = conn.cursor()

        known_parts = _load_known_parts(cur)
        print(f"device_library: {len(known_parts)} part numbers loaded")

        # Clear seed-managed rules so pattern changes (e.g. SC anchor
        # tightening in Phase 1.3) take effect on re-run.  User-added
        # rules via the Flask UI have a different source_reference and
        # are preserved.
        cur.execute("""
            DELETE FROM device_mapping_rules
            WHERE source_reference LIKE 'ingestion_%.py::%'
        """)
        deleted = cur.rowcount
        if deleted:
            print(f"Cleared {deleted} seed-managed rules prior to re-insert")

        inserted = {"baselines": 0, "sc": 0, "irradiation": 0}
        skipped = []

        # Baselines
        for pattern, pattern_type, part_number, priority, notes in BASELINES_RULES:
            inserted["baselines"] += _insert(
                cur, pattern, pattern_type, "baselines", priority, part_number,
                "ingestion_baselines.py::match_device_type (_EXPERIMENT_RULES)",
                notes, known_parts, skipped)

        # SC
        for dirname, part_number in SC_LEGACY_DIRS:
            inserted["sc"] += _insert(
                cur, _sc_pattern(dirname), "regex", "sc", 100, part_number,
                "ingestion_sc.py::DEVICE_DIR_MAP",
                f"Matches directory component '{dirname}'",
                known_parts, skipped)

        # Irradiation
        for chip_id, part_number in IRRADIATION_RULES:
            inserted["irradiation"] += _insert(
                cur, chip_id, "substring", "irradiation", 100, part_number,
                "ingestion_irradiation.py::CHIP_ID_TO_DEVICE",
                None, known_parts, skipped)

        # Irradiation generic-diode lock-ins (legacy bidirectional library
        # fallback — see comment on IRRADIATION_GENERIC_DIODE_RULES).
        for chip_id, part_number in IRRADIATION_GENERIC_DIODE_RULES:
            inserted["irradiation"] += _insert(
                cur, chip_id, "substring", "irradiation", 100, part_number,
                "ingestion_irradiation.py::match_device_from_library "
                "(bidirectional library fallback lock-in)",
                "Arbitrary lock-in of legacy non-deterministic behaviour: "
                "chip_id 'Diode'/'diode' had no CHIP_ID_TO_DEVICE entry "
                "but matched any library part containing 'Diode' via a "
                "bidirectional substring test.  Observed result was "
                "Cree-Diode-1200V; locked in here for determinism.",
                known_parts, skipped)

        conn.commit()

        print("\nInserted (new rows only; existing rules left unchanged):")
        for scope, n in inserted.items():
            print(f"  {scope:12s} +{n}")

        if skipped:
            print(f"\nSkipped {len(skipped)} rules "
                  "(part_number missing from device_library):")
            for scope, pattern_type, pattern, part_number in skipped:
                print(f"  [{scope}/{pattern_type}] {pattern!r} -> {part_number!r}")
            print("\nTo include these rules, add the missing part_numbers to "
                  "device_library via the Flask UI (/devices) and re-run "
                  "this script.")

        # Final per-scope totals as a sanity check.
        cur.execute(
            "SELECT scope, COUNT(*) FROM device_mapping_rules "
            "GROUP BY scope ORDER BY scope")
        print("\nCurrent device_mapping_rules state:")
        for scope, count in cur.fetchall():
            print(f"  {scope:12s} {count}")

        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
