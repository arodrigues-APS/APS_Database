#!/usr/bin/env python3
"""
Promote Pristine Characterization Files to the Baselines Population
====================================================================
Scans `baselines_metadata` for files that represent clean, pre-stress
characterization of an undamaged device but are currently excluded from
the pristine-population statistics.  Two sources are supported:

  pre_irrad   — rows with data_source='irradiation' AND irrad_role='pre_irrad'
                (captured before a beam run; keep irrad_campaign_id intact
                so the file still appears in the Irradiation dashboard)

  sc_pristine — rows with data_source='sc_ruggedness' AND test_condition='pristine'
                (captured before a short-circuit stress event; keep sample_group
                intact so the file still appears in the SC dashboard)

For each candidate file the gate extracts the relevant electrical parameter
(Vth for IdVg/Vth, Rds(on) for IdVd, V(BR)DSS for Blocking, Vsd for
3rd_Quadrant) and compares it to the pristine-population distribution for
the same device_type.

Gate rule (pure population-only, no datasheet specs):
    promote if |param - median| <= K * IQR  (default K = 3.0)
    else reject with `promotion_decision = 'rejected_<param>'`.

Promotion is a single-column flip: `data_source := 'baselines'`.  All
origin metadata (irrad_campaign_id / irrad_role for pre_irrad; sample_group /
test_condition / sc_condition_label for sc_pristine) is preserved.

The script is idempotent: a second run skips files with
`promotion_decision` already set.  To re-adjudicate a device_type after
the baselines population grows, clear the decision column for those rows
first.

Usage:
    python3 promote_to_baselines.py --source pre_irrad --dry-run
    python3 promote_to_baselines.py --source pre_irrad
    python3 promote_to_baselines.py --source sc_pristine --dry-run
    python3 promote_to_baselines.py --source sc_pristine
    python3 promote_to_baselines.py --source sc_pristine --device-type C2M0080120D
    python3 promote_to_baselines.py --source pre_irrad --min-n 5 --iqr-mult 3.0

Bootstrap mode:
    python3 promote_to_baselines.py --source pre_irrad --bootstrap --dry-run

    Bypasses the IQR gate entirely and flips data_source on trust of the
    pristine label.  Intended to seed empty pristine populations so the
    real gate has something to compare against next run.  Also re-visits
    rows previously parked as 'insufficient_pop' or 'insufficient_data',
    but never re-visits rows that were rejected on the merits.
"""

import argparse
import json
import sys
from time import perf_counter

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import Json

from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


# ── Source configuration ────────────────────────────────────────────────────
SOURCE_CONFIGS = {
    'pre_irrad': {
        'label':       'Pre-Irradiation',
        'predicate':   "data_source = 'irradiation' AND irrad_role = 'pre_irrad'",
        # On promotion, also clear the irradiation-specific degradation flag.
        'flip_extras': "is_likely_irradiated = FALSE,",
    },
    'sc_pristine': {
        'label':       'SC Pristine',
        'predicate':   "data_source = 'sc_ruggedness' AND test_condition = 'pristine'",
        'flip_extras': "",
    },
}


# ── Category → parameter map ────────────────────────────────────────────────
CATEGORY_TO_PARAM = {
    'Vth':          'vth_v',
    'IdVg':         'vth_v',
    'IdVd':         'rdson_mohm',
    'Blocking':     'bvdss_v',
    '3rd_Quadrant': 'vsd_v',
}

PARAM_LABELS = {
    'vth_v':      ('Vth', 'V'),
    'rdson_mohm': ('Rds(on)', 'mΩ'),
    'bvdss_v':    ('V(BR)DSS', 'V'),
    'vsd_v':      ('Vsd', 'V'),
}


# ── Per-file parameter extraction ───────────────────────────────────────────
# Mirrors the extraction logic in BOXPLOT_PARAMS_SQL
# (create_baselines_dashboard_device_library.py:659) but:
#   - groups by md.id (per-file) instead of device_id (per-device)
#   - sources raw baselines_measurements directly, so candidate files
#     that are not yet data_source='baselines' are included
#   - applies the same bin-rounding (0.01 V) + compliance-clamp filter
#     (<99% of max |i_drain|) used by baselines_per_device
EXTRACT_PER_FILE_SQL = """
WITH candidate_ids AS (
    SELECT id FROM baselines_metadata
    WHERE {source_predicate}
      AND ({decision_filter})
      AND device_type IS NOT NULL
      {device_type_filter}
),
/* Per-file binned aggregation — parallels baselines_per_device but keyed
   on md.id, restricted to candidates. */
cpf AS (
    SELECT
        md.id AS metadata_id,
        md.device_type,
        md.manufacturer,
        md.measurement_category,
        ROUND(m.v_gate::numeric, 2)::double precision AS v_gate_bin,
        CASE
            WHEN md.measurement_category IN ('IdVg', 'Vth')
                 AND md.drain_bias_value IS NOT NULL
            THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
            WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
            THEN ROUND(m.v_drain::numeric, 2)::double precision
            ELSE NULL
        END AS v_drain_bin,
        AVG(m.i_drain)       AS avg_i_drain,
        AVG(ABS(m.i_drain))  AS avg_abs_i_drain
    FROM baselines_measurements m
    JOIN baselines_metadata md  ON m.metadata_id = md.id
    JOIN candidate_ids     c    ON c.id = md.id
    LEFT JOIN baselines_run_max_current rmc ON rmc.metadata_id = md.id
    WHERE (m.v_gate  IS NULL OR ABS(m.v_gate)  < 1e30)
      AND (m.v_drain IS NULL OR ABS(m.v_drain) < 1e30)
      AND (m.i_drain IS NULL OR ABS(m.i_drain) < 1e30)
      AND (m.i_drain IS NULL
           OR rmc.max_abs_i_drain IS NULL
           OR ABS(m.i_drain) < 0.99 * rmc.max_abs_i_drain)
    GROUP BY
        md.id, md.device_type, md.manufacturer, md.measurement_category,
        ROUND(m.v_gate::numeric, 2)::double precision,
        CASE
            WHEN md.measurement_category IN ('IdVg', 'Vth')
                 AND md.drain_bias_value IS NOT NULL
            THEN ROUND(md.drain_bias_value::numeric, 2)::double precision
            WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
            THEN ROUND(m.v_drain::numeric, 2)::double precision
            ELSE NULL
        END
),

/* ── Vth per file ──────────────────────────────────────────────────────── */
vth_peak AS (
    SELECT metadata_id,
           GREATEST(0.005, MAX(avg_i_drain) * 0.01) AS i_thresh
    FROM cpf
    WHERE measurement_category IN ('Vth', 'IdVg')
      AND avg_i_drain > 0
    GROUP BY metadata_id
),
vth_cross AS (
    SELECT c.metadata_id, c.v_drain_bin,
           MIN(c.v_gate_bin) AS vth_v
    FROM cpf c
    JOIN vth_peak p USING (metadata_id)
    WHERE c.measurement_category IN ('Vth', 'IdVg')
      AND c.avg_i_drain >= p.i_thresh
    GROUP BY c.metadata_id, c.v_drain_bin
),
vth_min_vds AS (
    SELECT metadata_id, MIN(ABS(v_drain_bin)) AS min_abs_vds
    FROM vth_cross GROUP BY metadata_id
),
vth_ext AS (
    SELECT c.metadata_id, c.vth_v, c.v_drain_bin AS vth_test_vds
    FROM vth_cross c
    JOIN vth_min_vds m USING (metadata_id)
    WHERE ABS(c.v_drain_bin) = m.min_abs_vds
),

/* ── Rds(on) per file ──────────────────────────────────────────────────── */
rdson_bias AS (
    SELECT metadata_id,
           MAX(v_gate_bin)  AS max_vgs,
           MAX(v_drain_bin) AS max_vds
    FROM cpf
    WHERE measurement_category = 'IdVd'
      AND avg_i_drain > 0
    GROUP BY metadata_id
),
rdson_ext AS (
    SELECT c.metadata_id,
           SUM(c.v_drain_bin * c.v_drain_bin) /
             NULLIF(SUM(c.v_drain_bin * c.avg_i_drain), 0) * 1000.0
               AS rdson_mohm,
           b.max_vgs AS rdson_test_vgs
    FROM cpf c
    JOIN rdson_bias b USING (metadata_id)
    WHERE c.measurement_category = 'IdVd'
      AND c.v_gate_bin  BETWEEN b.max_vgs - 1.0 AND b.max_vgs + 1.0
      AND c.v_drain_bin >  0.0
      AND c.v_drain_bin <= LEAST(b.max_vds * 0.15, 2.0)
      AND c.avg_i_drain >  0.0
    GROUP BY c.metadata_id, b.max_vgs
),

/* ── V(BR)DSS per file ─────────────────────────────────────────────────── */
bvdss_crossed AS (
    SELECT metadata_id, MIN(v_drain_bin) AS bvdss_v
    FROM cpf
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND avg_abs_i_drain >= 100e-6
    GROUP BY metadata_id
),
bvdss_held AS (
    SELECT metadata_id, MAX(v_drain_bin) AS bvdss_v
    FROM cpf
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND metadata_id NOT IN (SELECT metadata_id FROM bvdss_crossed)
    GROUP BY metadata_id
),
bvdss_ext AS (
    SELECT * FROM bvdss_crossed
    UNION ALL
    SELECT * FROM bvdss_held
),

/* ── Vsd per file ──────────────────────────────────────────────────────── */
q3_anchor AS (
    /* Body-diode forward drop is the source-drain voltage at a given
       reverse current when the channel is off.  Anchor to the LEAST
       negative gate bin where Id<0 (typically Vgs ≈ 0); MIN() would
       pick the deep blocking bias and sample the off-state I-V, not
       the body-diode.                                                   */
    SELECT metadata_id,
           MAX(v_gate_bin) AS anchor_vgs
    FROM cpf
    WHERE measurement_category = '3rd_Quadrant'
      AND avg_i_drain < 0
    GROUP BY metadata_id
),
q3_target AS (
    /* Compute the reference current WITHIN the anchor slice.  HAVING
       drops files whose peak |Id| at the anchor is below 10 mA — that
       is reverse leakage, not diode conduction, and Vsd extracted
       there would be the noise-floor knee.  The LEFT JOIN downstream
       then yields NULL Vsd so the candidate honestly flows to
       insufficient_data instead of polluting the population.           */
    SELECT c.metadata_id,
           a.anchor_vgs,
           MIN(c.avg_i_drain) * 0.1 AS target_id
    FROM cpf c
    JOIN q3_anchor a USING (metadata_id)
    WHERE c.measurement_category = '3rd_Quadrant'
      AND c.avg_i_drain < 0
      AND c.v_gate_bin BETWEEN a.anchor_vgs - 0.5 AND a.anchor_vgs + 0.5
    GROUP BY c.metadata_id, a.anchor_vgs
    HAVING ABS(MIN(c.avg_i_drain)) >= 0.010
),
vsd_ranked AS (
    SELECT c.metadata_id, ABS(c.v_drain_bin) AS vsd_v,
           ROW_NUMBER() OVER (
               PARTITION BY c.metadata_id
               ORDER BY ABS(c.avg_i_drain - t.target_id) ASC
           ) AS rn
    FROM cpf c
    JOIN q3_target t USING (metadata_id)
    WHERE c.measurement_category = '3rd_Quadrant'
      AND c.v_gate_bin BETWEEN t.anchor_vgs - 0.5 AND t.anchor_vgs + 0.5
      AND c.avg_i_drain < 0
),
vsd_ext AS (
    SELECT metadata_id, vsd_v FROM vsd_ranked WHERE rn = 1
)

SELECT md.id              AS metadata_id,
       md.device_id,
       md.device_type,
       md.manufacturer,
       md.measurement_category,
       md.measurement_type,
       md.filename,
       md.irrad_campaign_id,
       vth.vth_v,
       vth.vth_test_vds,
       rd.rdson_mohm,
       rd.rdson_test_vgs,
       bv.bvdss_v,
       vs.vsd_v
FROM baselines_metadata md
JOIN candidate_ids    c   ON c.id = md.id
LEFT JOIN vth_ext     vth ON vth.metadata_id = md.id
LEFT JOIN rdson_ext   rd  ON rd.metadata_id  = md.id
LEFT JOIN bvdss_ext   bv  ON bv.metadata_id  = md.id
LEFT JOIN vsd_ext     vs  ON vs.metadata_id  = md.id
ORDER BY md.device_type, md.device_id, md.measurement_category
"""


# ── Population statistics (pristine-only) ───────────────────────────────────
# Computes median/Q1/Q3 per device_type from the per-device parameter
# values produced by BOXPLOT_PARAMS_SQL.  Inlined here (rather than importing
# from create_baselines_dashboard_device_library) to avoid pulling in the
# Superset API dependency.  baselines_per_device was updated to filter by
# data_source, so this reads pristine-only data.
POPULATION_STATS_SQL = """
WITH
idvd_dev_bias AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_gate_bin)  AS max_vgs,
           MAX(v_drain_bin) AS max_vds
    FROM baselines_per_device
    WHERE measurement_category = 'IdVd'
      AND dev_avg_i_drain > 0
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),
q3_dev_anchor AS (
    /* Body-diode VGS anchor: least-negative bin where Id<0.  See the
       commentary on q3_anchor in EXTRACT_PER_FILE_SQL for why MIN()
       would be wrong here.                                             */
    SELECT device_id, device_type, manufacturer,
           MAX(v_gate_bin) AS anchor_vgs
    FROM baselines_per_device
    WHERE measurement_category = '3rd_Quadrant'
      AND dev_avg_i_drain < 0
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),
vth_dev_peak AS (
    SELECT device_id, device_type, manufacturer,
           GREATEST(0.005, MAX(dev_avg_i_drain) * 0.01) AS i_thresh
    FROM baselines_per_device
    WHERE measurement_category = 'Vth'
      AND dev_avg_i_drain > 0
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),
vth_dev_crossing AS (
    SELECT b.device_id, b.device_type, b.manufacturer, b.v_drain_bin,
           MIN(b.v_gate_bin) AS vth_v
    FROM baselines_per_device b
    JOIN vth_dev_peak t USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'Vth'
      AND b.dev_avg_i_drain >= t.i_thresh
      AND NOT b.is_likely_irradiated
    GROUP BY b.device_id, b.device_type, b.manufacturer, b.v_drain_bin
),
vth_dev_min_vds AS (
    SELECT device_id, device_type, manufacturer,
           MIN(ABS(v_drain_bin)) AS min_abs_vds
    FROM vth_dev_crossing
    GROUP BY device_id, device_type, manufacturer
),
vth_per_device AS (
    SELECT c.device_id, c.device_type, c.manufacturer, c.vth_v
    FROM vth_dev_crossing c
    JOIN vth_dev_min_vds m USING (device_id, device_type, manufacturer)
    WHERE ABS(c.v_drain_bin) = m.min_abs_vds
),
rdson_per_device AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           SUM(b.v_drain_bin * b.v_drain_bin) /
             NULLIF(SUM(b.v_drain_bin * b.dev_avg_i_drain), 0)
               * 1000.0 AS rdson_mohm
    FROM baselines_per_device b
    JOIN idvd_dev_bias m USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'IdVd'
      AND b.v_gate_bin  BETWEEN m.max_vgs - 1.0 AND m.max_vgs + 1.0
      AND b.v_drain_bin >  0.0
      AND b.v_drain_bin <= LEAST(m.max_vds * 0.15, 2.0)
      AND b.dev_avg_i_drain > 0.0
      AND NOT b.is_likely_irradiated
    GROUP BY b.device_id, b.device_type, b.manufacturer
),
vsd_dev_target AS (
    /* Reference current computed WITHIN the anchor slice, with a
       10 mA conduction floor.  Devices whose pristine 3rd_Quadrant
       data is only reverse leakage (µA-range) drop out and the
       outer LEFT JOIN yields NULL Vsd.                                 */
    SELECT b.device_id, b.device_type, b.manufacturer,
           a.anchor_vgs,
           MIN(b.dev_avg_i_drain) * 0.1 AS target_id
    FROM baselines_per_device b
    JOIN q3_dev_anchor a USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = '3rd_Quadrant'
      AND b.dev_avg_i_drain < 0
      AND NOT b.is_likely_irradiated
      AND b.v_gate_bin BETWEEN a.anchor_vgs - 0.5 AND a.anchor_vgs + 0.5
    GROUP BY b.device_id, b.device_type, b.manufacturer, a.anchor_vgs
    HAVING ABS(MIN(b.dev_avg_i_drain)) >= 0.010
),
vsd_dev_ranked AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           ABS(b.v_drain_bin) AS vsd_v,
           ROW_NUMBER() OVER (
               PARTITION BY b.device_id, b.device_type, b.manufacturer
               ORDER BY ABS(b.dev_avg_i_drain - t.target_id) ASC
           ) AS rn
    FROM baselines_per_device b
    JOIN vsd_dev_target t USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = '3rd_Quadrant'
      AND b.v_gate_bin BETWEEN t.anchor_vgs - 0.5 AND t.anchor_vgs + 0.5
      AND b.dev_avg_i_drain < 0
      AND NOT b.is_likely_irradiated
),
vsd_per_device AS (
    SELECT device_id, device_type, manufacturer, vsd_v
    FROM vsd_dev_ranked WHERE rn = 1
),
bvdss_dev_crossed AS (
    SELECT device_id, device_type, manufacturer,
           MIN(v_drain_bin) AS bvdss_v
    FROM baselines_per_device
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND dev_avg_abs_i_drain >= 100e-6
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),
bvdss_dev_held AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_drain_bin) AS bvdss_v
    FROM baselines_per_device
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND NOT is_likely_irradiated
      AND (device_id, device_type, manufacturer) NOT IN (
          SELECT device_id, device_type, manufacturer
          FROM bvdss_dev_crossed
      )
    GROUP BY device_id, device_type, manufacturer
),
bvdss_per_device AS (
    SELECT * FROM bvdss_dev_crossed
    UNION ALL
    SELECT * FROM bvdss_dev_held
),
all_devices AS (
    SELECT DISTINCT device_id, device_type, manufacturer FROM (
        SELECT device_id, device_type, manufacturer FROM vth_per_device
        UNION SELECT device_id, device_type, manufacturer FROM rdson_per_device
        UNION SELECT device_id, device_type, manufacturer FROM vsd_per_device
        UNION SELECT device_id, device_type, manufacturer FROM bvdss_per_device
    ) u
),
per_device AS (
    SELECT a.device_type,
           v.vth_v,
           CASE WHEN r.rdson_mohm > 0 AND r.rdson_mohm < 1e6
                THEN r.rdson_mohm ELSE NULL END AS rdson_mohm,
           s.vsd_v,
           bv.bvdss_v
    FROM all_devices a
    LEFT JOIN vth_per_device    v  USING (device_id, device_type, manufacturer)
    LEFT JOIN rdson_per_device  r  USING (device_id, device_type, manufacturer)
    LEFT JOIN vsd_per_device    s  USING (device_id, device_type, manufacturer)
    LEFT JOIN bvdss_per_device  bv USING (device_id, device_type, manufacturer)
)
SELECT
    device_type,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY vth_v)      AS vth_med,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vth_v)      AS vth_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vth_v)      AS vth_q3,
    COUNT(vth_v)                                             AS vth_n,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY rdson_mohm) AS rdson_med,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rdson_mohm) AS rdson_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rdson_mohm) AS rdson_q3,
    COUNT(rdson_mohm)                                        AS rdson_n,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bvdss_v)    AS bvdss_med,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bvdss_v)    AS bvdss_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bvdss_v)    AS bvdss_q3,
    COUNT(bvdss_v)                                           AS bvdss_n,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY vsd_v)      AS vsd_med,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vsd_v)      AS vsd_q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vsd_v)      AS vsd_q3,
    COUNT(vsd_v)                                             AS vsd_n
FROM per_device
WHERE device_type IS NOT NULL
GROUP BY device_type
"""


# ── Decision logic ──────────────────────────────────────────────────────────

def decide(extracted, pop, param, iqr_mult, min_n):
    """
    Return (decision, reason, margins_dict) for one candidate file.

    extracted : dict with key param (from EXTRACT_PER_FILE_SQL row)
    pop       : dict with <param>_med, <param>_q1, <param>_q3, <param>_n
    param     : one of 'vth_v', 'rdson_mohm', 'bvdss_v', 'vsd_v'
    iqr_mult  : bandwidth multiplier (e.g. 3.0)
    min_n     : minimum pristine population size

    Outcomes:
      ('promoted',          'vth_v=1.85 within 3.00*IQR of med=2.02', {...})
      ('rejected_vth_v',    'vth_v=-0.12 outside 3.00*IQR of med=2.02', {...})
      ('insufficient_data', 'no <param> extractable', {})
      ('insufficient_pop',  'device_type has 2 baselines (<5)', {})
    """
    value = extracted.get(param)
    if value is None:
        return ('insufficient_data',
                f'no {PARAM_LABELS[param][0]} extractable from file', {})

    stem = {'vth_v': 'vth', 'rdson_mohm': 'rdson',
            'bvdss_v': 'bvdss', 'vsd_v': 'vsd'}[param]
    n   = pop.get(f'{stem}_n') or 0
    med = pop.get(f'{stem}_med')
    q1  = pop.get(f'{stem}_q1')
    q3  = pop.get(f'{stem}_q3')

    if n < min_n or med is None or q1 is None or q3 is None:
        return ('insufficient_pop',
                f'device_type has {n} pristine baselines (<{min_n})', {})

    iqr = float(q3) - float(q1)
    low  = float(med) - iqr_mult * iqr
    high = float(med) + iqr_mult * iqr

    margins = {
        param:        float(value),
        f'{param}_median': float(med),
        f'{param}_iqr':    iqr,
        f'{param}_low':    low,
        f'{param}_high':   high,
        f'{param}_n_pop':  int(n),
        f'{param}_zscore_iqr':
            (float(value) - float(med)) / iqr if iqr > 0 else None,
    }

    label, unit = PARAM_LABELS[param]
    if low <= float(value) <= high:
        return ('promoted',
                f'{label}={float(value):.3f} {unit} within '
                f'{iqr_mult:.2f}·IQR of median={float(med):.3f} {unit} '
                f'(n={n})',
                margins)
    else:
        return (f'rejected_{param}',
                f'{label}={float(value):.3f} {unit} outside '
                f'[{low:.3f}, {high:.3f}] {unit} '
                f'(median={float(med):.3f}, IQR={iqr:.3f}, n={n})',
                margins)


# ── Database actions ────────────────────────────────────────────────────────

def fetch_candidates(cur, source_predicate, device_type_filter, bootstrap=False):
    extra = ""
    params = []
    if device_type_filter:
        extra = "AND device_type = %s"
        params = [device_type_filter]
    if bootstrap:
        # Re-visit rows previously blocked by the gate's preconditions,
        # including insufficient_data (no extractable parameter).
        # Don't touch rows that failed on the merits (rejected_*) or were
        # already promoted/bootstrapped.
        decision_filter = (
            "promotion_decision IS NULL "
            "OR promotion_decision IN ('insufficient_pop', 'insufficient_data')"
        )
    else:
        # Also pick up any insufficient_pop rows from a previous run so that
        # re-running without --bootstrap auto-bootstraps them if the population
        # is still too small.
        decision_filter = (
            "promotion_decision IS NULL "
            "OR promotion_decision = 'insufficient_pop'"
        )
    sql = EXTRACT_PER_FILE_SQL.format(
        source_predicate=source_predicate,
        decision_filter=decision_filter,
        device_type_filter=extra,
    )
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_population_stats(cur):
    cur.execute(POPULATION_STATS_SQL)
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def apply_decision(cur, metadata_id, decision, reason, margins, flip_extras):
    """Write the audit row.  Promoted and bootstrapped rows flip data_source."""
    if decision in ('promoted', 'bootstrapped'):
        cur.execute(f"""
            UPDATE baselines_metadata
            SET data_source        = 'baselines',
                {flip_extras}
                promotion_decision = %s,
                promotion_reason   = %s,
                promotion_ts       = NOW(),
                gate_params        = %s
            WHERE id = %s
        """, (decision, reason, Json(margins), metadata_id))
    else:
        cur.execute("""
            UPDATE baselines_metadata
            SET promotion_decision = %s,
                promotion_reason   = %s,
                promotion_ts       = NOW(),
                gate_params        = %s
            WHERE id = %s
        """, (decision, reason, Json(margins), metadata_id))


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True,
                    choices=list(SOURCE_CONFIGS),
                    help="Which pristine file population to adjudicate "
                         "(pre_irrad | sc_pristine)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report decisions without writing to the DB")
    ap.add_argument("--min-n", type=int, default=5,
                    help="Minimum pristine population size per device_type "
                         "(default: 5)")
    ap.add_argument("--iqr-mult", type=float, default=3.0,
                    help="Bandwidth multiplier around the median (default: 3.0)")
    ap.add_argument("--device-type", type=str, default=None,
                    help="Only adjudicate files for this device_type")
    ap.add_argument("--bootstrap", action="store_true",
                    help="Bypass the IQR gate and promote every candidate on "
                         "trust of its pristine label. Picks up rows still "
                         "open (NULL or 'insufficient_data') plus 'insufficient_pop'. "
                         "Use when you also want to sweep files whose parameters "
                         "could not be extracted (insufficient_data) — empty "
                         "populations are handled automatically without this flag. "
                         "Rejected rows are NOT re-visited.")
    args = ap.parse_args()

    cfg = SOURCE_CONFIGS[args.source]

    print("=" * 72)
    print(f"{cfg['label']} → Baselines Promotion Gate")
    print(f"Target: postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
    if args.bootstrap:
        print("Gate:   BYPASSED (bootstrap mode — trusting pristine label)")
    else:
        print(f"Gate:   median ± {args.iqr_mult:.2f}·IQR   "
              f"min_n={args.min_n}")
    if args.device_type:
        print(f"Filter: device_type = {args.device_type}")
    if args.dry_run:
        print("MODE:   DRY RUN (no database writes)")
    print("=" * 72)

    t0 = perf_counter()
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASSWORD)
    conn.autocommit = False
    cur = conn.cursor()

    print("\nFetching pristine population stats per device_type...")
    pop_stats = fetch_population_stats(cur)
    print(f"  {len(pop_stats)} device_type(s) with pristine data")
    for dt, s in sorted(pop_stats.items()):
        ns = f"Vth:{s['vth_n'] or 0} Rds:{s['rdson_n'] or 0} " \
             f"BV:{s['bvdss_n'] or 0} Vf:{s['vsd_n'] or 0}"
        print(f"    {dt:30s}  n[{ns}]")

    print("\nExtracting parameters per candidate file...")
    candidates = fetch_candidates(cur, cfg['predicate'], args.device_type,
                                  bootstrap=args.bootstrap)
    print(f"  {len(candidates)} {args.source} file(s) to adjudicate")

    counts = {}
    print(f"\n{'─' * 72}")
    hdr = f"{'#id':>6}  {'device_type':<22}  {'cat':<14}  " \
          f"{'param':<12} {'value':>10}  decision → reason"
    print(hdr)
    print("─" * 72)

    for cand in candidates:
        mid  = cand['metadata_id']
        dt   = cand['device_type']
        cat  = cand['measurement_category']
        param = CATEGORY_TO_PARAM.get(cat)

        if args.bootstrap:
            # Bypass the gate entirely; trust the pristine label.
            # Extract the parameter when possible so the audit row still
            # records the value, but do not compare against any population.
            value = cand.get(param) if param else None
            margins = {}
            if param and value is not None:
                margins[param] = float(value)
            if param is None:
                reason = f"bootstrap: category {cat!r} not gatable"
            elif value is None:
                label = PARAM_LABELS[param][0]
                reason = f"bootstrap: no {label} extractable (gate bypassed)"
            else:
                label, unit = PARAM_LABELS[param]
                reason = (f"bootstrap: {label}={float(value):.3f} {unit} "
                          f"(gate bypassed)")
            decision = 'bootstrapped'
        elif param is None:
            decision = 'insufficient_data'
            reason   = f"measurement_category={cat!r} not gatable"
            margins  = {}
        else:
            pop = pop_stats.get(dt, {})
            decision, reason, margins = decide(
                cand, pop, param, args.iqr_mult, args.min_n
            )
            if decision == 'insufficient_pop':
                # Auto-bootstrap when no gate population exists — no need for
                # a separate --bootstrap pass just because the pool is empty.
                value = cand.get(param)
                label, unit = PARAM_LABELS[param]
                reason = (f"auto-bootstrap: {label}={float(value):.3f} {unit} "
                          f"(insufficient population)")
                margins = {param: float(value)}
                decision = 'bootstrapped'

        counts[decision] = counts.get(decision, 0) + 1

        value_str = "—"
        if param and cand.get(param) is not None:
            value_str = f"{cand[param]:.3g}"

        print(f"{mid:>6}  {(dt or '?'):<22.22}  {cat:<14.14}  "
              f"{param or '—':<12.12} {value_str:>10}  "
              f"{decision} → {reason[:80]}")

        if not args.dry_run:
            apply_decision(cur, mid, decision, reason, margins,
                           cfg['flip_extras'])

    if not args.dry_run:
        conn.commit()

    print("─" * 72)
    print(f"\nSummary ({sum(counts.values())} total):")
    for k in sorted(counts):
        print(f"  {k:30s}  {counts[k]}")

    # Show net effect on the population for device_types that gained rows.
    flipped = counts.get('promoted', 0) + counts.get('bootstrapped', 0)
    if not args.dry_run and flipped > 0:
        print("\nUpdated per-device_type pristine file counts:")
        cur.execute("""
            SELECT device_type,
                   COUNT(*) FILTER (WHERE data_source IS NULL
                                      OR data_source = 'baselines') AS n_baselines,
                   COUNT(*) FILTER (WHERE promotion_decision = 'promoted')
                                                                    AS n_promoted,
                   COUNT(*) FILTER (WHERE promotion_decision = 'bootstrapped')
                                                                    AS n_bootstrapped
            FROM baselines_metadata
            WHERE device_type IS NOT NULL
            GROUP BY device_type
            HAVING COUNT(*) FILTER
                (WHERE promotion_decision IN ('promoted', 'bootstrapped')) > 0
            ORDER BY device_type
        """)
        for dt, n_bl, n_pr, n_bs in cur.fetchall():
            parts = []
            if n_pr: parts.append(f"promoted: {n_pr}")
            if n_bs: parts.append(f"bootstrapped: {n_bs}")
            print(f"    {dt:30s}  baselines={n_bl:>4}  ({', '.join(parts)})")

    cur.close()
    conn.close()
    elapsed = perf_counter() - t0
    print(f"\n{'=' * 72}")
    if args.dry_run:
        print(f"DRY RUN complete — no changes written ({elapsed:.1f}s).")
    else:
        print(f"Done ({elapsed:.1f}s).")
    print("=" * 72)


if __name__ == "__main__":
    main()
