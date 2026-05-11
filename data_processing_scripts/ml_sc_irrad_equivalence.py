#!/usr/bin/env python3
"""
Short-Circuit / Avalanche / Irradiation Damage-Equivalence Views
================================================================
Build tri-source damage-equivalence SQL views (SC, avalanche, irradiation),
plus nearest-neighbor artifacts for irradiation → SC matching.

Bridge metric: median (ΔVth, ΔRds(on), ΔBV) per stress event, where
pristine baseline is the device_type-wide median across all files
labeled pristine / pre_irrad.  Runs extract_damage_metrics.py first
if gate_params is not populated.

What it does:
  1. Creates/refreshes SQL views:
       * `damage_equivalence_view` contains one row per SC condition,
         avalanche sample group, and irradiation run, each with median
         ΔVth / ΔRds / ΔBV, IQR, and sample counts.
       * `damage_equivalence_match_view` ranks nearest fingerprints across
         three pairings: SC↔irradiation, SC↔avalanche, avalanche↔irradiation.
       * `damage_equivalence_coverage_view` summarizes which device types
         have enough SC/irradiation overlap to compare.
       * `damage_equivalence_match_segment_view` expands rank-1 usable
         matches into two endpoints so Superset can draw focused links.
       * `damage_equivalence_prediction_*` views add a separate exploratory
         V2 predicted-irradiation layer without mutating the measured views.
  2. In Python: builds a per-device-type damage-space nearest-neighbor
     retriever. The distance metric is a reliability-weighted Euclidean
     score over available axes (ΔVth, ΔRds, ΔBV), normalized by per-axis
     robust scale. Missing axes are dropped pairwise.
  3. Writes `out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv` and
     two scatter plots (ΔVth vs ΔRds, ΔVth vs ΔBV).

CLI prediction mode (uses the cached view):
    python3 ml_sc_irrad_equivalence.py --ion Au --energy 1162 --let 67.1 \
        --device-type C2M0080120D

Usage:
    python3 ml_sc_irrad_equivalence.py --rebuild       # build view + plots
    python3 ml_sc_irrad_equivalence.py --ion Au --energy 1162 --let 67.1
    python3 ml_sc_irrad_equivalence.py --device-type C2M0080120D --rebuild
"""

import argparse
import csv
import sys
from pathlib import Path

try:
    import numpy as np
except ImportError:
    sys.exit("numpy is required: pip install --break-system-packages numpy")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("matplotlib is required: "
             "pip install --break-system-packages matplotlib")

from db_config import get_connection


# ── SQL view ────────────────────────────────────────────────────────────────
# Pristine baseline: median Vth/Rds/BV per device_type, computed over the same
# source-aware reference population used by the device-library views.  The
# reference_device_key keeps campaign/source/sample context in the physical
# device identity before the final device_type median is taken.
DAMAGE_VIEW_SQL = """
DROP VIEW IF EXISTS damage_equivalence_prediction_match_segment_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_prediction_match_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_prediction_coverage_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_prediction_fingerprint_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_match_segment_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_match_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_coverage_view CASCADE;
DROP VIEW IF EXISTS damage_equivalence_view CASCADE;

CREATE VIEW damage_equivalence_view AS
WITH pristine_pool AS (
    SELECT CONCAT_WS(
               ':',
               COALESCE(NULLIF(data_source, ''), 'baselines'),
               COALESCE(irrad_campaign_id::text, NULLIF(experiment, ''), 'no-context'),
               COALESCE(NULLIF(sample_group, ''), NULLIF(device_id, ''), 'metadata-' || id::text)
           ) AS reference_device_key,
           device_type,
           (gate_params->>'vth_v')::double precision      AS vth,
           (gate_params->>'rdson_mohm')::double precision AS rds,
           (gate_params->>'bvdss_v')::double precision    AS bv
    FROM baselines_metadata
    WHERE device_type IS NOT NULL
      AND gate_params IS NOT NULL
      AND NOT is_likely_irradiated
      AND (
            (data_source IS NULL OR data_source = 'baselines')
         OR irrad_role = 'pre_irrad'
         OR test_condition IN ('pristine', 'pre_avalanche')
      )
),
pristine_devices AS (
    SELECT reference_device_key,
           device_type,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vth) AS vth,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rds) AS rds,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bv)  AS bv
    FROM pristine_pool
    GROUP BY reference_device_key, device_type
),
pristine_stats AS (
    SELECT device_type,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vth) AS pristine_vth,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rds) AS pristine_rds,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bv)  AS pristine_bv
    FROM pristine_devices
    GROUP BY device_type
),
sc_per_file AS (
    SELECT md.device_type,
           md.sample_group,
           md.sc_voltage_v,
           md.sc_duration_us,
           CASE WHEN ABS((md.gate_params->>'vth_v')::double precision
                         - p.pristine_vth) <= 10.0
                THEN (md.gate_params->>'vth_v')::double precision - p.pristine_vth
                ELSE NULL END AS dvth,
           CASE WHEN ABS((md.gate_params->>'rdson_mohm')::double precision
                         - p.pristine_rds) <= 10000.0
                THEN (md.gate_params->>'rdson_mohm')::double precision - p.pristine_rds
                ELSE NULL END AS drds,
           CASE WHEN ABS((md.gate_params->>'bvdss_v')::double precision
                         - p.pristine_bv) <= 2000.0
                THEN (md.gate_params->>'bvdss_v')::double precision - p.pristine_bv
                ELSE NULL END AS dbv
    FROM baselines_metadata md
    JOIN pristine_stats p USING (device_type)
    WHERE md.data_source = 'sc_ruggedness'
      AND md.test_condition = 'post_sc'
      AND md.sc_voltage_v IS NOT NULL
      AND md.sc_duration_us IS NOT NULL
      AND md.gate_params IS NOT NULL
),
sc_fp AS (
    SELECT device_type, sc_voltage_v, sc_duration_us,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dvth)      AS dvth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth) AS dvth_iqr,
           COUNT(dvth)                                            AS dvth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY drds)      AS drds,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds) AS drds_iqr,
           COUNT(drds)                                            AS drds_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dbv)       AS dbv,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)  AS dbv_iqr,
           COUNT(dbv)                                             AS dbv_n,
           COUNT(DISTINCT sample_group)                           AS n_samples
    FROM sc_per_file
    GROUP BY device_type, sc_voltage_v, sc_duration_us
),
irrad_per_file AS (
    SELECT md.device_type,
           md.device_id,
           md.irrad_run_id,
           CASE WHEN ABS((md.gate_params->>'vth_v')::double precision
                         - p.pristine_vth) <= 10.0
                THEN (md.gate_params->>'vth_v')::double precision - p.pristine_vth
                ELSE NULL END AS dvth,
           CASE WHEN ABS((md.gate_params->>'rdson_mohm')::double precision
                         - p.pristine_rds) <= 10000.0
                THEN (md.gate_params->>'rdson_mohm')::double precision - p.pristine_rds
                ELSE NULL END AS drds,
           CASE WHEN ABS((md.gate_params->>'bvdss_v')::double precision
                         - p.pristine_bv) <= 2000.0
                THEN (md.gate_params->>'bvdss_v')::double precision - p.pristine_bv
                ELSE NULL END AS dbv
    FROM baselines_metadata md
    JOIN pristine_stats p USING (device_type)
    WHERE md.irrad_role = 'post_irrad'
      AND md.irrad_run_id IS NOT NULL
      AND md.gate_params IS NOT NULL
),
irrad_fp AS (
    SELECT ipf.device_type, ipf.irrad_run_id,
           r.ion_species, r.beam_energy_mev, r.let_surface,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dvth)      AS dvth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth) AS dvth_iqr,
           COUNT(dvth)                                            AS dvth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY drds)      AS drds,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds) AS drds_iqr,
           COUNT(drds)                                            AS drds_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dbv)       AS dbv,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)  AS dbv_iqr,
           COUNT(dbv)                                             AS dbv_n,
           COUNT(DISTINCT device_id)                              AS n_samples
    FROM irrad_per_file ipf
    JOIN irradiation_runs r ON r.id = ipf.irrad_run_id
    GROUP BY ipf.device_type, ipf.irrad_run_id,
             r.ion_species, r.beam_energy_mev, r.let_surface
),
avalanche_per_file AS (
    SELECT md.device_type,
           COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''), 'unknown')
                                                        AS avalanche_sample_group,
           md.device_id,
           CASE WHEN ABS((md.gate_params->>'vth_v')::double precision
                         - p.pristine_vth) <= 10.0
                THEN (md.gate_params->>'vth_v')::double precision - p.pristine_vth
                ELSE NULL END AS dvth,
           CASE WHEN ABS((md.gate_params->>'rdson_mohm')::double precision
                         - p.pristine_rds) <= 10000.0
                THEN (md.gate_params->>'rdson_mohm')::double precision - p.pristine_rds
                ELSE NULL END AS drds,
           CASE WHEN ABS((md.gate_params->>'bvdss_v')::double precision
                         - p.pristine_bv) <= 2000.0
                THEN (md.gate_params->>'bvdss_v')::double precision - p.pristine_bv
                ELSE NULL END AS dbv
    FROM baselines_metadata md
    JOIN pristine_stats p USING (device_type)
    WHERE md.data_source = 'curve_tracer_avalanche_iv'
      AND md.test_condition = 'post_avalanche'
      AND md.gate_params IS NOT NULL
),
avalanche_fp AS (
    SELECT device_type, avalanche_sample_group,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dvth)      AS dvth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth) AS dvth_iqr,
           COUNT(dvth)                                            AS dvth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY drds)      AS drds,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds) AS drds_iqr,
           COUNT(drds)                                            AS drds_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dbv)       AS dbv,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)  AS dbv_iqr,
           COUNT(dbv)                                             AS dbv_n,
           COUNT(DISTINCT COALESCE(NULLIF(device_id, ''), avalanche_sample_group))
                                                        AS n_samples
    FROM avalanche_per_file
    GROUP BY device_type, avalanche_sample_group
),
raw_fp AS (
    SELECT 'sc'::text                             AS source,
           device_type,
           sc_voltage_v, sc_duration_us,
           NULL::text                              AS ion_species,
           NULL::double precision                  AS beam_energy_mev,
           NULL::double precision                  AS let_surface,
           NULL::integer                           AS irrad_run_id,
           NULL::text                              AS avalanche_sample_group,
           dvth, dvth_iqr, dvth_n,
           drds, drds_iqr, drds_n,
           dbv,  dbv_iqr,  dbv_n,
           n_samples,
           sc_voltage_v::text || 'V / '
             || sc_duration_us::text || 'us'       AS label
    FROM sc_fp
    UNION ALL
    SELECT 'irrad'::text                          AS source,
           device_type,
           NULL::double precision                 AS sc_voltage_v,
           NULL::double precision                 AS sc_duration_us,
           ion_species, beam_energy_mev, let_surface,
           irrad_run_id,
           NULL::text                             AS avalanche_sample_group,
           dvth, dvth_iqr, dvth_n,
           drds, drds_iqr, drds_n,
           dbv,  dbv_iqr,  dbv_n,
           n_samples,
           COALESCE(ion_species, '?')
             || ' @ ' || COALESCE(beam_energy_mev::text, '?') || ' MeV'
             || ' (LET ' || COALESCE(let_surface::text, '?') || ')' AS label
    FROM irrad_fp
    UNION ALL
    SELECT 'avalanche'::text                      AS source,
           device_type,
           NULL::double precision                 AS sc_voltage_v,
           NULL::double precision                 AS sc_duration_us,
           NULL::text                             AS ion_species,
           NULL::double precision                 AS beam_energy_mev,
           NULL::double precision                 AS let_surface,
           NULL::integer                          AS irrad_run_id,
           avalanche_sample_group,
           dvth, dvth_iqr, dvth_n,
           drds, drds_iqr, drds_n,
           dbv,  dbv_iqr,  dbv_n,
           n_samples,
           'sample ' || avalanche_sample_group    AS label
    FROM avalanche_fp
),
source_counts AS (
    SELECT device_type,
           COUNT(*) FILTER (WHERE source = 'sc')        AS device_sc_count,
           COUNT(*) FILTER (WHERE source = 'irrad')     AS device_irrad_count,
           COUNT(*) FILTER (WHERE source = 'avalanche') AS device_avalanche_count
    FROM raw_fp
    GROUP BY device_type
)
SELECT fp.*,
       sc.device_sc_count,
       sc.device_irrad_count,
       sc.device_avalanche_count,
       CASE
         WHEN sc.device_sc_count > 0 AND sc.device_irrad_count > 0
              AND sc.device_avalanche_count > 0
           THEN 'SC + irradiation + avalanche'
         WHEN sc.device_sc_count > 0 AND sc.device_irrad_count > 0
           THEN 'SC + irradiation'
         WHEN sc.device_sc_count > 0 AND sc.device_avalanche_count > 0
           THEN 'SC + avalanche'
         WHEN sc.device_irrad_count > 0 AND sc.device_avalanche_count > 0
           THEN 'irradiation + avalanche'
         WHEN sc.device_sc_count > 0
           THEN 'SC only'
         WHEN sc.device_irrad_count > 0
           THEN 'irradiation only'
         WHEN sc.device_avalanche_count > 0
           THEN 'avalanche only'
         ELSE 'no data'
       END AS device_pair_status
FROM raw_fp fp
JOIN source_counts sc USING (device_type);

CREATE VIEW damage_equivalence_match_view AS
WITH pair_defs AS (
    SELECT 'sc_vs_irradiation'::text AS pair_type,
           'sc'::text AS left_source, 'irrad'::text AS right_source
    UNION ALL
    SELECT 'sc_vs_avalanche'::text,
           'sc'::text, 'avalanche'::text
    UNION ALL
    SELECT 'avalanche_vs_irradiation'::text,
           'avalanche'::text, 'irrad'::text
),
fp AS (
    SELECT * FROM damage_equivalence_view
),
axis_stats AS (
    SELECT device_type,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth)
             FILTER (WHERE dvth IS NOT NULL)              AS dvth_q1,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             FILTER (WHERE dvth IS NOT NULL)              AS dvth_q3,
           STDDEV_SAMP(dvth)                              AS dvth_std,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds)
             FILTER (WHERE drds IS NOT NULL)              AS drds_q1,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             FILTER (WHERE drds IS NOT NULL)              AS drds_q3,
           STDDEV_SAMP(drds)                              AS drds_std,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)
             FILTER (WHERE dbv IS NOT NULL)               AS dbv_q1,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             FILTER (WHERE dbv IS NOT NULL)               AS dbv_q3,
           STDDEV_SAMP(dbv)                               AS dbv_std
    FROM fp
    GROUP BY device_type
),
axis_scales AS (
    SELECT device_type,
           GREATEST(
             COALESCE(NULLIF(dvth_q3 - dvth_q1, 0.0) / 1.349,
                      NULLIF(dvth_std, 0.0), 1.0),
             1e-6
           ) AS dvth_scale,
           GREATEST(
             COALESCE(NULLIF(drds_q3 - drds_q1, 0.0) / 1.349,
                      NULLIF(drds_std, 0.0), 1.0),
             1e-6
           ) AS drds_scale,
           GREATEST(
             COALESCE(NULLIF(dbv_q3 - dbv_q1, 0.0) / 1.349,
                      NULLIF(dbv_std, 0.0), 1.0),
             1e-6
           ) AS dbv_scale
    FROM axis_stats
),
pairs_raw AS (
    SELECT pd.pair_type,
           pd.left_source,
           pd.right_source,
           rf.device_type,
           lf.label AS left_label,
           rf.label AS right_label,
           CASE
             WHEN rf.source = 'irrad'
               THEN COALESCE(rf.irrad_run_id::text, rf.label)
             WHEN rf.source = 'avalanche'
               THEN COALESCE(rf.avalanche_sample_group, rf.label)
             ELSE COALESCE(rf.label, '?')
           END AS right_fingerprint_key,
           lf.sc_voltage_v AS left_sc_voltage_v,
           lf.sc_duration_us AS left_sc_duration_us,
           lf.ion_species AS left_ion_species,
           lf.beam_energy_mev AS left_beam_energy_mev,
           lf.let_surface AS left_let_surface,
           lf.irrad_run_id AS left_irrad_run_id,
           lf.avalanche_sample_group AS left_avalanche_sample_group,
           rf.sc_voltage_v AS right_sc_voltage_v,
           rf.sc_duration_us AS right_sc_duration_us,
           rf.ion_species AS right_ion_species,
           rf.beam_energy_mev AS right_beam_energy_mev,
           rf.let_surface AS right_let_surface,
           rf.irrad_run_id AS right_irrad_run_id,
           rf.avalanche_sample_group AS right_avalanche_sample_group,
           lf.dvth AS left_dvth,
           lf.dvth_iqr AS left_dvth_iqr,
           lf.dvth_n AS left_dvth_n,
           lf.drds AS left_drds,
           lf.drds_iqr AS left_drds_iqr,
           lf.drds_n AS left_drds_n,
           lf.dbv AS left_dbv,
           lf.dbv_iqr AS left_dbv_iqr,
           lf.dbv_n AS left_dbv_n,
           lf.n_samples AS left_n_samples,
           rf.dvth AS right_dvth,
           rf.dvth_iqr AS right_dvth_iqr,
           rf.dvth_n AS right_dvth_n,
           rf.drds AS right_drds,
           rf.drds_iqr AS right_drds_iqr,
           rf.drds_n AS right_drds_n,
           rf.dbv AS right_dbv,
           rf.dbv_iqr AS right_dbv_iqr,
           rf.dbv_n AS right_dbv_n,
           rf.n_samples AS right_n_samples,
           ax.dvth_scale,
           ax.drds_scale,
           ax.dbv_scale,
           CASE WHEN rf.dvth IS NOT NULL AND lf.dvth IS NOT NULL
                THEN 1 ELSE 0 END AS has_dvth,
           CASE WHEN rf.drds IS NOT NULL AND lf.drds IS NOT NULL
                THEN 1 ELSE 0 END AS has_drds,
           CASE WHEN rf.dbv IS NOT NULL AND lf.dbv IS NOT NULL
                THEN 1 ELSE 0 END AS has_dbv,
           CASE WHEN rf.dvth IS NOT NULL AND lf.dvth IS NOT NULL
                THEN ABS(rf.dvth - lf.dvth) END AS abs_delta_dvth,
           CASE WHEN rf.drds IS NOT NULL AND lf.drds IS NOT NULL
                THEN ABS(rf.drds - lf.drds) END AS abs_delta_drds,
           CASE WHEN rf.dbv IS NOT NULL AND lf.dbv IS NOT NULL
                THEN ABS(rf.dbv - lf.dbv) END AS abs_delta_dbv,
           CASE WHEN rf.dvth IS NOT NULL AND lf.dvth IS NOT NULL THEN
             SQRT(
               (SQRT(GREATEST(COALESCE(rf.dvth_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(rf.dvth_iqr, 0.0))))
               *
               (SQRT(GREATEST(COALESCE(lf.dvth_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(lf.dvth_iqr, 0.0))))
             )
           END AS dvth_weight,
           CASE WHEN rf.drds IS NOT NULL AND lf.drds IS NOT NULL THEN
             SQRT(
               (SQRT(GREATEST(COALESCE(rf.drds_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(rf.drds_iqr, 0.0))))
               *
               (SQRT(GREATEST(COALESCE(lf.drds_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(lf.drds_iqr, 0.0))))
             )
           END AS drds_weight,
           CASE WHEN rf.dbv IS NOT NULL AND lf.dbv IS NOT NULL THEN
             SQRT(
               (SQRT(GREATEST(COALESCE(rf.dbv_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(rf.dbv_iqr, 0.0))))
               *
               (SQRT(GREATEST(COALESCE(lf.dbv_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(lf.dbv_iqr, 0.0))))
             )
           END AS dbv_weight
    FROM pair_defs pd
    JOIN fp lf ON lf.source = pd.left_source
    JOIN fp rf ON rf.source = pd.right_source
              AND rf.device_type = lf.device_type
    JOIN axis_scales ax ON ax.device_type = lf.device_type
),
pairs AS (
    SELECT pr.*,
           (has_dvth + has_drds + has_dbv) AS comparable_axes,
           CONCAT_WS(', ',
             CASE WHEN has_dvth = 1 THEN 'ΔVth' END,
             CASE WHEN has_drds = 1 THEN 'ΔRds(on)' END,
             CASE WHEN has_dbv = 1 THEN 'ΔV(BR)DSS' END
           ) AS comparable_axis_labels,
           SQRT(
             (
               COALESCE(dvth_weight
                 * POWER((right_dvth - left_dvth) / dvth_scale, 2), 0.0)
               + COALESCE(drds_weight
                 * POWER((right_drds - left_drds) / drds_scale, 2), 0.0)
               + COALESCE(dbv_weight
                 * POWER((right_dbv - left_dbv) / dbv_scale, 2), 0.0)
             )
             / NULLIF(
               COALESCE(dvth_weight, 0.0)
               + COALESCE(drds_weight, 0.0)
               + COALESCE(dbv_weight, 0.0),
               0.0
             )
           ) AS nearest_distance
    FROM pairs_raw pr
),
ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (
             PARTITION BY pair_type, device_type, right_fingerprint_key
             ORDER BY nearest_distance ASC NULLS LAST,
                      comparable_axes DESC,
                      left_label ASC
           ) AS match_rank,
           COUNT(*) OVER (
             PARTITION BY pair_type, device_type, right_fingerprint_key
           ) AS left_candidate_count
    FROM pairs p
    WHERE comparable_axes > 0
)
SELECT ranked.*,
       CASE
         WHEN comparable_axes >= 3 AND nearest_distance <= 0.75
           THEN 'strong'
         WHEN comparable_axes >= 2 AND nearest_distance <= 1.5
           THEN 'usable'
         WHEN nearest_distance <= 2.5
           THEN 'weak'
         ELSE 'inspect manually'
       END AS comparability_status,
       right_label AS irrad_label,
       right_ion_species AS ion_species,
       right_beam_energy_mev AS beam_energy_mev,
       right_let_surface AS let_surface,
       right_irrad_run_id AS irrad_run_id,
       left_label AS sc_label,
       left_sc_voltage_v AS sc_voltage_v,
       left_sc_duration_us AS sc_duration_us,
       right_dvth AS irrad_dvth,
       right_dvth_iqr AS irrad_dvth_iqr,
       right_dvth_n AS irrad_dvth_n,
       right_drds AS irrad_drds,
       right_drds_iqr AS irrad_drds_iqr,
       right_drds_n AS irrad_drds_n,
       right_dbv AS irrad_dbv,
       right_dbv_iqr AS irrad_dbv_iqr,
       right_dbv_n AS irrad_dbv_n,
       right_n_samples AS irrad_n_samples,
       left_dvth AS sc_dvth,
       left_dvth_iqr AS sc_dvth_iqr,
       left_dvth_n AS sc_dvth_n,
       left_drds AS sc_drds,
       left_drds_iqr AS sc_drds_iqr,
       left_drds_n AS sc_drds_n,
       left_dbv AS sc_dbv,
       left_dbv_iqr AS sc_dbv_iqr,
       left_dbv_n AS sc_dbv_n,
       left_n_samples AS sc_n_samples,
       left_candidate_count AS sc_candidate_count
FROM ranked;

CREATE VIEW damage_equivalence_coverage_view AS
WITH pair_defs AS (
    SELECT 'sc_vs_irradiation'::text AS pair_type,
           'sc'::text AS left_source, 'irrad'::text AS right_source
    UNION ALL
    SELECT 'sc_vs_avalanche'::text,
           'sc'::text, 'avalanche'::text
    UNION ALL
    SELECT 'avalanche_vs_irradiation'::text,
           'avalanche'::text, 'irrad'::text
),
device_pool AS (
    SELECT DISTINCT device_type FROM damage_equivalence_view
),
fp_counts AS (
    SELECT pd.pair_type,
           pd.left_source,
           pd.right_source,
           d.device_type,
           COUNT(*) FILTER (WHERE fp.source = pd.left_source)  AS n_left_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.right_source) AS n_right_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.left_source AND fp.dvth IS NOT NULL)
                                                            AS left_dvth_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.right_source AND fp.dvth IS NOT NULL)
                                                            AS right_dvth_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.left_source AND fp.drds IS NOT NULL)
                                                            AS left_drds_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.right_source AND fp.drds IS NOT NULL)
                                                            AS right_drds_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.left_source AND fp.dbv IS NOT NULL)
                                                            AS left_dbv_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = pd.right_source AND fp.dbv IS NOT NULL)
                                                            AS right_dbv_fingerprints
    FROM pair_defs pd
    CROSS JOIN device_pool d
    LEFT JOIN damage_equivalence_view fp
           ON fp.device_type = d.device_type
    GROUP BY pd.pair_type, pd.left_source, pd.right_source, d.device_type
),
match_counts AS (
    SELECT pair_type,
           device_type,
           COUNT(*) AS comparable_pair_count,
           COUNT(DISTINCT right_fingerprint_key) AS comparable_right_count,
           MIN(nearest_distance) AS best_distance,
           COUNT(*) FILTER (WHERE comparable_axes >= 2) AS pairs_with_2plus_axes
    FROM damage_equivalence_match_view
    GROUP BY pair_type, device_type
)
SELECT fp.pair_type,
       fp.left_source,
       fp.right_source,
       fp.device_type,
       fp.n_left_fingerprints,
       fp.n_right_fingerprints,
       COALESCE(mc.comparable_pair_count, 0) AS comparable_pair_count,
       COALESCE(mc.comparable_right_count, 0) AS comparable_right_count,
       mc.best_distance,
       CONCAT_WS(', ',
         CASE WHEN fp.left_dvth_fingerprints > 0
                AND fp.right_dvth_fingerprints > 0
              THEN 'ΔVth' END,
         CASE WHEN fp.left_drds_fingerprints > 0
                AND fp.right_drds_fingerprints > 0
              THEN 'ΔRds(on)' END,
         CASE WHEN fp.left_dbv_fingerprints > 0
                AND fp.right_dbv_fingerprints > 0
              THEN 'ΔV(BR)DSS' END
       ) AS comparable_axis_labels,
       fp.left_dvth_fingerprints,
       fp.right_dvth_fingerprints,
       fp.left_drds_fingerprints,
       fp.right_drds_fingerprints,
       fp.left_dbv_fingerprints,
       fp.right_dbv_fingerprints,
       CASE
         WHEN fp.n_left_fingerprints = 0 OR fp.n_right_fingerprints = 0
           THEN 'missing counterpart'
         WHEN COALESCE(mc.comparable_pair_count, 0) = 0
           THEN 'no shared damage axes'
         WHEN mc.best_distance <= 0.75
              AND COALESCE(mc.pairs_with_2plus_axes, 0) > 0
           THEN 'strong matches available'
         WHEN mc.best_distance <= 1.5
              AND COALESCE(mc.pairs_with_2plus_axes, 0) > 0
           THEN 'usable matches available'
         ELSE 'weak/inspect manually'
       END AS comparability_status,
       CASE
         WHEN fp.left_source = 'sc' THEN fp.n_left_fingerprints
         WHEN fp.right_source = 'sc' THEN fp.n_right_fingerprints
         ELSE 0
       END AS n_sc_fingerprints,
       CASE
         WHEN fp.left_source = 'irrad' THEN fp.n_left_fingerprints
         WHEN fp.right_source = 'irrad' THEN fp.n_right_fingerprints
         ELSE 0
       END AS n_irrad_fingerprints,
       CASE
         WHEN fp.left_source = 'avalanche' THEN fp.n_left_fingerprints
         WHEN fp.right_source = 'avalanche' THEN fp.n_right_fingerprints
         ELSE 0
       END AS n_avalanche_fingerprints
FROM fp_counts fp
LEFT JOIN match_counts mc USING (pair_type, device_type);

CREATE VIEW damage_equivalence_match_segment_view AS
SELECT m.pair_type,
       m.left_source,
       m.right_source,
       m.device_type,
       m.right_fingerprint_key,
       m.right_label,
       m.left_label,
       m.right_irrad_run_id AS irrad_run_id,
       m.right_ion_species AS ion_species,
       m.right_beam_energy_mev AS beam_energy_mev,
       m.right_let_surface AS let_surface,
       m.left_sc_voltage_v AS sc_voltage_v,
       m.left_sc_duration_us AS sc_duration_us,
       m.match_rank,
       m.nearest_distance,
       m.comparable_axes,
       m.comparable_axis_labels,
       m.comparability_status,
       m.pair_type || ' | ' || m.device_type || ' | '
         || m.right_label || ' <-> ' || m.left_label AS match_label,
       m.right_source AS endpoint_source,
       m.right_label AS endpoint_label,
       1 AS endpoint_order,
       m.right_dvth AS dvth,
       m.right_drds AS drds,
       m.right_dbv AS dbv,
       m.right_n_samples AS n_samples
FROM damage_equivalence_match_view m
WHERE m.match_rank = 1
  AND m.comparability_status IN ('strong', 'usable')
UNION ALL
SELECT m.pair_type,
       m.left_source,
       m.right_source,
       m.device_type,
       m.right_fingerprint_key,
       m.right_label,
       m.left_label,
       m.right_irrad_run_id AS irrad_run_id,
       m.right_ion_species AS ion_species,
       m.right_beam_energy_mev AS beam_energy_mev,
       m.right_let_surface AS let_surface,
       m.left_sc_voltage_v AS sc_voltage_v,
       m.left_sc_duration_us AS sc_duration_us,
       m.match_rank,
       m.nearest_distance,
       m.comparable_axes,
       m.comparable_axis_labels,
       m.comparability_status,
       m.pair_type || ' | ' || m.device_type || ' | '
         || m.right_label || ' <-> ' || m.left_label AS match_label,
       m.left_source AS endpoint_source,
       m.left_label AS endpoint_label,
       2 AS endpoint_order,
       m.left_dvth AS dvth,
       m.left_drds AS drds,
       m.left_dbv AS dbv,
       m.left_n_samples AS n_samples
FROM damage_equivalence_match_view m
WHERE m.match_rank = 1
  AND m.comparability_status IN ('strong', 'usable');

CREATE VIEW damage_equivalence_prediction_fingerprint_view AS
WITH pristine_pool AS (
    SELECT CONCAT_WS(
               ':',
               COALESCE(NULLIF(data_source, ''), 'baselines'),
               COALESCE(irrad_campaign_id::text, NULLIF(experiment, ''), 'no-context'),
               COALESCE(NULLIF(sample_group, ''), NULLIF(device_id, ''), 'metadata-' || id::text)
           ) AS reference_device_key,
           device_type,
           (gate_params->>'vth_v')::double precision      AS vth,
           (gate_params->>'rdson_mohm')::double precision AS rds,
           (gate_params->>'bvdss_v')::double precision    AS bv
    FROM baselines_metadata
    WHERE device_type IS NOT NULL
      AND gate_params IS NOT NULL
      AND NOT is_likely_irradiated
      AND (
            (data_source IS NULL OR data_source = 'baselines')
         OR irrad_role = 'pre_irrad'
         OR test_condition IN ('pristine', 'pre_avalanche')
      )
),
pristine_devices AS (
    SELECT reference_device_key,
           device_type,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vth) AS vth,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rds) AS rds,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bv)  AS bv
    FROM pristine_pool
    GROUP BY reference_device_key, device_type
),
pristine_stats AS (
    SELECT device_type,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vth) AS pristine_vth,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rds) AS pristine_rds,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bv)  AS pristine_bv
    FROM pristine_devices
    GROUP BY device_type
),
model_context AS (
    SELECT mr.id AS model_run_id,
           mr.model_version,
           mr.algorithm,
           mr.model_status,
           mr.trained_at,
           mr.id = (SELECT MAX(id) FROM iv_physical_model_runs) AS is_latest_model_run
    FROM iv_physical_model_runs mr
),
predicted_axis_rows AS (
    SELECT mc.model_run_id,
           mc.model_version,
           mc.algorithm,
           mc.model_status,
           mc.trained_at,
           mc.is_latest_model_run,
           pp.reference_tier,
           COALESCE(pp.validation_mode_used, 'unknown') AS validation_mode_used,
           rp.device_type,
           COALESCE(pp.irrad_run_id, rp.irrad_run_id) AS irrad_run_id,
           COALESCE(pp.ion_species, rp.ion_species) AS ion_species,
           COALESCE(pp.beam_energy_mev, rp.beam_energy_mev) AS beam_energy_mev,
           COALESCE(pp.let_surface, rp.let_surface) AS let_surface,
           COALESCE(pp.let_bragg_peak, rp.let_bragg_peak) AS let_bragg_peak,
           COALESCE(pp.range_um, rp.range_um) AS range_um,
           COALESCE(pp.beam_type, rp.beam_type) AS beam_type,
           COALESCE(pp.fluence_at_meas, rp.fluence_at_meas) AS fluence_at_meas,
           CASE
             WHEN pp.target_type = 'delta_vth_v'
              AND rp.pre_vth_v IS NOT NULL
              AND pp.predicted_value IS NOT NULL
              AND ps.pristine_vth IS NOT NULL
              AND ABS((rp.pre_vth_v + pp.predicted_value) - ps.pristine_vth) <= 10.0
               THEN (rp.pre_vth_v + pp.predicted_value) - ps.pristine_vth
             ELSE NULL
           END AS dvth,
           CASE
             WHEN pp.target_type = 'log_rdson_ratio'
              AND rp.pre_rdson_mohm IS NOT NULL
              AND rp.pre_rdson_mohm > 0.0
              AND pp.predicted_value IS NOT NULL
              AND ABS(pp.predicted_value) <= 10.0
              AND ps.pristine_rds IS NOT NULL
              AND ABS((rp.pre_rdson_mohm * EXP(pp.predicted_value)) - ps.pristine_rds) <= 10000.0
               THEN (rp.pre_rdson_mohm * EXP(pp.predicted_value)) - ps.pristine_rds
             ELSE NULL
           END AS drds,
           CASE
             WHEN pp.target_type = 'delta_vth_v'
              AND rp.pre_vth_v IS NOT NULL
              AND pp.predicted_value IS NOT NULL
               THEN rp.pre_vth_v + pp.predicted_value
             ELSE NULL
           END AS predicted_post_vth,
           CASE
             WHEN pp.target_type = 'log_rdson_ratio'
              AND rp.pre_rdson_mohm IS NOT NULL
              AND rp.pre_rdson_mohm > 0.0
              AND pp.predicted_value IS NOT NULL
              AND ABS(pp.predicted_value) <= 10.0
               THEN rp.pre_rdson_mohm * EXP(pp.predicted_value)
             ELSE NULL
           END AS predicted_post_rds,
           pp.target_type,
           pp.confidence_level,
           pp.confidence_score,
           pp.donor_count,
           pp.donor_distance,
           pp.validation_gate_pass,
           pp.validation_supported_fraction,
           pp.validation_supported_pairs,
           pp.validation_total_pairs,
           pp.baseline_reference_count,
           pp.baseline_reference_spread,
           pp.baseline_reference_method
    FROM iv_physical_parameter_predictions pp
    JOIN model_context mc ON mc.model_run_id = pp.model_run_id
    JOIN iv_physical_response_pairs rp ON rp.id = pp.pair_id
    JOIN pristine_stats ps ON ps.device_type = rp.device_type
    WHERE pp.stress_type = 'irradiation'
      AND pp.support_status = 'ok'
      AND pp.confidence_level IN ('strong', 'weak')
),
prediction_context AS (
    SELECT DISTINCT model_run_id, model_version, algorithm, model_status,
           trained_at, is_latest_model_run, reference_tier,
           validation_mode_used
    FROM predicted_axis_rows
),
measured_context AS (
    SELECT fp.source,
           pc.model_run_id,
           pc.model_version,
           pc.algorithm,
           pc.model_status,
           pc.trained_at,
           pc.is_latest_model_run,
           pc.reference_tier,
           pc.validation_mode_used,
           false AS is_predicted,
           NULL::text AS prediction_source,
           'measured'::text AS fingerprint_confidence,
           fp.device_type,
           fp.sc_voltage_v,
           fp.sc_duration_us,
           fp.ion_species,
           fp.beam_energy_mev,
           fp.let_surface,
           NULL::double precision AS let_bragg_peak,
           NULL::double precision AS range_um,
           NULL::text AS beam_type,
           NULL::double precision AS fluence_at_meas,
           fp.irrad_run_id,
           fp.avalanche_sample_group,
           fp.dvth,
           fp.dvth_iqr,
           fp.dvth_n,
           fp.drds,
           fp.drds_iqr,
           fp.drds_n,
           fp.dbv,
           fp.dbv_iqr,
           fp.dbv_n,
           fp.n_samples,
           fp.n_samples AS measured_sample_count,
           0::bigint AS prediction_count,
           0::bigint AS strong_prediction_count,
           0::bigint AS weak_prediction_count,
           0::bigint AS dvth_prediction_count,
           0::bigint AS drds_prediction_count,
           0::bigint AS dbv_prediction_count,
           NULL::double precision AS median_confidence_score,
           NULL::double precision AS median_donor_count,
           NULL::double precision AS median_donor_distance,
           NULL::double precision AS median_validation_supported_fraction,
           NULL::integer AS median_validation_supported_pairs,
           NULL::integer AS median_validation_total_pairs,
           NULL::boolean AS validation_gate_pass_all,
           NULL::bigint AS validation_gate_pass_count,
           NULL::double precision AS median_baseline_reference_count,
           NULL::double precision AS median_baseline_reference_spread,
           NULL::text AS baseline_reference_method,
           NULL::double precision AS median_predicted_post_vth,
           NULL::double precision AS median_predicted_post_rds,
           fp.label
    FROM damage_equivalence_view fp
    JOIN prediction_context pc ON TRUE
    WHERE fp.source IN ('sc', 'irrad')
),
predicted_fp AS (
    SELECT 'predicted_irrad'::text AS source,
           model_run_id,
           model_version,
           algorithm,
           model_status,
           trained_at,
           is_latest_model_run,
           reference_tier,
           validation_mode_used,
           true AS is_predicted,
           'iv_physical_v2'::text AS prediction_source,
           CASE
             WHEN COUNT(*) FILTER (WHERE confidence_level = 'weak') > 0
               THEN 'weak'
             ELSE 'strong'
           END AS fingerprint_confidence,
           device_type,
           NULL::double precision AS sc_voltage_v,
           NULL::double precision AS sc_duration_us,
           ion_species,
           beam_energy_mev,
           let_surface,
           let_bragg_peak,
           range_um,
           beam_type,
           fluence_at_meas,
           irrad_run_id,
           NULL::text AS avalanche_sample_group,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dvth)
             FILTER (WHERE dvth IS NOT NULL) AS dvth,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             FILTER (WHERE dvth IS NOT NULL)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth)
               FILTER (WHERE dvth IS NOT NULL) AS dvth_iqr,
           CASE WHEN COUNT(dvth) > 0 THEN 1 ELSE 0 END AS dvth_n,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY drds)
             FILTER (WHERE drds IS NOT NULL) AS drds,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             FILTER (WHERE drds IS NOT NULL)
             - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds)
               FILTER (WHERE drds IS NOT NULL) AS drds_iqr,
           CASE WHEN COUNT(drds) > 0 THEN 1 ELSE 0 END AS drds_n,
           NULL::double precision AS dbv,
           NULL::double precision AS dbv_iqr,
           0::bigint AS dbv_n,
           NULL::bigint AS n_samples,
           0::bigint AS measured_sample_count,
           COUNT(*) AS prediction_count,
           COUNT(*) FILTER (WHERE confidence_level = 'strong') AS strong_prediction_count,
           COUNT(*) FILTER (WHERE confidence_level = 'weak') AS weak_prediction_count,
           COUNT(dvth) AS dvth_prediction_count,
           COUNT(drds) AS drds_prediction_count,
           0::bigint AS dbv_prediction_count,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY confidence_score)
             FILTER (WHERE confidence_score IS NOT NULL) AS median_confidence_score,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY donor_count)
             FILTER (WHERE donor_count IS NOT NULL) AS median_donor_count,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY donor_distance)
             FILTER (WHERE donor_distance IS NOT NULL) AS median_donor_distance,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY validation_supported_fraction)
             FILTER (WHERE validation_supported_fraction IS NOT NULL)
             AS median_validation_supported_fraction,
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY validation_supported_pairs)
             FILTER (WHERE validation_supported_pairs IS NOT NULL))::integer
             AS median_validation_supported_pairs,
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY validation_total_pairs)
             FILTER (WHERE validation_total_pairs IS NOT NULL))::integer
             AS median_validation_total_pairs,
           BOOL_AND(COALESCE(validation_gate_pass, false)) AS validation_gate_pass_all,
           COUNT(*) FILTER (WHERE validation_gate_pass) AS validation_gate_pass_count,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY baseline_reference_count)
             FILTER (WHERE baseline_reference_count IS NOT NULL)
             AS median_baseline_reference_count,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY baseline_reference_spread)
             FILTER (WHERE baseline_reference_spread IS NOT NULL)
             AS median_baseline_reference_spread,
           STRING_AGG(DISTINCT baseline_reference_method, ', ' ORDER BY baseline_reference_method)
             FILTER (WHERE baseline_reference_method IS NOT NULL)
             AS baseline_reference_method,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY predicted_post_vth)
             FILTER (WHERE predicted_post_vth IS NOT NULL) AS median_predicted_post_vth,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY predicted_post_rds)
             FILTER (WHERE predicted_post_rds IS NOT NULL) AS median_predicted_post_rds,
           'predicted '
             || COALESCE(ion_species, '?')
             || ' @ ' || COALESCE(beam_energy_mev::text, '?') || ' MeV'
             || ' (LET ' || COALESCE(let_surface::text, '?') || ')'
             || ' | ' || reference_tier
             || ' | ' || validation_mode_used
             || ' | ' ||
             CASE
               WHEN COUNT(*) FILTER (WHERE confidence_level = 'weak') > 0
                 THEN 'weak'
               ELSE 'strong'
             END AS label
    FROM predicted_axis_rows
    WHERE dvth IS NOT NULL OR drds IS NOT NULL
    GROUP BY model_run_id, model_version, algorithm, model_status, trained_at,
             is_latest_model_run, reference_tier, validation_mode_used,
             device_type, irrad_run_id, ion_species, beam_energy_mev,
             let_surface, let_bragg_peak, range_um, beam_type, fluence_at_meas
),
raw_fp AS (
    SELECT * FROM measured_context
    UNION ALL
    SELECT * FROM predicted_fp
),
source_counts AS (
    SELECT model_run_id,
           reference_tier,
           validation_mode_used,
           device_type,
           COUNT(*) FILTER (WHERE source = 'sc') AS device_sc_count,
           COUNT(*) FILTER (WHERE source = 'irrad') AS device_irrad_count,
           COUNT(*) FILTER (WHERE source = 'predicted_irrad') AS device_predicted_irrad_count
    FROM raw_fp
    GROUP BY model_run_id, reference_tier, validation_mode_used, device_type
)
SELECT fp.*,
       sc.device_sc_count,
       sc.device_irrad_count,
       sc.device_predicted_irrad_count,
       CASE
         WHEN sc.device_sc_count > 0
              AND sc.device_irrad_count > 0
              AND sc.device_predicted_irrad_count > 0
           THEN 'SC + measured irradiation + predicted irradiation'
         WHEN sc.device_sc_count > 0
              AND sc.device_predicted_irrad_count > 0
           THEN 'SC + predicted irradiation'
         WHEN sc.device_irrad_count > 0
              AND sc.device_predicted_irrad_count > 0
           THEN 'measured irradiation + predicted irradiation'
         WHEN sc.device_sc_count > 0 AND sc.device_irrad_count > 0
           THEN 'SC + measured irradiation'
         WHEN sc.device_sc_count > 0
           THEN 'SC only'
         WHEN sc.device_irrad_count > 0
           THEN 'measured irradiation only'
         WHEN sc.device_predicted_irrad_count > 0
           THEN 'predicted irradiation only'
         ELSE 'no data'
       END AS device_pair_status
FROM raw_fp fp
JOIN source_counts sc
  ON sc.model_run_id = fp.model_run_id
 AND sc.reference_tier = fp.reference_tier
 AND sc.validation_mode_used = fp.validation_mode_used
 AND sc.device_type = fp.device_type;

CREATE VIEW damage_equivalence_prediction_match_view AS
WITH fp AS (
    SELECT * FROM damage_equivalence_prediction_fingerprint_view
),
axis_stats AS (
    SELECT model_run_id,
           reference_tier,
           validation_mode_used,
           device_type,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dvth)
             FILTER (WHERE dvth IS NOT NULL) AS dvth_q1,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dvth)
             FILTER (WHERE dvth IS NOT NULL) AS dvth_q3,
           STDDEV_SAMP(dvth) AS dvth_std,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY drds)
             FILTER (WHERE drds IS NOT NULL) AS drds_q1,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY drds)
             FILTER (WHERE drds IS NOT NULL) AS drds_q3,
           STDDEV_SAMP(drds) AS drds_std,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dbv)
             FILTER (WHERE dbv IS NOT NULL) AS dbv_q1,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dbv)
             FILTER (WHERE dbv IS NOT NULL) AS dbv_q3,
           STDDEV_SAMP(dbv) AS dbv_std
    FROM fp
    GROUP BY model_run_id, reference_tier, validation_mode_used, device_type
),
axis_scales AS (
    SELECT model_run_id,
           reference_tier,
           validation_mode_used,
           device_type,
           GREATEST(
             COALESCE(NULLIF(dvth_q3 - dvth_q1, 0.0) / 1.349,
                      NULLIF(dvth_std, 0.0), 1.0),
             1e-6
           ) AS dvth_scale,
           GREATEST(
             COALESCE(NULLIF(drds_q3 - drds_q1, 0.0) / 1.349,
                      NULLIF(drds_std, 0.0), 1.0),
             1e-6
           ) AS drds_scale,
           GREATEST(
             COALESCE(NULLIF(dbv_q3 - dbv_q1, 0.0) / 1.349,
                      NULLIF(dbv_std, 0.0), 1.0),
             1e-6
           ) AS dbv_scale
    FROM axis_stats
),
pairs_raw AS (
    SELECT 'sc_vs_predicted_irrad'::text AS pair_type,
           'sc'::text AS left_source,
           'predicted_irrad'::text AS right_source,
           rf.model_run_id,
           rf.model_version,
           rf.algorithm,
           rf.model_status,
           rf.trained_at,
           rf.is_latest_model_run,
           rf.reference_tier,
           rf.validation_mode_used,
           rf.device_type,
           lf.label AS left_label,
           rf.label AS right_label,
           rf.model_run_id::text || '|'
             || rf.reference_tier || '|'
             || rf.validation_mode_used || '|'
             || COALESCE(rf.irrad_run_id::text, rf.label)
             AS right_fingerprint_key,
           lf.sc_voltage_v AS left_sc_voltage_v,
           lf.sc_duration_us AS left_sc_duration_us,
           rf.ion_species AS right_ion_species,
           rf.beam_energy_mev AS right_beam_energy_mev,
           rf.let_surface AS right_let_surface,
           rf.let_bragg_peak AS right_let_bragg_peak,
           rf.range_um AS right_range_um,
           rf.fluence_at_meas AS right_fluence_at_meas,
           rf.irrad_run_id AS right_irrad_run_id,
           lf.dvth AS left_dvth,
           lf.dvth_iqr AS left_dvth_iqr,
           lf.dvth_n AS left_dvth_n,
           lf.drds AS left_drds,
           lf.drds_iqr AS left_drds_iqr,
           lf.drds_n AS left_drds_n,
           lf.dbv AS left_dbv,
           lf.dbv_iqr AS left_dbv_iqr,
           lf.dbv_n AS left_dbv_n,
           lf.n_samples AS left_n_samples,
           lf.measured_sample_count AS left_measured_sample_count,
           rf.dvth AS right_dvth,
           rf.dvth_iqr AS right_dvth_iqr,
           rf.dvth_n AS right_dvth_n,
           rf.drds AS right_drds,
           rf.drds_iqr AS right_drds_iqr,
           rf.drds_n AS right_drds_n,
           rf.dbv AS right_dbv,
           rf.dbv_iqr AS right_dbv_iqr,
           rf.dbv_n AS right_dbv_n,
           rf.n_samples AS right_n_samples,
           rf.measured_sample_count AS right_measured_sample_count,
           rf.prediction_count AS right_prediction_count,
           rf.strong_prediction_count AS right_strong_prediction_count,
           rf.weak_prediction_count AS right_weak_prediction_count,
           rf.dvth_prediction_count AS right_dvth_prediction_count,
           rf.drds_prediction_count AS right_drds_prediction_count,
           rf.dbv_prediction_count AS right_dbv_prediction_count,
           rf.fingerprint_confidence AS right_fingerprint_confidence,
           rf.median_confidence_score AS right_median_confidence_score,
           rf.median_donor_count AS right_median_donor_count,
           rf.median_donor_distance AS right_median_donor_distance,
           rf.median_validation_supported_fraction AS right_median_validation_supported_fraction,
           rf.median_validation_supported_pairs AS right_median_validation_supported_pairs,
           rf.median_validation_total_pairs AS right_median_validation_total_pairs,
           rf.validation_gate_pass_all AS right_validation_gate_pass_all,
           rf.validation_gate_pass_count AS right_validation_gate_pass_count,
           ax.dvth_scale,
           ax.drds_scale,
           ax.dbv_scale,
           CASE WHEN rf.dvth IS NOT NULL AND lf.dvth IS NOT NULL
                THEN 1 ELSE 0 END AS has_dvth,
           CASE WHEN rf.drds IS NOT NULL AND lf.drds IS NOT NULL
                THEN 1 ELSE 0 END AS has_drds,
           CASE WHEN rf.dbv IS NOT NULL AND lf.dbv IS NOT NULL
                THEN 1 ELSE 0 END AS has_dbv,
           CASE WHEN rf.dvth IS NOT NULL AND lf.dvth IS NOT NULL
                THEN ABS(rf.dvth - lf.dvth) END AS abs_delta_dvth,
           CASE WHEN rf.drds IS NOT NULL AND lf.drds IS NOT NULL
                THEN ABS(rf.drds - lf.drds) END AS abs_delta_drds,
           CASE WHEN rf.dbv IS NOT NULL AND lf.dbv IS NOT NULL
                THEN ABS(rf.dbv - lf.dbv) END AS abs_delta_dbv,
           CASE WHEN rf.dvth IS NOT NULL AND lf.dvth IS NOT NULL THEN
             SQRT(
               (SQRT(GREATEST(COALESCE(rf.dvth_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(rf.dvth_iqr, 0.0))))
               *
               (SQRT(GREATEST(COALESCE(lf.dvth_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(lf.dvth_iqr, 0.0))))
             )
           END AS dvth_weight,
           CASE WHEN rf.drds IS NOT NULL AND lf.drds IS NOT NULL THEN
             SQRT(
               (SQRT(GREATEST(COALESCE(rf.drds_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(rf.drds_iqr, 0.0))))
               *
               (SQRT(GREATEST(COALESCE(lf.drds_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(lf.drds_iqr, 0.0))))
             )
           END AS drds_weight,
           CASE WHEN rf.dbv IS NOT NULL AND lf.dbv IS NOT NULL THEN
             SQRT(
               (SQRT(GREATEST(COALESCE(rf.dbv_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(rf.dbv_iqr, 0.0))))
               *
               (SQRT(GREATEST(COALESCE(lf.dbv_n, 1), 1)::double precision)
                 / (1.0 + ABS(COALESCE(lf.dbv_iqr, 0.0))))
             )
           END AS dbv_weight
    FROM fp lf
    JOIN fp rf ON rf.source = 'predicted_irrad'
              AND lf.source = 'sc'
              AND rf.model_run_id = lf.model_run_id
              AND rf.reference_tier = lf.reference_tier
              AND rf.validation_mode_used = lf.validation_mode_used
              AND rf.device_type = lf.device_type
    JOIN axis_scales ax
      ON ax.model_run_id = rf.model_run_id
     AND ax.reference_tier = rf.reference_tier
     AND ax.validation_mode_used = rf.validation_mode_used
     AND ax.device_type = rf.device_type
),
pairs AS (
    SELECT pr.*,
           (has_dvth + has_drds + has_dbv) AS comparable_axes,
           CONCAT_WS(', ',
             CASE WHEN has_dvth = 1 THEN 'ΔVth' END,
             CASE WHEN has_drds = 1 THEN 'ΔRds(on)' END,
             CASE WHEN has_dbv = 1 THEN 'ΔV(BR)DSS' END
           ) AS comparable_axis_labels,
           SQRT(
             (
               COALESCE(dvth_weight
                 * POWER((right_dvth - left_dvth) / dvth_scale, 2), 0.0)
               + COALESCE(drds_weight
                 * POWER((right_drds - left_drds) / drds_scale, 2), 0.0)
               + COALESCE(dbv_weight
                 * POWER((right_dbv - left_dbv) / dbv_scale, 2), 0.0)
             )
             / NULLIF(
               COALESCE(dvth_weight, 0.0)
               + COALESCE(drds_weight, 0.0)
               + COALESCE(dbv_weight, 0.0),
               0.0
             )
           ) AS nearest_distance
    FROM pairs_raw pr
),
ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (
             PARTITION BY pair_type, model_run_id, reference_tier,
                          validation_mode_used, device_type,
                          right_fingerprint_key
             ORDER BY nearest_distance ASC NULLS LAST,
                      comparable_axes DESC,
                      left_label ASC
           ) AS match_rank,
           COUNT(*) OVER (
             PARTITION BY pair_type, model_run_id, reference_tier,
                          validation_mode_used, device_type,
                          right_fingerprint_key
           ) AS left_candidate_count
    FROM pairs p
    WHERE comparable_axes > 0
)
SELECT ranked.*,
       CASE
         WHEN comparable_axes >= 3 AND nearest_distance <= 0.75
           THEN 'strong'
         WHEN comparable_axes >= 2 AND nearest_distance <= 1.5
           THEN 'usable'
         WHEN nearest_distance <= 2.5
           THEN 'weak'
         ELSE 'inspect manually'
       END AS comparability_status,
       right_label AS predicted_irrad_label,
       right_ion_species AS ion_species,
       right_beam_energy_mev AS beam_energy_mev,
       right_let_surface AS let_surface,
       right_irrad_run_id AS irrad_run_id,
       left_label AS sc_label,
       left_sc_voltage_v AS sc_voltage_v,
       left_sc_duration_us AS sc_duration_us,
       right_dvth AS predicted_irrad_dvth,
       right_dvth_iqr AS predicted_irrad_dvth_iqr,
       right_dvth_n AS predicted_irrad_dvth_n,
       right_drds AS predicted_irrad_drds,
       right_drds_iqr AS predicted_irrad_drds_iqr,
       right_drds_n AS predicted_irrad_drds_n,
       right_dbv AS predicted_irrad_dbv,
       right_dbv_iqr AS predicted_irrad_dbv_iqr,
       right_dbv_n AS predicted_irrad_dbv_n,
       right_prediction_count AS predicted_irrad_prediction_count,
       right_strong_prediction_count AS predicted_irrad_strong_prediction_count,
       right_weak_prediction_count AS predicted_irrad_weak_prediction_count,
       left_dvth AS sc_dvth,
       left_dvth_iqr AS sc_dvth_iqr,
       left_dvth_n AS sc_dvth_n,
       left_drds AS sc_drds,
       left_drds_iqr AS sc_drds_iqr,
       left_drds_n AS sc_drds_n,
       left_dbv AS sc_dbv,
       left_dbv_iqr AS sc_dbv_iqr,
       left_dbv_n AS sc_dbv_n,
       left_n_samples AS sc_n_samples,
       left_candidate_count AS sc_candidate_count
FROM ranked;

CREATE VIEW damage_equivalence_prediction_coverage_view AS
WITH fp AS (
    SELECT * FROM damage_equivalence_prediction_fingerprint_view
),
context AS (
    SELECT DISTINCT model_run_id, model_version, is_latest_model_run,
           reference_tier, validation_mode_used, device_type
    FROM fp
    WHERE source = 'predicted_irrad'
),
fp_counts AS (
    SELECT 'sc_vs_predicted_irrad'::text AS pair_type,
           'sc'::text AS left_source,
           'predicted_irrad'::text AS right_source,
           c.model_run_id,
           c.model_version,
           c.is_latest_model_run,
           c.reference_tier,
           c.validation_mode_used,
           c.device_type,
           COUNT(*) FILTER (WHERE fp.source = 'sc') AS n_left_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'predicted_irrad') AS n_right_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'sc' AND fp.dvth IS NOT NULL)
             AS left_dvth_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'predicted_irrad' AND fp.dvth IS NOT NULL)
             AS right_dvth_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'sc' AND fp.drds IS NOT NULL)
             AS left_drds_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'predicted_irrad' AND fp.drds IS NOT NULL)
             AS right_drds_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'sc' AND fp.dbv IS NOT NULL)
             AS left_dbv_fingerprints,
           COUNT(*) FILTER (WHERE fp.source = 'predicted_irrad' AND fp.dbv IS NOT NULL)
             AS right_dbv_fingerprints,
           SUM(fp.prediction_count) FILTER (WHERE fp.source = 'predicted_irrad')
             AS prediction_count,
           SUM(fp.strong_prediction_count) FILTER (WHERE fp.source = 'predicted_irrad')
             AS strong_prediction_count,
           SUM(fp.weak_prediction_count) FILTER (WHERE fp.source = 'predicted_irrad')
             AS weak_prediction_count
    FROM context c
    LEFT JOIN fp
      ON fp.model_run_id = c.model_run_id
     AND fp.reference_tier = c.reference_tier
     AND fp.validation_mode_used = c.validation_mode_used
     AND fp.device_type = c.device_type
    GROUP BY c.model_run_id, c.model_version, c.is_latest_model_run,
             c.reference_tier, c.validation_mode_used, c.device_type
),
match_counts AS (
    SELECT model_run_id,
           reference_tier,
           validation_mode_used,
           device_type,
           COUNT(*) AS comparable_pair_count,
           COUNT(DISTINCT right_fingerprint_key) AS comparable_right_count,
           MIN(nearest_distance) AS best_distance,
           COUNT(*) FILTER (WHERE comparable_axes >= 2) AS pairs_with_2plus_axes
    FROM damage_equivalence_prediction_match_view
    GROUP BY model_run_id, reference_tier, validation_mode_used, device_type
)
SELECT fp.pair_type,
       fp.left_source,
       fp.right_source,
       fp.model_run_id,
       fp.model_version,
       fp.is_latest_model_run,
       fp.reference_tier,
       fp.validation_mode_used,
       fp.device_type,
       fp.n_left_fingerprints,
       fp.n_right_fingerprints,
       COALESCE(mc.comparable_pair_count, 0) AS comparable_pair_count,
       COALESCE(mc.comparable_right_count, 0) AS comparable_right_count,
       mc.best_distance,
       CONCAT_WS(', ',
         CASE WHEN fp.left_dvth_fingerprints > 0
                AND fp.right_dvth_fingerprints > 0
              THEN 'ΔVth' END,
         CASE WHEN fp.left_drds_fingerprints > 0
                AND fp.right_drds_fingerprints > 0
              THEN 'ΔRds(on)' END,
         CASE WHEN fp.left_dbv_fingerprints > 0
                AND fp.right_dbv_fingerprints > 0
              THEN 'ΔV(BR)DSS' END
       ) AS comparable_axis_labels,
       fp.left_dvth_fingerprints,
       fp.right_dvth_fingerprints,
       fp.left_drds_fingerprints,
       fp.right_drds_fingerprints,
       fp.left_dbv_fingerprints,
       fp.right_dbv_fingerprints,
       COALESCE(fp.prediction_count, 0) AS prediction_count,
       COALESCE(fp.strong_prediction_count, 0) AS strong_prediction_count,
       COALESCE(fp.weak_prediction_count, 0) AS weak_prediction_count,
       CASE
         WHEN fp.n_left_fingerprints = 0 OR fp.n_right_fingerprints = 0
           THEN 'missing counterpart'
         WHEN COALESCE(mc.comparable_pair_count, 0) = 0
           THEN 'no shared damage axes'
         WHEN mc.best_distance <= 0.75
              AND COALESCE(mc.pairs_with_2plus_axes, 0) > 0
           THEN 'strong matches available'
         WHEN mc.best_distance <= 1.5
              AND COALESCE(mc.pairs_with_2plus_axes, 0) > 0
           THEN 'usable matches available'
         ELSE 'weak/inspect manually'
       END AS comparability_status
FROM fp_counts fp
LEFT JOIN match_counts mc
  ON mc.model_run_id = fp.model_run_id
 AND mc.reference_tier = fp.reference_tier
 AND mc.validation_mode_used = fp.validation_mode_used
 AND mc.device_type = fp.device_type;

CREATE VIEW damage_equivalence_prediction_match_segment_view AS
SELECT m.pair_type,
       m.left_source,
       m.right_source,
       m.model_run_id,
       m.model_version,
       m.is_latest_model_run,
       m.reference_tier,
       m.validation_mode_used,
       m.device_type,
       m.right_fingerprint_key,
       m.right_label,
       m.left_label,
       m.right_irrad_run_id AS irrad_run_id,
       m.right_ion_species AS ion_species,
       m.right_beam_energy_mev AS beam_energy_mev,
       m.right_let_surface AS let_surface,
       m.left_sc_voltage_v AS sc_voltage_v,
       m.left_sc_duration_us AS sc_duration_us,
       m.match_rank,
       m.nearest_distance,
       m.comparable_axes,
       m.comparable_axis_labels,
       m.comparability_status,
       m.right_fingerprint_confidence AS fingerprint_confidence,
       m.right_prediction_count AS prediction_count,
       m.right_strong_prediction_count AS strong_prediction_count,
       m.right_weak_prediction_count AS weak_prediction_count,
       m.pair_type || ' | model ' || m.model_run_id::text || ' | '
         || m.reference_tier || ' | ' || m.validation_mode_used || ' | '
         || m.device_type || ' | '
         || m.right_label || ' <-> ' || m.left_label AS match_label,
       m.right_source AS endpoint_source,
       m.right_label AS endpoint_label,
       1 AS endpoint_order,
       m.right_dvth AS dvth,
       m.right_drds AS drds,
       m.right_dbv AS dbv,
       m.right_prediction_count AS n_samples
FROM damage_equivalence_prediction_match_view m
WHERE m.match_rank = 1
  AND m.comparability_status IN ('strong', 'usable')
UNION ALL
SELECT m.pair_type,
       m.left_source,
       m.right_source,
       m.model_run_id,
       m.model_version,
       m.is_latest_model_run,
       m.reference_tier,
       m.validation_mode_used,
       m.device_type,
       m.right_fingerprint_key,
       m.right_label,
       m.left_label,
       m.right_irrad_run_id AS irrad_run_id,
       m.right_ion_species AS ion_species,
       m.right_beam_energy_mev AS beam_energy_mev,
       m.right_let_surface AS let_surface,
       m.left_sc_voltage_v AS sc_voltage_v,
       m.left_sc_duration_us AS sc_duration_us,
       m.match_rank,
       m.nearest_distance,
       m.comparable_axes,
       m.comparable_axis_labels,
       m.comparability_status,
       'measured'::text AS fingerprint_confidence,
       0::bigint AS prediction_count,
       0::bigint AS strong_prediction_count,
       0::bigint AS weak_prediction_count,
       m.pair_type || ' | model ' || m.model_run_id::text || ' | '
         || m.reference_tier || ' | ' || m.validation_mode_used || ' | '
         || m.device_type || ' | '
         || m.right_label || ' <-> ' || m.left_label AS match_label,
       m.left_source AS endpoint_source,
       m.left_label AS endpoint_label,
       2 AS endpoint_order,
       m.left_dvth AS dvth,
       m.left_drds AS drds,
       m.left_dbv AS dbv,
       m.left_n_samples AS n_samples
FROM damage_equivalence_prediction_match_view m
WHERE m.match_rank = 1
  AND m.comparability_status IN ('strong', 'usable');
"""


# ── Output paths ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "out" / "sc_irrad_equivalence"
DAMAGE_AXES = ("dvth", "drds", "dbv")
SOURCE_PLOT_COLORS = {
    "sc": "#1f77b4",
    "irrad": "#d55e00",
}


# ── Helpers ─────────────────────────────────────────────────────────────────
def ensure_gate_params_populated(conn):
    """If gate_params is empty everywhere, hint to run extract_damage_metrics.py first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM baselines_metadata "
            "WHERE gate_params->>'vth_v' IS NOT NULL"
        )
        n = cur.fetchone()[0]
    if n < 10:
        print("WARNING: gate_params looks empty across baselines_metadata "
              f"(only {n} rows with vth_v). Run:\n"
              "    python3 extract_damage_metrics.py\n"
              "first to populate damage metrics, or this view will be empty.",
              file=sys.stderr)


def rebuild_view(conn):
    print("Rebuilding damage_equivalence_view …", flush=True)
    with conn.cursor() as cur:
        cur.execute(DAMAGE_VIEW_SQL)
    conn.commit()


def load_fingerprints(conn, device_type=None):
    sql = """
        SELECT source, device_type, sc_voltage_v, sc_duration_us,
               ion_species, beam_energy_mev, let_surface, irrad_run_id,
               dvth, dvth_iqr, dvth_n,
               drds, drds_iqr, drds_n,
               dbv,  dbv_iqr,  dbv_n,
               n_samples, label
        FROM damage_equivalence_view
    """
    params = []
    if device_type:
        sql += " WHERE device_type = %s"
        params.append(device_type)
    sql += " ORDER BY device_type, source, label"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def _robust_axis_scale(values):
    """Robust per-axis scale used to normalize damage differences."""
    vals = np.array([float(v) for v in values if v is not None], dtype=float)
    if vals.size < 2:
        return 1.0
    q1, q3 = np.percentile(vals, [25.0, 75.0])
    iqr = float(q3 - q1)
    if iqr > 1e-12:
        # Convert IQR to Gaussian-equivalent sigma.
        return max(iqr / 1.349, 1e-6)
    std = float(np.std(vals))
    return max(std, 1.0)


def _fit_axis_scales(group):
    """Return per-axis robust scales for one device_type group."""
    return {
        axis: _robust_axis_scale([fp.get(axis) for fp in group])
        for axis in DAMAGE_AXES
    }


def _axis_reliability(fp, axis):
    """Reliability from axis sample count and spread (IQR)."""
    n = fp.get(f"{axis}_n")
    iqr = fp.get(f"{axis}_iqr")
    n_term = np.sqrt(max(float(n), 1.0)) if n is not None else 1.0
    iqr_term = 1.0 / (1.0 + abs(float(iqr))) if iqr is not None else 1.0
    return n_term * iqr_term


def _damage_space_distance(ir_fp, sc_fp, axis_scales):
    """Reliability-weighted distance between one irrad and one SC fingerprint."""
    weighted_sq = 0.0
    total_w = 0.0
    n_dims = 0
    for axis in DAMAGE_AXES:
        iv = ir_fp.get(axis)
        sv = sc_fp.get(axis)
        if iv is None or sv is None:
            continue
        scale = axis_scales.get(axis, 1.0)
        if scale <= 0:
            scale = 1.0
        delta = (float(iv) - float(sv)) / scale
        w = np.sqrt(_axis_reliability(ir_fp, axis) *
                    _axis_reliability(sc_fp, axis))
        weighted_sq += w * delta * delta
        total_w += w
        n_dims += 1

    if n_dims == 0 or total_w <= 0.0:
        return None, 0
    return float(np.sqrt(weighted_sq / total_w)), n_dims


def compute_matches(fps, k=3):
    """For each irrad fingerprint, retrieve k nearest SC fingerprints in the
    same device_type using reliability-weighted damage-space distance.

    Distance is computed over axis intersection (ΔVth/ΔRds/ΔBV) and each
    axis is normalized by a robust per-device_type scale.

    Returns list of {irrad_fp, matches: [(sc_fp, distance, n_dims)]}.
    """
    per_dev = {}
    for fp in fps:
        per_dev.setdefault(fp["device_type"], []).append(fp)

    results = []
    for dev, grp in per_dev.items():
        sc = [f for f in grp if f["source"] == "sc"]
        ir = [f for f in grp if f["source"] == "irrad"]
        if not sc or not ir:
            continue

        axis_scales = _fit_axis_scales(grp)

        for irfp in ir:
            dists = []
            for scfp in sc:
                d, n_dims = _damage_space_distance(irfp, scfp, axis_scales)
                if d is None:
                    continue
                dists.append((scfp, d, n_dims))
            dists.sort(key=lambda t: t[1])
            results.append({"irrad": irfp, "matches": dists[:k]})
    return results


def write_csv(results, path):
    fields = [
        "device_type", "ion_species", "beam_energy_mev", "let_surface",
        "irrad_run_id", "irrad_dvth", "irrad_drds", "irrad_dbv",
        "irrad_n_samples",
        "nearest_sc_voltage_v", "nearest_sc_duration_us",
        "nearest_distance", "nearest_n_dims",
        "sc_dvth", "sc_drds", "sc_dbv", "sc_n_samples",
        "k3_alternatives",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            ir = r["irrad"]
            if not r["matches"]:
                continue
            first = r["matches"][0]
            alts = "; ".join(
                f"{m[0]['sc_voltage_v']}V/{m[0]['sc_duration_us']}us (d={m[1]:.2f})"
                for m in r["matches"][1:]
            )
            w.writerow({
                "device_type":      ir["device_type"],
                "ion_species":      ir["ion_species"],
                "beam_energy_mev":  ir["beam_energy_mev"],
                "let_surface":      ir["let_surface"],
                "irrad_run_id":     ir["irrad_run_id"],
                "irrad_dvth":       ir["dvth"],
                "irrad_drds":       ir["drds"],
                "irrad_dbv":        ir["dbv"],
                "irrad_n_samples":  ir["n_samples"],
                "nearest_sc_voltage_v":  first[0]["sc_voltage_v"],
                "nearest_sc_duration_us":first[0]["sc_duration_us"],
                "nearest_distance":      round(first[1], 3),
                "nearest_n_dims":        first[2],
                "sc_dvth":          first[0]["dvth"],
                "sc_drds":          first[0]["drds"],
                "sc_dbv":           first[0]["dbv"],
                "sc_n_samples":     first[0]["n_samples"],
                "k3_alternatives":  alts,
            })


def plot_pair(fps, x_key, y_key, x_label, y_label, path):
    # Restrict to device_types that have BOTH SC and irradiation fingerprints —
    # those are the only points that matter for equivalence matching; others
    # would just be visual noise.
    dtypes_with_sc    = {f["device_type"] for f in fps if f["source"] == "sc"}
    dtypes_with_irrad = {f["device_type"] for f in fps if f["source"] == "irrad"}
    keep = dtypes_with_sc & dtypes_with_irrad
    fps = [f for f in fps if f["device_type"] in keep]

    sc = [f for f in fps if f["source"] == "sc"
          and f.get(x_key) is not None and f.get(y_key) is not None]
    ir = [f for f in fps if f["source"] == "irrad"
          and f.get(x_key) is not None and f.get(y_key) is not None]
    if not sc and not ir:
        print(f"  skipping {path.name}: no data on both axes")
        return

    fig, ax = plt.subplots(figsize=(11, 7))
    for fp in sc:
        c = SOURCE_PLOT_COLORS["sc"]
        ax.scatter(fp[x_key], fp[y_key], marker="o",
                   s=40 + 8 * (fp["n_samples"] or 1), c=c, alpha=0.55,
                   edgecolors="k", linewidths=0.4)
        ax.annotate(f"{fp['sc_voltage_v']:g}V/{fp['sc_duration_us']:g}us",
                    (fp[x_key], fp[y_key]), fontsize=6.5, alpha=0.75,
                    xytext=(5, 3), textcoords="offset points")
    for fp in ir:
        c = SOURCE_PLOT_COLORS["irrad"]
        ax.scatter(fp[x_key], fp[y_key], marker="^",
                   s=80 + 14 * (fp["n_samples"] or 1), c=c,
                   edgecolors="k", linewidths=0.7)
        ion = fp.get("ion_species") or "?"
        energy = fp.get("beam_energy_mev")
        lbl = f"{ion}/{energy:g}MeV" if energy is not None else ion
        ax.annotate(lbl, (fp[x_key], fp[y_key]), fontsize=8,
                    xytext=(5, 3), textcoords="offset points", weight="bold")

    ax.axhline(0, color="k", lw=0.5, alpha=0.3)
    ax.axvline(0, color="k", lw=0.5, alpha=0.3)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(f"{x_label} vs {y_label}: SC vs Irradiation")
    ax.scatter([], [], marker="o", c=SOURCE_PLOT_COLORS["sc"], label="SC")
    ax.scatter([], [], marker="^", c=SOURCE_PLOT_COLORS["irrad"],
               label="Irradiation")
    ax.legend(fontsize=8, loc="best")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  wrote {path}")


def print_cli_prediction(conn, ion, energy, let, device_type):
    """Find the irrad fingerprint in the view closest to (ion, energy, LET)
    for the given device_type, then print its nearest SC match.
    """
    fps = load_fingerprints(conn, device_type=device_type)
    if not fps:
        print(f"No fingerprints for device_type={device_type}.", file=sys.stderr)
        sys.exit(1)

    irrad = [f for f in fps if f["source"] == "irrad"]
    if not irrad:
        print(f"No irradiation fingerprints for device_type={device_type}.",
              file=sys.stderr)
        sys.exit(1)

    def query_score(f):
        s = 0.0
        if ion and f.get("ion_species"):
            s += 0.0 if ion.lower() == f["ion_species"].lower() else 10.0
        if energy is not None and f.get("beam_energy_mev") is not None:
            s += abs(energy - f["beam_energy_mev"]) / max(abs(energy), 1.0)
        if let is not None and f.get("let_surface") is not None:
            s += abs(let - f["let_surface"]) / max(abs(let), 1.0)
        return s
    target = min(irrad, key=query_score)

    results = compute_matches(fps, k=3)
    match = next((r for r in results if r["irrad"] is target), None)
    if not match or not match["matches"]:
        print("No SC match could be computed (missing damage axes).",
              file=sys.stderr)
        sys.exit(1)

    first = match["matches"][0]
    scfp, dist, n_dims = first
    print(f"\nClosest irradiation fingerprint in the view:")
    print(f"  {target['label']}  (device_type={target['device_type']})")
    print(f"  ΔVth={target['dvth']}, ΔRds={target['drds']}, ΔBV={target['dbv']}, "
          f"n_samples={target['n_samples']}")
    print("\nNearest SC condition (reliability-weighted damage-space distance):")
    print(f"  {scfp['sc_voltage_v']:g} V × {scfp['sc_duration_us']:g} µs")
    print(f"  distance = {dist:.3f}  (over {n_dims} damage axes)")
    print(f"  SC ΔVth={scfp['dvth']}, ΔRds={scfp['drds']}, ΔBV={scfp['dbv']}, "
          f"n_samples={scfp['n_samples']}")
    if dist > 1.5:
        print("\n  WARNING: distance > 1.5 — this is a weak match. "
              f"Check IQR and consider extending the dataset.")
    if len(match["matches"]) > 1:
        print(f"\nAlternatives (k=3):")
        for scfp2, d2, nd2 in match["matches"][1:]:
            print(f"  {scfp2['sc_voltage_v']:g} V × {scfp2['sc_duration_us']:g} µs  "
                  f"(distance={d2:.3f}, {nd2} axes)")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--rebuild", action="store_true",
                    help="Recreate damage_equivalence_view, CSV, and plots")
    ap.add_argument("--device-type",
                    help="Restrict CLI prediction to one device_type")
    ap.add_argument("--ion", help="Ion species for CLI prediction (e.g. Au)")
    ap.add_argument("--energy", type=float,
                    help="Beam energy in MeV for CLI prediction")
    ap.add_argument("--let", type=float,
                    help="LET at surface (MeV·cm²/mg) for CLI prediction")
    args = ap.parse_args()

    conn = get_connection()
    try:
        ensure_gate_params_populated(conn)
        if args.rebuild:
            rebuild_view(conn)
            fps = load_fingerprints(conn, device_type=args.device_type)
            print(
                f"Loaded {len(fps)} fingerprints "
                f"({sum(1 for f in fps if f['source'] == 'sc')} SC, "
                f"{sum(1 for f in fps if f['source'] == 'avalanche')} avalanche, "
                f"{sum(1 for f in fps if f['source'] == 'irrad')} irrad)"
            )

            results = compute_matches(fps, k=3)
            print(f"Computed nearest-SC matches for "
                  f"{len(results)} irradiation fingerprints.")

            csv_path = OUT_DIR / "irrad_to_sc_equivalents.csv"
            write_csv(results, csv_path)
            print(f"Wrote {csv_path}")

            plot_pair(fps, "dvth", "drds",
                      "ΔVth (V)", "ΔRds(on) (mΩ)",
                      OUT_DIR / "damage_scatter_dvth_drds.png")
            plot_pair(fps, "dvth", "dbv",
                      "ΔVth (V)", "ΔV(BR)DSS (V)",
                      OUT_DIR / "damage_scatter_dvth_dbv.png")

        if args.ion or args.energy or args.let:
            if not args.device_type:
                sys.exit("--device-type is required for CLI prediction")
            print_cli_prediction(conn, args.ion, args.energy, args.let,
                                 args.device_type)
        elif not args.rebuild:
            ap.print_help()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
