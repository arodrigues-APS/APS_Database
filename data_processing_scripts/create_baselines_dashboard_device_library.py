#!/usr/bin/env python3
"""
Create the "Baselines Device Library" dashboard in Apache Superset via its REST API.

This dashboard shows **averaged device performance** across all runs of the
same device type, using the ``baselines_device_averages`` SQL view.  Instead
of plotting every individual experiment curve, it computes mean ± standard
deviation per voltage bin, giving an overview of typical behaviour and
run-to-run consistency.

Dashboard design
================
Filters (cascading):
  1. Manufacturer         – optional, multi-select
  2. Device Type          – required, multi-select (cascades from Manufacturer)
  3. Measurement Category – required, multi-select
  6. Likely Irradiated    – optional, shows all data by default; select
                            false for pristine-only or true for irradiated-only

Tabs:
  1. Mean Curves          – averaged device performance (default)
  2. ±1σ Bands            – upper/lower standard-deviation bounds
  3. Individual Runs      – every run that contributes to the mean

Mean Curves tab charts:
  1. Data Summary         – table: device_type × category → n_devices, n_runs
  2. IdVg Transfer Curves – one line per device × V_drain bias (integer V)
  3. IdVd Output Curves   – one line per device × V_gate bias (5 V steps)
  4. 3rd Quadrant         – one line per device × V_gate bias (integer V)
  5. Igss Mean            – avg |I_Gate| vs V_Gate (log-Y)
  6. Vth Mean Curves      – one line per device × V_drain bias (integer V)
  7. Consistency Overview  – CV and std-dev per device × category
  8. Registered Devices   – reference table from device_library
  9. Calculated Parameters – Vth, Rds(on), Igss, Vsd, gfs, V_gfs_peak,
                             SS, Id_on, BV_DSS, IDSS derived from data
                             (virtual dataset: device_calculated_params)

Individual Runs tab charts:
  1. Run Summary          – table: device_id × experiment × measurement_type
  2. IdVg Transfer Curves – one line per device_id × measurement_type
  3. IdVd Output Curves   – one line per device_id × measurement_type
  4. 3rd Quadrant         – one line per device_id × measurement_type
  5. Igss Gate Leakage    – one line per device_id × measurement_type (log-Y)
  6. Vth Curves           – one line per device_id × measurement_type

Workflow:
  Pick manufacturer → narrows device type list
  Pick device type  → summary + curve charts populate
  Pick category     → relevant charts populate
  Bias conditions are shown as separate lines (auto-grouped),
  no manual bias selection needed.
  Switch to "Individual Runs" tab to see every run behind the mean.

Usage:
    source /tmp/aps_venv/bin/activate
    python3 create_baselines_dashboard_device_library.py
"""

import json
import sys

from superset_api import (get_session, find_database, find_or_create_dataset,
                          refresh_dataset_columns, create_chart,
                          create_or_update_dashboard, build_json_metadata)
from db_config import SUPERSET_URL


# ── Calculated Parameters SQL ─────────────────────────────────────────────────
# Derives key electrical parameters from baselines_device_averages.
# Each CTE targets a specific measurement category and test condition that
# mirrors the datasheet specification for the C2M0080120D (and similar devices).

CALCULATED_PARAMS_SQL = """WITH
/* ══════════════════════════════════════════════════════════════════════════
   Per-device-first calculation: extract every parameter from each
   individual device's curve, then aggregate (mean ± σ) across devices.
   This avoids artefacts caused by computing parameters on group-averaged
   curves where the device population can change between voltage bins.

   NOTE: All CTEs filter on NOT is_likely_irradiated so that calculated
   parameters reflect pristine device characteristics only.
   ══════════════════════════════════════════════════════════════════════════ */

/* ── STEP 1: Discover per-device test conditions ─────────────────────── */

-- Highest Vgs and Vds per device in IdVd sweep (for Rds(on))
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

-- Highest Vds and Vgs per device in IdVg sweep (for gfs, Id_on)
idvg_dev_bias AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_drain_bin) AS max_vds,
           MAX(v_gate_bin)  AS max_vgs
    FROM baselines_per_device
    WHERE measurement_category = 'IdVg'
      AND dev_avg_i_drain > 0
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),

-- Most negative Vgs and peak negative Id per device in 3rd-Quadrant (for Vsd)
q3_dev_bias AS (
    SELECT device_id, device_type, manufacturer,
           MIN(v_gate_bin)       AS min_vgs,
           MIN(dev_avg_i_drain)  AS min_id
    FROM baselines_per_device
    WHERE measurement_category = '3rd_Quadrant'
      AND dev_avg_i_drain < 0
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),

-- Highest Vgs per device in Igss sweep (for Igss)
igss_dev_bias AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_gate_bin) AS max_vgs
    FROM baselines_per_device
    WHERE measurement_category = 'Igss'
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),

/* ── STEP 2: Calculate each parameter per device ─────────────────────── */

/* ── Vth: Gate Threshold Voltage (per device) ────────────────────────────
   Calculated by: sweep gate voltage up from 0 V and watch drain current.
   The moment current exceeds a threshold, that gate voltage is Vth.
   Threshold = whichever is larger: 5 mA or 1% of the peak current seen
   in the sweep (adaptive, so it works across device sizes).
   We pick the result at the lowest available Vds to stay close to the
   datasheet condition of Vds ≈ Vgs.                                       */
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
    SELECT b.device_id, b.device_type, b.manufacturer,
           b.v_drain_bin,
           MIN(b.v_gate_bin) AS vth_v,
           t.i_thresh
    FROM baselines_per_device b
    JOIN vth_dev_peak t USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'Vth'
      AND b.dev_avg_i_drain >= t.i_thresh
      AND NOT b.is_likely_irradiated
    GROUP BY b.device_id, b.device_type, b.manufacturer,
             b.v_drain_bin, t.i_thresh
),
vth_dev_min_vds AS (
    SELECT device_id, device_type, manufacturer,
           MIN(ABS(v_drain_bin)) AS min_abs_vds
    FROM vth_dev_crossing
    GROUP BY device_id, device_type, manufacturer
),
vth_per_device AS (
    SELECT c.device_id, c.device_type, c.manufacturer,
           c.vth_v,
           c.v_drain_bin       AS vth_test_vds,
           c.i_thresh * 1000.0 AS vth_thresh_ma
    FROM vth_dev_crossing c
    JOIN vth_dev_min_vds m USING (device_id, device_type, manufacturer)
    WHERE ABS(c.v_drain_bin) = m.min_abs_vds
),
vth_vals AS (
    SELECT device_type, manufacturer,
           AVG(vth_v)          AS vth_v,
           STDDEV(vth_v)       AS vth_v_std,
           MIN(vth_v)          AS vth_v_min,
           MAX(vth_v)          AS vth_v_max,
           COUNT(*)            AS vth_n_devices,
           AVG(vth_test_vds)   AS vth_test_vds,
           AVG(vth_thresh_ma)  AS vth_thresh_ma
    FROM vth_per_device
    GROUP BY device_type, manufacturer
),

/* ── Rds(on): Drain-Source On-State Resistance (per device) ──────────────
   Calculated by: at low drain voltages the device behaves like a simple
   resistor (V = I × R).  We take the output sweep at the highest gate
   voltage available, restrict to the very low Vds region (≤ 2 V or 15%
   of max Vds, whichever is smaller), then fit a straight line through
   the origin to all those points.  The slope of that line is 1/R.
   Multiply by 1000 to report in milliohms.                                */
rdson_per_device AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           SUM(b.v_drain_bin * b.v_drain_bin) /
               NULLIF(SUM(b.v_drain_bin * b.dev_avg_i_drain), 0)
               * 1000.0 AS rdson_mohm,
           m.max_vgs     AS rdson_test_vgs
    FROM baselines_per_device b
    JOIN idvd_dev_bias m USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'IdVd'
      AND b.v_gate_bin  BETWEEN m.max_vgs - 1.0 AND m.max_vgs + 1.0
      AND b.v_drain_bin >  0.0
      AND b.v_drain_bin <= LEAST(m.max_vds * 0.15, 2.0)
      AND b.dev_avg_i_drain > 0.0
      AND NOT b.is_likely_irradiated
    GROUP BY b.device_id, b.device_type, b.manufacturer, m.max_vgs
),
rdson_vals AS (
    SELECT device_type, manufacturer,
           AVG(rdson_mohm)         AS rdson_mohm,
           STDDEV(rdson_mohm)      AS rdson_mohm_std,
           MIN(rdson_mohm)         AS rdson_mohm_min,
           MAX(rdson_mohm)         AS rdson_mohm_max,
           COUNT(*)                AS rdson_n_devices,
           AVG(rdson_test_vgs)     AS rdson_test_vgs
    FROM rdson_per_device
    WHERE rdson_mohm > 0 AND rdson_mohm < 1e6
    GROUP BY device_type, manufacturer
),

/* ── Igss: Gate-Source Leakage Current (per device) ──────────────────────
   Calculated by: with the drain grounded, ramp gate voltage up and
   measure how much current sneaks through the gate oxide.  A healthy
   gate oxide conducts almost nothing (picoamps to nanoamps).  We take
   the reading at the highest gate voltage tested and report it in nA.
   Elevated Igss is a sign of oxide damage.                                */
igss_per_device AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           MAX(b.dev_avg_abs_i_gate) * 1.0e9 AS igss_max_na,
           m.max_vgs                          AS igss_test_vgs
    FROM baselines_per_device b
    JOIN igss_dev_bias m USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'Igss'
      AND b.v_gate_bin >= m.max_vgs - 1.0
      AND NOT b.is_likely_irradiated
    GROUP BY b.device_id, b.device_type, b.manufacturer, m.max_vgs
),
igss_vals AS (
    SELECT device_type, manufacturer,
           AVG(igss_max_na)        AS igss_max_na,
           STDDEV(igss_max_na)     AS igss_max_na_std,
           MIN(igss_max_na)        AS igss_max_na_min,
           MAX(igss_max_na)        AS igss_max_na_max,
           COUNT(*)                AS igss_n_devices,
           AVG(igss_test_vgs)      AS igss_test_vgs
    FROM igss_per_device
    GROUP BY device_type, manufacturer
),

/* ── Vsd: Body Diode Forward Voltage (per device) ────────────────────────
   Calculated by: force current backwards through the device (source to
   drain) at a negative gate voltage to characterise the body diode.
   Find the peak current in the sweep, take 10% of it as a reference
   point, then find the drain-source voltage at that reference current.
   That voltage is the diode forward drop.  We use 10% of peak rather
   than a fixed current so the method works regardless of how far the
   sweep was taken.                                                         */
vsd_dev_target AS (
    SELECT device_id, device_type, manufacturer,
           min_vgs,
           min_id * 0.1 AS target_id
    FROM q3_dev_bias
),
vsd_dev_ranked AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           ABS(b.v_drain_bin) AS vsd_v,
           t.min_vgs          AS vsd_test_vgs,
           ABS(t.target_id)   AS vsd_ref_id_a,
           ROW_NUMBER() OVER (
               PARTITION BY b.device_id, b.device_type, b.manufacturer
               ORDER BY ABS(b.dev_avg_i_drain - t.target_id) ASC
           ) AS rn
    FROM baselines_per_device b
    JOIN vsd_dev_target t USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = '3rd_Quadrant'
      AND b.v_gate_bin BETWEEN t.min_vgs - 0.5 AND t.min_vgs + 0.5
      AND b.dev_avg_i_drain < 0
      AND NOT b.is_likely_irradiated
),
vsd_per_device AS (
    SELECT device_id, device_type, manufacturer,
           vsd_v, vsd_test_vgs, vsd_ref_id_a
    FROM vsd_dev_ranked WHERE rn = 1
),
vsd_vals AS (
    SELECT device_type, manufacturer,
           AVG(vsd_v)          AS vsd_v,
           STDDEV(vsd_v)       AS vsd_v_std,
           MIN(vsd_v)          AS vsd_v_min,
           MAX(vsd_v)          AS vsd_v_max,
           COUNT(*)            AS vsd_n_devices,
           AVG(vsd_test_vgs)   AS vsd_test_vgs,
           AVG(vsd_ref_id_a)   AS vsd_ref_id_a
    FROM vsd_per_device
    GROUP BY device_type, manufacturer
),

/* ── gfs / v_gfs_peak: Forward Transconductance (per device) ─────────────
   Calculated by: transconductance measures how effectively the gate
   controls the drain current — how many extra amps you get per extra
   volt on the gate.  We take the transfer sweep (Id vs Vgs) at the
   highest drain voltage available, compute the slope between every
   consecutive pair of points (ΔId / ΔVgs), and keep the maximum slope.
   That peak slope is gfs.  We also record the gate voltage where the
   peak occurred (v_gfs_peak_v).  Each device is processed individually
   to avoid artefacts from mixing different devices between Vgs bins.      */
gfs_dev_pts AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           b.v_gate_bin,
           m.max_vds AS gfs_test_vds,
           (b.dev_avg_i_drain
               - LAG(b.dev_avg_i_drain) OVER (
                     PARTITION BY b.device_id, b.device_type, b.manufacturer
                     ORDER BY b.v_gate_bin
                 )
           ) / NULLIF(
               b.v_gate_bin
               - LAG(b.v_gate_bin) OVER (
                     PARTITION BY b.device_id, b.device_type, b.manufacturer
                     ORDER BY b.v_gate_bin
                 ),
               0
           ) AS gfs_point
    FROM baselines_per_device b
    JOIN idvg_dev_bias m USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'IdVg'
      AND b.v_drain_bin BETWEEN m.max_vds - 1.0 AND m.max_vds + 1.0
      AND NOT b.is_likely_irradiated
),
gfs_dev_ranked AS (
    SELECT device_id, device_type, manufacturer,
           v_gate_bin, gfs_test_vds, gfs_point,
           ROW_NUMBER() OVER (
               PARTITION BY device_id, device_type, manufacturer
               ORDER BY gfs_point DESC NULLS LAST
           ) AS rn
    FROM gfs_dev_pts
    WHERE gfs_point IS NOT NULL AND gfs_point > 0
),
gfs_per_device AS (
    SELECT device_id, device_type, manufacturer,
           gfs_point   AS gfs_s,
           v_gate_bin  AS v_gfs_peak_v,
           gfs_test_vds
    FROM gfs_dev_ranked
    WHERE rn = 1
),
gfs_vals AS (
    SELECT device_type, manufacturer,
           AVG(gfs_s)          AS gfs_s,
           STDDEV(gfs_s)       AS gfs_s_std,
           MIN(gfs_s)          AS gfs_s_min,
           MAX(gfs_s)          AS gfs_s_max,
           COUNT(*)            AS gfs_n_devices,
           AVG(v_gfs_peak_v)   AS v_gfs_peak_v,
           AVG(gfs_test_vds)   AS gfs_test_vds
    FROM gfs_per_device
    GROUP BY device_type, manufacturer
),

/* ── SS: Subthreshold Slope (per device) ─────────────────────────────────
   Calculated by: in the off-state, drain current rises exponentially
   as gate voltage increases — meaning equal gate voltage steps cause
   equal multiplicative increases in current.  SS measures how many mV
   of gate voltage are needed to increase current by 10×.  Lower is
   better (sharper turn-on).  We restrict to the 1 nA–1 mA window (the
   subthreshold region), compute the mV/decade slope between every
   consecutive pair of points, and keep the steepest (minimum) slope
   per device.  SiC typically reads 200–500 mV/dec due to high interface
   trap density; radiation degrades it further.                             */
ss_dev_pts AS (
    SELECT device_id, device_type, manufacturer,
           v_gate_bin,
           LOG(10, NULLIF(dev_avg_abs_i_drain, 0)::numeric) AS log10_id,
           LAG(LOG(10, NULLIF(dev_avg_abs_i_drain, 0)::numeric)) OVER (
               PARTITION BY device_id, device_type, manufacturer
               ORDER BY v_gate_bin
           )                                    AS lag_log10_id,
           LAG(v_gate_bin) OVER (
               PARTITION BY device_id, device_type, manufacturer
               ORDER BY v_gate_bin
           )                                    AS lag_vgs
    FROM baselines_per_device
    WHERE measurement_category = 'Vth'
      AND dev_avg_abs_i_drain BETWEEN 1e-9 AND 1e-3
      AND NOT is_likely_irradiated
),
ss_per_device AS (
    SELECT device_id, device_type, manufacturer,
           MIN(
               (v_gate_bin - lag_vgs)
               / NULLIF(log10_id - lag_log10_id, 0)
               * 1000.0
           ) AS ss_mv_dec
    FROM ss_dev_pts
    WHERE log10_id     IS NOT NULL
      AND lag_log10_id IS NOT NULL
      AND log10_id     > lag_log10_id
      AND v_gate_bin   > lag_vgs
    GROUP BY device_id, device_type, manufacturer
),
ss_vals AS (
    SELECT device_type, manufacturer,
           AVG(ss_mv_dec)      AS ss_mv_dec,
           STDDEV(ss_mv_dec)   AS ss_mv_dec_std,
           MIN(ss_mv_dec)      AS ss_mv_dec_min,
           MAX(ss_mv_dec)      AS ss_mv_dec_max,
           COUNT(*)            AS ss_n_devices
    FROM ss_per_device
    WHERE ss_mv_dec > 0
    GROUP BY device_type, manufacturer
),

/* ── Id_on: On-State Saturation Current (per device) ─────────────────────
   Calculated by: the simplest of the parameters — with the device fully
   biased on (highest gate and drain voltages tested), what is the most
   current it delivered?  We take the maximum drain current reading from
   those conditions in the transfer sweep.  This is a pulsed bench value
   and will typically exceed the datasheet DC rating, which is thermally
   limited rather than electrically limited.                                */
idon_per_device AS (
    SELECT b.device_id, b.device_type, b.manufacturer,
           MAX(b.dev_avg_i_drain) AS id_on_a,
           m.max_vds              AS id_on_test_vds,
           m.max_vgs              AS id_on_test_vgs
    FROM baselines_per_device b
    JOIN idvg_dev_bias m USING (device_id, device_type, manufacturer)
    WHERE b.measurement_category = 'IdVg'
      AND b.v_drain_bin BETWEEN m.max_vds - 1.0 AND m.max_vds + 1.0
      AND b.v_gate_bin  BETWEEN m.max_vgs - 0.5 AND m.max_vgs + 0.5
      AND NOT b.is_likely_irradiated
    GROUP BY b.device_id, b.device_type, b.manufacturer,
             m.max_vds, m.max_vgs
),
idon_vals AS (
    SELECT device_type, manufacturer,
           AVG(id_on_a)          AS id_on_a,
           STDDEV(id_on_a)       AS id_on_a_std,
           MIN(id_on_a)          AS id_on_a_min,
           MAX(id_on_a)          AS id_on_a_max,
           COUNT(*)              AS id_on_n_devices,
           AVG(id_on_test_vds)   AS id_on_test_vds,
           AVG(id_on_test_vgs)   AS id_on_test_vgs
    FROM idon_per_device
    GROUP BY device_type, manufacturer
),

/* ── V(BR)DSS: Drain-Source Breakdown Voltage (per device) ───────────────
   Calculated by: with the gate off (Vgs ≈ 0 V), ramp drain voltage up
   and watch for the device to lose blocking ability — indicated by drain
   current exceeding 100 µA (the standard datasheet criterion).  The
   voltage where that crossing first occurs is the breakdown voltage.
   If current never reaches 100 µA across the whole sweep, the device
   held blocking and we report the max voltage tested with a 'held' flag.
   A group-level flag then summarises whether all devices broke, all
   held, or the lot was mixed.                                              */
bvdss_dev_crossed AS (
    SELECT device_id, device_type, manufacturer,
           MIN(v_drain_bin)  AS bvdss_v,
           'breakdown'       AS bvdss_flag
    FROM baselines_per_device
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND dev_avg_abs_i_drain >= 100e-6
      AND NOT is_likely_irradiated
    GROUP BY device_id, device_type, manufacturer
),
bvdss_dev_held AS (
    SELECT device_id, device_type, manufacturer,
           MAX(v_drain_bin)          AS bvdss_v,
           'held (>= max tested)'   AS bvdss_flag
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
bv_vals AS (
    SELECT device_type, manufacturer,
           AVG(bvdss_v)       AS bvdss_v,
           STDDEV(bvdss_v)    AS bvdss_v_std,
           MIN(bvdss_v)       AS bvdss_v_min,
           MAX(bvdss_v)       AS bvdss_v_max,
           COUNT(*)           AS bvdss_n_devices,
           SUM(CASE WHEN bvdss_flag = 'breakdown' THEN 1 ELSE 0 END)
                              AS bvdss_n_breakdown,
           CASE
               WHEN COUNT(*) = SUM(CASE WHEN bvdss_flag = 'held (>= max tested)'
                                        THEN 1 ELSE 0 END)
               THEN 'all held (>= max tested)'
               WHEN COUNT(*) = SUM(CASE WHEN bvdss_flag = 'breakdown'
                                        THEN 1 ELSE 0 END)
               THEN 'all breakdown'
               ELSE 'mixed (' ||
                    SUM(CASE WHEN bvdss_flag = 'breakdown' THEN 1 ELSE 0 END)
                    || ' broke, ' ||
                    SUM(CASE WHEN bvdss_flag = 'held (>= max tested)' THEN 1 ELSE 0 END)
                    || ' held)'
           END AS bvdss_flag
    FROM bvdss_per_device
    GROUP BY device_type, manufacturer
),

/* ── IDSS: Off-State Drain Leakage Current (per device) ──────────────────
   Calculated by: same blocking sweep as V(BR)DSS (gate off, drain ramped
   up), but instead of looking for breakdown we ask: how much current
   leaks through at high voltage?  We take the top 15% of the voltage
   range tested (the highest-stress portion) and average the drain
   current across those points.  Averaging rather than picking a single
   point makes the result less sensitive to noise.  Reported in µA.        */
idss_dev_pts AS (
    SELECT device_id, device_type, manufacturer,
           dev_avg_abs_i_drain,
           v_drain_bin,
           MAX(v_drain_bin) OVER (
               PARTITION BY device_id, device_type, manufacturer
           ) AS max_vd
    FROM baselines_per_device
    WHERE measurement_category = 'Blocking'
      AND v_gate_bin BETWEEN -1.0 AND 1.0
      AND NOT is_likely_irradiated
),
idss_per_device AS (
    SELECT device_id, device_type, manufacturer,
           AVG(dev_avg_abs_i_drain) * 1.0e6 AS idss_ua,
           MAX(max_vd)                      AS idss_test_vds
    FROM idss_dev_pts
    WHERE v_drain_bin >= max_vd * 0.85
    GROUP BY device_id, device_type, manufacturer
),
idss_vals AS (
    SELECT device_type, manufacturer,
           AVG(idss_ua)          AS idss_ua,
           STDDEV(idss_ua)       AS idss_ua_std,
           MIN(idss_ua)          AS idss_ua_min,
           MAX(idss_ua)          AS idss_ua_max,
           COUNT(*)              AS idss_n_devices,
           AVG(idss_test_vds)    AS idss_test_vds
    FROM idss_per_device
    GROUP BY device_type, manufacturer
)

/* ── STEP 3: Final output – mean values with statistics ──────────────── */

SELECT
    COALESCE(v.device_type,  r.device_type,  i.device_type,  s.device_type,
             g.device_type,  ss.device_type, io.device_type, bv.device_type,
             iss.device_type)                                       AS device_type,
    COALESCE(v.manufacturer, r.manufacturer, i.manufacturer, s.manufacturer,
             g.manufacturer, ss.manufacturer, io.manufacturer, bv.manufacturer,
             iss.manufacturer)                                       AS manufacturer,

    -- Threshold voltage (mean across devices) + conditions + spread
    ROUND(v.vth_v::numeric,           2)  AS vth_v,
    ROUND(v.vth_test_vds::numeric,    1)  AS vth_test_vds,
    ROUND(v.vth_thresh_ma::numeric,   3)  AS vth_thresh_ma,
    ROUND(v.vth_v_std::numeric,       3)  AS vth_v_std,
    ROUND(v.vth_v_min::numeric,       2)  AS vth_v_min,
    ROUND(v.vth_v_max::numeric,       2)  AS vth_v_max,
    v.vth_n_devices,

    -- On-state resistance (mean across devices) + Vgs condition + spread
    ROUND(r.rdson_mohm::numeric,      1)  AS rdson_mohm,
    ROUND(r.rdson_test_vgs::numeric,  1)  AS rdson_test_vgs,
    ROUND(r.rdson_mohm_std::numeric,  2)  AS rdson_mohm_std,
    ROUND(r.rdson_mohm_min::numeric,  1)  AS rdson_mohm_min,
    ROUND(r.rdson_mohm_max::numeric,  1)  AS rdson_mohm_max,
    r.rdson_n_devices,

    -- Gate leakage (mean across devices) + Vgs condition + spread
    ROUND(i.igss_max_na::numeric,     2)  AS igss_max_na,
    ROUND(i.igss_test_vgs::numeric,   1)  AS igss_test_vgs,
    ROUND(i.igss_max_na_std::numeric, 3)  AS igss_max_na_std,
    ROUND(i.igss_max_na_min::numeric, 2)  AS igss_max_na_min,
    ROUND(i.igss_max_na_max::numeric, 2)  AS igss_max_na_max,
    i.igss_n_devices,

    -- Body diode forward voltage (mean across devices) + conditions + spread
    ROUND(s.vsd_v::numeric,           2)  AS vsd_v,
    ROUND(s.vsd_test_vgs::numeric,    1)  AS vsd_test_vgs,
    ROUND(s.vsd_ref_id_a::numeric,    2)  AS vsd_ref_id_a,
    ROUND(s.vsd_v_std::numeric,       3)  AS vsd_v_std,
    ROUND(s.vsd_v_min::numeric,       2)  AS vsd_v_min,
    ROUND(s.vsd_v_max::numeric,       2)  AS vsd_v_max,
    s.vsd_n_devices,

    -- Transconductance (mean across devices) + Vds condition + spread
    ROUND(g.gfs_s::numeric,           2)  AS gfs_s,
    ROUND(g.v_gfs_peak_v::numeric,    1)  AS v_gfs_peak_v,
    ROUND(g.gfs_test_vds::numeric,    1)  AS gfs_test_vds,
    ROUND(g.gfs_s_std::numeric,       3)  AS gfs_s_std,
    ROUND(g.gfs_s_min::numeric,       2)  AS gfs_s_min,
    ROUND(g.gfs_s_max::numeric,       2)  AS gfs_s_max,
    g.gfs_n_devices,

    -- Subthreshold slope (mean across devices) + spread
    ROUND(ss.ss_mv_dec::numeric,      0)  AS ss_mv_dec,
    ROUND(ss.ss_mv_dec_std::numeric,  1)  AS ss_mv_dec_std,
    ROUND(ss.ss_mv_dec_min::numeric,  0)  AS ss_mv_dec_min,
    ROUND(ss.ss_mv_dec_max::numeric,  0)  AS ss_mv_dec_max,
    ss.ss_n_devices,

    -- On-state saturation current (mean across devices) + conditions + spread
    ROUND(io.id_on_a::numeric,        1)  AS id_on_a,
    ROUND(io.id_on_test_vds::numeric, 1)  AS id_on_test_vds,
    ROUND(io.id_on_test_vgs::numeric, 1)  AS id_on_test_vgs,
    ROUND(io.id_on_a_std::numeric,    2)  AS id_on_a_std,
    ROUND(io.id_on_a_min::numeric,    1)  AS id_on_a_min,
    ROUND(io.id_on_a_max::numeric,    1)  AS id_on_a_max,
    io.id_on_n_devices,

    -- Breakdown voltage (mean across devices) + flag + spread
    ROUND(bv.bvdss_v::numeric,        0)  AS bvdss_v,
    bv.bvdss_flag                         AS bvdss_flag,
    ROUND(bv.bvdss_v_std::numeric,    1)  AS bvdss_v_std,
    ROUND(bv.bvdss_v_min::numeric,    0)  AS bvdss_v_min,
    ROUND(bv.bvdss_v_max::numeric,    0)  AS bvdss_v_max,
    bv.bvdss_n_devices,
    bv.bvdss_n_breakdown,

    -- Off-state leakage (mean across devices) + Vds condition + spread
    ROUND(iss.idss_ua::numeric,       3)  AS idss_ua,
    ROUND(iss.idss_test_vds::numeric, 0)  AS idss_test_vds,
    ROUND(iss.idss_ua_std::numeric,   4)  AS idss_ua_std,
    ROUND(iss.idss_ua_min::numeric,   3)  AS idss_ua_min,
    ROUND(iss.idss_ua_max::numeric,   3)  AS idss_ua_max,
    iss.idss_n_devices

FROM      vth_vals   v
FULL JOIN rdson_vals r   USING (device_type, manufacturer)
FULL JOIN igss_vals  i   USING (device_type, manufacturer)
FULL JOIN vsd_vals   s   USING (device_type, manufacturer)
FULL JOIN gfs_vals   g   USING (device_type, manufacturer)
FULL JOIN ss_vals    ss  USING (device_type, manufacturer)
FULL JOIN idon_vals  io  USING (device_type, manufacturer)
FULL JOIN bv_vals    bv  USING (device_type, manufacturer)
FULL JOIN idss_vals  iss USING (device_type, manufacturer)
ORDER BY device_type
"""

def find_or_create_virtual_dataset(session, db_id, name, sql_query,
                                   schema="public"):
    """Find or create a SQL-based (virtual) dataset; update SQL if it exists."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/dataset/",
        params={"q": json.dumps({
            "filters": [{"col": "table_name", "opr": "eq", "value": name}],
            "page_size": 100,
        })},
    )
    resp.raise_for_status()
    for ds in resp.json()["result"]:
        if ds.get("table_name") == name:
            ds_id = ds["id"]
            session.put(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
                        json={"sql": sql_query})
            print(f"  Virtual dataset '{name}' exists (id={ds_id})")
            return ds_id

    resp = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json={
        "database": db_id,
        "table_name": name,
        "schema": schema,
        "sql": sql_query,
    })
    if resp.ok:
        ds_id = resp.json()["id"]
        print(f"  Created virtual dataset '{name}' (id={ds_id})")
        return ds_id
    print(f"  ERROR creating virtual dataset '{name}': "
          f"{resp.status_code} {resp.text[:200]}")
    return None


# refresh_dataset_columns() and create_chart() are imported from superset_api.


# ── Dashboard Layout ─────────────────────────────────────────────────────────

def build_dashboard_layout(charts, sigma_charts=None, individual_charts=None):
    """Build position_json from (chart_id, uuid, name, width, height) tuples.

    If *sigma_charts* is provided, creates a tabbed layout:
    'Mean Curves' (first tab, shown by default) and '±1σ Bands' (second tab).
    If *individual_charts* is also provided, adds a third 'Individual Runs' tab.
    """
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": [], "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER", "id": "HEADER_ID",
            "meta": {"text": "Baselines Device Library"},
        },
    }
    def _add_chart_rows(chart_list, prefix, parents):
        """Add ROW+CHART entries for a list of charts. Returns row IDs."""
        row_ids = []
        for i, (cid, cuuid, cname, width, height) in enumerate(chart_list):
            if cid is None:
                continue
            row_id = f"ROW-{prefix}-{i}"
            chart_key = f"CHART-{prefix}-{i}"
            layout[row_id] = {
                "type": "ROW", "id": row_id,
                "children": [chart_key],
                "parents": parents,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            layout[chart_key] = {
                "type": "CHART", "id": chart_key, "children": [],
                "parents": parents + [row_id],
                "meta": {
                    "chartId": cid, "width": width, "height": height,
                    "sliceName": cname, "uuid": cuuid,
                },
            }
            row_ids.append(row_id)
        return row_ids

    if sigma_charts:
        # Tabbed layout: Mean Curves + ±1σ Bands + (optionally) Individual Runs
        tabs_id = "TABS-bl"
        tab_mean_id = "TAB-mean"
        tab_sigma_id = "TAB-sigma"
        tab_indiv_id = "TAB-individual"

        tab_children = [tab_mean_id, tab_sigma_id]
        if individual_charts:
            tab_children.append(tab_indiv_id)

        layout["GRID_ID"]["children"] = [tabs_id]
        layout[tabs_id] = {
            "type": "TABS", "id": tabs_id,
            "children": tab_children,
            "parents": ["ROOT_ID", "GRID_ID"],
        }

        mean_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_mean_id]
        mean_rows = _add_chart_rows(charts, "mean", mean_parents)
        layout[tab_mean_id] = {
            "type": "TAB", "id": tab_mean_id,
            "children": mean_rows,
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": "Mean Curves"},
        }

        sigma_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_sigma_id]
        sigma_rows = _add_chart_rows(sigma_charts, "sigma", sigma_parents)
        layout[tab_sigma_id] = {
            "type": "TAB", "id": tab_sigma_id,
            "children": sigma_rows,
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": "\u00b11\u03c3 Bands"},
        }

        if individual_charts:
            indiv_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_indiv_id]
            indiv_rows = _add_chart_rows(individual_charts, "indiv",
                                         indiv_parents)
            layout[tab_indiv_id] = {
                "type": "TAB", "id": tab_indiv_id,
                "children": indiv_rows,
                "parents": ["ROOT_ID", "GRID_ID", tabs_id],
                "meta": {"text": "Individual Runs"},
            }
    else:
        # Flat layout (no sigma charts)
        row_children = _add_chart_rows(charts, "bl", ["ROOT_ID", "GRID_ID"])
        layout["GRID_ID"]["children"] = row_children

    return layout


# ── Native Filters ───────────────────────────────────────────────────────────

def build_native_filters(chart_ids, avg_ds_id, always_excluded=None,
                         v_drain_chart_ids=None, v_gate_chart_ids=None,
                         indiv_ds_id=None, calc_ds_id=None,
                         meta_ds_id=None):
    """
    Five native filters for the averaged device-performance dashboard:

    1. Likely Irradiated    – boolean, defaults to false (pristine only)
    2. Manufacturer         – optional, multi-select
    3. Device Type          – required, cascades from Manufacturer
    4. V_Drain Bias (V)     – optional range, scoped to IdVg/Vth charts
    5. V_Gate Bias (V)      – optional range, scoped to IdVd/3rdQ charts

    If *indiv_ds_id* is provided, the bias filters also target the
    individual-runs dataset (same column names: v_drain_bias, v_gate_bias).
    If *meta_ds_id* is provided, Manufacturer and Device Type filters also
    target baselines_metadata so the TSP Parameters table is filtered.
    *calc_ds_id* is accepted but not used as a filter target — the calculated-
    parameters chart is excluded from all filters (always-excluded) to avoid
    a failing virtual-dataset SQL from breaking the filter dropdowns.
    """
    irr_fid = "NATIVE_FILTER-likely-irradiated"
    mfr_fid = "NATIVE_FILTER-manufacturer"
    dev_fid = "NATIVE_FILTER-device-type"
    vd_fid  = "NATIVE_FILTER-v-drain-bias"
    vg_fid  = "NATIVE_FILTER-v-gate-bias"

    always_excluded = always_excluded or []
    filtered = [c for c in chart_ids if c not in always_excluded]
    v_drain_chart_ids = v_drain_chart_ids or []
    v_gate_chart_ids = v_gate_chart_ids or []

    # Charts NOT in a bias filter's scope must be excluded for that filter
    vd_excluded = [c for c in chart_ids if c not in v_drain_chart_ids]
    vg_excluded = [c for c in chart_ids if c not in v_gate_chart_ids]

    filters = [
        {
            "id": mfr_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Manufacturer",
            "filterType": "filter_select",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "manufacturer"}}]
                       + ([{"datasetId": meta_ds_id,
                            "column": {"name": "manufacturer"}}]
                          if meta_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": list(always_excluded)},
            "type": "NATIVE_FILTER",
            "description": "Filter by device manufacturer",
            "chartsInScope": filtered,
            "tabsInScope": [],
        },
        {
            "id": dev_fid,
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": "Device Type",
            "filterType": "filter_select",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "device_type"}}]
                       + ([{"datasetId": meta_ds_id,
                            "column": {"name": "device_type"}}]
                          if meta_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [mfr_fid],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": list(always_excluded)},
            "type": "NATIVE_FILTER",
            "description": "Select device type(s) from the device library",
            "chartsInScope": filtered,
            "tabsInScope": [],
        },
        {
            "id": vd_fid,
            "controlValues": {
                "enableEmptyFilter": False,
            },
            "name": "V_Drain Bias (V) → IdVg, Vth",
            "filterType": "filter_range",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "v_drain_bias"}}]
                       + ([{"datasetId": indiv_ds_id,
                            "column": {"name": "v_drain_bias"}}]
                          if indiv_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": vd_excluded},
            "type": "NATIVE_FILTER",
            "description": "Select V_Drain bias values (IdVg / Vth charts)",
            "chartsInScope": v_drain_chart_ids,
            "tabsInScope": [],
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression":
                    "measurement_category IN ('IdVg', 'Vth')",
                "clause": "WHERE",
            }],
        },
        {
            "id": vg_fid,
            "controlValues": {
                "enableEmptyFilter": False,
            },
            "name": "V_Gate Bias (V) → IdVd, 3rd Quadrant",
            "filterType": "filter_range",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "v_gate_bias"}}]
                       + ([{"datasetId": indiv_ds_id,
                            "column": {"name": "v_gate_bias"}}]
                          if indiv_ds_id else []),
            "defaultDataMask": {"extraFormData": {},
                                "filterState": {"value": None}},
            "cascadeParentIds": [dev_fid],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": vg_excluded},
            "type": "NATIVE_FILTER",
            "description": "Select V_Gate bias values (IdVd / 3rd Quadrant charts)",
            "chartsInScope": v_gate_chart_ids,
            "tabsInScope": [],
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression":
                    "measurement_category IN ('IdVd', '3rd_Quadrant')",
                "clause": "WHERE",
            }],
        },
        {
            "id": irr_fid,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "name": "Likely Irradiated",
            "filterType": "filter_select",
            "targets": [{"datasetId": avg_ds_id,
                         "column": {"name": "is_likely_irradiated"}}]
                       + ([{"datasetId": indiv_ds_id,
                            "column": {"name": "is_likely_irradiated"}}]
                          if indiv_ds_id else [])
                       + ([{"datasetId": meta_ds_id,
                            "column": {"name": "is_likely_irradiated"}}]
                          if meta_ds_id else []),
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {"value": None},
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"],
                      "excluded": list(always_excluded)},
            "type": "NATIVE_FILTER",
            "description": "Filter by irradiation status "
                           "(select false for pristine-only, true for "
                           "irradiated-only, or leave empty for all data)",
            "chartsInScope": filtered,
            "tabsInScope": [],
        },
    ]
    return filters


# build_json_metadata() and create_or_update_dashboard() are imported from superset_api.


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Creating Baselines Device Library Dashboard (Averaged Performance)")
    print("=" * 70)

    # 1. Authenticate
    print("\n1. Authenticating...")
    session = get_session()
    print("   OK")

    # 2. Find database
    print("\n2. Finding database...")
    db_id = find_database(session)
    if not db_id:
        print("  Please add the mosfets database connection first.")
        sys.exit(1)

    # 3. Create datasets
    print("\n3. Creating datasets...")
    avg_ds = find_or_create_dataset(session, db_id, "baselines_device_averages")
    devlib_ds = find_or_create_dataset(session, db_id, "device_library")
    indiv_ds = find_or_create_dataset(session, db_id,
                                      "baselines_view_device_library")
    if not avg_ds:
        print("  FATAL: Could not create baselines_device_averages dataset.")
        print("  Run ingestion_baselines.py first to create the view.")
        sys.exit(1)
    if not indiv_ds:
        print("  WARNING: Could not create baselines_view_device_library dataset.")
        print("  Individual Runs tab will be skipped.")
    meta_ds = find_or_create_dataset(session, db_id, "baselines_metadata")
    if not meta_ds:
        print("  WARNING: Could not create baselines_metadata dataset.")
        print("  TSP Parameters table will be skipped.")
    calc_ds = find_or_create_virtual_dataset(
        session, db_id, "device_calculated_params", CALCULATED_PARAMS_SQL
    )
    if not calc_ds:
        print("  WARNING: Could not create device_calculated_params dataset.")
        print("  Calculated Parameters chart will be skipped.")
    for ds_id in [avg_ds, devlib_ds, indiv_ds, meta_ds, calc_ds]:
        if ds_id:
            refresh_dataset_columns(session, ds_id)

    # 4. Create charts
    print("\n4. Creating charts...")

    # Common adhoc filter: only show bins with ≥2 devices (real averaging)
    min_dev_filter = {
        "expressionType": "SQL",
        "sqlExpression": "n_devices >= 2",
        "clause": "WHERE",
    }

    # Helper to build a category filter
    def cat_filter(cat):
        return {
            "expressionType": "SQL",
            "sqlExpression": f"measurement_category = '{cat}'",
            "clause": "WHERE",
        }

    # Simple line-chart params for averaged curve charts.
    # Each chart groups by (device_type, coarsened_bias) so that every
    # bias condition appears as a separate line — matching the clean
    # per-experiment look of the Baselines dashboard.
    def curve_params(x_axis, bias_col, bias_round, cat, x_title, y_title,
                     metric_expr="AVG(avg_i_drain)",
                     metric_label="Mean I_Drain (A)",
                     log_y=False, series_limit=0):
        """
        Parameters
        ----------
        x_axis     : str       – column for the x-axis (swept voltage)
        bias_col   : str|None  – column to group bias conditions by
        bias_round : int       – rounding divisor for the bias column
                                 (1 = integer, 5 = 5 V steps, etc.)
        cat        : str       – measurement_category value
        """
        groupby = ["device_type"]
        if bias_col:
            if bias_round == 1:
                sql = f"ROUND({bias_col})"
            else:
                sql = f"ROUND({bias_col} / {bias_round}) * {bias_round}"
            groupby.append({
                "expressionType": "SQL",
                "sqlExpression": sql,
                "label": bias_col.replace("_bin", " (V)"),
            })

        params = {
            "x_axis": x_axis,
            "time_grain_sqla": None,
            "x_axis_sort_asc": True,
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": metric_expr,
                "label": metric_label,
            }],
            "groupby": groupby,
            "adhoc_filters": [cat_filter(cat), min_dev_filter],
            "row_limit": 50000,
            "truncate_metric": True,
            "show_legend": True,
            "legendType": "scroll",
            "rich_tooltip": True,
            "x_axis_title": x_title,
            "y_axis_title": y_title,
            "y_axis_format": "SMART_NUMBER",
            "truncateYAxis": False,
            "y_axis_bounds": [None, None],
            "tooltipTimeFormat": "smart_date",
            "markerEnabled": False,
            "connectNulls": True,
            "zoomable": True,
            "sort_series_type": "max",
            "sort_series_ascending": False,
        }
        if log_y:
            params["logAxis"] = "y"
        if series_limit:
            params["series_limit"] = series_limit
            params["series_limit_metric"] = {
                "expressionType": "SQL",
                "sqlExpression": "SUM(n_points)",
                "label": "_rank_by_frequency",
            }
        return params

    def sigma_curve_params(x_axis, bias_col, bias_round, cat, x_title, y_title,
                          upper_expr="AVG(upper_i_drain)",
                          lower_expr="AVG(lower_i_drain)",
                          upper_label="+1\u03c3 I_Drain (A)",
                          lower_label="\u22121\u03c3 I_Drain (A)",
                          log_y=False, series_limit=0):
        """Like curve_params but with two metrics: upper and lower ±1σ bounds."""
        groupby = ["device_type"]
        if bias_col:
            if bias_round == 1:
                sql = f"ROUND({bias_col})"
            else:
                sql = f"ROUND({bias_col} / {bias_round}) * {bias_round}"
            groupby.append({
                "expressionType": "SQL",
                "sqlExpression": sql,
                "label": bias_col.replace("_bin", " (V)"),
            })

        params = {
            "x_axis": x_axis,
            "time_grain_sqla": None,
            "x_axis_sort_asc": True,
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": upper_expr,
                    "label": upper_label,
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": lower_expr,
                    "label": lower_label,
                },
            ],
            "groupby": groupby,
            "adhoc_filters": [cat_filter(cat), min_dev_filter],
            "row_limit": 50000,
            "truncate_metric": True,
            "show_legend": True,
            "legendType": "scroll",
            "rich_tooltip": True,
            "x_axis_title": x_title,
            "y_axis_title": y_title,
            "y_axis_format": "SMART_NUMBER",
            "truncateYAxis": False,
            "y_axis_bounds": [None, None],
            "tooltipTimeFormat": "smart_date",
            "markerEnabled": False,
            "connectNulls": True,
            "zoomable": True,
            "sort_series_type": "max",
            "sort_series_ascending": False,
        }
        if log_y:
            params["logAxis"] = "y"
        if series_limit:
            params["series_limit"] = series_limit
            params["series_limit_metric"] = {
                "expressionType": "SQL",
                "sqlExpression": "SUM(n_points)",
                "label": "_rank_by_frequency",
            }
        return params

    chart_defs = [
        # 0 – Data Summary: how many devices / runs per category
        (
            "Device Library – Data Summary",
            avg_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["manufacturer", "device_type",
                            "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_devices)",
                     "label": "Devices Averaged"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_runs)",
                     "label": "Total Runs"},
                    {"expressionType": "SQL",
                     "sqlExpression": "SUM(n_points)",
                     "label": "Total Points"},
                ],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 10000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),

        # 1 – IdVg Transfer Curves
        #     Sweep V_gate, one line per (device, V_drain bias)
        #     V_drain has ~6 integer values → readable
        (
            "Device Library – IdVg Transfer Curves",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_gate_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="IdVg",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 2 – Vth Curves
        #     Sweep V_gate, one line per (device, V_drain bias)
        #     V_drain has ~3 values → very clean
        (
            "Device Library – Vth Curves (Mean)",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_gate_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="Vth",
                x_title="V_Gate (V)",
                y_title="Mean I_Drain (A)",
            ),
            12, 60,
        ),

        # 3 – IdVd Output Curves
        #     Sweep V_drain, one line per (device, V_gate bias)
        #     Use integer V_gate rounding; series_limit keeps the
        #     most common bias values to avoid cluttered legends.
        (
            "Device Library – IdVd Output Curves",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_drain_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="IdVd",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                series_limit=10,
            ),
            12, 60,
        ),

        # 4 – 3rd Quadrant
        #     Sweep V_drain, one line per (device, V_gate bias)
        #     V_gate has ~8 integer values → readable
        (
            "Device Library – 3rd Quadrant",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_drain_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="3rd_Quadrant",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 5 – Igss Mean Gate Leakage (log Y, no bias dimension)
        (
            "Device Library – Igss Gate Leakage (Mean)",
            avg_ds,
            "echarts_timeseries_line",
            curve_params(
                x_axis="v_gate_bin",
                bias_col=None, bias_round=1,
                cat="Igss",
                x_title="V_Gate (V)",
                y_title="Mean |I_Gate| (A)",
                metric_expr="AVG(avg_abs_i_gate)",
                metric_label="Mean |I_Gate| (A)",
                log_y=True,
            ),
            12, 60,
        ),

        # 6 – Consistency: coefficient of variation (std/|mean|)
        (
            "Device Library – Run-to-Run Consistency",
            avg_ds,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["device_type", "measurement_category"],
                "metrics": [
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_devices)",
                     "label": "Max Devices"},
                    {"expressionType": "SQL",
                     "sqlExpression": "MAX(n_runs)",
                     "label": "Max Runs"},
                    {"expressionType": "SQL",
                     "sqlExpression": (
                         "AVG(CASE WHEN ABS(avg_i_drain) > 1e-10 "
                         "THEN std_i_drain / ABS(avg_i_drain) END)"
                     ),
                     "label": "Avg CV (σ/μ)"},
                    {"expressionType": "SQL",
                     "sqlExpression": (
                         "AVG(CASE WHEN ABS(avg_i_drain) > 1e-10 "
                         "THEN std_i_drain END)"
                     ),
                     "label": "Avg Std Dev"},
                ],
                "adhoc_filters": [min_dev_filter],
                "all_columns": [],
                "order_by_cols": [],
                "row_limit": 1000,
                "include_time": False,
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),

        # 7 – Registered Devices (unfiltered reference table)
        (
            "Device Library – Registered Devices",
            devlib_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "part_number", "device_category", "manufacturer",
                    "voltage_rating", "rdson_mohm", "current_rating_a",
                    "package_type", "notes",
                ],
                "row_limit": 500,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ),
    ]

    # 8 – Calculated Parameters (virtual dataset)
    # Per-device-first: each parameter is extracted from each individual
    # device's curve, then aggregated (mean ± σ, min, max, n_devices)
    # across devices.  No hardcoded bias values.  Works for any device type.
    # Each parameter is paired with test conditions and spread statistics.
    if calc_ds:
        chart_defs.append((
            "Device Library – Calculated Parameters",
            calc_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "device_type", "manufacturer",
                    # Vth group
                    "vth_v", "vth_test_vds", "vth_thresh_ma",
                    "vth_v_std", "vth_v_min", "vth_v_max", "vth_n_devices",
                    # Rds(on) group
                    "rdson_mohm", "rdson_test_vgs",
                    "rdson_mohm_std", "rdson_mohm_min", "rdson_mohm_max",
                    "rdson_n_devices",
                    # Igss group
                    "igss_max_na", "igss_test_vgs",
                    "igss_max_na_std", "igss_max_na_min", "igss_max_na_max",
                    "igss_n_devices",
                    # Vsd group
                    "vsd_v", "vsd_test_vgs", "vsd_ref_id_a",
                    "vsd_v_std", "vsd_v_min", "vsd_v_max", "vsd_n_devices",
                    # gfs group
                    "gfs_s", "v_gfs_peak_v", "gfs_test_vds",
                    "gfs_s_std", "gfs_s_min", "gfs_s_max", "gfs_n_devices",
                    # Subthreshold slope
                    "ss_mv_dec",
                    "ss_mv_dec_std", "ss_mv_dec_min", "ss_mv_dec_max",
                    "ss_n_devices",
                    # Id_on group
                    "id_on_a", "id_on_test_vds", "id_on_test_vgs",
                    "id_on_a_std", "id_on_a_min", "id_on_a_max",
                    "id_on_n_devices",
                    # BV_DSS group
                    "bvdss_v", "bvdss_flag",
                    "bvdss_v_std", "bvdss_v_min", "bvdss_v_max",
                    "bvdss_n_devices", "bvdss_n_breakdown",
                    # IDSS group
                    "idss_ua", "idss_test_vds",
                    "idss_ua_std", "idss_ua_min", "idss_ua_max",
                    "idss_n_devices",
                ],
                "column_config": {
                    "vth_v":            {"label": "V_th (V)"},
                    "vth_test_vds":     {"label": "V_th: test Vds (V)"},
                    "vth_thresh_ma":    {"label": "V_th: threshold Id (mA)"},
                    "vth_v_std":        {"label": "V_th σ (V)"},
                    "vth_v_min":        {"label": "V_th min (V)"},
                    "vth_v_max":        {"label": "V_th max (V)"},
                    "vth_n_devices":    {"label": "V_th: # devices"},
                    "rdson_mohm":       {"label": "Rds(on) (mΩ)"},
                    "rdson_test_vgs":   {"label": "Rds(on): test Vgs (V)"},
                    "rdson_mohm_std":   {"label": "Rds(on) σ (mΩ)"},
                    "rdson_mohm_min":   {"label": "Rds(on) min (mΩ)"},
                    "rdson_mohm_max":   {"label": "Rds(on) max (mΩ)"},
                    "rdson_n_devices":  {"label": "Rds(on): # devices"},
                    "igss_max_na":      {"label": "Igss max (nA)"},
                    "igss_test_vgs":    {"label": "Igss: test Vgs (V)"},
                    "igss_max_na_std":  {"label": "Igss σ (nA)"},
                    "igss_max_na_min":  {"label": "Igss min (nA)"},
                    "igss_max_na_max":  {"label": "Igss max val (nA)"},
                    "igss_n_devices":   {"label": "Igss: # devices"},
                    "vsd_v":            {"label": "V_sd (V)"},
                    "vsd_test_vgs":     {"label": "V_sd: test Vgs (V)"},
                    "vsd_ref_id_a":     {"label": "V_sd: ref Id (A)"},
                    "vsd_v_std":        {"label": "V_sd σ (V)"},
                    "vsd_v_min":        {"label": "V_sd min (V)"},
                    "vsd_v_max":        {"label": "V_sd max (V)"},
                    "vsd_n_devices":    {"label": "V_sd: # devices"},
                    "gfs_s":            {"label": "g_fs (S)"},
                    "v_gfs_peak_v":     {"label": "g_fs: Vgs at peak (V)"},
                    "gfs_test_vds":     {"label": "g_fs: test Vds (V)"},
                    "gfs_s_std":        {"label": "g_fs σ (S)"},
                    "gfs_s_min":        {"label": "g_fs min (S)"},
                    "gfs_s_max":        {"label": "g_fs max (S)"},
                    "gfs_n_devices":    {"label": "g_fs: # devices"},
                    "ss_mv_dec":        {"label": "Subthresh. Slope (mV/dec)"},
                    "ss_mv_dec_std":    {"label": "SS σ (mV/dec)"},
                    "ss_mv_dec_min":    {"label": "SS min (mV/dec)"},
                    "ss_mv_dec_max":    {"label": "SS max (mV/dec)"},
                    "ss_n_devices":     {"label": "SS: # devices"},
                    "id_on_a":          {"label": "Id_on (A)"},
                    "id_on_test_vds":   {"label": "Id_on: test Vds (V)"},
                    "id_on_test_vgs":   {"label": "Id_on: test Vgs (V)"},
                    "id_on_a_std":      {"label": "Id_on σ (A)"},
                    "id_on_a_min":      {"label": "Id_on min (A)"},
                    "id_on_a_max":      {"label": "Id_on max (A)"},
                    "id_on_n_devices":  {"label": "Id_on: # devices"},
                    "bvdss_v":          {"label": "V(BR)DSS (V)  @ Id=100µA"},
                    "bvdss_flag":       {"label": "V(BR)DSS: result"},
                    "bvdss_v_std":      {"label": "V(BR)DSS σ (V)"},
                    "bvdss_v_min":      {"label": "V(BR)DSS min (V)"},
                    "bvdss_v_max":      {"label": "V(BR)DSS max (V)"},
                    "bvdss_n_devices":  {"label": "V(BR)DSS: # devices"},
                    "bvdss_n_breakdown": {"label": "V(BR)DSS: # breakdown"},
                    "idss_ua":          {"label": "IDSS (µA)"},
                    "idss_test_vds":    {"label": "IDSS: test Vds (V)"},
                    "idss_ua_std":      {"label": "IDSS σ (µA)"},
                    "idss_ua_min":      {"label": "IDSS min (µA)"},
                    "idss_ua_max":      {"label": "IDSS max (µA)"},
                    "idss_n_devices":   {"label": "IDSS: # devices"},
                },
                "row_limit": 500,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 60,
        ))

    # ── ±1σ chart definitions (one per curve chart) ────────────────────────
    sigma_chart_defs = [
        # 0 – IdVg ±1σ
        (
            "Device Library – IdVg Transfer Curves (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_gate_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="IdVg",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 1 – Vth ±1σ
        (
            "Device Library – Vth Curves (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_gate_bin",
                bias_col="v_drain_bin", bias_round=1,
                cat="Vth",
                x_title="V_Gate (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 2 – IdVd ±1σ
        (
            "Device Library – IdVd Output Curves (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_drain_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="IdVd",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
                series_limit=10,
            ),
            12, 60,
        ),

        # 3 – 3rd Quadrant ±1σ
        (
            "Device Library – 3rd Quadrant (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_drain_bin",
                bias_col="v_gate_bin", bias_round=1,
                cat="3rd_Quadrant",
                x_title="V_Drain (V)",
                y_title="I_Drain (A)",
            ),
            12, 60,
        ),

        # 4 – Igss ±1σ
        (
            "Device Library – Igss Gate Leakage (±1σ)",
            avg_ds,
            "echarts_timeseries_line",
            sigma_curve_params(
                x_axis="v_gate_bin",
                bias_col=None, bias_round=1,
                cat="Igss",
                x_title="V_Gate (V)",
                y_title="|I_Gate| (A)",
                upper_expr="AVG(avg_abs_i_gate + COALESCE(std_i_gate, 0))",
                lower_expr="AVG(GREATEST(avg_abs_i_gate - COALESCE(std_i_gate, 0), 0))",
                upper_label="+1σ |I_Gate| (A)",
                lower_label="−1σ |I_Gate| (A)",
                log_y=True,
            ),
            12, 60,
        ),
    ]

    # ── Individual Runs chart definitions (one per curve chart) ──────────
    # These use baselines_view_device_library and group by device_id so
    # each physical run appears as its own line.

    def indiv_curve_params(x_axis, cat, x_title, y_title,
                           metric_expr="AVG(i_drain)",
                           metric_label="I_Drain (A)",
                           log_y=False, series_limit=0,
                           bias_col=None):
        """Line-chart params for individual-run curves.

        Groups by (device_id, measurement_type, bias_col) so every run
        at each bias condition is a separate line.  A series_limit keeps
        the chart readable.
        """
        groupby = ["device_id", "measurement_type"]
        if bias_col:
            groupby.append(bias_col)
        params = {
            "x_axis": x_axis,
            "time_grain_sqla": None,
            "x_axis_sort_asc": True,
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": metric_expr,
                "label": metric_label,
            }],
            "groupby": groupby,
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression": f"measurement_category = '{cat}'",
                "clause": "WHERE",
            }],
            "row_limit": 100000,
            "truncate_metric": True,
            "show_legend": True,
            "legendType": "scroll",
            "rich_tooltip": True,
            "x_axis_title": x_title,
            "y_axis_title": y_title,
            "y_axis_format": "SMART_NUMBER",
            "truncateYAxis": False,
            "y_axis_bounds": [None, None],
            "tooltipTimeFormat": "smart_date",
            "markerEnabled": False,
            "connectNulls": True,
            "zoomable": True,
            "sort_series_type": "max",
            "sort_series_ascending": False,
        }
        if log_y:
            params["logAxis"] = "y"
        if series_limit:
            params["series_limit"] = series_limit
            # Rank by number of *distinct* x-axis values so actual sweeps
            # (many voltage points) outrank single-point measurements
            # (e.g. Rdson at one fixed v_drain).
            params["series_limit_metric"] = {
                "expressionType": "SQL",
                "sqlExpression": f"COUNT(DISTINCT {x_axis})",
                "label": "_rank_by_sweep_range",
            }
        return params

    individual_chart_defs = []
    if indiv_ds:
        individual_chart_defs = [
            # 0 – Run Summary table
            (
                "Device Library – Run Summary (Individual)",
                indiv_ds,
                "table",
                {
                    "query_mode": "aggregate",
                    "groupby": ["device_type", "device_id", "experiment",
                                "measurement_type", "measurement_category"],
                    "metrics": [
                        {"expressionType": "SQL",
                         "sqlExpression": "COUNT(*)",
                         "label": "Data Points"},
                    ],
                    "all_columns": [],
                    "order_by_cols": [],
                    "row_limit": 10000,
                    "include_time": False,
                    "table_timestamp_format": "smart_date",
                },
                12, 50,
            ),

            # 1 – IdVg Transfer Curves (Individual Runs)
            #     bias = V_drain (integer-rounded)
            (
                "Device Library – IdVg Transfer (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_gate_bin",
                    cat="IdVg",
                    x_title="V_Gate (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_drain_bias",
                ),
                12, 60,
            ),

            # 2 – Vth Curves (Individual Runs)
            #     bias = V_drain (integer-rounded)
            (
                "Device Library – Vth Curves (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_gate_bin",
                    cat="Vth",
                    x_title="V_Gate (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_drain_bias",
                ),
                12, 60,
            ),

            # 3 – IdVd Output Curves (Individual Runs)
            #     bias = V_gate (integer-rounded)
            (
                "Device Library – IdVd Output (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_drain_bin",
                    cat="IdVd",
                    x_title="V_Drain (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_gate_bias",
                ),
                12, 60,
            ),

            # 4 – 3rd Quadrant (Individual Runs)
            #     bias = V_gate (integer-rounded)
            (
                "Device Library – 3rd Quadrant (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_drain_bin",
                    cat="3rd_Quadrant",
                    x_title="V_Drain (V)",
                    y_title="I_Drain (A)",
                    bias_col="v_gate_bias",
                ),
                12, 60,
            ),

            # 5 – Igss Gate Leakage (Individual Runs)
            #     no bias dimension (single-variable sweep)
            (
                "Device Library – Igss Gate Leakage (Individual Runs)",
                indiv_ds,
                "echarts_timeseries_line",
                indiv_curve_params(
                    x_axis="v_gate_bin",
                    cat="Igss",
                    x_title="V_Gate (V)",
                    y_title="|I_Gate| (A)",
                    metric_expr="AVG(ABS(i_gate))",
                    metric_label="|I_Gate| (A)",
                    log_y=True,
                ),
                12, 60,
            ),
        ]

    # 6 – TSP Parameters table (on Individual Runs tab)
    #     Uses baselines_metadata directly; shows instrument settings
    #     per measurement run so users can diagnose disjointed curves.
    if meta_ds:
        individual_chart_defs.append((
            "Device Library – TSP Parameters",
            meta_ds,
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "device_type", "manufacturer",
                    "experiment", "device_id",
                    "measurement_type", "measurement_category",
                    "sweep_start", "sweep_stop", "sweep_points",
                    "bias_value", "bias_channel", "drain_bias_value",
                    "compliance_ch1", "compliance_ch2",
                    "meas_time", "hold_time", "plc",
                    "step_num", "step_start", "step_stop",
                    "delay_time",
                ],
                "adhoc_filters": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "tsp_path IS NOT NULL",
                        "clause": "WHERE",
                    },
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "device_type IS NOT NULL",
                        "clause": "WHERE",
                    },
                ],
                "row_limit": 5000,
                "include_time": False,
                "order_by_cols": [],
                "table_timestamp_format": "smart_date",
            },
            12, 50,
        ))

    # ── Create all charts ─────────────────────────────────────────────────
    charts_info = []  # (id, uuid, name, width, height)
    chart_ids_only = []

    for name, ds_id, viz_type, params, width, height in chart_defs:
        cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
        charts_info.append((cid, cuuid, name, width, height))
        if cid:
            chart_ids_only.append(cid)

    sigma_charts_info = []
    sigma_chart_ids = []

    print("\n   Creating ±1σ charts...")
    for name, ds_id, viz_type, params, width, height in sigma_chart_defs:
        cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
        sigma_charts_info.append((cid, cuuid, name, width, height))
        if cid:
            sigma_chart_ids.append(cid)

    indiv_charts_info = []
    indiv_chart_ids = []

    if individual_chart_defs:
        print("\n   Creating Individual Runs charts...")
        for name, ds_id, viz_type, params, width, height in individual_chart_defs:
            cid, cuuid = create_chart(session, name, ds_id, viz_type, params)
            indiv_charts_info.append((cid, cuuid, name, width, height))
            if cid:
                indiv_chart_ids.append(cid)

    all_chart_ids = chart_ids_only + sigma_chart_ids + indiv_chart_ids

    devlib_chart_id = charts_info[7][0]  # registered devices table
    # Calculated params chart is at index 8 if it was created; exclude it
    # from all filters to prevent a failing virtual-dataset SQL from breaking
    # the Manufacturer / Device Type filter dropdowns.
    calc_chart_id = charts_info[8][0] if len(charts_info) > 8 else None
    always_excluded = [c for c in [devlib_chart_id, calc_chart_id] if c]

    # Collect chart IDs for bias-filter scoping
    # V_Drain bias → IdVg Transfer + Vth (mean, sigma, individual)
    # Individual view now has v_drain_bias column matching the averages view.
    v_drain_ids = [
        charts_info[1][0], charts_info[2][0],           # mean IdVg, Vth
        sigma_charts_info[0][0], sigma_charts_info[1][0], # sigma IdVg, Vth
    ]
    if len(indiv_charts_info) > 5:
        v_drain_ids += [indiv_charts_info[1][0],         # indiv IdVg
                        indiv_charts_info[2][0]]         # indiv Vth
    v_drain_chart_ids = [c for c in v_drain_ids if c]

    # V_Gate bias → IdVd Output + 3rd Quadrant (mean, sigma, individual)
    v_gate_ids = [
        charts_info[3][0], charts_info[4][0],           # mean IdVd, 3rdQ
        sigma_charts_info[2][0], sigma_charts_info[3][0], # sigma IdVd, 3rdQ
    ]
    if len(indiv_charts_info) > 4:
        v_gate_ids += [indiv_charts_info[3][0],          # indiv IdVd
                       indiv_charts_info[4][0]]          # indiv 3rdQ
    v_gate_chart_ids = [c for c in v_gate_ids if c]

    # 5. Build dashboard with native filters (tabbed layout)
    print("\n5. Creating dashboard with native filters (tabbed layout)...")
    position_json = build_dashboard_layout(
        charts_info,
        sigma_charts=sigma_charts_info,
        individual_charts=indiv_charts_info or None,
    )
    native_filters = build_native_filters(
        all_chart_ids, avg_ds,
        always_excluded=always_excluded,
        v_drain_chart_ids=v_drain_chart_ids,
        v_gate_chart_ids=v_gate_chart_ids,
        indiv_ds_id=indiv_ds,
        calc_ds_id=calc_ds,
        meta_ds_id=meta_ds,
    )
    json_metadata = build_json_metadata(all_chart_ids, native_filters)
    dash_id = create_or_update_dashboard(
        session, "Baselines Device Library", position_json, json_metadata,
        slug="baselines-device-library",
    )

    # 6. Associate charts with dashboard
    print("\n6. Associating charts with dashboard...")
    if dash_id:
        for cid in all_chart_ids:
            resp = session.put(
                f"{SUPERSET_URL}/api/v1/chart/{cid}",
                json={"dashboards": [dash_id]},
            )
            status = "OK" if resp.ok else f"FAIL ({resp.status_code})"
            print(f"  Chart {cid} -> dashboard {dash_id}: {status}")

    print("\n" + "=" * 70)
    if dash_id:
        print("Dashboard ready!")
        print(f"  URL: {SUPERSET_URL}/superset/dashboard/baselines-device-library/")
        calc_chart_count = 1 if calc_ds else 0
        print(f"  Charts: {len(all_chart_ids)} "
              f"({len(chart_ids_only)} mean [{calc_chart_count} calculated]"
              f" + {len(sigma_chart_ids)} ±1σ"
              f" + {len(indiv_chart_ids)} individual)")
        print("  Tabs:")
        print("    1. Mean Curves      (default, shown on load)")
        print("    2. ±1σ Bands        (click tab to view)")
        print("    3. Individual Runs  (click tab to see every run + TSP params)")
        print("  Filters:")
        print("    1. Manufacturer         (optional, narrows device list)")
        print("    2. Device Type          (required, from device library)")
        print("    3. Measurement Category (required, cascades)")
        print("    4. V_Drain Bias (V)     (toggle biases for IdVg/Vth)")
        print("    5. V_Gate Bias (V)      (toggle biases for IdVd/3rdQ)")
        print("    6. Likely Irradiated    (all data by default; select false/true to filter)")
    else:
        print("Dashboard creation failed — see errors above.")
    print("=" * 70)


if __name__ == "__main__":
    main()
