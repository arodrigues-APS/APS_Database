-- Waveform-derived proxy-readiness features.
-- Owned by data_processing_scripts/create_proxy_readiness_dashboard.py.
-- apply_schema: pipeline-owned
--
-- This layer is intentionally descriptive.  It extracts waveform file/event
-- phenotypes and readiness coverage; it does not assert stress equivalence.

DROP MATERIALIZED VIEW IF EXISTS stress_proxy_candidate_summary_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_proxy_candidate_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_test_context_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_proxy_gate_zero_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_proxy_readiness_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_waveform_basis_feature_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_waveform_event_features CASCADE;
DROP MATERIALIZED VIEW IF EXISTS stress_waveform_file_features CASCADE;

CREATE MATERIALIZED VIEW stress_waveform_file_features AS
WITH file_metadata AS (
    SELECT
        'sc'::text AS source,
        md.id AS metadata_id,
        md.experiment,
        md.device_id,
        md.sample_group,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''))
            AS physical_sample_key,
        md.device_type,
        md.manufacturer,
        md.filename,
        md.csv_path,
        md.measurement_category,
        md.test_condition,
        md.sc_condition_label AS stress_condition_label,
        md.sc_voltage_v,
        md.sc_duration_us,
        md.sc_vgs_on_v,
        md.sc_vgs_off_v,
        NULL::text AS avalanche_family,
        NULL::text AS avalanche_mode,
        NULL::double precision AS avalanche_energy_j_metadata,
        NULL::double precision AS avalanche_peak_current_a_metadata,
        NULL::double precision AS avalanche_inductance_mh,
        NULL::double precision AS avalanche_temperature_c,
        NULL::double precision AS avalanche_gate_bias_v,
        NULL::text AS avalanche_outcome,
        NULL::integer AS irrad_run_id,
        NULL::text AS ion_species,
        NULL::double precision AS beam_energy_mev,
        NULL::double precision AS let_surface,
        NULL::double precision AS let_bragg_peak,
        NULL::double precision AS range_um,
        NULL::text AS beam_type,
        NULL::double precision AS fluence_at_meas
    FROM baselines_metadata md
    WHERE md.data_source = 'sc_ruggedness'
      AND md.measurement_category = 'SC_Waveform'

    UNION ALL

    SELECT
        'avalanche'::text AS source,
        md.id AS metadata_id,
        md.experiment,
        md.device_id,
        md.sample_group,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''))
            AS physical_sample_key,
        md.device_type,
        md.manufacturer,
        md.filename,
        md.csv_path,
        md.measurement_category,
        md.test_condition,
        md.avalanche_condition_label AS stress_condition_label,
        NULL::double precision AS sc_voltage_v,
        NULL::double precision AS sc_duration_us,
        NULL::double precision AS sc_vgs_on_v,
        NULL::double precision AS sc_vgs_off_v,
        md.avalanche_family,
        md.avalanche_mode,
        md.avalanche_energy_j AS avalanche_energy_j_metadata,
        md.avalanche_peak_current_a AS avalanche_peak_current_a_metadata,
        md.avalanche_inductance_mh,
        md.avalanche_temperature_c,
        md.avalanche_gate_bias_v,
        md.avalanche_outcome,
        NULL::integer AS irrad_run_id,
        NULL::text AS ion_species,
        NULL::double precision AS beam_energy_mev,
        NULL::double precision AS let_surface,
        NULL::double precision AS let_bragg_peak,
        NULL::double precision AS range_um,
        NULL::text AS beam_type,
        NULL::double precision AS fluence_at_meas
    FROM baselines_metadata md
    WHERE md.data_source = 'avalanche'

    UNION ALL

    SELECT
        'irradiation'::text AS source,
        md.id AS metadata_id,
        md.experiment,
        md.device_id,
        md.sample_group,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''))
            AS physical_sample_key,
        md.device_type,
        md.manufacturer,
        md.filename,
        md.csv_path,
        md.measurement_category,
        md.irrad_role AS test_condition,
        COALESCE(ir.ion_species, '?') || ' '
            || COALESCE(ir.beam_energy_mev::text, '?') || ' MeV '
            || COALESCE(ir.beam_type, ic.beam_type, '') AS stress_condition_label,
        NULL::double precision AS sc_voltage_v,
        NULL::double precision AS sc_duration_us,
        NULL::double precision AS sc_vgs_on_v,
        NULL::double precision AS sc_vgs_off_v,
        NULL::text AS avalanche_family,
        NULL::text AS avalanche_mode,
        NULL::double precision AS avalanche_energy_j_metadata,
        NULL::double precision AS avalanche_peak_current_a_metadata,
        NULL::double precision AS avalanche_inductance_mh,
        NULL::double precision AS avalanche_temperature_c,
        NULL::double precision AS avalanche_gate_bias_v,
        NULL::text AS avalanche_outcome,
        md.irrad_run_id,
        ir.ion_species,
        ir.beam_energy_mev,
        ir.let_surface,
        ir.let_bragg_peak,
        ir.range_um,
        COALESCE(ir.beam_type, ic.beam_type) AS beam_type,
        md.fluence_at_meas
    FROM baselines_metadata md
    JOIN irradiation_campaigns ic ON ic.id = md.irrad_campaign_id
    LEFT JOIN irradiation_runs ir ON ir.id = md.irrad_run_id
    WHERE md.irrad_campaign_id IS NOT NULL
      AND md.measurement_category = 'Irradiation'
),
raw_points AS (
    SELECT
        'sc'::text AS source,
        md.id AS metadata_id,
        m.point_index,
        CASE WHEN m.time_val IS NOT NULL AND ABS(m.time_val) < 1e30
             THEN m.time_val END AS time_s,
        CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
             THEN m.v_drain END AS vds,
        CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
             THEN m.i_drain END AS id_drain,
        CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
             THEN m.v_gate END AS vgs,
        CASE WHEN m.i_gate IS NOT NULL AND ABS(m.i_gate) < 1e30
             THEN m.i_gate END AS igs
    FROM baselines_measurements m
    JOIN baselines_metadata md ON md.id = m.metadata_id
    WHERE md.data_source = 'sc_ruggedness'
      AND md.measurement_category = 'SC_Waveform'

    UNION ALL

    SELECT
        'avalanche'::text AS source,
        md.id AS metadata_id,
        m.point_index,
        CASE WHEN m.time_val IS NOT NULL AND ABS(m.time_val) < 1e30
             THEN m.time_val END AS time_s,
        CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
             THEN m.v_drain END AS vds,
        CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
             THEN m.i_drain END AS id_drain,
        CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
             THEN m.v_gate END AS vgs,
        CASE WHEN m.i_gate IS NOT NULL AND ABS(m.i_gate) < 1e30
             THEN m.i_gate END AS igs
    FROM baselines_measurements m
    JOIN baselines_metadata md ON md.id = m.metadata_id
    WHERE md.data_source = 'avalanche'

    UNION ALL

    SELECT
        'irradiation'::text AS source,
        md.id AS metadata_id,
        m.point_index,
        CASE WHEN m.time_val IS NOT NULL AND ABS(m.time_val) < 1e30
             THEN m.time_val END AS time_s,
        CASE WHEN m.v_drain IS NOT NULL AND ABS(m.v_drain) < 1e30
             THEN m.v_drain END AS vds,
        CASE WHEN m.i_drain IS NOT NULL AND ABS(m.i_drain) < 1e30
             THEN m.i_drain END AS id_drain,
        CASE WHEN m.v_gate IS NOT NULL AND ABS(m.v_gate) < 1e30
             THEN m.v_gate END AS vgs,
        CASE WHEN m.i_gate IS NOT NULL AND ABS(m.i_gate) < 1e30
             THEN m.i_gate END AS igs
    FROM baselines_measurements m
    JOIN baselines_metadata md ON md.id = m.metadata_id
    WHERE md.irrad_campaign_id IS NOT NULL
      AND md.measurement_category = 'Irradiation'
),
ordered_points AS (
    SELECT
        rp.*,
        LAG(time_s) OVER point_order AS prev_time_s,
        LAG(vds) OVER point_order AS prev_vds,
        LAG(id_drain) OVER point_order AS prev_id_drain
    FROM raw_points rp
    WINDOW point_order AS (
        PARTITION BY source, metadata_id
        ORDER BY time_s NULLS LAST, point_index NULLS LAST
    )
),
segments AS (
    SELECT
        *,
        CASE
            WHEN time_s IS NOT NULL
             AND prev_time_s IS NOT NULL
             AND vds IS NOT NULL
             AND id_drain IS NOT NULL
             AND prev_vds IS NOT NULL
             AND prev_id_drain IS NOT NULL
            THEN GREATEST(time_s - prev_time_s, 0.0)
        END AS dt_s,
        vds * id_drain AS inst_power_w,
        ABS(vds * id_drain) AS abs_power_w,
        CASE
            WHEN time_s IS NOT NULL
             AND prev_time_s IS NOT NULL
             AND vds IS NOT NULL
             AND id_drain IS NOT NULL
             AND prev_vds IS NOT NULL
             AND prev_id_drain IS NOT NULL
            THEN (
                GREATEST(prev_vds * prev_id_drain, 0.0)
              + GREATEST(vds * id_drain, 0.0)
            ) / 2.0 * GREATEST(time_s - prev_time_s, 0.0)
        END AS energy_vds_id_segment_j,
        CASE
            WHEN time_s IS NOT NULL
             AND prev_time_s IS NOT NULL
             AND vds IS NOT NULL
             AND id_drain IS NOT NULL
             AND prev_vds IS NOT NULL
             AND prev_id_drain IS NOT NULL
            THEN (
                ABS(prev_vds * prev_id_drain)
              + ABS(vds * id_drain)
            ) / 2.0 * GREATEST(time_s - prev_time_s, 0.0)
        END AS energy_abs_segment_j
    FROM ordered_points
),
point_aggs AS (
    SELECT
        source,
        metadata_id,
        COUNT(*) AS n_points,
        COUNT(*) FILTER (
            WHERE time_s IS NOT NULL AND vds IS NOT NULL AND id_drain IS NOT NULL
        ) AS valid_power_points,
        COUNT(*) FILTER (
            WHERE time_s IS NOT NULL AND vgs IS NOT NULL AND igs IS NOT NULL
        ) AS valid_gate_points,
        MIN(time_s) AS time_start_s,
        MAX(time_s) AS time_end_s,
        MAX(ABS(id_drain)) FILTER (WHERE id_drain IS NOT NULL) AS peak_abs_id_a,
        MAX(ABS(igs)) FILTER (WHERE igs IS NOT NULL) AS peak_abs_ig_a,
        MAX(vds) FILTER (WHERE vds IS NOT NULL) AS max_vds_v,
        MIN(vds) FILTER (WHERE vds IS NOT NULL) AS min_vds_v,
        MAX(ABS(vds * id_drain)) FILTER (
            WHERE vds IS NOT NULL AND id_drain IS NOT NULL
        ) AS peak_abs_power_w,
        SUM(energy_vds_id_segment_j) FILTER (WHERE dt_s IS NOT NULL)
            AS energy_vds_id_j,
        SUM(energy_abs_segment_j) FILTER (WHERE dt_s IS NOT NULL)
            AS energy_abs_j,
        ARRAY_AGG(vds ORDER BY time_s, point_index)
            FILTER (WHERE vds IS NOT NULL) AS vds_values,
        ARRAY_AGG(id_drain ORDER BY time_s, point_index)
            FILTER (WHERE id_drain IS NOT NULL) AS id_values,
        ARRAY_AGG(igs ORDER BY time_s, point_index)
            FILTER (WHERE igs IS NOT NULL) AS ig_values
    FROM segments
    GROUP BY source, metadata_id
),
peak_points AS (
    SELECT DISTINCT ON (source, metadata_id)
        source,
        metadata_id,
        time_s AS time_peak_abs_id_s
    FROM raw_points
    WHERE time_s IS NOT NULL
      AND id_drain IS NOT NULL
    ORDER BY source, metadata_id, ABS(id_drain) DESC, time_s, point_index
),
half_power_duration AS (
    SELECT
        s.source,
        s.metadata_id,
        SUM(s.dt_s) FILTER (
            WHERE pa.peak_abs_power_w IS NOT NULL
              AND s.abs_power_w >= 0.5 * pa.peak_abs_power_w
        ) AS duration_above_half_peak_power_s
    FROM segments s
    JOIN point_aggs pa
      ON pa.source = s.source
     AND pa.metadata_id = s.metadata_id
    GROUP BY s.source, s.metadata_id
),
damage_family_counts AS (
    SELECT
        source,
        device_type,
        COUNT(*) AS family_post_iv_damage_fingerprints,
        COUNT(*) FILTER (WHERE dvth IS NOT NULL) AS family_post_iv_dvth_fingerprints,
        COUNT(*) FILTER (WHERE drds IS NOT NULL) AS family_post_iv_drds_fingerprints,
        COUNT(*) FILTER (WHERE dbv IS NOT NULL) AS family_post_iv_dbv_fingerprints
    FROM damage_equivalence_view
    GROUP BY source, device_type
),
damage_condition_counts AS (
    SELECT
        source,
        device_type,
        sc_voltage_v,
        sc_duration_us,
        irrad_run_id,
        avalanche_sample_group,
        COUNT(*) AS condition_post_iv_damage_fingerprints,
        COUNT(*) FILTER (WHERE dvth IS NOT NULL) AS condition_post_iv_dvth_fingerprints,
        COUNT(*) FILTER (WHERE drds IS NOT NULL) AS condition_post_iv_drds_fingerprints,
        COUNT(*) FILTER (WHERE dbv IS NOT NULL) AS condition_post_iv_dbv_fingerprints
    FROM damage_equivalence_view
    GROUP BY source, device_type, sc_voltage_v, sc_duration_us,
             irrad_run_id, avalanche_sample_group
),
sample_post_iv_companions AS (
    SELECT
        'sc'::text AS source,
        md.device_type,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''))
            AS physical_sample_key,
        md.sc_voltage_v,
        md.sc_duration_us,
        NULL::integer AS irrad_run_id,
        NULL::text AS avalanche_sample_group,
        COUNT(*) AS exact_post_iv_companion_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'vth_v') IS NOT NULL)
            AS exact_post_iv_dvth_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'rdson_mohm') IS NOT NULL)
            AS exact_post_iv_drds_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'bvdss_v') IS NOT NULL)
            AS exact_post_iv_dbv_files
    FROM baselines_metadata md
    WHERE md.data_source = 'sc_ruggedness'
      AND md.test_condition = 'post_sc'
      AND md.gate_params IS NOT NULL
      AND COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, '')) IS NOT NULL
    GROUP BY md.device_type, physical_sample_key,
             md.sc_voltage_v, md.sc_duration_us

    UNION ALL

    SELECT
        'avalanche'::text AS source,
        md.device_type,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''))
            AS physical_sample_key,
        NULL::double precision AS sc_voltage_v,
        NULL::double precision AS sc_duration_us,
        NULL::integer AS irrad_run_id,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''), 'unknown')
            AS avalanche_sample_group,
        COUNT(*) AS exact_post_iv_companion_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'vth_v') IS NOT NULL)
            AS exact_post_iv_dvth_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'rdson_mohm') IS NOT NULL)
            AS exact_post_iv_drds_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'bvdss_v') IS NOT NULL)
            AS exact_post_iv_dbv_files
    FROM baselines_metadata md
    WHERE md.data_source = 'curve_tracer_avalanche_iv'
      AND md.test_condition = 'post_avalanche'
      AND md.gate_params IS NOT NULL
      AND COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, '')) IS NOT NULL
    GROUP BY md.device_type, physical_sample_key, avalanche_sample_group

    UNION ALL

    SELECT
        'irrad'::text AS source,
        md.device_type,
        COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, ''))
            AS physical_sample_key,
        NULL::double precision AS sc_voltage_v,
        NULL::double precision AS sc_duration_us,
        md.irrad_run_id,
        NULL::text AS avalanche_sample_group,
        COUNT(*) AS exact_post_iv_companion_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'vth_v') IS NOT NULL)
            AS exact_post_iv_dvth_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'rdson_mohm') IS NOT NULL)
            AS exact_post_iv_drds_files,
        COUNT(*) FILTER (WHERE (md.gate_params->>'bvdss_v') IS NOT NULL)
            AS exact_post_iv_dbv_files
    FROM baselines_metadata md
    WHERE md.irrad_role = 'post_irrad'
      AND md.irrad_run_id IS NOT NULL
      AND md.gate_params IS NOT NULL
      AND COALESCE(NULLIF(md.sample_group, ''), NULLIF(md.device_id, '')) IS NOT NULL
    GROUP BY md.device_type, physical_sample_key, md.irrad_run_id
)
SELECT
    fm.source,
    'file'::text AS feature_level,
    fm.metadata_id,
    fm.experiment,
    fm.device_id,
    fm.sample_group,
    fm.physical_sample_key,
    fm.device_type,
    COALESCE(fm.device_type, NULLIF(fm.device_id, ''), 'unknown') AS device_label,
    fm.manufacturer,
    fm.filename,
    fm.csv_path,
    fm.measurement_category,
    fm.test_condition,
    fm.stress_condition_label,
    fm.sc_voltage_v,
    fm.sc_duration_us,
    fm.sc_vgs_on_v,
    fm.sc_vgs_off_v,
    fm.avalanche_family,
    fm.avalanche_mode,
    fm.avalanche_energy_j_metadata,
    fm.avalanche_peak_current_a_metadata,
    fm.avalanche_inductance_mh,
    fm.avalanche_temperature_c,
    fm.avalanche_gate_bias_v,
    fm.avalanche_outcome,
    fm.irrad_run_id,
    fm.ion_species,
    fm.beam_energy_mev,
    fm.let_surface,
    fm.let_bragg_peak,
    fm.range_um,
    fm.beam_type,
    fm.fluence_at_meas,
    COALESCE(pa.n_points, 0) AS n_points,
    COALESCE(pa.valid_power_points, 0) AS valid_power_points,
    COALESCE(pa.valid_gate_points, 0) AS valid_gate_points,
    pa.time_start_s,
    pa.time_end_s,
    CASE WHEN pa.time_start_s IS NOT NULL AND pa.time_end_s IS NOT NULL
         THEN GREATEST(pa.time_end_s - pa.time_start_s, 0.0) END AS duration_s,
    pp.time_peak_abs_id_s,
    CASE WHEN pp.time_peak_abs_id_s IS NOT NULL AND pa.time_start_s IS NOT NULL
         THEN GREATEST(pp.time_peak_abs_id_s - pa.time_start_s, 0.0) END
         AS time_to_peak_abs_id_s,
    pa.peak_abs_id_a,
    pa.peak_abs_ig_a,
    pa.max_vds_v,
    pa.min_vds_v,
    pa.peak_abs_power_w,
    COALESCE(
        fm.avalanche_energy_j_metadata,
        CASE
            WHEN fm.source = 'avalanche'
             AND fm.avalanche_inductance_mh IS NOT NULL
             AND COALESCE(fm.avalanche_peak_current_a_metadata, pa.peak_abs_id_a)
                 IS NOT NULL
            THEN 0.5
               * (fm.avalanche_inductance_mh / 1000.0)
               * POWER(COALESCE(fm.avalanche_peak_current_a_metadata,
                                pa.peak_abs_id_a), 2)
            ELSE NULL
        END
    ) AS commanded_or_stored_energy_j,
    pa.energy_vds_id_j,
    pa.energy_abs_j,
    hpd.duration_above_half_peak_power_s,
    CASE WHEN array_length(pa.vds_values, 1) > 0 THEN pa.vds_values[1] END
        AS initial_vds_v,
    CASE WHEN array_length(pa.vds_values, 1) > 0
         THEN pa.vds_values[array_length(pa.vds_values, 1)] END AS final_vds_v,
    CASE WHEN array_length(pa.id_values, 1) > 0 THEN pa.id_values[1] END
        AS initial_id_a,
    CASE WHEN array_length(pa.id_values, 1) > 0
         THEN pa.id_values[array_length(pa.id_values, 1)] END AS final_id_a,
    CASE WHEN array_length(pa.ig_values, 1) > 0 THEN pa.ig_values[1] END
        AS initial_ig_a,
    CASE WHEN array_length(pa.ig_values, 1) > 0
         THEN pa.ig_values[array_length(pa.ig_values, 1)] END AS final_ig_a,
    CASE
        WHEN pa.max_vds_v IS NOT NULL
         AND array_length(pa.vds_values, 1) > 0
        THEN GREATEST(
            pa.max_vds_v - pa.vds_values[array_length(pa.vds_values, 1)],
            0.0
        ) / GREATEST(ABS(pa.max_vds_v), 1.0)
    END AS vds_collapse_fraction,
    CASE
        WHEN pa.peak_abs_id_a IS NOT NULL
         AND pa.peak_abs_ig_a IS NOT NULL
         AND pa.peak_abs_id_a + pa.peak_abs_ig_a > 0.0
        THEN pa.peak_abs_ig_a / (pa.peak_abs_id_a + pa.peak_abs_ig_a)
    END AS gate_peak_fraction,
    COALESCE(cc.condition_post_iv_damage_fingerprints, 0) AS post_iv_damage_fingerprints,
    COALESCE(cc.condition_post_iv_dvth_fingerprints, 0) AS post_iv_dvth_fingerprints,
    COALESCE(cc.condition_post_iv_drds_fingerprints, 0) AS post_iv_drds_fingerprints,
    COALESCE(cc.condition_post_iv_dbv_fingerprints, 0) AS post_iv_dbv_fingerprints,
    (
        CASE WHEN COALESCE(cc.condition_post_iv_dvth_fingerprints, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(cc.condition_post_iv_drds_fingerprints, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(cc.condition_post_iv_dbv_fingerprints, 0) > 0 THEN 1 ELSE 0 END
    ) AS post_iv_axis_count,
    COALESCE(fc.family_post_iv_damage_fingerprints, 0) AS family_post_iv_damage_fingerprints,
    COALESCE(fc.family_post_iv_dvth_fingerprints, 0) AS family_post_iv_dvth_fingerprints,
    COALESCE(fc.family_post_iv_drds_fingerprints, 0) AS family_post_iv_drds_fingerprints,
    COALESCE(fc.family_post_iv_dbv_fingerprints, 0) AS family_post_iv_dbv_fingerprints,
    (
        CASE WHEN COALESCE(fc.family_post_iv_dvth_fingerprints, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(fc.family_post_iv_drds_fingerprints, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(fc.family_post_iv_dbv_fingerprints, 0) > 0 THEN 1 ELSE 0 END
    ) AS family_post_iv_axis_count,
    COALESCE(spc.exact_post_iv_companion_files, 0) AS exact_post_iv_companion_files,
    COALESCE(spc.exact_post_iv_dvth_files, 0) AS exact_post_iv_dvth_files,
    COALESCE(spc.exact_post_iv_drds_files, 0) AS exact_post_iv_drds_files,
    COALESCE(spc.exact_post_iv_dbv_files, 0) AS exact_post_iv_dbv_files,
    (
        CASE WHEN COALESCE(spc.exact_post_iv_dvth_files, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(spc.exact_post_iv_drds_files, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(spc.exact_post_iv_dbv_files, 0) > 0 THEN 1 ELSE 0 END
    ) AS exact_post_iv_axis_count,
    pa.energy_vds_id_j IS NOT NULL AS has_energy,
    FALSE AS has_energy_proxy_only,
    array_length(pa.vds_values, 1) > 0 AS has_collapse,
    COALESCE(pa.valid_gate_points, 0) >= 2 AS has_gate,
    pa.energy_vds_id_j IS NOT NULL
        AND array_length(pa.vds_values, 1) > 0
        AND COALESCE(pa.valid_gate_points, 0) >= 2 AS has_full_waveform,
    COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
        AS has_condition_post_iv,
    COALESCE(spc.exact_post_iv_companion_files, 0) > 0
        AS has_exact_sample_post_iv,
    COALESCE(fc.family_post_iv_damage_fingerprints, 0) > 0
        AS has_family_post_iv,
    pa.energy_vds_id_j IS NOT NULL
        AND COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
        AS has_waveform_plus_post_iv,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN fm.device_type IS NULL THEN 'missing_device_type' END,
        CASE WHEN COALESCE(pa.valid_power_points, 0) < 2
             THEN 'insufficient_power_points' END,
        CASE WHEN fm.source = 'sc'
               AND (fm.sc_voltage_v IS NULL OR fm.sc_duration_us IS NULL)
             THEN 'missing_sc_condition' END,
        CASE WHEN fm.source = 'avalanche'
               AND fm.avalanche_mode IS NULL
             THEN 'missing_avalanche_mode' END,
        CASE WHEN fm.source = 'avalanche'
               AND COALESCE(fm.avalanche_outcome, 'unknown') = 'unknown'
             THEN 'unknown_avalanche_outcome' END,
        CASE WHEN fm.source = 'irradiation'
               AND fm.irrad_run_id IS NULL
             THEN 'missing_irrad_run_id' END,
        CASE WHEN fm.source = 'irradiation'
               AND fm.ion_species IS NULL
             THEN 'missing_ion_species' END,
        CASE WHEN fm.source = 'irradiation'
               AND fm.let_surface IS NULL
             THEN 'missing_let_surface' END,
        CASE WHEN COALESCE(cc.condition_post_iv_damage_fingerprints, 0) = 0
             THEN 'no_condition_post_iv_damage_fingerprint' END,
        CASE WHEN COALESCE(spc.exact_post_iv_companion_files, 0) = 0
             THEN 'no_exact_sample_post_iv_companion' END,
        CASE WHEN COALESCE(fc.family_post_iv_damage_fingerprints, 0) = 0
             THEN 'no_device_family_post_iv_damage_fingerprint' END
    ], NULL)::text[] AS quality_flags,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN pa.energy_vds_id_j IS NOT NULL THEN 'energy_available' END,
        CASE WHEN pa.vds_values IS NOT NULL THEN 'collapse_available' END,
        CASE WHEN COALESCE(pa.valid_gate_points, 0) >= 2 THEN 'gate_available' END,
        CASE WHEN COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
             THEN 'post_iv_available' END,
        CASE WHEN COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
               AND pa.energy_vds_id_j IS NOT NULL
             THEN 'waveform_plus_post_iv' END
    ], NULL)::text[] AS available_basis_flags,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN pa.energy_vds_id_j IS NOT NULL THEN 'energy_available' END,
        CASE WHEN array_length(pa.vds_values, 1) > 0 THEN 'collapse_available' END,
        CASE WHEN COALESCE(pa.valid_gate_points, 0) >= 2 THEN 'gate_coupled' END,
        CASE WHEN pa.energy_vds_id_j IS NOT NULL
               AND array_length(pa.vds_values, 1) > 0
               AND COALESCE(pa.valid_gate_points, 0) >= 2
             THEN 'full_waveform' END,
        CASE WHEN COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
               AND pa.energy_vds_id_j IS NOT NULL
             THEN 'waveform_plus_post_iv' END
    ], NULL)::text[] AS match_basis_flags,
    ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY[
        CASE WHEN pa.energy_vds_id_j IS NOT NULL THEN 'energy_available' END,
        CASE WHEN array_length(pa.vds_values, 1) > 0 THEN 'collapse_available' END,
        CASE WHEN COALESCE(pa.valid_gate_points, 0) >= 2 THEN 'gate_coupled' END,
        CASE WHEN pa.energy_vds_id_j IS NOT NULL
               AND array_length(pa.vds_values, 1) > 0
               AND COALESCE(pa.valid_gate_points, 0) >= 2
             THEN 'full_waveform' END,
        CASE WHEN COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
               AND pa.energy_vds_id_j IS NOT NULL
             THEN 'waveform_plus_post_iv' END
    ], NULL), ', ') AS match_basis_labels,
    CASE
        WHEN pa.energy_vds_id_j IS NOT NULL
         AND COALESCE(cc.condition_post_iv_damage_fingerprints, 0) > 0
            THEN 'waveform_plus_post_iv'
        WHEN pa.energy_vds_id_j IS NOT NULL
         AND array_length(pa.vds_values, 1) > 0
         AND COALESCE(pa.valid_gate_points, 0) >= 2
            THEN 'full_waveform'
        WHEN pa.energy_vds_id_j IS NOT NULL
         AND array_length(pa.vds_values, 1) > 0
            THEN 'energy_plus_collapse'
        WHEN COALESCE(pa.valid_gate_points, 0) >= 2
            THEN 'gate_metric_only'
        WHEN pa.energy_vds_id_j IS NOT NULL
            THEN 'energy_only'
        WHEN array_length(pa.vds_values, 1) > 0
            THEN 'collapse_only'
        ELSE 'no_waveform_basis'
    END AS match_basis_class,
    CASE
        WHEN fm.device_type IS NULL THEN 'metadata_blocked'
        WHEN COALESCE(pa.valid_power_points, 0) < 2 THEN 'waveform_blocked'
        WHEN COALESCE(cc.condition_post_iv_damage_fingerprints, 0) = 0 THEN 'no_condition_post_iv_context'
        ELSE 'ready_for_descriptive_matching'
    END AS readiness_status
FROM file_metadata fm
LEFT JOIN point_aggs pa
  ON pa.source = fm.source
 AND pa.metadata_id = fm.metadata_id
LEFT JOIN peak_points pp
  ON pp.source = fm.source
 AND pp.metadata_id = fm.metadata_id
LEFT JOIN half_power_duration hpd
  ON hpd.source = fm.source
 AND hpd.metadata_id = fm.metadata_id
LEFT JOIN damage_family_counts fc
  ON fc.source = CASE
                   WHEN fm.source = 'irradiation' THEN 'irrad'
                   ELSE fm.source
                 END
 AND fc.device_type IS NOT DISTINCT FROM fm.device_type
LEFT JOIN damage_condition_counts cc
  ON cc.source = CASE
                   WHEN fm.source = 'irradiation' THEN 'irrad'
                   ELSE fm.source
                 END
 AND cc.device_type IS NOT DISTINCT FROM fm.device_type
 AND (
      (fm.source = 'sc'
       AND cc.sc_voltage_v IS NOT DISTINCT FROM fm.sc_voltage_v
       AND cc.sc_duration_us IS NOT DISTINCT FROM fm.sc_duration_us)
   OR (fm.source = 'irradiation'
       AND cc.irrad_run_id IS NOT DISTINCT FROM fm.irrad_run_id)
   OR (fm.source = 'avalanche'
       AND cc.avalanche_sample_group IS NOT DISTINCT FROM
           COALESCE(fm.physical_sample_key, 'unknown'))
 )
LEFT JOIN sample_post_iv_companions spc
  ON spc.source = CASE
                    WHEN fm.source = 'irradiation' THEN 'irrad'
                    ELSE fm.source
                  END
 AND spc.device_type IS NOT DISTINCT FROM fm.device_type
 AND fm.physical_sample_key IS NOT NULL
 AND spc.physical_sample_key IS NOT DISTINCT FROM fm.physical_sample_key
 AND (
      (fm.source = 'sc'
       AND spc.sc_voltage_v IS NOT DISTINCT FROM fm.sc_voltage_v
       AND spc.sc_duration_us IS NOT DISTINCT FROM fm.sc_duration_us)
   OR (fm.source = 'irradiation'
       AND spc.irrad_run_id IS NOT DISTINCT FROM fm.irrad_run_id)
   OR (fm.source = 'avalanche')
 );

CREATE INDEX idx_stress_waveform_file_features_source
    ON stress_waveform_file_features(source);
CREATE INDEX idx_stress_waveform_file_features_device
    ON stress_waveform_file_features(device_type);
CREATE UNIQUE INDEX idx_stress_waveform_file_features_key
    ON stress_waveform_file_features(source, metadata_id);

CREATE MATERIALIZED VIEW stress_waveform_event_features AS
WITH irrad_event_base AS (
    SELECT
        e.*,
        e.event_energy_proxy_j AS rectangular_event_energy_proxy_j
    FROM irradiation_single_event_energy_view e
)
SELECT
    f.source,
    'file_as_event'::text AS event_record_type,
    f.metadata_id,
    NULL::bigint AS event_id,
    NULL::integer AS event_index,
    CASE
        WHEN f.source = 'sc' THEN 'SC_Waveform'
        WHEN f.source = 'avalanche' THEN COALESCE(f.avalanche_mode, 'Avalanche_Waveform')
        ELSE 'Waveform'
    END AS event_type,
    CASE
        WHEN f.source = 'sc' THEN 'electrical_short_circuit'
        WHEN f.source = 'avalanche' THEN 'inductive_avalanche'
        ELSE 'waveform_file'
    END AS path_type,
    NULL::boolean AS is_catastrophic,
    NULL::double precision AS confidence,
    f.experiment,
    f.device_id,
    f.sample_group,
    f.physical_sample_key,
    f.device_type,
    f.device_label,
    f.manufacturer,
    f.filename,
    f.stress_condition_label,
    f.sc_voltage_v,
    f.sc_duration_us,
    f.avalanche_family,
    f.avalanche_mode,
    f.avalanche_outcome,
    f.irrad_run_id,
    f.ion_species,
    f.beam_energy_mev,
    f.let_surface,
    f.fluence_at_meas,
    f.time_start_s,
    f.time_peak_abs_id_s AS time_peak_s,
    f.time_end_s,
    f.duration_s AS event_duration_s,
    f.energy_vds_id_j AS event_energy_vds_id_j,
    f.energy_vds_id_j AS event_electrical_terminal_energy_j,
    f.energy_abs_j AS event_energy_abs_j,
    NULL::double precision AS event_energy_proxy_j,
    f.energy_vds_id_j AS file_energy_vds_id_j,
    f.commanded_or_stored_energy_j,
    (f.energy_vds_id_j IS NOT NULL OR f.commanded_or_stored_energy_j IS NOT NULL)
        AS energy_is_comparable,
    'full_file_waveform'::text AS energy_window_basis,
    'none'::text AS energy_censored_reason,
    1.0::double precision AS active_window_confidence,
    'file'::text AS energy_level,
    f.peak_abs_id_a,
    f.peak_abs_ig_a,
    f.peak_abs_power_w,
    CASE
        WHEN f.source = 'sc' THEN COALESCE(
            CASE
                WHEN f.max_vds_v IS NOT NULL OR f.min_vds_v IS NOT NULL
                THEN GREATEST(
                    COALESCE(ABS(f.max_vds_v), 0.0),
                    COALESCE(ABS(f.min_vds_v), 0.0)
                )
            END,
            ABS(f.sc_voltage_v)
        )
        WHEN f.source = 'avalanche'
         AND (f.max_vds_v IS NOT NULL OR f.min_vds_v IS NOT NULL)
        THEN GREATEST(
            COALESCE(ABS(f.max_vds_v), 0.0),
            COALESCE(ABS(f.min_vds_v), 0.0)
        )
    END AS stress_observed_abs_vds_v,
    f.initial_vds_v AS vds_before_v,
    f.final_vds_v AS vds_after_v,
    f.final_vds_v - f.initial_vds_v AS vds_delta_v,
    f.initial_id_a AS id_before_a,
    f.final_id_a AS id_after_a,
    ABS(f.final_id_a - f.initial_id_a) AS delta_id_abs_a,
    f.initial_ig_a AS ig_before_a,
    f.final_ig_a AS ig_after_a,
    ABS(f.final_ig_a - f.initial_ig_a) AS delta_ig_abs_a,
    f.vds_collapse_fraction,
    f.gate_peak_fraction AS gate_delta_fraction,
    f.post_iv_damage_fingerprints,
    f.post_iv_axis_count,
    f.family_post_iv_damage_fingerprints,
    f.family_post_iv_axis_count,
    f.exact_post_iv_companion_files,
    f.exact_post_iv_axis_count,
    f.has_energy,
    f.has_energy_proxy_only,
    f.has_collapse,
    f.has_gate,
    f.has_full_waveform,
    f.has_condition_post_iv,
    f.has_exact_sample_post_iv,
    f.has_family_post_iv,
    f.has_waveform_plus_post_iv,
    f.available_basis_flags,
    f.match_basis_flags,
    f.match_basis_labels,
    f.match_basis_class,
    f.quality_flags,
    f.readiness_status
FROM stress_waveform_file_features f
WHERE f.source IN ('sc', 'avalanche')

UNION ALL

SELECT
    'irradiation'::text AS source,
    'detected_single_event'::text AS event_record_type,
    e.metadata_id,
    e.event_id,
    e.event_index,
    e.event_type,
    e.path_type,
    e.is_catastrophic,
    e.confidence,
    e.experiment,
    e.device_id,
    f.sample_group,
    f.physical_sample_key,
    e.device_type,
    COALESCE(e.device_type, NULLIF(e.device_id, ''), 'unknown') AS device_label,
    e.manufacturer,
    e.filename,
    COALESCE(e.ion_species, '?') || ' '
        || COALESCE(e.beam_energy_mev::text, '?') || ' MeV '
        || COALESCE(e.beam_type, '') AS stress_condition_label,
    NULL::double precision AS sc_voltage_v,
    NULL::double precision AS sc_duration_us,
    NULL::text AS avalanche_family,
    NULL::text AS avalanche_mode,
    NULL::text AS avalanche_outcome,
    f.irrad_run_id,
    e.ion_species,
    e.beam_energy_mev,
    e.let_mev_cm2_mg AS let_surface,
    f.fluence_at_meas,
    e.time_start AS time_start_s,
    e.time_peak AS time_peak_s,
    e.time_end AS time_end_s,
    e.event_duration_s,
    e.event_energy_vds_id_j,
    e.event_energy_vds_id_j AS event_electrical_terminal_energy_j,
    e.event_energy_abs_j,
    e.rectangular_event_energy_proxy_j AS event_energy_proxy_j,
    NULL::double precision AS file_energy_vds_id_j,
    NULL::double precision AS commanded_or_stored_energy_j,
    COALESCE(e.energy_is_comparable, FALSE) AS energy_is_comparable,
    COALESCE(e.active_window_basis, 'not_analyzed') AS energy_window_basis,
    COALESCE(e.energy_censored_reason, 'not_analyzed') AS energy_censored_reason,
    e.active_window_confidence,
    COALESCE(e.energy_level, 'unknown') AS energy_level,
    GREATEST(ABS(e.id_before_a), ABS(e.id_after_a)) AS peak_abs_id_a,
    GREATEST(ABS(e.ig_before_a), ABS(e.ig_after_a)) AS peak_abs_ig_a,
    CASE
        WHEN e.vds_before_v IS NOT NULL
         AND e.delta_id_abs_a IS NOT NULL
        THEN ABS(e.vds_before_v) * e.delta_id_abs_a
    END AS peak_abs_power_w,
    CASE
        WHEN e.vds_before_v IS NOT NULL OR e.vds_after_v IS NOT NULL
        THEN GREATEST(
            COALESCE(ABS(e.vds_before_v), 0.0),
            COALESCE(ABS(e.vds_after_v), 0.0)
        )
    END AS stress_observed_abs_vds_v,
    e.vds_before_v,
    e.vds_after_v,
    e.vds_delta_v,
    e.id_before_a,
    e.id_after_a,
    e.delta_id_abs_a,
    e.ig_before_a,
    e.ig_after_a,
    e.delta_ig_abs_a,
    CASE
        WHEN e.vds_before_v IS NOT NULL AND ABS(e.vds_before_v) > 0.0
        THEN GREATEST(-e.vds_delta_v, 0.0) / ABS(e.vds_before_v)
    END AS vds_collapse_fraction,
    e.gate_delta_fraction,
    COALESCE(f.post_iv_damage_fingerprints, 0) AS post_iv_damage_fingerprints,
    COALESCE(f.post_iv_axis_count, 0) AS post_iv_axis_count,
    COALESCE(f.family_post_iv_damage_fingerprints, 0)
        AS family_post_iv_damage_fingerprints,
    COALESCE(f.family_post_iv_axis_count, 0) AS family_post_iv_axis_count,
    COALESCE(f.exact_post_iv_companion_files, 0) AS exact_post_iv_companion_files,
    COALESCE(f.exact_post_iv_axis_count, 0) AS exact_post_iv_axis_count,
    e.event_energy_vds_id_j IS NOT NULL
        AND COALESCE(e.energy_is_comparable, FALSE) AS has_energy,
    e.event_energy_vds_id_j IS NULL
        AND e.rectangular_event_energy_proxy_j IS NOT NULL AS has_energy_proxy_only,
    e.vds_delta_v IS NOT NULL AS has_collapse,
    e.gate_delta_fraction IS NOT NULL AS has_gate,
    e.event_energy_vds_id_j IS NOT NULL
        AND COALESCE(e.energy_is_comparable, FALSE)
        AND e.vds_delta_v IS NOT NULL
        AND e.gate_delta_fraction IS NOT NULL AS has_full_waveform,
    COALESCE(f.has_condition_post_iv, FALSE) AS has_condition_post_iv,
    COALESCE(f.has_exact_sample_post_iv, FALSE) AS has_exact_sample_post_iv,
    COALESCE(f.has_family_post_iv, FALSE) AS has_family_post_iv,
    e.event_energy_vds_id_j IS NOT NULL
        AND COALESCE(e.energy_is_comparable, FALSE)
        AND COALESCE(f.has_condition_post_iv, FALSE)
        AS has_waveform_plus_post_iv,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
             THEN 'energy_available' END,
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND NOT COALESCE(e.energy_is_comparable, FALSE)
             THEN 'event_energy_not_comparable' END,
        CASE WHEN e.event_energy_vds_id_j IS NULL
               AND e.rectangular_event_energy_proxy_j IS NOT NULL
             THEN 'event_energy_proxy_only' END,
        CASE WHEN COALESCE(e.energy_censored_reason, 'none') <> 'none'
             THEN 'energy_censored_' || e.energy_censored_reason END,
        CASE WHEN e.vds_delta_v IS NOT NULL THEN 'collapse_available' END,
        CASE WHEN e.gate_delta_fraction IS NOT NULL THEN 'gate_available' END,
        CASE WHEN COALESCE(f.has_condition_post_iv, FALSE)
             THEN 'post_iv_available' END,
        CASE WHEN COALESCE(f.has_exact_sample_post_iv, FALSE)
             THEN 'exact_post_iv_companion_available' END,
        CASE WHEN COALESCE(f.has_condition_post_iv, FALSE)
               AND e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
             THEN 'waveform_plus_post_iv' END
    ], NULL)::text[] AS available_basis_flags,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
             THEN 'energy_available' END,
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND NOT COALESCE(e.energy_is_comparable, FALSE)
             THEN 'event_energy_not_comparable' END,
        CASE WHEN e.event_energy_vds_id_j IS NULL
               AND e.rectangular_event_energy_proxy_j IS NOT NULL
             THEN 'event_energy_proxy_only' END,
        CASE WHEN e.vds_delta_v IS NOT NULL THEN 'collapse_available' END,
        CASE WHEN e.gate_delta_fraction IS NOT NULL THEN 'gate_coupled' END,
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
               AND e.vds_delta_v IS NOT NULL
               AND e.gate_delta_fraction IS NOT NULL
             THEN 'full_waveform' END,
        CASE WHEN COALESCE(f.has_condition_post_iv, FALSE)
               AND e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
             THEN 'waveform_plus_post_iv' END
    ], NULL)::text[] AS match_basis_flags,
    ARRAY_TO_STRING(ARRAY_REMOVE(ARRAY[
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
             THEN 'energy_available' END,
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND NOT COALESCE(e.energy_is_comparable, FALSE)
             THEN 'event_energy_not_comparable' END,
        CASE WHEN e.event_energy_vds_id_j IS NULL
               AND e.rectangular_event_energy_proxy_j IS NOT NULL
             THEN 'event_energy_proxy_only' END,
        CASE WHEN e.vds_delta_v IS NOT NULL THEN 'collapse_available' END,
        CASE WHEN e.gate_delta_fraction IS NOT NULL THEN 'gate_coupled' END,
        CASE WHEN e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
               AND e.vds_delta_v IS NOT NULL
               AND e.gate_delta_fraction IS NOT NULL
             THEN 'full_waveform' END,
        CASE WHEN COALESCE(f.has_condition_post_iv, FALSE)
               AND e.event_energy_vds_id_j IS NOT NULL
               AND COALESCE(e.energy_is_comparable, FALSE)
             THEN 'waveform_plus_post_iv' END
    ], NULL), ', ') AS match_basis_labels,
    CASE
        WHEN e.event_energy_vds_id_j IS NOT NULL
         AND COALESCE(e.energy_is_comparable, FALSE)
         AND COALESCE(f.has_condition_post_iv, FALSE)
            THEN 'waveform_plus_post_iv'
        WHEN e.event_energy_vds_id_j IS NOT NULL
         AND COALESCE(e.energy_is_comparable, FALSE)
         AND e.vds_delta_v IS NOT NULL
         AND e.gate_delta_fraction IS NOT NULL
            THEN 'full_waveform'
        WHEN e.event_energy_vds_id_j IS NOT NULL
         AND COALESCE(e.energy_is_comparable, FALSE)
         AND e.vds_delta_v IS NOT NULL
            THEN 'energy_plus_collapse'
        WHEN e.event_energy_vds_id_j IS NOT NULL
         AND NOT COALESCE(e.energy_is_comparable, FALSE)
            THEN 'event_energy_not_comparable'
        WHEN e.event_energy_vds_id_j IS NULL
         AND e.rectangular_event_energy_proxy_j IS NOT NULL
            THEN 'event_energy_proxy_only'
        WHEN e.gate_delta_fraction IS NOT NULL
            THEN 'gate_metric_only'
        WHEN e.vds_delta_v IS NOT NULL
            THEN 'collapse_only'
        ELSE 'no_waveform_basis'
    END AS match_basis_class,
    ARRAY_REMOVE(COALESCE(f.quality_flags, ARRAY[]::text[]) || ARRAY[
        CASE WHEN e.event_type IS NULL THEN 'missing_event_type' END,
        CASE WHEN NOT COALESCE(e.energy_is_comparable, FALSE)
             THEN 'event_energy_not_comparable' END,
        CASE WHEN COALESCE(e.energy_level, 'unknown') <> 'event'
             THEN 'event_energy_not_event_level' END,
        CASE WHEN COALESCE(e.energy_censored_reason, 'none') <> 'none'
             THEN 'event_energy_censored_' || e.energy_censored_reason END,
        CASE WHEN e.active_window_confidence IS NOT NULL
               AND e.active_window_confidence < 0.80
             THEN 'low_active_window_confidence' END,
        CASE WHEN e.event_energy_vds_id_j IS NULL
               AND e.rectangular_event_energy_proxy_j IS NOT NULL
             THEN 'event_energy_not_integrated_proxy_only' END,
        CASE WHEN e.event_energy_vds_id_j IS NULL
               AND e.rectangular_event_energy_proxy_j IS NULL
             THEN 'missing_event_energy' END
    ]::text[], NULL)::text[] AS quality_flags,
    CASE
        WHEN e.device_type IS NULL THEN 'metadata_blocked'
        WHEN e.event_type IS NULL THEN 'event_label_blocked'
        WHEN NOT COALESCE(e.energy_is_comparable, FALSE)
          THEN 'energy_not_comparable'
        WHEN COALESCE(f.has_condition_post_iv, FALSE) = FALSE
          THEN 'no_condition_post_iv_context'
        ELSE 'ready_for_descriptive_matching'
    END AS readiness_status
FROM irrad_event_base e
LEFT JOIN stress_waveform_file_features f
  ON f.source = 'irradiation'
 AND f.metadata_id = e.metadata_id;

CREATE INDEX idx_stress_waveform_event_features_source
    ON stress_waveform_event_features(source);
CREATE INDEX idx_stress_waveform_event_features_device
    ON stress_waveform_event_features(device_type);
CREATE INDEX idx_stress_waveform_event_features_type
    ON stress_waveform_event_features(event_type);
CREATE INDEX idx_stress_waveform_event_features_readiness
    ON stress_waveform_event_features(readiness_status);

CREATE MATERIALIZED VIEW stress_waveform_basis_feature_view AS
SELECT
    f.feature_level,
    f.source,
    f.metadata_id,
    NULL::bigint AS event_id,
    NULL::integer AS event_index,
    f.device_type,
    f.physical_sample_key,
    f.readiness_status,
    f.match_basis_class,
    basis_flag
FROM stress_waveform_file_features f
CROSS JOIN LATERAL UNNEST(f.match_basis_flags) AS basis_flag

UNION ALL

SELECT
    'event'::text AS feature_level,
    e.source,
    e.metadata_id,
    e.event_id,
    e.event_index,
    e.device_type,
    e.physical_sample_key,
    e.readiness_status,
    e.match_basis_class,
    basis_flag
FROM stress_waveform_event_features e
CROSS JOIN LATERAL UNNEST(e.match_basis_flags) AS basis_flag;

CREATE INDEX idx_stress_waveform_basis_feature_source
    ON stress_waveform_basis_feature_view(source);
CREATE INDEX idx_stress_waveform_basis_feature_device
    ON stress_waveform_basis_feature_view(device_type);
CREATE INDEX idx_stress_waveform_basis_feature_flag
    ON stress_waveform_basis_feature_view(basis_flag);

CREATE MATERIALIZED VIEW stress_proxy_readiness_view AS
WITH device_keys AS (
    SELECT DISTINCT device_type
    FROM stress_waveform_file_features
    UNION
    SELECT DISTINCT device_type
    FROM damage_equivalence_view
),
file_counts AS (
    SELECT
        device_type,
        COUNT(*) FILTER (WHERE source = 'sc') AS sc_waveform_files,
        COUNT(*) FILTER (WHERE source = 'avalanche') AS avalanche_waveform_files,
        COUNT(*) FILTER (
            WHERE source = 'avalanche' AND UPPER(avalanche_mode) IN ('UID', 'UIS')
        ) AS uid_uis_waveform_files,
        COUNT(*) FILTER (WHERE source = 'irradiation') AS irradiation_waveform_files,
        COUNT(*) FILTER (WHERE device_type IS NULL) AS missing_device_type_files,
        COUNT(*) FILTER (WHERE readiness_status = 'ready_for_descriptive_matching')
            AS ready_waveform_files,
        COUNT(*) FILTER (WHERE has_condition_post_iv)
            AS waveform_files_with_condition_post_iv,
        COUNT(*) FILTER (WHERE has_exact_sample_post_iv)
            AS waveform_files_with_exact_post_iv,
        COUNT(*) FILTER (WHERE has_waveform_plus_post_iv)
            AS waveform_files_with_waveform_plus_post_iv,
        COUNT(*) FILTER (
            WHERE has_waveform_plus_post_iv
              AND (source = 'sc'
                   OR (source = 'avalanche'
                       AND UPPER(avalanche_mode) IN ('UID', 'UIS')))
        ) AS electrical_proxy_waveform_plus_post_iv_files,
        COUNT(*) FILTER (
            WHERE has_waveform_plus_post_iv
              AND source = 'irradiation'
        ) AS irradiation_waveform_plus_post_iv_files
    FROM stress_waveform_file_features
    GROUP BY device_type
),
event_counts AS (
    SELECT
        device_type,
        COUNT(*) AS waveform_events,
        COUNT(*) FILTER (WHERE source = 'irradiation') AS irradiation_events,
        COUNT(*) FILTER (WHERE event_type = 'SEB') AS seb_events,
        COUNT(*) FILTER (WHERE event_type = 'SELCI') AS selc_i_events,
        COUNT(*) FILTER (WHERE event_type = 'SELCII') AS selc_ii_events,
        COUNT(*) FILTER (WHERE is_catastrophic) AS catastrophic_events,
        COUNT(*) FILTER (WHERE readiness_status = 'ready_for_descriptive_matching')
            AS ready_events,
        COUNT(*) FILTER (WHERE has_condition_post_iv)
            AS events_with_condition_post_iv,
        COUNT(*) FILTER (WHERE has_exact_sample_post_iv)
            AS events_with_exact_post_iv,
        COUNT(*) FILTER (WHERE has_waveform_plus_post_iv)
            AS events_with_waveform_plus_post_iv,
        COUNT(*) FILTER (
            WHERE has_waveform_plus_post_iv
              AND source = 'irradiation'
        ) AS irradiation_events_with_waveform_plus_post_iv,
        COUNT(*) FILTER (WHERE has_energy_proxy_only)
            AS events_with_energy_proxy_only
    FROM stress_waveform_event_features
    GROUP BY device_type
),
damage_counts AS (
    SELECT
        device_type,
        COUNT(*) AS post_iv_damage_fingerprints,
        COUNT(*) FILTER (WHERE source = 'sc') AS sc_damage_fingerprints,
        COUNT(*) FILTER (WHERE source = 'avalanche') AS avalanche_damage_fingerprints,
        COUNT(*) FILTER (WHERE source = 'irrad') AS irradiation_damage_fingerprints,
        COUNT(*) FILTER (WHERE dvth IS NOT NULL) AS dvth_fingerprints,
        COUNT(*) FILTER (WHERE drds IS NOT NULL) AS drds_fingerprints,
        COUNT(*) FILTER (WHERE dbv IS NOT NULL) AS dbv_fingerprints
    FROM damage_equivalence_view
    GROUP BY device_type
)
SELECT
    dk.device_type,
    COALESCE(dk.device_type, '<null>') AS device_type_label,
    COALESCE(fc.sc_waveform_files, 0) AS sc_waveform_files,
    COALESCE(fc.avalanche_waveform_files, 0) AS avalanche_waveform_files,
    COALESCE(fc.uid_uis_waveform_files, 0) AS uid_uis_waveform_files,
    COALESCE(fc.irradiation_waveform_files, 0) AS irradiation_waveform_files,
    COALESCE(ec.waveform_events, 0) AS waveform_events,
    COALESCE(ec.irradiation_events, 0) AS irradiation_events,
    COALESCE(ec.seb_events, 0) AS seb_events,
    COALESCE(ec.selc_i_events, 0) AS selc_i_events,
    COALESCE(ec.selc_ii_events, 0) AS selc_ii_events,
    COALESCE(ec.catastrophic_events, 0) AS catastrophic_events,
    COALESCE(dc.post_iv_damage_fingerprints, 0) AS post_iv_damage_fingerprints,
    COALESCE(dc.sc_damage_fingerprints, 0) AS sc_damage_fingerprints,
    COALESCE(dc.avalanche_damage_fingerprints, 0) AS avalanche_damage_fingerprints,
    COALESCE(dc.irradiation_damage_fingerprints, 0) AS irradiation_damage_fingerprints,
    COALESCE(dc.dvth_fingerprints, 0) AS dvth_fingerprints,
    COALESCE(dc.drds_fingerprints, 0) AS drds_fingerprints,
    COALESCE(dc.dbv_fingerprints, 0) AS dbv_fingerprints,
    (
        CASE WHEN COALESCE(dc.dvth_fingerprints, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(dc.drds_fingerprints, 0) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COALESCE(dc.dbv_fingerprints, 0) > 0 THEN 1 ELSE 0 END
    ) AS comparable_damage_axis_count,
    COALESCE(fc.missing_device_type_files, 0) AS missing_device_type_files,
    COALESCE(fc.ready_waveform_files, 0) AS ready_waveform_files,
    COALESCE(fc.waveform_files_with_condition_post_iv, 0)
        AS waveform_files_with_condition_post_iv,
    COALESCE(fc.waveform_files_with_exact_post_iv, 0)
        AS waveform_files_with_exact_post_iv,
    COALESCE(fc.waveform_files_with_waveform_plus_post_iv, 0)
        AS waveform_files_with_waveform_plus_post_iv,
    COALESCE(fc.electrical_proxy_waveform_plus_post_iv_files, 0)
        AS electrical_proxy_waveform_plus_post_iv_files,
    COALESCE(fc.irradiation_waveform_plus_post_iv_files, 0)
        AS irradiation_waveform_plus_post_iv_files,
    COALESCE(ec.ready_events, 0) AS ready_events,
    COALESCE(ec.events_with_condition_post_iv, 0) AS events_with_condition_post_iv,
    COALESCE(ec.events_with_exact_post_iv, 0) AS events_with_exact_post_iv,
    COALESCE(ec.events_with_waveform_plus_post_iv, 0)
        AS events_with_waveform_plus_post_iv,
    COALESCE(ec.irradiation_events_with_waveform_plus_post_iv, 0)
        AS irradiation_events_with_waveform_plus_post_iv,
    COALESCE(ec.events_with_energy_proxy_only, 0) AS events_with_energy_proxy_only,
    (
        COALESCE(fc.sc_waveform_files, 0)
      + COALESCE(fc.uid_uis_waveform_files, 0)
    ) AS electrical_proxy_waveform_files,
    (
        dk.device_type IS NOT NULL
        AND COALESCE(fc.electrical_proxy_waveform_plus_post_iv_files, 0) > 0
        AND (
            COALESCE(fc.irradiation_waveform_plus_post_iv_files, 0)
          + COALESCE(ec.irradiation_events_with_waveform_plus_post_iv, 0)
        ) > 0
    ) AS gate_zero_candidate,
    CASE
        WHEN dk.device_type IS NULL THEN 'missing_device_type'
        WHEN (
            COALESCE(fc.sc_waveform_files, 0)
          + COALESCE(fc.uid_uis_waveform_files, 0)
        ) = 0 THEN 'missing_sc_or_uid_uis_waveforms'
        WHEN (
            COALESCE(fc.irradiation_waveform_files, 0)
          + COALESCE(ec.irradiation_events, 0)
        ) = 0 THEN 'missing_irradiation_waveforms_or_events'
        WHEN COALESCE(fc.electrical_proxy_waveform_plus_post_iv_files, 0) = 0
          THEN 'missing_electrical_proxy_post_iv_overlap'
        WHEN (
            COALESCE(fc.irradiation_waveform_plus_post_iv_files, 0)
          + COALESCE(ec.irradiation_events_with_waveform_plus_post_iv, 0)
        ) = 0 THEN 'missing_irradiation_post_iv_overlap'
        ELSE 'gate_zero_candidate'
    END AS proxy_readiness_status
FROM device_keys dk
LEFT JOIN file_counts fc ON fc.device_type IS NOT DISTINCT FROM dk.device_type
LEFT JOIN event_counts ec ON ec.device_type IS NOT DISTINCT FROM dk.device_type
LEFT JOIN damage_counts dc ON dc.device_type IS NOT DISTINCT FROM dk.device_type;

CREATE INDEX idx_stress_proxy_readiness_device
    ON stress_proxy_readiness_view(device_type);
CREATE INDEX idx_stress_proxy_readiness_status
    ON stress_proxy_readiness_view(proxy_readiness_status);

CREATE MATERIALIZED VIEW stress_proxy_gate_zero_view AS
WITH summary AS (
    SELECT
        COUNT(*) FILTER (WHERE gate_zero_candidate) AS candidate_device_families,
        COUNT(*) FILTER (WHERE sc_waveform_files > 0) AS device_families_with_sc_waveforms,
        COUNT(*) FILTER (WHERE uid_uis_waveform_files > 0) AS device_families_with_uid_uis_waveforms,
        COUNT(*) FILTER (
            WHERE irradiation_waveform_files > 0 OR irradiation_events > 0
        ) AS device_families_with_irradiation_waveforms_or_events,
        COUNT(*) FILTER (WHERE post_iv_damage_fingerprints > 0)
            AS device_families_with_post_iv_damage,
        COUNT(*) FILTER (WHERE electrical_proxy_waveform_plus_post_iv_files > 0)
            AS device_families_with_electrical_proxy_post_iv_overlap,
        COUNT(*) FILTER (
            WHERE irradiation_waveform_plus_post_iv_files > 0
               OR irradiation_events_with_waveform_plus_post_iv > 0
        ) AS device_families_with_irradiation_post_iv_overlap,
        SUM(sc_waveform_files) AS sc_waveform_files,
        SUM(uid_uis_waveform_files) AS uid_uis_waveform_files,
        SUM(irradiation_waveform_files) AS irradiation_waveform_files,
        SUM(irradiation_events) AS irradiation_events,
        SUM(post_iv_damage_fingerprints) AS post_iv_damage_fingerprints,
        SUM(electrical_proxy_waveform_plus_post_iv_files)
            AS electrical_proxy_waveform_plus_post_iv_files,
        SUM(irradiation_waveform_plus_post_iv_files)
            AS irradiation_waveform_plus_post_iv_files,
        SUM(irradiation_events_with_waveform_plus_post_iv)
            AS irradiation_events_with_waveform_plus_post_iv,
        STRING_AGG(device_type, ', ' ORDER BY device_type)
            FILTER (WHERE gate_zero_candidate) AS candidate_device_types
    FROM stress_proxy_readiness_view
    WHERE device_type IS NOT NULL
)
SELECT
    candidate_device_families,
    candidate_device_families >= 3 AS gate_zero_pass,
    CASE
        WHEN candidate_device_families >= 3
          THEN 'gate_zero_pass'
        ELSE 'gate_zero_fail_current_state'
    END AS gate_zero_status,
    device_families_with_sc_waveforms,
    device_families_with_uid_uis_waveforms,
    device_families_with_irradiation_waveforms_or_events,
    device_families_with_post_iv_damage,
    device_families_with_electrical_proxy_post_iv_overlap,
    device_families_with_irradiation_post_iv_overlap,
    sc_waveform_files,
    uid_uis_waveform_files,
    irradiation_waveform_files,
    irradiation_events,
    post_iv_damage_fingerprints,
    electrical_proxy_waveform_plus_post_iv_files,
    irradiation_waveform_plus_post_iv_files,
    irradiation_events_with_waveform_plus_post_iv,
    candidate_device_types
FROM summary;

CREATE MATERIALIZED VIEW stress_test_context_view AS
WITH radiation_rollup AS (
    SELECT *
    FROM radiation_stress_dose_summary_view
    WHERE dose_scope IN ('event_window', 'single_particle')
      AND (
            dose_scope <> 'event_window'
         OR radiation_deposited_energy_j IS NOT NULL
         OR radiation_dose_gy IS NOT NULL
         OR radiation_deposited_energy_total_j IS NOT NULL
         OR radiation_dose_total_gy IS NOT NULL
      )
),
event_base AS (
    SELECT
        e.*,
        CASE
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
            THEN CASE
                WHEN COALESCE(e.energy_is_comparable, FALSE)
                 AND e.energy_level = 'event'
                 AND e.event_energy_vds_id_j IS NOT NULL
                THEN e.event_energy_vds_id_j
            END
            ELSE COALESCE(
                e.event_energy_vds_id_j,
                e.event_energy_proxy_j,
                e.file_energy_vds_id_j,
                e.commanded_or_stored_energy_j
            )
        END AS electrical_terminal_energy_j,
        CASE
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
             AND COALESCE(e.energy_is_comparable, FALSE)
             AND e.energy_level = 'event'
             AND e.event_energy_vds_id_j IS NOT NULL
                THEN 'integrated_event_vds_id'
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
             AND e.event_energy_vds_id_j IS NOT NULL
                THEN 'non_comparable_integrated_event'
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
             AND e.event_energy_proxy_j IS NOT NULL
                THEN 'proxy_event_rectangular_excluded'
            WHEN e.event_energy_vds_id_j IS NOT NULL THEN 'integrated_event_vds_id'
            WHEN e.event_energy_proxy_j IS NOT NULL THEN 'proxy_event_rectangular'
            WHEN e.file_energy_vds_id_j IS NOT NULL THEN 'integrated_file_vds_id'
            WHEN e.commanded_or_stored_energy_j IS NOT NULL THEN 'commanded_or_stored'
            ELSE 'missing'
        END AS electrical_terminal_energy_basis,
        CASE
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
            THEN CASE
                WHEN COALESCE(e.energy_is_comparable, FALSE)
                 AND e.energy_level = 'event'
                 AND e.event_energy_vds_id_j IS NOT NULL
                THEN e.event_energy_vds_id_j
            END
            ELSE COALESCE(
                e.event_energy_vds_id_j,
                e.event_energy_proxy_j,
                e.file_energy_vds_id_j,
                e.commanded_or_stored_energy_j
            )
        END AS stress_energy_j,
        CASE
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
             AND COALESCE(e.energy_is_comparable, FALSE)
             AND e.energy_level = 'event'
             AND e.event_energy_vds_id_j IS NOT NULL
                THEN 'integrated_event_vds_id'
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
             AND e.event_energy_vds_id_j IS NOT NULL
                THEN 'non_comparable_integrated_event'
            WHEN e.source = 'irradiation'
             AND e.event_record_type = 'detected_single_event'
             AND e.event_energy_proxy_j IS NOT NULL
                THEN 'proxy_event_rectangular_excluded'
            WHEN e.event_energy_vds_id_j IS NOT NULL THEN 'integrated_event_vds_id'
            WHEN e.event_energy_proxy_j IS NOT NULL THEN 'proxy_event_rectangular'
            WHEN e.file_energy_vds_id_j IS NOT NULL THEN 'integrated_file_vds_id'
            WHEN e.commanded_or_stored_energy_j IS NOT NULL THEN 'commanded_or_stored'
            ELSE 'missing'
        END AS stress_energy_basis,
        COALESCE(rad_event.dose_scope, rad_single.dose_scope)
            AS radiation_dose_scope,
        COALESCE(rad_event.fluence_basis, rad_single.fluence_basis)
            AS radiation_fluence_basis,
        COALESCE(rad_event.radiation_energy_basis,
                 rad_single.radiation_energy_basis)
            AS radiation_energy_basis,
        COALESCE(rad_event.radiation_deposited_energy_j,
                 rad_single.radiation_deposited_energy_j)
            AS radiation_deposited_energy_j,
        COALESCE(rad_event.radiation_deposited_energy_electronic_j,
                 rad_single.radiation_deposited_energy_electronic_j)
            AS radiation_deposited_energy_electronic_j,
        COALESCE(rad_event.radiation_deposited_energy_nuclear_j,
                 rad_single.radiation_deposited_energy_nuclear_j)
            AS radiation_deposited_energy_nuclear_j,
        COALESCE(rad_event.radiation_deposited_energy_total_j,
                 rad_single.radiation_deposited_energy_total_j)
            AS radiation_deposited_energy_total_j,
        COALESCE(rad_event.radiation_dose_electronic_gy,
                 rad_single.radiation_dose_electronic_gy)
            AS radiation_dose_electronic_gy,
        COALESCE(rad_event.radiation_dose_nuclear_gy,
                 rad_single.radiation_dose_nuclear_gy)
            AS radiation_dose_nuclear_gy,
        COALESCE(rad_event.radiation_dose_total_gy,
                 rad_single.radiation_dose_total_gy)
            AS radiation_dose_total_gy,
        COALESCE(rad_event.radiation_dose_gy, rad_single.radiation_dose_gy)
            AS radiation_dose_gy,
        COALESCE(rad_event.radiation_total_dose_gy,
                 rad_single.radiation_total_dose_gy)
            AS radiation_total_dose_gy,
        COALESCE(rad_event.layer_count, rad_single.layer_count)
            AS radiation_layer_count,
        COALESCE(rad_event.calculated_layer_count,
                 rad_single.calculated_layer_count)
            AS radiation_calculated_layer_count,
        COALESCE(rad_event.modeled_mass_kg, rad_single.modeled_mass_kg)
            AS radiation_modeled_mass_kg,
        COALESCE(rad_event.min_energy_in_mev, rad_single.min_energy_in_mev)
            AS radiation_min_energy_in_mev,
        COALESCE(rad_event.min_energy_out_mev, rad_single.min_energy_out_mev)
            AS radiation_min_energy_out_mev,
        COALESCE(rad_event.stopped_in_any_layer,
                 rad_single.stopped_in_any_layer)
            AS radiation_stopped_in_any_layer,
        COALESCE(rad_event.min_range_margin_um,
                 rad_single.min_range_margin_um)
            AS radiation_min_range_margin_um,
        CASE
            WHEN e.event_duration_s IS NOT NULL AND e.event_duration_s > 0.0
                THEN e.event_duration_s
            WHEN e.sc_duration_us IS NOT NULL AND e.sc_duration_us > 0.0
                THEN e.sc_duration_us / 1000000.0
        END AS stress_duration_s
    FROM stress_waveform_event_features e
    LEFT JOIN radiation_rollup rad_event
      ON rad_event.dose_scope = 'event_window'
     AND rad_event.event_id IS NOT DISTINCT FROM e.event_id
     AND rad_event.metadata_id IS NOT DISTINCT FROM e.metadata_id
    LEFT JOIN radiation_rollup rad_single
      ON rad_single.dose_scope = 'single_particle'
     AND rad_single.event_id IS NOT DISTINCT FROM e.event_id
     AND rad_single.metadata_id IS NOT DISTINCT FROM e.metadata_id
),
rated AS (
    SELECT
        eb.*,
        dl.device_category,
        CASE
            WHEN dl.voltage_rating ~ '[0-9]' THEN
                SUBSTRING(dl.voltage_rating FROM '[0-9]+[.]?[0-9]*')::double precision
                * CASE WHEN dl.voltage_rating ~* 'k[[:space:]]*v|kv'
                       THEN 1000.0 ELSE 1.0 END
        END AS rated_voltage_v,
        CASE
            WHEN dl.current_rating_a ~ '[0-9]' THEN
                SUBSTRING(dl.current_rating_a FROM '[0-9]+[.]?[0-9]*')::double precision
        END AS rated_current_a
    FROM event_base eb
    LEFT JOIN device_library dl ON dl.part_number = eb.device_type
),
normalized AS (
    SELECT
        r.*,
        COALESCE(
            r.stress_observed_abs_vds_v,
            CASE
                WHEN r.vds_before_v IS NOT NULL OR r.vds_after_v IS NOT NULL THEN
                    GREATEST(
                        COALESCE(ABS(r.vds_before_v), 0.0),
                        COALESCE(ABS(r.vds_after_v), 0.0)
                    )
            END
        ) AS observed_abs_vds_v,
        CASE
            WHEN r.rated_voltage_v IS NOT NULL AND r.rated_voltage_v > 0.0
             AND (
                    r.stress_observed_abs_vds_v IS NOT NULL
                 OR r.vds_before_v IS NOT NULL
                 OR r.vds_after_v IS NOT NULL
             )
            THEN COALESCE(
                    r.stress_observed_abs_vds_v,
                    GREATEST(
                        COALESCE(ABS(r.vds_before_v), 0.0),
                        COALESCE(ABS(r.vds_after_v), 0.0)
                    )
                 ) / r.rated_voltage_v
        END AS normalized_vds,
        CASE
            WHEN r.rated_current_a IS NOT NULL AND r.rated_current_a > 0.0
             AND r.peak_abs_id_a IS NOT NULL
            THEN r.peak_abs_id_a / r.rated_current_a
        END AS normalized_current,
        CASE
            WHEN r.stress_energy_j IS NOT NULL
             AND r.stress_energy_j > 0.0
             AND r.stress_duration_s IS NOT NULL
             AND r.stress_duration_s > 0.0
            THEN r.stress_energy_j / r.stress_duration_s
        END AS average_terminal_power_w
    FROM rated r
),
scored AS (
    SELECT
        n.*,
        (
            CASE WHEN n.electrical_terminal_energy_j IS NOT NULL THEN 1.0 ELSE 0.0 END
          + CASE WHEN n.vds_collapse_fraction IS NOT NULL THEN 0.75 ELSE 0.0 END
          + CASE WHEN n.gate_delta_fraction IS NOT NULL THEN 0.50 ELSE 0.0 END
          + CASE WHEN n.has_condition_post_iv THEN 0.75 ELSE 0.0 END
          + CASE WHEN n.normalized_vds BETWEEN 0.30 AND 1.50 THEN 0.50 ELSE 0.0 END
        ) AS application_likeness_score
    FROM normalized n
)
SELECT
    s.source,
    s.event_record_type,
    s.metadata_id,
    s.event_id,
    s.event_index,
    s.source || ':' || s.metadata_id::text || ':'
        || COALESCE(s.event_id::text, 'file') AS stress_record_key,
    s.experiment,
    s.device_id,
    s.sample_group,
    s.physical_sample_key,
    s.device_type,
    s.device_label,
    s.manufacturer,
    s.device_category,
    s.filename,
    s.stress_condition_label,
    s.event_type,
    s.path_type,
    s.is_catastrophic,
    s.confidence,
    s.sc_voltage_v,
    s.sc_duration_us,
    s.avalanche_family,
    s.avalanche_mode,
    s.avalanche_outcome,
    s.irrad_run_id,
    s.ion_species,
    s.beam_energy_mev,
    s.let_surface,
    s.fluence_at_meas,
    s.time_start_s,
    s.time_peak_s,
    s.time_end_s,
    s.stress_duration_s,
    s.electrical_terminal_energy_j,
    s.electrical_terminal_energy_basis,
    s.stress_energy_j,
    s.stress_energy_basis,
    s.average_terminal_power_w,
    s.radiation_dose_scope,
    s.radiation_fluence_basis,
    s.radiation_energy_basis,
    s.radiation_deposited_energy_j,
    s.radiation_deposited_energy_electronic_j,
    s.radiation_deposited_energy_nuclear_j,
    s.radiation_deposited_energy_total_j,
    s.radiation_dose_electronic_gy,
    s.radiation_dose_nuclear_gy,
    s.radiation_dose_total_gy,
    s.radiation_dose_gy,
    s.radiation_total_dose_gy,
    s.radiation_layer_count,
    s.radiation_calculated_layer_count,
    s.radiation_modeled_mass_kg,
    s.radiation_min_energy_in_mev,
    s.radiation_min_energy_out_mev,
    s.radiation_stopped_in_any_layer,
    s.radiation_min_range_margin_um,
    s.energy_is_comparable,
    s.energy_window_basis,
    s.energy_censored_reason,
    s.active_window_confidence,
    s.energy_level,
    s.event_energy_vds_id_j,
    s.event_energy_proxy_j,
    s.file_energy_vds_id_j,
    s.commanded_or_stored_energy_j,
    s.peak_abs_id_a,
    s.peak_abs_ig_a,
    s.peak_abs_power_w,
    s.vds_before_v,
    s.vds_after_v,
    s.vds_delta_v,
    s.delta_id_abs_a,
    s.delta_ig_abs_a,
    s.vds_collapse_fraction,
    s.gate_delta_fraction,
    s.rated_voltage_v,
    s.rated_current_a,
    s.observed_abs_vds_v,
    s.normalized_vds,
    s.normalized_current,
    s.post_iv_damage_fingerprints,
    s.post_iv_axis_count,
    s.family_post_iv_damage_fingerprints,
    s.family_post_iv_axis_count,
    s.exact_post_iv_companion_files,
    s.exact_post_iv_axis_count,
    s.has_energy,
    s.has_energy_proxy_only,
    s.has_collapse,
    s.has_gate,
    s.has_full_waveform,
    s.has_condition_post_iv,
    s.has_exact_sample_post_iv,
    s.has_family_post_iv,
    s.has_waveform_plus_post_iv,
    CASE
        WHEN s.source IN ('sc', 'avalanche') THEN 'robustness_overstress'
        WHEN s.source = 'irradiation' AND UPPER(COALESCE(s.event_type, '')) = 'SEB'
            THEN 'robustness_single_event_burnout'
        WHEN s.source = 'irradiation'
         AND UPPER(COALESCE(s.event_type, '')) IN ('SELCI', 'SELCII', 'MIXED')
            THEN 'reliability_single_event_leakage'
        WHEN s.source = 'irradiation' THEN 'radiation_response_unspecified'
        ELSE 'unknown'
    END AS stress_regime,
    'Figure 1: Stress Landscape'::text AS figure1_panel_label,
    CASE
        WHEN s.source IN ('sc', 'avalanche') THEN 'robustness'
        WHEN s.source = 'irradiation' AND UPPER(COALESCE(s.event_type, '')) = 'SEB'
            THEN 'robustness'
        WHEN s.source = 'irradiation'
         AND UPPER(COALESCE(s.event_type, '')) IN ('SELCI', 'SELCII', 'MIXED')
            THEN 'reliability'
        WHEN s.source = 'irradiation' THEN 'radiation'
        ELSE 'unknown'
    END AS figure1_regime_family,
    CASE
        WHEN s.source IN ('sc', 'avalanche') THEN 'outside_datasheet_soa'
        WHEN s.source = 'irradiation' AND UPPER(COALESCE(s.event_type, '')) = 'SEB'
            THEN 'outside_soa_single_event'
        WHEN s.source = 'irradiation'
         AND UPPER(COALESCE(s.event_type, '')) IN ('SELCI', 'SELCII', 'MIXED')
            THEN 'near_boundary_or_latent_damage'
        ELSE 'unknown'
    END AS soa_relation,
    CASE
        WHEN s.source = 'sc' THEN 'circuit_short_circuit'
        WHEN s.source = 'avalanche' THEN 'inductive_avalanche_' || LOWER(COALESCE(s.avalanche_mode, 'unknown'))
        WHEN s.source = 'irradiation' THEN 'radiation_single_event'
        ELSE 'unknown'
    END AS test_method_class,
    CASE
        WHEN s.stress_duration_s IS NULL THEN 'unknown_timescale'
        WHEN s.stress_duration_s <= 0.00001 THEN 'sub_10us_transient'
        WHEN s.stress_duration_s <= 0.001 THEN 'microsecond_to_millisecond_pulse'
        WHEN s.stress_duration_s <= 1.0 THEN 'subsecond_event'
        ELSE 'long_duration_or_file_window'
    END AS test_timescale_class,
    CASE
        WHEN s.source <> 'irradiation' THEN 'not_radiation'
        WHEN UPPER(COALESCE(s.event_type, '')) IN ('SEB', 'SELCI', 'SELCII', 'MIXED')
            THEN 'single_event_effect'
        WHEN LOWER(COALESCE(s.ion_species, '')) = 'proton'
            THEN 'proton_tid_or_displacement_context'
        ELSE 'radiation_context_unspecified'
    END AS radiation_mechanism_class,
    CASE
        WHEN COALESCE(s.is_catastrophic, FALSE)
          OR LOWER(COALESCE(s.avalanche_outcome, '')) LIKE '%fail%'
            THEN 'destructive_or_catastrophic'
        WHEN s.source = 'irradiation'
         AND UPPER(COALESCE(s.event_type, '')) IN ('SELCI', 'SELCII')
            THEN 'potentially_reversible_or_latent'
        WHEN s.has_condition_post_iv THEN 'post_iv_measured'
        ELSE 'unknown_no_post_iv'
    END AS response_reversibility,
    s.application_likeness_score,
    CASE
        WHEN s.application_likeness_score >= 2.50 THEN 'high_context_overlap'
        WHEN s.application_likeness_score >= 1.50 THEN 'partial_context_overlap'
        ELSE 'low_context_overlap'
    END AS application_likeness,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN s.device_type IS NULL THEN 'missing_device_type' END,
        CASE WHEN s.rated_voltage_v IS NULL THEN 'missing_device_voltage_rating' END,
        CASE WHEN s.rated_current_a IS NULL THEN 'missing_device_current_rating' END,
        CASE WHEN s.electrical_terminal_energy_j IS NULL THEN 'missing_electrical_terminal_energy' END,
        CASE WHEN s.has_energy_proxy_only THEN 'energy_proxy_only' END,
        CASE WHEN s.source = 'irradiation'
               AND s.event_record_type = 'detected_single_event'
               AND NOT COALESCE(s.energy_is_comparable, FALSE)
             THEN 'energy_not_comparable' END,
        CASE WHEN s.source = 'irradiation'
               AND s.event_record_type = 'detected_single_event'
               AND COALESCE(s.energy_level, 'unknown') <> 'event'
             THEN 'irradiation_energy_not_event_level' END,
        CASE WHEN s.source = 'irradiation'
               AND s.event_record_type = 'detected_single_event'
               AND COALESCE(s.energy_censored_reason, 'none') <> 'none'
             THEN 'energy_censored_' || s.energy_censored_reason END,
        CASE WHEN s.source = 'irradiation'
               AND s.event_record_type = 'detected_single_event'
               AND s.active_window_confidence IS NOT NULL
               AND s.active_window_confidence < 0.80
             THEN 'low_active_window_confidence' END,
        CASE WHEN s.source = 'irradiation'
               AND s.radiation_deposited_energy_j IS NULL
             THEN 'missing_radiation_deposition' END,
        CASE WHEN s.vds_collapse_fraction IS NULL THEN 'missing_vds_collapse' END,
        CASE WHEN s.gate_delta_fraction IS NULL THEN 'missing_gate_coupling' END,
        CASE WHEN NOT s.has_condition_post_iv THEN 'missing_condition_post_iv' END,
        CASE WHEN s.normalized_vds IS NOT NULL THEN 'normalized_voltage_available' END,
        CASE WHEN s.normalized_current IS NOT NULL THEN 'normalized_current_available' END
    ], NULL)::text[] AS context_flags,
    s.match_basis_class,
    s.readiness_status,
    s.quality_flags
FROM scored s;

CREATE INDEX idx_stress_test_context_source
    ON stress_test_context_view(source);
CREATE INDEX idx_stress_test_context_device
    ON stress_test_context_view(device_type);
CREATE INDEX idx_stress_test_context_record
    ON stress_test_context_view(stress_record_key);
CREATE INDEX idx_stress_test_context_regime
    ON stress_test_context_view(stress_regime);
CREATE INDEX idx_stress_test_context_figure1_regime_family
    ON stress_test_context_view(figure1_regime_family);

CREATE MATERIALIZED VIEW stress_proxy_candidate_view AS
WITH targets AS (
    SELECT *
    FROM stress_test_context_view
    WHERE source = 'irradiation'
      AND event_record_type = 'detected_single_event'
      AND device_type IS NOT NULL
      AND COALESCE(energy_is_comparable, FALSE)
      AND energy_level = 'event'
      AND electrical_terminal_energy_basis = 'integrated_event_vds_id'
      AND electrical_terminal_energy_j IS NOT NULL
      AND electrical_terminal_energy_j > 0.0
),
candidates AS (
    SELECT *
    FROM stress_test_context_view
    WHERE source IN ('sc', 'avalanche')
      AND device_type IS NOT NULL
      AND electrical_terminal_energy_j IS NOT NULL
      AND electrical_terminal_energy_j > 0.0
),
pairs AS (
    SELECT
        t.stress_record_key AS target_stress_record_key,
        t.metadata_id AS target_metadata_id,
        t.event_id AS target_event_id,
        t.event_index AS target_event_index,
        t.device_type,
        t.device_label AS target_device_label,
        t.filename AS target_filename,
        t.stress_condition_label AS target_stress_condition_label,
        t.event_type AS target_event_type,
        t.path_type AS target_path_type,
        t.is_catastrophic AS target_is_catastrophic,
        t.irrad_run_id AS target_irrad_run_id,
        t.ion_species AS target_ion_species,
        t.beam_energy_mev AS target_beam_energy_mev,
        t.let_surface AS target_let_surface,
        t.fluence_at_meas AS target_fluence_at_meas,
        t.radiation_dose_scope AS target_radiation_dose_scope,
        t.radiation_fluence_basis AS target_radiation_fluence_basis,
        t.radiation_energy_basis AS target_radiation_energy_basis,
        t.radiation_deposited_energy_j AS target_radiation_deposited_energy_j,
        t.radiation_deposited_energy_electronic_j AS target_radiation_deposited_energy_electronic_j,
        t.radiation_deposited_energy_nuclear_j AS target_radiation_deposited_energy_nuclear_j,
        t.radiation_deposited_energy_total_j AS target_radiation_deposited_energy_total_j,
        t.radiation_dose_electronic_gy AS target_radiation_dose_electronic_gy,
        t.radiation_dose_nuclear_gy AS target_radiation_dose_nuclear_gy,
        t.radiation_dose_total_gy AS target_radiation_dose_total_gy,
        t.radiation_dose_gy AS target_radiation_dose_gy,
        t.radiation_total_dose_gy AS target_radiation_total_dose_gy,
        t.radiation_layer_count AS target_radiation_layer_count,
        t.radiation_min_energy_in_mev AS target_radiation_min_energy_in_mev,
        t.radiation_min_energy_out_mev AS target_radiation_min_energy_out_mev,
        t.radiation_stopped_in_any_layer AS target_radiation_stopped_in_any_layer,
        t.radiation_min_range_margin_um AS target_radiation_min_range_margin_um,
        t.stress_regime AS target_stress_regime,
        t.soa_relation AS target_soa_relation,
        t.test_method_class AS target_test_method_class,
        t.test_timescale_class AS target_timescale_class,
        t.radiation_mechanism_class AS target_radiation_mechanism_class,
        t.response_reversibility AS target_response_reversibility,
        t.application_likeness AS target_application_likeness,
        t.electrical_terminal_energy_j AS target_energy_j,
        t.electrical_terminal_energy_basis AS target_energy_basis,
        t.energy_is_comparable AS target_energy_is_comparable,
        t.energy_window_basis AS target_energy_window_basis,
        t.energy_censored_reason AS target_energy_censored_reason,
        t.active_window_confidence AS target_active_window_confidence,
        t.energy_level AS target_energy_level,
        t.stress_duration_s AS target_duration_s,
        t.peak_abs_id_a AS target_peak_abs_id_a,
        t.peak_abs_ig_a AS target_peak_abs_ig_a,
        t.vds_before_v AS target_vds_before_v,
        t.vds_after_v AS target_vds_after_v,
        t.vds_collapse_fraction AS target_vds_collapse_fraction,
        t.gate_delta_fraction AS target_gate_delta_fraction,
        t.normalized_vds AS target_normalized_vds,
        t.normalized_current AS target_normalized_current,
        t.has_condition_post_iv AS target_has_condition_post_iv,
        t.post_iv_axis_count AS target_post_iv_axis_count,
        t.context_flags AS target_context_flags,
        c.stress_record_key AS candidate_stress_record_key,
        c.source AS candidate_source,
        c.metadata_id AS candidate_metadata_id,
        c.event_id AS candidate_event_id,
        c.event_index AS candidate_event_index,
        c.sample_group AS candidate_sample_group,
        c.physical_sample_key AS candidate_physical_sample_key,
        c.filename AS candidate_filename,
        c.stress_condition_label AS candidate_stress_condition_label,
        c.event_type AS candidate_event_type,
        c.path_type AS candidate_path_type,
        c.sc_voltage_v AS candidate_sc_voltage_v,
        c.sc_duration_us AS candidate_sc_duration_us,
        c.avalanche_family AS candidate_avalanche_family,
        c.avalanche_mode AS candidate_avalanche_mode,
        c.avalanche_outcome AS candidate_avalanche_outcome,
        c.stress_regime AS candidate_stress_regime,
        c.soa_relation AS candidate_soa_relation,
        c.test_method_class AS candidate_test_method_class,
        c.test_timescale_class AS candidate_timescale_class,
        c.response_reversibility AS candidate_response_reversibility,
        c.application_likeness AS candidate_application_likeness,
        c.electrical_terminal_energy_j AS candidate_energy_j,
        c.electrical_terminal_energy_basis AS candidate_energy_basis,
        c.energy_is_comparable AS candidate_energy_is_comparable,
        c.energy_window_basis AS candidate_energy_window_basis,
        c.energy_censored_reason AS candidate_energy_censored_reason,
        c.active_window_confidence AS candidate_active_window_confidence,
        c.energy_level AS candidate_energy_level,
        c.stress_duration_s AS candidate_duration_s,
        c.peak_abs_id_a AS candidate_peak_abs_id_a,
        c.peak_abs_ig_a AS candidate_peak_abs_ig_a,
        c.vds_before_v AS candidate_vds_before_v,
        c.vds_after_v AS candidate_vds_after_v,
        c.vds_collapse_fraction AS candidate_vds_collapse_fraction,
        c.gate_delta_fraction AS candidate_gate_delta_fraction,
        c.normalized_vds AS candidate_normalized_vds,
        c.normalized_current AS candidate_normalized_current,
        c.has_condition_post_iv AS candidate_has_condition_post_iv,
        c.post_iv_axis_count AS candidate_post_iv_axis_count,
        c.context_flags AS candidate_context_flags,
        ABS(LN(c.electrical_terminal_energy_j) - LN(t.electrical_terminal_energy_j)) AS log_energy_delta,
        CASE
            WHEN c.vds_collapse_fraction IS NOT NULL
             AND t.vds_collapse_fraction IS NOT NULL
            THEN ABS(c.vds_collapse_fraction - t.vds_collapse_fraction)
        END AS collapse_delta,
        CASE
            WHEN c.gate_delta_fraction IS NOT NULL
             AND t.gate_delta_fraction IS NOT NULL
            THEN ABS(c.gate_delta_fraction - t.gate_delta_fraction)
        END AS gate_delta,
        CASE
            WHEN c.stress_duration_s IS NOT NULL AND c.stress_duration_s > 0.0
             AND t.stress_duration_s IS NOT NULL AND t.stress_duration_s > 0.0
            THEN ABS(LN(c.stress_duration_s) - LN(t.stress_duration_s))
        END AS duration_log_delta,
        CASE
            WHEN t.path_type IS NOT NULL
             AND c.path_type IS NOT NULL
             AND t.path_type = c.path_type THEN 0.0
            WHEN UPPER(COALESCE(t.event_type, '')) = 'SEB'
             AND c.source = 'avalanche'
             AND COALESCE(c.vds_collapse_fraction, 0.0) >= 0.30 THEN 0.15
            WHEN UPPER(COALESCE(t.event_type, '')) = 'SEB'
             AND c.source = 'sc' THEN 0.25
            WHEN UPPER(COALESCE(t.event_type, '')) IN ('SELCI', 'SELCII', 'MIXED')
             AND c.source = 'sc' THEN 0.50
            WHEN t.path_type IS NULL OR c.path_type IS NULL THEN 0.25
            ELSE 0.75
        END AS path_penalty
    FROM targets t
    JOIN candidates c ON c.device_type = t.device_type
    WHERE ABS(LN(c.electrical_terminal_energy_j) - LN(t.electrical_terminal_energy_j)) <= 5.0
),
distances AS (
    SELECT
        p.*,
        SQRT(
            POWER(COALESCE(p.collapse_delta, 0.75) / 0.25, 2)
          + POWER(COALESCE(p.gate_delta, 0.25) / 0.20, 2)
          + POWER(p.path_penalty, 2)
        ) AS phenotype_distance,
        SQRT(
            POWER(p.log_energy_delta, 2)
          + POWER(
                SQRT(
                    POWER(COALESCE(p.collapse_delta, 0.75) / 0.25, 2)
                  + POWER(COALESCE(p.gate_delta, 0.25) / 0.20, 2)
                  + POWER(p.path_penalty, 2)
                ),
                2
            )
          + 0.01 * POWER(COALESCE(p.duration_log_delta, 1.0), 2)
        ) AS waveform_distance
    FROM pairs p
),
evidence AS (
    SELECT
        d.*,
        dm.comparability_status AS measured_comparability_status,
        dm.comparable_axes AS measured_comparable_axes,
        dm.comparable_axis_labels AS measured_comparable_axis_labels,
        dm.nearest_distance AS measured_damage_distance,
        dm.match_rank AS measured_match_rank,
        dm.match_scope AS measured_match_scope,
        pm.model_run_id AS prediction_model_run_id,
        pm.model_version AS prediction_model_version,
        pm.algorithm AS prediction_algorithm,
        pm.reference_tier AS prediction_reference_tier,
        pm.validation_mode_used AS prediction_validation_mode,
        pm.comparability_status AS prediction_comparability_status,
        pm.comparable_axes AS prediction_comparable_axes,
        pm.comparable_axis_labels AS prediction_comparable_axis_labels,
        pm.nearest_distance AS prediction_damage_distance,
        pm.right_fingerprint_confidence AS prediction_fingerprint_confidence,
        pm.right_median_confidence_score AS prediction_median_confidence_score,
        pm.right_validation_gate_pass_all AS prediction_validation_gate_pass_all,
        COALESCE(dm.nearest_distance, pm.nearest_distance) AS best_damage_distance,
        CASE
            WHEN dm.nearest_distance IS NOT NULL THEN 'measured_damage'
            WHEN pm.nearest_distance IS NOT NULL THEN 'predicted_damage'
            ELSE 'waveform_only'
        END AS damage_evidence_tier
    FROM distances d
    LEFT JOIN LATERAL (
        SELECT
            m.comparability_status,
            m.comparable_axes,
            m.comparable_axis_labels,
            m.nearest_distance,
            m.match_rank,
            CASE
                WHEN d.candidate_source = 'sc'
                 AND m.left_sc_voltage_v IS NOT DISTINCT FROM d.candidate_sc_voltage_v
                 AND m.left_sc_duration_us IS NOT DISTINCT FROM d.candidate_sc_duration_us
                    THEN 'exact_condition'
                WHEN d.candidate_source = 'avalanche'
                 AND LOWER(m.left_avalanche_sample_group) = LOWER(
                     COALESCE(d.candidate_physical_sample_key, d.candidate_sample_group)
                 )
                    THEN 'exact_condition'
                ELSE 'device_run_best_damage'
            END AS match_scope
        FROM damage_equivalence_match_view m
        WHERE m.device_type IS NOT DISTINCT FROM d.device_type
          AND m.right_irrad_run_id IS NOT DISTINCT FROM d.target_irrad_run_id
          AND (
              (d.candidate_source = 'sc' AND m.pair_type = 'sc_vs_irradiation')
              OR (d.candidate_source = 'avalanche' AND m.pair_type = 'avalanche_vs_irradiation')
          )
        ORDER BY
            CASE
                WHEN d.candidate_source = 'sc'
                 AND m.left_sc_voltage_v IS NOT DISTINCT FROM d.candidate_sc_voltage_v
                 AND m.left_sc_duration_us IS NOT DISTINCT FROM d.candidate_sc_duration_us
                    THEN 1
                WHEN d.candidate_source = 'avalanche'
                 AND LOWER(m.left_avalanche_sample_group) = LOWER(
                     COALESCE(d.candidate_physical_sample_key, d.candidate_sample_group)
                 )
                    THEN 1
                ELSE 2
            END,
            CASE m.comparability_status
                WHEN 'strong' THEN 1
                WHEN 'usable' THEN 2
                WHEN 'weak' THEN 3
                ELSE 4
            END,
            m.nearest_distance ASC NULLS LAST,
            m.match_rank ASC NULLS LAST
        LIMIT 1
    ) dm ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            p.model_run_id,
            p.model_version,
            p.algorithm,
            p.reference_tier,
            p.validation_mode_used,
            p.comparability_status,
            p.comparable_axes,
            p.comparable_axis_labels,
            p.nearest_distance,
            p.right_fingerprint_confidence,
            p.right_median_confidence_score,
            p.right_validation_gate_pass_all
        FROM damage_equivalence_prediction_match_view p
        WHERE d.candidate_source = 'sc'
          AND p.pair_type = 'sc_vs_predicted_irrad'
          AND p.is_latest_model_run
          AND p.device_type IS NOT DISTINCT FROM d.device_type
          AND p.right_irrad_run_id IS NOT DISTINCT FROM d.target_irrad_run_id
          AND p.left_sc_voltage_v IS NOT DISTINCT FROM d.candidate_sc_voltage_v
          AND p.left_sc_duration_us IS NOT DISTINCT FROM d.candidate_sc_duration_us
        ORDER BY
            CASE p.comparability_status
                WHEN 'strong' THEN 1
                WHEN 'usable' THEN 2
                WHEN 'weak' THEN 3
                ELSE 4
            END,
            p.nearest_distance ASC NULLS LAST,
            p.comparable_axes DESC NULLS LAST
        LIMIT 1
    ) pm ON TRUE
),
classified AS (
    SELECT
        e.*,
        SQRT(POWER(e.waveform_distance, 2)
             + POWER(COALESCE(e.best_damage_distance, 2.50), 2))
            AS combined_screening_distance,
        CASE
            WHEN e.log_energy_delta > 4.0 THEN 'energy_out_of_range'
            WHEN e.phenotype_distance > 2.50 THEN 'phenotype_mismatch'
            WHEN e.measured_comparability_status IN ('strong', 'usable')
             AND e.measured_match_scope = 'exact_condition'
             AND e.waveform_distance <= 1.75 THEN 'measured_damage_candidate'
            WHEN e.prediction_comparability_status IN ('strong', 'usable')
             AND e.waveform_distance <= 1.75 THEN 'predicted_damage_candidate'
            WHEN e.measured_comparability_status IN ('strong', 'usable')
             AND e.waveform_distance <= 2.25 THEN 'device_run_measured_candidate'
            WHEN e.measured_comparability_status = 'weak'
             AND e.waveform_distance <= 3.00 THEN 'weak_measured_candidate'
            WHEN e.waveform_distance <= 1.25 THEN 'waveform_only_candidate'
            WHEN e.measured_comparability_status IS NULL
             AND e.prediction_comparability_status IS NULL THEN 'missing_damage_context'
            ELSE 'inspect_manually'
        END AS candidate_status,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN e.log_energy_delta > 4.0 THEN 'energy_far_out_of_range' END,
            CASE WHEN e.phenotype_distance > 2.50 THEN 'phenotype_distance_high' END,
            CASE WHEN e.collapse_delta IS NULL THEN 'missing_collapse_overlap' END,
            CASE WHEN e.gate_delta IS NULL THEN 'missing_gate_overlap' END,
            CASE WHEN e.duration_log_delta IS NULL THEN 'missing_duration_overlap' END,
            CASE WHEN e.measured_comparability_status IS NULL
                   AND e.prediction_comparability_status IS NULL
                 THEN 'missing_damage_context' END,
            CASE WHEN e.measured_comparability_status IS NULL
                   AND e.prediction_comparability_status IS NOT NULL
                 THEN 'predicted_damage_only' END,
            CASE WHEN e.measured_match_scope = 'device_run_best_damage'
                 THEN 'damage_context_device_run_not_exact_candidate' END,
            CASE WHEN NOT e.candidate_has_condition_post_iv
                 THEN 'candidate_missing_condition_post_iv' END,
            CASE WHEN NOT e.target_has_condition_post_iv
                 THEN 'target_missing_condition_post_iv' END,
            CASE WHEN e.candidate_energy_basis LIKE '%proxy%'
                 THEN 'candidate_energy_proxy_basis' END,
            CASE WHEN e.target_energy_basis LIKE '%proxy%'
                 THEN 'target_energy_proxy_basis' END,
            CASE WHEN NOT COALESCE(e.target_energy_is_comparable, FALSE)
                 THEN 'target_energy_not_comparable' END,
            CASE WHEN COALESCE(e.target_energy_level, 'unknown') <> 'event'
                 THEN 'target_energy_not_event_level' END,
            CASE WHEN COALESCE(e.target_energy_censored_reason, 'none') <> 'none'
                 THEN 'target_energy_censored_' || e.target_energy_censored_reason END,
            CASE WHEN e.target_energy_window_basis IN (
                    'unknown_no_fluence',
                    'fluence_reset_artifact',
                    'fluence_static_or_missing_progression',
                    'not_analyzed'
                 ) THEN 'target_energy_window_uncertain' END
        ], NULL)::text[] AS candidate_blockers
    FROM evidence e
),
ranked AS (
    SELECT
        c.*,
        CASE c.candidate_status
            WHEN 'measured_damage_candidate' THEN 1
            WHEN 'predicted_damage_candidate' THEN 2
            WHEN 'device_run_measured_candidate' THEN 3
            WHEN 'weak_measured_candidate' THEN 4
            WHEN 'waveform_only_candidate' THEN 5
            WHEN 'inspect_manually' THEN 6
            WHEN 'missing_damage_context' THEN 5
            WHEN 'phenotype_mismatch' THEN 6
            ELSE 7
        END AS candidate_status_priority,
        CASE
            WHEN c.candidate_status = 'measured_damage_candidate'
             AND c.combined_screening_distance <= 1.50 THEN 'high_screening_confidence'
            WHEN c.candidate_status = 'measured_damage_candidate'
                THEN 'medium_screening_confidence'
            WHEN c.candidate_status = 'predicted_damage_candidate'
                THEN 'model_supported_screening_confidence'
            WHEN c.candidate_status = 'device_run_measured_candidate'
                THEN 'low_device_run_damage_screening_confidence'
            WHEN c.candidate_status = 'weak_measured_candidate'
                THEN 'low_measured_damage_screening_confidence'
            WHEN c.candidate_status = 'waveform_only_candidate'
                THEN 'low_waveform_only_confidence'
            ELSE 'blocked_or_manual_review'
        END AS replacement_confidence,
        ROW_NUMBER() OVER (
            PARTITION BY c.target_stress_record_key
            ORDER BY
                CASE c.candidate_status
                    WHEN 'measured_damage_candidate' THEN 1
                    WHEN 'predicted_damage_candidate' THEN 2
                    WHEN 'device_run_measured_candidate' THEN 3
                    WHEN 'weak_measured_candidate' THEN 4
                    WHEN 'waveform_only_candidate' THEN 5
                    WHEN 'inspect_manually' THEN 6
                    WHEN 'missing_damage_context' THEN 5
                    WHEN 'phenotype_mismatch' THEN 6
                    ELSE 7
                END,
                c.combined_screening_distance ASC NULLS LAST,
                c.waveform_distance ASC NULLS LAST,
                c.candidate_source,
                c.candidate_stress_record_key
        ) AS candidate_rank
    FROM classified c
)
SELECT
    target_stress_record_key,
    target_metadata_id,
    target_event_id,
    target_event_index,
    target_filename,
    target_stress_condition_label,
    device_type,
    target_device_label,
    target_event_type,
    target_path_type,
    target_is_catastrophic,
    target_irrad_run_id,
    target_ion_species,
    target_beam_energy_mev,
    target_let_surface,
    target_fluence_at_meas,
    target_radiation_dose_scope,
    target_radiation_fluence_basis,
    target_radiation_energy_basis,
    target_radiation_deposited_energy_j,
    target_radiation_deposited_energy_electronic_j,
    target_radiation_deposited_energy_nuclear_j,
    target_radiation_deposited_energy_total_j,
    target_radiation_dose_electronic_gy,
    target_radiation_dose_nuclear_gy,
    target_radiation_dose_total_gy,
    target_radiation_dose_gy,
    target_radiation_total_dose_gy,
    target_radiation_layer_count,
    target_radiation_min_energy_in_mev,
    target_radiation_min_energy_out_mev,
    target_radiation_stopped_in_any_layer,
    target_radiation_min_range_margin_um,
    target_stress_regime,
    target_soa_relation,
    target_test_method_class,
    target_timescale_class,
    target_radiation_mechanism_class,
    target_response_reversibility,
    target_application_likeness,
    target_energy_j,
    target_energy_basis,
    target_energy_is_comparable,
    target_energy_window_basis,
    target_energy_censored_reason,
    target_active_window_confidence,
    target_energy_level,
    target_duration_s,
    target_peak_abs_id_a,
    target_peak_abs_ig_a,
    target_vds_before_v,
    target_vds_after_v,
    target_vds_collapse_fraction,
    target_gate_delta_fraction,
    target_normalized_vds,
    target_normalized_current,
    target_has_condition_post_iv,
    target_post_iv_axis_count,
    target_context_flags,
    candidate_stress_record_key,
    candidate_rank,
    candidate_source,
    candidate_metadata_id,
    candidate_event_id,
    candidate_event_index,
    candidate_sample_group,
    candidate_physical_sample_key,
    candidate_filename,
    candidate_stress_condition_label,
    candidate_event_type,
    candidate_path_type,
    candidate_sc_voltage_v,
    candidate_sc_duration_us,
    candidate_avalanche_family,
    candidate_avalanche_mode,
    candidate_avalanche_outcome,
    candidate_stress_regime,
    candidate_soa_relation,
    candidate_test_method_class,
    candidate_timescale_class,
    candidate_response_reversibility,
    candidate_application_likeness,
    candidate_energy_j,
    candidate_energy_basis,
    candidate_energy_is_comparable,
    candidate_energy_window_basis,
    candidate_energy_censored_reason,
    candidate_active_window_confidence,
    candidate_energy_level,
    candidate_duration_s,
    candidate_peak_abs_id_a,
    candidate_peak_abs_ig_a,
    candidate_vds_before_v,
    candidate_vds_after_v,
    candidate_vds_collapse_fraction,
    candidate_gate_delta_fraction,
    candidate_normalized_vds,
    candidate_normalized_current,
    candidate_has_condition_post_iv,
    candidate_post_iv_axis_count,
    candidate_context_flags,
    log_energy_delta,
    collapse_delta,
    gate_delta,
    duration_log_delta,
    path_penalty,
    phenotype_distance,
    waveform_distance,
    measured_comparability_status,
    measured_comparable_axes,
    measured_comparable_axis_labels,
    measured_damage_distance,
    measured_match_rank,
    measured_match_scope,
    prediction_model_run_id,
    prediction_model_version,
    prediction_algorithm,
    prediction_reference_tier,
    prediction_validation_mode,
    prediction_comparability_status,
    prediction_comparable_axes,
    prediction_comparable_axis_labels,
    prediction_damage_distance,
    prediction_fingerprint_confidence,
    prediction_median_confidence_score,
    prediction_validation_gate_pass_all,
    best_damage_distance,
    damage_evidence_tier,
    combined_screening_distance,
    candidate_status,
    candidate_status_priority,
    replacement_confidence,
    candidate_blockers
FROM ranked
WHERE candidate_rank <= 10;

CREATE INDEX idx_stress_proxy_candidate_target_rank
    ON stress_proxy_candidate_view(target_stress_record_key, candidate_rank);
CREATE INDEX idx_stress_proxy_candidate_device
    ON stress_proxy_candidate_view(device_type);
CREATE INDEX idx_stress_proxy_candidate_status
    ON stress_proxy_candidate_view(candidate_status);
CREATE INDEX idx_stress_proxy_candidate_source
    ON stress_proxy_candidate_view(candidate_source);
CREATE INDEX idx_stress_proxy_candidate_confidence
    ON stress_proxy_candidate_view(replacement_confidence);

CREATE MATERIALIZED VIEW stress_proxy_candidate_summary_view AS
WITH top_candidates AS (
    SELECT *
    FROM stress_proxy_candidate_view
    WHERE candidate_rank = 1
)
SELECT
    candidate_source,
    target_event_type,
    target_path_type,
    candidate_status,
    replacement_confidence,
    COUNT(*) AS top_target_events,
    COUNT(DISTINCT device_type) AS device_type_count,
    COUNT(*) FILTER (WHERE damage_evidence_tier = 'measured_damage')
        AS measured_damage_top_events,
    COUNT(*) FILTER (WHERE damage_evidence_tier = 'predicted_damage')
        AS predicted_damage_top_events,
    COUNT(*) FILTER (WHERE damage_evidence_tier = 'waveform_only')
        AS waveform_only_top_events,
    MIN(combined_screening_distance) AS best_combined_screening_distance,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY combined_screening_distance)
        AS median_combined_screening_distance,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY waveform_distance)
        AS median_waveform_distance,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY best_damage_distance)
        AS median_damage_distance,
    STRING_AGG(DISTINCT device_type, ', ' ORDER BY device_type)
        AS device_types
FROM top_candidates
GROUP BY candidate_source, target_event_type, target_path_type,
         candidate_status, replacement_confidence;

CREATE INDEX idx_stress_proxy_candidate_summary_status
    ON stress_proxy_candidate_summary_view(candidate_status);
CREATE INDEX idx_stress_proxy_candidate_summary_source
    ON stress_proxy_candidate_summary_view(candidate_source);

