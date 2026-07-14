-- apply_schema: pipeline-owned
--
-- Legacy snapshot model: this SQL intentionally reads cpvd, dptgraphs, and
-- dptslopes, which are historical aggregate tables with no current ingestion
-- owner. It is therefore opt-in and must never be applied by core schema
-- bootstrap. The dashboard builder checks those prerequisites before use.
--
-- Modern, auditable views for the legacy capacitance-voltage (CV) and
-- double-pulse-test (DPT) tables.
--
-- Source-unit contract recovered from the original ingestion implementation:
-- DPT CSV time (seconds) was multiplied by 1e6 before insertion, therefore
-- dptgraphs.time is microseconds.  Cumulative energy converts the trapezoidal
-- time delta back to seconds with 1e-6.  The energy is intentionally the
-- positive drain-terminal energy over the imported waveform window; it is not
-- called Eon/Eoff because no independently curated switching windows exist.

CREATE OR REPLACE VIEW cv_characterization_view AS
SELECT
    id,
    device AS manufacturer,
    sample AS sample_id,
    tablename AS source_table,
    temperature AS temperature_c,
    identifier AS device_type,
    LOWER(cp_type) AS capacitance_type,
    v_drain AS drain_bias_v,
    c_p AS capacitance_f
FROM cpvd
WHERE v_drain IS NOT NULL
  AND c_p IS NOT NULL
  AND c_p > 0.0;

COMMENT ON VIEW cv_characterization_view IS
    'Normalized CV measurements. capacitance_f is the measured parallel capacitance; capacitance_type preserves cds/cgd/cgg/cgs rather than silently relabeling these as datasheet Ciss/Coss/Crss.';

CREATE OR REPLACE VIEW dpt_characterization_view AS
WITH ordered AS (
    SELECT
        id,
        device AS manufacturer,
        sample AS sample_id,
        tablename AS capture_key,
        temperature AS temperature_c,
        identifier AS device_type,
        dptvds AS nominal_bus_voltage_v,
        dptids AS nominal_drain_current_a,
        time AS time_us,
        time - MIN(time) OVER (PARTITION BY tablename) AS time_relative_us,
        v_drain AS drain_voltage_v,
        i_drain AS drain_current_a,
        v_gate AS gate_voltage_v,
        v_drain * i_drain AS instantaneous_power_w,
        LAG(time) OVER (
            PARTITION BY tablename ORDER BY time, id
        ) AS previous_time_us,
        LAG(v_drain * i_drain) OVER (
            PARTITION BY tablename ORDER BY time, id
        ) AS previous_power_w
    FROM dptgraphs
    WHERE time IS NOT NULL
), integrated AS (
    SELECT
        *,
        GREATEST(instantaneous_power_w, 0.0) AS positive_power_w,
        CASE
            WHEN previous_time_us IS NULL
              OR previous_power_w IS NULL
              OR time_us < previous_time_us
                THEN 0.0
            ELSE
                0.5
                * (
                    GREATEST(instantaneous_power_w, 0.0)
                    + GREATEST(previous_power_w, 0.0)
                  )
                * (time_us - previous_time_us)
                * 1e-6
        END AS positive_energy_increment_j
    FROM ordered
)
SELECT
    id,
    manufacturer,
    sample_id,
    capture_key,
    temperature_c,
    device_type,
    nominal_bus_voltage_v,
    nominal_drain_current_a,
    time_us,
    time_relative_us,
    drain_voltage_v,
    drain_current_a,
    gate_voltage_v,
    instantaneous_power_w,
    positive_power_w,
    SUM(positive_energy_increment_j) OVER (
        PARTITION BY capture_key
        ORDER BY time_us, id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_positive_energy_j,
    'imported_capture_window'::text AS energy_window_basis,
    'time_us_recovered_from_ingestion_x1e6'::text AS time_unit_provenance
FROM integrated;

COMMENT ON VIEW dpt_characterization_view IS
    'Normalized DPT waveforms with recovered microsecond time, instantaneous drain-terminal power, and trapezoidal cumulative positive energy over the imported capture window. The cumulative quantity is diagnostic and is not a curated Eon/Eoff metric.';

CREATE OR REPLACE VIEW dpt_switching_metric_view AS
SELECT
    id,
    device AS manufacturer,
    sample AS sample_id,
    tablename AS capture_key,
    temperature AS temperature_c,
    identifier AS device_type,
    dptvds AS nominal_bus_voltage_v,
    dptids AS nominal_drain_current_a,
    turnoffdvdt AS turn_off_dv_dt_v_per_us,
    turnondvdt AS turn_on_dv_dt_v_per_us,
    turnoffdidt AS turn_off_di_dt_a_per_us,
    turnondidt AS turn_on_di_dt_a_per_us,
    ABS(turnoffdvdt) AS turn_off_dv_dt_magnitude_v_per_us,
    ABS(turnondvdt) AS turn_on_dv_dt_magnitude_v_per_us,
    ABS(turnoffdidt) AS turn_off_di_dt_magnitude_a_per_us,
    ABS(turnondidt) AS turn_on_di_dt_magnitude_a_per_us,
    'legacy_10_90_extraction'::text AS extraction_method
FROM dptslopes;

COMMENT ON VIEW dpt_switching_metric_view IS
    'Legacy 10-90% DPT slew metrics with explicit sign and magnitude columns. Values are V/us and A/us because the source parser converted waveform time to microseconds before differentiation.';
