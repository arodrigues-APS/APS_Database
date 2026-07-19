-- Append evaluation semantics and target units without changing the existing
-- validation-view column order consumed by deployed Superset datasets.

CREATE OR REPLACE VIEW iv_damage_validation_summary_view AS
SELECT
    result.model_run_id,
    result.split_scheme,
    result.split_role,
    unit.stress_type,
    unit.target_type,
    unit.device_type,
    unit.ion_species,
    result.support_status,
    count(*) AS independent_units,
    count(DISTINCT unit.physical_device_key) AS physical_devices,
    count(DISTINCT unit.campaign_key) AS campaigns,
    avg(result.abs_residual) FILTER (WHERE result.abs_residual IS NOT NULL)
        AS mean_abs_error,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY result.abs_residual)
        FILTER (WHERE result.abs_residual IS NOT NULL) AS median_abs_error,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY result.abs_residual)
        FILTER (WHERE result.abs_residual IS NOT NULL) AS p90_abs_error,
    avg(result.residual) FILTER (WHERE result.residual IS NOT NULL) AS mean_bias,
    avg(result.interval_hit::integer) FILTER (WHERE result.interval_hit IS NOT NULL)
        AS interval_coverage,
    model.model_version,
    result.evaluation_kind,
    CASE unit.target_type
        WHEN 'delta_vth_v' THEN 'V'
        ELSE 'ln(ratio)'
    END AS response_unit
FROM iv_damage_validation_results result
JOIN iv_damage_response_units unit ON unit.id = result.response_unit_id
JOIN iv_damage_model_runs model ON model.id = result.model_run_id
GROUP BY result.model_run_id, result.split_scheme, result.split_role,
         unit.stress_type, unit.target_type, unit.device_type, unit.ion_species,
         result.support_status, model.model_version, result.evaluation_kind;

CREATE VIEW iv_damage_curve_projection_view AS
SELECT
    projection.id AS curve_projection_id,
    prediction.id AS scalar_prediction_id,
    request.request_key,
    model.model_version,
    request.stress_type,
    request.target_type,
    method.projection_kind,
    method.method_version,
    curve.curve_family,
    curve.measurement_protocol_id,
    projection.projection_status,
    projection.evidence_status,
    projection.decision_eligible,
    point.point_index,
    point.x_value_v,
    point.pre_i_drain_a,
    point.predicted_i_drain_a,
    point.predicted_lower_a,
    point.predicted_upper_a,
    projection.created_at
FROM iv_damage_curve_projections projection
JOIN iv_damage_predictions prediction ON prediction.id = projection.prediction_id
JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
JOIN iv_damage_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_curve_projection_methods method ON method.id = projection.projection_method_id
JOIN iv_damage_curve_snapshots curve ON curve.id = projection.pre_curve_snapshot_id
LEFT JOIN iv_damage_curve_projection_points point
  ON point.curve_projection_id = projection.id;

COMMENT ON VIEW iv_damage_curve_projection_view IS
    'Deterministic scalar projection only; keep separate from learned functional-curve predictions.';
