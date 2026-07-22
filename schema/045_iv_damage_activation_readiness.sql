-- V3 activation readiness, governed evidence-batch ledger, and scalar
-- prediction provenance. This migration is additive and intentionally leaves
-- the certified lifecycle introduced by migrations 032--044 unchanged.

CREATE TABLE iv_damage_evidence_batches (
    id BIGSERIAL PRIMARY KEY,
    batch_key TEXT NOT NULL UNIQUE,
    manifest_version INTEGER NOT NULL CHECK (manifest_version = 1),
    plan_sha CHAR(64) NOT NULL,
    manifest JSONB NOT NULL,
    plan_report JSONB NOT NULL,
    prepared_by TEXT NOT NULL,
    prepared_at TIMESTAMPTZ NOT NULL,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    applied_by TEXT,
    applied_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'approved'
        CHECK (status IN ('approved', 'applying', 'applied', 'failed')),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (approved_by IS NULL OR approved_by <> prepared_by),
    CHECK ((approved_by IS NULL) = (approved_at IS NULL)),
    CHECK ((applied_by IS NULL) = (applied_at IS NULL))
);

CREATE UNIQUE INDEX iv_damage_evidence_batches_plan_sha_idx
    ON iv_damage_evidence_batches (plan_sha);

CREATE TABLE iv_damage_evidence_batch_items (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL
        REFERENCES iv_damage_evidence_batches(id) ON DELETE RESTRICT,
    item_key TEXT NOT NULL,
    item_type TEXT NOT NULL
        CHECK (item_type IN ('acquisition', 'stress_session', 'observation', 'response_unit')),
    item_order INTEGER NOT NULL CHECK (item_order >= 0),
    payload_sha CHAR(64) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'applied', 'failed')),
    result_identity JSONB,
    last_error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    applied_at TIMESTAMPTZ,
    UNIQUE (batch_id, item_key),
    UNIQUE (batch_id, item_order),
    CHECK ((status = 'applied') = (applied_at IS NOT NULL)),
    CHECK (status <> 'applied' OR result_identity IS NOT NULL)
);

CREATE INDEX iv_damage_evidence_batch_items_status_idx
    ON iv_damage_evidence_batch_items (batch_id, status, item_order);

CREATE FUNCTION iv_damage_guard_evidence_batch_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'iv_damage_evidence_batches is append-only';
    END IF;
    IF NEW.batch_key IS DISTINCT FROM OLD.batch_key
       OR NEW.manifest_version IS DISTINCT FROM OLD.manifest_version
       OR NEW.plan_sha IS DISTINCT FROM OLD.plan_sha
       OR NEW.manifest IS DISTINCT FROM OLD.manifest
       OR NEW.plan_report IS DISTINCT FROM OLD.plan_report
       OR NEW.prepared_by IS DISTINCT FROM OLD.prepared_by
       OR NEW.prepared_at IS DISTINCT FROM OLD.prepared_at
       OR NEW.approved_by IS DISTINCT FROM OLD.approved_by
       OR NEW.approved_at IS DISTINCT FROM OLD.approved_at
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'approved evidence manifest identity and payload are immutable';
    END IF;
    IF NOT (
        NEW.status = OLD.status
        OR (OLD.status = 'approved' AND NEW.status = 'applying')
        OR (OLD.status = 'applying' AND NEW.status IN ('applied', 'failed'))
        OR (OLD.status = 'failed' AND NEW.status = 'applying')
    ) THEN
        RAISE EXCEPTION 'invalid evidence batch transition: % -> %',
            OLD.status, NEW.status;
    END IF;
    IF OLD.applied_by IS NOT NULL
       AND (
           NEW.applied_by IS DISTINCT FROM OLD.applied_by
           OR NEW.applied_at IS DISTINCT FROM OLD.applied_at
       ) THEN
        RAISE EXCEPTION 'evidence batch application audit is immutable';
    END IF;
    IF (NEW.status = 'applied') IS DISTINCT FROM (
        NEW.applied_by IS NOT NULL
        AND btrim(NEW.applied_by) <> ''
        AND NEW.applied_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'applied evidence batch requires complete application audit';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_evidence_batch_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_evidence_batches
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_evidence_batch_lifecycle();

CREATE FUNCTION iv_damage_guard_evidence_batch_item_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'iv_damage_evidence_batch_items is append-only';
    END IF;
    IF NEW.batch_id IS DISTINCT FROM OLD.batch_id
       OR NEW.item_key IS DISTINCT FROM OLD.item_key
       OR NEW.item_type IS DISTINCT FROM OLD.item_type
       OR NEW.item_order IS DISTINCT FROM OLD.item_order
       OR NEW.payload_sha IS DISTINCT FROM OLD.payload_sha THEN
        RAISE EXCEPTION 'approved evidence item identity and payload are immutable';
    END IF;
    IF NOT (
        NEW.status = OLD.status
        OR (OLD.status IN ('pending', 'failed')
            AND NEW.status IN ('applied', 'failed'))
    ) THEN
        RAISE EXCEPTION 'invalid evidence item transition: % -> %',
            OLD.status, NEW.status;
    END IF;
    IF NEW.attempt_count < OLD.attempt_count THEN
        RAISE EXCEPTION 'evidence item attempt count cannot decrease';
    END IF;
    IF OLD.status = 'applied'
       AND (
           NEW.result_identity IS DISTINCT FROM OLD.result_identity
           OR NEW.applied_at IS DISTINCT FROM OLD.applied_at
           OR NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
       ) THEN
        RAISE EXCEPTION 'applied evidence item result is immutable';
    END IF;
    IF NEW.status = 'applied' THEN
        IF NEW.result_identity IS NULL OR NEW.applied_at IS NULL THEN
            RAISE EXCEPTION 'applied evidence item requires result identity and timestamp';
        END IF;
    ELSIF NEW.result_identity IS NOT NULL OR NEW.applied_at IS NOT NULL THEN
        RAISE EXCEPTION 'unapplied evidence item cannot carry an applied result';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER iv_damage_evidence_batch_item_lifecycle_guard
BEFORE UPDATE OR DELETE ON iv_damage_evidence_batch_items
FOR EACH ROW EXECUTE FUNCTION iv_damage_guard_evidence_batch_item_lifecycle();

CREATE VIEW iv_damage_claim_activation_status_view AS
WITH claim_domains AS (
    SELECT 'scalar'::TEXT AS claim_type, stress_type, target_type,
           NULL::TEXT AS curve_family,
           CASE target_type WHEN 'delta_vth_v' THEN 'V' ELSE 'ln(ratio)' END AS response_unit
    FROM (VALUES ('sc'), ('irradiation')) stress(stress_type)
    CROSS JOIN (VALUES ('delta_vth_v'), ('log_rdson_ratio')) target(target_type)
    UNION ALL
    SELECT 'curve', stress_type, NULL, curve_family, 'A'
    FROM (VALUES ('sc'), ('irradiation')) stress(stress_type)
    CROSS JOIN (VALUES ('IdVg'), ('IdVd')) family(curve_family)
), scalar_counts AS (
    SELECT domain.stress_type, domain.target_type,
        (SELECT count(*) FROM iv_damage_response_units unit
          WHERE unit.stress_type = domain.stress_type AND unit.target_type = domain.target_type
            AND unit.quality_status = 'usable' AND unit.required_features_complete) AS evidence_count,
        (SELECT count(*) FROM iv_damage_extraction_methods method
          WHERE method.target_type = domain.target_type AND method.approved) AS method_count,
        (SELECT count(*) FROM iv_damage_acceptance_policies policy
          WHERE policy.stress_type = domain.stress_type AND policy.target_type = domain.target_type
            AND policy.approved) AS policy_count,
        (SELECT count(*) FROM iv_damage_dataset_snapshots snapshot
          WHERE snapshot.domain_summary->>'stress_type' = domain.stress_type
            AND snapshot.domain_summary->>'target_type' = domain.target_type) AS snapshot_count,
        (SELECT count(*) FROM iv_damage_model_runs model
          WHERE model.stress_type = domain.stress_type AND model.target_type = domain.target_type) AS model_count,
        (SELECT count(*) FROM iv_damage_external_certifications certification
          JOIN iv_damage_model_runs model ON model.id = certification.model_run_id
          WHERE model.stress_type = domain.stress_type AND model.target_type = domain.target_type
            AND certification.passed) AS certification_count,
        (SELECT count(*) FROM iv_damage_model_deployments deployment
          JOIN iv_damage_model_runs model ON model.id = deployment.model_run_id
          WHERE model.stress_type = domain.stress_type AND model.target_type = domain.target_type
            AND deployment.deployment_mode = 'shadow' AND deployment.active) AS shadow_count,
        (SELECT count(*) FROM iv_damage_model_releases release
          JOIN iv_damage_model_runs model ON model.id = release.model_run_id
          WHERE model.stress_type = domain.stress_type AND model.target_type = domain.target_type
            AND release.active) AS decision_count,
        (SELECT count(*) FROM iv_damage_prediction_requests request
          WHERE request.stress_type = domain.stress_type AND request.target_type = domain.target_type) AS request_count,
        (SELECT count(*) FROM iv_damage_predictions prediction
          JOIN iv_damage_prediction_requests request ON request.id = prediction.request_id
          WHERE request.stress_type = domain.stress_type AND request.target_type = domain.target_type) AS prediction_count,
        (SELECT count(*) FROM iv_damage_prediction_outcomes outcome
          JOIN iv_damage_prediction_requests request ON request.id = outcome.request_id
          WHERE request.stress_type = domain.stress_type AND request.target_type = domain.target_type) AS outcome_count
    FROM (VALUES ('sc', 'delta_vth_v'), ('sc', 'log_rdson_ratio'),
                 ('irradiation', 'delta_vth_v'), ('irradiation', 'log_rdson_ratio'))
         domain(stress_type, target_type)
), curve_counts AS (
    SELECT domain.stress_type, domain.curve_family,
        (SELECT count(*) FROM iv_damage_curve_response_pairs pair
          JOIN iv_damage_response_units unit ON unit.id = pair.response_unit_id
          JOIN iv_damage_curve_snapshots pre ON pre.id = pair.pre_curve_snapshot_id
          WHERE unit.stress_type = domain.stress_type AND pre.curve_family = domain.curve_family
            AND pair.quality_status = 'usable') AS evidence_count,
        0::BIGINT AS method_count,
        (SELECT count(*) FROM iv_damage_curve_model_runs model
          JOIN iv_damage_acceptance_policies policy ON policy.id = model.acceptance_policy_id
          WHERE model.stress_type = domain.stress_type AND model.curve_family = domain.curve_family
            AND policy.approved) AS policy_count,
        (SELECT count(DISTINCT member.dataset_snapshot_id) FROM iv_damage_curve_snapshot_members member
          JOIN iv_damage_curve_response_pairs pair ON pair.id = member.curve_response_pair_id
          JOIN iv_damage_response_units unit ON unit.id = pair.response_unit_id
          JOIN iv_damage_curve_snapshots pre ON pre.id = pair.pre_curve_snapshot_id
          WHERE unit.stress_type = domain.stress_type AND pre.curve_family = domain.curve_family) AS snapshot_count,
        (SELECT count(*) FROM iv_damage_curve_model_runs model
          WHERE model.stress_type = domain.stress_type AND model.curve_family = domain.curve_family) AS model_count,
        (SELECT count(*) FROM iv_damage_curve_external_certifications certification
          JOIN iv_damage_curve_model_runs model ON model.id = certification.curve_model_run_id
          WHERE model.stress_type = domain.stress_type AND model.curve_family = domain.curve_family
            AND certification.passed) AS certification_count,
        (SELECT count(*) FROM iv_damage_curve_model_deployments deployment
          JOIN iv_damage_curve_model_runs model ON model.id = deployment.curve_model_run_id
          WHERE model.stress_type = domain.stress_type AND model.curve_family = domain.curve_family
            AND deployment.deployment_mode = 'shadow' AND deployment.active) AS shadow_count,
        (SELECT count(*) FROM iv_damage_curve_model_deployments deployment
          JOIN iv_damage_curve_model_runs model ON model.id = deployment.curve_model_run_id
          WHERE model.stress_type = domain.stress_type AND model.curve_family = domain.curve_family
            AND deployment.deployment_mode = 'decision' AND deployment.active) AS decision_count,
        (SELECT count(*) FROM iv_damage_curve_prediction_requests request
          WHERE request.stress_type = domain.stress_type AND request.curve_family = domain.curve_family) AS request_count,
        (SELECT count(*) FROM iv_damage_curve_predictions prediction
          JOIN iv_damage_curve_prediction_requests request ON request.id = prediction.request_id
          WHERE request.stress_type = domain.stress_type AND request.curve_family = domain.curve_family) AS prediction_count,
        (SELECT count(*) FROM iv_damage_curve_prediction_outcomes outcome
          JOIN iv_damage_curve_predictions prediction ON prediction.id = outcome.curve_prediction_id
          JOIN iv_damage_curve_prediction_requests request ON request.id = prediction.request_id
          WHERE request.stress_type = domain.stress_type AND request.curve_family = domain.curve_family) AS outcome_count
    FROM (VALUES ('sc', 'IdVg'), ('sc', 'IdVd'),
                 ('irradiation', 'IdVg'), ('irradiation', 'IdVd'))
         domain(stress_type, curve_family)
), combined AS (
    SELECT domains.*, counts.evidence_count, counts.method_count, counts.policy_count,
           counts.snapshot_count, counts.model_count, counts.certification_count,
           counts.shadow_count, counts.decision_count, counts.request_count,
           counts.prediction_count, counts.outcome_count
    FROM claim_domains domains
    JOIN scalar_counts counts USING (stress_type, target_type)
    WHERE domains.claim_type = 'scalar'
    UNION ALL
    SELECT domains.*, counts.evidence_count, counts.method_count, counts.policy_count,
           counts.snapshot_count, counts.model_count, counts.certification_count,
           counts.shadow_count, counts.decision_count, counts.request_count,
           counts.prediction_count, counts.outcome_count
    FROM claim_domains domains
    JOIN curve_counts counts USING (stress_type, curve_family)
    WHERE domains.claim_type = 'curve'
)
SELECT combined.*,
    CASE
      WHEN evidence_count = 0 THEN 'evidence'
      WHEN claim_type = 'scalar' AND method_count = 0 THEN 'extraction_method'
      WHEN policy_count = 0 THEN 'acceptance_policy'
      WHEN snapshot_count = 0 THEN 'dataset_snapshot'
      WHEN model_count = 0 THEN 'model_development'
      WHEN certification_count = 0 THEN 'external_certification'
      WHEN shadow_count = 0 THEN 'shadow_deployment'
      WHEN request_count = 0 THEN 'prospective_request'
      WHEN prediction_count = 0 THEN 'prediction_scoring'
      WHEN outcome_count = 0 THEN 'prospective_outcome'
      WHEN decision_count = 0 THEN 'decision_release'
      ELSE 'active' END AS blocking_stage,
    CASE
      WHEN evidence_count = 0 THEN 'Plan governed raw-measurement evidence; scalar application also requires an independently approved extraction method.'
      WHEN claim_type = 'scalar' AND method_count = 0 THEN 'Register and independently approve the extraction method.'
      WHEN policy_count = 0 THEN 'Create and independently approve a complete acceptance policy.'
      WHEN snapshot_count = 0 THEN 'Freeze a leak-free dataset snapshot.'
      WHEN model_count = 0 THEN 'Train and select one development candidate.'
      WHEN certification_count = 0 THEN 'Have the external custodian consume the sealed holdout once.'
      WHEN shadow_count = 0 THEN 'Activate the externally certified model in shadow mode.'
      WHEN request_count = 0 THEN 'Submit an in-cohort prospective request.'
      WHEN prediction_count = 0 THEN 'Run scoring for the active shadow deployment.'
      WHEN outcome_count = 0 THEN 'Collect prospective outcomes; output remains screening-only.'
      WHEN decision_count = 0 THEN 'Assess shadow evidence before any explicit promotion.'
      ELSE 'Monitor the active claim.' END AS next_action
FROM combined;

CREATE VIEW iv_damage_scalar_prediction_provenance_view AS
SELECT
    monitoring.prediction_id,
    monitoring.request_id,
    monitoring.request_key,
    model.model_version,
    request.physical_device_key,
    request.stress_type,
    request.target_type,
    CASE request.target_type WHEN 'delta_vth_v' THEN 'V' ELSE 'ln(ratio)' END AS response_unit,
    request.measurement_protocol_id,
    request.requested_prediction_horizon_s,
    prediction.deployment_mode,
    monitoring.support_status,
    monitoring.evidence_status,
    monitoring.predicted_response,
    monitoring.predicted_response_lower,
    monitoring.predicted_response_upper,
    prediction.predicted_post_value,
    prediction.predicted_post_lower,
    prediction.predicted_post_upper,
    monitoring.ood_score,
    monitoring.ood_threshold,
    monitoring.decision_eligible,
    CASE WHEN prediction.deployment_mode = 'shadow'
         THEN 'SCREENING ONLY' ELSE 'DECISION ELIGIBILITY REQUIRES ALL RELEASE GATES' END AS usage_label,
    monitoring.observed_response,
    monitoring.residual,
    monitoring.created_at,
    monitoring.matched_at
FROM iv_damage_prediction_monitoring_view monitoring
JOIN iv_damage_predictions prediction ON prediction.id = monitoring.prediction_id
JOIN iv_damage_prediction_requests request ON request.id = monitoring.request_id
JOIN iv_damage_model_runs model ON model.id = monitoring.model_run_id;

COMMENT ON TABLE iv_damage_evidence_batches IS
    'Approved immutable manifest plans for resumable governed evidence admission.';
COMMENT ON TABLE iv_damage_evidence_batch_items IS
    'Per-item application ledger; applied items are skipped during safe resume.';
COMMENT ON VIEW iv_damage_claim_activation_status_view IS
    'Always returns four scalar and four curve claim domains, including zero-data lifecycle blockers.';
COMMENT ON VIEW iv_damage_scalar_prediction_provenance_view IS
    'Scalar prediction provenance with units, deployment mode, intervals, and explicit shadow screening label.';
