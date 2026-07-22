-- Retrospective V3 research prediction lane.
--
-- This migration is intentionally additive.  Research evidence is isolated
-- from the certified iv_damage_* lifecycle and is never decision eligible.

CREATE TABLE iv_damage_research_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_version TEXT NOT NULL UNIQUE,
    snapshot_hash CHAR(64) NOT NULL UNIQUE,
    claim_class TEXT NOT NULL DEFAULT 'retrospective_research'
        CHECK (claim_class = 'retrospective_research'),
    stress_type TEXT NOT NULL DEFAULT 'irradiation'
        CHECK (stress_type = 'irradiation'),
    target_type TEXT NOT NULL DEFAULT 'delta_vth_v'
        CHECK (target_type = 'delta_vth_v'),
    curve_family TEXT NOT NULL DEFAULT 'IdVg' CHECK (curve_family = 'IdVg'),
    reference_policy TEXT NOT NULL CHECK (reference_policy IN ('same_device', 'library_screening')),
    research_protocol_id TEXT NOT NULL,
    target_current_a DOUBLE PRECISION NOT NULL CHECK (target_current_a > 0 AND iv_damage_is_finite(target_current_a)),
    horizon_status TEXT NOT NULL DEFAULT 'unknown_or_heterogeneous'
        CHECK (horizon_status = 'unknown_or_heterogeneous'),
    source_cutoff TIMESTAMPTZ NOT NULL,
    source_query TEXT NOT NULL,
    source_code_sha TEXT NOT NULL,
    source_fingerprint CHAR(64) NOT NULL,
    pair_count INTEGER NOT NULL CHECK (pair_count >= 0),
    device_count INTEGER NOT NULL CHECK (device_count >= 0),
    campaign_count INTEGER NOT NULL CHECK (campaign_count >= 0),
    run_count INTEGER NOT NULL CHECK (run_count >= 0),
    extraction_audit JSONB NOT NULL DEFAULT '{}'::jsonb,
    limitations JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE iv_damage_research_curve_pairs (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES iv_damage_research_snapshots(id) ON DELETE RESTRICT,
    source_pair_id BIGINT NOT NULL,
    pair_key TEXT NOT NULL,
    pre_feature_id BIGINT NOT NULL,
    post_feature_id BIGINT NOT NULL,
    pre_metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE RESTRICT,
    post_metadata_id INTEGER NOT NULL REFERENCES baselines_metadata(id) ON DELETE RESTRICT,
    pre_point_hash CHAR(64) NOT NULL,
    post_point_hash CHAR(64) NOT NULL,
    pair_payload_hash CHAR(64) NOT NULL,
    physical_device_key TEXT NOT NULL,
    device_type TEXT NOT NULL,
    manufacturer TEXT,
    campaign_key TEXT,
    run_key TEXT,
    ion_species TEXT,
    beam_energy_mev DOUBLE PRECISION,
    let_surface DOUBLE PRECISION,
    range_um DOUBLE PRECISION,
    beam_type TEXT,
    fluence DOUBLE PRECISION,
    fluence_missing BOOLEAN NOT NULL,
    pre_vds_v DOUBLE PRECISION,
    post_vds_v DOUBLE PRECISION,
    protocol_compatible BOOLEAN NOT NULL,
    extraction_config JSONB NOT NULL,
    pre_vth_v DOUBLE PRECISION,
    post_vth_v DOUBLE PRECISION,
    observed_delta_vth_v DOUBLE PRECISION,
    extraction_diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
    pre_vg_min DOUBLE PRECISION,
    pre_vg_max DOUBLE PRECISION,
    post_vg_min DOUBLE PRECISION,
    post_vg_max DOUBLE PRECISION,
    pre_point_count INTEGER NOT NULL CHECK (pre_point_count >= 0),
    post_point_count INTEGER NOT NULL CHECK (post_point_count >= 0),
    common_grid_point_count INTEGER NOT NULL CHECK (common_grid_point_count >= 0),
    admission_status TEXT NOT NULL CHECK (admission_status IN ('admitted', 'excluded')),
    exclusion_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    horizon_status TEXT NOT NULL DEFAULT 'unknown_or_heterogeneous'
        CHECK (horizon_status = 'unknown_or_heterogeneous'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (snapshot_id, source_pair_id),
    UNIQUE (snapshot_id, pair_key),
    CHECK (fluence_missing = (fluence IS NULL)),
    CHECK (admission_status = 'excluded' OR (
        protocol_compatible AND pre_vth_v IS NOT NULL AND post_vth_v IS NOT NULL
        AND observed_delta_vth_v IS NOT NULL AND common_grid_point_count > 0
    ))
);

CREATE TABLE iv_damage_research_curve_pair_points (
    id BIGSERIAL PRIMARY KEY,
    curve_pair_id BIGINT NOT NULL REFERENCES iv_damage_research_curve_pairs(id) ON DELETE RESTRICT,
    curve_role TEXT NOT NULL CHECK (curve_role IN ('pre', 'post')),
    point_order INTEGER NOT NULL CHECK (point_order >= 0),
    source_point_index INTEGER NOT NULL CHECK (source_point_index >= 0),
    source_point_id BIGINT NOT NULL,
    v_gate_v DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(v_gate_v)),
    v_drain_v DOUBLE PRECISION CHECK (v_drain_v IS NULL OR iv_damage_is_finite(v_drain_v)),
    i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(i_drain_a)),
    point_hash CHAR(64) NOT NULL,
    UNIQUE (curve_pair_id, curve_role, point_order),
    UNIQUE (curve_pair_id, curve_role, source_point_id)
);

-- V2 relations are pipeline-owned rather than forward migrations. Production
-- has them before migration 046; bare architecture databases may not. Add the
-- restrictive lineage constraints whenever their source relations exist.
DO $$
BEGIN
    IF to_regclass('public.iv_physical_response_pairs') IS NOT NULL THEN
        ALTER TABLE iv_damage_research_curve_pairs
            ADD CONSTRAINT iv_damage_research_pair_source_fk
            FOREIGN KEY (source_pair_id) REFERENCES iv_physical_response_pairs(id)
            ON DELETE RESTRICT;
    END IF;
    IF to_regclass('public.iv_physical_curve_features') IS NOT NULL THEN
        ALTER TABLE iv_damage_research_curve_pairs
            ADD CONSTRAINT iv_damage_research_pre_feature_fk
            FOREIGN KEY (pre_feature_id) REFERENCES iv_physical_curve_features(id)
            ON DELETE RESTRICT;
        ALTER TABLE iv_damage_research_curve_pairs
            ADD CONSTRAINT iv_damage_research_post_feature_fk
            FOREIGN KEY (post_feature_id) REFERENCES iv_physical_curve_features(id)
            ON DELETE RESTRICT;
    END IF;
    IF to_regclass('public.baselines_measurements') IS NOT NULL THEN
        ALTER TABLE iv_damage_research_curve_pair_points
            ADD CONSTRAINT iv_damage_research_source_point_fk
            FOREIGN KEY (source_point_id) REFERENCES baselines_measurements(id)
            ON DELETE RESTRICT;
    END IF;
END
$$;

CREATE TABLE iv_damage_research_split_assignments (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES iv_damage_research_snapshots(id) ON DELETE RESTRICT,
    curve_pair_id BIGINT NOT NULL REFERENCES iv_damage_research_curve_pairs(id) ON DELETE RESTRICT,
    validation_scheme TEXT NOT NULL CHECK (validation_scheme IN ('leave_device', 'leave_run', 'leave_campaign')),
    fold_number INTEGER NOT NULL CHECK (fold_number >= 0),
    held_out_group_key TEXT NOT NULL,
    physical_device_key TEXT NOT NULL,
    assignment_hash CHAR(64) NOT NULL,
    UNIQUE (snapshot_id, curve_pair_id, validation_scheme),
    UNIQUE (snapshot_id, validation_scheme, assignment_hash)
);

CREATE TABLE iv_damage_research_model_runs (
    id BIGSERIAL PRIMARY KEY,
    run_version TEXT NOT NULL UNIQUE,
    snapshot_id BIGINT NOT NULL REFERENCES iv_damage_research_snapshots(id) ON DELETE RESTRICT,
    model_family TEXT NOT NULL CHECK (model_family IN ('baseline', 'scalar', 'hybrid_curve', 'direct_curve')),
    method TEXT NOT NULL CHECK (method IN ('zero_damage', 'v2_donor', 'huber', 'extra_trees', 'hybrid_huber', 'hybrid_extra_trees', 'ridge_residual', 'direct_functional')),
    validation_scheme TEXT NOT NULL CHECK (validation_scheme IN ('leave_device', 'leave_run', 'leave_campaign')),
    feature_mode TEXT NOT NULL CHECK (feature_mode IN ('physics_only', 'within_observed_condition')),
    feature_contract JSONB NOT NULL,
    estimator_config JSONB NOT NULL,
    random_seed INTEGER NOT NULL,
    artifact_path TEXT,
    artifact_checksum CHAR(64),
    development_status TEXT NOT NULL DEFAULT 'candidate'
        CHECK (development_status IN ('candidate', 'evaluated', 'preferred', 'failed', 'retired')),
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    limitations JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_code_sha TEXT NOT NULL,
    source_fingerprint CHAR(64) NOT NULL,
    error TEXT,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    CHECK ((artifact_path IS NULL) = (artifact_checksum IS NULL)),
    CHECK (development_status <> 'evaluated' OR completed_at IS NOT NULL)
);

CREATE TABLE iv_damage_research_fold_manifests (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_research_model_runs(id) ON DELETE RESTRICT,
    fold_number INTEGER NOT NULL CHECK (fold_number >= 0),
    held_out_group_key TEXT NOT NULL,
    training_device_keys JSONB NOT NULL CHECK (jsonb_typeof(training_device_keys) = 'array'),
    training_device_hash CHAR(64) NOT NULL,
    preprocessing_manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (model_run_id, fold_number, held_out_group_key)
);

CREATE TABLE iv_damage_research_scalar_predictions (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_research_model_runs(id) ON DELETE RESTRICT,
    curve_pair_id BIGINT NOT NULL REFERENCES iv_damage_research_curve_pairs(id) ON DELETE RESTRICT,
    fold_manifest_id BIGINT NOT NULL REFERENCES iv_damage_research_fold_manifests(id) ON DELETE RESTRICT,
    validation_scheme TEXT NOT NULL CHECK (validation_scheme IN ('leave_device', 'leave_run', 'leave_campaign')),
    fold_number INTEGER NOT NULL CHECK (fold_number >= 0),
    held_out_group_key TEXT NOT NULL,
    observed_delta_vth_v DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(observed_delta_vth_v)),
    predicted_delta_vth_v DOUBLE PRECISION CHECK (predicted_delta_vth_v IS NULL OR iv_damage_is_finite(predicted_delta_vth_v)),
    predicted_lower_v DOUBLE PRECISION,
    predicted_upper_v DOUBLE PRECISION,
    residual_v DOUBLE PRECISION,
    absolute_error_v DOUBLE PRECISION,
    support_status TEXT NOT NULL CHECK (support_status IN ('supported', 'abstained')),
    support_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    claim_class TEXT NOT NULL DEFAULT 'retrospective_research' CHECK (claim_class = 'retrospective_research'),
    prediction_context TEXT NOT NULL DEFAULT 'historical_out_of_fold' CHECK (prediction_context = 'historical_out_of_fold'),
    evidence_status TEXT NOT NULL DEFAULT 'exploratory' CHECK (evidence_status = 'exploratory'),
    decision_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (NOT decision_eligible),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_run_id, curve_pair_id),
    CHECK (support_status = 'abstained' OR predicted_delta_vth_v IS NOT NULL)
);

CREATE TABLE iv_damage_research_curve_predictions (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_research_model_runs(id) ON DELETE RESTRICT,
    scalar_prediction_id BIGINT NOT NULL REFERENCES iv_damage_research_scalar_predictions(id) ON DELETE RESTRICT,
    curve_pair_id BIGINT NOT NULL REFERENCES iv_damage_research_curve_pairs(id) ON DELETE RESTRICT,
    validation_scheme TEXT NOT NULL CHECK (validation_scheme IN ('leave_device', 'leave_run', 'leave_campaign')),
    fold_number INTEGER NOT NULL,
    held_out_group_key TEXT NOT NULL,
    scalar_shift_v DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(scalar_shift_v)),
    scalar_shift_source TEXT NOT NULL CHECK (scalar_shift_source = 'out_of_fold_predicted'),
    correction_applied BOOLEAN NOT NULL,
    correction_norm DOUBLE PRECISION,
    fallback_reason TEXT,
    support_status TEXT NOT NULL CHECK (support_status IN ('supported', 'fallback', 'abstained')),
    support_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::text[],
    mae_a DOUBLE PRECISION,
    max_abs_error_a DOUBLE PRECISION,
    normalized_rmse DOUBLE PRECISION,
    transformed_mae DOUBLE PRECISION,
    predicted_vth_error_v DOUBLE PRECISION,
    supported_voltage_fraction DOUBLE PRECISION CHECK (supported_voltage_fraction BETWEEN 0 AND 1),
    claim_class TEXT NOT NULL DEFAULT 'retrospective_research' CHECK (claim_class = 'retrospective_research'),
    prediction_context TEXT NOT NULL DEFAULT 'historical_out_of_fold' CHECK (prediction_context = 'historical_out_of_fold'),
    evidence_status TEXT NOT NULL DEFAULT 'exploratory' CHECK (evidence_status = 'exploratory'),
    decision_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (NOT decision_eligible),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_run_id, curve_pair_id),
    CHECK (correction_applied OR fallback_reason IS NOT NULL)
);

CREATE TABLE iv_damage_research_curve_prediction_points (
    id BIGSERIAL PRIMARY KEY,
    curve_prediction_id BIGINT NOT NULL REFERENCES iv_damage_research_curve_predictions(id) ON DELETE RESTRICT,
    point_order INTEGER NOT NULL CHECK (point_order >= 0),
    v_gate_v DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(v_gate_v)),
    series_name TEXT NOT NULL CHECK (series_name IN (
        'pre_measured', 'post_measured', 'zero_damage', 'v2_donor_projection',
        'huber_scalar_projection', 'extra_trees_scalar_projection',
        'hybrid_huber', 'hybrid_extra_trees', 'direct_functional',
        'oracle_shift_diagnostic', 'empirical_lower', 'empirical_upper'
    )),
    i_drain_a DOUBLE PRECISION NOT NULL CHECK (iv_damage_is_finite(i_drain_a)),
    truth_only BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (curve_prediction_id, point_order, series_name),
    CHECK ((series_name = 'post_measured') = truth_only)
);

CREATE TABLE iv_damage_research_metrics (
    id BIGSERIAL PRIMARY KEY,
    model_run_id BIGINT NOT NULL REFERENCES iv_damage_research_model_runs(id) ON DELETE RESTRICT,
    aggregation_level TEXT NOT NULL CHECK (aggregation_level IN ('pair', 'device_macro', 'run_macro', 'campaign_macro')),
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    supported_pairs INTEGER NOT NULL DEFAULT 0,
    supported_devices INTEGER NOT NULL DEFAULT 0,
    abstained_pairs INTEGER NOT NULL DEFAULT 0,
    denominator_note TEXT NOT NULL,
    UNIQUE (model_run_id, aggregation_level, metric_name)
);

CREATE FUNCTION iv_damage_research_guard_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END
$$;

DO $$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'iv_damage_research_snapshots', 'iv_damage_research_curve_pairs',
        'iv_damage_research_curve_pair_points', 'iv_damage_research_split_assignments',
        'iv_damage_research_fold_manifests', 'iv_damage_research_scalar_predictions',
        'iv_damage_research_curve_predictions', 'iv_damage_research_curve_prediction_points',
        'iv_damage_research_metrics'
    ] LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION iv_damage_research_guard_immutable()',
            relation_name || '_immutable_guard', relation_name
        );
    END LOOP;
END
$$;

CREATE FUNCTION iv_damage_research_guard_split_assignment()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE pair_snapshot BIGINT; pair_device TEXT;
BEGIN
    SELECT snapshot_id,physical_device_key INTO pair_snapshot,pair_device
      FROM iv_damage_research_curve_pairs WHERE id=NEW.curve_pair_id;
    IF pair_snapshot IS DISTINCT FROM NEW.snapshot_id THEN
        RAISE EXCEPTION 'split assignment snapshot does not match curve pair';
    END IF;
    IF pair_device IS DISTINCT FROM NEW.physical_device_key THEN
        RAISE EXCEPTION 'split assignment device does not match curve pair';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER iv_damage_research_split_assignment_guard
BEFORE INSERT ON iv_damage_research_split_assignments
FOR EACH ROW EXECUTE FUNCTION iv_damage_research_guard_split_assignment();


CREATE FUNCTION iv_damage_research_guard_model_run()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.development_status <> 'candidate' OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'research model runs must enter lifecycle as candidates';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'iv_damage_research_model_runs is append-only'; END IF;
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.run_version IS DISTINCT FROM OLD.run_version
       OR NEW.snapshot_id IS DISTINCT FROM OLD.snapshot_id OR NEW.model_family IS DISTINCT FROM OLD.model_family
       OR NEW.method IS DISTINCT FROM OLD.method OR NEW.validation_scheme IS DISTINCT FROM OLD.validation_scheme
       OR NEW.feature_mode IS DISTINCT FROM OLD.feature_mode OR NEW.feature_contract IS DISTINCT FROM OLD.feature_contract
       OR NEW.estimator_config IS DISTINCT FROM OLD.estimator_config OR NEW.random_seed IS DISTINCT FROM OLD.random_seed
       OR NEW.source_code_sha IS DISTINCT FROM OLD.source_code_sha OR NEW.source_fingerprint IS DISTINCT FROM OLD.source_fingerprint
       OR NEW.created_by IS DISTINCT FROM OLD.created_by OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'research model identity and configuration are immutable';
    END IF;
    IF OLD.development_status <> 'candidate' AND (
        NEW.artifact_path IS DISTINCT FROM OLD.artifact_path
        OR NEW.artifact_checksum IS DISTINCT FROM OLD.artifact_checksum
        OR NEW.metrics IS DISTINCT FROM OLD.metrics
        OR NEW.limitations IS DISTINCT FROM OLD.limitations
        OR NEW.error IS DISTINCT FROM OLD.error
        OR NEW.completed_at IS DISTINCT FROM OLD.completed_at
    ) THEN
        RAISE EXCEPTION 'completed research model artifact and evidence are immutable';
    END IF;
    IF NOT (NEW.development_status = OLD.development_status OR
        (OLD.development_status = 'candidate' AND NEW.development_status IN ('evaluated','failed')) OR
        (OLD.development_status = 'evaluated' AND NEW.development_status IN ('preferred','failed','retired')) OR
        (OLD.development_status = 'preferred' AND NEW.development_status IN ('failed','retired'))) THEN
        RAISE EXCEPTION 'invalid research model status transition: % -> %', OLD.development_status, NEW.development_status;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER iv_damage_research_model_run_guard BEFORE INSERT OR UPDATE OR DELETE ON iv_damage_research_model_runs
FOR EACH ROW EXECUTE FUNCTION iv_damage_research_guard_model_run();

CREATE FUNCTION iv_damage_research_guard_oof_prediction()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    held_device TEXT; pair_snapshot BIGINT; pair_observed DOUBLE PRECISION;
    training_devices JSONB; manifest_run BIGINT; manifest_fold INTEGER; manifest_group TEXT;
    model_snapshot BIGINT; model_scheme TEXT;
    split_fold INTEGER; split_group TEXT; split_device TEXT;
BEGIN
    SELECT physical_device_key,snapshot_id,observed_delta_vth_v
      INTO held_device,pair_snapshot,pair_observed
      FROM iv_damage_research_curve_pairs WHERE id=NEW.curve_pair_id;
    SELECT snapshot_id,validation_scheme INTO model_snapshot,model_scheme
      FROM iv_damage_research_model_runs WHERE id=NEW.model_run_id;
    SELECT training_device_keys,model_run_id,fold_number,held_out_group_key
      INTO training_devices,manifest_run,manifest_fold,manifest_group
      FROM iv_damage_research_fold_manifests WHERE id=NEW.fold_manifest_id;
    SELECT fold_number,held_out_group_key,physical_device_key
      INTO split_fold,split_group,split_device
      FROM iv_damage_research_split_assignments
      WHERE snapshot_id=pair_snapshot AND curve_pair_id=NEW.curve_pair_id
        AND validation_scheme=NEW.validation_scheme;
    IF model_snapshot IS DISTINCT FROM pair_snapshot OR model_scheme IS DISTINCT FROM NEW.validation_scheme THEN
        RAISE EXCEPTION 'scalar prediction model, pair, and validation scheme are inconsistent';
    END IF;
    IF split_fold IS NULL OR split_fold IS DISTINCT FROM NEW.fold_number
       OR split_group IS DISTINCT FROM NEW.held_out_group_key
       OR split_device IS DISTINCT FROM held_device THEN
        RAISE EXCEPTION 'scalar prediction does not match its frozen split assignment';
    END IF;
    IF pair_observed IS DISTINCT FROM NEW.observed_delta_vth_v THEN
        RAISE EXCEPTION 'scalar prediction truth does not match frozen pair truth';
    END IF;
    IF manifest_run IS DISTINCT FROM NEW.model_run_id OR manifest_fold IS DISTINCT FROM NEW.fold_number
       OR manifest_group IS DISTINCT FROM NEW.held_out_group_key THEN
        RAISE EXCEPTION 'prediction does not match its fold manifest';
    END IF;
    IF training_devices ? held_device THEN
        RAISE EXCEPTION 'held-out physical device appears in training manifest: %', held_device;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER iv_damage_research_oof_prediction_guard BEFORE INSERT ON iv_damage_research_scalar_predictions
FOR EACH ROW EXECUTE FUNCTION iv_damage_research_guard_oof_prediction();

CREATE FUNCTION iv_damage_research_guard_curve_prediction()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    pair_snapshot BIGINT; model_snapshot BIGINT; model_scheme TEXT;
    scalar_pair BIGINT; scalar_scheme TEXT; scalar_fold INTEGER; scalar_group TEXT;
    scalar_shift DOUBLE PRECISION; scalar_snapshot BIGINT;
BEGIN
    SELECT snapshot_id INTO pair_snapshot FROM iv_damage_research_curve_pairs WHERE id=NEW.curve_pair_id;
    SELECT snapshot_id,validation_scheme INTO model_snapshot,model_scheme
      FROM iv_damage_research_model_runs WHERE id=NEW.model_run_id;
    SELECT scalar.curve_pair_id,scalar.validation_scheme,scalar.fold_number,
           scalar.held_out_group_key,scalar.predicted_delta_vth_v,scalar_model.snapshot_id
      INTO scalar_pair,scalar_scheme,scalar_fold,scalar_group,scalar_shift,scalar_snapshot
      FROM iv_damage_research_scalar_predictions scalar
      JOIN iv_damage_research_model_runs scalar_model ON scalar_model.id=scalar.model_run_id
      WHERE scalar.id=NEW.scalar_prediction_id;
    IF model_snapshot IS DISTINCT FROM pair_snapshot OR scalar_snapshot IS DISTINCT FROM pair_snapshot THEN
        RAISE EXCEPTION 'curve prediction model, scalar prediction, and pair snapshots differ';
    END IF;
    IF scalar_pair IS DISTINCT FROM NEW.curve_pair_id
       OR model_scheme IS DISTINCT FROM NEW.validation_scheme
       OR scalar_scheme IS DISTINCT FROM NEW.validation_scheme
       OR scalar_fold IS DISTINCT FROM NEW.fold_number
       OR scalar_group IS DISTINCT FROM NEW.held_out_group_key THEN
        RAISE EXCEPTION 'curve prediction does not match scalar out-of-fold provenance';
    END IF;
    IF scalar_shift IS NULL OR scalar_shift IS DISTINCT FROM NEW.scalar_shift_v THEN
        RAISE EXCEPTION 'curve prediction shift does not match scalar prediction';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER iv_damage_research_curve_prediction_guard
BEFORE INSERT ON iv_damage_research_curve_predictions
FOR EACH ROW EXECUTE FUNCTION iv_damage_research_guard_curve_prediction();

CREATE VIEW iv_damage_research_status_view AS
SELECT snapshot.snapshot_version, snapshot.snapshot_hash, snapshot.research_protocol_id,
       snapshot.target_current_a, snapshot.claim_class, snapshot.horizon_status,
       snapshot.pair_count, snapshot.device_count, snapshot.campaign_count, snapshot.run_count,
       snapshot.limitations, count(model.id) AS model_runs,
       count(*) FILTER (WHERE model.development_status = 'preferred') AS preferred_models,
       false AS decision_eligible
FROM iv_damage_research_snapshots snapshot
LEFT JOIN iv_damage_research_model_runs model ON model.snapshot_id = snapshot.id
GROUP BY snapshot.id;

CREATE VIEW iv_damage_research_cohort_view AS
SELECT snapshot.snapshot_version, pair.pair_key, pair.physical_device_key, pair.device_type,
       pair.manufacturer, pair.campaign_key, pair.run_key, pair.ion_species,
       pair.beam_energy_mev, pair.let_surface, pair.range_um, pair.beam_type,
       pair.fluence, pair.fluence_missing, pair.pre_vds_v, pair.post_vds_v,
       pair.pre_vth_v, pair.post_vth_v, pair.observed_delta_vth_v,
       pair.admission_status, pair.exclusion_reasons, pair.horizon_status,
       snapshot.claim_class, false AS decision_eligible
FROM iv_damage_research_curve_pairs pair
JOIN iv_damage_research_snapshots snapshot ON snapshot.id = pair.snapshot_id;

CREATE VIEW iv_damage_research_scalar_validation_view AS
SELECT snapshot.snapshot_version, model.run_version AS model_version, model.method,
       prediction.validation_scheme, prediction.fold_number, prediction.held_out_group_key,
       pair.pair_key, pair.physical_device_key, pair.device_type, pair.campaign_key,
       pair.run_key, pair.ion_species, prediction.observed_delta_vth_v,
       prediction.predicted_delta_vth_v, prediction.residual_v, prediction.absolute_error_v,
       prediction.support_status, prediction.support_reasons, prediction.claim_class,
       prediction.prediction_context, prediction.evidence_status, prediction.decision_eligible,
       snapshot.horizon_status
FROM iv_damage_research_scalar_predictions prediction
JOIN iv_damage_research_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_research_curve_pairs pair ON pair.id = prediction.curve_pair_id
JOIN iv_damage_research_snapshots snapshot ON snapshot.id = pair.snapshot_id;

CREATE VIEW iv_damage_research_curve_plot_view AS
SELECT snapshot.snapshot_version, model.run_version AS model_version, model.method,
       prediction.validation_scheme, prediction.fold_number, prediction.held_out_group_key,
       pair.pair_key, pair.physical_device_key, pair.device_type, pair.campaign_key, pair.run_key,
       point.point_order, point.v_gate_v, point.series_name, point.i_drain_a,
       point.truth_only, prediction.correction_applied, prediction.fallback_reason,
       prediction.support_status, prediction.claim_class, prediction.prediction_context,
       prediction.evidence_status, prediction.decision_eligible, snapshot.horizon_status
FROM iv_damage_research_curve_prediction_points point
JOIN iv_damage_research_curve_predictions prediction ON prediction.id = point.curve_prediction_id
JOIN iv_damage_research_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_research_curve_pairs pair ON pair.id = prediction.curve_pair_id
JOIN iv_damage_research_snapshots snapshot ON snapshot.id = pair.snapshot_id;

CREATE VIEW iv_damage_research_curve_metrics_view AS
SELECT snapshot.snapshot_version, model.run_version AS model_version, model.method,
       prediction.validation_scheme, prediction.fold_number, prediction.held_out_group_key,
       pair.pair_key, pair.physical_device_key, pair.device_type, pair.campaign_key, pair.run_key,
       prediction.mae_a, prediction.max_abs_error_a, prediction.normalized_rmse,
       prediction.transformed_mae, prediction.predicted_vth_error_v,
       prediction.supported_voltage_fraction, prediction.correction_applied,
       prediction.correction_norm, prediction.fallback_reason, prediction.support_status,
       prediction.support_reasons, prediction.claim_class, prediction.prediction_context,
       prediction.evidence_status, prediction.decision_eligible, snapshot.horizon_status
FROM iv_damage_research_curve_predictions prediction
JOIN iv_damage_research_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_research_curve_pairs pair ON pair.id = prediction.curve_pair_id
JOIN iv_damage_research_snapshots snapshot ON snapshot.id = pair.snapshot_id;

CREATE VIEW iv_damage_research_residual_diagnostics_view AS
SELECT snapshot.snapshot_version, model.run_version AS model_version, model.method,
       model.validation_scheme, model.estimator_config->'residual_pca_explained_variance' AS pca_explained_variance,
       prediction.held_out_group_key, pair.pair_key, pair.physical_device_key,
       prediction.correction_norm, prediction.correction_applied, prediction.fallback_reason,
       prediction.support_status, prediction.claim_class, prediction.prediction_context,
       prediction.evidence_status, prediction.decision_eligible, snapshot.horizon_status
FROM iv_damage_research_curve_predictions prediction
JOIN iv_damage_research_model_runs model ON model.id = prediction.model_run_id
JOIN iv_damage_research_curve_pairs pair ON pair.id = prediction.curve_pair_id
JOIN iv_damage_research_snapshots snapshot ON snapshot.id = pair.snapshot_id;

CREATE VIEW iv_damage_research_limitations_view AS
SELECT snapshot.snapshot_version, limitation.key AS limitation_key,
       limitation.value AS limitation_value, snapshot.claim_class,
       snapshot.horizon_status, false AS decision_eligible
FROM iv_damage_research_snapshots snapshot
CROSS JOIN LATERAL jsonb_each(snapshot.limitations) limitation;

CREATE INDEX iv_damage_research_pair_snapshot_idx ON iv_damage_research_curve_pairs(snapshot_id, admission_status);
CREATE INDEX iv_damage_research_split_idx ON iv_damage_research_split_assignments(snapshot_id, validation_scheme, fold_number);
CREATE INDEX iv_damage_research_scalar_pair_idx ON iv_damage_research_scalar_predictions(curve_pair_id);
CREATE INDEX iv_damage_research_curve_pair_idx ON iv_damage_research_curve_predictions(curve_pair_id);
CREATE INDEX iv_damage_research_curve_points_plot_idx ON iv_damage_research_curve_prediction_points(curve_prediction_id, series_name, point_order);
