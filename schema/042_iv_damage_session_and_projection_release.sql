-- Complete stress-session audit and make projection certification a real
-- claim gate rather than a method-approval flag.

ALTER TABLE iv_damage_stress_sessions
    ADD COLUMN review_reason TEXT,
    ADD CONSTRAINT iv_damage_session_manual_review_reason_ck CHECK (
        identity_source <> 'manual_review'
        OR (review_reason IS NOT NULL AND btrim(review_reason) <> '')
    );

CREATE VIEW iv_damage_curve_projection_release_gate_view AS
SELECT
    method.id AS projection_method_id,
    method.method_version,
    method.projection_kind,
    method.target_type,
    method.curve_family,
    snapshot.domain_summary->>'stress_type' AS stress_type,
    method.approved AS method_approved,
    certification.dataset_snapshot_id,
    certification.passed AS external_certification_passed,
    certification.metrics,
    certification.gate_checks,
    certification.certified_by,
    certification.certified_at
FROM iv_damage_curve_projection_methods method
LEFT JOIN iv_damage_curve_projection_certifications certification
  ON certification.projection_method_id = method.id
LEFT JOIN iv_damage_dataset_snapshots snapshot
  ON snapshot.id = certification.dataset_snapshot_id;

COMMENT ON VIEW iv_damage_curve_projection_release_gate_view IS
    'Projection method approval and curve-level external certification are independent gates.';
