-- Trigger helper consumed by migration 035.  The filename deliberately sorts
-- after 034_iv_damage_hardening.sql and conforms to the migration-runner name
-- contract.

CREATE FUNCTION iv_damage_reject_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is immutable scientific evidence', TG_TABLE_NAME;
END
$$;
