-- One prospectively acquired response is valid truth for every model that made
-- a prediction before that acquisition.  Keep one immutable outcome per exact
-- prediction, not one outcome per request/response globally.

ALTER TABLE iv_damage_prediction_outcomes
    DROP CONSTRAINT iv_damage_prediction_outcomes_request_id_key,
    DROP CONSTRAINT iv_damage_prediction_outcomes_response_unit_id_key;

CREATE INDEX iv_damage_outcomes_request_idx
    ON iv_damage_prediction_outcomes (request_id, matched_at);
CREATE INDEX iv_damage_outcomes_response_idx
    ON iv_damage_prediction_outcomes (response_unit_id, matched_at);

COMMENT ON CONSTRAINT iv_damage_outcome_prediction_uq
    ON iv_damage_prediction_outcomes IS
    'One immutable evaluation per exact prediction; shared prospective truth may evaluate shadow and decision models.';
