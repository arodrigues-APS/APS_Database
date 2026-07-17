"""Read-only downstream adapter for released V3 damage predictions."""

from __future__ import annotations


CANONICAL_PREDICTION_VIEW = "iv_damage_decision_eligible_prediction_view"
EQUIVALENCE_INPUT_VIEW = "iv_damage_equivalence_input_view"
EQUIVALENCE_FINGERPRINT_VIEW = "iv_damage_equivalence_fingerprint_view"


def load_equivalence_fingerprints(conn, *, device_type: str | None = None):
    """Load only fingerprints that passed the canonical database boundary."""

    sql = f"""
        SELECT model_run_id, model_version, algorithm, device_type,
               ion_species, beam_energy_mev, let_surface, range_um,
               fluence_or_dose, dvth_v, dvth_lower_v, dvth_upper_v,
               dvth_prediction_count, drdson_mohm, drdson_lower_mohm,
               drdson_upper_mohm, drdson_prediction_count,
               independent_physical_devices, prediction_count,
               active_release_since, latest_prediction_at,
               prediction_evidence_basis
        FROM {EQUIVALENCE_FINGERPRINT_VIEW}
    """
    parameters: list[object] = []
    if device_type:
        sql += " WHERE device_type = %s"
        parameters.append(device_type)
    sql += " ORDER BY device_type, ion_species, beam_energy_mev, let_surface"
    with conn.cursor() as cursor:
        cursor.execute(sql, parameters)
        columns = [item[0] for item in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
