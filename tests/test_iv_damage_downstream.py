from aps.ml.iv_damage_downstream import (
    CANONICAL_PREDICTION_VIEW,
    EQUIVALENCE_FINGERPRINT_VIEW,
    load_equivalence_fingerprints,
)


class Cursor:
    description = [("model_run_id",), ("device_type",)]

    def __init__(self):
        self.sql = None
        self.parameters = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, parameters):
        self.sql = sql
        self.parameters = parameters

    def fetchall(self):
        return [(7, "C2M")]


class Connection:
    def __init__(self):
        self.last_cursor = Cursor()

    def cursor(self):
        return self.last_cursor


def test_downstream_loader_reads_v3_adapter_and_parameterizes_device():
    connection = Connection()
    rows = load_equivalence_fingerprints(connection, device_type="C2M")
    assert rows == [{"model_run_id": 7, "device_type": "C2M"}]
    assert f"FROM {EQUIVALENCE_FINGERPRINT_VIEW}" in connection.last_cursor.sql
    assert CANONICAL_PREDICTION_VIEW == "iv_damage_decision_eligible_prediction_view"
    assert "iv_physical" not in connection.last_cursor.sql
    assert connection.last_cursor.parameters == ["C2M"]
