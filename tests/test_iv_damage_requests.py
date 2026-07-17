from aps.ml.iv_damage_repository import PredictionRequest, request_key
from aps.ml.iv_damage_requests import submit_prediction_request


class Cursor:
    def __init__(self, inserted):
        self.inserted = inserted
        self.calls = []
        self.fetches = [(44,)] if inserted else [None, (44,)]

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchone(self):
        return self.fetches.pop(0)

    def close(self):
        pass


class Connection:
    def __init__(self, inserted=True):
        self.cursor_instance = Cursor(inserted)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def prospective():
    return PredictionRequest(
        physical_device_key="device-1", device_type="C2M", manufacturer="Wolfspeed",
        measurement_protocol_id="protocol-v1", stress_type="irradiation",
        target_type="delta_vth_v", pre_value=3.0, pre_uncertainty=0.01,
        reference_policy="same_device",
        stress_features={"ion_species": "Xe", "beam_energy_mev": 100},
        request_source="unit-test", requested_by="tester",
    )


def test_request_hash_includes_manufacturer_and_submission_is_idempotent():
    request = prospective()
    changed = PredictionRequest(**{**request.__dict__, "manufacturer": "Other"})
    assert request_key(request) != request_key(changed)

    inserted = submit_prediction_request(Connection(True), request)
    assert inserted.request_id == 44
    assert inserted.created

    connection = Connection(False)
    replay = submit_prediction_request(connection, request)
    assert replay.request_id == 44
    assert not replay.created
    assert len(connection.cursor_instance.calls) == 2
    assert connection.commits == 1
