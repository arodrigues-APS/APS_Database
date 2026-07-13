import requests
import pytest

from aps.superset.superset_api import (
    SupersetAuthenticationError,
    SupersetClient,
    SupersetResponseError,
    SupersetTimeouts,
    SupersetTransportError,
    get_session,
)


class Response:
    def __init__(self, payload=None, *, status_code=200, text=""):
        self._payload = payload or {}
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = text

    def json(self):
        return self._payload


class QueueSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.headers = {}
        self.calls = []

    def _request(self, method, endpoint, **kwargs):
        self.calls.append((method, endpoint, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def get(self, endpoint, **kwargs):
        return self._request("GET", endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        return self._request("POST", endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        return self._request("PUT", endpoint, **kwargs)

    def close(self):
        pass


def test_safe_get_retries_once_and_applies_connect_read_timeouts():
    transport = QueueSession(
        [requests.Timeout("temporary"), Response({"result": []})]
    )
    client = SupersetClient(
        transport,
        "http://superset",
        timeouts=SupersetTimeouts(2.0, 9.0),
        retry_delay_seconds=0,
    )

    response = client.get("/api/v1/dashboard/")

    assert response.ok
    assert len(transport.calls) == 2
    assert transport.calls[0][2]["timeout"] == (2.0, 9.0)


def test_mutating_request_is_never_retried_after_transport_timeout():
    transport = QueueSession([requests.Timeout("unknown mutation outcome")])
    client = SupersetClient(transport, "http://superset", retry_delay_seconds=0)

    with pytest.raises(SupersetTransportError, match="POST"):
        client.post("/api/v1/chart/", json={})

    assert len(transport.calls) == 1


def test_non_success_response_raises_typed_error():
    transport = QueueSession([Response(status_code=500, text="broken")])
    client = SupersetClient(transport, "http://superset", retry_delay_seconds=0)

    with pytest.raises(SupersetResponseError, match="HTTP 500"):
        client.put("/api/v1/chart/12", json={})


def test_authentication_requires_form_jwt_and_api_csrf_tokens():
    transport = QueueSession(
        [
            Response(text='<input name="csrf_token" value="form-token">'),
            Response(),
            Response({"access_token": "jwt-token"}),
            Response({"result": "api-csrf"}),
        ]
    )

    client = get_session(
        "http://superset",
        "admin",
        "secret",
        session_factory=lambda: transport,
    )

    assert client.headers["Authorization"] == "Bearer jwt-token"
    assert client.headers["X-CSRFToken"] == "api-csrf"
    assert all(call[2]["timeout"] == (5.0, 60.0) for call in transport.calls)


def test_authentication_fails_closed_when_jwt_is_missing():
    transport = QueueSession(
        [
            Response(text='<input name="csrf_token" value="form-token">'),
            Response(),
            Response({}),
        ]
    )

    with pytest.raises(SupersetAuthenticationError, match="access token"):
        get_session(
            "http://superset",
            "admin",
            "secret",
            session_factory=lambda: transport,
        )
