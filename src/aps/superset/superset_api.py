"""
Shared Apache Superset REST API helpers for APS Database dashboard scripts.

Extracted from create_baselines_dashboard.py, create_sc_dashboard.py, and
create_baselines_dashboard_device_library.py to eliminate duplication.

All functions use db_config for connection defaults.
"""

import json
import re
import time
from dataclasses import dataclass
from typing import Any

import requests

from aps.config import get_settings
from aps.db_config import SUPERSET_URL, SUPERSET_USER, SUPERSET_PASS


class SupersetApiError(RuntimeError):
    """Base error for a Superset transport or API contract failure."""


class SupersetTransportError(SupersetApiError):
    """Raised when an HTTP request could not receive a response."""


class SupersetResponseError(SupersetApiError):
    """Raised when Superset returns an unexpected non-success response."""


class SupersetAuthenticationError(SupersetApiError):
    """Raised when login cannot establish both required API credentials."""


@dataclass(frozen=True)
class SupersetTimeouts:
    """Connect/read timeout values applied to every Superset HTTP request."""

    connect_seconds: float = 5.0
    read_seconds: float = 60.0

    def as_requests_timeout(self) -> tuple[float, float]:
        return (self.connect_seconds, self.read_seconds)


class SupersetClient:
    """Strict transport wrapper around a requests-compatible session.

    GET requests receive one bounded retry for transient transport or gateway
    failures. Mutating requests are never retried automatically because a
    timeout may occur after Superset has applied a change.
    """

    def __init__(
        self,
        session,
        superset_url: str,
        *,
        timeouts: SupersetTimeouts | None = None,
        safe_retries: int = 1,
        retry_delay_seconds: float = 0.1,
    ):
        self._session = session
        self._superset_url = superset_url.rstrip("/")
        self.timeouts = timeouts or SupersetTimeouts()
        self.safe_retries = max(0, safe_retries)
        self.retry_delay_seconds = max(0.0, retry_delay_seconds)

    @property
    def headers(self):
        return self._session.headers

    def _absolute_url(self, url: str) -> str:
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"{self._superset_url}/{url.lstrip('/')}"

    def request(
        self,
        method: str,
        url: str,
        *,
        allowed_status: tuple[int, ...] = (),
        **kwargs: Any,
    ):
        method = method.upper()
        endpoint = self._absolute_url(url)
        retry_count = self.safe_retries if method == "GET" else 0
        request = getattr(self._session, method.lower())
        for attempt in range(retry_count + 1):
            try:
                response = request(
                    endpoint,
                    timeout=self.timeouts.as_requests_timeout(),
                    **kwargs,
                )
            except requests.RequestException as exc:
                if attempt < retry_count:
                    if self.retry_delay_seconds:
                        time.sleep(self.retry_delay_seconds * (attempt + 1))
                    continue
                raise SupersetTransportError(
                    f"{method} {endpoint} failed before receiving a response: {exc}"
                ) from exc

            status_code = int(getattr(response, "status_code", 0))
            if status_code in {502, 503, 504} and attempt < retry_count:
                if self.retry_delay_seconds:
                    time.sleep(self.retry_delay_seconds * (attempt + 1))
                continue
            if not response.ok and status_code not in allowed_status:
                text = str(getattr(response, "text", ""))[:500]
                raise SupersetResponseError(
                    f"{method} {endpoint} returned HTTP {status_code}: {text}"
                )
            return response
        raise AssertionError("Superset request loop exited without a response")

    def get(self, url: str, **kwargs: Any):
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any):
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any):
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any):
        return self.request("DELETE", url, **kwargs)

    def close(self) -> None:
        self._session.close()


def _request(session, method: str, endpoint: str, *, allowed_status=(), **kwargs):
    """Send a strict request while retaining compatibility with test doubles."""
    if isinstance(session, SupersetClient):
        return session.request(
            method,
            endpoint,
            allowed_status=tuple(allowed_status),
            **kwargs,
        )
    response = getattr(session, method.lower())(endpoint, **kwargs)
    status_code = int(getattr(response, "status_code", 0))
    if not response.ok and status_code not in allowed_status:
        text = str(getattr(response, "text", ""))[:500]
        raise SupersetResponseError(
            f"{method.upper()} {endpoint} returned HTTP {status_code}: {text}"
        )
    return response


# ── Authentication ──────────────────────────────────────────────────────────

def get_session(
    superset_url=None,
    username=None,
    password=None,
    *,
    session_factory=requests.Session,
):
    """Authenticate and return a strict, timeout-aware API client."""
    if superset_url is None or username is None or password is None:
        settings = get_settings()
        configured_url, configured_user, configured_password = (
            settings.superset_credentials()
        )
        url = superset_url or configured_url
        user = username or configured_user
        pw = password or configured_password
        timeouts = SupersetTimeouts(
            settings.superset_connect_timeout_seconds,
            settings.superset_read_timeout_seconds,
        )
    else:
        url = superset_url or SUPERSET_URL
        user = username or SUPERSET_USER
        pw = password or SUPERSET_PASS
        timeouts = SupersetTimeouts()

    session = SupersetClient(session_factory(), url, timeouts=timeouts)

    login_page = session.get(f"{url}/login/")
    match = re.search(
        r'name="csrf_token"[^>]*value="([^"]+)"',
        login_page.text,
    )
    if not match:
        raise SupersetAuthenticationError(
            "could not find CSRF token on Superset login page"
        )
    session.post(
        f"{url}/login/",
        data={
            "username": user,
            "password": pw,
            "csrf_token": match.group(1),
        },
        allow_redirects=True,
    )

    token_response = session.post(
        f"{url}/api/v1/security/login",
        json={
            "username": user,
            "password": pw,
            "provider": "db",
            "refresh": True,
        },
    )
    access_token = token_response.json().get("access_token")
    if not access_token:
        raise SupersetAuthenticationError(
            "Superset login did not return an access token"
        )
    session.headers["Authorization"] = f"Bearer {access_token}"

    csrf_response = session.get(f"{url}/api/v1/security/csrf_token/")
    csrf = csrf_response.json().get("result", "")
    if not csrf:
        raise SupersetAuthenticationError(
            "Superset did not return an API CSRF token"
        )
    session.headers["X-CSRFToken"] = csrf
    session.headers["Referer"] = url
    session.headers["Content-Type"] = "application/json"
    return session


def _url(session):
    """Get the Superset URL from the session (set by get_session)."""
    return getattr(session, '_superset_url', SUPERSET_URL)


# ── Database Discovery ──────────────────────────────────────────────────────

def find_database(session):
    """Find the database connection for the mosfets DB."""
    url = _url(session)
    resp = _request(
        session,
        "get",
        f"{url}/api/v1/database/",
        params={"q": json.dumps({"page_size": 100})},
    )
    for db in resp.json()["result"]:
        name = db.get("database_name", "").lower()
        if "mosfet" in name or "postgresql" in name or "aps" in name:
            print(f"  Found database: {db['database_name']} (id={db['id']})")
            return db["id"]
    for db in resp.json()["result"]:
        detail = _request(
            session,
            "get",
            f"{url}/api/v1/database/{db['id']}",
        ).json()
        uri = detail.get("result", {}).get("sqlalchemy_uri", "")
        if "mosfets" in uri or "postgresqlv2" in uri or "5435" in uri:
            print(f"  Found database by URI: {db['database_name']} (id={db['id']})")
            return db["id"]
    raise SupersetApiError(
        "could not find an APS/mosfets database in the Superset catalog"
    )


# ── Dataset Management ──────────────────────────────────────────────────────

def find_or_create_dataset(session, db_id, table_name, schema="public"):
    """Find or create a dataset for a given table/view."""
    url = _url(session)
    resp = _request(
        session,
        "get",
        f"{url}/api/v1/dataset/",
        params={"q": json.dumps({
            "filters": [{"col": "table_name", "opr": "eq", "value": table_name}],
            "page_size": 100,
        })},
    )
    resp.raise_for_status()
    for ds in resp.json()["result"]:
        if ds.get("table_name") == table_name:
            print(f"  Dataset '{table_name}' exists (id={ds['id']})")
            return ds["id"]

    resp = _request(
        session,
        "post",
        f"{url}/api/v1/dataset/",
        json={"database": db_id, "table_name": table_name, "schema": schema},
    )
    ds_id = resp.json()["id"]
    print(f"  Created dataset '{table_name}' (id={ds_id})")
    return ds_id


def refresh_dataset_columns(session, ds_id):
    """Refresh dataset columns and clear cached column statistics."""
    url = _url(session)
    _request(
        session,
        "put",
        f"{url}/api/v1/dataset/{ds_id}/refresh",
        json={},
    )
    _request(session, "get", f"{url}/api/v1/dataset/{ds_id}")
    _request(session, "put", f"{url}/api/v1/dataset/{ds_id}", json={})
    _request(
        session,
        "put",
        f"{url}/api/v1/dataset/{ds_id}/refresh",
        json={},
    )


# ── Chart Management ────────────────────────────────────────────────────────

def create_chart(session, name, datasource_id, viz_type, params, description=None):
    """Create or update a chart. Returns (chart_id, chart_uuid)."""
    url = _url(session)
    resp = _request(
        session,
        "get",
        f"{url}/api/v1/chart/",
        params={"q": json.dumps({
            "filters": [{"col": "slice_name", "opr": "eq", "value": name}],
            "page_size": 100,
        })},
    )
    for chart in resp.json()["result"]:
        if chart.get("slice_name") == name:
            payload = {
                "params": json.dumps(params),
                "viz_type": viz_type,
                "datasource_id": datasource_id,
                "datasource_type": "table",
            }
            if description is not None:
                payload["description"] = description
            _request(
                session,
                "put",
                f"{url}/api/v1/chart/{chart['id']}",
                json=payload,
            )
            detail = _request(
                session,
                "get",
                f"{url}/api/v1/chart/{chart['id']}",
            ).json()
            uid = detail.get("result", {}).get("uuid")
            if not uid:
                raise SupersetApiError(
                    f"chart {chart['id']} update succeeded but UUID verification failed"
                )
            print(f"  Chart '{name}' updated (id={chart['id']})")
            return chart["id"], uid

    payload = {
        "slice_name": name,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    if description is not None:
        payload["description"] = description
    resp = _request(session, "post", f"{url}/api/v1/chart/", json=payload)
    chart_id = resp.json()["id"]
    detail = _request(
        session,
        "get",
        f"{url}/api/v1/chart/{chart_id}",
    ).json()
    real_uuid = detail.get("result", {}).get("uuid")
    if not real_uuid:
        raise SupersetApiError(
            f"chart {chart_id} creation succeeded but UUID verification failed"
        )
    print(f"  Created chart '{name}' (id={chart_id})")
    return chart_id, real_uuid


# ── Dashboard Management ────────────────────────────────────────────────────

PUBLIC_DASHBOARD_ROLE = "Public"


def _result_items(payload):
    result = payload.get("result", payload) if isinstance(payload, dict) else payload
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for key in ("data", "result", "roles"):
            value = result.get(key)
            if isinstance(value, list):
                return value
        return [result]
    return []


def _item_id(item):
    if isinstance(item, int):
        return item
    if isinstance(item, str) and item.isdigit():
        return int(item)
    if not isinstance(item, dict):
        return None
    for key in ("id", "value", "pk"):
        value = item.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _find_role_id(session, role_name=PUBLIC_DASHBOARD_ROLE):
    """Return a role id when the Superset API exposes role choices."""
    url = _url(session)
    filters = {
        "filters": [{"col": "name", "opr": "eq", "value": role_name}],
        "page_size": 100,
    }
    lookups = (
        (f"{url}/api/v1/dashboard/related/roles", {"q": json.dumps(filters)}),
        (f"{url}/api/v1/security/roles/search/", {"q": json.dumps(filters)}),
    )
    for endpoint, params in lookups:
        resp = _request(
            session,
            "get",
            endpoint,
            allowed_status=(404, 405),
            params=params,
        )
        if not resp.ok:
            continue
        for item in _result_items(resp.json()):
            if not isinstance(item, dict):
                continue
            names = {item.get("name"), item.get("text"), item.get("label")}
            if role_name in names:
                role_id = _item_id(item)
                if role_id is not None:
                    return role_id
    return None


def _dashboard_role_ids(session, dashboard_id):
    url = _url(session)
    resp = _request(
        session,
        "get",
        f"{url}/api/v1/dashboard/{dashboard_id}",
    )
    result = resp.json().get("result", {})
    return [
        role_id
        for role_id in (_item_id(role) for role in result.get("roles", []))
        if role_id is not None
    ]


def _public_dashboard_role_ids(session, dashboard_id=None):
    role_ids = set(_dashboard_role_ids(session, dashboard_id) if dashboard_id else [])
    public_role_id = _find_role_id(session)
    if public_role_id is None:
        print(
            "  WARNING: Public role id not available through the API; "
            "Superset startup sync will assign dashboard access after restart"
        )
    else:
        role_ids.add(public_role_id)
    return sorted(role_ids)


def _save_dashboard(session, method, endpoint, payload):
    resp = _request(
        session,
        method,
        endpoint,
        allowed_status=(400, 403, 422),
        json=payload,
    )
    if resp.ok:
        return resp
    if "roles" not in payload or resp.status_code not in (400, 403, 422):
        raise SupersetResponseError(
            f"{method.upper()} {endpoint} returned HTTP {resp.status_code}: "
            f"{resp.text[:500]}"
        )

    fallback = dict(payload)
    fallback.pop("roles", None)
    retry = _request(session, method, endpoint, json=fallback)
    print(
        "  WARNING: dashboard saved without immediate Public role "
        "assignment; Superset startup sync must repair it"
    )
    return retry


def create_or_update_dashboard(session, title, position_json, json_metadata,
                               slug):
    """Create or update a dashboard by slug. Returns dashboard id."""
    url = _url(session)
    resp = _request(
        session,
        "get",
        f"{url}/api/v1/dashboard/",
        params={"q": json.dumps({
            "filters": [{"col": "slug", "opr": "eq", "value": slug}],
            "page_size": 10,
        })},
    )
    existing_id = None
    for dash in resp.json()["result"]:
        if dash.get("slug") == slug:
            existing_id = dash["id"]
            break

    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
        "position_json": json.dumps(position_json),
        "json_metadata": json.dumps(json_metadata),
    }
    role_ids = _public_dashboard_role_ids(session, existing_id)
    if role_ids:
        payload["roles"] = role_ids

    if existing_id:
        resp = _save_dashboard(
            session, "put", f"{url}/api/v1/dashboard/{existing_id}", payload
        )
        print(f"  Updated dashboard (id={existing_id})")
        return existing_id

    resp = _save_dashboard(session, "post", f"{url}/api/v1/dashboard/", payload)
    dash_id = resp.json()["id"]
    print(f"  Created dashboard (id={dash_id})")
    return dash_id


def build_json_metadata(chart_ids, native_filters):
    """Build the json_metadata dict for a Superset dashboard."""
    chart_config = {}
    for cid in chart_ids:
        chart_config[str(cid)] = {
            "id": cid,
            "crossFilters": {
                "scope": "global",
                "chartsInScope": [c for c in chart_ids if c != cid],
            },
        }
    return {
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "color_scheme": "",
        "label_colors": {},
        "cross_filters_enabled": True,
        "chart_configuration": chart_config,
        "global_chart_configuration": {
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "chartsInScope": chart_ids,
        },
        "native_filter_configuration": native_filters,
        "default_filters": "{}",
        "shared_label_colors": {},
        "color_scheme_domain": [],
        "filter_scopes": {},
    }
