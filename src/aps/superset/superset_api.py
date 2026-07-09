"""
Shared Apache Superset REST API helpers for APS Database dashboard scripts.

Extracted from create_baselines_dashboard.py, create_sc_dashboard.py, and
create_baselines_dashboard_device_library.py to eliminate duplication.

All functions use db_config for connection defaults.
"""

import json
import re
import uuid

import requests

from aps.db_config import SUPERSET_URL, SUPERSET_USER, SUPERSET_PASS
from aps.superset.dashboard_png_export import (
    export_chart_png,
    register_dataset_for_png_export,
)


# ── Authentication ──────────────────────────────────────────────────────────

def get_session(superset_url=None, username=None, password=None):
    """Authenticate with both form login and JWT token.

    Form login establishes Flask-Login session (so current_user is a real
    user object, needed for dataset creation which sets owners).
    JWT token satisfies the API Authorization header requirement.
    """
    url = superset_url or SUPERSET_URL
    user = username or SUPERSET_USER
    pw = password or SUPERSET_PASS

    session = requests.Session()

    # 1. Form-based login to establish Flask-Login session cookie
    resp = session.get(f"{url}/login/")
    resp.raise_for_status()
    match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', resp.text)
    if not match:
        raise RuntimeError("Could not find CSRF token on login page")
    resp = session.post(
        f"{url}/login/",
        data={"username": user, "password": pw,
              "csrf_token": match.group(1)},
        allow_redirects=True,
    )
    resp.raise_for_status()

    # 2. JWT token for API auth header
    resp = requests.post(
        f"{url}/api/v1/security/login",
        json={"username": user, "password": pw,
              "provider": "db", "refresh": True},
    )
    if resp.ok:
        access_token = resp.json().get("access_token")
        if access_token:
            session.headers["Authorization"] = f"Bearer {access_token}"

    # 3. API CSRF token for mutating requests
    resp = session.get(f"{url}/api/v1/security/csrf_token/")
    if resp.ok:
        csrf = resp.json().get("result", "")
        if csrf:
            session.headers["X-CSRFToken"] = csrf
            session.headers["Referer"] = url

    session.headers["Content-Type"] = "application/json"
    # Store URL on session so other helpers can use it
    session._superset_url = url
    return session


def _url(session):
    """Get the Superset URL from the session (set by get_session)."""
    return getattr(session, '_superset_url', SUPERSET_URL)


# ── Database Discovery ──────────────────────────────────────────────────────

def find_database(session):
    """Find the database connection for the mosfets DB."""
    url = _url(session)
    resp = session.get(
        f"{url}/api/v1/database/",
        params={"q": json.dumps({"page_size": 100})},
    )
    resp.raise_for_status()
    for db in resp.json()["result"]:
        name = db.get("database_name", "").lower()
        if "mosfet" in name or "postgresql" in name or "aps" in name:
            print(f"  Found database: {db['database_name']} (id={db['id']})")
            return db["id"]
    for db in resp.json()["result"]:
        detail = session.get(f"{url}/api/v1/database/{db['id']}").json()
        uri = detail.get("result", {}).get("sqlalchemy_uri", "")
        if "mosfets" in uri or "postgresqlv2" in uri or "5435" in uri:
            print(f"  Found database by URI: {db['database_name']} (id={db['id']})")
            return db["id"]
    print("  ERROR: Could not find database.")
    return None


# ── Dataset Management ──────────────────────────────────────────────────────

def find_or_create_dataset(session, db_id, table_name, schema="public"):
    """Find or create a dataset for a given table/view."""
    url = _url(session)
    resp = session.get(
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
            register_dataset_for_png_export(ds["id"], table_name, schema)
            return ds["id"]

    resp = session.post(f"{url}/api/v1/dataset/", json={
        "database": db_id, "table_name": table_name, "schema": schema,
    })
    if resp.ok:
        ds_id = resp.json()["id"]
        print(f"  Created dataset '{table_name}' (id={ds_id})")
        register_dataset_for_png_export(ds_id, table_name, schema)
        return ds_id
    print(f"  ERROR creating dataset '{table_name}': "
          f"{resp.status_code} {resp.text[:200]}")
    return None


def refresh_dataset_columns(session, ds_id):
    """Refresh dataset columns and clear cached column statistics."""
    url = _url(session)
    session.put(f"{url}/api/v1/dataset/{ds_id}/refresh", json={})
    resp = session.get(f"{url}/api/v1/dataset/{ds_id}")
    if resp.ok:
        session.put(f"{url}/api/v1/dataset/{ds_id}", json={})
        session.put(f"{url}/api/v1/dataset/{ds_id}/refresh", json={})


# ── Chart Management ────────────────────────────────────────────────────────

def create_chart(session, name, datasource_id, viz_type, params, description=None):
    """Create or update a chart. Returns (chart_id, chart_uuid)."""
    url = _url(session)
    resp = session.get(
        f"{url}/api/v1/chart/",
        params={"q": json.dumps({
            "filters": [{"col": "slice_name", "opr": "eq", "value": name}],
            "page_size": 100,
        })},
    )
    if resp.ok:
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
                update_resp = session.put(
                    f"{url}/api/v1/chart/{chart['id']}",
                    json=payload,
                )
                detail = session.get(
                    f"{url}/api/v1/chart/{chart['id']}"
                ).json()
                uid = detail.get("result", {}).get("uuid", str(uuid.uuid4()))
                status = "updated" if update_resp.ok else "exists (update failed)"
                print(f"  Chart '{name}' {status} (id={chart['id']})")
                export_chart_png(name, datasource_id, viz_type, params)
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
    resp = session.post(f"{url}/api/v1/chart/", json=payload)
    if resp.ok:
        chart_id = resp.json()["id"]
        detail = session.get(f"{url}/api/v1/chart/{chart_id}").json()
        real_uuid = detail.get("result", {}).get("uuid", str(uuid.uuid4()))
        print(f"  Created chart '{name}' (id={chart_id})")
        export_chart_png(name, datasource_id, viz_type, params)
        return chart_id, real_uuid
    print(f"  ERROR creating chart '{name}': "
          f"{resp.status_code} {resp.text[:300]}")
    return None, None


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
        resp = session.get(endpoint, params=params)
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
    resp = session.get(f"{url}/api/v1/dashboard/{dashboard_id}")
    if not resp.ok:
        return []
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
    request = session.put if method == "put" else session.post
    resp = request(endpoint, json=payload)
    if resp.ok or "roles" not in payload or resp.status_code not in (400, 403, 422):
        return resp

    fallback = dict(payload)
    fallback.pop("roles", None)
    retry = request(endpoint, json=fallback)
    if retry.ok:
        print(
            "  WARNING: dashboard saved without immediate Public role "
            "assignment; Superset startup sync will repair it"
        )
    return retry


def create_or_update_dashboard(session, title, position_json, json_metadata,
                               slug):
    """Create or update a dashboard by slug. Returns dashboard id."""
    url = _url(session)
    resp = session.get(
        f"{url}/api/v1/dashboard/",
        params={"q": json.dumps({
            "filters": [{"col": "slug", "opr": "eq", "value": slug}],
            "page_size": 10,
        })},
    )
    existing_id = None
    if resp.ok:
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
        if resp.ok:
            print(f"  Updated dashboard (id={existing_id})")
            return existing_id
        print(f"  ERROR updating: {resp.status_code} {resp.text[:300]}")
        return existing_id

    resp = _save_dashboard(session, "post", f"{url}/api/v1/dashboard/", payload)
    if resp.ok:
        dash_id = resp.json()["id"]
        print(f"  Created dashboard (id={dash_id})")
        return dash_id
    print(f"  ERROR creating: {resp.status_code} {resp.text[:300]}")
    return None


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
