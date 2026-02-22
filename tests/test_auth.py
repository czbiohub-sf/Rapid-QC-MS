"""Tests for Okta SSO auth middleware (dashboard/auth.py).

These tests use plain Flask test clients and mock authlib to avoid real HTTP.
The Dash app is only used for the open-mode smoke test.
"""

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask


@dataclass
class MockSettings:
    okta_domain: str | None = None
    okta_client_id: str | None = None
    okta_client_secret: str | None = None
    session_secret: str | None = None


def _configured_settings():
    return MockSettings(
        okta_domain="example.okta.com",
        okta_client_id="client123",
        okta_client_secret="secret456",
    )


# ---------------------------------------------------------------------------
# Open mode (no Okta config)
# ---------------------------------------------------------------------------


def test_init_auth_noop_when_not_configured():
    """With no Okta config the dashboard returns 200 without any redirect."""
    from rapidqcms.dashboard.app import app

    with app.server.test_client() as c:
        resp = c.get("/")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------


def test_init_auth_registers_routes_when_configured():
    """/login, /oauth/callback, and /logout are registered when Okta is configured."""
    from rapidqcms.dashboard.auth import init_auth

    server = Flask(__name__)
    server.secret_key = "test"

    with patch("rapidqcms.dashboard.auth.OAuth"):
        init_auth(server, _configured_settings())

    rules = {r.rule for r in server.url_map.iter_rules()}
    assert "/login" in rules
    assert "/oauth/callback" in rules
    assert "/logout" in rules


# ---------------------------------------------------------------------------
# Before-request guard
# ---------------------------------------------------------------------------


def _make_guarded_server():
    """Return a Flask server with Okta guard active and one protected route."""
    from rapidqcms.dashboard.auth import init_auth

    server = Flask(__name__)
    server.secret_key = "test"

    with patch("rapidqcms.dashboard.auth.OAuth"):
        init_auth(server, _configured_settings())

    @server.route("/")
    def index():
        return "OK"

    return server


def test_before_request_redirects_unauthenticated():
    """Unauthenticated GET / is redirected to /login."""
    server = _make_guarded_server()
    with server.test_client() as c:
        resp = c.get("/")
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_before_request_passes_open_paths():
    """Dash internal paths (/_dash-*) bypass the auth guard."""
    from rapidqcms.dashboard.auth import init_auth

    server = Flask(__name__)
    server.secret_key = "test"

    with patch("rapidqcms.dashboard.auth.OAuth"):
        init_auth(server, _configured_settings())

    @server.route("/_dash-layout")
    def dash_layout():
        return "layout"

    with server.test_client() as c:
        resp = c.get("/_dash-layout")
    assert resp.status_code == 200


def test_before_request_passes_authenticated():
    """Requests with user in session are allowed through."""
    server = _make_guarded_server()
    with server.test_client() as c:
        with c.session_transaction() as sess:
            sess["user"] = {"sub": "u1", "name": "Alice", "email": "alice@example.com"}
        resp = c.get("/")
    assert resp.status_code == 200
