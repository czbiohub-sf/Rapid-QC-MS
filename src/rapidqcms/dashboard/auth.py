"""Okta SSO authentication for Rapid-QC-MS dashboard.

Opt-in: if RAPIDQCMS_OKTA_DOMAIN, RAPIDQCMS_OKTA_CLIENT_ID, and
RAPIDQCMS_OKTA_CLIENT_SECRET are not set, init_auth() is a no-op and the
dashboard runs in open mode (local dev / testing unchanged).
"""

import hashlib
from datetime import timedelta

from flask import redirect, request, session, url_for

try:
    from authlib.integrations.flask_client import OAuth
except ImportError:  # pragma: no cover
    OAuth = None  # type: ignore[assignment,misc]


def init_auth(server, settings):
    """Wire up Okta OIDC authentication on the Flask server.

    If any of okta_domain / okta_client_id / okta_client_secret is None,
    returns immediately without registering any routes or guards.
    """
    if not all([settings.okta_domain, settings.okta_client_id, settings.okta_client_secret]):
        return

    # Session secret: use explicit setting if provided, else derive deterministically
    # from client credentials so gunicorn workers share the same key.
    if settings.session_secret:
        server.secret_key = settings.session_secret
    else:
        server.secret_key = hashlib.sha256(
            (settings.okta_client_id + settings.okta_client_secret).encode()
        ).hexdigest()

    server.permanent_session_lifetime = timedelta(hours=8)

    oauth = OAuth(server)
    oauth.register(
        name="okta",
        server_metadata_url=(
            f"https://{settings.okta_domain}/.well-known/openid-configuration"
        ),
        client_id=settings.okta_client_id,
        client_secret=settings.okta_client_secret,
        client_kwargs={"scope": "openid profile email"},
    )

    @server.route("/login")
    def login():
        callback_url = url_for("auth_callback", _external=True)
        return oauth.okta.authorize_redirect(callback_url)

    @server.route("/oauth/callback")
    def auth_callback():
        token = oauth.okta.authorize_access_token()
        userinfo = token.get("userinfo") or {}
        session["user"] = {
            "sub": userinfo.get("sub"),
            "name": userinfo.get("name"),
            "email": userinfo.get("email"),
        }
        session.permanent = True
        next_url = session.pop("next", "/")
        return redirect(next_url)

    @server.route("/logout")
    def logout():
        session.clear()
        logout_url = (
            f"https://{settings.okta_domain}/oauth2/v1/logout"
            f"?post_logout_redirect_uri={url_for('login', _external=True)}"
        )
        return redirect(logout_url)

    _OPEN_PREFIXES = ("/login", "/oauth/callback", "/_dash-", "/_assets", "/assets")

    @server.before_request
    def _auth_guard():
        path = request.path
        if any(path.startswith(p) for p in _OPEN_PREFIXES):
            return None
        if session.get("user"):
            return None
        session["next"] = request.url
        return redirect("/login")
