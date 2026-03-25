"""Dash application entry point.

Usage:
  Development:
    rapidqcms serve --debug

  Production (gunicorn):
    gunicorn "rapidqcms.dashboard.app:server" --workers 4 --bind 0.0.0.0:8050
"""

from dash import Dash
import dash_bootstrap_components as dbc

local_stylesheet = {
    "href": "https://fonts.googleapis.com/css2?"
            "family=Lato:wght@400;700&display=swap",
    "rel": "stylesheet",
}

app = Dash(
    __name__,
    title="Rapid-QC-MS",
    suppress_callback_exceptions=True,
    external_stylesheets=[local_stylesheet, dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

# Gunicorn entry point
server = app.server

from rapidqcms.dashboard.layout import serve_layout  # noqa: E402
app.layout = serve_layout

from rapidqcms.dashboard.callbacks import register_callbacks  # noqa: E402
register_callbacks(app)
