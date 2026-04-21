"""Rapid-QC-MS dashboard package.

The Dash application is instantiated in app.py.  Import `app` from there
for use in tests or gunicorn entry points.
"""

from rapidqcms.dashboard.app import app  # noqa: F401

__all__ = ["app"]
