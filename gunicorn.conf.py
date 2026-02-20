# gunicorn.conf.py
# Production WSGI config for the Dash dashboard server.
#
# Start with:
#   gunicorn "rapidqcms.DashWebApp:server" -c gunicorn.conf.py
#
# (Dash exposes the underlying Flask app as `app.server`; DashWebApp.py
# will be updated to export `server = app.server` in Phase 4.)

bind = "0.0.0.0:8050"
workers = 2          # Dash callbacks are synchronous; keep this low
timeout = 120        # allow slow plot callbacks to complete
worker_class = "sync"
accesslog = "-"
errorlog = "-"
loglevel = "info"
