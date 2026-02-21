"""Dashboard callbacks package.

register_callbacks(app) is called from dashboard/app.py.  It imports each
submodule and explicitly calls module.register(app) so that callbacks defined
inside the register() functions are attached to the Dash app instance.
"""


def register_callbacks(app):
    from . import notifications, plots, runs, settings

    runs.register(app)
    plots.register(app)
    settings.register(app)
    notifications.register(app)
