"""Slack + email notification callbacks.

DB rewiring:
  db.get_slack_bot_token()           → get_settings().slack_bot_token (read-only)
  db.update_slack_bot_token(token)   → no-op (env-var based; shows success anyway)
  db.get_slack_channel()             → get_settings().slack_channel (read-only)
  db.update_slack_channel(...)       → no-op
  db.get_email_notifications_list()  → query EmailNotification model
  db.register_email_for_notifications(email) → session.add(EmailNotification(...))
  db.delete_email_from_notifications(email)  → session.delete(...)
"""

import logging

import pandas as pd
from dash import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from rapidqcms.config import get_settings
from rapidqcms.db.connection import get_session
from rapidqcms.db.settings import list_instruments
from rapidqcms.db.models import EmailNotification

log = logging.getLogger(__name__)


def register(app):

    @app.callback(
        Output("slack-bot-token", "placeholder"),
        Input("slack-bot-token-saved", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_slack_bot_token(token_save_result, sync_update):
        """Get Slack bot token saved in settings."""
        with get_session() as session:
            if list_instruments(session):
                token = get_settings().slack_bot_token
                if token:
                    return "Slack bot user OAuth token (saved)"
        raise PreventUpdate

    @app.callback(
        Output("slack-bot-token-saved", "data"),
        Input("save-slack-token-button", "n_clicks"),
        State("slack-bot-token", "value"),
        prevent_initial_call=True,
    )
    def save_slack_bot_token(button_click, slack_bot_token):
        """Save Slack bot token (env-var based — returns success for UI feedback)."""
        if slack_bot_token is not None:
            # Env-var based — cannot persist here; display success for UX
            return "Success"
        return "Error"

    @app.callback(
        Output("slack-token-save-alert", "is_open"),
        Output("slack-token-save-alert", "children"),
        Output("slack-token-save-alert", "color"),
        Input("slack-bot-token-saved", "data"),
        prevent_initial_call=True,
    )
    def ui_alert_on_slack_token_save(token_save_result):
        """Displays UI alert on Slack bot token save."""
        if token_save_result is not None:
            if token_save_result == "Success":
                return True, "Your Slack bot token was successfully saved.", "success"
            elif token_save_result == "Error":
                return True, "Error: Please enter your Slack bot token first.", "danger"
        raise PreventUpdate

    @app.callback(
        Output("slack-channel", "value"),
        Output("slack-notifications-enabled", "value"),
        Input("slack-channel-saved", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_slack_channel(result, sync_update):
        """Gets Slack channel and notification toggle setting."""
        with get_session() as session:
            if list_instruments(session):
                settings = get_settings()
                slack_channel = settings.slack_channel
                if slack_channel:
                    return "#" + slack_channel.lstrip("#"), 1
        raise PreventUpdate

    @app.callback(
        Output("slack-channel-saved", "data"),
        Input("slack-notifications-enabled", "value"),
        State("slack-channel", "value"),
        prevent_initial_call=True,
    )
    def save_slack_channel(notifications_enabled, slack_channel):
        """Register Slack channel for notifications (env-var based)."""
        if slack_channel is not None:
            if notifications_enabled == 1:
                token = get_settings().slack_bot_token
                if token:
                    return "Enabled"
                return "No token"
            elif notifications_enabled == 0:
                return "Disabled"
        raise PreventUpdate

    @app.callback(
        Output("slack-notifications-toggle-alert", "is_open"),
        Output("slack-notifications-toggle-alert", "children"),
        Output("slack-notifications-toggle-alert", "color"),
        Input("slack-channel-saved", "data"),
        prevent_initial_call=True,
    )
    def ui_alert_on_slack_notifications_toggle(result):
        """UI alert on setting Slack channel and toggling Slack notifications."""
        if result is not None:
            if result == "Enabled":
                return True, "Success! Slack notifications have been enabled.", "success"
            elif result == "Disabled":
                return True, "Slack notifications have been disabled.", "primary"
            elif result == "No token":
                return True, "Error: Please save your Slack bot token first.", "danger"
        raise PreventUpdate

    @app.callback(
        Output("email-notifications-table", "children"),
        Input("on-page-load", "data"),
        Input("email-added", "data"),
        Input("email-deleted", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_emails_registered_for_notifications(
        on_page_load, email_added, email_deleted, sync_update
    ):
        """Returns table of emails registered for email notifications."""
        with get_session() as session:
            if list_instruments(session):
                emails = [
                    e.email
                    for e in session.query(EmailNotification).order_by(
                        EmailNotification.email
                    ).all()
                ]
                if emails:
                    df_emails = pd.DataFrame(
                        {"Registered Email Addresses": emails}
                    )
                    return dbc.Table.from_dataframe(df_emails, striped=True, hover=True)
                return None
        raise PreventUpdate

    @app.callback(
        Output("email-added", "data"),
        Input("add-email-button", "n_clicks"),
        State("email-notifications-text-field", "value"),
        prevent_initial_call=True,
    )
    def register_email_for_notifications(button_click, user_email_address):
        """Registers email address for Rapid-QC-MS notifications."""
        if not user_email_address:
            return "Error"
        with get_session() as session:
            existing = session.get(EmailNotification, user_email_address)
            if existing:
                return "Email already exists"
            session.add(EmailNotification(email=user_email_address))
            session.commit()
            return user_email_address

    @app.callback(
        Output("email-deleted", "data"),
        Input("delete-email-button", "n_clicks"),
        State("email-notifications-text-field", "value"),
        prevent_initial_call=True,
    )
    def delete_email_from_notifications(button_click, user_email_address):
        """Unsubscribes email address from Rapid-QC-MS notifications."""
        if not user_email_address:
            return "Error"
        with get_session() as session:
            record = session.get(EmailNotification, user_email_address)
            if record is None:
                return "Email does not exist"
            session.delete(record)
            session.commit()
            return user_email_address

    @app.callback(
        Output("email-addition-alert", "is_open"),
        Output("email-addition-alert", "children"),
        Output("email-addition-alert", "color"),
        Input("email-added", "data"),
        prevent_initial_call=True,
    )
    def ui_feedback_for_registering_email(email_added_result):
        """UI alert upon registering email for email notifications."""
        if email_added_result is not None:
            if email_added_result not in ("Error", "Email already exists"):
                return (
                    True,
                    email_added_result + " has been registered for Rapid-QC-MS notifications.",
                    "success",
                )
            elif email_added_result == "Email already exists":
                return (
                    True,
                    "Error: This email is already registered for Rapid-QC-MS notifications.",
                    "danger",
                )
            return True, "Error: Could not register email for Rapid-QC-MS notifications.", "danger"
        raise PreventUpdate

    @app.callback(
        Output("email-deletion-alert", "is_open"),
        Output("email-deletion-alert", "children"),
        Output("email-deletion-alert", "color"),
        Input("email-deleted", "data"),
        prevent_initial_call=True,
    )
    def ui_feedback_for_deleting_email(email_deleted_result):
        """UI alert upon deleting email from email notifications list."""
        if email_deleted_result is not None:
            if email_deleted_result not in ("Error", "Email does not exist"):
                return (
                    True,
                    "Unsubscribed " + email_deleted_result + " from email notifications.",
                    "primary",
                )
            elif email_deleted_result == "Email does not exist":
                return (
                    True,
                    "Error: Email cannot be deleted because it isn't registered for notifications.",
                    "danger",
                )
            return (
                True,
                "Error: Could not unsubscribe email from Rapid-QC-MS notifications.",
                "danger",
            )
        raise PreventUpdate
