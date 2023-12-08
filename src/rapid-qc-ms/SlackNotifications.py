import ssl
import ms_autoqc.DatabaseFunctions as db
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def send_message(message):

    """
    Posts a message to the Slack channel registered for notifications in Settings > General.

    For details on the Slack API, see: https://slack.dev/python-slack-sdk/

    Args:
        message (str): Message to send to the Slack channel

    Returns:
        Response object on success, or Error object on failure
    """

    # SSL
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Retrieve Slack bot token and Slack channel
    slack_token = db.get_slack_bot_token()
    client = WebClient(token=slack_token, ssl=ssl_context)

    # Send Slack message
    try:
        slack_channel = db.get_slack_channel()
        response = client.chat_postMessage(channel=slack_channel, text=message)
        return response
    except SlackApiError as error:
        print("Slack API error:", error)
        return error