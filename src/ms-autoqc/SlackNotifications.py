import DatabaseFunctions as db
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def send_message(message):

    """
    Posts a message to the Slack channel registered for notifications in Settings > General
    """

    # Retrieve Slack bot token and Slack channel
    slack_token = db.get_slack_bot_token()
    client = WebClient(token=slack_token)

    # Send Slack message
    try:
        slack_channel = db.get_slack_channel()
        response = client.chat_postMessage(channel=slack_channel, text=message)
        return response
    except SlackApiError as error:
        print("Slack API error:", error)
        return error