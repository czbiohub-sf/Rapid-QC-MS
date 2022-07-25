import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class SlackNotification:

    """
    Class for sending Slack notifications upon QC fail
    """

    def __init__(self):

        slack_token = "xoxb-105182011382-3832497551799-BbryUFZbyWByPi7kNPy2r22P"
        self.client = WebClient(token=slack_token)

    def send_message(self, message):

        """
        Posts Slack message to #ms-auto-qc channel
        """

        try:
            response = self.client.chat_postMessage(
                channel="ms-auto-qc",
                text=message
            )
        except SlackApiError as e:
            assert e.response["error"]