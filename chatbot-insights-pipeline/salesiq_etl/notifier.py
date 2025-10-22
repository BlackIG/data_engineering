"""
notifier.py
-----------
Slack notifier wrapper. Keeps alerts out of the main logic and never
crashes the app if Slack is unavailable.
"""

from __future__ import annotations

import requests


class Notifier:
    """
    Thin Slack webhook client.

    Parameters
    ----------
    webhook : str | None
        Slack webhook URL, or None to disable notifications.
    log : logging.Logger
        Logger for warning on unsuccessful posts.
    """

    def __init__(self, webhook: str | None, log):
        self.webhook = webhook
        self.log = log

    def send(self, msg: str) -> None:
        """
        Send a simple text message to Slack.

        This method never raises. Failures are logged at WARNING level.
        """
        if not self.webhook:
            return
        try:
            requests.post(self.webhook, json={"text": msg}, timeout=8)
        except Exception as e:
            try:
                self.log.warning("Slack notify failed: %s", e)
            except Exception:
                pass

    def send_pretty(self, title: str, data: dict) -> None:
        """
        Send a multi-line message with a code block of key/values.
        """
        body = f"{title}\n```" + "\n".join(f"{k}: {v}" for k, v in data.items()) + "```"
        self.send(body)
