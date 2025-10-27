# app.py
import os
import json
import logging
from flask import Flask, jsonify, request
from main import main  # your existing ETL entrypoint

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("salesiq-app")

app = Flask(__name__)

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

def notify_slack(msg: str, extra: dict | None = None):
    """Best-effort Slack notifier; never raises."""
    if not SLACK_WEBHOOK:
        return
    try:
        import requests
        payload = {"text": msg}
        if extra:
            payload["blocks"] = [
                {"type": "section", "text": {"type": "mrkdwn", "text": msg}},
                {"type": "section", "text": {"type": "mrkdwn", "text": "```" + json.dumps(extra, indent=2) + "```"}},
            ]
        requests.post(SLACK_WEBHOOK, json=payload, timeout=8)
    except Exception as e:
        log.warning("Slack notify failed: %s", e)

@app.get("/")
def health():
    return jsonify({"ok": True})

@app.post("/run")
def run():
    # Optional: accept a small payload if you want (e.g., dry_run=true)
    params = request.get_json(silent=True) or {}
    try:
        log.info("Run started | params=%s", params)
        main()  # your ETL does the work; keep prints/logs in there
        log.info("Run finished OK")
        notify_slack("salesiq-app:Run finished OK")
        return jsonify({"status": "ok"})
    except Exception as e:
        log.exception("Run failed")
        notify_slack("❌ SalesIQ job failed", {"error": str(e)})
        # Return 500 so Cloud Scheduler can retry if configured
        return jsonify({"status": "error", "detail": str(e)}), 500

if __name__ == "__main__":
    # Local dev only; in Cloud Run we use gunicorn
    app.run(host="0.0.0.0", port=8080)
