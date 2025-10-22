"""
extractor.py
------------
All communication with Zoho SalesIQ:
- OAuth token refresh
- Listing conversation IDs with retry/backoff
- Exporting conversations
- Fetching conversation details
"""

from __future__ import annotations

import random
import time
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SalesIQExtractor:
    """
    Zoho SalesIQ API client with resilient HTTP session and backoff.

    Parameters
    ----------
    cfg : Settings
        Loaded application settings.
    log : logging.Logger
        Logger instance.
    """

    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log
        self.session = self._make_session()

    # ------------------------- session -------------------------

    def _make_session(self) -> requests.Session:
        """Return a requests Session with sane retry defaults."""
        retry = Retry(
            total=6,
            connect=3,
            read=3,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        s = requests.Session()
        s.headers.update({"Accept": "application/json", "Connection": "keep-alive"})
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    # ------------------------- auth -------------------------

    def get_access_token(self) -> str:
        """
        Refresh an OAuth access token using the configured refresh token.

        Returns
        -------
        str
            Access token string.

        Raises
        ------
        requests.HTTPError
            If the token endpoint fails.
        RuntimeError
            If the response does not contain an access_token.
        """
        # Derive Zoho Accounts base (common pattern): replace 'salesiq.' → 'accounts.'
        accounts_base = self.cfg.siq_base_url.replace("salesiq.", "accounts.")
        url = f"{accounts_base}/oauth/{self.cfg.api_version}/token"
        params = {
            "grant_type": "refresh_token",
            "refresh_token": self.cfg.siq_refresh_token,
            "client_id": self.cfg.siq_client_id,
            "client_secret": self.cfg.siq_client_secret,
        }
        r = self.session.post(url, data=params, timeout=(10, 45))
        r.raise_for_status()
        token = (r.json() or {}).get("access_token")
        if not token:
            raise RuntimeError("No access_token in token response")
        return token

    # ------------------------- list IDs -------------------------

    def fetch_conversation_ids(
        self,
        access_token: str,
        status: str,
        from_ms: int,
        to_ms: int,
        limit: int = 99,
    ) -> List[str]:
        """
        List conversation IDs for a department within a time window.

        Retries per page on network timeouts and 429/5xx, honoring Retry-After.

        Returns
        -------
        List[str]
            Sorted unique conversation ID strings.
        """
        base = f"{self.cfg.siq_base_url}/api/{self.cfg.api_version}/{self.cfg.screen_name}/conversations"
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Accept": "application/json",
        }

        ids: set[str] = set()
        page = 1
        per_page = min(limit, 99)

        while True:
            params = {
                "limit": per_page,
                "page": page,
                "department_id": self.cfg.siq_department_id,
                "from_time": from_ms,
                "to_time": to_ms,
            }
            if status:
                params["status"] = status

            max_attempts = 5
            base_delay = 1.5
            attempt = 0

            while True:
                try:
                    r = self.session.get(base, headers=headers, params=params, timeout=(10, 90))

                    if r.status_code in (429, 500, 502, 503, 504):
                        attempt += 1
                        if attempt >= max_attempts:
                            r.raise_for_status()
                        ra = r.headers.get("Retry-After")
                        delay = float(ra) if (ra and ra.isdigit()) else random.uniform(0, base_delay * (2 ** attempt))
                        self.log.warning("list page=%d status=%d retry %.1fs", page, r.status_code, delay)
                        time.sleep(delay)
                        continue

                    r.raise_for_status()
                    break

                except requests.exceptions.ReadTimeout:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    delay = random.uniform(0, base_delay * (2 ** attempt))
                    self.log.warning("list page=%d timeout retry %.1fs", page, delay)
                    time.sleep(delay)

            rows = (r.json() or {}).get("data") or []
            if not rows:
                break

            for obj in rows:
                cid = obj.get("id")
                if cid is not None:
                    ids.add(str(cid))

            if len(rows) < per_page:
                break
            page += 1

        out = sorted(ids)
        self.log.info("list_conversations total_ids=%d", len(out))
        return out

    # ------------------------- export & details -------------------------

    def export_chat_json(self, access_token: str, chat_id: str) -> Dict[str, Any]:
        """
        Export a single conversation as JSON.

        Returns
        -------
        Dict[str, Any]
            Export payload from Zoho.
        """
        url = f"{self.cfg.siq_base_url}/api/{self.cfg.api_version}/{self.cfg.screen_name}/conversations/{chat_id}/export"
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Accept": "application/json",
        }
        r = self.session.post(url, headers=headers, json={"data": "messages", "format": "json"}, timeout=(10, 120))
        r.raise_for_status()
        return r.json()

    def get_conversation_details(self, access_token: str, chat_id: str) -> Dict[str, Any]:
        """
        Fetch details for a single conversation ID.

        Returns
        -------
        Dict[str, Any]
            Details payload (typically {"data": {...}}).
        """
        url = f"{self.cfg.siq_base_url}/api/{self.cfg.api_version}/{self.cfg.screen_name}/conversations/{chat_id}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Accept": "application/json",
        }
        r = self.session.get(url, headers=headers, timeout=(10, 90))
        r.raise_for_status()
        return r.json()
