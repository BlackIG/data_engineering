"""
transformer.py
--------------
Transform/flatten raw SalesIQ payloads into analytics-friendly records.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _get(d: Any, path: str, default=None):
    """Safe getter for nested dict/list with 'a.b.0.c' dot-notation."""
    cur = d
    for key in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(key, default)
        elif isinstance(cur, list):
            try:
                cur = cur[int(key)]
            except Exception:
                return default
        else:
            return default
    return cur


def _epoch_like_to_iso8601(x) -> Optional[str]:
    """Coerce epoch seconds or ms to ISO-8601 UTC string."""
    if x is None:
        return None
    try:
        v = int(x)
    except (TypeError, ValueError):
        return None
    if v < 100_000_000_000:  # seconds → ms
        v *= 1000
    dt = datetime.fromtimestamp(v / 1000, tz=timezone.utc)
    return dt.isoformat()


def map_conversation_details(raw: Dict[str, Any], chat_id: str) -> Dict[str, Any]:
    """
    Flatten GET /conversations/{id} (wrapped {"data": {...}}) to a tags/metadata record.
    """
    data: Dict[str, Any] = raw.get("data") or {}

    tags_raw = data.get("tags") or []

    def _tag_name(t):
        if isinstance(t, dict):
            return t.get("name")
        if isinstance(t, str):
            return t
        return None

    tags_out = [n for n in (_tag_name(t) for t in tags_raw) if n]

    return {
        "chat_id": chat_id,
        "system_chat_id": data.get("chat_id"),
        "type": data.get("type"),
        "start_time": _epoch_like_to_iso8601(data.get("start_time")),
        "end_time": _epoch_like_to_iso8601(data.get("end_time")),
        "phone": _get(data, "visitor.phone"),
        "email": _get(data, "visitor.email"),
        "tags": tags_out,
        "ticket_id": data.get("reference_id"),
    }


def extract_one_chat(export_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten one exported chat JSON to the requested schema + chat[] array.
    """
    url = _get(export_obj, "url", "")
    data = _get(export_obj, "data", {}) or export_obj

    if url and "/conversations/" in url:
        chat_id = url.split("/conversations/")[1].split("/export")[0]
    else:
        chat_id = str(_get(data, "id", ""))

    chat_start_time = _epoch_like_to_iso8601(_get(data, "visitor_details.time.start_time"))
    chat_end_time = _epoch_like_to_iso8601(_get(data, "visitor_details.time.end_time"))
    operating_system = (
        _get(data, "visitor_details.Operating_system")
        or _get(data, "visitor_details.operating_system")
        or _get(data, "visitor_details.os")
    )
    first_message = _get(data, "visitor_details.question")
    visitor_name = _get(data, "visitor_details.visitor_name")
    visitor_email = _get(data, "visitor_details.email")

    happiness_rating: Optional[int] = None
    feedback: Optional[str] = None
    chat_closed_by: Optional[str] = None
    chat_array: List[Dict[str, Any]] = []

    msgs = _get(data, "messages", []) or []

    def _seq_num(s):
        try:
            return int(_get(s, "sequence_id") or 0)
        except (TypeError, ValueError):
            return 0

    msgs_sorted = sorted(msgs, key=_seq_num)

    for s in msgs_sorted:
        s_type = _get(s, "type")

        if s_type == "text":
            flow_rating = _get(s, "meta.card_data.value") if _get(s, "meta.card_data.type") == "happiness-rating" else None
            chat_array.append(
                {
                    "id": _get(s, "sequence_id"),
                    "sender": _get(s, "sender.type"),
                    "message": _get(s, "message.text"),
                    "meta": {
                        "articles": _get(s, "meta.display_card.articles"),
                        "flow_rating": flow_rating,
                    },
                }
            )

        elif s_type == "info":
            mode = _get(s, "message.mode")
            if mode == "feedback":
                happiness_rating = _get(s, "message.rating")
                feedback = _get(s, "message.message")
            elif mode == "chatclosed":
                chat_closed_by = _get(s, "message.chat_close_by")

    return {
        "chat_id": chat_id,
        "chat_start_time": chat_start_time,
        "chat_end_time": chat_end_time,
        "operating_system": operating_system,
        "first_message": first_message,
        "visitor_name": visitor_name,
        "visitor_email": visitor_email,
        "happiness_rating": happiness_rating,
        "feedback": feedback,
        "chat_closed_by": chat_closed_by,
        "chat": chat_array,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
    }
