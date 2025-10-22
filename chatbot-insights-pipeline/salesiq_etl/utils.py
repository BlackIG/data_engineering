"""
utils.py
--------
Miscellaneous helpers:
- /tmp-safe local cleanup
- GCS JSONL streaming reader
- Time window utilities (yesterday + incremental from BQ)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from dateutil import tz
from google.cloud import bigquery, storage


def safe_unlink(path: str | None, log=None) -> None:
    """
    Best-effort local file deletion.
    Logs info on success, warns on failures, never raises.
    """
    if not path:
        return
    try:
        os.unlink(path)
        if log:
            log.info("cleanup_ok path=%s", path)
    except FileNotFoundError:
        if log:
            log.info("cleanup_skipped (not found) path=%s", path)
    except Exception as e:
        if log:
            log.warning("cleanup_failed path=%s error=%s", path, e)


def gcs_open_jsonl(gcs_uri: str, project: str) -> Iterable[Dict[str, Any]]:
    """
    Stream a JSONL file from GCS and yield objects line-by-line.

    Parameters
    ----------
    gcs_uri : str
        Must be of the form gs://bucket/path/to/file.jsonl
    project : str
        GCP project for client initialization.

    Yields
    ------
    Dict[str, Any]
        Parsed JSON objects per line.

    Raises
    ------
    FileNotFoundError
        If the object does not exist.
    ValueError
        If the GCS URI is invalid.
    """
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")

    tail = gcs_uri[5:]
    bucket_name, _, blob_path = tail.partition("/")
    if not bucket_name or not blob_path:
        raise ValueError(f"GCS URI must be gs://bucket/object, got: {gcs_uri}")

    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"GCS object not found: {gcs_uri}")

    for line in blob.download_as_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


def yesterday_window_ms(tz_name: str = "Africa/Lagos") -> Tuple[int, int, str]:
    """
    Build a [yesterday-00:00, yesterday-23:59:59.999] window in ms for the given TZ.
    Returns (from_ms, to_ms, 'YYYY-MM-DD').
    """
    zone = tz.gettz(tz_name)
    now_local = datetime.now(zone)
    start = (now_local - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000), start.date().isoformat()


def incremental_window_ms_from_bq(
    project: str,
    tz_name: str = "Africa/Lagos",
) -> tuple[int, int, str]:
    """
    Build an incremental window based on the last ingested start_time from the
    target tags table.

    Logic:
    - last = MAX(start_time) from `gold-courage-194810.zoho_desk.salesiq_conversation_tags`
    - if last is NULL: use yesterday_window_ms(tz_name)
    - else: from = last + 1ms; to = min(from + 1 day - 1ms, now_utc)

    Returns
    -------
    (from_ms, to_ms, label_ymd)
    """
    client = bigquery.Client(project=project)
    sql = """
      SELECT IFNULL(MAX(start_time), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
       AS last_interaction_time
      FROM `gold-courage-194810.zoho_desk.salesiq_conversation_tags`
    """
    row = next(iter(client.query(sql).result()), None)
    last = row.last_interaction_time if row else None

    if last is None:
        return yesterday_window_ms(tz_name)

    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)

    from_dt = last + timedelta(milliseconds=1)
    to_dt = from_dt + timedelta(days=1) - timedelta(milliseconds=1)

    now_utc = datetime.now(timezone.utc)
    if to_dt > now_utc:
        to_dt = now_utc

    local = tz.gettz(tz_name)
    label_ymd = from_dt.astimezone(local).date().isoformat()

    return int(from_dt.timestamp() * 1000), int(to_dt.timestamp() * 1000), label_ymd
