"""
logger.py
---------
Logging bootstrap and a convenience contextmanager that logs START/END/FAIL
with wall-clock duration per section.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone



def get_logger(name: str = "salesiq") -> logging.Logger:
    """
    Initialize and return a module-level logger.

    The log format is Cloud Run / Cloud Logging friendly.
    Level can be overridden via LOG_LEVEL env var.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger(name)

def _utc_now_iso():
    # e.g., 2025-10-22T09:15:30.123456+00:00
    return datetime.now(timezone.utc).isoformat()



@contextmanager
def log_section(log: logging.Logger, name: str, **kv):
    """
    Log START/END/FAIL with timestamps and duration_ms.
    """
    start_ts = _utc_now_iso()
    t0 = time.perf_counter()
    ctx = " ".join(f"{k}={v}" for k, v in kv.items()) if kv else ""
    log.info("START %s %s start_ts=%s", name, ctx, start_ts)
    try:
        yield
        dur_ms = int((time.perf_counter() - t0) * 1000)
        end_ts = _utc_now_iso()
        log.info("END   %s duration_ms=%d start_ts=%s end_ts=%s", name, dur_ms, start_ts, end_ts)
    except Exception as e:
        dur_ms = int((time.perf_counter() - t0) * 1000)
        end_ts = _utc_now_iso()
        log.exception("FAIL  %s duration_ms=%d start_ts=%s end_ts=%s error=%s", name, dur_ms, start_ts, end_ts,  e)
        raise