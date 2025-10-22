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


@contextmanager
def log_section(log: logging.Logger, name: str, **kv):
    """
    Context manager to log START/END/FAIL for a named section.

    Parameters
    ----------
    log : logging.Logger
        Logger to write to.
    name : str
        Logical section name (e.g., "export-chats").
    **kv :
        Extra key/value context that will be stringified in START.
    """
    t0 = time.perf_counter()
    ctx = " ".join(f"{k}={v}" for k, v in kv.items()) if kv else ""
    log.info("START %s %s", name, ctx)
    try:
        yield
        dur_ms = int((time.perf_counter() - t0) * 1000)
        log.info("END   %s duration_ms=%d", name, dur_ms)
    except Exception as e:
        dur_ms = int((time.perf_counter() - t0) * 1000)
        log.exception("FAIL  %s duration_ms=%d error=%s", name, dur_ms, e)
        raise
