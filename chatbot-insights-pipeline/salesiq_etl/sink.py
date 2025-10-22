"""
sink.py
-------
Local temp file management and GCS uploads for the "lake" layer.

- Writes JSONL files under /tmp by default (Cloud Run friendly)
- Cleans up local files on success
- Notifies Slack and logs on failures
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
from typing import Any, Dict, Iterable, Optional

from google.cloud import storage


class Sink:
    """
    Sink for staging JSONL locally and uploading to GCS.

    Parameters
    ----------
    cfg : Settings
        Service configuration with project and bucket.
    log : logging.Logger
        Logger instance.
    notifier : Notifier
        Slack notifier to alert on errors.
    """

    def __init__(self, cfg, log, notifier):
        self.cfg = cfg
        self.log = log
        self.notifier = notifier
        self.storage = storage.Client(project=cfg.project)

    # ------------------------- JSONL write -------------------------

    def write_jsonl_stream(self, objs: Iterable[Dict[str, Any]], local_path: Optional[str] = None) -> Optional[str]:
        """
        Stream-write JSON objects to a JSON Lines file (UTF-8).

        - If `local_path` is None, a unique file is created under /tmp.
        - Returns the absolute path if at least one object was written, else None.
        - On any exception, attempts to delete the partial file, notifies Slack, and re-raises.

        Parameters
        ----------
        objs : Iterable[Dict[str, Any]]
            An iterable yielding JSON-serializable dicts.
        local_path : Optional[str]
            Destination path. If None, a temp file is created in /tmp.

        Returns
        -------
        Optional[str]
            Absolute path to the written file, or None if no records.
        """
        if local_path is None:
            fd, tmp = tempfile.mkstemp(prefix="salesiq_", suffix=".jsonl", dir="/tmp")
            os.close(fd)
            local_path = tmp
        else:
            pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        wrote = False
        try:
            with open(local_path, "w", encoding="utf-8") as f:
                for obj in objs:
                    f.write(json.dumps(obj, ensure_ascii=False))
                    f.write("\n")
                    wrote = True

            if not wrote:
                try:
                    os.unlink(local_path)
                except Exception:
                    pass
                return None

            return os.path.abspath(local_path)

        except Exception as e:
            try:
                if os.path.exists(local_path):
                    os.unlink(local_path)
            except Exception:
                pass
            self.notifier.send(
                "❌ write_jsonl_stream error\n"
                f"```path: {local_path}\nerror: {e}```"
            )
            try:
                self.log.exception("write_jsonl_stream failed path=%s", local_path)
            except Exception:
                pass
            raise

    # ------------------------- GCS upload -------------------------

    def upload_to_gcs(self, local_path: str, dest_blob: str) -> str:
        """
        Upload a local file to GCS. Deletes the local file on success.

        Parameters
        ----------
        local_path : str
            Path to the local file.
        dest_blob : str
            Destination blob/path relative to the bucket/prefix.

        Returns
        -------
        str
            GCS URI (gs://...).

        Raises
        ------
        Exception
            Re-raises any upload error after Slack/log.
        """
        bucket_uri = self.cfg.bucket
        if not bucket_uri.startswith("gs://"):
            raise ValueError("GCS_BUCKET must start with gs://")

        _, _, tail = bucket_uri.partition("gs://")
        bucket_name, _, base_prefix = tail.partition("/")
        dest = (base_prefix.rstrip("/") + "/" + dest_blob.lstrip("/")) if base_prefix else dest_blob.lstrip("/")

        try:
            blob = self.storage.bucket(bucket_name).blob(dest)
            blob.upload_from_filename(local_path)
            uri = f"gs://{bucket_name}/{dest}"
            try:
                os.unlink(local_path)
            except Exception:
                pass
            self.log.info("gcs_upload ok uri=%s", uri)
            return uri
        except Exception as e:
            self.notifier.send(
                "❌ GCS upload failed\n"
                f"```bucket: {bucket_name}\n dest: {dest}\n path: {local_path}\n error: {e}```"
            )
            self.log.exception("gcs_upload failed dest=%s path=%s", dest, local_path)
            raise
