"""
loader.py
---------
BigQuery loads (server-side from GCS) and post-load stored procedures.
"""

from __future__ import annotations

from typing import List
from google.cloud import bigquery


class Loader:
    """
    BigQuery loader and post-load runner.

    Parameters
    ----------
    cfg : Settings
        App settings (project, dataset, location).
    log : logging.Logger
        Logger.
    notifier : Notifier
        Slack notifier for failures.
    """

    def __init__(self, cfg, log, notifier):
        self.cfg = cfg
        self.log = log
        self.notifier = notifier
        self.bq = bigquery.Client(project=cfg.project)

    # ------------------------- utils -------------------------

    def ensure_table(self, dataset: str, table: str, schema: List[bigquery.SchemaField]) -> None:
        """Create table if missing (idempotent)."""
        table_id = f"{self.cfg.project}.{dataset}.{table}"
        try:
            self.bq.get_table(table_id)
        except Exception:
            self.bq.create_table(bigquery.Table(table_id, schema=schema))
            self.log.info("bq_table created table_id=%s", table_id)

    # ------------------------- loads -------------------------

    def load_jsonl_uri(
        self,
        dataset: str,
        table: str,
        gcs_uri: str,
        schema: List[bigquery.SchemaField],
        write_disposition: str = "WRITE_TRUNCATE",
        location: str | None = None,
    ) -> None:
        """
        Load a JSONL file from GCS into BigQuery.

        Raises on failure after Slack notification.
        """
        table_id = f"{self.cfg.project}.{dataset}.{table}"
        self.ensure_table(dataset, table, schema)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
        )
        loc = location or self.cfg.bq_location
        self.log.info("bq_load start table=%s uri=%s", table_id, gcs_uri)
        try:
            job = self.bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config, location=loc)
            job.result()
            self.log.info("bq_load done table=%s", table_id)
        except Exception as e:
            self.notifier.send(
                "❌ BigQuery load failed\n"
                f"```table: {table_id}\nuri: {gcs_uri}\nerror: {e}```"
            )
            self.log.exception("bq_load failed table=%s uri=%s", table_id, gcs_uri)
            raise

    # ------------------------- procs -------------------------

    def run_upserts(self) -> None:
        """
        Execute post-load stored procedures.

        NOTE: Update these statements to match your project/dataset if needed.
        """
        procs = [
            "CALL `gold-courage-194810.zoho_desk.spUpsertTagDetails`();",
            "CALL `gold-courage-194810.zoho_desk.spUpsertConversations`();",
        ]
        for stmt in procs:
            self.log.info("bq_proc start: %s", stmt.strip())
            job = self.bq.query(stmt)
            job.result()
            self.log.info("bq_proc done: %s", stmt.strip())
