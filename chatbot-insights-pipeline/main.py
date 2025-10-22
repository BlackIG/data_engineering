"""
main.py
-------
End-to-end orchestrator for the Zoho SalesIQ → GCS → BigQuery ETL.

Flow:
  1) Load settings & bootstrap logging/notifier
  2) Get OAuth token from Zoho
  3) Compute incremental time window (from BigQuery) or fallback to yesterday
  4) List conversation IDs for the window
     - If zero-run → optional BQ log row + Slack info, then exit
  5) Fetch conversation details → JSONL → GCS → BigQuery (tagsDelta)
  6) Export chat JSON → JSONL → GCS
  7) Transform exported chats → JSONL → GCS → BigQuery (conversationDelta)
  8) Run BigQuery stored procedures to upsert final tables
  9) Slack success notification
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from google.cloud import bigquery


from salesiq_etl import (
    load_settings, get_logger,log_section, Notifier, SalesIQExtractor, Sink,
    map_conversation_details, extract_one_chat, Loader,
    gcs_open_jsonl, incremental_window_ms_from_bq, yesterday_window_ms
)


# ------------------------------------------------------------------------------
# BigQuery Schemas (match your existing tables)
# ------------------------------------------------------------------------------

salesiq_conversation_runs_schema = [
    bigquery.SchemaField("run_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("department_id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("from_time", "TIMESTAMP"),
    bigquery.SchemaField("to_time", "TIMESTAMP"),
    bigquery.SchemaField("num_conversations", "INT64"),
    bigquery.SchemaField("gcs_uri", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
]

salesiq_conversation_tags_schema = [
    bigquery.SchemaField("chat_id", "STRING"),
    bigquery.SchemaField("system_chat_id", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("start_time", "TIMESTAMP"),
    bigquery.SchemaField("end_time", "TIMESTAMP"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("tags", "JSON"),
    bigquery.SchemaField("ticket_id", "STRING"),
]

sales_iq_conversations_schema = [
    bigquery.SchemaField("chat_id", "STRING"),
    bigquery.SchemaField("chat_start_time", "TIMESTAMP"),
    bigquery.SchemaField("chat_end_time", "TIMESTAMP"),
    bigquery.SchemaField("operating_system", "STRING"),
    bigquery.SchemaField("first_message", "STRING"),
    bigquery.SchemaField("visitor_name", "STRING"),
    bigquery.SchemaField("visitor_email", "STRING"),
    bigquery.SchemaField("happiness_rating", "INT64"),
    bigquery.SchemaField("feedback", "STRING"),
    bigquery.SchemaField("chat_closed_by", "STRING"),
    bigquery.SchemaField("chat", "JSON"),
    bigquery.SchemaField("run_timestamp", "TIMESTAMP"),
]


# ------------------------------------------------------------------------------
# Optional run-log helper (single-row insert to a log table)
# ------------------------------------------------------------------------------

def log_run_to_bigquery(
    project: str,
    dataset: str,
    table: str,
    row: Dict[str, Any],
    schema: List[bigquery.SchemaField],
    location: str = "EU",
    log=None,
    notifier: Optional[Notifier] = None,
) -> None:
    """
    Ensure the log table exists and insert one JSON row.

    Parameters
    ----------
    project, dataset, table : str
        Target table identifier pieces.
    row : Dict[str, Any]
        Row to insert as JSON (must match schema).
    schema : List[bigquery.SchemaField]
        Table schema to create on first use.
    location : str
        BigQuery location (should match dataset region).
    log : logging.Logger
        Logger to write progress/failures to.
    notifier : Notifier | None
        Slack notifier for failures.
    """
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    # Ensure table exists
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=schema))
        if log:
            log.info("run_log table created %s", table_id)

    # Insert one row
    try:
        errors = client.insert_rows_json(table_id, [row], location=location)
        if errors and log:
            log.warning("run_log insert warnings: %s", errors)
    except Exception as e:
        if notifier:
            notifier.send(
                "⚠️ BigQuery run-log insert failed\n"
                f"```table: {table_id}\nerror: {e}\nrow: {row}```"
            )
        if log:
            log.exception("run_log insert failed table=%s", table_id)
        # non-fatal for pipeline
        return


# ------------------------------------------------------------------------------
# Main Orchestrator
# ------------------------------------------------------------------------------

def main() -> None:
    """
    Execute the full ETL job once.
    Designed to be called by app.py (/run) or directly from CLI.
    """
    cfg = load_settings()
    log = get_logger("salesiq")
    notify = Notifier(cfg.slack_webhook, log)
    extractor = SalesIQExtractor(cfg, log)
    sink = Sink(cfg, log, notify)
    loader = Loader(cfg, log, notify)

    RUN_ID = uuid.uuid4().hex
    log.info("run_start run_id=%s", RUN_ID)

    # 1) OAuth token
    with log_section(log, "oauth-token"):
        try:
            access_token = extractor.get_access_token()
        except Exception as e:
            notify.send(
                "❌ OAuth token fetch failed\n"
                f"```error: {e}```"
            )
            raise

    # 2) Window (incremental from BigQuery; fallback to yesterday)
    with log_section(log, "window-calc", tz=cfg.tz_name):
        try:
            from_ms, to_ms, ymd = incremental_window_ms_from_bq(project=cfg.project, tz_name=cfg.tz_name)
        except Exception as e:
            notify.send(
                "❌ Window calculation failed (BQ)\n"
                f"```project: {cfg.project}\nerror: {e}```"
            )
            # Fallback for resiliency (still report via Slack)
            from_ms, to_ms, ymd = yesterday_window_ms(cfg.tz_name)
            notify.send(
                "ℹ️ Falling back to yesterday_window_ms\n"
                f"```ymd: {ymd}\nfrom: {from_ms}\nto: {to_ms}```"
            )

    # 3) List conversation IDs
    with log_section(log, "list-conversations", ymd=ymd):
        try:
            status_filter = "closed"  # adjust as needed
            ids = extractor.fetch_conversation_ids(
                access_token=access_token,
                status=status_filter,
                from_ms=from_ms,
                to_ms=to_ms,
                limit=99,
            )
            log.info("conversation_ids count=%d", len(ids))
        except Exception as e:
            notify.send(
                "❌ List conversations failed\n"
                f"```ymd: {ymd}\nerror: {e}```"
            )
            raise

        if not ids:
            # Optional: write a zero-run log row
            try:
                row = {
                    "run_timestamp": datetime.now(timezone.utc).isoformat(),
                    "department_id": cfg.siq_department_id,
                    "status": status_filter or "ALL",
                    "from_time": datetime.fromtimestamp(from_ms/1000, tz=timezone.utc).isoformat(),
                    "to_time": datetime.fromtimestamp(to_ms/1000, tz=timezone.utc).isoformat(),
                    "num_conversations": 0,
                    "gcs_uri": "",
                    "run_id": RUN_ID,
                }
                log_run_to_bigquery(
                    project=cfg.project,
                    dataset=cfg.dataset,
                    table=os.getenv("BQ_LOG_TABLE", "salesiq_conversation_runs"),
                    row=row,
                    schema=salesiq_conversation_runs_schema,
                    location=cfg.bq_location,
                    log=log,
                    notifier=notify,
                )
            except Exception:
                # already logged inside helper
                pass

            notify.send("ℹ️ SalesIQ zero-run: no conversations in window")
            log.info("run_end (zero-run) run_id=%s", RUN_ID)
            return

    # 4) Fetch conversation details → JSONL → GCS → BQ (tagsDelta)
    details_uri = None
    details_count = 0
    with log_section(log, "details-fetch-upload-load", ymd=ymd):
        def _iter_details():
            nonlocal details_count
            for cid in ids:
                try:
                    raw = extractor.get_conversation_details(access_token, cid)
                    details_count += 1
                    yield map_conversation_details(raw, str(cid))
                except Exception as e:
                    log.warning("details_fetch_failed id=%s err=%s", cid, e)

        local_details = sink.write_jsonl_stream(_iter_details())
        if local_details:
            dest_blob = f"salesiq/details/date={ymd}/details_{RUN_ID}.jsonl"
            details_uri = sink.upload_to_gcs(local_details, dest_blob)
            loader.load_jsonl_uri(
                dataset=cfg.dataset,
                table="salesiq_conversation_tagsDelta",
                gcs_uri=details_uri,
                schema=salesiq_conversation_tags_schema,
                write_disposition="WRITE_TRUNCATE",
                location=cfg.bq_location,
            )

    # 5) Export raw chats → JSONL → GCS
    chats_uri = None
    exports = 0
    with log_section(log, "export-chats-upload", ymd=ymd):
        def _iter_exports():
            nonlocal exports
            for cid in ids:
                try:
                    obj = extractor.export_chat_json(access_token, cid)
                    exports += 1
                    yield obj
                except Exception as e:
                    log.warning("export_failed id=%s err=%s", cid, e)

        local_chats = sink.write_jsonl_stream(_iter_exports())
        if local_chats:
            dest_blob = f"salesiq/chats/date={ymd}/chats_{RUN_ID}.jsonl"
            chats_uri = sink.upload_to_gcs(local_chats, dest_blob)

    # 6) Transform exported chats → JSONL → GCS → BQ (conversationDelta)
    loaded_any = False
    with log_section(log, "transform-and-load-chats", ymd=ymd):
        if chats_uri:
            # Transform by streaming back from GCS
            records: List[Dict[str, Any]] = []
            try:
                for obj in gcs_open_jsonl(chats_uri, project=cfg.project):
                    try:
                        records.append(extract_one_chat(obj))
                    except Exception as e:
                        log.warning("transform_one_chat_failed err=%s", e)
            except Exception as e:
                notify.send(
                    "❌ Failed to read exported chats from GCS\n"
                    f"```uri: {chats_uri}\nerror: {e}```"
                )
                raise

            if records:
                # Spill transformed to JSONL (temp) and upload
                def _iter_records():
                    for r in records:
                        yield r

                local_transformed = sink.write_jsonl_stream(_iter_records())
                if local_transformed:
                    transformed_blob = f"salesiq/transformed/date={ymd}/chats_transformed_{RUN_ID}.jsonl"
                    transformed_uri = sink.upload_to_gcs(local_transformed, transformed_blob)

                    # Server-side load to Delta table
                    loader.load_jsonl_uri(
                        dataset=cfg.dataset,
                        table="salesiq_conversationDelta",
                        gcs_uri=transformed_uri,
                        schema=sales_iq_conversations_schema,
                        write_disposition="WRITE_TRUNCATE",
                        location=cfg.bq_location,
                    )
                    loaded_any = True
        else:
            log.info("no_chats_uri → skip transform/load")

    # 7) Post-load stored procedures
    with log_section(log, "post-load-upserts"):
        if loaded_any or details_uri:
            try:
                loader.run_upserts()
            except Exception as e:
                notify.send(
                    "❌ Post-load stored procedures failed\n"
                    f"```error: {e}```"
                )
                raise
        else:
            log.info("skip upserts: nothing loaded this run")

    # 8) Success notify
    notify.send(
        "✅ Export SalesIQ chat - Run Success\n"
        "```"
        f"run_id: {RUN_ID}\n"
        f"ymd: {ymd}\n"
        f"window: {datetime.fromtimestamp(from_ms/1000, tz=timezone.utc).isoformat()} → "
        f"{datetime.fromtimestamp(to_ms/1000, tz=timezone.utc).isoformat()}\n"
        f"exports: chats={exports} details={details_count}\n"
        f"uris: chats={chats_uri or '(none)'} details={details_uri or '(none)'}\n"
        "```"
    )
    log.info("run_end run_id=%s", RUN_ID)


if __name__ == "__main__":
    main()
