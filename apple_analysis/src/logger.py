# Databricks notebook source
from datetime import datetime


class AbstractJobLogger:
    def __init__(self, job_name):
        self.job_name = job_name

    def log(self):
        pass

class JobLogger(AbstractJobLogger):
    def __init__(self, job_name):
        super().__init__(job_name)

    def log(self, job_status, record_count, remarks):
        # Create a single-row DataFrame with log details
        log_df = spark.createDataFrame([
            (self.job_name, datetime.now(), job_status, record_count, remarks)
        ], ["job_name", "run_timestamp", "status", "record_count", "remarks"])

        # Write the log to a Delta table
        log_df.write.mode("append").format("delta").saveAsTable("workspace.apple_analysis.job_run_log")
