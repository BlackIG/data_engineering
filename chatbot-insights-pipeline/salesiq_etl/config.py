"""
config.py
---------
Centralized configuration loading for the SalesIQ ETL service.

Reads environment variables (12-factor style) and exposes a single
Settings dataclass. Raises early if required variables are missing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _require_env(name: str) -> str:
    """Return the env var or raise a RuntimeError with a clear message."""
    v = os.getenv(name).strip().strip('\'"')
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable application settings loaded from environment variables."""

    project: str
    dataset: str
    bucket: str  # e.g., gs://bucket[/prefix]
    siq_client_id: str
    siq_client_secret: str
    siq_refresh_token: str
    siq_department_id: str
    siq_base_url: str  # e.g., https://salesiq.zoho.com
    api_version: str   # e.g., v2
    screen_name: str
    slack_webhook: str | None
    tz_name: str = "Africa/Lagos"
    bq_location: str = "EU"  # BigQuery dataset location (EU/US/…)


def load_settings() -> Settings:
    """
    Load and validate settings from environment variables.

    Returns
    -------
    Settings
        A frozen dataclass with all configuration values.
    """
    return Settings(
        project=_require_env("GOOGLE_CLOUD_PROJECT"),
        dataset=_require_env("BQ_DATASET"),
        bucket=_require_env("GCS_BUCKET"),
        siq_client_id=_require_env("SALESIQ_CLIENT_ID"),
        siq_client_secret=_require_env("SALESIQ_CLIENT_SECRET"),
        siq_refresh_token=_require_env("SALESIQ_REFRESH_TOKEN"),
        siq_department_id=_require_env("SALESIQ_DEPARTMENT_ID"),
        siq_base_url=_require_env("SALES_IQ_BASE_URL"),
        api_version=_require_env("API_VERSION"),
        screen_name=_require_env("SCREEN_NAME"),
        slack_webhook=os.getenv("SLACK_WEBHOOK"),
    )
