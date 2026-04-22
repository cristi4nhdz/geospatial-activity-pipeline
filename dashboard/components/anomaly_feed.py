# dashboard/components/anomaly_feed.py
"""
Anomaly Feed Component

Fetches scored anomaly events from Snowflake
and formats them for dashboard display.
"""

from __future__ import annotations
import logging
from contextlib import closing
import pandas as pd
import snowflake.connector
import streamlit as st
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("dashboard.log")
logger = logging.getLogger(__name__)


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Create and return a Snowflake connection.

    Returns:
        snowflake.connector.SnowflakeConnection: Active connection.
    """
    sf = config["snowflake"]
    return snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=sf["password"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"],
    )


def _normalize_anomaly_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names and types for downstream dashboard use.

    Lowercases all column names, coerces numeric columns to float,
    and coerces datetime columns to pandas Timestamp.

    Args:
        df: Raw DataFrame from Snowflake query.

    Returns:
        Normalized DataFrame, or the original empty DataFrame if input is empty.
    """
    if df.empty:
        return df

    df = df.copy()
    df.columns = df.columns.str.lower()

    numeric_cols = [
        "row_px",
        "col_px",
        "patch_size",
        "mean_delta",
        "max_delta",
        "ndvi_score",
        "cnn_score",
        "confidence",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    datetime_cols = ["date_old", "date_new", "detected_at"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


@st.cache_data(ttl=60, show_spinner=False)
def fetch_anomaly_events(limit: int = 50) -> pd.DataFrame:
    """
    Fetch top anomaly events from Snowflake ordered by confidence.

    Args:
        limit: Maximum number of events to return.

    Returns:
        DataFrame with anomaly event data.
    """
    query = """
        SELECT
            date_old,
            date_new,
            row_px,
            col_px,
            patch_size,
            mean_delta,
            max_delta,
            ndvi_score,
            cnn_score,
            confidence,
            detected_at
        FROM anomaly_events
        ORDER BY confidence DESC, detected_at DESC
        LIMIT %(limit)s
    """

    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql(query, conn, params={"limit": int(limit)})
        df = _normalize_anomaly_df(df)
        logger.info("Fetched %s anomaly events", len(df))
        return df
    except Exception:
        logger.exception("Failed to fetch anomaly events")
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def fetch_anomaly_events_above_threshold(
    threshold: float = 0.0,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Fetch anomaly events above a confidence threshold.

    Args:
        threshold: Minimum confidence score to include.
        limit: Optional maximum number of rows.

    Returns:
        DataFrame with filtered anomaly events.
    """
    query = """
        SELECT
            date_old,
            date_new,
            row_px,
            col_px,
            patch_size,
            mean_delta,
            max_delta,
            ndvi_score,
            cnn_score,
            confidence,
            detected_at
        FROM anomaly_events
        WHERE confidence >= %(threshold)s
        ORDER BY confidence DESC, detected_at DESC
    """
    params: dict[str, float | int] = {"threshold": float(threshold)}

    if limit is not None:
        query += "\nLIMIT %(limit)s"
        params["limit"] = int(limit)

    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql(query, conn, params=params)
        return _normalize_anomaly_df(df)
    except Exception:
        logger.exception("Failed to fetch anomaly events above threshold")
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def fetch_anomaly_summary() -> dict:
    """
    Fetch summary statistics for anomaly events.

    Returns:
        dict with total events, avg confidence, top confidence, date pairs.
    """
    query = """
        SELECT
            COUNT(*) AS total_events,
            ROUND(AVG(confidence), 4) AS avg_confidence,
            ROUND(MAX(confidence), 4) AS top_confidence,
            COUNT(DISTINCT TO_VARCHAR(date_old) || '_' || TO_VARCHAR(date_new)) AS date_pairs,
            MIN(date_old) AS earliest_date,
            MAX(date_new) AS latest_date
        FROM anomaly_events
    """

    default_result = {
        "total_events": 0,
        "avg_confidence": 0.0,
        "top_confidence": 0.0,
        "date_pairs": 0,
        "earliest_date": "N/A",
        "latest_date": "N/A",
    }

    try:
        with closing(get_connection()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(query)
                row = cursor.fetchone()

        if not row:
            return default_result

        return {
            "total_events": int(row[0] or 0),
            "avg_confidence": float(row[1] or 0.0),
            "top_confidence": float(row[2] or 0.0),
            "date_pairs": int(row[3] or 0),
            "earliest_date": str(row[4]) if row[4] is not None else "N/A",
            "latest_date": str(row[5]) if row[5] is not None else "N/A",
        }
    except Exception:
        logger.exception("Failed to fetch anomaly summary")
        return default_result
