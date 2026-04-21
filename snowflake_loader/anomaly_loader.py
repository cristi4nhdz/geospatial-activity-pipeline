# snowflake_loader/anomaly_loader.py
"""
Anomaly Loader Module

Reads scored anomaly event JSON files from imagery/events/
and loads each event into the Snowflake anomaly_events table.
"""
import json
import logging
from pathlib import Path
import snowflake.connector
from config.config_loader import config
from config.logging_config import setup_logging

import os

# Only set up logging if not running inside Airflow
if not os.environ.get("AIRFLOW_CTX_DAG_ID"):
    setup_logging("sentinel_fetch.log")
logger = logging.getLogger(__name__)

EVENTS_DIR = Path("imagery/events")


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


def record_exists(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    date_old: str,
    date_new: str,
    row_px: int,
    col_px: int,
) -> bool:
    """
    Check if an anomaly event already exists in Snowflake.

    Args:
        cur: Active Snowflake cursor.
        date_old: Older date string.
        date_new: Newer date string.
        row_px: Patch row pixel offset.
        col_px: Patch col pixel offset.

    Returns:
        bool: True if record exists, False otherwise.
    """
    cur.execute(
        """
        SELECT COUNT(*) FROM anomaly_events
        WHERE date_old = %s AND date_new = %s
        AND row_px = %s AND col_px = %s
    """,
        (date_old, date_new, row_px, col_px),
    )
    return cur.fetchone()[0] > 0


def load_events(
    event_file: Path, cur: snowflake.connector.cursor.SnowflakeCursor
) -> int:
    """
    Load anomaly events from a single JSON file into Snowflake.

    Args:
        event_file: Path to the anomaly events JSON file.
        cur: Active Snowflake cursor.

    Returns:
        int: Number of events loaded.
    """
    date_parts = event_file.stem.replace("anomalies_", "").split("_vs_")
    date_old = date_parts[0]
    date_new = date_parts[1]

    with open(event_file, "r", encoding="utf-8") as f:
        events = json.load(f)

    loaded = 0
    skipped = 0

    for event in events:
        if record_exists(cur, date_old, date_new, event["row"], event["col"]):
            skipped += 1
            continue

        cur.execute(
            """
            INSERT INTO anomaly_events (
                date_old, date_new, row_px, col_px, patch_size,
                mean_delta, max_delta, ndvi_score, cnn_score,
                confidence, detected_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
        """,
            (
                date_old,
                date_new,
                event["row"],
                event["col"],
                event["patch_size"],
                event["mean_delta"],
                event["max_delta"],
                event["ndvi_score"],
                event["cnn_score"],
                event["confidence"],
                event["detected_at"],
            ),
        )
        loaded += 1

    logger.info("File: %s | Loaded: %s | Skipped: %s", event_file.name, loaded, skipped)
    return loaded


def main() -> None:
    """
    Load all anomaly event JSON files into Snowflake.
    """
    event_files = list(EVENTS_DIR.glob("anomalies_*.json"))

    if not event_files:
        logger.warning("No anomaly event files found in %s", EVENTS_DIR)
        return

    conn = get_connection()
    cur = conn.cursor()

    total_loaded = 0
    try:
        for event_file in event_files:
            total_loaded += load_events(event_file, cur)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    logger.info("Anomaly loader complete, %s total events loaded", total_loaded)


if __name__ == "__main__":
    main()
