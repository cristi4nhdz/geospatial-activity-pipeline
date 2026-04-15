# snowflake_loader/setup.py
"""
Snowflake Setup Module

Creates the database, schema, and anomaly_events table
in Snowflake if they do not already exist.
Run once before the anomaly loader.
"""
import logging

import snowflake.connector
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("snowflake_setup.log")
logger = logging.getLogger(__name__)

ANOMALY_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS anomaly_events (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    date_old VARCHAR(8),
    date_new VARCHAR(8),
    row_px INTEGER,
    col_px INTEGER,
    patch_size INTEGER,
    mean_delta FLOAT,
    max_delta FLOAT,
    ndvi_score FLOAT,
    cnn_score FLOAT,
    confidence FLOAT,
    detected_at TIMESTAMP_TZ,
    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
"""


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
    )


def setup() -> None:
    """
    Create Snowflake database, schema, and anomaly_events table.
    """
    sf = config["snowflake"]
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {sf['database']}")
        logger.info("Database %s ready", sf["database"])

        cur.execute(f"USE DATABASE {sf['database']}")
        cur.execute(f"USE SCHEMA {sf['schema']}")

        cur.execute(ANOMALY_EVENTS_DDL)
        logger.info("Table anomaly_events ready")

    finally:
        cur.close()
        conn.close()

    logger.info("Snowflake setup complete")


if __name__ == "__main__":
    setup()
