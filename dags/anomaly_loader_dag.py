# dags/anomaly_loader_dag.py
"""
Anomaly Loader DAG

Loads scored anomaly events from imagery/events/ JSON files
into Snowflake for warehousing and downstream querying.

Scheduled daily.
"""
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "geo-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

EVENTS_DIR = Path("/opt/airflow/imagery/events")


def load_anomalies_to_snowflake() -> None:
    """
    Read all anomaly event JSON files from imagery/events/
    and load each event into Snowflake anomaly_events table.
    """
    import snowflake.connector
    from config.config_loader import config

    sf = config["snowflake"]
    conn = snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=sf["password"],
        database=sf["database"],
        schema=sf["schema"],
        warehouse=sf["warehouse"],
    )
    cursor = conn.cursor()

    # create table if not exists
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS anomaly_events (
            id INTEGER AUTOINCREMENT PRIMARY KEY,
            date_old VARCHAR,
            date_new VARCHAR,
            row_px INTEGER,
            col_px INTEGER,
            patch_size INTEGER,
            mean_delta FLOAT,
            max_delta FLOAT,
            ndvi_score FLOAT,
            cnn_score FLOAT,
            confidence FLOAT,
            detected_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    event_files = list(EVENTS_DIR.glob("anomalies_*.json"))
    if not event_files:
        print(f"No anomaly event files found in {EVENTS_DIR}")
        return

    total_loaded = 0
    for event_file in event_files:
        date_parts = event_file.stem.replace("anomalies_", "").split("_vs_")
        date_old = date_parts[0]
        date_new = date_parts[1]

        with open(event_file, "r", encoding="utf-8") as f:
            events = json.load(f)

        for event in events:
            cursor.execute(
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
            total_loaded += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {total_loaded} anomaly events into Snowflake")


with DAG(
    dag_id="anomaly_loader",
    description="Loads scored anomaly events from JSON files into Snowflake",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["snowflake", "anomaly", "loader"],
) as dag:

    t1 = PythonOperator(
        task_id="load_anomalies_to_snowflake",
        python_callable=load_anomalies_to_snowflake,
    )
