# dags/imagery_pipeline_dag.py
"""
Imagery Pipeline DAG

Orchestrates the full Sentinel-2 imagery pipeline:
fetch -> process -> upload -> change detection -> anomaly scoring

Scheduled every 5 days to match Sentinel-2 revisit time.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "geo-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def fetch_tile() -> None:
    from imagery.sentinel_fetch import main

    main()


def process_tile() -> None:
    from imagery.tile_processor import main

    main()


def upload_tile() -> None:
    from imagery.tile_uploader import main

    main()


def run_change_detection() -> None:
    from imagery.change_detection import main

    main()


def run_anomaly_scorer() -> None:
    from imagery.anomaly_scorer import main

    main()


with DAG(
    dag_id="imagery_pipeline",
    description="Sentinel-2 fetch, process, upload, change detection, and anomaly scoring",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["imagery", "sentinel", "change-detection"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_sentinel_tile",
        python_callable=fetch_tile,
    )

    t2 = PythonOperator(
        task_id="process_tile",
        python_callable=process_tile,
    )

    t3 = PythonOperator(
        task_id="upload_tile_to_minio",
        python_callable=upload_tile,
    )

    t4 = PythonOperator(
        task_id="run_change_detection",
        python_callable=run_change_detection,
    )

    t5 = PythonOperator(
        task_id="score_anomalies",
        python_callable=run_anomaly_scorer,
    )

    t1 >> t2 >> t3 >> t4 >> t5
