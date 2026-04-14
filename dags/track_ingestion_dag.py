# dags/track_ingestion_dag.py
"""
Track Ingestion DAG

Orchestrates the AIS vessel and ADS-B aircraft Kafka consumers,
reading from Kafka topics and upserting tracks into PostGIS.
Runs the lag monitor after each ingestion window to check health.

Scheduled hourly.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "geo-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def run_vessel_consumer() -> None:
    """
    Run the vessel consumer for a fixed window then exit.
    Reads from ais.vessels and upserts into PostGIS vessel_tracks.
    """
    import json
    import psycopg2
    from kafka import KafkaConsumer
    from datetime import datetime, timezone
    from config.config_loader import config

    db = config["postgis"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["db"],
        user=db["user"],
        password=db["password"],
    )
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        config["kafka"]["topics"]["vessels"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="vessel-consumer-group",
        consumer_timeout_ms=30000,
    )

    INSERT_SQL = """
        INSERT INTO vessel_tracks (
            mmsi, vessel_name, geom, speed_knots, heading,
            course, nav_status, source, received_at
        ) VALUES (
            %(mmsi)s, %(vessel_name)s,
            ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
            %(speed_knots)s, %(heading)s, %(course)s,
            %(nav_status)s, %(source)s, %(received_at)s
        )
    """

    count = 0
    for message in consumer:
        vessel = message.value
        if vessel.get("latitude") and vessel.get("longitude"):
            vessel["received_at"] = datetime.now(timezone.utc)
            try:
                cursor.execute(INSERT_SQL, vessel)
                conn.commit()
                count += 1
            except Exception:
                conn.rollback()

    cursor.close()
    conn.close()
    consumer.close()
    print(f"Vessel consumer finished - {count} records upserted")


def run_aircraft_consumer() -> None:
    """
    Run the aircraft consumer for a fixed window then exit.
    Reads from adsb.aircraft and inserts into PostGIS aircraft_tracks.
    """
    import json
    import psycopg2
    from datetime import datetime, timezone
    from config.config_loader import config

    db = config["postgis"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["db"],
        user=db["user"],
        password=db["password"],
    )
    cursor = conn.cursor()

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        config["kafka"]["topics"]["aircraft"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="aircraft-consumer-group",
        consumer_timeout_ms=30000,
    )

    INSERT_SQL = """
        INSERT INTO aircraft_tracks (
            icao24, callsign, origin_country, geom,
            altitude_m, velocity_ms, heading, vertical_rate,
            squawk, source, received_at
        ) VALUES (
            %(icao24)s, %(callsign)s, %(origin_country)s,
            ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
            %(altitude_m)s, %(velocity_ms)s, %(heading)s,
            %(vertical_rate)s, %(squawk)s, %(source)s, %(received_at)s
        )
    """

    count = 0
    for message in consumer:
        aircraft = message.value
        if aircraft.get("latitude") and aircraft.get("longitude"):
            aircraft["received_at"] = datetime.now(timezone.utc)
            try:
                cursor.execute(INSERT_SQL, aircraft)
                conn.commit()
                count += 1
            except Exception:
                conn.rollback()

    cursor.close()
    conn.close()
    consumer.close()
    print(f"Aircraft consumer finished - {count} records inserted")


def run_lag_monitor() -> None:
    """
    Run the Kafka consumer lag monitor and print results.
    """
    from ingestion.consumers.lag_monitor import get_lag
    from config.config_loader import config

    bootstrap = config["kafka"]["bootstrap_servers"]
    topics = config["kafka"]["topics"]

    for group, topic in [
        ("vessel-consumer-group", topics["vessels"]),
        ("aircraft-consumer-group", topics["aircraft"]),
    ]:
        lag_data = get_lag(bootstrap, group, topic)
        for partition, data in lag_data.items():
            print(
                f"Group: {group} | Topic: {topic} | "
                f"Partition: {partition} | Lag: {data['lag']}"
            )


with DAG(
    dag_id="track_ingestion",
    description="AIS vessel and ADS-B aircraft Kafka consumers writing to PostGIS",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["ingestion", "kafka", "postgis"],
) as dag:

    t1 = PythonOperator(
        task_id="vessel_consumer",
        python_callable=run_vessel_consumer,
    )

    t2 = PythonOperator(
        task_id="aircraft_consumer",
        python_callable=run_aircraft_consumer,
    )

    t3 = PythonOperator(
        task_id="lag_monitor",
        python_callable=run_lag_monitor,
    )

    [t1, t2] >> t3
