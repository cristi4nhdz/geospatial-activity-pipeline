# ingestion/consumers/vessel_consumer.py
"""
Vessel Consumer Module

Reads normalized AIS vessel records from Kafka and upserts
them into the vessel_tracks table in PostGIS.
"""
import json
import logging
from datetime import datetime, timezone
import psycopg2
from kafka import KafkaConsumer
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("vessel_consumer.log")
logger = logging.getLogger(__name__)


def build_consumer() -> KafkaConsumer:
    """
    Create and configure a Kafka consumer for the vessels topic.

    Returns:
        KafkaConsumer: Configured consumer instance.
    """
    return KafkaConsumer(
        config["kafka"]["topics"]["vessels"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="vessel-consumer-group",
    )


def build_connection() -> psycopg2.extensions.connection:
    """
    Create a PostGIS database connection.

    Returns:
        psycopg2 connection instance.
    """
    db = config["postgis"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["db"],
        user=db["user"],
        password=db["password"],
    )


INSERT_SQL = """
    INSERT INTO vessel_tracks (
        mmsi, vessel_name, geom,
        speed_knots, heading, course, nav_status,
        source, received_at
    )
    VALUES (
        %(mmsi)s, %(vessel_name)s,
        ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
        %(speed_knots)s, %(heading)s, %(course)s, %(nav_status)s,
        %(source)s, %(received_at)s
    )
"""


def upsert_vessel(cursor: psycopg2.extensions.cursor, vessel: dict) -> None:
    """
    Insert a normalized vessel record into vessel_tracks.

    Args:
        cursor: Active psycopg2 cursor.
        vessel: Normalized vessel dict from Kafka.
    """
    if not vessel.get("latitude") or not vessel.get("longitude"):
        return

    vessel["received_at"] = datetime.now(timezone.utc)
    cursor.execute(INSERT_SQL, vessel)


def main() -> None:
    """
    Run the vessel consumer loop.

    Continuously reads from ais.vessels and upserts
    records into PostGIS vessel_tracks table.
    """
    consumer = build_consumer()
    conn = build_connection()
    cursor = conn.cursor()

    logger.info(
        "Vessel consumer started, listening on %s",
        config["kafka"]["topics"]["vessels"],
    )

    try:
        for message in consumer:
            vessel = message.value
            try:
                upsert_vessel(cursor, vessel)
                conn.commit()
                logger.info(
                    "Upserted | MMSI: %s | Name: %s | Lat: %s Lon: %s",
                    vessel.get("mmsi"),
                    vessel.get("vessel_name"),
                    vessel.get("latitude"),
                    vessel.get("longitude"),
                )
            except Exception as e:
                conn.rollback()
                logger.warning("Failed to upsert vessel: %s", e)

    except KeyboardInterrupt:
        logger.info("Shutting down vessel consumer")
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
