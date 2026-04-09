# ingestion/consumers/aircraft_consumer.py
"""
Aircraft Consumer Module

Reads normalized ADS-B aircraft records from Kafka and inserts
them into the aircraft_tracks table in PostGIS.
"""
import json
import logging
from datetime import datetime, timezone
import psycopg2
from kafka import KafkaConsumer
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("aircraft_consumer.log")
logger = logging.getLogger(__name__)


def build_consumer() -> KafkaConsumer:
    """
    Create and configure a Kafka consumer for the aircraft topic.

    Returns:
        KafkaConsumer: Configured consumer instance.
    """
    return KafkaConsumer(
        config["kafka"]["topics"]["aircraft"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="aircraft-consumer-group",
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
    INSERT INTO aircraft_tracks (
        icao24, callsign, origin_country, geom,
        altitude_m, velocity_ms, heading, vertical_rate,
        squawk, source, received_at
    )
    VALUES (
        %(icao24)s, %(callsign)s, %(origin_country)s,
        ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
        %(altitude_m)s, %(velocity_ms)s, %(heading)s, %(vertical_rate)s,
        %(squawk)s, %(source)s, %(received_at)s
    )
"""


def insert_aircraft(cursor: psycopg2.extensions.cursor, aircraft: dict) -> None:
    """
    Insert a normalized aircraft record into aircraft_tracks.

    Args:
        cursor: Active psycopg2 cursor.
        aircraft: Normalized aircraft dict from Kafka.
    """
    if not aircraft.get("latitude") or not aircraft.get("longitude"):
        return

    aircraft["received_at"] = datetime.now(timezone.utc)
    cursor.execute(INSERT_SQL, aircraft)


def main() -> None:
    """
    Run the aircraft consumer loop.

    Continuously reads from adsb.aircraft and inserts
    records into PostGIS aircraft_tracks table.
    """
    consumer = build_consumer()
    conn = build_connection()
    cursor = conn.cursor()

    logger.info(
        "Aircraft consumer started, listening on %s",
        config["kafka"]["topics"]["aircraft"],
    )

    try:
        for message in consumer:
            aircraft = message.value
            try:
                insert_aircraft(cursor, aircraft)
                conn.commit()
                logger.info(
                    "Inserted | ICAO: %s | Callsign: %s | Lat: %s Lon: %s | Alt: %sm",
                    aircraft.get("icao24"),
                    aircraft.get("callsign"),
                    aircraft.get("latitude"),
                    aircraft.get("longitude"),
                    aircraft.get("altitude_m"),
                )
            except Exception as e:
                conn.rollback()
                logger.warning("Failed to insert aircraft: %s", e)

    except KeyboardInterrupt:
        logger.info("Shutting down aircraft consumer")
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
