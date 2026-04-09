# ingestion/adsb_producer.py
"""
ADS-B Producer Module

This module fetches live aircraft state data from the OpenSky API,
normalizes it, and publishes it to a Kafka topic.
"""
import json
import time
import logging
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("adsb_producer.log")

logger = logging.getLogger(__name__)

# OpenSky state vector field positions
FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]


def build_producer() -> KafkaProducer:
    """
    Create and configure a Kafka producer instance.

    Returns:
        KafkaProducer: Configured Kafka producer for sending JSON messages.
    """
    return KafkaProducer(
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_aircraft() -> list[dict]:
    """
    Fetch aircraft state vectors from the OpenSky API.

    Applies a geographic bounding box filter and converts raw
    state vectors into dictionaries using predefined field mappings.

    Returns:
        list[dict]: List of raw aircraft state dictionaries.
    """
    url = f"{config['opensky']['base_url']}/states/all"
    bbox = config["opensky"]["bounding_box"]

    params = {
        "lamin": bbox["min_lat"],
        "lamax": bbox["max_lat"],
        "lomin": bbox["min_lon"],
        "lomax": bbox["max_lon"],
    }
    auth = (
        config["opensky"]["username"],
        config["opensky"]["password"],
    )

    try:
        response = requests.get(url, params=params, auth=auth, timeout=10)
        response.raise_for_status()
        data = response.json()
        states = data.get("states") or []
        return [dict(zip(FIELDS, s)) for s in states]
    except requests.RequestException as e:
        logger.warning("OpenSky request failed: %s", e)
        return []


def normalize_aircraft(raw: dict) -> dict | None:
    """
    Normalize raw aircraft data into a structured format.

    Filters out invalid or irrelevant records (e.g., missing coordinates
    or aircraft on the ground) and standardizes field names and units.

    Args:
        raw (dict): Raw aircraft state data.

    Returns:
        dict | None: Normalized aircraft record, or None if filtered out.
    """
    try:
        if raw.get("latitude") is None or raw.get("longitude") is None:
            return None
        if raw.get("on_ground"):
            return None

        return {
            "icao24": raw.get("icao24", "").strip(),
            "callsign": (raw.get("callsign") or "").strip(),
            "origin_country": raw.get("origin_country"),
            "latitude": raw.get("latitude"),
            "longitude": raw.get("longitude"),
            "altitude_m": raw.get("baro_altitude"),
            "velocity_ms": raw.get("velocity"),
            "heading": raw.get("true_track"),
            "vertical_rate": raw.get("vertical_rate"),
            "on_ground": raw.get("on_ground"),
            "squawk": raw.get("squawk"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "opensky",
        }
    except Exception as e:
        logger.warning("Failed to normalize aircraft: %s", e)
        return None


def main() -> None:
    """
    Run the ADS-B producer loop.

    Continuously polls the OpenSky API, normalizes aircraft data,
    and publishes it to a Kafka topic at a fixed interval.
    """
    producer = build_producer()
    topic = config["kafka"]["topics"]["aircraft"]
    poll_interval = 30  # seconds, OpenSky rate limit

    logger.info("ADS-B producer started")
    logger.info("Polling OpenSky every %ss, topic: %s", poll_interval, topic)

    try:
        while True:
            aircraft_list = fetch_aircraft()

            if not aircraft_list:
                logger.info("No aircraft in configured bounding box or request failed")
            else:
                published = 0
                for raw in aircraft_list:
                    aircraft = normalize_aircraft(raw)
                    if aircraft:
                        producer.send(topic, value=aircraft)
                        logger.info(
                            "Published | ICAO: %s | Callsign: %s | Lat: %s Lon: %s | Alt: %sm",
                            aircraft["icao24"],
                            aircraft["callsign"],
                            aircraft["latitude"],
                            aircraft["longitude"],
                            aircraft["altitude_m"],
                        )
                        published += 1

                producer.flush()
                logger.info("Batch done, %s aircraft published", published)

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("Shutting down ADS-B producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
