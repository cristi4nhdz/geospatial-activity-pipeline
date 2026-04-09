# ingestion/ais_producer.py
"""
AIS Producer Module

This module connects to the AISStream WebSocket API, processes
incoming vessel data, normalizes it, and publishes it to a Kafka topic.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
import websockets
from kafka import KafkaProducer
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("ais_producer.log")

logger = logging.getLogger(__name__)


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


def normalize_vessel(raw: dict) -> dict | None:
    """
    Normalize raw AIS vessel data into a structured format.

    Extracts relevant metadata and position report fields,
    filtering out unsupported message types.

    Args:
        raw (dict): Raw AIS message from the WebSocket stream.

    Returns:
        dict | None: Normalized vessel record, or None if not applicable.
    """
    try:
        meta = raw.get("MetaData", {})
        msg = raw.get("Message", {})

        # AIS message type 1/2/3, position report
        pos = (
            msg.get("PositionReport")
            or msg.get("StandardClassBPositionReport")
            or msg.get("ExtendedClassBPositionReport")
        )

        if not pos:
            return None

        return {
            "mmsi": meta.get("MMSI"),
            "vessel_name": meta.get("ShipName", "").strip(),
            "latitude": meta.get("latitude"),
            "longitude": meta.get("longitude"),
            "speed_knots": pos.get("Sog"),
            "heading": pos.get("TrueHeading"),
            "course": pos.get("Cog"),
            "nav_status": pos.get("NavigationalStatus"),
            "timestamp": meta.get("time_utc") or datetime.now(timezone.utc).isoformat(),
            "source": "aisstream",
        }
    except Exception as e:
        logger.warning("Failed to normalize AIS message: %s", e)
        return None


async def stream_vessels(producer: KafkaProducer) -> None:
    """
    Stream vessel data from AISStream and publish to Kafka.

    Establishes a WebSocket connection, subscribes to a bounding box,
    processes incoming messages, and sends normalized vessel data
    to the configured Kafka topic.

    Args:
        producer (KafkaProducer): Kafka producer instance.
    """
    topic = config["kafka"]["topics"]["vessels"]
    api_key = config["ais"]["api_key"]
    ws_url = config["ais"]["ws_url"]
    bbox = config["ais"]["bounding_box"]

    subscribe_msg = {
        "APIKey": api_key,
        "BoundingBoxes": [
            [[bbox["min_lat"], bbox["min_lon"]], [bbox["max_lat"], bbox["max_lon"]]]
        ],
    }

    logger.info("Connecting to AISStream, topic: %s", topic)

    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps(subscribe_msg))
        logger.info("Subscribed to AISStream, waiting for vessel data")

        async for raw_msg in ws:
            try:
                raw = json.loads(raw_msg)
                vessel = normalize_vessel(raw)

                if vessel and vessel["mmsi"]:
                    producer.send(topic, value=vessel)
                    logger.info(
                        "Published | MMSI: %s | Name: %s | Lat: %s Lon: %s | Speed: %skn",
                        vessel["mmsi"],
                        vessel["vessel_name"],
                        vessel["latitude"],
                        vessel["longitude"],
                        vessel["speed_knots"],
                    )

            except json.JSONDecodeError as e:
                logger.warning("Bad JSON from AISStream: %s", e)


def main() -> None:
    """
    Run the AIS producer.

    Initializes the Kafka producer and starts the asynchronous
    vessel streaming loop.
    """
    producer = build_producer()
    logger.info("AIS producer started")
    try:
        asyncio.run(stream_vessels(producer))
    except KeyboardInterrupt:
        logger.info("Shutting down AIS producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
