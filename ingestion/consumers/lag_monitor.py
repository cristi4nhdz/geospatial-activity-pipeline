# ingestion/consumers/lag_monitor.py
"""
Kafka Consumer Lag Monitor

Reports the consumer lag for each tracked topic and consumer group.
Lag is the number of messages in Kafka that have not yet been consumed.
"""
import logging
import time
from kafka import KafkaConsumer, TopicPartition
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("lag_monitor.log")
logger = logging.getLogger(__name__)

MONITOR_GROUPS = [
    {
        "group_id": "vessel-consumer-group",
        "topic": config["kafka"]["topics"]["vessels"],
    },
    {
        "group_id": "aircraft-consumer-group",
        "topic": config["kafka"]["topics"]["aircraft"],
    },
]


def get_lag(bootstrap_servers: str, group_id: str, topic: str) -> dict:
    """
    Calculate consumer lag for a given group and topic.

    Args:
        bootstrap_servers: Kafka bootstrap server address.
        group_id: Consumer group ID to check.
        topic: Topic name to check.

    Returns:
        dict with partition, current offset, end offset, and lag.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return {}

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    end_offsets = consumer.end_offsets(topic_partitions)
    committed = {tp: consumer.committed(tp) or 0 for tp in topic_partitions}

    results = {}
    for tp in topic_partitions:
        end = end_offsets[tp]
        current = committed[tp]
        lag = end - current
        results[tp.partition] = {
            "partition": tp.partition,
            "committed_offset": current,
            "end_offset": end,
            "lag": lag,
        }

    consumer.close()
    return results


def report(interval: int = 30) -> None:
    """
    Continuously report lag for all monitored consumer groups.

    Args:
        interval: Seconds between each lag check.
    """
    bootstrap_servers = config["kafka"]["bootstrap_servers"]

    logger.info("Lag monitor started, checking every %ss", interval)

    try:
        while True:
            for entry in MONITOR_GROUPS:
                group_id = entry["group_id"]
                topic = entry["topic"]

                lag_data = get_lag(bootstrap_servers, group_id, topic)

                if not lag_data:
                    logger.warning("No partition data for %s / %s", group_id, topic)
                    continue

                for partition, data in lag_data.items():
                    logger.info(
                        "Group: %s | Topic: %s | Partition: %s | "
                        "Committed: %s | End: %s | Lag: %s",
                        group_id,
                        topic,
                        partition,
                        data["committed_offset"],
                        data["end_offset"],
                        data["lag"],
                    )

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutting down lag monitor")


if __name__ == "__main__":
    report()
