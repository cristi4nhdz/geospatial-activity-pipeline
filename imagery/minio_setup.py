# imagery/minio_setup.py
"""
MinIO Setup Module

Creates the sentinel-tiles bucket in MinIO if it does not already exist.
Run once after bringing up the Docker stack.
"""
import os
import logging
from minio import Minio
from minio.error import S3Error
from config.config_loader import config
from config.logging_config import setup_logging

if not os.environ.get("AIRFLOW_CTX_DAG_ID"):
    setup_logging("minio_setup.log")
logger = logging.getLogger(__name__)


def get_client() -> Minio:
    """
    Create and return a MinIO client instance.

    Returns:
        Minio: Configured MinIO client.
    """
    m = config["minio"]
    return Minio(
        m["endpoint"],
        access_key=m["access_key"],
        secret_key=m["secret_key"],
        secure=False,
    )


def create_bucket(client: Minio, bucket: str) -> None:
    """
    Create a MinIO bucket if it does not already exist.

    Args:
        client: MinIO client instance.
        bucket: Name of the bucket to create.
    """
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Created bucket: %s", bucket)
    else:
        logger.info("Bucket already exists: %s", bucket)


def main() -> None:
    """
    Run MinIO bucket setup.
    """
    client = get_client()
    bucket = config["minio"]["bucket"]

    logger.info("Connecting to MinIO at %s", config["minio"]["endpoint"])

    try:
        create_bucket(client, bucket)
        logger.info("MinIO setup complete, bucket: %s", bucket)
    except S3Error as e:
        logger.error("MinIO setup failed: %s", e)


if __name__ == "__main__":
    main()
