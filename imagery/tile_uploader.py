# imagery/tile_uploader.py
"""
Tile Uploader Module

Uploads processed Sentinel-2 GeoTIFF tiles from the local
processed directory to the MinIO sentinel-tiles bucket.
"""
import logging
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("tile_uploader.log")
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("imagery/processed")


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


def upload_tile(client: Minio, bucket: str, tile_path: Path) -> str:
    """
    Upload a single GeoTIFF tile to MinIO.

    Args:
        client: MinIO client instance.
        bucket: Target bucket name.
        tile_path: Path to the local GeoTIFF file.

    Returns:
        str: Object key the tile was uploaded under.
    """
    date_str = tile_path.stem.split("_")[2]
    object_key = f"{date_str}/{tile_path.name}"

    client.fput_object(
        bucket,
        object_key,
        str(tile_path),
        content_type="image/tiff",
    )

    logger.info("Uploaded %s -> %s/%s", tile_path.name, bucket, object_key)
    return object_key


def upload_all(date_filter: str = None) -> list[str]:
    """
    Upload all processed GeoTIFF tiles to MinIO.

    Args:
        date_filter: Optional date string (YYYYMMDD) to filter tiles by date.

    Returns:
        list of uploaded object keys.
    """
    client = get_client()
    bucket = config["minio"]["bucket"]
    uploaded = []

    tiles = list(PROCESSED_DIR.glob("*.tif"))
    if not tiles:
        logger.warning("No processed tiles found in %s", PROCESSED_DIR)
        return []

    if date_filter:
        tiles = [t for t in tiles if date_filter in t.name]

    for tile_path in tiles:
        try:
            key = upload_tile(client, bucket, tile_path)
            uploaded.append(key)
        except S3Error as e:
            logger.error("Failed to upload %s: %s", tile_path.name, e)

    logger.info("Upload complete, %s tiles uploaded", len(uploaded))
    return uploaded


def main() -> None:
    """
    Upload all processed tiles to MinIO sentinel-tiles bucket.
    """
    logger.info("Starting tile upload to MinIO")
    keys = upload_all()
    for key in keys:
        logger.info("Stored: %s", key)


if __name__ == "__main__":
    main()
