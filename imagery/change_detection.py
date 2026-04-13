# imagery/change_detection.py
"""
Change Detection Module

Downloads B04 and B08 tiles from MinIO for the two most recent dates,
computes NDVI delta between them, and flags anomaly patches where
the change exceeds the configured threshold.
"""
import os
import sys
import logging
from pathlib import Path
import numpy as np
from minio import Minio

os.environ["GDAL_DATA"] = sys.prefix + "/Library/share/gdal"
os.environ["GDAL_DRIVER_PATH"] = sys.prefix + "/Library/lib/gdalplugins"
import rasterio
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("change_detection.log")
logger = logging.getLogger(__name__)

TEMP_DIR = Path("imagery/tmp")


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


def list_dates(client: Minio, bucket: str) -> list[str]:
    """
    List available tile dates in the MinIO bucket.

    Args:
        client: MinIO client instance.
        bucket: Bucket name to list.

    Returns:
        Sorted list of date strings (YYYYMMDD).
    """
    objects = client.list_objects(bucket, recursive=False)
    dates = sorted({obj.object_name.split("/")[0] for obj in objects})
    logger.info("Available tile dates: %s", dates)
    return dates


def download_band(client: Minio, bucket: str, date: str, band: str) -> Path:
    """
    Download a specific band tile from MinIO to a local temp directory.

    Args:
        client: MinIO client instance.
        bucket: Bucket name.
        date: Date string (YYYYMMDD).
        band: Band name (e.g. B04).

    Returns:
        Path to the downloaded file.
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    aoi_name = config["aoi"]["name"]
    object_key = f"{date}/{aoi_name}_{date}_{band}.tif"
    local_path = TEMP_DIR / f"{aoi_name}_{date}_{band}.tif"

    client.fget_object(bucket, object_key, str(local_path))
    logger.info("Downloaded %s -> %s", object_key, local_path)
    return local_path


def compute_ndvi(b04_path: Path, b08_path: Path) -> np.ndarray:
    """
    Compute NDVI from B04 (red) and B08 (NIR) bands.

    NDVI = (B08 - B04) / (B08 + B04)

    Args:
        b04_path: Path to the B04 (red) GeoTIFF.
        b08_path: Path to the B08 (NIR) GeoTIFF.

    Returns:
        2D numpy array of NDVI values.
    """
    with rasterio.open(b04_path) as src:
        b04 = src.read(1).astype(float)
    with rasterio.open(b08_path) as src:
        b08 = src.read(1).astype(float)

    np.seterr(divide="ignore", invalid="ignore")
    ndvi = np.where(
        (b08 + b04) == 0,
        0,
        (b08 - b04) / (b08 + b04),
    )
    return ndvi


def detect_anomalies(
    ndvi_old: np.ndarray,
    ndvi_new: np.ndarray,
    patch_size: int,
    threshold: float,
) -> list[dict]:
    """
    Compare two NDVI arrays and flag patches where change exceeds threshold.

    Args:
        ndvi_old: NDVI array from the older date.
        ndvi_new: NDVI array from the newer date.
        patch_size: Size of each patch in pixels.
        threshold: Minimum NDVI delta to flag as anomaly.

    Returns:
        List of anomaly dicts with patch coordinates and delta score.
    """
    delta = np.abs(ndvi_new - ndvi_old)
    anomalies = []

    rows, cols = delta.shape
    for row in range(0, rows, patch_size):
        for col in range(0, cols, patch_size):
            patch = delta[row : row + patch_size, col : col + patch_size]
            mean_delta = float(np.mean(patch))

            if mean_delta >= threshold:
                anomalies.append(
                    {
                        "row": row,
                        "col": col,
                        "patch_size": patch_size,
                        "mean_delta": round(mean_delta, 4),
                        "max_delta": round(float(np.max(patch)), 4),
                    }
                )

    logger.info(
        "Detected %s anomaly patches (threshold: %s)", len(anomalies), threshold
    )
    return anomalies


def run(date_old: str, date_new: str) -> list[dict]:
    """
    Run change detection between two tile dates.

    Args:
        date_old: Older date string (YYYYMMDD).
        date_new: Newer date string (YYYYMMDD).

    Returns:
        List of anomaly patch dicts.
    """
    client = get_client()
    bucket = config["minio"]["bucket"]
    cd_config = config["change_detection"]

    logger.info("Running change detection: %s -> %s", date_old, date_new)

    # download bands for both dates
    b04_old = download_band(client, bucket, date_old, "B04")
    b08_old = download_band(client, bucket, date_old, "B08")
    b04_new = download_band(client, bucket, date_new, "B04")
    b08_new = download_band(client, bucket, date_new, "B08")

    # compute NDVI for both dates
    ndvi_old = compute_ndvi(b04_old, b08_old)
    ndvi_new = compute_ndvi(b04_new, b08_new)

    logger.info(
        "NDVI old shape: %s | NDVI new shape: %s", ndvi_old.shape, ndvi_new.shape
    )

    # detect anomalies
    anomalies = detect_anomalies(
        ndvi_old,
        ndvi_new,
        patch_size=cd_config["patch_size"],
        threshold=cd_config["ndvi_threshold"],
    )

    for a in anomalies:
        logger.info(
            "Anomaly patch | row: %s col: %s | mean_delta: %s | max_delta: %s",
            a["row"],
            a["col"],
            a["mean_delta"],
            a["max_delta"],
        )

    return anomalies


def main() -> None:
    """
    Run change detection on the two most recent tile dates in MinIO.
    """
    client = get_client()
    bucket = config["minio"]["bucket"]
    dates = list_dates(client, bucket)

    if len(dates) < 2:
        logger.warning(
            "Need at least 2 tile dates for change detection, found: %s", len(dates)
        )
        logger.info(
            "Run sentinel_fetch + tile_processor + tile_uploader again to add a second date"
        )
        return

    date_old, date_new = dates[-2], dates[-1]
    run(date_old, date_new)


if __name__ == "__main__":
    main()
