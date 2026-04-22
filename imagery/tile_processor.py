# imagery/tile_processor.py
"""
Sentinel-2 Tile Processor Module

Extracts spectral bands from a downloaded Sentinel-2 zip,
reprojects to WGS84, clips to the configured AOI bounding box,
and converts to Cloud-Optimized GeoTIFF (COG) format.
"""
import os
import logging
import zipfile
from pathlib import Path
import rasterio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject
from shapely.geometry import box
from config.config_loader import config
from config.logging_config import setup_logging


if not os.environ.get("AIRFLOW_CTX_DAG_ID"):
    setup_logging("tile_processor.log")
logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("/opt/airflow/imagery/processed")
EXTRACT_DIR = Path("/tmp/s2")
BANDS = {
    "B04": "red",
    "B08": "nir",
}


def extract_bands(zip_path: Path) -> dict[str, Path]:
    """
    Extract relevant spectral band files from a Sentinel-2 zip archive.

    Args:
        zip_path: Path to the downloaded Sentinel-2 zip file.

    Returns:
        dict mapping band name to extracted file path.
    """
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

    band_paths = {}

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            for band in BANDS:
                if f"_{band}_10m.jp2" in name or f"_{band}_20m.jp2" in name:
                    if band not in band_paths:
                        zf.extract(name, EXTRACT_DIR)
                        band_paths[band] = EXTRACT_DIR / name
                        logger.info("Extracted band %s: %s", band, name)

    return band_paths


def reproject_and_clip(band_path: Path, band_name: str, date_str: str) -> Path:
    """
    Reproject a band to WGS84, clip to AOI, and save as COG.

    Args:
        band_path: Path to the extracted band JP2 file.
        band_name: Band identifier (e.g. B04).
        date_str: Date string used in output filename.

    Returns:
        Path to the processed COG file.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    bbox = config["aoi"]["bbox"]
    aoi_name = config["aoi"]["name"]

    dst_crs = CRS.from_epsg(4326)
    output_path = PROCESSED_DIR / f"{aoi_name}_{date_str}_{band_name}.tif"

    with rasterio.open(band_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )

        kwargs = src.meta.copy()
        kwargs.update(
            {
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height,
                "driver": "GTiff",
            }
        )

        # reproject to WGS84
        reprojected_path = PROCESSED_DIR / f"_tmp_{band_name}.tif"
        with rasterio.open(reprojected_path, "w", **kwargs) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest,
            )

    # clip to AOI
    aoi_geom = [
        box(
            bbox["min_lon"], bbox["min_lat"], bbox["max_lon"], bbox["max_lat"]
        ).__geo_interface__
    ]

    with rasterio.open(reprojected_path) as src:
        clipped, clipped_transform = mask(src, aoi_geom, crop=True)
        clipped_meta = src.meta.copy()
        clipped_meta.update(
            {
                "height": clipped.shape[1],
                "width": clipped.shape[2],
                "transform": clipped_transform,
            }
        )

    # write COG
    with rasterio.open(output_path, "w", **clipped_meta) as dst:
        dst.write(clipped)

    reprojected_path.unlink()
    logger.info("Processed band %s -> %s", band_name, output_path)
    return output_path


def process(zip_path: Path) -> dict[str, Path]:
    """
    Process a Sentinel-2 zip, extract, reproject, clip, and save as COG.

    Args:
        zip_path: Path to the downloaded Sentinel-2 zip file.

    Returns:
        dict mapping band name to processed COG file path.
    """
    date_str = zip_path.stem.split("_")[2][:8]
    logger.info("Processing tile: %s | Date: %s", zip_path.name, date_str)

    band_paths = extract_bands(zip_path)
    processed = {}

    for band_name, band_path in band_paths.items():
        processed[band_name] = reproject_and_clip(band_path, band_name, date_str)

    logger.info("Processing complete, %s bands processed", len(processed))
    return processed


def main() -> None:
    """
    Process the most recently downloaded Sentinel-2 tile.
    """
    downloads = list(Path("/opt/airflow/imagery/downloads").glob("*.zip"))
    if not downloads:
        logger.error("No downloaded tiles found in imagery/downloads")
        return

    latest = max(downloads, key=lambda p: p.stat().st_mtime)
    logger.info("Processing most recent tile: %s", latest.name)
    process(latest)


if __name__ == "__main__":
    main()
