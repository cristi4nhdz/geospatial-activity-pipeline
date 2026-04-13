# imagery/sentinel_fetch.py
"""
Sentinel-2 Fetch Module

Authenticates with Copernicus Dataspace, searches for available
Sentinel-2 L2A tiles over the configured AOI, and downloads the
least cloudy tile from the last 30 days.
"""
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("sentinel_fetch.log")
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = Path("imagery/downloads")


def get_access_token() -> str:
    """
    Authenticate with Copernicus Dataspace and return an access token.

    Returns:
        str: Bearer access token.
    """
    c = config["copernicus"]
    response = requests.post(
        c["token_url"],
        data={
            "grant_type": "password",
            "username": config["copernicus"]["username"],
            "password": config["copernicus"]["password"],
            "client_id": "cdse-public",
        },
        timeout=30,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    logger.info("Authenticated with Copernicus Dataspace")
    return token


def search_tiles(token: str, days_back: int = 30) -> list[dict]:
    """
    Search for available Sentinel-2 L2A tiles over the configured AOI.

    Args:
        token: Copernicus access token.
        days_back: Number of days to look back for available tiles.

    Returns:
        list[dict]: List of available product metadata, sorted by cloud cover.
    """
    bbox = config["aoi"]["bbox"]
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    footprint = (
        f"POLYGON(("
        f"{bbox['min_lon']} {bbox['min_lat']},"
        f"{bbox['max_lon']} {bbox['min_lat']},"
        f"{bbox['max_lon']} {bbox['max_lat']},"
        f"{bbox['min_lon']} {bbox['max_lat']},"
        f"{bbox['min_lon']} {bbox['min_lat']}"
        f"))"
    )

    params = {
        "$filter": (
            f"Collection/Name eq 'SENTINEL-2' and "
            f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
            f"and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A') and "
            f"ContentDate/Start gt {start} and "
            f"ContentDate/Start lt {end} and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{footprint}')"
        ),
        "$orderby": "ContentDate/Start desc",
        "$top": 5,
    }

    response = requests.get(
        config["copernicus"]["search_url"],
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    products = response.json().get("value", [])
    logger.info("Found %s Sentinel-2 tiles over AOI", len(products))
    return products


def download_tile(token: str, product: dict) -> Path:
    """
    Download a Sentinel-2 tile by product ID.

    Args:
        token: Copernicus access token.
        product: Product metadata dict from search results.

    Returns:
        Path: Local path to the downloaded file.
    """
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    product_id = product["Id"]
    product_name = product["Name"]
    url = (
        f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
    )

    logger.info("Downloading tile: %s", product_name)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    response = session.get(
        url,
        allow_redirects=False,
        timeout=30,
    )

    # follow redirect with auth header preserved
    while response.status_code in (301, 302, 303, 307, 308):
        redirect_url = response.headers["Location"]
        response = session.get(redirect_url, allow_redirects=False, timeout=30)

    response.raise_for_status()

    output_path = DOWNLOAD_DIR / f"{product_name}.zip"
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("Downloaded tile to: %s", output_path)
    return output_path


def main() -> Path | None:
    """
    Fetch the least cloudy Sentinel-2 tile for the configured AOI.

    Returns:
        Path to downloaded tile zip, or None if no tiles found.
    """
    token = get_access_token()
    products = search_tiles(token)

    if not products:
        logger.warning("No Sentinel-2 tiles found for AOI in the last 30 days")
        return None

    best = products[0]
    logger.info(
        "Selected tile: %s | Cloud cover: %s%%",
        best["Name"],
        best.get("Attributes", [{}])[0].get("Value", "unknown"),
    )

    return download_tile(token, best)


if __name__ == "__main__":
    main()
