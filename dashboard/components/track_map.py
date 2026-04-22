# dashboard/components/track_map.py
"""
Track Map Component

Fetches vessel and aircraft tracks from PostGIS.
"""

from __future__ import annotations
import logging
import math
from contextlib import closing
from datetime import datetime, timedelta, timezone
import pandas as pd
import psycopg2
import streamlit as st
from config.config_loader import config
from config.logging_config import setup_logging

setup_logging("dashboard.log")
logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    """
    Create and return a PostGIS database connection using config credentials.

    Returns:
        Active psycopg2 connection.
    """
    db = config["postgis"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["db"],
        user=db["user"],
        password=db["password"],
    )


def _normalize_track_df(
    df: pd.DataFrame, datetime_cols: list[str] | None = None
) -> pd.DataFrame:
    """
    Normalize datetime columns in a track dataframe to UTC-aware Timestamps.

    Args:
        df: Raw track dataframe from PostGIS query.
        datetime_cols: Column names to coerce to datetime. Defaults to ['received_at'].

    Returns:
        Normalized dataframe, or original empty dataframe if input is empty.
    """
    if df.empty:
        return df

    df = df.copy()
    datetime_cols = datetime_cols or ["received_at"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


@st.cache_data(ttl=30, show_spinner=False)
def fetch_vessel_tracks(hours: int = 24) -> pd.DataFrame:
    """
    Fetch the latest position per vessel within the time window.

    Returns one row per MMSI, ordered by most recent ping.

    Args:
        hours: Number of hours to look back.

    Returns:
        DataFrame with one row per vessel, or empty DataFrame on error.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = """
        SELECT DISTINCT ON (mmsi)
            mmsi,
            vessel_name,
            ST_X(geom) AS longitude,
            ST_Y(geom) AS latitude,
            speed_knots,
            heading,
            nav_status,
            received_at
        FROM vessel_tracks
        WHERE received_at >= %s
        ORDER BY mmsi, received_at DESC
        LIMIT 5000
    """
    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql_query(query, conn, params=(since,))
        return _normalize_track_df(df)
    except Exception:
        logger.exception("Failed to fetch vessel tracks")
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_vessel_history(mmsi: int, hours: int = 24) -> pd.DataFrame:
    """
    Fetch all pings for a specific vessel in chronological order.

    Args:
        mmsi: MMSI identifier of the target vessel.
        hours: Number of hours to look back.

    Returns:
        DataFrame of all pings for the vessel sorted oldest-first,
        or empty DataFrame on error.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = """
        SELECT
            mmsi,
            vessel_name,
            ST_X(geom) AS longitude,
            ST_Y(geom) AS latitude,
            speed_knots,
            heading,
            received_at
        FROM vessel_tracks
        WHERE mmsi = %s
          AND received_at >= %s
        ORDER BY received_at ASC
    """
    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql_query(query, conn, params=(mmsi, since))
        return _normalize_track_df(df)
    except Exception:
        logger.exception("Failed to fetch vessel history")
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_all_vessel_history(hours: int = 24) -> pd.DataFrame:
    """
    All pings for potential loitering vessels only.

    Pre-filters to vessels that have at least 3 slow-speed pings
    in the window before fetching full history, avoiding pulling
    all pings for every vessel in a busy AOI.

    Args:
        hours: Number of hours to look back.

    Returns:
        DataFrame of all pings for candidate loitering vessels,
        or empty DataFrame on error.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = """
        SELECT
            mmsi,
            vessel_name,
            ST_X(geom) AS longitude,
            ST_Y(geom) AS latitude,
            speed_knots,
            heading,
            received_at
        FROM vessel_tracks
        WHERE received_at >= %s
          AND mmsi IN (
              SELECT mmsi
              FROM vessel_tracks
              WHERE received_at >= %s
                AND speed_knots <= 5.0
              GROUP BY mmsi
              HAVING COUNT(*) >= 3
          )
        ORDER BY mmsi, received_at ASC
        LIMIT 50000
    """
    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql_query(query, conn, params=(since, since))
        return _normalize_track_df(df)
    except Exception:
        logger.exception("Failed to fetch all vessel history")
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_aircraft_tracks(hours: int = 24) -> pd.DataFrame:
    """
    Fetch the latest position per aircraft within the time window.

    Returns one row per ICAO24, ordered by most recent ping.

    Args:
        hours: Number of hours to look back.

    Returns:
        DataFrame with one row per aircraft, or empty DataFrame on error.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = """
        SELECT DISTINCT ON (icao24)
            icao24,
            callsign,
            origin_country,
            ST_X(geom) AS longitude,
            ST_Y(geom) AS latitude,
            altitude_m,
            velocity_ms,
            heading,
            received_at
        FROM aircraft_tracks
        WHERE received_at >= %s
        ORDER BY icao24, received_at DESC
        LIMIT 2000
    """
    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql_query(query, conn, params=(since,))
        return _normalize_track_df(df)
    except Exception:
        logger.exception("Failed to fetch aircraft tracks")
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_vessels_in_radius(
    lat: float, lon: float, radius_km: float, hours: int = 24
) -> pd.DataFrame:
    """
    Fetch the latest position per vessel within a geographic radius.

    Uses PostGIS ST_DWithin for spatial filtering.

    Args:
        lat: Center latitude in decimal degrees.
        lon: Center longitude in decimal degrees.
        radius_km: Search radius in kilometres.
        hours: Number of hours to look back.

    Returns:
        DataFrame with one row per vessel within the radius,
        or empty DataFrame on error.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    radius_m = radius_km * 1000.0
    query = """
        SELECT DISTINCT ON (mmsi)
            mmsi,
            vessel_name,
            ST_X(geom) AS longitude,
            ST_Y(geom) AS latitude,
            speed_knots,
            heading,
            received_at
        FROM vessel_tracks
        WHERE received_at >= %s
          AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
          )
        ORDER BY mmsi, received_at DESC
    """
    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql_query(query, conn, params=(since, lon, lat, radius_m))
        return _normalize_track_df(df)
    except Exception:
        logger.exception("Failed to fetch vessels in radius")
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_aircraft_in_radius(
    lat: float, lon: float, radius_km: float, hours: int = 24
) -> pd.DataFrame:
    """
    Fetch the latest position per aircraft within a geographic radius.

    Uses PostGIS ST_DWithin for spatial filtering.

    Args:
        lat: Center latitude in decimal degrees.
        lon: Center longitude in decimal degrees.
        radius_km: Search radius in kilometres.
        hours: Number of hours to look back.

    Returns:
        DataFrame with one row per aircraft within the radius,
        or empty DataFrame on error.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    radius_m = radius_km * 1000.0
    query = """
        SELECT DISTINCT ON (icao24)
            icao24,
            callsign,
            origin_country,
            ST_X(geom) AS longitude,
            ST_Y(geom) AS latitude,
            altitude_m,
            velocity_ms,
            heading,
            received_at
        FROM aircraft_tracks
        WHERE received_at >= %s
          AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
          )
        ORDER BY icao24, received_at DESC
    """
    try:
        with closing(get_connection()) as conn:
            df = pd.read_sql_query(query, conn, params=(since, lon, lat, radius_m))
        return _normalize_track_df(df)
    except Exception:
        logger.exception("Failed to fetch aircraft in radius")
        return pd.DataFrame()


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Compute haversine great-circle distance between two points in kilometres.

    Args:
        lat1: Latitude of point 1 in decimal degrees.
        lon1: Longitude of point 1 in decimal degrees.
        lat2: Latitude of point 2 in decimal degrees.
        lon2: Longitude of point 2 in decimal degrees.

    Returns:
        Distance in kilometres.
    """
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def detect_loitering_vessels(
    history_df: pd.DataFrame,
    min_pings: int = 8,
    max_avg_speed_knots: float = 5.0,
    max_radius_km: float = 1.5,
    min_duration_minutes: float = 45.0,
) -> set[int]:
    """
    Detect vessels exhibiting loitering behavior.

    Loitering here means:
    - enough pings to establish behavior
    - observed over long enough duration
    - relatively low average speed
    - remained within a small operating radius

    Returns a set of MMSIs considered loitering.
    """
    if history_df.empty:
        return set()

    df = history_df.copy()
    df["speed_knots"] = pd.to_numeric(df["speed_knots"], errors="coerce").fillna(0.0)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    if "received_at" in df.columns:
        df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce", utc=True)

    loitering: set[int] = set()

    for mmsi, group in df.groupby("mmsi", dropna=True):
        group = group.dropna(
            subset=["latitude", "longitude", "received_at"]
        ).sort_values("received_at")
        if len(group) < min_pings:
            continue

        duration_minutes = (
            group["received_at"].max() - group["received_at"].min()
        ).total_seconds() / 60.0
        if duration_minutes < min_duration_minutes:
            continue

        avg_speed = float(group["speed_knots"].mean())
        if avg_speed > max_avg_speed_knots:
            continue

        center_lat = float(group["latitude"].mean())
        center_lon = float(group["longitude"].mean())

        distances = group.apply(
            lambda r: _distance_km(
                center_lat, center_lon, float(r["latitude"]), float(r["longitude"])
            ),
            axis=1,
        )
        max_distance_from_center = float(distances.max()) if len(distances) else 999.0
        if max_distance_from_center > max_radius_km:
            continue

        loitering.add(int(mmsi))

    return loitering


def get_aoi_bounds() -> dict:
    """
    Return the AOI bounding box dict from config.

    Returns:
        Dict with min_lat, max_lat, min_lon, max_lon keys.
    """
    return config["aoi"]["bbox"]
