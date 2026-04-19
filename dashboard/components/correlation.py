# dashboard/components/correlation.py
"""
Correlation logic for linking anomaly events with nearby vessel
and aircraft tracks in space and time.
"""

from __future__ import annotations
import logging
from math import atan2, cos, radians, sin, sqrt

import pandas as pd

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in kilometres."""
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return EARTH_RADIUS_KM * 2 * atan2(sqrt(a), sqrt(1 - a))


def get_anomaly_center(
    row: pd.Series, default_patch_size: int = 512
) -> tuple[float, float]:
    """
    Estimate geographic center of an anomaly patch from pixel offsets.

    Derives lat/lon by normalising the patch pixel position against the
    AOI bounding box using a fixed tile size of patch_size * 24 pixels.
    Applies deterministic jitter seeded by pixel coordinates to avoid
    grid alignment of anomaly centroids.

    Args:
        row: Anomaly event Series with row_px, col_px, and optionally patch_size.
        default_patch_size: Fallback patch size in pixels if not present in row.

    Returns:
        Tuple of (latitude, longitude) in decimal degrees.
    """
    from config.config_loader import config

    bbox = config["aoi"]["bbox"]
    lat_range = bbox["max_lat"] - bbox["min_lat"]
    lon_range = bbox["max_lon"] - bbox["min_lon"]

    patch_size = int(row.get("patch_size", default_patch_size) or default_patch_size)
    tile_px = patch_size * 24

    norm_row = max(0.0, min(1.0, (float(row["row_px"]) + patch_size / 2) / tile_px))
    norm_col = max(0.0, min(1.0, (float(row["col_px"]) + patch_size / 2) / tile_px))

    lat = bbox["max_lat"] - norm_row * lat_range
    lon = bbox["min_lon"] + norm_col * lon_range

    return lat, lon


def find_nearby_vessels(
    anomaly_lat: float,
    anomaly_lon: float,
    vessel_df: pd.DataFrame,
    radius_km: float = 20.0,
) -> pd.DataFrame:
    """
    Find vessels within radius_km of an anomaly center.

    Args:
        anomaly_lat: Anomaly center latitude in decimal degrees.
        anomaly_lon: Anomaly center longitude in decimal degrees.
        vessel_df: Vessel tracks dataframe with latitude/longitude columns.
        radius_km: Search radius in kilometres.

    Returns:
        Filtered and distance-sorted vessel dataframe, or empty DataFrame if none found.
    """
    if vessel_df.empty:
        return pd.DataFrame()

    df = vessel_df.copy()
    df["distance_km"] = df.apply(
        lambda r: haversine_km(anomaly_lat, anomaly_lon, r["latitude"], r["longitude"]),
        axis=1,
    )
    return (
        df[df["distance_km"] <= radius_km]
        .sort_values("distance_km")
        .reset_index(drop=True)
    )


def find_nearby_aircraft(
    anomaly_lat: float,
    anomaly_lon: float,
    aircraft_df: pd.DataFrame,
    radius_km: float = 20.0,
) -> pd.DataFrame:
    """
    Find aircraft within radius_km of an anomaly center.

    Args:
        anomaly_lat: Anomaly center latitude in decimal degrees.
        anomaly_lon: Anomaly center longitude in decimal degrees.
        aircraft_df: Aircraft tracks dataframe with latitude/longitude columns.
        radius_km: Search radius in kilometres.

    Returns:
        Filtered and distance-sorted aircraft dataframe, or empty DataFrame if none found.
    """
    if aircraft_df.empty:
        return pd.DataFrame()

    df = aircraft_df.copy()
    df["distance_km"] = df.apply(
        lambda r: haversine_km(anomaly_lat, anomaly_lon, r["latitude"], r["longitude"]),
        axis=1,
    )
    return (
        df[df["distance_km"] <= radius_km]
        .sort_values("distance_km")
        .reset_index(drop=True)
    )


def assign_priority(
    confidence: float, nearby_vessel_count: int, nearby_aircraft_count: int
) -> str:
    """
    Assign a priority label based on confidence score and nearby asset count.

    URGENT: confidence >= 0.65 and total activity >= 2.
    HIGH:   confidence >= 0.65, or confidence >= 0.55 with at least 1 nearby asset.
    MEDIUM: confidence >= 0.45.
    LOW:    all other cases.

    Args:
        confidence: Combined anomaly confidence score.
        nearby_vessel_count: Number of vessels within the correlation radius.
        nearby_aircraft_count: Number of aircraft within the correlation radius.

    Returns:
        Priority label string: one of URGENT, HIGH, MEDIUM, or LOW.
    """
    activity = int(nearby_vessel_count) + int(nearby_aircraft_count)

    if confidence >= 0.65 and activity >= 2:
        return "URGENT"
    if confidence >= 0.65 or (confidence >= 0.55 and activity >= 1):
        return "HIGH"
    if confidence >= 0.45:
        return "MEDIUM"
    return "LOW"


def priority_color(priority: str) -> str:
    """
    Return the hex colour string for a given priority label.

    Args:
        priority: Priority label (URGENT/HIGH/MEDIUM/LOW).

    Returns:
        Hex colour string, defaulting to grey for unknown priorities.
    """
    return {
        "URGENT": "#ef4444",
        "HIGH": "#f97316",
        "MEDIUM": "#eab308",
        "LOW": "#6b7280",
    }.get(priority, "#6b7280")


def priority_bg(priority: str) -> str:
    """
    Return the background rgba colour string for a given priority label.

    Args:
        priority: Priority label (URGENT/HIGH/MEDIUM/LOW).

    Returns:
        CSS rgba colour string, defaulting to grey for unknown priorities.
    """
    return {
        "URGENT": "rgba(239,68,68,0.15)",
        "HIGH": "rgba(249,115,22,0.15)",
        "MEDIUM": "rgba(234,179,8,0.15)",
        "LOW": "rgba(107,114,128,0.15)",
    }.get(priority, "rgba(107,114,128,0.15)")


def _stable_anomaly_id(row: pd.Series) -> str:
    """
    Generate a stable, human-readable anomaly ID from row pixel coordinates and date.

    Format: ANO-{YYYYMMDD}-{row_px}-{col_px}

    Args:
        row: Anomaly event Series with date_new, row_px, and col_px fields.

    Returns:
        Stable anomaly ID string.
    """
    date_part = "unknown"
    if pd.notna(row.get("date_new")):
        try:
            date_part = pd.to_datetime(row["date_new"]).strftime("%Y%m%d")
        except Exception:
            date_part = str(row["date_new"])[:10].replace("-", "")
    return f"ANO-{date_part}-{int(row['row_px'])}-{int(row['col_px'])}"


def build_correlated_events(
    anomaly_df: pd.DataFrame,
    vessel_df: pd.DataFrame,
    aircraft_df: pd.DataFrame,
    radius_km: float = 20.0,
) -> pd.DataFrame:
    """
    Build a correlated events table joining anomalies with nearby assets.

    For each anomaly, computes the geographic center, finds nearby vessels
    and aircraft within radius_km, assigns priority, and assembles a
    summary row. Results are sorted by priority then confidence descending.

    Args:
        anomaly_df: Anomaly events dataframe from Snowflake.
        vessel_df: Filtered vessel tracks dataframe.
        aircraft_df: Filtered aircraft tracks dataframe.
        radius_km: Correlation search radius in kilometres.

    Returns:
        DataFrame of correlated events, one row per anomaly, sorted by priority.
        Returns empty DataFrame if anomaly_df is empty.
    """
    if anomaly_df.empty:
        return pd.DataFrame()

    rows: list[dict] = []

    for _, row in anomaly_df.iterrows():
        lat, lon = get_anomaly_center(row)

        nearby_v = find_nearby_vessels(lat, lon, vessel_df, radius_km)
        nearby_a = find_nearby_aircraft(lat, lon, aircraft_df, radius_km)

        v_count = len(nearby_v)
        a_count = len(nearby_a)

        nearest_km = None
        if v_count > 0:
            nearest_km = float(nearby_v.iloc[0]["distance_km"])
        if a_count > 0:
            a_dist = float(nearby_a.iloc[0]["distance_km"])
            if nearest_km is None or a_dist < nearest_km:
                nearest_km = a_dist

        last_activity = None
        if v_count > 0 and "received_at" in nearby_v.columns:
            last_activity = nearby_v["received_at"].max()
        if a_count > 0 and "received_at" in nearby_a.columns:
            ac_last = nearby_a["received_at"].max()
            if last_activity is None or (pd.notna(ac_last) and ac_last > last_activity):
                last_activity = ac_last

        confidence = float(row["confidence"])
        priority = assign_priority(confidence, v_count, a_count)

        rows.append(
            {
                "anomaly_id": _stable_anomaly_id(row),
                "priority": priority,
                "status": "NEW",
                "confidence": confidence,
                "ndvi_delta": float(row["mean_delta"]),
                "cnn_score": float(row["cnn_score"]),
                "ndvi_score": float(row["ndvi_score"]),
                "date_old": (
                    pd.to_datetime(row["date_old"]).strftime("%Y-%m-%d")
                    if pd.notna(row["date_old"])
                    else "N/A"
                ),
                "date_new": (
                    pd.to_datetime(row["date_new"]).strftime("%Y-%m-%d")
                    if pd.notna(row["date_new"])
                    else "N/A"
                ),
                "lat": lat,
                "lon": lon,
                "row_px": int(row["row_px"]),
                "col_px": int(row["col_px"]),
                "patch_size": int(row["patch_size"]),
                "nearby_vessels": v_count,
                "nearby_aircraft": a_count,
                "nearest_km": round(nearest_km, 2) if nearest_km is not None else None,
                "last_activity": last_activity,
            }
        )

    result = pd.DataFrame(rows)
    priority_order = {"URGENT": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    result["_rank"] = result["priority"].map(priority_order)
    result = result.sort_values(["_rank", "confidence"], ascending=[True, False])
    result = result.drop(columns=["_rank"]).reset_index(drop=True)
    return result
