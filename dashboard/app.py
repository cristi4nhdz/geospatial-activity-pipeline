# dashboard/app.py
"""
Geospatial Intelligence Dashboard
Fused vessel/aircraft tracking + Sentinel land-change detection
"""

from __future__ import annotations
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import numpy as np
import pandas as pd
import pydeck as pdk
import rasterio
from rasterio.windows import Window
import streamlit as st
from config.config_loader import config
from dashboard.components.anomaly_feed import (
    fetch_anomaly_events,
    fetch_anomaly_summary,
)
from dashboard.components.analyst_summary import render_analyst_summary
from dashboard.components.correlation import (
    build_correlated_events,
    find_nearby_aircraft,
    find_nearby_vessels,
)
from dashboard.components.kpi import (
    render_anomaly_event_card,
    render_aoi_summary,
    render_how_to_interpret,
    render_kpi_cards,
)
from dashboard.components.track_map import (
    detect_loitering_vessels,
    fetch_aircraft_tracks,
    fetch_all_vessel_history,
    fetch_vessel_history,
    fetch_vessel_tracks,
    get_aoi_bounds,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Geospatial Intelligence Platform",
    layout="wide",
    initial_sidebar_state="expanded",
)

MOVING_THRESHOLD = 0.5


def vessel_fill_color(speed: float, selected: bool = False) -> list[int]:
    """Return RGBA fill color for a vessel dot based on speed and selection state."""
    if selected:
        return [59, 130, 246, 255]

    if speed is None or pd.isna(speed):
        return [100, 100, 100, 170]

    if speed < MOVING_THRESHOLD:
        return [107, 114, 128, 190]  # docked

    if speed < 5:
        return [34, 197, 94, 220]  # slow
    if speed < 12:
        return [234, 179, 8, 220]  # medium
    return [249, 115, 22, 230]  # fast


def vessel_line_color(
    is_selected: bool, is_loitering: bool, is_correlated: bool
) -> list[int]:
    """Return RGBA outline color for a vessel dot based on its status flags."""
    if is_selected:
        return [255, 255, 255, 220]

    if is_correlated:
        return [239, 68, 68, 230]  # red = anomaly link

    if is_loitering:
        return [234, 179, 8, 230]  # gold = loitering

    return [255, 255, 255, 30]


MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

AOI_NAME = config.get("aoi", {}).get("name", "Remote Monitored Area")
AOI_SUBTITLE = config.get("aoi", {}).get(
    "subtitle",
    "Fused Tracking + Land Change Intelligence Dashboard",
)

st.markdown(
    """
<style>
    html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', sans-serif; }
    .block-container { padding-top:1.2rem; padding-bottom:1rem; padding-left:2rem; padding-right:2rem; }
    #MainMenu {visibility:hidden;} footer {visibility:hidden;} header {visibility:hidden;}

    .dash-header { display:flex; align-items:center; gap:1rem; margin-bottom:0.2rem; }
    .dash-title { font-size:1.4rem; font-weight:700; color:#f0f0f0; letter-spacing:-0.02em; margin:0; }
    .dash-badge { background:#1f2937; border:1px solid #374151; border-radius:6px; padding:0.2rem 0.6rem;
        font-size:0.68rem; color:#10b981; font-weight:600; letter-spacing:0.08em; text-transform:uppercase; }
    .dash-subtitle { font-size:0.78rem; color:#6b7280; margin-bottom:1rem; }

    .section-header { font-size:0.72rem; font-weight:600; color:#9ca3af; text-transform:uppercase;
        letter-spacing:0.1em; margin-bottom:0.5rem; margin-top:1rem; }

    .detail-label { font-size:0.68rem; color:#6b7280; text-transform:uppercase;
        letter-spacing:0.08em; margin-bottom:0.2rem; }
    .detail-value {
        font-size:0.9rem;
        color:#f9fafb;
        font-weight:500;
        margin-bottom:0.8rem;
        white-space:normal;
        overflow-wrap:anywhere;
    }
    .info-box {
        background:#111827;
        border:1px solid #1f2937;
        border-radius:8px;
        padding:0.8rem 1rem;
        font-size:0.78rem;
        color:#9ca3af;
        line-height:1.7;
        margin-top:0.3rem;
        margin-bottom:1.6rem;
    }

    .stat-row { display:flex; gap:1rem; margin-bottom:0.6rem; flex-wrap:wrap; }
    .stat-item { background:#111827; border:1px solid #1f2937; border-radius:8px;
        padding:0.5rem 0.8rem; flex:1; min-width:80px; }
    .stat-item-label { font-size:0.65rem; color:#6b7280; text-transform:uppercase; letter-spacing:0.08em; }
    .stat-item-value { font-size:1rem; font-weight:700; color:#f9fafb; }

    .warning-chip {
        display:inline-block;
        background:#3f1d1d;
        color:#fca5a5;
        border:1px solid #7f1d1d;
        border-radius:999px;
        padding:0.2rem 0.55rem;
        font-size:0.72rem;
        margin:0.2rem 0.3rem 0 0;
    }

    .ok-chip {
        display:inline-block;
        background:#132c22;
        color:#86efac;
        border:1px solid #166534;
        border-radius:999px;
        padding:0.2rem 0.55rem;
        font-size:0.72rem;
        margin:0.2rem 0.3rem 0 0;
    }

    [data-testid="stSidebar"] { background:#0d1117; border-right:1px solid #1f2937; }
    [data-testid="stSidebar"] label { font-size:0.78rem !important; color:#9ca3af !important; }

    div[data-testid="stTabs"] button { font-size:0.82rem; font-weight:500; color:#6b7280; }
    div[data-testid="stTabs"] button[aria-selected="true"] { color:#f9fafb; border-bottom-color:#3b82f6; }

    div[data-testid="stMetric"] {
        background: #0d1117;
        border: 1px solid #1f2937;
        padding: 0.75rem 0.9rem;
        border-radius: 10px;
        margin-bottom: 0.5rem;
    }

    div[data-testid="stMetricLabel"] {
        color: #9ca3af !important;
    }

    div[data-testid="stMetricValue"] {
        color: #f9fafb !important;
    }

    div[data-testid="stHorizontalBlock"] > div {
        gap: 0.6rem;
    }

    hr { border-color:#1f2937; margin:0.8rem 0; }

    /* spacing fix for info boxes / callouts */
    div[data-testid="stAlert"] {
        margin-bottom: 1.4rem !important;
    }

    /* general spacing between stacked blocks */
    .block-container > div {
        margin-bottom: 0.8rem;
    }
</style>
""",
    unsafe_allow_html=True,
)


def add_layer_type(df: pd.DataFrame, layer_type: str) -> pd.DataFrame:
    """Add a layer_type column to a dataframe for tooltip routing."""
    out = df.copy()
    out["layer_type"] = layer_type
    return out


def ensure_tooltip_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all required tooltip columns exist on a dataframe, filling missing ones with empty strings."""
    out = df.copy()
    needed = {
        "layer_type": "",
        "tooltip_title": "",
        "tooltip_line_1": "",
        "tooltip_line_2": "",
        "tooltip_line_3": "",
        "tooltip_line_4": "",
        "tooltip_line_5": "",
    }
    for col, default in needed.items():
        if col not in out.columns:
            out[col] = default
    return out


def render_map(
    layers: list[pdk.Layer | None], lat: float, lon: float, zoom: int = 10
) -> None:
    """Render a pydeck map with unified tooltip styling."""
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer for layer in layers if layer is not None],
            initial_view_state=pdk.ViewState(
                latitude=lat, longitude=lon, zoom=zoom, pitch=0
            ),
            tooltip={
                "html": """
                    <div style='font-family:Inter,sans-serif;padding:8px 12px;font-size:12px;min-width:170px;max-width:260px'>
                        <div style='font-weight:700;color:#f9fafb;margin-bottom:6px;font-size:13px'>{tooltip_title}</div>
                        <div style='color:#d1d5db'>{tooltip_line_1}</div>
                        <div style='color:#d1d5db'>{tooltip_line_2}</div>
                        <div style='color:#d1d5db'>{tooltip_line_3}</div>
                        <div style='color:#d1d5db'>{tooltip_line_4}</div>
                        <div style='color:#d1d5db'>{tooltip_line_5}</div>
                    </div>
                """,
                "style": {
                    "backgroundColor": "#1f2937",
                    "border": "1px solid #374151",
                    "borderRadius": "8px",
                },
            },
            map_style=MAP_STYLE,
        ),
        use_container_width=True,
    )


def build_vessel_layer(
    df: pd.DataFrame,
    selected_mmsi: int | None = None,
    loitering_mmsis: set[int] | None = None,
    correlated_mmsis: set[int] | None = None,
) -> pdk.Layer | None:
    """
    Build a ScatterplotLayer for vessel positions.

    Colors encode speed. Selected vessel is highlighted blue and enlarged.
    Unselected vessels are greyed out when a selection is active.
    Correlated vessels have a red outline. Loitering vessels have a gold outline.
    """
    if df.empty:
        return None

    loitering_mmsis = loitering_mmsis or set()
    correlated_mmsis = correlated_mmsis or set()

    df = df.copy()
    df["vessel_name"] = df["vessel_name"].fillna("Unknown")
    df["speed_knots"] = df["speed_knots"].fillna(0).round(1)
    df["heading"] = df["heading"].fillna(0).astype(int)
    df["received_at_str"] = df["received_at"].astype(str).str[:19]

    has_selection = selected_mmsi is not None

    def get_fill(r: pd.Series) -> list[int]:
        is_sel = int(r["mmsi"]) == selected_mmsi if selected_mmsi is not None else False
        base = vessel_fill_color(float(r["speed_knots"]), is_sel)

        if has_selection and not is_sel:
            return [base[0], base[1], base[2], 110]
        return base

    def get_line(r: pd.Series) -> list[int]:
        is_sel = int(r["mmsi"]) == selected_mmsi if selected_mmsi is not None else False
        is_loit = int(r["mmsi"]) in loitering_mmsis
        is_corr = int(r["mmsi"]) in correlated_mmsis
        return vessel_line_color(is_sel, is_loit, is_corr)

    def get_radius(r: pd.Series) -> int:
        is_sel = selected_mmsi is not None and int(r["mmsi"]) == selected_mmsi
        is_corr = int(r["mmsi"]) in correlated_mmsis
        is_loit = int(r["mmsi"]) in loitering_mmsis

        if is_sel:
            return 620
        if is_corr:
            return 360
        if is_loit:
            return 320
        return 220 if float(r["speed_knots"]) < MOVING_THRESHOLD else 300

    def get_line_width(r: pd.Series) -> int:
        is_sel = selected_mmsi is not None and int(r["mmsi"]) == selected_mmsi
        is_corr = int(r["mmsi"]) in correlated_mmsis
        is_loit = int(r["mmsi"]) in loitering_mmsis

        if is_sel:
            return 3
        if is_corr:
            return 3
        if is_loit:
            return 2
        return 1

    def build_flag(r: pd.Series) -> str:
        flags = []
        if int(r["mmsi"]) in correlated_mmsis:
            flags.append("Correlated")
        if int(r["mmsi"]) in loitering_mmsis:
            flags.append("Loitering")
        return " | ".join(flags) if flags else "Normal"

    df["color"] = df.apply(get_fill, axis=1)
    df["line_color"] = df.apply(get_line, axis=1)
    df["radius"] = df.apply(get_radius, axis=1)
    df["line_width"] = df.apply(get_line_width, axis=1)
    df["alert_flag"] = df.apply(build_flag, axis=1)

    df["tooltip_title"] = "Vessel"
    df["tooltip_line_1"] = "Name: " + df["vessel_name"].astype(str)
    df["tooltip_line_2"] = "MMSI: " + df["mmsi"].astype(str)
    df["tooltip_line_3"] = "Speed: " + df["speed_knots"].astype(str) + " kn"
    df["tooltip_line_4"] = "Flag: " + df["alert_flag"].astype(str)
    df["tooltip_line_5"] = "Time: " + df["received_at_str"].astype(str)
    df = ensure_tooltip_columns(add_layer_type(df, "vessel"))

    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_line_color="line_color",
        get_radius="radius",
        get_line_width="line_width",
        pickable=True,
        auto_highlight=True,
        stroked=True,
    )


def build_ping_history_layer(history_df: pd.DataFrame) -> pdk.Layer | None:
    """
    Build a ScatterplotLayer for historical pings of a selected vessel.

    Each dot is hoverable and shows speed, heading, and timestamp at that ping.
    """
    if history_df.empty or len(history_df) < 2:
        return None

    df = history_df.copy()
    df["speed_knots"] = df["speed_knots"].fillna(0).round(1)
    df["heading"] = df["heading"].fillna(0).astype(int)
    df["received_at_str"] = df["received_at"].astype(str).str[:19]
    df["vessel_name"] = df["vessel_name"].fillna("Unknown")
    df["color"] = [[59, 130, 246, 60]] * len(df)
    df["radius"] = 80

    df["tooltip_title"] = "🔵 Vessel Ping"
    df["tooltip_line_1"] = "Name: " + df["vessel_name"].astype(str)
    df["tooltip_line_2"] = "Speed: " + df["speed_knots"].astype(str) + " kn"
    df["tooltip_line_3"] = "Heading: " + df["heading"].astype(str) + "°"
    df["tooltip_line_4"] = "Time: " + df["received_at_str"].astype(str)
    df["tooltip_line_5"] = ""
    df = ensure_tooltip_columns(add_layer_type(df, "vessel_ping"))

    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
        stroked=False,
    )


def build_track_path_layer(history_df: pd.DataFrame) -> pdk.Layer | None:
    """Build a PathLayer connecting all historical pings for a selected vessel in chronological order."""
    if history_df.empty or len(history_df) < 2:
        return None

    path = [[r["longitude"], r["latitude"]] for _, r in history_df.iterrows()]
    return pdk.Layer(
        "PathLayer",
        data=pd.DataFrame({"path": [path]}),
        get_path="path",
        get_color=[59, 130, 246, 120],
        get_width=25,
        pickable=False,
    )


def build_track_start_layer(history_df: pd.DataFrame) -> pdk.Layer | None:
    """Build a ScatterplotLayer marking the oldest ping in the track history with a green dot."""
    if history_df.empty:
        return None

    start = history_df.iloc[[0]].copy()
    start["color"] = [[16, 185, 129, 255]]
    start["radius"] = [150]
    start["tooltip_title"] = "🟢 Track Start"
    start["tooltip_line_1"] = "Name: " + start["vessel_name"].fillna("Unknown").astype(
        str
    )
    start["tooltip_line_2"] = "Time: " + start["received_at"].astype(str).str[:19]
    start["tooltip_line_3"] = ""
    start["tooltip_line_4"] = ""
    start["tooltip_line_5"] = ""
    start = ensure_tooltip_columns(add_layer_type(start, "track_start"))

    return pdk.Layer(
        "ScatterplotLayer",
        data=start,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
    )


def build_vessel_label_layer(
    df: pd.DataFrame, selected_mmsi: int | None
) -> pdk.Layer | None:
    """Build a TextLayer rendering the vessel name above the selected vessel dot."""
    if df.empty or selected_mmsi is None:
        return None
    sel = df[df["mmsi"] == selected_mmsi].copy()
    if sel.empty:
        return None
    sel["label"] = sel["vessel_name"]
    return pdk.Layer(
        "TextLayer",
        data=sel,
        get_position=["longitude", "latitude"],
        get_text="label",
        get_size=13,
        get_color=[255, 255, 255, 220],
        get_anchor="middle",
        get_alignment_baseline="bottom",
        get_pixel_offset=[0, -22],
    )


def build_aircraft_layer(
    df: pd.DataFrame, selected_icao24: str | None = None
) -> pdk.Layer | None:
    """
    Build a ScatterplotLayer for aircraft positions.

    Normal aircraft are cyan. Selected aircraft is purple and enlarged.
    Unselected aircraft are greyed out when a selection is active.
    """
    if df.empty:
        return None

    df = df.copy()
    df["callsign"] = df["callsign"].fillna("Unknown").astype(str).str.strip()
    df["origin_country"] = df["origin_country"].fillna("Unknown").astype(str)
    df["altitude_m"] = df["altitude_m"].fillna(0).round(0).astype(int)
    df["velocity_ms"] = df["velocity_ms"].fillna(0).round(1)
    df["heading"] = df["heading"].fillna(0).round(0).astype(int)
    df["received_at_str"] = df["received_at"].astype(str).str[:19]

    has_selection = selected_icao24 is not None

    def get_color(r: pd.Series) -> list[int]:
        is_sel = (
            str(r["icao24"]) == str(selected_icao24)
            if selected_icao24 is not None
            else False
        )
        if has_selection and not is_sel:
            return [90, 90, 90, 110]  # de-emphasized
        if is_sel:
            return [168, 85, 247, 255]  # selected aircraft = purple
        return [56, 189, 248, 235]  # normal aircraft = cyan

    def get_radius(r: pd.Series) -> int:
        if selected_icao24 is not None and str(r["icao24"]) == str(selected_icao24):
            return 520
        if has_selection:
            return 220
        return 350

    df["color"] = df.apply(get_color, axis=1)
    df["radius"] = df.apply(get_radius, axis=1)

    df["tooltip_title"] = "Aircraft"
    df["tooltip_line_1"] = "Callsign: " + df["callsign"].astype(str)
    df["tooltip_line_2"] = "ICAO24: " + df["icao24"].astype(str)
    df["tooltip_line_3"] = "Altitude: " + df["altitude_m"].astype(str) + " m"
    df["tooltip_line_4"] = "Velocity: " + df["velocity_ms"].astype(str) + " m/s"
    df["tooltip_line_5"] = "Time: " + df["received_at_str"].astype(str)
    df = ensure_tooltip_columns(add_layer_type(df, "aircraft"))

    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
        stroked=True,
        get_line_color=[255, 255, 255, 40],
        get_line_width=2,
    )


def build_aoi_layer(bbox: dict) -> pdk.Layer:
    """Build a PolygonLayer rendering the AOI bounding box as a green outlined rectangle."""
    polygon = [
        [
            [bbox["min_lon"], bbox["min_lat"]],
            [bbox["max_lon"], bbox["min_lat"]],
            [bbox["max_lon"], bbox["max_lat"]],
            [bbox["min_lon"], bbox["max_lat"]],
            [bbox["min_lon"], bbox["min_lat"]],
        ]
    ]
    return pdk.Layer(
        "PolygonLayer",
        data=pd.DataFrame({"polygon": [polygon]}),
        get_polygon="polygon",
        get_fill_color=[16, 185, 129, 8],
        get_line_color=[16, 185, 129, 140],
        get_line_width=50,
        pickable=False,
    )


def build_anomaly_layer(
    correlated_df: pd.DataFrame, selected_id: str | None = None
) -> pdk.Layer | None:
    """
    Build a ScatterplotLayer for anomaly event positions.

    Selected anomaly is brighter and larger. Radius scales with patch size and confidence.
    """
    if correlated_df.empty:
        return None

    df = correlated_df.copy()
    df["longitude"] = df["lon"]
    df["latitude"] = df["lat"]

    def anomaly_color(aid: str) -> list[int]:
        return [239, 68, 68, 230] if aid == selected_id else [239, 68, 68, 150]

    def anomaly_radius(row: pd.Series) -> int:
        patch_size = int(row.get("patch_size", 512))
        confidence = float(row.get("confidence", 0.5))
        base = max(220, min(550, int(patch_size * 0.55)))
        scaled = int(base * (0.8 + confidence * 0.8))
        if row["anomaly_id"] == selected_id:
            return int(scaled * 1.35)
        return scaled

    df["color"] = df["anomaly_id"].apply(anomaly_color)
    df["radius"] = df.apply(anomaly_radius, axis=1)

    df["tooltip_title"] = "⚠ Anomaly"
    df["tooltip_line_1"] = "ID: " + df["anomaly_id"].astype(str)
    df["tooltip_line_2"] = "Confidence: " + df["confidence"].round(3).astype(str)
    df["tooltip_line_3"] = "NDVI Δ: " + df["ndvi_delta"].round(3).astype(str)
    df["tooltip_line_4"] = "CNN: " + df["cnn_score"].round(3).astype(str)
    df["tooltip_line_5"] = (
        "Window: " + df["date_old"].astype(str) + " → " + df["date_new"].astype(str)
    )
    df = ensure_tooltip_columns(add_layer_type(df, "anomaly"))

    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
        stroked=True,
        get_line_color=[239, 68, 68, 180],
        get_line_width=2,
    )


def build_radius_circle_layer(lat: float, lon: float, radius_km: float) -> pdk.Layer:
    """Build a PolygonLayer rendering a correlation radius circle around a given lat/lon point."""
    steps = 64
    coords = []
    for i in range(steps + 1):
        angle = math.radians(i * 360 / steps)
        dlat = (radius_km / 111.0) * math.cos(angle)
        cos_lat = math.cos(math.radians(lat))
        dlon = (
            0.0
            if abs(cos_lat) < 1e-6
            else (radius_km / (111.0 * cos_lat)) * math.sin(angle)
        )
        coords.append([lon + dlon, lat + dlat])

    return pdk.Layer(
        "PolygonLayer",
        data=pd.DataFrame({"polygon": [coords]}),
        get_polygon="polygon",
        get_fill_color=[239, 68, 68, 5],
        get_line_color=[239, 68, 68, 70],
        get_line_width=20,
        pickable=False,
    )


def get_correlated_vessel_mmsis(correlated_df, vessel_df, radius_km):
    """
    Return the set of MMSIs for vessels that fall within the correlation radius of any anomaly.

    Used to highlight correlated vessels with a red outline on the map.
    """
    if correlated_df.empty or vessel_df.empty:
        return set()

    out = set()

    for _, r in correlated_df.iterrows():
        nearby = find_nearby_vessels(
            float(r["lat"]),
            float(r["lon"]),
            vessel_df,
            radius_km,
        )
        if not nearby.empty:
            out.update(nearby["mmsi"].dropna().astype(int).tolist())

    return out


@st.cache_data(ttl=30, show_spinner=False)
def load_dashboard_data(
    hours: int,
    selected_mmsi: int | None = None,
) -> tuple[
    dict, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict, pd.DataFrame
]:
    """
    Load all core dashboard data in a single cached call.

    Fetches anomaly events from Snowflake, vessel and aircraft tracks from PostGIS,
    full vessel history for loiter detection, AOI bounds, and vessel ping history
    for the currently selected vessel.

    Args:
        hours: Number of hours to look back for tracking data.
        selected_mmsi: MMSI of the currently selected vessel, or None.

    Returns:
        Tuple of summary dict, anomaly_df, vessel_df, aircraft_df,
        all_history_df, bbox dict, and vessel_history df.
    """
    summary = fetch_anomaly_summary()
    anomaly_df = fetch_anomaly_events(limit=50)
    vessel_df = fetch_vessel_tracks(hours=hours)
    aircraft_df = fetch_aircraft_tracks(hours=hours)
    all_history_df = fetch_all_vessel_history(hours=hours)
    bbox = get_aoi_bounds()
    vessel_history = (
        fetch_vessel_history(selected_mmsi, hours=hours)
        if selected_mmsi is not None
        else pd.DataFrame()
    )
    return (
        summary,
        anomaly_df,
        vessel_df,
        aircraft_df,
        all_history_df,
        bbox,
        vessel_history,
    )


def apply_vessel_filters(
    vessel_df: pd.DataFrame,
    vessel_filter: list[str],
    loitering_mmsis: set[int],
    speed_range: tuple[float, float],
    moving_threshold: float,
) -> pd.DataFrame:
    """
    Apply sidebar filters to the vessel dataframe.

    Filters by vessel state (Moving, Docked, Loitering) and speed range.
    Returns a deduplicated subset of the input dataframe.

    Args:
        vessel_df: Raw vessel tracks dataframe.
        vessel_filter: List of selected vessel state options.
        loitering_mmsis: Set of MMSIs detected as loitering.
        speed_range: Tuple of (min_speed, max_speed) in knots.
        moving_threshold: Speed threshold in knots that separates moving from docked.

    Returns:
        Filtered vessel dataframe.
    """
    if vessel_df.empty:
        return vessel_df

    if not vessel_filter:
        filtered = vessel_df.copy()
    else:
        parts: list[pd.DataFrame] = []

        if "Moving" in vessel_filter:
            parts.append(vessel_df[vessel_df["speed_knots"] >= moving_threshold])

        if "Docked / Anchored" in vessel_filter:
            parts.append(vessel_df[vessel_df["speed_knots"] < moving_threshold])

        if "Loitering" in vessel_filter:
            parts.append(vessel_df[vessel_df["mmsi"].isin(loitering_mmsis)])

        if parts:
            filtered = pd.concat(parts, ignore_index=True).drop_duplicates(
                subset="mmsi"
            )
        else:
            filtered = vessel_df.iloc[0:0].copy()

    if "speed_knots" not in filtered.columns:
        return vessel_df.iloc[0:0].copy()

    filtered = filtered[
        (filtered["speed_knots"] >= speed_range[0])
        & (filtered["speed_knots"] <= speed_range[1])
    ]

    return filtered


def _imagery_search_dirs() -> list[Path]:
    """Return the ordered list of local directories to search for Sentinel GeoTIFF files."""
    return [
        PROJECT_ROOT / "imagery" / "processed",
        PROJECT_ROOT / "imagery" / "downloads",
        PROJECT_ROOT / "imagery" / "events",
        PROJECT_ROOT / "imagery",
    ]


@st.cache_data(ttl=300, show_spinner=False)
def list_local_rasters() -> list[str]:
    """Return a sorted deduplicated list of all local GeoTIFF file paths found across imagery directories."""
    files: list[str] = []
    for root in _imagery_search_dirs():
        if not root.exists():
            continue
        for ext in ("*.tif", "*.tiff", "*.TIF", "*.TIFF"):
            files.extend(str(p) for p in root.rglob(ext))
    return sorted(set(files))


def _find_raster_for_date(date_hint: str | None) -> str | None:
    """
    Find the best matching local raster file for a given date hint string.

    Matches by compact date (YYYYMMDD) or ISO date (YYYY-MM-DD) in the filename.

    Args:
        date_hint: Date string in any common format, or None.

    Returns:
        Path string of the best match, or None if not found.
    """
    rasters = list_local_rasters()
    if not rasters:
        return None

    if date_hint:
        date_hint = str(date_hint)[:10]
        compact = date_hint.replace("-", "")
        matches = [
            p for p in rasters if compact in Path(p).name or date_hint in Path(p).name
        ]
        if matches:
            return sorted(matches)[-1]

    return None


def _pick_before_after_rasters(
    date_old: str | None, date_new: str | None
) -> tuple[str | None, str | None]:
    """
    Select before and after raster paths for a given anomaly collection window.

    Falls back to the two most recent rasters if no date-matched files are found.

    Args:
        date_old: Earlier collection date string.
        date_new: Later collection date string.

    Returns:
        Tuple of (before_path, after_path), either of which may be None.
    """
    rasters = list_local_rasters()
    if not rasters:
        return None, None

    old_path = _find_raster_for_date(date_old)
    new_path = _find_raster_for_date(date_new)

    if old_path or new_path:
        return old_path, new_path

    rasters = sorted(rasters)
    if len(rasters) == 1:
        return rasters[0], rasters[0]
    return rasters[-2], rasters[-1]


def _normalize_rgb(data: np.ndarray) -> np.ndarray:
    """
    Normalize a (C, H, W) float32 array to a (H, W, 3) uint8 RGB image.

    Applies 2nd/98th percentile contrast stretch and clips to [0, 255].
    """
    data = np.nan_to_num(data, nan=0.0, posinf=0.0, neginf=0.0)
    rgb = np.moveaxis(data, 0, -1)
    lo = np.percentile(rgb, 2)
    hi = np.percentile(rgb, 98)
    if hi <= lo:
        hi = lo + 1.0
    rgb = np.clip((rgb - lo) / (hi - lo), 0, 1)
    return (rgb * 255).astype("uint8")


def _build_rgb_preview_from_dataset(
    src: rasterio.io.DatasetReader, out_h: int, out_w: int
) -> tuple[np.ndarray, str]:
    """
    Read and normalize a rasterio dataset into a displayable RGB preview array.

    Handles 1-band (greyscale), 2-band (derived), and 3+ band (true color) inputs.

    Args:
        src: Open rasterio dataset.
        out_h: Output height in pixels.
        out_w: Output width in pixels.

    Returns:
        Tuple of (uint8 RGB array, description string).
    """
    count = src.count

    if count >= 3:
        data = src.read(
            [1, 2, 3],
            out_shape=(3, out_h, out_w),
            resampling=rasterio.enums.Resampling.bilinear,
        ).astype("float32")
        return _normalize_rgb(data), "3-band preview"

    if count == 2:
        data = src.read(
            [1, 2],
            out_shape=(2, out_h, out_w),
            resampling=rasterio.enums.Resampling.bilinear,
        ).astype("float32")
        derived = np.stack([data[1], data[0], data[0]], axis=0)
        return _normalize_rgb(derived), "2-band derived preview"

    band = src.read(
        1,
        out_shape=(out_h, out_w),
        resampling=rasterio.enums.Resampling.bilinear,
    ).astype("float32")
    gray = np.stack([band, band, band], axis=0)
    return _normalize_rgb(gray), "single-band preview"


@st.cache_data(ttl=300, show_spinner=False)
def load_raster_preview(path_str: str, max_dim: int = 900) -> tuple[np.ndarray, dict]:
    """
    Load and downsample a GeoTIFF to a preview-sized RGB array.

    Args:
        path_str: Absolute path to the GeoTIFF file.
        max_dim: Maximum dimension (width or height) of the output preview.

    Returns:
        Tuple of (uint8 RGB array, metadata dict).
    """
    path = Path(path_str)

    with rasterio.open(path) as src:
        scale = max(src.width, src.height) / max_dim
        if scale < 1:
            out_h, out_w = src.height, src.width
        else:
            out_h = max(1, int(src.height / scale))
            out_w = max(1, int(src.width / scale))

        image, mode_desc = _build_rgb_preview_from_dataset(src, out_h, out_w)

        meta = {
            "path": str(path),
            "name": path.name,
            "mode_desc": mode_desc,
            "orig_width": src.width,
            "orig_height": src.height,
            "preview_width": out_w,
            "preview_height": out_h,
            "count": src.count,
            "nodata": src.nodata,
        }
        return image, meta


def _draw_rect(
    img: np.ndarray,
    x: int,
    y: int,
    w: int,
    h: int,
    color: tuple[int, int, int] = (255, 64, 64),
    thickness: int = 3,
) -> np.ndarray:
    """
    Draw a colored rectangle outline on a copy of an image array.

    Args:
        img: Input (H, W, 3) uint8 image.
        x: Left edge in pixels.
        y: Top edge in pixels.
        w: Width in pixels.
        h: Height in pixels.
        color: RGB tuple for the outline color.
        thickness: Line thickness in pixels.

    Returns:
        Copy of the image with the rectangle drawn.
    """
    out = img.copy()
    hh, ww = out.shape[:2]

    x1 = max(0, min(ww - 1, x))
    y1 = max(0, min(hh - 1, y))
    x2 = max(0, min(ww - 1, x + w))
    y2 = max(0, min(hh - 1, y + h))

    for t in range(thickness):
        if y1 + t < hh:
            out[y1 + t, x1 : x2 + 1] = color
        if y2 - t >= 0:
            out[y2 - t, x1 : x2 + 1] = color
        if x1 + t < ww:
            out[y1 : y2 + 1, x1 + t] = color
        if x2 - t >= 0:
            out[y1 : y2 + 1, x2 - t] = color

    return out


def draw_patch_box_on_preview(
    preview_img: np.ndarray,
    preview_meta: dict,
    row_px: int,
    col_px: int,
    patch_size: int,
) -> np.ndarray:
    """
    Draw a red bounding box on a preview image at the location of an anomaly patch.

    Scales pixel coordinates from original raster resolution to preview resolution.

    Args:
        preview_img: Downsampled preview image array.
        preview_meta: Metadata dict from load_raster_preview.
        row_px: Top-left row pixel offset in original raster coordinates.
        col_px: Top-left column pixel offset in original raster coordinates.
        patch_size: Patch size in original raster pixels.

    Returns:
        Preview image with bounding box drawn.
    """
    scale_x = preview_meta["preview_width"] / preview_meta["orig_width"]
    scale_y = preview_meta["preview_height"] / preview_meta["orig_height"]

    x = int(col_px * scale_x)
    y = int(row_px * scale_y)
    w = max(2, int(patch_size * scale_x))
    h = max(2, int(patch_size * scale_y))

    return _draw_rect(preview_img, x, y, w, h)


def _clip_window(
    row_px: int, col_px: int, patch_size: int, width: int, height: int
) -> Window:
    """
    Compute a rasterio Window clamped to raster bounds.

    Args:
        row_px: Row offset in pixels.
        col_px: Column offset in pixels.
        patch_size: Requested window size in pixels.
        width: Raster width in pixels.
        height: Raster height in pixels.

    Returns:
        Rasterio Window object.
    """
    row = max(0, min(height - 1, row_px))
    col = max(0, min(width - 1, col_px))

    win_h = min(patch_size, height - row)
    win_w = min(patch_size, width - col)

    return Window(col_off=col, row_off=row, width=win_w, height=win_h)


def _estimate_bright_fraction(img: np.ndarray) -> float:
    """
    Estimate the fraction of pixels with brightness >= 230 in an image.

    Used as a proxy for cloud or haze contamination.

    Args:
        img: RGB or greyscale image array.

    Returns:
        Float fraction of bright pixels in [0, 1].
    """
    if img.size == 0:
        return 0.0
    if img.ndim == 3:
        brightness = img.mean(axis=2)
    else:
        brightness = img
    return float((brightness >= 230).mean())


@st.cache_data(ttl=300, show_spinner=False)
def load_patch_crop(
    path_str: str, row_px: int, col_px: int, patch_size: int, max_dim: int = 320
) -> tuple[np.ndarray, dict]:
    """
    Load a cropped patch from a GeoTIFF at a given pixel offset and compute quality diagnostics.

    Args:
        path_str: Absolute path to the GeoTIFF file.
        row_px: Top-left row in original raster pixels.
        col_px: Top-left column in original raster pixels.
        patch_size: Patch size in pixels.
        max_dim: Maximum dimension for the output crop image.

    Returns:
        Tuple of (uint8 RGB crop array, diagnostics dict).
    """
    path = Path(path_str)

    with rasterio.open(path) as src:
        window = _clip_window(row_px, col_px, patch_size, src.width, src.height)
        win_h = int(window.height)
        win_w = int(window.width)

        if win_h <= 0 or win_w <= 0:
            raise ValueError("Patch window is outside raster bounds")

        scale = max(win_w, win_h) / max_dim
        if scale < 1:
            out_h, out_w = win_h, win_w
        else:
            out_h = max(1, int(win_h / scale))
            out_w = max(1, int(win_w / scale))

        count = src.count

        raw = src.read(
            list(range(1, min(count, 3) + 1)),
            window=window,
            out_shape=(min(count, 3), out_h, out_w),
            resampling=rasterio.enums.Resampling.bilinear,
        ).astype("float32")

        if count >= 3:
            img = _normalize_rgb(raw[:3])
        elif count == 2:
            derived = np.stack([raw[1], raw[0], raw[0]], axis=0)
            img = _normalize_rgb(derived)
        else:
            band = raw[0]
            gray = np.stack([band, band, band], axis=0)
            img = _normalize_rgb(gray)

        try:
            mask = src.read_masks(1, window=window, out_shape=(out_h, out_w))
            mask_nodata_fraction = float((mask == 0).mean())
        except Exception:
            mask_nodata_fraction = 0.0

        raw_mean = raw.mean(axis=0) if raw.ndim == 3 else raw
        near_zero_fraction = float((raw_mean <= 1e-6).mean())
        display_dark_fraction = float((img.mean(axis=2) <= 3).mean())

        nodata_fraction = max(
            mask_nodata_fraction, near_zero_fraction, display_dark_fraction
        )

        edge_margin = min(
            row_px,
            col_px,
            src.height - (row_px + patch_size),
            src.width - (col_px + patch_size),
        )
        edge_near = edge_margin < patch_size * 0.25
        bright_fraction = _estimate_bright_fraction(img)

        diagnostics = {
            "path": str(path),
            "name": path.name,
            "nodata_fraction": nodata_fraction,
            "mask_nodata_fraction": mask_nodata_fraction,
            "near_zero_fraction": near_zero_fraction,
            "display_dark_fraction": display_dark_fraction,
            "bright_fraction": bright_fraction,
            "near_edge": bool(edge_near),
            "window_h": win_h,
            "window_w": win_w,
        }
        return img, diagnostics


def render_quality_chips(diag: dict) -> None:
    """
    Render HTML quality indicator chips for a patch crop diagnostic result.

    Shows warnings for edge proximity, high no-data fraction, and high bright pixel fraction.

    Args:
        diag: Diagnostics dict from load_patch_crop.
    """
    chips: list[str] = []

    if diag.get("near_edge"):
        chips.append('<span class="warning-chip">Near raster edge</span>')
    else:
        chips.append('<span class="ok-chip">Not near edge</span>')

    nodata_fraction = float(diag.get("nodata_fraction", 0.0))
    if nodata_fraction >= 0.25:
        chips.append(
            f'<span class="warning-chip">High empty/no-data {nodata_fraction:.0%}</span>'
        )
    elif nodata_fraction > 0:
        chips.append(
            f'<span class="ok-chip">Empty/no-data {nodata_fraction:.0%}</span>'
        )
    else:
        chips.append('<span class="ok-chip">No empty/no-data detected</span>')

    bright_fraction = float(diag.get("bright_fraction", 0.0))
    if bright_fraction >= 0.35:
        chips.append(
            f'<span class="warning-chip">High bright fraction {bright_fraction:.0%} (possible cloud/haze)</span>'
        )
    elif bright_fraction >= 0.15:
        chips.append(
            f'<span class="warning-chip">Moderate bright fraction {bright_fraction:.0%}</span>'
        )
    else:
        chips.append(
            f'<span class="ok-chip">Bright fraction {bright_fraction:.0%}</span>'
        )

    st.markdown("".join(chips), unsafe_allow_html=True)


def render_sentinel_anomaly_views(
    date_old: str | None,
    date_new: str | None,
    row_px: int | None,
    col_px: int | None,
    patch_size: int | None,
) -> None:
    """
    Render the full Sentinel imagery comparison panel for a selected anomaly.

    Shows before/after scene previews with patch bounding box, and before/after
    patch crops with quality diagnostic chips.

    Args:
        date_old: Earlier collection date string.
        date_new: Later collection date string.
        row_px: Anomaly patch row offset in original raster pixels.
        col_px: Anomaly patch column offset in original raster pixels.
        patch_size: Patch size in pixels.
    """
    old_path, new_path = _pick_before_after_rasters(date_old, date_new)

    st.markdown(
        '<div class="section-header">Sentinel Raster Preview</div>',
        unsafe_allow_html=True,
    )

    if not old_path and not new_path:
        st.info(
            "No local Sentinel GeoTIFFs were found in imagery/processed, imagery/downloads, "
            "imagery/events, or imagery/. Run the imagery pipeline first, or point the dashboard "
            "at your processed raster outputs."
        )
        return

    c1, c2 = st.columns(2)

    with c1:
        with st.container(border=True):
            st.markdown("**Before Scene**")
            if old_path:
                try:
                    img_old, meta_old = load_raster_preview(old_path)
                    if (
                        row_px is not None
                        and col_px is not None
                        and patch_size is not None
                    ):
                        img_old = draw_patch_box_on_preview(
                            img_old, meta_old, row_px, col_px, patch_size
                        )
                    st.image(
                        img_old,
                        caption=f"{meta_old['name']} - {meta_old['mode_desc']}",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.warning(f"Failed to render before-scene preview: {e}")
            else:
                st.info("No matching raster found for the earlier collection date.")

    with c2:
        with st.container(border=True):
            st.markdown("**After Scene**")
            if new_path:
                try:
                    img_new, meta_new = load_raster_preview(new_path)
                    if (
                        row_px is not None
                        and col_px is not None
                        and patch_size is not None
                    ):
                        img_new = draw_patch_box_on_preview(
                            img_new, meta_new, row_px, col_px, patch_size
                        )
                    st.image(
                        img_new,
                        caption=f"{meta_new['name']} - {meta_new['mode_desc']}",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.warning(f"Failed to render after-scene preview: {e}")
            else:
                st.info("No matching raster found for the later collection date.")

    if row_px is None or col_px is None or patch_size is None:
        return

    st.markdown(
        '<div class="section-header">Anomaly Patch Crop</div>', unsafe_allow_html=True
    )
    c3, c4 = st.columns(2)

    with c3:
        with st.container(border=True):
            st.markdown("**Before Patch**")
            if old_path:
                try:
                    patch_old, diag_old = load_patch_crop(
                        old_path, row_px, col_px, patch_size
                    )
                    st.image(
                        patch_old,
                        caption=f"{diag_old['name']} - patch @ row {row_px}, col {col_px}",
                        use_container_width=True,
                    )
                    render_quality_chips(diag_old)
                except Exception as e:
                    st.warning(f"Failed to render before-patch crop: {e}")
            else:
                st.info("No matching raster found for the earlier collection date.")

    with c4:
        with st.container(border=True):
            st.markdown("**After Patch**")
            if new_path:
                try:
                    patch_new, diag_new = load_patch_crop(
                        new_path, row_px, col_px, patch_size
                    )
                    st.image(
                        patch_new,
                        caption=f"{diag_new['name']} - patch @ row {row_px}, col {col_px}",
                        use_container_width=True,
                    )
                    render_quality_chips(diag_new)
                except Exception as e:
                    st.warning(f"Failed to render after-patch crop: {e}")
            else:
                st.info("No matching raster found for the later collection date.")

    st.caption(
        "Red box marks the anomaly patch on the full scene preview. Patch diagnostics flag "
        "edge effects, empty/no-data coverage, and high bright-pixel fractions that may indicate "
        "cloud or haze contamination."
    )


def main() -> None:
    """
    Main entry point for the Geospatial Intelligence Dashboard.

    Initialises session state, renders the sidebar, loads all data,
    applies filters, and renders the four-tab intelligence dashboard.
    """
    for key, default in [
        ("selected_vessel_mmsi", None),
        ("selected_aircraft_icao24", None),
        ("selected_anomaly_id", None),
        ("hours", 24),
        ("show_vessels", True),
        ("show_aircraft", True),
        ("loiter_only", False),
        ("radius_km", 20),
        ("conf_min", 0.0),
        ("corr_hours", 24),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    st.markdown(
        f"""
        <div class="dash-header">
            <div class="dash-title">Geospatial Intelligence Platform</div>
            <div class="dash-badge">LIVE</div>
        </div>
        <div class="dash-subtitle">
            {AOI_NAME} - {AOI_SUBTITLE}
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:

        # ───────────────────────── WORKSPACE / PRESETS ─────────────────────────
        st.markdown(
            '<div class="section-header">Workspace</div>', unsafe_allow_html=True
        )
        priority_default = ["URGENT", "HIGH", "MEDIUM", "LOW"]
        if st.button("Reset Filters", use_container_width=True):
            st.session_state.hours = 24
            st.session_state.show_vessels = True
            st.session_state.show_aircraft = True
            st.session_state.radius_km = 20
            st.session_state.conf_min = 0.0
            st.session_state.corr_hours = 24
            st.session_state.vessel_filter = ["Moving", "Docked / Anchored"]
            st.session_state.speed_range = (0.0, 30.0)
            st.session_state.alt_range = (0, 15000)
            st.session_state.priority_filter = ["URGENT", "HIGH", "MEDIUM", "LOW"]
            st.session_state.selected_vessel_mmsi = None
            st.session_state.selected_aircraft_icao24 = None
            st.rerun()

        st.markdown("---")

        # ───────────────────────── DISPLAY ─────────────────────────
        st.markdown('<div class="section-header">Display</div>', unsafe_allow_html=True)

        hours = st.slider(
            "Time Window (hours)", 1, 72, st.session_state.hours, key="hours"
        )

        show_vessels = st.checkbox(
            "Show Vessels",
            key="show_vessels",
        )

        show_aircraft = st.checkbox(
            "Show Aircraft",
            key="show_aircraft",
        )

        st.markdown("---")

        # ───────────────────────── ASSET FILTERS ─────────────────────────
        st.markdown(
            '<div class="section-header">Asset Filters</div>', unsafe_allow_html=True
        )

        vessel_filter = st.multiselect(
            "Vessel State",
            ["Moving", "Docked / Anchored", "Loitering"],
            default=["Moving", "Docked / Anchored"],
            key="vessel_filter",
        )

        speed_range = st.slider(
            "Vessel Speed (kn)",
            0.0,
            30.0,
            (0.0, 30.0),
            step=0.5,
            key="speed_range",
        )

        alt_range = st.slider(
            "Aircraft Altitude (m)",
            0,
            15000,
            (0, 15000),
            step=100,
            key="alt_range",
        )

        st.markdown("---")

        # ───────────────────────── ANOMALY FILTERS ─────────────────────────
        st.markdown(
            '<div class="section-header">Anomaly Filters</div>', unsafe_allow_html=True
        )

        conf_min = st.slider(
            "Confidence Threshold",
            0.0,
            1.0,
            st.session_state.conf_min,
            step=0.01,
            key="conf_min",
        )

        priority_filter = st.multiselect(
            "Priority",
            ["URGENT", "HIGH", "MEDIUM", "LOW"],
            default=["URGENT", "HIGH", "MEDIUM", "LOW"],
            key="priority_filter",
        )

        st.markdown("---")

        # ───────────────────────── CORRELATION ─────────────────────────
        st.markdown(
            '<div class="section-header">Correlation</div>', unsafe_allow_html=True
        )

        radius_km = st.slider(
            "Nearby Asset Radius (km)",
            5,
            200,
            st.session_state.radius_km,
            key="radius_km",
        )

        corr_hours = st.slider(
            "Activity Window (hours)",
            6,
            72,
            st.session_state.corr_hours,
            key="corr_hours",
        )

        st.markdown("---")

        # ───────────────────────── LEGEND ─────────────────────────
        with st.expander("Legend", expanded=False):
            st.markdown(
                """
                <div style="font-size:0.75rem;color:#9ca3af;line-height:1.8">

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#6b7280;margin-right:8px"></span>Docked / Anchored</div>

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#00c832;margin-right:8px"></span>Moving Slow</div>

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#ff6400;margin-right:8px"></span>Moving Fast</div>

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#eab308;margin-right:8px"></span>Loitering</div>

                <hr style="border-color:#1f2937; margin:6px 0">

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#3b82f6;margin-right:8px"></span>Selected Vessel</div>

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#10b981;margin-right:8px"></span>Track Start</div>

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#38bdf8;margin-right:8px"></span>Aircraft</div>
                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#a855f7;margin-right:8px"></span>Selected Aircraft</div>

                <hr style="border-color:#1f2937; margin:6px 0">

                <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#ef4444;margin-right:8px"></span>Anomaly</div>

                </div>
                """,
                unsafe_allow_html=True,
            )

    last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with st.spinner("Loading intelligence data..."):
        (
            summary,
            anomaly_df,
            vessel_df,
            aircraft_df,
            all_history_df,
            bbox,
            vessel_history,
        ) = load_dashboard_data(
            hours,
            selected_mmsi=st.session_state.selected_vessel_mmsi,
        )

    loitering_mmsis = detect_loitering_vessels(all_history_df)

    filtered_vessels = apply_vessel_filters(
        vessel_df,
        vessel_filter,
        loitering_mmsis,
        speed_range,
        MOVING_THRESHOLD,
    )

    filtered_aircraft = aircraft_df.copy()
    if not filtered_aircraft.empty:
        filtered_aircraft = filtered_aircraft[
            (filtered_aircraft["altitude_m"] >= alt_range[0])
            & (filtered_aircraft["altitude_m"] <= alt_range[1])
        ]

    correlated_df = build_correlated_events(
        anomaly_df,
        filtered_vessels,
        filtered_aircraft,
        radius_km=radius_km,
    )

    if not correlated_df.empty:
        correlated_df = correlated_df[correlated_df["confidence"] >= conf_min]
        correlated_df = correlated_df[correlated_df["priority"].isin(priority_filter)]

    correlated_vessel_mmsis = get_correlated_vessel_mmsis(
        correlated_df,
        filtered_vessels,
        radius_km,
    )

    high_priority = 0
    if not correlated_df.empty:
        high_priority = len(
            correlated_df[correlated_df["priority"].isin(["URGENT", "HIGH"])]
        )

    if st.session_state.selected_anomaly_id is None and not correlated_df.empty:
        st.session_state.selected_anomaly_id = correlated_df.iloc[0]["anomaly_id"]

    if (
        st.session_state.selected_anomaly_id is not None
        and not correlated_df.empty
        and st.session_state.selected_anomaly_id not in set(correlated_df["anomaly_id"])
    ):
        st.session_state.selected_anomaly_id = correlated_df.iloc[0]["anomaly_id"]

    aoi_center_lat = (bbox["min_lat"] + bbox["max_lat"]) / 2
    aoi_center_lon = (bbox["min_lon"] + bbox["max_lon"]) / 2

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Mission Overview",
            "Live Tracking",
            "Land Change Detection",
            "Correlated Events",
        ]
    )

    with tab1:
        render_kpi_cards(
            vessel_count=len(filtered_vessels) if show_vessels else 0,
            aircraft_count=len(filtered_aircraft) if show_aircraft else 0,
            anomaly_count=len(correlated_df),
            high_priority_count=high_priority,
            last_updated=last_updated,
        )

        st.markdown("<div style='margin-top:1.2rem'></div>", unsafe_allow_html=True)
        col_left, col_right = st.columns([1, 1.6])

        with col_left:
            render_aoi_summary(
                area_name=AOI_NAME,
                monitoring_window=f"Last {hours}h",
                date_old=summary["earliest_date"],
                date_new=summary["latest_date"],
                anomaly_count=len(correlated_df),
                avg_confidence=summary["avg_confidence"],
            )
            render_how_to_interpret()

        with col_right:
            overview_layers = [build_aoi_layer(bbox)]

            if show_vessels:
                overview_layers.append(
                    build_vessel_layer(
                        filtered_vessels,
                        loitering_mmsis=loitering_mmsis,
                        correlated_mmsis=correlated_vessel_mmsis,
                    )
                )

            if show_aircraft:
                overview_layers.append(build_aircraft_layer(filtered_aircraft))

            overview_layers.append(
                build_anomaly_layer(correlated_df, st.session_state.selected_anomaly_id)
            )

            render_map(overview_layers, aoi_center_lat, aoi_center_lon, zoom=9)

    with tab2:
        moving_count = (
            len(filtered_vessels[filtered_vessels["speed_knots"] >= MOVING_THRESHOLD])
            if not filtered_vessels.empty
            else 0
        )
        docked_count = (
            len(filtered_vessels[filtered_vessels["speed_knots"] < MOVING_THRESHOLD])
            if not filtered_vessels.empty
            else 0
        )

        visible_loitering = (
            filtered_vessels[filtered_vessels["mmsi"].isin(loitering_mmsis)]
            if not filtered_vessels.empty
            else pd.DataFrame()
        )
        loiter_count = len(visible_loitering)

        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric("Total Vessels", len(filtered_vessels))
        mc2.metric("Moving", moving_count)
        mc3.metric("Docked", docked_count)
        mc4.metric("Loitering", loiter_count)
        mc5.metric("Aircraft", len(filtered_aircraft))

        st.markdown("<div style='margin-top:0.6rem'></div>", unsafe_allow_html=True)
        map_col, detail_col = st.columns([2.2, 1])

        with map_col:
            vessel_options: dict[str, int | None] = {"None - show all vessels": None}
            if not filtered_vessels.empty and show_vessels:
                for _, r in filtered_vessels.sort_values("vessel_name").iterrows():
                    label = f"{r['vessel_name']} ({int(r['mmsi'])})"
                    if int(r["mmsi"]) in loitering_mmsis:
                        label += " 🟡"
                    vessel_options[label] = int(r["mmsi"])

            current_vessel_label = "None - show all vessels"
            if st.session_state.selected_vessel_mmsi is not None:
                for label, mmsi in vessel_options.items():
                    if mmsi == st.session_state.selected_vessel_mmsi:
                        current_vessel_label = label
                        break

            chosen_vessel_label = st.selectbox(
                "🔍 Select vessel to track",
                list(vessel_options.keys()),
                index=list(vessel_options.keys()).index(current_vessel_label),
                key="vessel_selectbox_live_tracking",
            )

            new_mmsi = vessel_options[chosen_vessel_label]
            if new_mmsi != st.session_state.selected_vessel_mmsi:
                st.session_state.selected_vessel_mmsi = new_mmsi
                if new_mmsi is not None:
                    st.session_state.selected_aircraft_icao24 = None
                st.rerun()

            aircraft_options: dict[str, str | None] = {"None - show all aircraft": None}
            if not filtered_aircraft.empty and show_aircraft:
                aircraft_sorted = filtered_aircraft.copy()
                aircraft_sorted["callsign_clean"] = (
                    aircraft_sorted["callsign"]
                    .fillna("Unknown")
                    .astype(str)
                    .str.strip()
                )

                for _, r in aircraft_sorted.sort_values(
                    ["callsign_clean", "icao24"]
                ).iterrows():
                    callsign = r["callsign_clean"] or "Unknown"
                    label = f"{callsign} ({r['icao24']})"
                    aircraft_options[label] = str(r["icao24"])

            current_aircraft_label = "None - show all aircraft"
            if st.session_state.selected_aircraft_icao24 is not None:
                for label, icao24 in aircraft_options.items():
                    if icao24 == st.session_state.selected_aircraft_icao24:
                        current_aircraft_label = label
                        break

            chosen_aircraft_label = st.selectbox(
                "✈ Select aircraft to track",
                list(aircraft_options.keys()),
                index=list(aircraft_options.keys()).index(current_aircraft_label),
                key="aircraft_selectbox_live_tracking",
            )

            new_icao24 = aircraft_options[chosen_aircraft_label]
            if new_icao24 != st.session_state.selected_aircraft_icao24:
                st.session_state.selected_aircraft_icao24 = new_icao24
                if new_icao24 is not None:
                    st.session_state.selected_vessel_mmsi = None
                st.rerun()

            selected_mmsi = st.session_state.selected_vessel_mmsi
            selected_icao24 = st.session_state.selected_aircraft_icao24

            tracking_layers: list[pdk.Layer | None] = [build_aoi_layer(bbox)]

            if show_vessels and not filtered_vessels.empty:
                tracking_layers.append(
                    build_vessel_layer(
                        filtered_vessels,
                        selected_mmsi,
                        loitering_mmsis,
                        correlated_vessel_mmsis,
                    )
                )
                if selected_mmsi is not None and not vessel_history.empty:
                    tracking_layers.append(build_ping_history_layer(vessel_history))
                    tracking_layers.append(build_track_path_layer(vessel_history))
                    tracking_layers.append(build_track_start_layer(vessel_history))
                    tracking_layers.append(
                        build_vessel_label_layer(filtered_vessels, selected_mmsi)
                    )

            if show_aircraft and not filtered_aircraft.empty:
                tracking_layers.append(
                    build_aircraft_layer(filtered_aircraft, selected_icao24)
                )

            if selected_mmsi is not None and not filtered_vessels.empty:
                sel_row = filtered_vessels[filtered_vessels["mmsi"] == selected_mmsi]
                if not sel_row.empty:
                    clat = float(sel_row.iloc[0]["latitude"])
                    clon = float(sel_row.iloc[0]["longitude"])
                    czoom = 13
                else:
                    clat, clon, czoom = aoi_center_lat, aoi_center_lon, 10
            elif selected_icao24 is not None and not filtered_aircraft.empty:
                sel_air = filtered_aircraft[
                    filtered_aircraft["icao24"].astype(str) == str(selected_icao24)
                ]
                if not sel_air.empty:
                    clat = float(sel_air.iloc[0]["latitude"])
                    clon = float(sel_air.iloc[0]["longitude"])
                    czoom = 12
                else:
                    clat, clon, czoom = aoi_center_lat, aoi_center_lon, 10
            else:
                clat, clon, czoom = aoi_center_lat, aoi_center_lon, 10

            if not show_vessels and not show_aircraft:
                st.info(
                    "Enable vessels or aircraft in the sidebar to see tracking data."
                )
            else:
                render_map(tracking_layers, clat, clon, czoom)

        with detail_col:
            with st.container(border=True):
                st.markdown(
                    '<div class="section-header">Asset Detail</div>',
                    unsafe_allow_html=True,
                )

                if selected_mmsi is not None and not filtered_vessels.empty:
                    sel_row = filtered_vessels[
                        filtered_vessels["mmsi"] == selected_mmsi
                    ]
                    if not sel_row.empty:
                        r = sel_row.iloc[0]
                        is_loitering = int(r["mmsi"]) in loitering_mmsis
                        status = (
                            "🟡 Loitering"
                            if is_loitering
                            else (
                                "🟢 Moving"
                                if float(r["speed_knots"]) >= MOVING_THRESHOLD
                                else "⚪ Docked"
                            )
                        )

                        st.markdown(
                            f"""
                            <div class="detail-label">Vessel Name</div>
                            <div class="detail-value">{r['vessel_name']}</div>
                            <div class="detail-label">MMSI</div>
                            <div class="detail-value">{int(r['mmsi'])}</div>
                            <div class="detail-label">Status</div>
                            <div class="detail-value">{status}</div>
                            <div class="detail-label">Speed</div>
                            <div class="detail-value">{float(r['speed_knots']):.1f} kn</div>
                            <div class="detail-label">Heading</div>
                            <div class="detail-value">{int(r['heading'])}°</div>
                            <div class="detail-label">Position</div>
                            <div class="detail-value">{float(r['latitude']):.4f}, {float(r['longitude']):.4f}</div>
                            <div class="detail-label">Last Seen</div>
                            <div class="detail-value">{str(r['received_at'])[:19]}</div>
                            """,
                            unsafe_allow_html=True,
                        )

                        if not vessel_history.empty:
                            avg_speed = vessel_history["speed_knots"].mean()
                            max_speed = vessel_history["speed_knots"].max()
                            st.markdown(
                                f"""
                                <div class="section-header" style="margin-top:1rem">Track History</div>
                                <div class="info-box">
                                    {len(vessel_history)} pings recorded over last {hours}h.<br/>
                                    Hover the small blue dots on the map to see speed,
                                    heading, and timestamp at each ping.
                                </div>
                                <div class="stat-row">
                                    <div class="stat-item">
                                        <div class="stat-item-label">Avg Speed</div>
                                        <div class="stat-item-value">{avg_speed:.1f} kn</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-item-label">Max Speed</div>
                                        <div class="stat-item-value">{max_speed:.1f} kn</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-item-label">Pings</div>
                                        <div class="stat-item-value">{len(vessel_history)}</div>
                                    </div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )

                elif (
                    st.session_state.selected_aircraft_icao24 is not None
                    and not filtered_aircraft.empty
                ):
                    sel_air = filtered_aircraft[
                        filtered_aircraft["icao24"].astype(str)
                        == str(st.session_state.selected_aircraft_icao24)
                    ]
                    if not sel_air.empty:
                        r = sel_air.iloc[0]
                        callsign = (
                            str(r["callsign"]).strip()
                            if pd.notna(r["callsign"])
                            else "Unknown"
                        )

                        st.markdown(
                            f"""
                            <div class="detail-label">Callsign</div>
                            <div class="detail-value">✈ {callsign or 'Unknown'}</div>
                            <div class="detail-label">ICAO24</div>
                            <div class="detail-value">{r['icao24']}</div>
                            <div class="detail-label">Altitude</div>
                            <div class="detail-value">{float(r['altitude_m']):.0f} m</div>
                            <div class="detail-label">Velocity</div>
                            <div class="detail-value">{float(r['velocity_ms']):.1f} m/s</div>
                            <div class="detail-label">Heading</div>
                            <div class="detail-value">{int(r['heading']) if pd.notna(r['heading']) else 0}°</div>
                            <div class="detail-label">Origin Country</div>
                            <div class="detail-value">{r['origin_country'] if pd.notna(r['origin_country']) else 'Unknown'}</div>
                            <div class="detail-label">Position</div>
                            <div class="detail-value">{float(r['latitude']):.4f}, {float(r['longitude']):.4f}</div>
                            <div class="detail-label">Last Seen</div>
                            <div class="detail-value">{str(r['received_at'])[:19]}</div>
                            """,
                            unsafe_allow_html=True,
                        )

                else:
                    st.markdown(
                        '<div class="info-box">Select a vessel or aircraft from the dropdowns to view details.</div>',
                        unsafe_allow_html=True,
                    )

                    if not filtered_aircraft.empty and show_aircraft:
                        st.markdown(
                            '<div class="section-header" style="margin-top:1rem">Aircraft</div>',
                            unsafe_allow_html=True,
                        )
                        for _, r in filtered_aircraft.head(6).iterrows():
                            st.markdown(
                                f"""
                                <div style="background:#111827;border:1px solid #1f2937;
                                    border-radius:6px;padding:0.5rem 0.8rem;
                                    margin-bottom:0.3rem;font-size:0.75rem">
                                    <span style="color:#f97316;font-weight:600">
                                        ✈ {r['callsign'] or 'Unknown'}
                                    </span>
                                    <span style="color:#6b7280;margin-left:0.5rem">
                                        {float(r['altitude_m']):.0f}m
                                    </span>
                                    <span style="color:#6b7280;margin-left:0.5rem">
                                        {float(r['velocity_ms']):.1f} m/s
                                    </span>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )

        with st.expander("📡 Vessel Track Data", expanded=False):
            if not filtered_vessels.empty:
                st.dataframe(
                    filtered_vessels[
                        [
                            "mmsi",
                            "vessel_name",
                            "latitude",
                            "longitude",
                            "speed_knots",
                            "heading",
                            "received_at",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No vessel tracks found")

        with st.expander("✈️ Aircraft Track Data", expanded=False):
            if not filtered_aircraft.empty:
                st.dataframe(
                    filtered_aircraft[
                        [
                            "icao24",
                            "callsign",
                            "origin_country",
                            "latitude",
                            "longitude",
                            "altitude_m",
                            "received_at",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No aircraft tracks found")

    with tab3:
        st.markdown(
            '<div class="section-header">Sentinel-2 Change Detection</div>',
            unsafe_allow_html=True,
        )

        if anomaly_df.empty:
            st.info("No anomaly events found in Snowflake.")
        else:
            ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 1.2])
            with ctrl1:
                conf_threshold = st.slider(
                    "Confidence threshold",
                    0.0,
                    1.0,
                    0.0,
                    step=0.01,
                    key="conf_threshold_tab3",
                )
            with ctrl2:
                ndvi_threshold = st.slider(
                    "NDVI delta threshold",
                    0.0,
                    1.0,
                    0.0,
                    step=0.01,
                    key="ndvi_threshold_tab3",
                )
            with ctrl3:
                st.markdown(
                    """
                    <div class="info-box" style="margin-top:1.6rem;">
                        This view combines anomaly selection, scene preview, patch crop,
                        and analyst assessment in one compact workflow.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            filtered_corr = correlated_df.copy()
            if not filtered_corr.empty:
                filtered_corr = filtered_corr.drop_duplicates(subset="anomaly_id")
                filtered_corr = filtered_corr[
                    (filtered_corr["confidence"] >= conf_threshold)
                    & (filtered_corr["ndvi_delta"] >= ndvi_threshold)
                ]

            top_left, top_right = st.columns([2.15, 1])

            with top_left:
                with st.container(border=True):
                    t3_layers = [
                        build_aoi_layer(bbox),
                        build_anomaly_layer(
                            filtered_corr, st.session_state.selected_anomaly_id
                        ),
                    ]

                    if st.session_state.selected_anomaly_id and not filtered_corr.empty:
                        sel_ano = filtered_corr[
                            filtered_corr["anomaly_id"]
                            == st.session_state.selected_anomaly_id
                        ]
                        if not sel_ano.empty:
                            alat = float(sel_ano.iloc[0]["lat"])
                            alon = float(sel_ano.iloc[0]["lon"])
                            render_map(t3_layers, alat, alon, zoom=12)
                        else:
                            render_map(
                                t3_layers, aoi_center_lat, aoi_center_lon, zoom=10
                            )
                    else:
                        render_map(t3_layers, aoi_center_lat, aoi_center_lon, zoom=10)

            with top_right:
                with st.container(border=True):
                    st.markdown(
                        '<div class="section-header">Anomaly Events</div>',
                        unsafe_allow_html=True,
                    )
                    st.markdown(
                        f'<div style="font-size:0.72rem;color:#6b7280;margin-bottom:0.7rem">{len(filtered_corr)} events above threshold</div>',
                        unsafe_allow_html=True,
                    )

                    visible_rows = filtered_corr.head(8)
                    for _, row in visible_rows.iterrows():
                        is_sel = (
                            row["anomaly_id"] == st.session_state.selected_anomaly_id
                        )
                        st.markdown(
                            render_anomaly_event_card(row.to_dict(), is_sel),
                            unsafe_allow_html=True,
                        )
                        if st.button(
                            f"{'▶ Selected' if is_sel else 'Select'} {row['anomaly_id']}",
                            key=f"t3btn_{row['anomaly_id']}",
                            use_container_width=True,
                        ):
                            st.session_state.selected_anomaly_id = row["anomaly_id"]
                            st.rerun()

                    if len(filtered_corr) > 8:
                        st.caption(
                            f"Showing top 8 of {len(filtered_corr)} anomaly events."
                        )

            if st.session_state.selected_anomaly_id and not filtered_corr.empty:
                sel_ano = filtered_corr[
                    filtered_corr["anomaly_id"] == st.session_state.selected_anomaly_id
                ]
                if not sel_ano.empty:
                    r = sel_ano.iloc[0]

                    st.markdown(
                        "<div style='margin-top:0.7rem'></div>", unsafe_allow_html=True
                    )

                    top1, top2 = st.columns([2.2, 1])
                    top1.metric("Anomaly ID", r["anomaly_id"])
                    top2.metric("Priority", r["priority"])

                    bot1, bot2, bot3 = st.columns(3)
                    bot1.metric("NDVI Delta", f"{r['ndvi_delta']:.3f}")
                    bot2.metric("CNN Score", f"{r['cnn_score']:.3f}")
                    bot3.metric("Confidence", f"{r['confidence']:.3f}")

                    with st.container(border=True):
                        st.markdown(
                            '<div class="section-header">Image-Based Assessment</div>',
                            unsafe_allow_html=True,
                        )

                        s1, s2, s3, s4 = st.columns([1.6, 1.8, 1, 1])
                        s1.markdown(
                            f"""
                            <div class="detail-label">Anomaly ID</div>
                            <div class="detail-value">{r['anomaly_id']}</div>
                            """,
                            unsafe_allow_html=True,
                        )
                        s2.markdown(
                            f"""
                            <div class="detail-label">Collection Window</div>
                            <div class="detail-value">{r['date_old']} → {r['date_new']}</div>
                            """,
                            unsafe_allow_html=True,
                        )
                        s3.markdown(
                            f"""
                            <div class="detail-label">NDVI Delta</div>
                            <div class="detail-value">{float(r['ndvi_delta']):.3f}</div>
                            """,
                            unsafe_allow_html=True,
                        )
                        s4.markdown(
                            f"""
                            <div class="detail-label">Model Confidence</div>
                            <div class="detail-value">{float(r['confidence']):.3f}</div>
                            """,
                            unsafe_allow_html=True,
                        )

                        st.markdown(
                            f"""
                            <div class="info-box" style="margin-top:0.4rem;">
                                This panel is for visual confirmation of anomaly <b>{r['anomaly_id']}</b>.
                                Use the scene previews, patch crops, and quality indicators below to assess
                                whether the detected signal is consistent with real surface change or is more
                                likely explained by cloud, haze, edge effects, or missing data.
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                    render_sentinel_anomaly_views(
                        date_old=str(r["date_old"]),
                        date_new=str(r["date_new"]),
                        row_px=int(r["row_px"]) if pd.notna(r["row_px"]) else None,
                        col_px=int(r["col_px"]) if pd.notna(r["col_px"]) else None,
                        patch_size=(
                            int(r["patch_size"]) if pd.notna(r["patch_size"]) else None
                        ),
                    )
            else:
                st.info(
                    "Select an anomaly from the right panel to inspect the scene preview and patch crop."
                )

        with st.expander("📊 All Anomaly Events", expanded=False):
            if not anomaly_df.empty:
                st.dataframe(anomaly_df, use_container_width=True, hide_index=True)

    with tab4:
        st.markdown(
            '<div class="section-header">Correlated Intelligence Events</div>',
            unsafe_allow_html=True,
        )

        corr_df_tab = correlated_df.copy()
        corr_df_tab = corr_df_tab.drop_duplicates(subset="anomaly_id")

        if corr_df_tab.empty:
            st.info("No correlated events match current filters.")
        else:
            col_table, col_map = st.columns([1, 1.8])

            with col_table:
                st.markdown(
                    '<div class="section-header">Ranked Events</div>',
                    unsafe_allow_html=True,
                )
                visible_rows = corr_df_tab.head(10)

                for _, row in visible_rows.iterrows():
                    is_sel = row["anomaly_id"] == st.session_state.selected_anomaly_id
                    st.markdown(
                        render_anomaly_event_card(row.to_dict(), is_sel),
                        unsafe_allow_html=True,
                    )

                    if st.button(
                        f"{'▶ Selected' if is_sel else 'Select'} {row['anomaly_id']}",
                        key=f"t4btn_{row['anomaly_id']}",
                        use_container_width=True,
                    ):
                        st.session_state.selected_anomaly_id = row["anomaly_id"]
                        st.rerun()

                if len(corr_df_tab) > 10:
                    st.caption(
                        f"Showing top 10 of {len(corr_df_tab)} correlated events."
                    )

            with col_map:
                selected_row = corr_df_tab[
                    corr_df_tab["anomaly_id"] == st.session_state.selected_anomaly_id
                ]

                if selected_row.empty:
                    selected_row = corr_df_tab.head(1)

                r = selected_row.iloc[0]
                center_lat = float(r["lat"])
                center_lon = float(r["lon"])
                zoom_level = 12

                st.caption(f"Focused on anomaly: {r['anomaly_id']}")

                nearby_v = find_nearby_vessels(
                    center_lat, center_lon, filtered_vessels, radius_km
                )
                nearby_a = find_nearby_aircraft(
                    center_lat, center_lon, filtered_aircraft, radius_km
                )

                layers = [
                    build_aoi_layer(bbox),
                    build_anomaly_layer(corr_df_tab, r["anomaly_id"]),
                    build_radius_circle_layer(center_lat, center_lon, radius_km),
                ]

                if not nearby_v.empty:
                    nearby_ids = set(nearby_v["mmsi"].dropna().astype(int).tolist())
                    layers.append(
                        build_vessel_layer(
                            nearby_v,
                            loitering_mmsis=loitering_mmsis,
                            correlated_mmsis=nearby_ids,
                        )
                    )

                render_map(layers, center_lat, center_lon, zoom=zoom_level)

                st.markdown("---")

                top1, top2 = st.columns([2.2, 1])
                top1.metric("Anomaly ID", r["anomaly_id"])
                top2.metric("Priority", r["priority"])

                bot1, bot2, bot3 = st.columns(3)
                bot1.metric("Nearby Vessels", f"{int(r['nearby_vessels']):,}")
                bot2.metric("Nearby Aircraft", f"{int(r['nearby_aircraft']):,}")
                bot3.metric(
                    "Nearest Asset",
                    (
                        f"{float(r['nearest_km']):.2f} km"
                        if pd.notna(r["nearest_km"])
                        else "N/A"
                    ),
                )

                render_analyst_summary(
                    anomaly_id=r["anomaly_id"],
                    priority=r["priority"],
                    confidence=float(r["confidence"]),
                    ndvi_delta=float(r["ndvi_delta"]),
                    cnn_score=float(r["cnn_score"]),
                    date_old=str(r["date_old"]),
                    date_new=str(r["date_new"]),
                    vessel_count=int(r["nearby_vessels"]),
                    aircraft_count=int(r["nearby_aircraft"]),
                    nearest_km=(
                        float(r["nearest_km"]) if pd.notna(r["nearest_km"]) else None
                    ),
                    last_activity=r["last_activity"],
                    radius_km=float(radius_km),
                )

                if int(r["nearby_vessels"]) > 0 or int(r["nearby_aircraft"]) > 0:
                    st.markdown("---")
                    na1, na2 = st.columns(2)

                    with na1:
                        if not nearby_v.empty:
                            st.markdown(
                                '<div class="section-header">Nearby Vessels</div>',
                                unsafe_allow_html=True,
                            )
                            st.dataframe(
                                nearby_v[
                                    [
                                        "vessel_name",
                                        "mmsi",
                                        "distance_km",
                                        "speed_knots",
                                        "heading",
                                        "received_at",
                                    ]
                                ].round({"distance_km": 2}),
                                use_container_width=True,
                                hide_index=True,
                            )

                    with na2:
                        if not nearby_a.empty:
                            st.markdown(
                                '<div class="section-header">Nearby Aircraft</div>',
                                unsafe_allow_html=True,
                            )
                            st.dataframe(
                                nearby_a[
                                    [
                                        "callsign",
                                        "icao24",
                                        "distance_km",
                                        "altitude_m",
                                        "velocity_ms",
                                        "received_at",
                                    ]
                                ].round({"distance_km": 2}),
                                use_container_width=True,
                                hide_index=True,
                            )

        with st.expander("📋 Full Correlated Events Table", expanded=False):
            st.dataframe(
                corr_df_tab[
                    [
                        "anomaly_id",
                        "priority",
                        "status",
                        "confidence",
                        "ndvi_delta",
                        "cnn_score",
                        "date_old",
                        "date_new",
                        "nearby_vessels",
                        "nearby_aircraft",
                        "nearest_km",
                        "last_activity",
                        "row_px",
                        "col_px",
                        "patch_size",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )


if __name__ == "__main__":
    main()
