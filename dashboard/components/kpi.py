# dashboard/components/kpi.py
"""
KPI rendering component for the intelligence dashboard.
"""

import streamlit as st
from dashboard.components.correlation import priority_color, priority_bg


def render_kpi_cards(
    vessel_count: int,
    aircraft_count: int,
    anomaly_count: int,
    high_priority_count: int,
    last_updated: str,
) -> None:
    """
    Render top-level KPI metric cards across five columns.

    Args:
        vessel_count: Number of active vessels in the current filter window.
        aircraft_count: Number of active aircraft in the current filter window.
        anomaly_count: Total number of correlated anomaly events.
        high_priority_count: Number of URGENT or HIGH priority events.
        last_updated: UTC timestamp string of the last data refresh.
    """
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(
        _card("Active Vessels", str(vessel_count), "#3b82f6", "⛵"),
        unsafe_allow_html=True,
    )
    c2.markdown(
        _card("Active Aircraft", str(aircraft_count), "#f97316", "✈"),
        unsafe_allow_html=True,
    )
    c3.markdown(
        _card("Anomalies Detected", str(anomaly_count), "#10b981", "🛰"),
        unsafe_allow_html=True,
    )
    c4.markdown(
        _card("High Priority", str(high_priority_count), "#ef4444", "⚠"),
        unsafe_allow_html=True,
    )
    c5.markdown(
        _card("Last Updated", last_updated, "#6b7280", "🕐", small=True),
        unsafe_allow_html=True,
    )


def render_aoi_summary(
    area_name: str,
    monitoring_window: str,
    date_old: str,
    date_new: str,
    anomaly_count: int,
    avg_confidence: float,
) -> None:
    """
    Render the AOI summary card showing area name, monitoring window, and statistics.

    Args:
        area_name: Display name of the area of interest.
        monitoring_window: Human-readable time window string, e.g. "Last 24h".
        date_old: Earlier imagery collection date string.
        date_new: Later imagery collection date string.
        anomaly_count: Number of anomaly events detected in this window.
        avg_confidence: Mean confidence score across all anomaly events.
    """
    st.markdown(
        f"""
    <div style="
        background:#111827;
        border:1px solid #1f2937;
        border-left:3px solid #10b981;
        border-radius:10px;
        padding:1rem 1.4rem;
        margin-bottom:1rem;
    ">
        <div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;
            letter-spacing:0.1em;margin-bottom:0.5rem">
            Area of Interest
        </div>
        <div style="font-size:1.1rem;font-weight:700;color:#f9fafb;margin-bottom:0.6rem">
            {area_name}
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem">
            <div>
                <div style="font-size:0.7rem;color:#6b7280">Monitoring Window</div>
                <div style="font-size:0.85rem;color:#d1d5db">{monitoring_window}</div>
            </div>
            <div>
                <div style="font-size:0.7rem;color:#6b7280">Imagery Dates</div>
                <div style="font-size:0.85rem;color:#d1d5db">{date_old} → {date_new}</div>
            </div>
            <div>
                <div style="font-size:0.7rem;color:#6b7280">Anomalies Detected</div>
                <div style="font-size:0.85rem;color:#10b981;font-weight:600">{anomaly_count}</div>
            </div>
            <div>
                <div style="font-size:0.7rem;color:#6b7280">Avg Confidence</div>
                <div style="font-size:0.85rem;color:#8b5cf6;font-weight:600">{avg_confidence:.3f}</div>
            </div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_how_to_interpret() -> None:
    """Render dashboard interpretation guide."""
    st.markdown(
        """
    <div style="
        background:#0d1117;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:1rem 1.4rem;
        margin-bottom:1rem;
    ">
        <div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;
            letter-spacing:0.1em;margin-bottom:0.8rem">
            How to Interpret This Dashboard
        </div>
        <div style="display:flex;flex-direction:column;gap:0.6rem">
            <div style="display:flex;align-items:flex-start;gap:0.8rem">
                <div style="background:#1f2937;border-radius:6px;padding:0.3rem 0.6rem;
                    font-size:0.72rem;color:#3b82f6;font-weight:600;white-space:nowrap">
                    TRACKING
                </div>
                <div style="font-size:0.78rem;color:#9ca3af">
                    Shows what vessels and aircraft are moving now or were recently
                    observed within the AOI. Use this to establish operational context.
                </div>
            </div>
            <div style="display:flex;align-items:flex-start;gap:0.8rem">
                <div style="background:#1f2937;border-radius:6px;padding:0.3rem 0.6rem;
                    font-size:0.72rem;color:#10b981;font-weight:600;white-space:nowrap">
                    IMAGERY
                </div>
                <div style="font-size:0.78rem;color:#9ca3af">
                    Shows what changed on the ground between two satellite collection
                    dates. NDVI delta and CNN confidence indicate degree and reliability
                    of detected change.
                </div>
            </div>
            <div style="display:flex;align-items:flex-start;gap:0.8rem">
                <div style="background:#1f2937;border-radius:6px;padding:0.3rem 0.6rem;
                    font-size:0.72rem;color:#ef4444;font-weight:600;white-space:nowrap">
                    CORRELATED
                </div>
                <div style="font-size:0.78rem;color:#9ca3af">
                    Events where satellite-detected change and movement activity overlap
                    in space and time. These warrant prioritised analyst attention.
                </div>
            </div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_anomaly_event_card(row: dict, is_selected: bool = False) -> str:
    """
    Render a single anomaly event card as an HTML string for the event list.

    Applies priority colour coding and highlights the selected event.

    Args:
        row: Dict representation of a correlated event row.
        is_selected: Whether this card is the currently selected anomaly.

    Returns:
        HTML string for use with st.markdown(unsafe_allow_html=True).
    """

    color = priority_color(row["priority"])
    bg = priority_bg(row["priority"]) if is_selected else "#111827"
    border = color if is_selected else "#1f2937"

    return f"""
    <div style="
        background:{bg};
        border:1px solid {border};
        border-left:3px solid {color};
        border-radius:8px;
        padding:0.6rem 0.8rem;
        margin-bottom:0.4rem;
    ">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem">
            <span style="font-size:0.82rem;font-weight:700;color:#f9fafb">{row['anomaly_id']}</span>
            <span style="font-size:0.7rem;font-weight:700;color:{color};
                background:{color}22;border-radius:999px;padding:0.15rem 0.5rem">
                {row['priority']}
            </span>
        </div>
        <div style="font-size:0.72rem;color:#6b7280;line-height:1.8">
            Confidence: <span style="color:#d1d5db">{row['confidence']:.3f}</span>
            &nbsp;|&nbsp;
            NDVI Δ: <span style="color:#d1d5db">{row['ndvi_delta']:.3f}</span>
            &nbsp;|&nbsp;
            CNN: <span style="color:#d1d5db">{row['cnn_score']:.3f}</span>
        </div>
        <div style="font-size:0.72rem;color:#6b7280;margin-top:0.2rem">
            {row['date_old']} → {row['date_new']}
            &nbsp;|&nbsp;
            <span style="color:#3b82f6">{row['nearby_vessels']}V</span>
            &nbsp;
            <span style="color:#f97316">{row['nearby_aircraft']}A</span>
            &nbsp;|&nbsp;
            <span style="color:#9ca3af">{row.get('status', 'NEW')}</span>
        </div>
    </div>
    """


def _card(label: str, value: str, color: str, icon: str, small: bool = False) -> str:
    """
    Render a single KPI metric card as an HTML string.

    Args:
        label: Card label text shown below the icon.
        value: Metric value displayed prominently.
        color: Hex colour string for the value text.
        icon: Emoji or symbol shown at the top of the card.
        small: If True, renders the value in a smaller font size.

    Returns:
        HTML string for use with st.markdown(unsafe_allow_html=True).
    """
    value_size = "1.0rem" if small else "1.6rem"
    return f"""
    <div style="
        background:#111827;
        border:1px solid #1f2937;
        border-radius:10px;
        padding:1rem 1.2rem;
        text-align:center;
        height:100%;
    ">
        <div style="font-size:1.2rem;margin-bottom:0.3rem">{icon}</div>
        <div style="font-size:0.72rem;color:#6b7280;text-transform:uppercase;
            letter-spacing:0.08em;margin-bottom:0.3rem">{label}</div>
        <div style="font-size:{value_size};font-weight:700;color:{color};
            word-break:break-all">{value}</div>
    </div>
    """
