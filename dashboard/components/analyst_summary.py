# dashboard/components/analyst_summary.py
"""
Template-based analyst narrative generator.
"""

from __future__ import annotations
import html
import streamlit as st
from dashboard.components.correlation import priority_bg, priority_color


def _change_description(ndvi_delta: float, cnn_score: float) -> str:
    """
    Generate a prose description of the land surface change signal.

    Describes magnitude based on NDVI delta and classifier reliability based on CNN score.

    Args:
        ndvi_delta: NDVI difference between before and after scenes.
        cnn_score: CNN classifier confidence score.

    Returns:
        Formatted prose string.
    """
    if ndvi_delta >= 0.35:
        magnitude = "significant"
    elif ndvi_delta >= 0.20:
        magnitude = "moderate"
    else:
        magnitude = "low-magnitude"

    if cnn_score >= 0.95:
        reliability = "with high classifier reliability"
    elif cnn_score >= 0.80:
        reliability = "with moderate classifier reliability"
    else:
        reliability = "with lower classifier confidence"

    return (
        f"Multi-spectral analysis of Sentinel-2 imagery reveals a {magnitude} "
        f"land-surface change event (NDVI Δ {ndvi_delta:.3f}) {reliability} "
        f"(CNN score {cnn_score:.3f}). The spatial pattern of the disturbance "
        f"is consistent with non-natural land-surface modification and warrants "
        f"structured analyst review."
    )


def _collection_window(date_old: str, date_new: str) -> str:
    """
    Generate a prose description of the imagery collection window.

    Args:
        date_old: Earlier collection date string.
        date_new: Later collection date string.

    Returns:
        Formatted prose string.
    """
    return (
        f"Change was detected between two satellite collection dates: "
        f"{date_old} and {date_new}. "
        f"The modification was not present in the earlier collection and "
        f"was confirmed in the later acquisition."
    )


def _movement_context(
    vessel_count: int,
    aircraft_count: int,
    nearest_km: float | None,
    last_activity: str,
    radius_km: float,
) -> str:
    """
    Generate a prose description of nearby vessel and aircraft activity.

    Describes the volume of movement within the correlation radius and
    the nearest recorded asset distance. Returns a no-activity statement
    if no vessels or aircraft were found.

    Args:
        vessel_count: Number of vessels within the correlation radius.
        aircraft_count: Number of aircraft within the correlation radius.
        nearest_km: Distance to the nearest asset in km, or None.
        last_activity: Timestamp string of the most recent nearby activity.
        radius_km: Correlation radius in km.

    Returns:
        Formatted prose string.
    """
    if vessel_count == 0 and aircraft_count == 0:
        return (
            f"No vessel or aircraft activity was recorded within the "
            f"{radius_km:.0f}km correlation radius during the monitoring window. "
            f"The absence of corroborating movement data reduces but does not "
            f"eliminate the analytical significance of the detected change."
        )

    parts = []
    if vessel_count > 0:
        parts.append(
            f"{vessel_count} vessel{'s' if vessel_count > 1 else ''} "
            f"{'were' if vessel_count > 1 else 'was'} recorded within the "
            f"{radius_km:.0f}km correlation radius"
        )
    if aircraft_count > 0:
        parts.append(
            f"{aircraft_count} aircraft "
            f"{'were' if aircraft_count > 1 else 'was'} observed in the same area"
        )

    activity_str = " and ".join(parts)
    nearest_str = (
        f" The nearest recorded asset was approximately {nearest_km:.1f}km "
        f"from the anomaly centroid."
        if nearest_km is not None
        else ""
    )
    last_str = (
        f" Most recent related activity was recorded at {last_activity}."
        if last_activity and str(last_activity) != "None" and last_activity != "N/A"
        else ""
    )

    return (
        f"Movement data within the correlation window indicates that {activity_str} "
        f"during the period spanning the two imagery collection dates."
        f"{nearest_str}{last_str} "
        f"The spatial and temporal overlap of detected land-surface change and "
        f"nearby movement activity increases the analytical relevance of this event."
    )


def _interpretation(
    priority: str, vessel_count: int, aircraft_count: int, ndvi_delta: float
) -> str:
    """
    Generate an analytical interpretation of the combined change and movement signals.

    Classifies the change type by NDVI delta magnitude and adjusts the correlation
    statement based on the total volume of nearby movement activity.

    Args:
        priority: Assigned priority label (URGENT/HIGH/MEDIUM/LOW).
        vessel_count: Number of nearby vessels.
        aircraft_count: Number of nearby aircraft.
        ndvi_delta: NDVI difference between before and after scenes.

    Returns:
        Formatted prose string.
    """
    activity_total = vessel_count + aircraft_count

    if ndvi_delta >= 0.35:
        change_type = (
            "structured ground disturbance consistent with organised human activity "
            "such as temporary fortification, vehicle staging, field logistics, "
            "or construction of support infrastructure"
        )
    elif ndvi_delta >= 0.20:
        change_type = (
            "moderate surface modification that may reflect organised human activity "
            "or significant vegetation disturbance with operational relevance"
        )
    else:
        change_type = (
            "low-magnitude surface change that may reflect minor human activity "
            "or environmental variation; further collection is recommended"
        )

    if activity_total >= 3:
        correlation_str = (
            "The volume of nearby movement activity further elevates the "
            "event's analytical priority. This combination of signals warrants "
            "immediate analyst attention and potential tasking for follow-on collection."
        )
    elif activity_total >= 1:
        correlation_str = (
            "The presence of nearby movement activity represents a corroborating "
            "signal. This event warrants analyst review and should be considered "
            "for follow-on collection tasking."
        )
    else:
        correlation_str = (
            "In the absence of corroborating movement data, the detected change "
            "should be assessed on its own merits. Continued monitoring of the "
            "area is recommended."
        )

    return f"The overall assessment indicates {change_type}. {correlation_str}"


def _priority_assessment(priority: str, confidence: float) -> str:
    """
    Generate a priority justification statement for the anomaly event.

    Args:
        priority: Assigned priority label (URGENT/HIGH/MEDIUM/LOW).
        confidence: Combined confidence score.

    Returns:
        Formatted prose string.
    """
    if priority == "URGENT":
        return (
            f"This event has been assigned URGENT priority based on a combined "
            f"confidence score of {confidence:.3f} and the presence of corroborating "
            f"movement activity within the correlation radius."
        )
    if priority == "HIGH":
        return (
            f"This event has been assigned HIGH priority. The confidence score of "
            f"{confidence:.3f} indicates a reliable detection, and nearby movement "
            f"activity increases its analytical significance."
        )
    if priority == "MEDIUM":
        return (
            f"This event has been assigned MEDIUM priority. The confidence score of "
            f"{confidence:.3f} warrants monitoring but does not yet meet the threshold "
            f"for immediate escalation."
        )
    return (
        f"This event has been assigned LOW priority. The confidence score of "
        f"{confidence:.3f} is below the high-confidence threshold. Further collection "
        f"is recommended before escalation."
    )


def generate_analyst_summary(
    anomaly_id: str,
    priority: str,
    confidence: float,
    ndvi_delta: float,
    cnn_score: float,
    date_old: str,
    date_new: str,
    vessel_count: int,
    aircraft_count: int,
    nearest_km: float | None,
    last_activity,
    radius_km: float = 20.0,
) -> str:
    """
    Generate a full structured intelligence assessment as a plain-text string.

    Assembles all narrative sections into a formatted report with header,
    change description, collection window, movement correlation, interpretation,
    and priority assessment. Includes a disclaimer footer.

    Args:
        anomaly_id: Stable anomaly identifier string.
        priority: Assigned priority label (URGENT/HIGH/MEDIUM/LOW).
        confidence: Combined confidence score.
        ndvi_delta: NDVI difference between before and after scenes.
        cnn_score: CNN classifier confidence score.
        date_old: Earlier collection date string.
        date_new: Later collection date string.
        vessel_count: Number of nearby vessels within the correlation radius.
        aircraft_count: Number of nearby aircraft within the correlation radius.
        nearest_km: Distance to the nearest asset in km, or None.
        last_activity: Timestamp of the most recent nearby asset activity.
        radius_km: Correlation radius used for movement lookup in km.

    Returns:
        Multi-line plain-text intelligence assessment string.
    """
    last_activity_str = (
        str(last_activity)[:19]
        if last_activity and str(last_activity) != "None"
        else "N/A"
    )

    return "\n".join(
        [
            f"INTELLIGENCE ASSESSMENT — {anomaly_id}",
            f"Collection Window: {date_old} → {date_new}  |  Priority: {priority}  |  Confidence: {confidence:.3f}",
            "",
            "LAND SURFACE CHANGE",
            _change_description(ndvi_delta, cnn_score),
            "",
            "COLLECTION WINDOW",
            _collection_window(date_old, date_new),
            "",
            "MOVEMENT CORRELATION",
            _movement_context(
                vessel_count, aircraft_count, nearest_km, last_activity_str, radius_km
            ),
            "",
            "INTERPRETATION",
            _interpretation(priority, vessel_count, aircraft_count, ndvi_delta),
            "",
            "PRIORITY ASSESSMENT",
            _priority_assessment(priority, confidence),
            "",
            "─" * 60,
            "This assessment is generated from automated analysis. All findings",
            "should be reviewed by a qualified analyst prior to dissemination.",
            "Do not treat automated outputs as confirmed intelligence.",
        ]
    )


def render_analyst_summary(
    anomaly_id: str,
    priority: str,
    confidence: float,
    ndvi_delta: float,
    cnn_score: float,
    date_old: str,
    date_new: str,
    vessel_count: int,
    aircraft_count: int,
    nearest_km: float | None,
    last_activity,
    radius_km: float = 20.0,
) -> None:
    """Render the analyst summary card in Streamlit."""
    color = priority_color(priority)
    bg = priority_bg(priority)

    summary = generate_analyst_summary(
        anomaly_id=anomaly_id,
        priority=priority,
        confidence=confidence,
        ndvi_delta=ndvi_delta,
        cnn_score=cnn_score,
        date_old=date_old,
        date_new=date_new,
        vessel_count=vessel_count,
        aircraft_count=aircraft_count,
        nearest_km=nearest_km,
        last_activity=last_activity,
        radius_km=radius_km,
    )

    st.markdown(
        f"""
        <div style="
            background:{bg};
            border:1px solid {color}44;
            border-left:3px solid {color};
            border-radius:10px;
            padding:1.2rem 1.4rem;
            margin-top:1rem;
        ">
            <div style="
                font-size:0.72rem;
                color:{color};
                text-transform:uppercase;
                letter-spacing:0.1em;
                font-weight:600;
                margin-bottom:0.8rem;
            ">Analyst Assessment — {html.escape(priority)}</div>
            <pre style="
                font-family:'Inter','Segoe UI',sans-serif;
                font-size:0.78rem;
                color:#d1d5db;
                white-space:pre-wrap;
                word-break:break-word;
                line-height:1.7;
                margin:0;
            ">{html.escape(summary)}</pre>
        </div>
        """,
        unsafe_allow_html=True,
    )
