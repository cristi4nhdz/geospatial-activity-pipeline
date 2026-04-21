# imagery/anomaly_scorer.py
"""
Anomaly Scorer Module

Combines NDVI band-difference thresholding and PyTorch CNN scoring
to produce a final confidence score for each anomaly patch.
Outputs scored anomaly events ready for Snowflake loading.
"""
import os
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
from config.config_loader import config
from config.logging_config import setup_logging
from imagery.change_detection import (
    compute_ndvi,
    detect_anomalies,
    download_band,
    get_client,
    list_dates,
)
from imagery.patch_classifier import PatchCNN, load_model, score_patch

# Only set up logging if not running inside Airflow
if not os.environ.get("AIRFLOW_CTX_DAG_ID"):
    setup_logging("anomaly_scorer.log")

logger = logging.getLogger(__name__)

EVENTS_DIR = Path("/opt/airflow/imagery/events")


def score_anomalies(
    anomalies: list[dict],
    ndvi_delta: np.ndarray,
    model: PatchCNN,
    patch_size: int,
) -> list[dict]:
    """
    Score each anomaly patch using the PyTorch CNN and combine
    with the NDVI delta score into a final confidence score.

    Args:
        anomalies: List of anomaly dicts from change detection.
        ndvi_delta: Full NDVI delta array.
        model: Trained PatchCNN model.
        patch_size: Size of each patch in pixels.

    Returns:
        List of scored anomaly event dicts.
    """
    scored = []

    for anomaly in anomalies:
        row = anomaly["row"]
        col = anomaly["col"]

        patch = ndvi_delta[row : row + patch_size, col : col + patch_size]
        cnn_score = score_patch(model, patch)

        # normalize ndvi delta to 0-1 range for combining
        ndvi_score = min(anomaly["mean_delta"] / 1.0, 1.0)

        # combined confidence - equal weight
        confidence = round((ndvi_score + cnn_score) / 2, 4)

        scored.append(
            {
                "row": row,
                "col": col,
                "patch_size": patch_size,
                "mean_delta": anomaly["mean_delta"],
                "max_delta": anomaly["max_delta"],
                "ndvi_score": round(ndvi_score, 4),
                "cnn_score": round(cnn_score, 4),
                "confidence": confidence,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        logger.info(
            "Patch row: %s col: %s | ndvi_score: %s | cnn_score: %s | confidence: %s",
            row,
            col,
            round(ndvi_score, 4),
            round(cnn_score, 4),
            confidence,
        )

    scored.sort(key=lambda x: x["confidence"], reverse=True)
    return scored


def save_events(events: list[dict], date_old: str, date_new: str) -> Path:
    """
    Save scored anomaly events to a local JSON file.

    Args:
        events: List of scored anomaly event dicts.
        date_old: Older date string.
        date_new: Newer date string.

    Returns:
        Path to the saved events file.
    """
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"anomalies_{date_old}_vs_{date_new}.json"
    output_path = EVENTS_DIR / filename

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)

    logger.info("Saved %s anomaly events to %s", len(events), output_path)
    return output_path


def run(date_old: str, date_new: str) -> list[dict]:
    """
    Run the full anomaly scoring pipeline for two tile dates.

    Args:
        date_old: Older date string (YYYYMMDD).
        date_new: Newer date string (YYYYMMDD).

    Returns:
        List of scored anomaly event dicts.
    """
    client = get_client()
    bucket = config["minio"]["bucket"]
    cd_config = config["change_detection"]

    logger.info("Scoring anomalies: %s -> %s", date_old, date_new)

    b04_old = download_band(client, bucket, date_old, "B04")
    b08_old = download_band(client, bucket, date_old, "B08")
    b04_new = download_band(client, bucket, date_new, "B04")
    b08_new = download_band(client, bucket, date_new, "B08")

    ndvi_old = compute_ndvi(b04_old, b08_old)
    ndvi_new = compute_ndvi(b04_new, b08_new)
    ndvi_delta = np.abs(ndvi_new - ndvi_old)

    anomalies = detect_anomalies(
        ndvi_old,
        ndvi_new,
        patch_size=cd_config["patch_size"],
        threshold=cd_config["ndvi_threshold"],
    )

    if not anomalies:
        logger.info("No anomalies detected")
        return []

    model = load_model()
    scored = score_anomalies(anomalies, ndvi_delta, model, cd_config["patch_size"])
    save_events(scored, date_old, date_new)

    logger.info(
        "Scoring complete - %s events | top confidence: %s",
        len(scored),
        scored[0]["confidence"] if scored else 0,
    )

    return scored


def main() -> None:
    """
    Run anomaly scoring on the two most recent tile dates in MinIO.
    """
    client = get_client()
    bucket = config["minio"]["bucket"]
    dates = list_dates(client, bucket)

    if len(dates) < 2:
        logger.warning("Need at least 2 tile dates - found: %s", len(dates))
        return

    date_old, date_new = dates[-2], dates[-1]
    run(date_old, date_new)


if __name__ == "__main__":
    main()
