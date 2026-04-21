# config/logging_config.py
"""
Logging Configuration Module

This module is responsible for configuring application-wide logging,
including file and console handlers.
"""
import logging
import os


def setup_logging(log_file: str = "app.log") -> None:
    """
    Configure application logging.

    Sets up logging with a standard format and attaches both a file
    handler and a stream handler. Intended for use in local development
    and can be adapted for containerized environments.

    Args:
        log_file (str): Name of the log file to write logs to.

    Returns:
        None
    """
    log_dir = "/opt/airflow/logs"
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, log_file)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(),
        ],
        force=True,
    )
