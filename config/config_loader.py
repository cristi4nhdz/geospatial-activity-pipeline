# config/config_loader.py
"""
Configuration Loader Module

This module loads application configuration from a YAML file
and exposes it as a dictionary for use across the application.
"""
from pathlib import Path
import yaml


def load_config() -> dict:
    """
    Load configuration from the settings.yaml file.

    Reads the YAML configuration file located in the same directory
    as this module and parses it into a Python dictionary.

    Returns:
        dict: Parsed configuration data.
    """
    config_path = Path(__file__).parent / "settings.yaml"
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


config: dict[str, object] = load_config()
