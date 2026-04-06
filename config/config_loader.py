# config/config_loader.py
"""Configuration loader for geospatial pipeline settings"""

from pathlib import Path
import yaml


def load_config() -> dict:
    """Loads configuration from the YAML file"""
    config_path = Path(__file__).parent / "settings.yaml"
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


config = load_config()
