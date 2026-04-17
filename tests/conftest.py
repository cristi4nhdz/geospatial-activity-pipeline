# tests/conftest.py
"""
Shared pytest fixtures for the geospatial pipeline test suite.
"""
import pytest


@pytest.fixture
def raw_ais_position_report():
    """Raw AIS Class A position report message from AISStream."""
    return {
        "MetaData": {
            "MMSI": 563056100,
            "ShipName": "PEARL MELODY",
            "latitude": 1.23738,
            "longitude": 103.75036,
            "time_utc": "2026-04-08 18:25:04.330991228 +0000 UTC",
        },
        "Message": {
            "PositionReport": {
                "Sog": 0.0,
                "TrueHeading": 111,
                "Cog": 152.1,
                "NavigationalStatus": 5,
            }
        },
    }


@pytest.fixture
def raw_ais_class_b():
    """Raw AIS Class B standard position report message."""
    return {
        "MetaData": {
            "MMSI": 338123456,
            "ShipName": "SMALL VESSEL",
            "latitude": 1.25000,
            "longitude": 103.80000,
            "time_utc": "2026-04-08 18:30:00.000000000 +0000 UTC",
        },
        "Message": {
            "StandardClassBPositionReport": {
                "Sog": 4.5,
                "TrueHeading": 270,
                "Cog": 268.0,
            }
        },
    }


@pytest.fixture
def raw_ais_no_position():
    """Raw AIS message with no position report — should be filtered."""
    return {
        "MetaData": {
            "MMSI": 123456789,
            "ShipName": "UNKNOWN",
            "latitude": 0.0,
            "longitude": 0.0,
        },
        "Message": {
            "StaticAndVoyageRelatedData": {
                "Name": "UNKNOWN",
            }
        },
    }


@pytest.fixture
def raw_aircraft():
    """Raw OpenSky aircraft state vector."""
    return {
        "icao24": "8a02ca",
        "callsign": "AWQ504  ",
        "origin_country": "Indonesia",
        "time_position": 1712600000,
        "last_contact": 1712600000,
        "longitude": 103.915,
        "latitude": 1.1998,
        "baro_altitude": 777.24,
        "on_ground": False,
        "velocity": 93.83,
        "true_track": 202.91,
        "vertical_rate": 8.13,
        "sensors": None,
        "geo_altitude": 800.0,
        "squawk": None,
        "spi": False,
        "position_source": 0,
    }


@pytest.fixture
def raw_aircraft_on_ground():
    """Raw OpenSky aircraft that is on the ground — should be filtered."""
    return {
        "icao24": "7c1234",
        "callsign": "QFA001  ",
        "origin_country": "Australia",
        "time_position": None,
        "last_contact": 1712600000,
        "longitude": 103.99,
        "latitude": 1.35,
        "baro_altitude": 0.0,
        "on_ground": True,
        "velocity": 0.0,
        "true_track": 0.0,
        "vertical_rate": 0.0,
        "sensors": None,
        "geo_altitude": 0.0,
        "squawk": None,
        "spi": False,
        "position_source": 0,
    }


@pytest.fixture
def raw_aircraft_no_coords():
    """Raw OpenSky aircraft with no coordinates — should be filtered."""
    return {
        "icao24": "abc123",
        "callsign": "TEST001 ",
        "origin_country": "Singapore",
        "time_position": None,
        "last_contact": 1712600000,
        "longitude": None,
        "latitude": None,
        "baro_altitude": None,
        "on_ground": False,
        "velocity": None,
        "true_track": None,
        "vertical_rate": None,
        "sensors": None,
        "geo_altitude": None,
        "squawk": None,
        "spi": False,
        "position_source": 0,
    }
