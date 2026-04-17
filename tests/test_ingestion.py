# tests/test_ingestion.py
"""
Tests for AIS and ADS-B ingestion normalization functions.
"""
from unittest.mock import MagicMock, patch

from ingestion.ais_producer import normalize_vessel
from ingestion.adsb_producer import normalize_aircraft


class TestNormalizeVessel:
    """Tests for AIS vessel normalization."""

    def test_class_a_position_report(self, raw_ais_position_report):
        """Class A vessel normalizes correctly."""
        result = normalize_vessel(raw_ais_position_report)

        assert result is not None
        assert result["mmsi"] == 563056100
        assert result["vessel_name"] == "PEARL MELODY"
        assert result["latitude"] == 1.23738
        assert result["longitude"] == 103.75036
        assert result["speed_knots"] == 0.0
        assert result["heading"] == 111
        assert result["nav_status"] == 5
        assert result["source"] == "aisstream"

    def test_class_b_position_report(self, raw_ais_class_b):
        """Class B vessel normalizes correctly."""
        result = normalize_vessel(raw_ais_class_b)

        assert result is not None
        assert result["mmsi"] == 338123456
        assert result["vessel_name"] == "SMALL VESSEL"
        assert result["speed_knots"] == 4.5
        assert result["nav_status"] is None

    def test_no_position_report_returns_none(self, raw_ais_no_position):
        """Message with no position report returns None."""
        result = normalize_vessel(raw_ais_no_position)
        assert result is None

    def test_vessel_name_stripped(self, raw_ais_position_report):
        """Vessel name whitespace is stripped."""
        raw_ais_position_report["MetaData"]["ShipName"] = "  PEARL MELODY  "
        result = normalize_vessel(raw_ais_position_report)
        assert result["vessel_name"] == "PEARL MELODY"

    def test_timestamp_fallback(self, raw_ais_position_report):
        """Falls back to current UTC time if time_utc is missing."""
        raw_ais_position_report["MetaData"].pop("time_utc")
        result = normalize_vessel(raw_ais_position_report)
        assert result is not None
        assert result["timestamp"] is not None

    def test_result_has_required_keys(self, raw_ais_position_report):
        """Normalized vessel has all required keys."""
        result = normalize_vessel(raw_ais_position_report)
        required = {
            "mmsi",
            "vessel_name",
            "latitude",
            "longitude",
            "speed_knots",
            "heading",
            "course",
            "nav_status",
            "timestamp",
            "source",
        }
        assert required.issubset(result.keys())


class TestNormalizeAircraft:
    """Tests for ADS-B aircraft normalization."""

    def test_airborne_aircraft_normalizes(self, raw_aircraft):
        """Airborne aircraft normalizes correctly."""
        result = normalize_aircraft(raw_aircraft)

        assert result is not None
        assert result["icao24"] == "8a02ca"
        assert result["callsign"] == "AWQ504"
        assert result["latitude"] == 1.1998
        assert result["longitude"] == 103.915
        assert result["altitude_m"] == 777.24
        assert result["source"] == "opensky"

    def test_on_ground_returns_none(self, raw_aircraft_on_ground):
        """Aircraft on ground is filtered out."""
        result = normalize_aircraft(raw_aircraft_on_ground)
        assert result is None

    def test_no_coordinates_returns_none(self, raw_aircraft_no_coords):
        """Aircraft with no coordinates is filtered out."""
        result = normalize_aircraft(raw_aircraft_no_coords)
        assert result is None

    def test_callsign_stripped(self, raw_aircraft):
        """Callsign whitespace is stripped."""
        result = normalize_aircraft(raw_aircraft)
        assert result["callsign"] == "AWQ504"

    def test_result_has_required_keys(self, raw_aircraft):
        """Normalized aircraft has all required keys."""
        result = normalize_aircraft(raw_aircraft)
        required = {
            "icao24",
            "callsign",
            "origin_country",
            "latitude",
            "longitude",
            "altitude_m",
            "velocity_ms",
            "heading",
            "vertical_rate",
            "on_ground",
            "squawk",
            "timestamp",
            "source",
        }
        assert required.issubset(result.keys())

    def test_timestamp_is_set(self, raw_aircraft):
        """Timestamp is set on normalized aircraft."""
        result = normalize_aircraft(raw_aircraft)
        assert result["timestamp"] is not None


class TestFetchAircraft:
    """Tests for OpenSky fetch function."""

    def test_fetch_aircraft_returns_list(self):
        """fetch_aircraft returns list of dicts on success."""
        with patch("ingestion.adsb_producer.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "states": [
                    [
                        "8a02ca",
                        "AWQ504  ",
                        "Indonesia",
                        1712600000,
                        1712600000,
                        103.915,
                        1.1998,
                        777.24,
                        False,
                        93.83,
                        202.91,
                        8.13,
                        None,
                        800.0,
                        None,
                        False,
                        0,
                    ]
                ]
            }
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            from ingestion.adsb_producer import fetch_aircraft

            result = fetch_aircraft()
            assert len(result) == 1
            assert result[0]["icao24"] == "8a02ca"

    def test_fetch_aircraft_returns_empty_on_error(self):
        """fetch_aircraft returns empty list on request error."""
        with patch("ingestion.adsb_producer.requests.get") as mock_get:
            import requests

            mock_get.side_effect = requests.RequestException("timeout")

            from ingestion.adsb_producer import fetch_aircraft

            result = fetch_aircraft()
            assert result == []

    def test_fetch_aircraft_handles_empty_states(self):
        """fetch_aircraft handles response with no states."""
        with patch("ingestion.adsb_producer.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {"states": None}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            from ingestion.adsb_producer import fetch_aircraft

            result = fetch_aircraft()
            assert result == []


class TestBuildProducer:
    """Tests for Kafka producer builder."""

    def test_build_producer_creates_kafka_producer(self):
        """build_producer creates a KafkaProducer instance."""
        with patch("ingestion.adsb_producer.KafkaProducer") as mock_kafka:
            from ingestion.adsb_producer import build_producer

            build_producer()
            assert mock_kafka.called

    def test_ais_build_producer_creates_kafka_producer(self):
        """AIS build_producer creates a KafkaProducer instance."""
        with patch("ingestion.ais_producer.KafkaProducer") as mock_kafka:
            from ingestion.ais_producer import build_producer

            build_producer()
            assert mock_kafka.called


class TestAISBuildProducer:
    """Tests for AIS producer builder."""

    def test_build_producer_sets_serializer(self):
        """build_producer configures JSON value serializer."""
        with patch("ingestion.ais_producer.KafkaProducer") as mock_kafka:
            from ingestion.ais_producer import build_producer

            build_producer()
            call_kwargs = mock_kafka.call_args[1]
            assert "value_serializer" in call_kwargs

    def test_normalize_vessel_extended_class_b(self):
        """Extended Class B position report normalizes correctly."""
        from ingestion.ais_producer import normalize_vessel

        raw = {
            "MetaData": {
                "MMSI": 338999999,
                "ShipName": "EXTENDED B",
                "latitude": 1.25,
                "longitude": 103.80,
                "time_utc": "2026-04-08T18:25:04+00:00",
            },
            "Message": {
                "ExtendedClassBPositionReport": {
                    "Sog": 2.5,
                    "TrueHeading": 45,
                    "Cog": 44.0,
                }
            },
        }

        result = normalize_vessel(raw)
        assert result is not None
        assert result["mmsi"] == 338999999
        assert result["speed_knots"] == 2.5


class TestADSBProducerMain:
    """Tests for ADS-B producer main loop."""

    def test_main_exits_on_keyboard_interrupt(self):
        """main exits cleanly on KeyboardInterrupt."""
        with patch("ingestion.adsb_producer.build_producer") as mock_producer, patch(
            "ingestion.adsb_producer.fetch_aircraft"
        ) as mock_fetch, patch("ingestion.adsb_producer.time.sleep") as mock_sleep:

            mock_fetch.side_effect = [[], KeyboardInterrupt]
            mock_producer.return_value = MagicMock()

            from ingestion.adsb_producer import main

            main()

            mock_producer.return_value.flush.assert_called()
            mock_producer.return_value.close.assert_called()

    def test_main_publishes_aircraft(self):
        """main publishes normalized aircraft to Kafka."""
        aircraft = {
            "icao24": "8a02ca",
            "callsign": "AWQ504",
            "origin_country": "Indonesia",
            "latitude": 1.1998,
            "longitude": 103.915,
            "altitude_m": 777.24,
            "velocity_ms": 93.83,
            "heading": 202.91,
            "vertical_rate": 8.13,
            "on_ground": False,
            "squawk": None,
            "timestamp": "2026-04-08T18:25:04+00:00",
            "source": "opensky",
        }

        call_count = {"count": 0}

        def mock_fetch():
            call_count["count"] += 1
            if call_count["count"] > 1:
                raise KeyboardInterrupt
            return [aircraft]

        with patch("ingestion.adsb_producer.build_producer") as mock_producer, patch(
            "ingestion.adsb_producer.fetch_aircraft", side_effect=mock_fetch
        ), patch(
            "ingestion.adsb_producer.normalize_aircraft", return_value=aircraft
        ), patch(
            "ingestion.adsb_producer.time.sleep"
        ):

            mock_prod = MagicMock()
            mock_producer.return_value = mock_prod

            from ingestion.adsb_producer import main

            main()

            mock_prod.send.assert_called()
