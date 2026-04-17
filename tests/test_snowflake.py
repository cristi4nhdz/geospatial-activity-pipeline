# tests/test_snowflake.py
"""
Tests for Snowflake loader functions.
Uses unittest.mock to avoid live Snowflake connections.
"""
import json
from unittest.mock import MagicMock, patch


class TestRecordExists:
    """Tests for duplicate detection in anomaly loader."""

    def test_record_exists_returns_true(self):
        """Returns True when record already exists in Snowflake."""
        from snowflake_loader.anomaly_loader import record_exists

        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)

        result = record_exists(cursor, "20260409", "20260411", 0, 0)
        assert result is True

    def test_record_exists_returns_false(self):
        """Returns False when record does not exist."""
        from snowflake_loader.anomaly_loader import record_exists

        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)

        result = record_exists(cursor, "20260409", "20260411", 512, 512)
        assert result is False

    def test_record_exists_queries_correct_fields(self):
        """Queries use date_old, date_new, row_px, col_px."""
        from snowflake_loader.anomaly_loader import record_exists

        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)

        record_exists(cursor, "20260409", "20260411", 0, 512)
        call_args = cursor.execute.call_args[0]
        assert "date_old" in call_args[0]
        assert "date_new" in call_args[0]
        assert "row_px" in call_args[0]
        assert "col_px" in call_args[0]


class TestLoadEvents:
    """Tests for event file loading logic."""

    def test_load_events_inserts_new_records(self, tmp_path):
        """New records are inserted into Snowflake."""
        from snowflake_loader.anomaly_loader import load_events

        events = [
            {
                "row": 0,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.3,
                "max_delta": 0.5,
                "ndvi_score": 0.3,
                "cnn_score": 0.95,
                "confidence": 0.625,
                "detected_at": "2026-04-13T13:41:38+00:00",
            }
        ]

        event_file = tmp_path / "anomalies_20260409_vs_20260411.json"
        event_file.write_text(json.dumps(events), encoding="utf-8")

        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)

        count = load_events(event_file, cursor)
        assert count == 1
        assert cursor.execute.call_count == 2

    def test_load_events_skips_duplicates(self, tmp_path):
        """Duplicate records are skipped."""
        from snowflake_loader.anomaly_loader import load_events

        events = [
            {
                "row": 0,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.3,
                "max_delta": 0.5,
                "ndvi_score": 0.3,
                "cnn_score": 0.95,
                "confidence": 0.625,
                "detected_at": "2026-04-13T13:41:38+00:00",
            }
        ]

        event_file = tmp_path / "anomalies_20260409_vs_20260411.json"
        event_file.write_text(json.dumps(events), encoding="utf-8")

        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)

        count = load_events(event_file, cursor)
        assert count == 0

    def test_load_events_extracts_dates_from_filename(self, tmp_path):
        """Date old and new are correctly extracted from filename."""
        from snowflake_loader.anomaly_loader import load_events

        events = [
            {
                "row": 0,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.3,
                "max_delta": 0.5,
                "ndvi_score": 0.3,
                "cnn_score": 0.95,
                "confidence": 0.625,
                "detected_at": "2026-04-13T13:41:38+00:00",
            }
        ]

        event_file = tmp_path / "anomalies_20260409_vs_20260411.json"
        event_file.write_text(json.dumps(events), encoding="utf-8")

        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)

        load_events(event_file, cursor)

        insert_call = cursor.execute.call_args_list[-1]
        params = insert_call[0][1]
        assert params[0] == "20260409"
        assert params[1] == "20260411"

    def test_load_events_returns_correct_count(self, tmp_path):
        """Returns correct count of loaded events."""
        from snowflake_loader.anomaly_loader import load_events

        events = [
            {
                "row": i * 512,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.3,
                "max_delta": 0.5,
                "ndvi_score": 0.3,
                "cnn_score": 0.95,
                "confidence": 0.625,
                "detected_at": "2026-04-13T13:41:38+00:00",
            }
            for i in range(5)
        ]

        event_file = tmp_path / "anomalies_20260409_vs_20260411.json"
        event_file.write_text(json.dumps(events), encoding="utf-8")

        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)

        count = load_events(event_file, cursor)
        assert count == 5


class TestSnowflakeSetup:
    """Tests for Snowflake setup DDL."""

    def test_ddl_contains_required_columns(self):
        """ANOMALY_EVENTS_DDL contains all required columns."""
        from snowflake_loader.setup import ANOMALY_EVENTS_DDL

        required_columns = [
            "date_old",
            "date_new",
            "row_px",
            "col_px",
            "patch_size",
            "mean_delta",
            "max_delta",
            "ndvi_score",
            "cnn_score",
            "confidence",
            "detected_at",
            "loaded_at",
        ]
        for col in required_columns:
            assert col in ANOMALY_EVENTS_DDL

    def test_ddl_creates_if_not_exists(self):
        """DDL uses CREATE TABLE IF NOT EXISTS."""
        from snowflake_loader.setup import ANOMALY_EVENTS_DDL

        assert "IF NOT EXISTS" in ANOMALY_EVENTS_DDL

    def test_ddl_has_autoincrement_id(self):
        """DDL has AUTOINCREMENT primary key."""
        from snowflake_loader.setup import ANOMALY_EVENTS_DDL

        assert "AUTOINCREMENT" in ANOMALY_EVENTS_DDL


class TestSnowflakeConnection:
    """Tests for Snowflake connection function."""

    def test_get_connection_uses_config(self):
        """get_connection uses snowflake config values."""
        with patch(
            "snowflake_loader.anomaly_loader.snowflake.connector.connect"
        ) as mock_conn:
            from snowflake_loader.anomaly_loader import get_connection

            get_connection()
            assert mock_conn.called
            call_kwargs = mock_conn.call_args[1]
            assert "account" in call_kwargs
            assert "user" in call_kwargs
            assert "password" in call_kwargs

    def test_setup_get_connection_uses_config(self):
        """setup get_connection uses snowflake config values."""
        with patch("snowflake_loader.setup.snowflake.connector.connect") as mock_conn:
            from snowflake_loader.setup import get_connection

            get_connection()
            assert mock_conn.called


class TestSnowflakeSetupMain:
    """Tests for snowflake setup main function."""

    def test_setup_executes_ddl(self):
        """setup executes CREATE DATABASE and CREATE TABLE."""
        with patch("snowflake_loader.setup.snowflake.connector.connect") as mock_conn:
            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            from snowflake_loader.setup import setup

            setup()

            assert mock_cursor.execute.call_count >= 3

    def test_setup_closes_connection(self):
        """setup closes cursor and connection after execution."""
        with patch("snowflake_loader.setup.snowflake.connector.connect") as mock_conn:
            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor

            from snowflake_loader.setup import setup

            setup()

            mock_cursor.close.assert_called_once()
            mock_conn.return_value.close.assert_called_once()


class TestAnomalyLoaderMain:
    """Tests for anomaly_loader main function."""

    def test_main_warns_when_no_files(self, tmp_path):
        """main warns when no event files found."""
        with patch("snowflake_loader.anomaly_loader.EVENTS_DIR", tmp_path), patch(
            "snowflake_loader.anomaly_loader.get_connection"
        ) as mock_conn:

            from snowflake_loader.anomaly_loader import main

            main()
            assert not mock_conn.called

    def test_main_loads_files_when_present(self, tmp_path):
        """main loads event files when present."""
        import json

        events = [
            {
                "row": 0,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.3,
                "max_delta": 0.5,
                "ndvi_score": 0.3,
                "cnn_score": 0.95,
                "confidence": 0.625,
                "detected_at": "2026-04-13T13:41:38+00:00",
            }
        ]

        event_file = tmp_path / "anomalies_20260409_vs_20260411.json"
        event_file.write_text(json.dumps(events), encoding="utf-8")

        with patch("snowflake_loader.anomaly_loader.EVENTS_DIR", tmp_path), patch(
            "snowflake_loader.anomaly_loader.get_connection"
        ) as mock_conn:

            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (0,)
            mock_conn.return_value.cursor.return_value = mock_cursor

            from snowflake_loader.anomaly_loader import main

            main()
            assert mock_conn.called
