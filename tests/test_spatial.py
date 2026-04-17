# tests/test_spatial.py
"""
Tests for PostGIS spatial schema and queries.
Requires Docker stack running with PostGIS on localhost:5432.
"""
import pytest
import psycopg2
from datetime import datetime, timezone

from config.config_loader import config


@pytest.fixture
def db_conn():
    """
    Create a PostGIS test connection and clean up test records after each test.
    """
    db = config["postgis"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["db"],
        user=db["user"],
        password=db["password"],
    )
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def db_cursor(db_conn):
    """Return a cursor from the test connection."""
    cursor = db_conn.cursor()
    yield cursor
    cursor.close()


class TestVesselTracksSchema:
    """Tests for vessel_tracks table schema and spatial operations."""

    def test_vessel_tracks_table_exists(self, db_cursor):
        """vessel_tracks table exists in PostGIS."""
        db_cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'vessel_tracks'
            )
        """
        )
        assert db_cursor.fetchone()[0] is True

    def test_vessel_tracks_has_geometry_column(self, db_cursor):
        """vessel_tracks has a geometry column."""
        db_cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'vessel_tracks'
            AND column_name = 'geom'
        """
        )
        assert db_cursor.fetchone() is not None

    def test_vessel_tracks_gist_index_exists(self, db_cursor):
        """GIST spatial index exists on vessel_tracks.geom."""
        db_cursor.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'vessel_tracks'
            AND indexname = 'idx_vessel_tracks_geom'
        """
        )
        assert db_cursor.fetchone() is not None

    def test_insert_vessel_track(self, db_cursor, db_conn):
        """Can insert a vessel track with valid geometry."""
        db_cursor.execute(
            """
            INSERT INTO vessel_tracks (
                mmsi, vessel_name, geom, speed_knots,
                heading, course, nav_status, source, received_at
            ) VALUES (
                999999999, 'TEST VESSEL',
                ST_SetSRID(ST_MakePoint(103.75, 1.23), 4326),
                5.0, 180, 180.0, 0, 'test',
                %s
            )
        """,
            (datetime.now(timezone.utc),),
        )
        db_conn.commit()

        db_cursor.execute("SELECT mmsi FROM vessel_tracks WHERE mmsi = 999999999")
        result = db_cursor.fetchone()
        assert result is not None
        assert result[0] == 999999999

        db_cursor.execute("DELETE FROM vessel_tracks WHERE mmsi = 999999999")
        db_conn.commit()

    def test_geometry_is_valid_point(self, db_cursor, db_conn):
        """Inserted geometry is a valid WGS84 point."""
        db_cursor.execute(
            """
            INSERT INTO vessel_tracks (
                mmsi, vessel_name, geom, speed_knots,
                heading, course, nav_status, source, received_at
            ) VALUES (
                999999998, 'TEST VESSEL 2',
                ST_SetSRID(ST_MakePoint(103.80, 1.27), 4326),
                3.0, 90, 90.0, 0, 'test',
                %s
            )
        """,
            (datetime.now(timezone.utc),),
        )
        db_conn.commit()

        db_cursor.execute(
            """
            SELECT ST_IsValid(geom), ST_SRID(geom), ST_GeometryType(geom)
            FROM vessel_tracks WHERE mmsi = 999999998
        """
        )
        is_valid, srid, geom_type = db_cursor.fetchone()
        assert is_valid is True
        assert srid == 4326
        assert geom_type == "ST_Point"

        db_cursor.execute("DELETE FROM vessel_tracks WHERE mmsi = 999999998")
        db_conn.commit()

    def test_spatial_query_within_bbox(self, db_cursor, db_conn):
        """Spatial query returns vessels within bounding box."""
        db_cursor.execute(
            """
            INSERT INTO vessel_tracks (
                mmsi, vessel_name, geom, speed_knots,
                heading, course, nav_status, source, received_at
            ) VALUES (
                999999997, 'BBOX TEST',
                ST_SetSRID(ST_MakePoint(103.80, 1.25), 4326),
                0.0, 0, 0.0, 5, 'test',
                %s
            )
        """,
            (datetime.now(timezone.utc),),
        )
        db_conn.commit()

        db_cursor.execute(
            """
            SELECT mmsi FROM vessel_tracks
            WHERE ST_Within(
                geom,
                ST_MakeEnvelope(103.5, 1.0, 104.5, 1.6, 4326)
            )
            AND mmsi = 999999997
        """
        )
        result = db_cursor.fetchone()
        assert result is not None
        assert result[0] == 999999997

        db_cursor.execute("DELETE FROM vessel_tracks WHERE mmsi = 999999997")
        db_conn.commit()


class TestAircraftTracksSchema:
    """Tests for aircraft_tracks table schema and spatial operations."""

    def test_aircraft_tracks_table_exists(self, db_cursor):
        """aircraft_tracks table exists in PostGIS."""
        db_cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'aircraft_tracks'
            )
        """
        )
        assert db_cursor.fetchone()[0] is True

    def test_aircraft_tracks_gist_index_exists(self, db_cursor):
        """GIST spatial index exists on aircraft_tracks.geom."""
        db_cursor.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'aircraft_tracks'
            AND indexname = 'idx_aircraft_tracks_geom'
        """
        )
        assert db_cursor.fetchone() is not None

    def test_insert_aircraft_track(self, db_cursor, db_conn):
        """Can insert an aircraft track with valid geometry."""
        db_cursor.execute(
            """
            INSERT INTO aircraft_tracks (
                icao24, callsign, origin_country, geom,
                altitude_m, velocity_ms, heading, vertical_rate,
                squawk, source, received_at
            ) VALUES (
                'test01', 'TESTFLT', 'Singapore',
                ST_SetSRID(ST_MakePoint(103.98, 1.33), 4326),
                10000.0, 250.0, 310.0, 0.0,
                NULL, 'test', %s
            )
        """,
            (datetime.now(timezone.utc),),
        )
        db_conn.commit()

        db_cursor.execute("SELECT icao24 FROM aircraft_tracks WHERE icao24 = 'test01'")
        result = db_cursor.fetchone()
        assert result is not None
        assert result[0] == "test01"

        db_cursor.execute("DELETE FROM aircraft_tracks WHERE icao24 = 'test01'")
        db_conn.commit()


class TestAoiSchema:
    """Tests for aoi table schema."""

    def test_aoi_table_exists(self, db_cursor):
        """aoi table exists in PostGIS."""
        db_cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'aoi'
            )
        """
        )
        assert db_cursor.fetchone()[0] is True

    def test_aoi_gist_index_exists(self, db_cursor):
        """GIST spatial index exists on aoi.geom."""
        db_cursor.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'aoi'
            AND indexname = 'idx_aoi_geom'
        """
        )
        assert db_cursor.fetchone() is not None
