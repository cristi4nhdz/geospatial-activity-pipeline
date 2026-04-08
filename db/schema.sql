-- db/schema.sql
-- Geospatial Activity Pipeline

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- AOI
CREATE TABLE IF NOT EXISTS aoi (
    id SERIAL PRIMARY KEY,
    aoi_name TEXT NOT NULL UNIQUE,
    description TEXT,
    geom GEOMETRY (POLYGON, 4326) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aoi_geom ON aoi USING gist (geom);

-- Vessel Tracks (AIS)
CREATE TABLE IF NOT EXISTS vessel_tracks (
    id BIGSERIAL PRIMARY KEY,
    mmsi BIGINT NOT NULL,
    vessel_name TEXT,
    geom GEOMETRY (POINT, 4326) NOT NULL,
    speed_knots NUMERIC(6, 2),
    heading NUMERIC(6, 2),
    course NUMERIC(6, 2),
    nav_status INTEGER,
    source TEXT DEFAULT 'aisstream',
    received_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vessel_tracks_geom ON vessel_tracks USING gist (
    geom
);
CREATE INDEX IF NOT EXISTS idx_vessel_tracks_mmsi ON vessel_tracks (mmsi);
CREATE INDEX IF NOT EXISTS idx_vessel_tracks_time ON vessel_tracks (
    received_at DESC
);

-- Aircraft Tracks (ADS-B)
CREATE TABLE IF NOT EXISTS aircraft_tracks (
    id BIGSERIAL PRIMARY KEY,
    icao24 TEXT NOT NULL,
    callsign TEXT,
    origin_country TEXT,
    geom GEOMETRY (POINT, 4326) NOT NULL,
    altitude_m NUMERIC(10, 2),
    velocity_ms NUMERIC(8, 2),
    heading NUMERIC(6, 2),
    vertical_rate NUMERIC(8, 2),
    squawk TEXT,
    source TEXT DEFAULT 'opensky',
    received_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aircraft_tracks_geom ON aircraft_tracks USING gist (
    geom
);
CREATE INDEX IF NOT EXISTS idx_aircraft_tracks_icao24 ON aircraft_tracks (
    icao24
);
CREATE INDEX IF NOT EXISTS idx_aircraft_tracks_time ON aircraft_tracks (
    received_at DESC
);
