-- Flight Telemetry Database Schema
-- Run this manually: psql -U nickbui -c "CREATE DATABASE flight_data;" && psql -U nickbui -d flight_data -f sql/init_flight_db.sql

CREATE TABLE IF NOT EXISTS realtime_flight_state (
    icao24              TEXT PRIMARY KEY,
    callsign            TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    altitude_m          DOUBLE PRECISION,
    velocity_mps        DOUBLE PRECISION,
    heading_deg         DOUBLE PRECISION,
    vertical_rate_mps   DOUBLE PRECISION,
    on_ground           BOOLEAN,
    last_event_time_utc TIMESTAMPTZ,
    updated_at_utc      TIMESTAMPTZ,
    source              TEXT
);
CREATE INDEX IF NOT EXISTS idx_flight_state_updated ON realtime_flight_state (updated_at_utc DESC);

CREATE TABLE IF NOT EXISTS realtime_airspace_grid_5m (
    window_start     TIMESTAMPTZ,
    window_end       TIMESTAMPTZ,
    grid_cell        TEXT,
    aircraft_count   INTEGER,
    avg_altitude_m   DOUBLE PRECISION,
    avg_velocity_mps DOUBLE PRECISION,
    PRIMARY KEY (window_start, grid_cell)
);
CREATE INDEX IF NOT EXISTS idx_airspace_grid_window ON realtime_airspace_grid_5m (window_end DESC, grid_cell);

CREATE TABLE IF NOT EXISTS telemetry_quality_5m (
    window_start              TIMESTAMPTZ PRIMARY KEY,
    window_end                TIMESTAMPTZ,
    total_messages            INTEGER,
    messages_with_position    INTEGER,
    messages_missing_position INTEGER,
    avg_event_lag_seconds     DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_quality_window ON telemetry_quality_5m (window_end DESC);
