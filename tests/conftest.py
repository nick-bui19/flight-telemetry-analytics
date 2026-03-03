"""Shared pytest fixtures for all test levels."""

import pytest

from producers.schema import FlightTelemetryEvent, SCHEMA_VERSION


@pytest.fixture
def sample_opensky_state() -> list:
    """Canonical 17-element OpenSky state vector with all fields populated."""
    return [
        "a1b2c3",          # 0  icao24
        "UAL123  ",        # 1  callsign (with trailing spaces)
        "United States",   # 2  origin_country
        1700000000.0,      # 3  time_posn (epoch)
        1700000005.0,      # 4  last_contact (epoch)
        -87.6298,          # 5  lon (Chicago)
        41.8781,           # 6  lat
        10500.0,           # 7  baro_altitude_m
        False,             # 8  on_ground
        245.3,             # 9  velocity_mps
        90.0,              # 10 heading_deg
        1.5,               # 11 vertical_rate_mps
        None,              # 12 sensors (ignored)
        10600.0,           # 13 geo_altitude_m
        "1234",            # 14 squawk
        False,             # 15 spi
        0,                 # 16 position_source
    ]


@pytest.fixture
def sample_opensky_state_nulls() -> list:
    """17-element OpenSky state vector with strategic nulls at positions 3, 5, 6, 7, 13."""
    return [
        "deadbe",    # 0  icao24
        "CSN302",    # 1  callsign
        "China",     # 2  origin_country
        None,        # 3  time_posn — null
        1700000010.0,# 4  last_contact
        None,        # 5  lon — null
        None,        # 6  lat — null
        None,        # 7  baro_altitude_m — null
        True,        # 8  on_ground
        80.0,        # 9  velocity_mps
        270.0,       # 10 heading_deg
        0.0,         # 11 vertical_rate_mps
        None,        # 12 sensors
        None,        # 13 geo_altitude_m — null
        None,        # 14 squawk
        False,       # 15 spi
        0,           # 16 position_source
    ]


@pytest.fixture
def sample_flight_event() -> FlightTelemetryEvent:
    """Fully populated FlightTelemetryEvent instance."""
    return FlightTelemetryEvent(
        schema_version=SCHEMA_VERSION,
        event_time_utc="2023-11-14T22:13:20Z",
        source="opensky",
        icao24="a1b2c3",
        ingestion_time_utc="2023-11-14T22:13:21Z",
        callsign="UAL123",
        lat=41.8781,
        lon=-87.6298,
        altitude_m=10600.0,
        velocity_mps=245.3,
        heading_deg=90.0,
        vertical_rate_mps=1.5,
        on_ground=False,
        last_contact_utc="2023-11-14T22:13:25Z",
        origin_country="United States",
        geo_altitude_m=10600.0,
        squawk="1234",
        spi=False,
        position_source=0,
        bbox_id="24.0_50.0_-125.0_-66.0",
    )
