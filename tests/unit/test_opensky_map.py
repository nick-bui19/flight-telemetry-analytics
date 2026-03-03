"""Golden-output unit tests for OpenSky state vector mapping."""

import pytest

from producers.normalizer import map_opensky_state
from producers.schema import FlightTelemetryEvent, DLQEvent

BBOX_ID = "24.0_50.0_-125.0_-66.0"


def test_full_array_all_fields(sample_opensky_state):
    result = map_opensky_state(sample_opensky_state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.icao24 == "a1b2c3"
    assert result.callsign == "UAL123"          # trailing spaces stripped
    assert result.origin_country == "United States"
    assert result.lat == pytest.approx(41.8781)
    assert result.lon == pytest.approx(-87.6298)
    assert result.altitude_m == pytest.approx(10600.0)  # geo wins over baro
    assert result.geo_altitude_m == pytest.approx(10600.0)
    assert result.velocity_mps == pytest.approx(245.3)
    assert result.heading_deg == pytest.approx(90.0)
    assert result.vertical_rate_mps == pytest.approx(1.5)
    assert result.on_ground is False
    assert result.squawk == "1234"
    assert result.spi is False
    assert result.position_source == 0
    assert result.bbox_id == BBOX_ID
    assert result.schema_version == 1
    assert result.source == "opensky"


def test_times_are_utc_iso8601(sample_opensky_state):
    result = map_opensky_state(sample_opensky_state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.event_time_utc.endswith("Z")
    assert result.ingestion_time_utc.endswith("Z")
    assert result.last_contact_utc.endswith("Z")


def test_null_lat_lon(sample_opensky_state_nulls):
    result = map_opensky_state(sample_opensky_state_nulls, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.lat is None
    assert result.lon is None


def test_lat_out_of_range_high(sample_opensky_state):
    state = list(sample_opensky_state)
    state[6] = 91.0
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.lat is None


def test_lat_out_of_range_low(sample_opensky_state):
    state = list(sample_opensky_state)
    state[6] = -91.0
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.lat is None


def test_lon_out_of_range(sample_opensky_state):
    state = list(sample_opensky_state)
    state[5] = 181.0
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.lon is None


def test_callsign_strip_whitespace(sample_opensky_state):
    state = list(sample_opensky_state)
    state[1] = "  CSN302  "
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.callsign == "CSN302"


def test_callsign_empty_becomes_null(sample_opensky_state):
    state = list(sample_opensky_state)
    state[1] = ""
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.callsign is None


def test_callsign_whitespace_only_becomes_null(sample_opensky_state):
    state = list(sample_opensky_state)
    state[1] = "    "
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.callsign is None


def test_negative_altitude_becomes_null(sample_opensky_state):
    state = list(sample_opensky_state)
    state[7] = -100.0   # baro
    state[13] = -50.0   # geo
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.altitude_m is None


def test_altitude_fallback_to_baro(sample_opensky_state):
    state = list(sample_opensky_state)
    state[13] = None    # no geo altitude
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.altitude_m == pytest.approx(10500.0)  # baro used


def test_on_ground_with_high_altitude(sample_opensky_state, caplog):
    import logging
    state = list(sample_opensky_state)
    state[8] = True    # on_ground
    state[13] = 5000.0 # geo_altitude high
    with caplog.at_level(logging.WARNING):
        result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.altitude_m is None
    assert "on_ground" in caplog.text.lower() or "altitude" in caplog.text.lower()


def test_missing_icao24_returns_dlq():
    state = [None] + [None] * 16
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, DLQEvent)
    assert result.error_type == "missing_required_field"


def test_empty_icao24_returns_dlq():
    state = [""] + ["x"] * 16
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, DLQEvent)


def test_icao24_lowercased(sample_opensky_state):
    state = list(sample_opensky_state)
    state[0] = "A1B2C3"
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.icao24 == "a1b2c3"


def test_event_time_falls_back_to_last_contact_when_time_posn_null(sample_opensky_state):
    state = list(sample_opensky_state)
    state[3] = None   # time_posn null
    # last_contact is 1700000005.0 → "2023-11-14T22:13:25Z"
    result = map_opensky_state(state, BBOX_ID)
    assert isinstance(result, FlightTelemetryEvent)
    assert result.event_time_utc == "2023-11-14T22:13:25Z"
