"""Unit tests for FlightTelemetryEvent schema."""

import pytest
from dataclasses import fields as dc_fields

from producers.schema import FlightTelemetryEvent, DLQEvent, SCHEMA_VERSION, KAFKA_TOPIC, DLQ_TOPIC


def test_schema_version_constant():
    assert SCHEMA_VERSION == 1


def test_kafka_topic_constant():
    assert KAFKA_TOPIC == "flight-telemetry-v1"


def test_dlq_topic_constant():
    assert DLQ_TOPIC == "flight-telemetry-dlq"


def test_required_fields_present():
    required = {"schema_version", "event_time_utc", "source", "icao24", "ingestion_time_utc"}
    field_names = {f.name for f in dc_fields(FlightTelemetryEvent)}
    assert required <= field_names


def test_to_kafka_dict_has_all_fields(sample_flight_event):
    d = sample_flight_event.to_kafka_dict()
    assert d["schema_version"] == SCHEMA_VERSION
    assert d["icao24"] == "a1b2c3"
    assert d["source"] == "opensky"
    assert d["callsign"] == "UAL123"
    assert d["lat"] == 41.8781
    assert d["lon"] == -87.6298
    assert d["altitude_m"] == 10600.0
    assert d["velocity_mps"] == 245.3
    assert d["heading_deg"] == 90.0
    assert d["vertical_rate_mps"] == 1.5
    assert d["on_ground"] is False
    assert d["squawk"] == "1234"
    assert d["spi"] is False
    assert d["position_source"] == 0
    assert d["bbox_id"] == "24.0_50.0_-125.0_-66.0"


def test_to_kafka_dict_is_json_serializable(sample_flight_event):
    import json
    payload = sample_flight_event.to_kafka_dict()
    # Should not raise
    serialized = json.dumps(payload)
    assert isinstance(serialized, str)


def test_nullable_fields_default_to_none():
    event = FlightTelemetryEvent(
        schema_version=1,
        event_time_utc="2023-01-01T00:00:00Z",
        source="simulator",
        icao24="abc123",
        ingestion_time_utc="2023-01-01T00:00:00Z",
    )
    assert event.callsign is None
    assert event.lat is None
    assert event.lon is None
    assert event.altitude_m is None
    assert event.on_ground is None


def test_dlq_event_to_kafka_dict():
    dlq = DLQEvent(
        error_time_utc="2023-01-01T00:00:00Z",
        error_type="missing_required_field",
        raw_icao24=None,
        raw_payload={"state_len": 17},
        message="icao24 is null",
    )
    d = dlq.to_kafka_dict()
    assert d["error_type"] == "missing_required_field"
    assert d["raw_icao24"] is None
    assert d["raw_payload"]["state_len"] == 17
