"""Unit tests for DLQ event structure and payload validation."""

import json
import pytest
from unittest.mock import MagicMock, call

from producers.schema import DLQEvent, DLQ_TOPIC
from producers.dlq import write_to_dlq


def _make_dlq_event(
    error_type="schema_validation",
    raw_icao24="a1b2c3",
    message="test error",
) -> DLQEvent:
    return DLQEvent(
        error_time_utc="2023-11-14T22:13:20Z",
        error_type=error_type,
        raw_icao24=raw_icao24,
        raw_payload={"state_len": 17},
        message=message,
    )


def test_dlq_event_has_required_fields():
    event = _make_dlq_event()
    d = event.to_kafka_dict()
    assert "error_time_utc" in d
    assert "error_type" in d
    assert "raw_icao24" in d
    assert "raw_payload" in d
    assert "message" in d


def test_dlq_error_type_preserved():
    event = _make_dlq_event(error_type="lat_out_of_range")
    assert event.to_kafka_dict()["error_type"] == "lat_out_of_range"


def test_dlq_null_icao24_allowed():
    event = _make_dlq_event(raw_icao24=None)
    d = event.to_kafka_dict()
    assert d["raw_icao24"] is None


def test_dlq_payload_is_json_serializable():
    event = _make_dlq_event()
    serialized = json.dumps(event.to_kafka_dict())
    assert isinstance(serialized, str)


def test_write_to_dlq_sends_to_correct_topic():
    mock_producer = MagicMock()
    event = _make_dlq_event()
    write_to_dlq(mock_producer, event, topic=DLQ_TOPIC)
    mock_producer.send.assert_called_once()
    args, kwargs = mock_producer.send.call_args
    assert args[0] == DLQ_TOPIC


def test_write_to_dlq_key_is_icao24():
    mock_producer = MagicMock()
    event = _make_dlq_event(raw_icao24="a1b2c3")
    write_to_dlq(mock_producer, event, topic=DLQ_TOPIC)
    _, kwargs = mock_producer.send.call_args
    assert kwargs["key"] == b"a1b2c3"


def test_write_to_dlq_null_icao24_key_is_none():
    mock_producer = MagicMock()
    event = _make_dlq_event(raw_icao24=None)
    write_to_dlq(mock_producer, event, topic=DLQ_TOPIC)
    _, kwargs = mock_producer.send.call_args
    assert kwargs["key"] is None


def test_write_to_dlq_value_is_valid_json():
    mock_producer = MagicMock()
    event = _make_dlq_event()
    write_to_dlq(mock_producer, event, topic=DLQ_TOPIC)
    _, kwargs = mock_producer.send.call_args
    payload_bytes = kwargs["value"]
    parsed = json.loads(payload_bytes.decode("utf-8"))
    assert parsed["error_type"] == "schema_validation"
