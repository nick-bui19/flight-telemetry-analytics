"""Integration tests: produce → consume/assert Kafka; upsert → assert Postgres."""

from __future__ import annotations

import json
import time

import psycopg2
import psycopg2.extras
import pytest

from producers.schema import DLQ_TOPIC, KAFKA_TOPIC, SCHEMA_VERSION, FlightTelemetryEvent


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_event(icao24: str, source: str = "simulator") -> FlightTelemetryEvent:
    now = "2023-11-14T22:13:20Z"
    return FlightTelemetryEvent(
        schema_version=SCHEMA_VERSION,
        event_time_utc=now,
        source=source,
        icao24=icao24,
        ingestion_time_utc=now,
        callsign=f"TST{icao24[-3:].upper()}",
        lat=41.0 + hash(icao24) % 10 * 0.1,
        lon=-87.0 - hash(icao24) % 10 * 0.1,
        altitude_m=10000.0,
        velocity_mps=240.0,
        heading_deg=90.0,
        vertical_rate_mps=0.5,
        on_ground=False,
        last_contact_utc=now,
        origin_country="United States",
        geo_altitude_m=10000.0,
        squawk="1234",
        spi=False,
        position_source=0,
        bbox_id="test_bbox",
    )


def _upsert_rows(conn, rows: list[tuple]) -> None:
    sql = """
        INSERT INTO realtime_flight_state
            (icao24, callsign, lat, lon, altitude_m, velocity_mps,
             heading_deg, vertical_rate_mps, on_ground,
             last_event_time_utc, updated_at_utc, source)
        VALUES %s
        ON CONFLICT (icao24) DO UPDATE SET
            callsign            = EXCLUDED.callsign,
            lat                 = EXCLUDED.lat,
            lon                 = EXCLUDED.lon,
            altitude_m          = EXCLUDED.altitude_m,
            velocity_mps        = EXCLUDED.velocity_mps,
            heading_deg         = EXCLUDED.heading_deg,
            vertical_rate_mps   = EXCLUDED.vertical_rate_mps,
            on_ground           = EXCLUDED.on_ground,
            last_event_time_utc = EXCLUDED.last_event_time_utc,
            updated_at_utc      = EXCLUDED.updated_at_utc,
            source              = EXCLUDED.source
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_produce_and_read_from_kafka(kafka_producer, kafka_consumer_factory, kafka_topics):
    """Produce 10 events; consume them and assert schema fields are present."""
    topic = kafka_topics["main"]
    events = [_make_event(f"abc{i:03d}") for i in range(10)]

    for ev in events:
        kafka_producer.send(topic, value=ev.to_kafka_dict(), key=ev.icao24)
    kafka_producer.flush()

    consumer = kafka_consumer_factory(topic)
    received = []
    for msg in consumer:
        received.append(json.loads(msg.value))
        if len(received) >= 10:
            break

    assert len(received) == 10
    for record in received:
        assert record["schema_version"] == SCHEMA_VERSION
        assert record["source"] == "simulator"
        assert "icao24" in record
        assert "lat" in record
        assert "lon" in record


def test_flight_state_upsert(pg_conn):
    """Upsert the same icao24 twice; assert 1 row with latest values."""
    icao24 = "test001"
    row_v1 = (icao24, "TST001", 41.0, -87.0, 9000.0, 200.0, 0.0, 0.0, False,
               "2023-11-14T22:13:20Z", "2023-11-14T22:13:20Z", "simulator")
    row_v2 = (icao24, "TST001", 41.5, -87.5, 11000.0, 260.0, 45.0, 2.0, False,
               "2023-11-14T22:14:20Z", "2023-11-14T22:14:20Z", "simulator")

    _upsert_rows(pg_conn, [row_v1])
    _upsert_rows(pg_conn, [row_v2])

    with pg_conn.cursor() as cur:
        cur.execute("SELECT altitude_m, velocity_mps FROM realtime_flight_state WHERE icao24 = %s", (icao24,))
        rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == pytest.approx(11000.0)   # latest altitude
    assert rows[0][1] == pytest.approx(260.0)      # latest velocity


def test_multiple_flights_inserted(pg_conn):
    """Insert 5 distinct aircraft; assert 5 rows exist."""
    icao24s = [f"multi{i:03d}" for i in range(5)]
    rows = [
        (icao24, f"FLT{i}", 40.0 + i, -80.0 - i, 10000.0, 230.0, 90.0, 0.0,
         False, "2023-11-14T22:13:20Z", "2023-11-14T22:13:20Z", "simulator")
        for i, icao24 in enumerate(icao24s)
    ]
    _upsert_rows(pg_conn, rows)

    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM realtime_flight_state WHERE icao24 = ANY(%s)",
            (icao24s,),
        )
        count = cur.fetchone()[0]

    assert count == 5


def test_dlq_receives_invalid_event(kafka_producer, kafka_consumer_factory, kafka_topics):
    """Produce a malformed event to DLQ; assert it appears."""
    from producers.schema import DLQEvent

    dlq_topic = kafka_topics["dlq"]
    dlq_event = DLQEvent(
        error_time_utc="2023-11-14T22:13:20Z",
        error_type="missing_required_field",
        raw_icao24=None,
        raw_payload={"state_len": 17},
        message="icao24 is null or empty",
    )
    payload = json.dumps(dlq_event.to_kafka_dict()).encode("utf-8")
    kafka_producer.send(dlq_topic, value=payload)
    kafka_producer.flush()

    consumer = kafka_consumer_factory(dlq_topic)
    received = []
    for msg in consumer:
        received.append(json.loads(msg.value))
        break  # just need 1

    assert len(received) >= 1
    assert received[0]["error_type"] == "missing_required_field"


def test_airspace_grid_insert(pg_conn):
    """Insert airspace grid rows and assert they can be queried."""
    rows = [
        ("2023-11-14T22:00:00Z", "2023-11-14T22:05:00Z", "test_41.9_-87.6", 3, 10500.0, 240.0),
    ]
    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO realtime_airspace_grid_5m
                (window_start, window_end, grid_cell, aircraft_count,
                 avg_altitude_m, avg_velocity_mps)
            VALUES %s
            ON CONFLICT (window_start, grid_cell) DO NOTHING
            """,
            rows,
        )
        cur.execute(
            "SELECT aircraft_count FROM realtime_airspace_grid_5m WHERE grid_cell = %s",
            ("test_41.9_-87.6",),
        )
        result = cur.fetchone()

    assert result is not None
    assert result[0] == 3


def test_quality_metrics_insert(pg_conn):
    """Insert quality metric rows and assert they can be queried."""
    rows = [
        ("2023-11-14T22:10:00Z", "2023-11-14T22:15:00Z", 100, 90, 10, 1.2),
    ]
    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO telemetry_quality_5m
                (window_start, window_end, total_messages,
                 messages_with_position, messages_missing_position,
                 avg_event_lag_seconds)
            VALUES %s
            ON CONFLICT (window_start) DO NOTHING
            """,
            rows,
        )
        cur.execute(
            "SELECT total_messages FROM telemetry_quality_5m WHERE window_start = %s",
            ("2023-11-14T22:10:00Z",),
        )
        result = cur.fetchone()

    assert result is not None
    assert result[0] == 100
