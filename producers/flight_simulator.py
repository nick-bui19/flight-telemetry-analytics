"""Deterministic flight telemetry simulator.

Produces FlightTelemetryEvent messages to Kafka using the same schema as
opensky_producer.py. Designed for local development and dashboard testing.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from producers.schema import KAFKA_TOPIC, SCHEMA_VERSION, FlightTelemetryEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME        = os.environ.get("KAFKA_TOPIC", KAFKA_TOPIC)
PRODUCER_CLIENT_ID      = os.environ.get("PRODUCER_CLIENT_ID", "flight-simulator")
POLL_INTERVAL_SECONDS   = float(os.environ.get("OPENSKY_POLL_SECONDS", "5"))

# CONUS bounding box defaults (same as .env.example)
BBOX_LAMIN = float(os.environ.get("OPENSKY_BBOX_LAMIN", "24.0"))
BBOX_LAMAX = float(os.environ.get("OPENSKY_BBOX_LAMAX", "50.0"))
BBOX_LOMIN = float(os.environ.get("OPENSKY_BBOX_LOMIN", "-125.0"))
BBOX_LOMAX = float(os.environ.get("OPENSKY_BBOX_LOMAX", "-66.0"))

# ── Deterministic fleet ───────────────────────────────────────────────────────
_AIRCRAFT = [
    {"icao24": "a00001", "callsign": "UAL123 ", "origin_country": "United States"},
    {"icao24": "a00002", "callsign": "DAL456 ", "origin_country": "United States"},
    {"icao24": "a00003", "callsign": "AAL789 ", "origin_country": "United States"},
    {"icao24": "a00004", "callsign": "SWA321 ", "origin_country": "United States"},
    {"icao24": "a00005", "callsign": "JBU654 ", "origin_country": "United States"},
    {"icao24": "a00006", "callsign": "SKW987 ", "origin_country": "United States"},
    {"icao24": "a00007", "callsign": "FFT111 ", "origin_country": "United States"},
    {"icao24": "a00008", "callsign": "NKS222 ", "origin_country": "United States"},
    {"icao24": "a00009", "callsign": "ASA333 ", "origin_country": "United States"},
    {"icao24": "a0000a", "callsign": "HAL444 ", "origin_country": "United States"},
]

# Mutable state so aircraft move between polls
_state: dict[str, dict] = {
    ac["icao24"]: {
        "lat":             random.uniform(BBOX_LAMIN, BBOX_LAMAX),
        "lon":             random.uniform(BBOX_LOMIN, BBOX_LOMAX),
        "altitude_m":      random.uniform(3000, 12000),
        "velocity_mps":    random.uniform(150, 280),
        "heading_deg":     random.uniform(0, 360),
        "vertical_rate_mps": random.uniform(-5, 5),
    }
    for ac in _AIRCRAFT
}


def _now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_event(ac: dict) -> FlightTelemetryEvent:
    s = _state[ac["icao24"]]

    # Nudge position and altitude slightly each call (simple random walk)
    s["lat"]              = max(BBOX_LAMIN, min(BBOX_LAMAX, s["lat"] + random.uniform(-0.05, 0.05)))
    s["lon"]              = max(BBOX_LOMIN, min(BBOX_LOMAX, s["lon"] + random.uniform(-0.05, 0.05)))
    s["altitude_m"]       = max(100, min(13000, s["altitude_m"] + random.uniform(-50, 50)))
    s["velocity_mps"]     = max(50, min(350, s["velocity_mps"] + random.uniform(-5, 5)))
    s["heading_deg"]      = (s["heading_deg"] + random.uniform(-2, 2)) % 360
    s["vertical_rate_mps"] = max(-20, min(20, s["vertical_rate_mps"] + random.uniform(-1, 1)))

    now = _now_utc_iso()
    bbox_id = f"{round(BBOX_LAMIN,1)}_{round(BBOX_LAMAX,1)}_{round(BBOX_LOMIN,1)}_{round(BBOX_LOMAX,1)}"

    return FlightTelemetryEvent(
        schema_version=SCHEMA_VERSION,
        event_time_utc=now,
        source="simulator",
        icao24=ac["icao24"],
        ingestion_time_utc=now,
        callsign=ac["callsign"].strip(),
        lat=round(s["lat"], 6),
        lon=round(s["lon"], 6),
        altitude_m=round(s["altitude_m"], 1),
        velocity_mps=round(s["velocity_mps"], 2),
        heading_deg=round(s["heading_deg"], 1),
        vertical_rate_mps=round(s["vertical_rate_mps"], 2),
        on_ground=False,
        last_contact_utc=now,
        origin_country=ac["origin_country"],
        geo_altitude_m=round(s["altitude_m"], 1),
        squawk=str(random.randint(1000, 7777)),
        spi=False,
        position_source=0,
        bbox_id=bbox_id,
    )


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=PRODUCER_CLIENT_ID,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def run() -> None:
    logger.info(
        "Starting flight simulator → topic=%s bootstrap=%s",
        KAFKA_TOPIC_NAME,
        KAFKA_BOOTSTRAP_SERVERS,
    )
    producer = _make_producer()

    while True:
        sent = 0
        for ac in _AIRCRAFT:
            event = _build_event(ac)
            payload = event.to_kafka_dict()
            try:
                producer.send(KAFKA_TOPIC_NAME, value=payload, key=event.icao24)
                sent += 1
            except KafkaError:
                logger.exception("Kafka send failed for icao24=%s", event.icao24)

        logger.info("Sent %d events", sent)
        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
