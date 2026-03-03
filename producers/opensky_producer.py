"""Live OpenSky Network producer.

Polls the OpenSky REST API, maps state vectors to FlightTelemetryEvent,
deduplicates, and publishes to Kafka. Invalid events go to the DLQ topic.
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from typing import Optional

import requests
from cachetools import TTLCache
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from producers.dlq import write_to_dlq
from producers.normalizer import map_opensky_state
from producers.schema import (
    DLQ_TOPIC,
    KAFKA_TOPIC,
    SCHEMA_VERSION,
    DLQEvent,
    FlightTelemetryEvent,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config (read at module load; crash on missing required vars) ───────────────
OPENSKY_USERNAME        = os.environ.get("OPENSKY_USERNAME", "")
OPENSKY_PASSWORD        = os.environ.get("OPENSKY_PASSWORD", "")
POLL_SECONDS            = float(os.environ.get("OPENSKY_POLL_SECONDS", "10"))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME        = os.environ.get("KAFKA_TOPIC", KAFKA_TOPIC)
DLQ_TOPIC_NAME          = os.environ.get("KAFKA_DLQ_TOPIC", DLQ_TOPIC)
PRODUCER_CLIENT_ID      = os.environ.get("PRODUCER_CLIENT_ID", "opensky-producer")
ENABLE_DEDUPE           = os.environ.get("ENABLE_DEDUPE", "true").lower() == "true"
DEDUPE_TTL_SECONDS      = int(os.environ.get("DEDUPE_TTL_SECONDS", "120"))

_REQUIRED_BBOX_VARS = ["OPENSKY_BBOX_LAMIN", "OPENSKY_BBOX_LAMAX", "OPENSKY_BBOX_LOMIN", "OPENSKY_BBOX_LOMAX"]
_missing = [v for v in _REQUIRED_BBOX_VARS if not os.environ.get(v)]
if _missing:
    logger.critical("Missing required env vars: %s — cannot start", _missing)
    sys.exit(1)

BBOX_LAMIN = float(os.environ["OPENSKY_BBOX_LAMIN"])
BBOX_LAMAX = float(os.environ["OPENSKY_BBOX_LAMAX"])
BBOX_LOMIN = float(os.environ["OPENSKY_BBOX_LOMIN"])
BBOX_LOMAX = float(os.environ["OPENSKY_BBOX_LOMAX"])

BBOX_ID = f"{round(BBOX_LAMIN,1)}_{round(BBOX_LAMAX,1)}_{round(BBOX_LOMIN,1)}_{round(BBOX_LOMAX,1)}"

OPENSKY_API_URL = (
    "https://opensky-network.org/api/states/all"
    f"?lamin={BBOX_LAMIN}&lamax={BBOX_LAMAX}&lomin={BBOX_LOMIN}&lomax={BBOX_LOMAX}"
)

# ── Dedupe cache ───────────────────────────────────────────────────────────────
_dedupe_cache: TTLCache = TTLCache(maxsize=10_000, ttl=DEDUPE_TTL_SECONDS)
dedupe_skipped_count: int = 0


def _dedupe_key(e: FlightTelemetryEvent) -> tuple:
    return (e.icao24, e.last_contact_utc, e.lat, e.lon, e.altitude_m, e.velocity_mps)


def _is_duplicate(event: FlightTelemetryEvent) -> bool:
    global dedupe_skipped_count
    if not ENABLE_DEDUPE:
        return False
    key = _dedupe_key(event)
    if key in _dedupe_cache:
        dedupe_skipped_count += 1
        return True
    _dedupe_cache[key] = True
    return False


# ── Fatal vs transient error classification ───────────────────────────────────
_FATAL_HTTP_CODES = {401, 403}
_TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}
_BACKOFF_BASE = 5.0
_BACKOFF_MAX  = 300.0


def _backoff_with_jitter(attempt: int) -> float:
    delay = min(_BACKOFF_BASE * (2 ** attempt), _BACKOFF_MAX)
    return delay + random.uniform(0, delay * 0.1)


# ── Kafka setup ────────────────────────────────────────────────────────────────
def _make_producer() -> KafkaProducer:
    """Create Kafka producer; raise on 3 consecutive connection failures (fatal)."""
    for attempt in range(3):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id=PRODUCER_CLIENT_ID,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except NoBrokersAvailable as exc:
            logger.error("Kafka bootstrap attempt %d/3 failed: %s", attempt + 1, exc)
            if attempt < 2:
                time.sleep(3)
    logger.critical("Kafka bootstrap unreachable after 3 attempts — cannot start")
    sys.exit(1)


# ── Main polling loop ─────────────────────────────────────────────────────────
def run() -> None:
    global dedupe_skipped_count

    logger.info(
        "Starting OpenSky producer bbox=%s topic=%s bootstrap=%s dedupe=%s",
        BBOX_ID,
        KAFKA_TOPIC_NAME,
        KAFKA_BOOTSTRAP_SERVERS,
        ENABLE_DEDUPE,
    )

    producer = _make_producer()
    auth: Optional[tuple[str, str]] = (OPENSKY_USERNAME, OPENSKY_PASSWORD) if OPENSKY_USERNAME else None
    transient_attempt = 0

    while True:
        try:
            resp = requests.get(OPENSKY_API_URL, auth=auth, timeout=10)

            # ── Fatal HTTP errors ──────────────────────────────────────────────
            if resp.status_code in _FATAL_HTTP_CODES:
                logger.critical(
                    "Fatal HTTP %d from OpenSky — bad credentials? Stopping.",
                    resp.status_code,
                )
                sys.exit(1)

            # ── Transient HTTP errors ─────────────────────────────────────────
            if resp.status_code in _TRANSIENT_HTTP_CODES or not resp.ok:
                delay = _backoff_with_jitter(transient_attempt)
                logger.warning(
                    "Transient HTTP %d from OpenSky; backoff attempt=%d delay=%.1fs",
                    resp.status_code,
                    transient_attempt,
                    delay,
                )
                transient_attempt += 1
                time.sleep(delay)
                continue

            # ── Success ───────────────────────────────────────────────────────
            transient_attempt = 0
            data = resp.json()
            states = data.get("states") or []

            sent = 0
            dlq_sent = 0
            skipped_before = dedupe_skipped_count

            for state in states:
                result = map_opensky_state(state, BBOX_ID)

                if isinstance(result, DLQEvent):
                    write_to_dlq(producer, result, DLQ_TOPIC_NAME)
                    dlq_sent += 1
                    continue

                if _is_duplicate(result):
                    continue

                payload = result.to_kafka_dict()
                try:
                    producer.send(KAFKA_TOPIC_NAME, value=payload, key=result.icao24)
                    sent += 1
                except KafkaError:
                    logger.exception("Kafka send failed for icao24=%s", result.icao24)

            producer.flush()
            dedupe_this_cycle = dedupe_skipped_count - skipped_before
            logger.info(
                "Poll done: states=%d sent=%d dlq=%d dedupe_skipped=%d (total_skipped=%d)",
                len(states),
                sent,
                dlq_sent,
                dedupe_this_cycle,
                dedupe_skipped_count,
            )

        except (requests.ConnectionError, requests.Timeout) as exc:
            delay = _backoff_with_jitter(transient_attempt)
            logger.warning(
                "Transient network error: %s; backoff attempt=%d delay=%.1fs",
                exc,
                transient_attempt,
                delay,
            )
            transient_attempt += 1
            time.sleep(delay)
            continue

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run()
