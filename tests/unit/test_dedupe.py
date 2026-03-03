"""Unit tests for producer-side deduplication logic."""

import time
import pytest
from cachetools import TTLCache

from producers.schema import FlightTelemetryEvent, SCHEMA_VERSION


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_event(icao24="a1b2c3", last_contact="2023-11-14T22:13:25Z", lat=41.8, lon=-87.6,
                altitude_m=10600.0, velocity_mps=245.3) -> FlightTelemetryEvent:
    return FlightTelemetryEvent(
        schema_version=SCHEMA_VERSION,
        event_time_utc="2023-11-14T22:13:20Z",
        source="opensky",
        icao24=icao24,
        ingestion_time_utc="2023-11-14T22:13:21Z",
        lat=lat,
        lon=lon,
        altitude_m=altitude_m,
        velocity_mps=velocity_mps,
        last_contact_utc=last_contact,
    )


def _dedupe_key(e: FlightTelemetryEvent) -> tuple:
    return (e.icao24, e.last_contact_utc, e.lat, e.lon, e.altitude_m, e.velocity_mps)


class DedupeCache:
    """Isolated dedupe cache instance for testing (mirrors opensky_producer logic)."""

    def __init__(self, ttl=2):
        self._cache: TTLCache = TTLCache(maxsize=1000, ttl=ttl)
        self.skipped = 0

    def is_duplicate(self, event: FlightTelemetryEvent) -> bool:
        key = _dedupe_key(event)
        if key in self._cache:
            self.skipped += 1
            return True
        self._cache[key] = True
        return False


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_first_event_passes_through():
    cache = DedupeCache()
    event = _make_event()
    assert cache.is_duplicate(event) is False


def test_duplicate_event_skipped():
    cache = DedupeCache()
    event = _make_event()
    cache.is_duplicate(event)       # first: inserts
    assert cache.is_duplicate(event) is True   # second: duplicate


def test_counter_incremented_on_skip():
    cache = DedupeCache()
    event = _make_event()
    cache.is_duplicate(event)
    cache.is_duplicate(event)
    cache.is_duplicate(event)
    assert cache.skipped == 2


def test_cache_expires_after_ttl():
    cache = DedupeCache(ttl=1)
    event = _make_event()
    cache.is_duplicate(event)       # insert
    time.sleep(1.1)                 # let TTL expire
    assert cache.is_duplicate(event) is False   # re-inserted, not a duplicate


def test_different_last_contact_passes_through():
    cache = DedupeCache()
    e1 = _make_event(last_contact="2023-11-14T22:13:25Z")
    e2 = _make_event(last_contact="2023-11-14T22:13:35Z")  # 10s later
    cache.is_duplicate(e1)
    assert cache.is_duplicate(e2) is False


def test_different_icao24_passes_through():
    cache = DedupeCache()
    e1 = _make_event(icao24="aaa111")
    e2 = _make_event(icao24="bbb222")
    cache.is_duplicate(e1)
    assert cache.is_duplicate(e2) is False


def test_different_position_passes_through():
    cache = DedupeCache()
    e1 = _make_event(lat=41.8, lon=-87.6)
    e2 = _make_event(lat=42.0, lon=-87.6)  # moved
    cache.is_duplicate(e1)
    assert cache.is_duplicate(e2) is False
