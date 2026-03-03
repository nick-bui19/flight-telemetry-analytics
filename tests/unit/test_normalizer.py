"""Unit tests for normalizer.py rules."""

import pytest

from producers.normalizer import (
    _normalize_callsign,
    _normalize_lat,
    _normalize_lon,
    _normalize_altitude,
    _epoch_to_utc_iso,
)


# ── callsign ──────────────────────────────────────────────────────────────────

def test_callsign_strips_whitespace():
    assert _normalize_callsign("  UAL123  ") == "UAL123"


def test_callsign_empty_becomes_none():
    assert _normalize_callsign("") is None


def test_callsign_whitespace_only_becomes_none():
    assert _normalize_callsign("   ") is None


def test_callsign_none_stays_none():
    assert _normalize_callsign(None) is None


def test_callsign_no_extra_whitespace():
    assert _normalize_callsign("CSN302") == "CSN302"


# ── lat ───────────────────────────────────────────────────────────────────────

def test_lat_valid():
    assert _normalize_lat(41.8781) == pytest.approx(41.8781)


def test_lat_boundary_90():
    assert _normalize_lat(90.0) == 90.0


def test_lat_boundary_neg90():
    assert _normalize_lat(-90.0) == -90.0


def test_lat_out_of_range_high():
    assert _normalize_lat(91.0) is None


def test_lat_out_of_range_low():
    assert _normalize_lat(-91.0) is None


def test_lat_none():
    assert _normalize_lat(None) is None


# ── lon ───────────────────────────────────────────────────────────────────────

def test_lon_valid():
    assert _normalize_lon(-87.6298) == pytest.approx(-87.6298)


def test_lon_boundary_180():
    assert _normalize_lon(180.0) == 180.0


def test_lon_boundary_neg180():
    assert _normalize_lon(-180.0) == -180.0


def test_lon_out_of_range_high():
    assert _normalize_lon(181.0) is None


def test_lon_out_of_range_low():
    assert _normalize_lon(-181.0) is None


def test_lon_none():
    assert _normalize_lon(None) is None


# ── altitude ──────────────────────────────────────────────────────────────────

def test_altitude_uses_geo_when_present():
    assert _normalize_altitude(10600.0, 10500.0, False, "abc") == pytest.approx(10600.0)


def test_altitude_fallback_to_baro():
    assert _normalize_altitude(None, 10500.0, False, "abc") == pytest.approx(10500.0)


def test_altitude_negative_becomes_none():
    assert _normalize_altitude(-10.0, None, False, "abc") is None


def test_altitude_both_none():
    assert _normalize_altitude(None, None, False, "abc") is None


def test_altitude_on_ground_high_nulled(caplog):
    import logging
    with caplog.at_level(logging.WARNING):
        result = _normalize_altitude(5000.0, None, True, "abc123")
    assert result is None
    assert "abc123" in caplog.text


def test_altitude_on_ground_low_ok():
    # on_ground=True but alt <= 100m is fine
    assert _normalize_altitude(50.0, None, True, "abc") == pytest.approx(50.0)


# ── time conversion ───────────────────────────────────────────────────────────

def test_epoch_to_utc_iso_has_z_suffix():
    result = _epoch_to_utc_iso(1700000000.0)
    assert result.endswith("Z")


def test_epoch_to_utc_iso_none():
    assert _epoch_to_utc_iso(None) is None


def test_epoch_to_utc_iso_correct_value():
    # 1700000000 == 2023-11-14T22:13:20Z
    assert _epoch_to_utc_iso(1700000000.0) == "2023-11-14T22:13:20Z"
