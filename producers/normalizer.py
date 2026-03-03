"""Normalization rules and OpenSky state-vector mapping."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Union

from producers.schema import DLQEvent, FlightTelemetryEvent

logger = logging.getLogger(__name__)

# OpenSky REST API state vector field positions (17-element list)
OPENSKY_FIELD_MAP = {
    "icao24":            0,
    "callsign":          1,
    "origin_country":    2,
    "time_posn":         3,   # epoch seconds, may be null
    "last_contact":      4,   # epoch seconds
    "lon":               5,
    "lat":               6,
    "baro_altitude_m":   7,
    "on_ground":         8,
    "velocity_mps":      9,
    "heading_deg":       10,
    "vertical_rate_mps": 11,
    # 12: sensors — ignored
    "geo_altitude_m":    13,
    "squawk":            14,
    "spi":               15,
    "position_source":   16,
}


def _epoch_to_utc_iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_callsign(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped if stripped else None


def _normalize_lat(lat: float | None) -> float | None:
    if lat is None:
        return None
    if not (-90 <= lat <= 90):
        return None
    return lat


def _normalize_lon(lon: float | None) -> float | None:
    if lon is None:
        return None
    if not (-180 <= lon <= 180):
        return None
    return lon


def _normalize_altitude(
    geo: float | None,
    baro: float | None,
    on_ground: bool | None,
    icao24: str,
) -> float | None:
    alt = geo if geo is not None else baro
    if alt is None:
        return None
    if alt < 0:
        return None
    # Sanity check: aircraft reports on_ground but altitude is suspiciously high
    if on_ground and alt > 100:
        logger.warning(
            "icao24=%s on_ground=True but altitude_m=%.1f (>100m) — nulling altitude",
            icao24,
            alt,
        )
        return None
    return alt


def map_opensky_state(
    state: list,
    bbox_id: str,
) -> Union[FlightTelemetryEvent, DLQEvent]:
    """Map a single OpenSky state vector (17-element list) to FlightTelemetryEvent.

    Returns a DLQEvent if required fields are missing or mapping fails.
    """
    now = _now_utc_iso()

    try:
        raw_icao24 = state[OPENSKY_FIELD_MAP["icao24"]]
        if not raw_icao24:
            return DLQEvent(
                error_time_utc=now,
                error_type="missing_required_field",
                raw_icao24=None,
                raw_payload={"state_len": len(state)},
                message="icao24 is null or empty",
            )
        icao24 = str(raw_icao24).lower()

        raw_lon              = state[OPENSKY_FIELD_MAP["lon"]]
        raw_lat              = state[OPENSKY_FIELD_MAP["lat"]]
        raw_baro_alt         = state[OPENSKY_FIELD_MAP["baro_altitude_m"]]
        raw_geo_alt          = state[OPENSKY_FIELD_MAP["geo_altitude_m"]]
        raw_on_ground        = state[OPENSKY_FIELD_MAP["on_ground"]]
        raw_velocity         = state[OPENSKY_FIELD_MAP["velocity_mps"]]
        raw_heading          = state[OPENSKY_FIELD_MAP["heading_deg"]]
        raw_vrate            = state[OPENSKY_FIELD_MAP["vertical_rate_mps"]]
        raw_callsign         = state[OPENSKY_FIELD_MAP["callsign"]]
        raw_origin_country   = state[OPENSKY_FIELD_MAP["origin_country"]]
        raw_squawk           = state[OPENSKY_FIELD_MAP["squawk"]]
        raw_spi              = state[OPENSKY_FIELD_MAP["spi"]]
        raw_position_source  = state[OPENSKY_FIELD_MAP["position_source"]]
        raw_time_posn        = state[OPENSKY_FIELD_MAP["time_posn"]]
        raw_last_contact     = state[OPENSKY_FIELD_MAP["last_contact"]]

        lat = _normalize_lat(raw_lat)
        lon = _normalize_lon(raw_lon)
        on_ground = bool(raw_on_ground) if raw_on_ground is not None else None
        geo_alt = float(raw_geo_alt) if raw_geo_alt is not None else None
        baro_alt = float(raw_baro_alt) if raw_baro_alt is not None else None
        altitude_m = _normalize_altitude(geo_alt, baro_alt, on_ground, icao24)

        event_time = _epoch_to_utc_iso(raw_time_posn) or _epoch_to_utc_iso(raw_last_contact) or now

        return FlightTelemetryEvent(
            schema_version=1,
            event_time_utc=event_time,
            source="opensky",
            icao24=icao24,
            ingestion_time_utc=now,
            callsign=_normalize_callsign(raw_callsign),
            lat=lat,
            lon=lon,
            altitude_m=altitude_m,
            velocity_mps=float(raw_velocity) if raw_velocity is not None else None,
            heading_deg=float(raw_heading) if raw_heading is not None else None,
            vertical_rate_mps=float(raw_vrate) if raw_vrate is not None else None,
            on_ground=on_ground,
            last_contact_utc=_epoch_to_utc_iso(raw_last_contact),
            origin_country=str(raw_origin_country) if raw_origin_country is not None else None,
            geo_altitude_m=geo_alt,
            squawk=str(raw_squawk) if raw_squawk is not None else None,
            spi=bool(raw_spi) if raw_spi is not None else None,
            position_source=int(raw_position_source) if raw_position_source is not None else None,
            bbox_id=bbox_id,
        )

    except (IndexError, TypeError, ValueError) as exc:
        raw_icao24_safe = state[0] if state else None
        return DLQEvent(
            error_time_utc=now,
            error_type="schema_validation",
            raw_icao24=str(raw_icao24_safe) if raw_icao24_safe is not None else None,
            raw_payload={"state_len": len(state) if state else 0},
            message=str(exc),
        )
