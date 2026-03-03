"""Insert flight-domain test rows into all three PostgreSQL tables.

Usage:
    python dashboard/populate_test_data.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname":   os.environ.get("POSTGRES_DB", "flight_data"),
    "user":     os.environ.get("POSTGRES_USER", "nickbui"),
    "password": os.environ.get("POSTGRES_PASSWORD", "dummy"),
}

NOW = datetime.now(tz=timezone.utc)


def _ts(delta_minutes: int = 0) -> str:
    return (NOW + timedelta(minutes=delta_minutes)).isoformat()


# ── Test rows ─────────────────────────────────────────────────────────────────

FLIGHT_STATE_ROWS = [
    ("a00001", "UAL123", 41.8781, -87.6298, 10600.0, 245.3, 90.0,  1.5,   False, _ts(-1), _ts(0), "simulator"),
    ("a00002", "DAL456", 33.7490, -84.3880,  9800.0, 230.0, 270.0, -0.5,  False, _ts(-2), _ts(0), "simulator"),
    ("a00003", "AAL789", 40.6413, -73.7781, 11200.0, 258.7, 45.0,   2.1,  False, _ts(-1), _ts(0), "simulator"),
    ("a00004", "SWA321", 29.9902, -95.3368,  7500.0, 210.0, 180.0,  0.0,  False, _ts(-3), _ts(0), "simulator"),
    ("a00005", "JBU654", 25.7959, -80.2870,  5000.0, 195.5, 355.0,  3.2,  False, _ts(-1), _ts(0), "simulator"),
    ("a00006", "SKW987", 47.4502,-122.3088,  8200.0, 220.0, 120.0, -1.8,  False, _ts(-2), _ts(0), "simulator"),
    ("a00007", "FFT111", 36.1245, -86.6782, 10100.0, 240.0, 300.0,  0.5,  False, _ts(-1), _ts(0), "simulator"),
    ("a00008", "NKS222", 32.8998, -97.0403,  9300.0, 235.0, 210.0, -2.0,  False, _ts(-2), _ts(0), "simulator"),
    ("a00009", "ASA333", 45.5898,-122.5951,  7800.0, 215.0, 85.0,   1.0,  False, _ts(-1), _ts(0), "simulator"),
    ("a0000a", "HAL444", 21.3245,-157.9251,  8900.0, 260.0, 260.0,  0.0,  False, _ts(-3), _ts(0), "simulator"),
]

AIRSPACE_GRID_ROWS = [
    (_ts(-10), _ts(-5), "41.9_-87.6", 3, 10400.0, 238.0),
    (_ts(-10), _ts(-5), "33.7_-84.4", 2,  9800.0, 230.0),
    (_ts(-10), _ts(-5), "40.6_-73.8", 4, 11000.0, 252.0),
    (_ts(-10), _ts(-5), "29.9_-95.3", 1,  7500.0, 210.0),
    (_ts(-5),  _ts(0),  "41.9_-87.6", 4, 10600.0, 245.0),
    (_ts(-5),  _ts(0),  "33.7_-84.4", 3,  9800.0, 232.0),
    (_ts(-5),  _ts(0),  "40.6_-73.8", 5, 11200.0, 258.0),
    (_ts(-5),  _ts(0),  "47.5_-122.3",2,  8200.0, 220.0),
]

QUALITY_ROWS = [
    (_ts(-25), _ts(-20), 120, 110, 10, 1.2),
    (_ts(-20), _ts(-15), 135, 128,  7, 1.0),
    (_ts(-15), _ts(-10), 142, 130, 12, 1.5),
    (_ts(-10), _ts(-5),  118, 112,  6, 0.9),
    (_ts(-5),  _ts(0),   155, 148,  7, 1.1),
]


def populate(conn) -> None:
    with conn:
        with conn.cursor() as cur:
            # realtime_flight_state
            psycopg2.extras.execute_values(
                cur,
                """
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
                """,
                FLIGHT_STATE_ROWS,
            )
            print(f"Inserted/updated {len(FLIGHT_STATE_ROWS)} flight state rows.")

            # realtime_airspace_grid_5m
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO realtime_airspace_grid_5m
                    (window_start, window_end, grid_cell, aircraft_count,
                     avg_altitude_m, avg_velocity_mps)
                VALUES %s
                ON CONFLICT (window_start, grid_cell) DO NOTHING
                """,
                AIRSPACE_GRID_ROWS,
            )
            print(f"Inserted {len(AIRSPACE_GRID_ROWS)} airspace grid rows.")

            # telemetry_quality_5m
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
                QUALITY_ROWS,
            )
            print(f"Inserted {len(QUALITY_ROWS)} quality metric rows.")


if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        populate(conn)
        print("Done.")
    finally:
        conn.close()
