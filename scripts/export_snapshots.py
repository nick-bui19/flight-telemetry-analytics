#!/usr/bin/env python3
"""Export Postgres tables to parquet snapshots for Streamlit Cloud demo.

Usage:
    python scripts/export_snapshots.py            # tries Postgres, falls back to hardcoded
    python scripts/export_snapshots.py --offline  # always uses hardcoded fallback
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).parent.parent / "dashboard" / "data"


def export_from_postgres() -> dict[str, pd.DataFrame]:
    """Returns dict of {table_name: DataFrame} or raises on connection failure."""
    import sqlalchemy

    url = os.environ.get("DB_URL") or (
        f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER', 'nickbui')}:"
        f"{os.environ.get('POSTGRES_PASSWORD', 'dummy')}@"
        f"{os.environ.get('POSTGRES_HOST', 'localhost')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ.get('POSTGRES_DB', 'flight_data')}"
    )
    engine = sqlalchemy.create_engine(url)
    with engine.connect() as conn:
        return {
            "realtime_flight_state":     pd.read_sql_table("realtime_flight_state", conn),
            "realtime_airspace_grid_5m": pd.read_sql_table("realtime_airspace_grid_5m", conn),
            "telemetry_quality_5m":      pd.read_sql_table("telemetry_quality_5m", conn),
        }


def hardcoded_fallback() -> dict[str, pd.DataFrame]:
    """Returns hardcoded snapshot data matching DB schema."""
    now = datetime.now(timezone.utc)

    def ts(delta_minutes: int = 0) -> datetime:
        return now + timedelta(minutes=delta_minutes)

    flight_state = pd.DataFrame(
        [
            ("a00001", "UAL123",  41.8781,  -87.6298, 10600.0, 245.3, 90.0,   1.5,  False, ts(-1), ts(0), "simulator"),
            ("a00002", "DAL456",  33.7490,  -84.3880,  9800.0, 230.0, 270.0, -0.5,  False, ts(-2), ts(0), "simulator"),
            ("a00003", "AAL789",  40.6413,  -73.7781, 11200.0, 258.7, 45.0,   2.1,  False, ts(-1), ts(0), "simulator"),
            ("a00004", "SWA321",  29.9902,  -95.3368,  7500.0, 210.0, 180.0,  0.0,  False, ts(-3), ts(0), "simulator"),
            ("a00005", "JBU654",  25.7959,  -80.2870,  5000.0, 195.5, 355.0,  3.2,  False, ts(-1), ts(0), "simulator"),
            ("a00006", "SKW987",  47.4502, -122.3088,  8200.0, 220.0, 120.0, -1.8,  False, ts(-2), ts(0), "simulator"),
            ("a00007", "FFT111",  36.1245,  -86.6782, 10100.0, 240.0, 300.0,  0.5,  False, ts(-1), ts(0), "simulator"),
            ("a00008", "NKS222",  32.8998,  -97.0403,  9300.0, 235.0, 210.0, -2.0,  False, ts(-2), ts(0), "simulator"),
            ("a00009", "ASA333",  45.5898, -122.5951,  7800.0, 215.0, 85.0,   1.0,  False, ts(-1), ts(0), "simulator"),
            ("a0000a", "HAL444",  21.3245, -157.9251,  8900.0, 260.0, 260.0,  0.0,  False, ts(-3), ts(0), "simulator"),
        ],
        columns=[
            "icao24", "callsign", "lat", "lon", "altitude_m", "velocity_mps",
            "heading_deg", "vertical_rate_mps", "on_ground",
            "last_event_time_utc", "updated_at_utc", "source",
        ],
    )

    airspace_grid = pd.DataFrame(
        [
            (ts(-10), ts(-5),  "41.9_-87.6",  3, 10400.0, 238.0),
            (ts(-10), ts(-5),  "33.7_-84.4",  2,  9800.0, 230.0),
            (ts(-10), ts(-5),  "40.6_-73.8",  4, 11000.0, 252.0),
            (ts(-10), ts(-5),  "29.9_-95.3",  1,  7500.0, 210.0),
            (ts(-5),  ts(0),   "41.9_-87.6",  4, 10600.0, 245.0),
            (ts(-5),  ts(0),   "33.7_-84.4",  3,  9800.0, 232.0),
            (ts(-5),  ts(0),   "40.6_-73.8",  5, 11200.0, 258.0),
            (ts(-5),  ts(0),   "47.5_-122.3", 2,  8200.0, 220.0),
        ],
        columns=["window_start", "window_end", "grid_cell", "aircraft_count", "avg_altitude_m", "avg_velocity_mps"],
    )

    quality = pd.DataFrame(
        [
            (ts(-25), ts(-20), 120, 110, 10, 1.2),
            (ts(-20), ts(-15), 135, 128,  7, 1.0),
            (ts(-15), ts(-10), 142, 130, 12, 1.5),
            (ts(-10), ts(-5),  118, 112,  6, 0.9),
            (ts(-5),  ts(0),   155, 148,  7, 1.1),
        ],
        columns=[
            "window_start", "window_end", "total_messages",
            "messages_with_position", "messages_missing_position", "avg_event_lag_seconds",
        ],
    )

    return {
        "realtime_flight_state":     flight_state,
        "realtime_airspace_grid_5m": airspace_grid,
        "telemetry_quality_5m":      quality,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="Skip Postgres, use hardcoded fallback")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tables: dict[str, pd.DataFrame] | None = None
    if not args.offline:
        try:
            tables = export_from_postgres()
            print("Exported from Postgres.")
        except Exception as e:
            print(f"Postgres unavailable ({e}), using hardcoded fallback.")

    if tables is None:
        tables = hardcoded_fallback()
        print("Using hardcoded fallback data.")

    for name, df in tables.items():
        path = OUTPUT_DIR / f"{name}.parquet"
        df.to_parquet(path, index=False)
        print(f"  Wrote {len(df)} rows → {path}")


if __name__ == "__main__":
    main()
