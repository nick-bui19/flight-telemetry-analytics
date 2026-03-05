"""Tests for snapshot (DEMO_MODE=snapshot) data loading."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

# Import the function under test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "dashboard"))
from app import _load_table

# Expected columns per table
FLIGHT_STATE_COLS = {
    "icao24", "callsign", "lat", "lon", "altitude_m", "velocity_mps",
    "heading_deg", "vertical_rate_mps", "on_ground", "last_event_time_utc",
    "updated_at_utc", "source",
}
GRID_COLS = {
    "window_start", "window_end", "grid_cell",
    "aircraft_count", "avg_altitude_m", "avg_velocity_mps",
}
QUALITY_COLS = {
    "window_start", "window_end", "total_messages", "messages_with_position",
    "messages_missing_position", "avg_event_lag_seconds",
}


def _write_parquet(tmp_path: Path, name: str, df: pd.DataFrame) -> None:
    df.to_parquet(tmp_path / f"{name}.parquet", index=False)


@pytest.fixture(autouse=True)
def force_snapshot_mode(monkeypatch):
    monkeypatch.setenv("DEMO_MODE", "snapshot")


def test_load_table_flight_state_columns(tmp_path):
    df = pd.DataFrame({c: [] for c in FLIGHT_STATE_COLS})
    _write_parquet(tmp_path, "realtime_flight_state", df)
    result = _load_table("realtime_flight_state", _snapshot_dir=tmp_path)
    assert set(result.columns) == FLIGHT_STATE_COLS


def test_load_table_grid_columns(tmp_path):
    df = pd.DataFrame({c: [] for c in GRID_COLS})
    _write_parquet(tmp_path, "realtime_airspace_grid_5m", df)
    result = _load_table("realtime_airspace_grid_5m", _snapshot_dir=tmp_path)
    assert set(result.columns) == GRID_COLS


def test_load_table_quality_columns(tmp_path):
    df = pd.DataFrame({c: [] for c in QUALITY_COLS})
    _write_parquet(tmp_path, "telemetry_quality_5m", df)
    result = _load_table("telemetry_quality_5m", _snapshot_dir=tmp_path)
    assert set(result.columns) == QUALITY_COLS


def test_load_table_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        _load_table("realtime_flight_state", _snapshot_dir=tmp_path)


def test_snapshot_live_map_no_time_filter(tmp_path):
    """Past timestamps are returned unchanged — snapshot mode skips the time filter."""
    old_ts = pd.Timestamp("2020-01-01T00:00:00Z")
    df = pd.DataFrame(
        {
            "icao24":        ["abc"],
            "callsign":      ["UAL1"],
            "lat":           [41.0],
            "lon":           [-87.0],
            "altitude_m":    [10000.0],
            "velocity_mps":  [250.0],
            "heading_deg":   [90.0],
            "vertical_rate_mps": [1.0],
            "on_ground":     [False],
            "last_event_time_utc": [old_ts],
            "updated_at_utc": [old_ts],
            "source":        ["simulator"],
        }
    )
    _write_parquet(tmp_path, "realtime_flight_state", df)
    result = _load_table("realtime_flight_state", _snapshot_dir=tmp_path)
    assert len(result) == 1  # not filtered out despite old timestamp
