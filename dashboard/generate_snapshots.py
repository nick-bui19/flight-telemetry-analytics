"""Generate realistic parquet snapshots for the demo dashboard.

Run from repo root:
    python dashboard/generate_snapshots.py
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

OUT_DIR = Path(__file__).parent / "data"
OUT_DIR.mkdir(exist_ok=True)

NOW = datetime.now(tz=timezone.utc)


def _ts(delta_minutes: float = 0) -> datetime:
    return NOW + timedelta(minutes=delta_minutes)


# ── Realistic airline callsign prefixes ───────────────────────────────────────

AIRLINES = [
    ("UAL", 3.0), ("DAL", 3.0), ("AAL", 3.0), ("SWA", 2.5),
    ("JBU", 1.5), ("SKW", 1.5), ("ASA", 1.0), ("FFT", 1.0),
    ("NKS", 1.0), ("HAL", 0.4), ("WJA", 0.5), ("ACA", 0.5),
    ("BAW", 0.4), ("AFR", 0.3), ("DLH", 0.3), ("UAE", 0.2),
    ("QXE", 0.8), ("RPA", 0.8), ("ENY", 0.7), ("PDT", 0.6),
    ("PSA", 0.6), ("FDX", 0.8), ("UPS", 0.6), ("GTI", 0.3),
    ("ABX", 0.3), ("CLX", 0.2), ("CPZ", 0.7), ("SJI", 0.5),
]
AIRLINE_PREFIXES = [a[0] for a in AIRLINES]
AIRLINE_WEIGHTS  = [a[1] for a in AIRLINES]

# Major US flight corridors — (lat_center, lon_center, radius_deg, weight)
CORRIDORS = [
    # Northeast megalopolis
    (40.7, -74.0, 3.5, 3.0),   # New York
    (42.4, -71.0, 2.0, 1.5),   # Boston
    (38.9, -77.0, 2.5, 1.5),   # Washington DC
    (39.9, -75.2, 1.5, 1.0),   # Philadelphia
    # Southeast
    (33.6, -84.4, 3.0, 2.5),   # Atlanta (massive hub)
    (25.8, -80.3, 2.5, 1.5),   # Miami
    (28.4, -81.3, 2.0, 1.2),   # Orlando
    (35.2, -80.9, 1.5, 0.8),   # Charlotte
    # Midwest
    (41.9, -87.6, 3.0, 2.5),   # Chicago (O'Hare)
    (39.1, -84.7, 1.5, 0.8),   # Cincinnati
    (44.9, -93.2, 1.5, 0.8),   # Minneapolis
    (39.3, -76.6, 1.5, 0.8),   # Baltimore/BWI
    # South/Central
    (32.9, -97.0, 2.5, 2.0),   # Dallas/Fort Worth
    (29.6, -95.3, 2.0, 1.5),   # Houston
    (36.1, -86.7, 1.5, 0.8),   # Nashville
    (30.3, -81.7, 1.2, 0.7),   # Jacksonville
    # Mountain / Southwest
    (39.9, -104.7, 2.0, 1.5),  # Denver
    (33.4, -112.0, 2.0, 1.2),  # Phoenix
    (36.1, -115.2, 1.5, 0.8),  # Las Vegas
    (35.0, -106.6, 1.0, 0.5),  # Albuquerque
    (29.5, -98.5, 1.0, 0.5),   # San Antonio
    # West Coast
    (33.9, -118.4, 3.0, 2.5),  # Los Angeles
    (37.6, -122.4, 2.5, 2.0),  # San Francisco
    (47.4, -122.3, 2.0, 1.5),  # Seattle
    (45.6, -122.6, 1.0, 0.6),  # Portland
    (32.7, -117.2, 1.5, 1.0),  # San Diego
    # Transcontinental en-route (scattered along common paths)
    (39.0, -100.0, 5.0, 1.5),  # Kansas mid-country
    (40.0, -90.0, 4.0, 1.2),   # Illinois/Missouri
    (40.0, -110.0, 4.0, 0.8),  # Utah/Nevada corridor
    (38.0, -95.0, 4.0, 1.0),   # Kansas/Oklahoma corridor
    (35.0, -100.0, 4.0, 0.8),  # Texas panhandle corridor
    # Canada transborder
    (43.7, -79.6, 1.5, 0.7),   # Toronto
    (45.5, -73.6, 1.5, 0.6),   # Montreal
    # Alaska / Hawaii
    (61.2, -149.9, 3.0, 0.4),  # Anchorage
    (21.3, -157.9, 2.0, 0.3),  # Honolulu
    (64.8, -147.9, 1.5, 0.2),  # Fairbanks
]

CORRIDOR_WEIGHTS = [c[3] for c in CORRIDORS]


def _sample_position() -> tuple[float, float]:
    corridor = random.choices(CORRIDORS, weights=CORRIDOR_WEIGHTS, k=1)[0]
    lat_c, lon_c, radius, _ = corridor
    lat = lat_c + np.random.normal(0, radius * 0.45)
    lon = lon_c + np.random.normal(0, radius * 0.45)
    return round(lat, 4), round(lon, 4)


def _make_callsign(idx: int) -> tuple[str, str]:
    prefix = random.choices(AIRLINE_PREFIXES, weights=AIRLINE_WEIGHTS, k=1)[0]
    num = random.randint(100, 9999)
    # ICAO24 hex — realistic 6-char hex, US range starts with 'a'
    icao24 = f"a{idx:05x}"
    return f"{prefix}{num}", icao24


def _realistic_altitude() -> float:
    r = random.random()
    if r < 0.04:
        return round(random.uniform(100, 1500), 0)    # climb/descent
    elif r < 0.12:
        return round(random.uniform(1500, 5000), 0)   # regional low
    elif r < 0.28:
        return round(random.uniform(5000, 8500), 0)   # medium haul
    elif r < 0.55:
        return round(random.uniform(8500, 10700), 0)  # FL280-FL350
    else:
        return round(random.uniform(10700, 12500), 0) # FL350-FL410 long-haul


def _realistic_velocity(altitude_m: float) -> float:
    if altitude_m < 1500:
        return round(random.uniform(60, 130), 1)
    elif altitude_m < 5000:
        return round(random.uniform(130, 200), 1)
    elif altitude_m < 8500:
        return round(random.uniform(190, 245), 1)
    else:
        return round(random.uniform(230, 285), 1)


def _realistic_vertical_rate(altitude_m: float) -> float:
    r = random.random()
    if altitude_m > 9000:
        if r < 0.82:
            return round(random.uniform(-0.5, 0.5), 2)
        elif r < 0.91:
            return round(random.uniform(0.5, 7.0), 2)
        else:
            return round(random.uniform(-7.0, -0.5), 2)
    else:
        if r < 0.38:
            return round(random.uniform(2.0, 12.0), 2)
        elif r < 0.68:
            return round(random.uniform(-12.0, -1.0), 2)
        else:
            return round(random.uniform(-0.5, 0.5), 2)


# ── Generate flight state ─────────────────────────────────────────────────────

N_AIRCRAFT = 480

rows = []
for i in range(N_AIRCRAFT):
    callsign, icao24 = _make_callsign(i)
    lat, lon = _sample_position()
    alt = _realistic_altitude()
    vel = _realistic_velocity(alt)
    hdg = round(random.uniform(0, 360), 1)
    vrate = _realistic_vertical_rate(alt)
    on_ground = (alt < 200) and (random.random() < 0.3)
    last_event = _ts(-random.uniform(0.05, 4.5))
    updated_at = _ts(-random.uniform(0, 1.5))
    source = random.choices(["opensky", "simulator"], weights=[0.92, 0.08], k=1)[0]
    rows.append({
        "icao24": icao24,
        "callsign": callsign,
        "lat": lat,
        "lon": lon,
        "altitude_m": alt,
        "velocity_mps": vel,
        "heading_deg": hdg,
        "vertical_rate_mps": vrate,
        "on_ground": on_ground,
        "last_event_time_utc": last_event,
        "updated_at_utc": updated_at,
        "source": source,
    })

flight_df = pd.DataFrame(rows)
flight_df.to_parquet(OUT_DIR / "realtime_flight_state.parquet", index=False)
print(f"Wrote {len(flight_df)} rows -> realtime_flight_state.parquet")


# ── Generate airspace grid ────────────────────────────────────────────────────

def _grid_cell(lat: float, lon: float) -> str:
    return f"{round(lat, 1)}_{round(lon, 1)}"


grid_rows = []
for window_offset in [-10, -5]:
    window_start = _ts(window_offset)
    window_end   = _ts(window_offset + 5)

    cell_data: dict[str, dict] = {}
    for row in rows:
        cell = _grid_cell(row["lat"], row["lon"])
        if cell not in cell_data:
            cell_data[cell] = {"alts": [], "vels": []}
        cell_data[cell]["alts"].append(row["altitude_m"] + random.uniform(-300, 300))
        cell_data[cell]["vels"].append(row["velocity_mps"] + random.uniform(-8, 8))

    for cell, data in cell_data.items():
        count = len(data["alts"])
        effective_count = max(1, count + random.randint(-1, 1))
        grid_rows.append({
            "window_start": window_start,
            "window_end": window_end,
            "grid_cell": cell,
            "aircraft_count": effective_count,
            "avg_altitude_m": round(float(np.mean(data["alts"])), 1),
            "avg_velocity_mps": round(float(np.mean(data["vels"])), 1),
        })

grid_df = pd.DataFrame(grid_rows)
grid_df.to_parquet(OUT_DIR / "realtime_airspace_grid_5m.parquet", index=False)
print(f"Wrote {len(grid_df)} rows -> realtime_airspace_grid_5m.parquet")


# ── Generate quality metrics ──────────────────────────────────────────────────
# 12 windows (~1 hour of history), total_messages in 2900-3500 range

quality_rows = []
for i in range(12):
    offset = -(12 - i) * 5
    window_start = _ts(offset)
    window_end   = _ts(offset + 5)

    total   = random.randint(2900, 3500)
    missing = random.randint(int(total * 0.02), int(total * 0.055))
    with_pos = total - missing
    avg_lag  = round(random.uniform(0.8, 2.2), 2)

    quality_rows.append({
        "window_start": window_start,
        "window_end": window_end,
        "total_messages": total,
        "messages_with_position": with_pos,
        "messages_missing_position": missing,
        "avg_event_lag_seconds": avg_lag,
    })

quality_df = pd.DataFrame(quality_rows)
quality_df.to_parquet(OUT_DIR / "telemetry_quality_5m.parquet", index=False)
print(f"Wrote {len(quality_df)} rows -> telemetry_quality_5m.parquet")
print("Done.")
