# Real-Time Flight Telemetry Streaming Analytics

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Tests](https://img.shields.io/badge/tests-70%20passing-brightgreen)

---

## Demo
https://flight-telemetry-analytics-jcqx4svd9ajvmurfbfkqrx.streamlit.app/

---

## Architecture

```
OpenSky REST API
      │
      ▼
 flight_stream.py          (Kafka Producer — polls every 15s, deduplicates, DLQ)
      │
      ▼ Kafka topic: flight-telemetry-v1
      │
      ▼
 Spark Structured Streaming (foreachBatch → 3 tables in Postgres)
      │
      ├─► realtime_flight_state       (upsert per ICAO24)
      ├─► realtime_airspace_grid_5m   (5-min tumbling window, ~11 km grid cells)
      └─► telemetry_quality_5m        (5-min quality metrics)
                │
                ▼
         PostgreSQL: flight_data
                │
                ▼
         Streamlit Dashboard  ◄── parquet snapshots (DEMO_MODE=snapshot)
```

---

## Key Features

- **Live flight map** — scatter-geo of tracked aircraft colored by altitude
- **Leaderboards** — top 10 by speed, altitude, and climb rate
- **Airspace density heatmap** — ~11 km grid cells, latest 5-min window
- **Telemetry quality panel** — position hit rate and event lag over time
- **Snapshot mode** — hosted demo backed by static parquet; no Kafka/Spark needed
- **Dead-letter queue** — malformed messages routed to a DLQ Kafka topic
- **OpenSky dedupe** — TTL cache on `(icao24, last_contact, lat, lon, alt, speed)`

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingest | Python, OpenSky REST API, `kafka-python` |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Storage | PostgreSQL (JDBC writes from Spark) |
| Dashboard | Streamlit, Plotly, pandas |
| Snapshot format | Apache Parquet (pyarrow) |
| Tests | pytest, testcontainers (Kafka + Postgres) |

---

## Quick Start (Dashboard Only)

```bash
# 1. Install dashboard dependencies
pip install -r dashboard/requirements.txt

# 2. (Optional) Refresh demo snapshots
python scripts/export_snapshots.py --offline

# 3. Run the dashboard in snapshot mode
DEMO_MODE=snapshot streamlit run dashboard/app.py
```

The dashboard defaults to `DEMO_MODE=snapshot`, so step 3 works without any running services.

---

<details>
<summary><strong>Full Pipeline (Kafka + Spark + OpenSky)</strong></summary>

### Prerequisites
- Docker (for Kafka)
- Java 11+ (for Spark)
- PostgreSQL running locally

### Setup

```bash
# Install all dependencies
pip install -r requirements.txt

# Create database and schema
psql -U nickbui -c "CREATE DATABASE flight_data;"
psql -U nickbui -d flight_data -f sql/init_flight_db.sql

# Start Kafka (via Docker)
cd kafka-setup && docker-compose up -d

# Start the Spark stream processor
./run_spark.sh

# Start the dashboard (simulator mode, no OpenSky credentials needed)
MODE=simulator ./start_dashboard.sh

# Or with live OpenSky data
OPENSKY_USER=your_user OPENSKY_PASS=your_pass MODE=opensky ./start_dashboard.sh
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MODE` | `simulator` | `simulator` or `opensky` |
| `DEMO_MODE` | `snapshot` | `snapshot` or `postgres` |
| `POSTGRES_HOST` | `localhost` | Postgres host |
| `POSTGRES_DB` | `flight_data` | Postgres database |
| `POSTGRES_USER` | `nickbui` | Postgres user |
| `POSTGRES_PASSWORD` | `dummy` | Postgres password |
| `OPENSKY_USER` | — | OpenSky API username |
| `OPENSKY_PASS` | — | OpenSky API password |

### Snapshot Refresh Workflow

After running the full pipeline locally, export fresh snapshots:

```bash
# Export from live Postgres
python scripts/export_snapshots.py

# Or offline (uses hardcoded data)
python scripts/export_snapshots.py --offline

# Commit and push — Streamlit Cloud auto-redeploys
git add dashboard/data/
git commit -m "chore: refresh demo snapshots"
git push
```

</details>

---

## Running Tests

```bash
# Unit tests only (no services required)
pytest tests/unit/ -v

# Integration tests (requires Docker)
pytest tests/integration/ -v
```

---

## Data Pipeline Notes

- **Dedupe key**: `(icao24, last_contact_utc, lat, lon, altitude_m, velocity_mps)` — TTL cache prevents duplicate Kafka messages
- **Grid cell**: `f"{round(lat,1)}_{round(lon,1)}"` (~11 km cells at mid-latitudes)
- **Upsert strategy**: `ON CONFLICT (icao24) DO UPDATE` for flight state; append-only for aggregations
- **DLQ**: Messages failing schema validation are written to `flight-telemetry-dlq` Kafka topic
