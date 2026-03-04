"""Flight telemetry Spark Structured Streaming processor.

Architecture:
  Kafka topic flight-telemetry-v1
    → parse_kafka_df()          → realtime_flight_state  (upsert via foreachBatch)
    → compute_airspace_grid()   → realtime_airspace_grid_5m (append via foreachBatch)
    → compute_quality_metrics() → telemetry_quality_5m      (append via foreachBatch)

Pure transform functions are separated from I/O callbacks for testability.
"""

from __future__ import annotations

import os
import sys
import logging

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Add project root to path so `producers` package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producers.schema import KAFKA_TOPIC, SPARK_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME        = os.environ.get("KAFKA_TOPIC", KAFKA_TOPIC)
POSTGRES_HOST           = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT           = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB             = os.environ.get("POSTGRES_DB", "flight_data")
POSTGRES_USER           = os.environ.get("POSTGRES_USER", "nickbui")
POSTGRES_PASSWORD       = os.environ.get("POSTGRES_PASSWORD", "dummy")
SPARK_CHECKPOINT_DIR    = os.environ.get("SPARK_CHECKPOINT_DIR", "./spark-checkpoints")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}


# ── Pure transform functions (testable with local SparkSession) ───────────────

def parse_kafka_df(raw_df: DataFrame) -> DataFrame:
    """Parse raw Kafka bytes → typed flight telemetry DataFrame."""
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), SPARK_SCHEMA).alias("d"))
        .select("d.*")
        # Parse ISO8601 strings to timestamps for windowing
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("event_time_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn(
            "ingestion_ts",
            F.to_timestamp(F.col("ingestion_time_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
    )
    return parsed


def compute_airspace_grid(parsed_df: DataFrame) -> DataFrame:
    """5-minute tumbling window aggregation by grid cell (~11 km resolution)."""
    with_grid = parsed_df.withColumn(
        "grid_cell",
        F.concat(
            F.round(F.col("lat"), 1).cast("string"),
            F.lit("_"),
            F.round(F.col("lon"), 1).cast("string"),
        ),
    ).filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())

    return (
        with_grid
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "5 minutes"),
            F.col("grid_cell"),
        )
        .agg(
            F.count("icao24").alias("aircraft_count"),
            F.avg("altitude_m").alias("avg_altitude_m"),
            F.avg("velocity_mps").alias("avg_velocity_mps"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("grid_cell"),
            F.col("aircraft_count"),
            F.col("avg_altitude_m"),
            F.col("avg_velocity_mps"),
        )
    )


def compute_quality_metrics(parsed_df: DataFrame) -> DataFrame:
    """5-minute tumbling window data quality aggregation."""
    return (
        parsed_df
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window(F.col("event_ts"), "5 minutes"))
        .agg(
            F.count("icao24").alias("total_messages"),
            F.sum(
                F.when(F.col("lat").isNotNull() & F.col("lon").isNotNull(), 1).otherwise(0)
            ).alias("messages_with_position"),
            F.sum(
                F.when(F.col("lat").isNull() | F.col("lon").isNull(), 1).otherwise(0)
            ).alias("messages_missing_position"),
            F.avg(
                F.unix_timestamp(F.col("ingestion_ts")) - F.unix_timestamp(F.col("event_ts"))
            ).alias("avg_event_lag_seconds"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("total_messages"),
            F.col("messages_with_position"),
            F.col("messages_missing_position"),
            F.col("avg_event_lag_seconds"),
        )
    )


# ── foreachBatch callbacks ────────────────────────────────────────────────────

def _get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def upsert_flight_state(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert current flight state per icao24 using psycopg2 execute_values."""
    rows = batch_df.collect()
    if not rows:
        return

    records = [
        (
            r["icao24"],
            r["callsign"],
            r["lat"],
            r["lon"],
            r["altitude_m"],
            r["velocity_mps"],
            r["heading_deg"],
            r["vertical_rate_mps"],
            r["on_ground"],
            r["event_time_utc"],   # stored as text; Postgres casts TIMESTAMPTZ
            r["ingestion_time_utc"],
            r["source"],
        )
        for r in rows
        if r["icao24"]
    ]
    if not records:
        return

    sql = """
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
    """
    conn = _get_pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, records)
        logger.info("batch_id=%d upserted %d flight state rows", batch_id, len(records))
    finally:
        conn.close()


def write_airspace_grid(batch_df: DataFrame, batch_id: int) -> None:
    """Append airspace grid aggregation rows via JDBC."""
    if batch_df.count() == 0:
        return
    batch_df.write.jdbc(JDBC_URL, "realtime_airspace_grid_5m", mode="append", properties=JDBC_PROPS)
    logger.info("batch_id=%d wrote airspace grid rows", batch_id)


def write_quality_metrics(batch_df: DataFrame, batch_id: int) -> None:
    """Append telemetry quality rows via JDBC."""
    if batch_df.count() == 0:
        return
    batch_df.write.jdbc(JDBC_URL, "telemetry_quality_5m", mode="append", properties=JDBC_PROPS)
    logger.info("batch_id=%d wrote quality metric rows", batch_id)


# ── writeStream wiring ────────────────────────────────────────────────────────

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FlightTelemetryProcessor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df     = parse_kafka_df(raw_df)
    grid_df       = compute_airspace_grid(parsed_df)
    quality_df    = compute_quality_metrics(parsed_df)

    flight_state_query = (
        parsed_df.writeStream
        .foreachBatch(upsert_flight_state)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/flight_state")
        .outputMode("update")
        .queryName("flight_state_upsert")
        .start()
    )

    grid_query = (
        grid_df.writeStream
        .foreachBatch(write_airspace_grid)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/airspace_grid")
        .outputMode("update")
        .queryName("airspace_grid_5m")
        .start()
    )

    quality_query = (
        quality_df.writeStream
        .foreachBatch(write_quality_metrics)
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/quality_metrics")
        .outputMode("update")
        .queryName("quality_metrics_5m")
        .start()
    )

    flight_state_query.awaitTermination()


if __name__ == "__main__":
    main()
