from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional

try:
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType,
        DoubleType, BooleanType,
    )
    _pyspark_available = True
except ImportError:
    _pyspark_available = False

SCHEMA_VERSION = 1
KAFKA_TOPIC = "flight-telemetry-v1"
DLQ_TOPIC = "flight-telemetry-dlq"


@dataclass
class FlightTelemetryEvent:
    # Required
    schema_version: int          # constant 1
    event_time_utc: str          # ISO 8601 UTC ending in Z
    source: str                  # "opensky" | "simulator"
    icao24: str                  # lowercase hex
    ingestion_time_utc: str      # ISO 8601 UTC ending in Z

    # Nullable
    callsign: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    altitude_m: Optional[float] = None   # geo_altitude_m if present, else baro_altitude_m
    velocity_mps: Optional[float] = None
    heading_deg: Optional[float] = None
    vertical_rate_mps: Optional[float] = None
    on_ground: Optional[bool] = None
    last_contact_utc: Optional[str] = None
    origin_country: Optional[str] = None
    geo_altitude_m: Optional[float] = None
    squawk: Optional[str] = None
    spi: Optional[bool] = None
    position_source: Optional[int] = None
    bbox_id: Optional[str] = None

    def to_kafka_dict(self) -> dict:
        return asdict(self)


@dataclass
class DLQEvent:
    error_time_utc: str
    error_type: str           # e.g. "schema_validation", "lat_out_of_range", "missing_required_field"
    raw_icao24: Optional[str]
    raw_payload: dict         # original raw data (small subset)
    message: str

    def to_kafka_dict(self) -> dict:
        return asdict(self)


# Spark schema derived from FlightTelemetryEvent field by field.
# Only defined when pyspark is available (not required for producers or unit tests).
if _pyspark_available:
    SPARK_SCHEMA = StructType([
        StructField("schema_version",     IntegerType(),   nullable=False),
        StructField("event_time_utc",     StringType(),    nullable=False),
        StructField("source",             StringType(),    nullable=False),
        StructField("icao24",             StringType(),    nullable=False),
        StructField("ingestion_time_utc", StringType(),    nullable=False),
        StructField("callsign",           StringType(),    nullable=True),
        StructField("lat",                DoubleType(),    nullable=True),
        StructField("lon",                DoubleType(),    nullable=True),
        StructField("altitude_m",         DoubleType(),    nullable=True),
        StructField("velocity_mps",       DoubleType(),    nullable=True),
        StructField("heading_deg",        DoubleType(),    nullable=True),
        StructField("vertical_rate_mps",  DoubleType(),    nullable=True),
        StructField("on_ground",          BooleanType(),   nullable=True),
        StructField("last_contact_utc",   StringType(),    nullable=True),
        StructField("origin_country",     StringType(),    nullable=True),
        StructField("geo_altitude_m",     DoubleType(),    nullable=True),
        StructField("squawk",             StringType(),    nullable=True),
        StructField("spi",                BooleanType(),   nullable=True),
        StructField("position_source",    IntegerType(),   nullable=True),
        StructField("bbox_id",            StringType(),    nullable=True),
    ])
else:
    SPARK_SCHEMA = None
