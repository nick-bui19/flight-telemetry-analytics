"""Dead-letter queue writer for invalid flight telemetry events."""

from __future__ import annotations

import json
import logging
from typing import Union

from kafka import KafkaProducer

from producers.schema import DLQ_TOPIC, DLQEvent

logger = logging.getLogger(__name__)


def write_to_dlq(producer: KafkaProducer, event: DLQEvent, topic: str = DLQ_TOPIC) -> None:
    """Serialize a DLQEvent and publish it to the DLQ Kafka topic."""
    payload = json.dumps(event.to_kafka_dict()).encode("utf-8")
    key = event.raw_icao24.encode("utf-8") if event.raw_icao24 else None
    try:
        producer.send(topic, value=payload, key=key)
    except Exception:
        logger.exception("Failed to write event to DLQ topic=%s icao24=%s", topic, event.raw_icao24)
