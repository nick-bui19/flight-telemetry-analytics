"""Integration test fixtures using testcontainers.

Spins up real Kafka and Postgres containers for each test session,
initialises the DB schema, and tears everything down afterwards.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import psycopg2
import pytest
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

# Path to DDL file relative to project root
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DDL_FILE = _PROJECT_ROOT / "sql" / "init_flight_db.sql"

KAFKA_IMAGE    = "confluentinc/cp-kafka:7.6.1"
POSTGRES_IMAGE = "postgres:16"


@pytest.fixture(scope="session")
def kafka_container():
    with KafkaContainer(image=KAFKA_IMAGE) as kafka:
        yield kafka


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(
        image=POSTGRES_IMAGE,
        dbname="flight_data",
        username="testuser",
        password="testpass",
    ) as pg:
        yield pg


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def pg_dsn(postgres_container) -> dict:
    return {
        "host":     postgres_container.get_container_host_ip(),
        "port":     postgres_container.get_exposed_port(5432),
        "dbname":   "flight_data",
        "user":     "testuser",
        "password": "testpass",
    }


@pytest.fixture(scope="session")
def pg_conn(pg_dsn):
    """Persistent psycopg2 connection for the test session."""
    conn = psycopg2.connect(**pg_dsn)
    conn.autocommit = True

    # Initialise schema
    ddl = _DDL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(ddl)

    yield conn
    conn.close()


@pytest.fixture(scope="session")
def kafka_topics(kafka_bootstrap):
    """Create required Kafka topics."""
    from producers.schema import DLQ_TOPIC, KAFKA_TOPIC
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    topics = [
        NewTopic(name=KAFKA_TOPIC,  num_partitions=1, replication_factor=1),
        NewTopic(name=DLQ_TOPIC,    num_partitions=1, replication_factor=1),
    ]
    # Ignore AlreadyExistsException
    try:
        admin.create_topics(topics)
    except Exception:
        pass
    admin.close()
    return {"main": KAFKA_TOPIC, "dlq": DLQ_TOPIC}


@pytest.fixture
def kafka_producer(kafka_bootstrap) -> KafkaProducer:
    import json
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    yield producer
    producer.close()


@pytest.fixture
def kafka_consumer_factory(kafka_bootstrap):
    """Returns a factory that creates a KafkaConsumer for a given topic."""
    consumers = []

    def _factory(topic: str) -> KafkaConsumer:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: m.decode("utf-8"),
        )
        consumers.append(consumer)
        return consumer

    yield _factory

    for c in consumers:
        c.close()
