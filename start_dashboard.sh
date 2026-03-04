#!/bin/bash

# Real-Time Flight Telemetry Dashboard Startup Script

# Source .env before cd so dirname "$0" is still correct
if [ -f "$(dirname "$0")/.env" ]; then
    set -a
    source "$(dirname "$0")/.env"
    set +a
fi

# Set working directory — all relative paths below assume project root
cd "$(dirname "$0")"

mkdir -p logs

MODE="${MODE:-simulator}"
KAFKA_BS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
POSTGRES_USER="${POSTGRES_USER:-nickbui}"
POSTGRES_DB="${POSTGRES_DB:-flight_data}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8501}"

PRODUCER_PID=""
SPARK_PID=""

# ── Cleanup (runs on Ctrl+C or exit) ──────────────────────────────────────────
cleanup() {
    echo ""
    echo "Shutting down services..."
    [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null
    [ -n "$SPARK_PID" ]    && kill "$SPARK_PID"    2>/dev/null
    cd kafka-setup && docker-compose down --timeout 10
    echo "All services stopped."
}
trap cleanup INT TERM EXIT

echo "Starting Real-Time Flight Telemetry Dashboard..."
echo "=================================================="
echo "Mode: $MODE"

# ── Virtual environment ────────────────────────────────────────────────────────
echo "[1/7] Activating virtual environment..."
source venv/bin/activate

# ── Docker ────────────────────────────────────────────────────────────────────
echo "[2/7] Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker Desktop and re-run this script."
    exit 1
fi

# ── Kafka ─────────────────────────────────────────────────────────────────────
echo "[3/7] Starting Kafka..."
cd kafka-setup
docker-compose up -d
cd ..

echo "      Waiting for Kafka broker to accept connections (up to 60s)..."
KAFKA_READY=0
for i in $(seq 1 30); do
    if python -c "
from kafka import KafkaAdminClient
KafkaAdminClient(bootstrap_servers='$KAFKA_BS', request_timeout_ms=2000).close()
" 2>/dev/null; then
        KAFKA_READY=1
        break
    fi
    sleep 2
done

if [ "$KAFKA_READY" -eq 0 ]; then
    echo "Kafka did not become ready within 60 seconds."
    echo "Check: docker-compose -f kafka-setup/docker-compose.yml logs kafka"
    exit 1
fi
echo "      Kafka is ready."

echo "      Ensuring topics exist..."
KAFKA_CONTAINER=$(docker ps \
    --filter "name=kafka" \
    --filter "status=running" \
    --format "{{.Names}}" | grep -v zookeeper | head -1)

docker exec "$KAFKA_CONTAINER" kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic flight-telemetry-v1 \
    --partitions 1 --replication-factor 1 > /dev/null

docker exec "$KAFKA_CONTAINER" kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic flight-telemetry-dlq \
    --partitions 1 --replication-factor 1 > /dev/null
echo "      Topics ready."

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
echo "[4/7] Checking PostgreSQL..."
if ! psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1;" > /dev/null 2>&1; then
    echo "PostgreSQL connection failed."
    echo "Make sure PostgreSQL is running and the flight_data database exists:"
    echo "  psql -d postgres -U $POSTGRES_USER -c 'CREATE DATABASE $POSTGRES_DB;'"
    echo "  psql -U $POSTGRES_USER -d $POSTGRES_DB -f sql/init_flight_db.sql"
    exit 1
fi
echo "      PostgreSQL OK."

# ── Producer ───────────────────────────────────────────────────────────────────
echo "[5/7] Starting producer ($MODE mode)..."
if [ "$MODE" = "opensky" ]; then
    python -m producers.opensky_producer > logs/producer.log 2>&1 &
else
    python -m producers.flight_simulator > logs/producer.log 2>&1 &
fi
PRODUCER_PID=$!
echo "      PID: $PRODUCER_PID  |  tail -f logs/producer.log"

sleep 3
if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
    echo "Producer crashed. Last 20 lines of logs/producer.log:"
    tail -20 logs/producer.log
    exit 1
fi
echo "      Producer is running."

# ── Spark ──────────────────────────────────────────────────────────────────────
echo "[6/7] Starting Spark..."
bash run_spark.sh > logs/spark.log 2>&1 &
SPARK_PID=$!
echo "      PID: $SPARK_PID  |  tail -f logs/spark.log"

sleep 5
if ! kill -0 "$SPARK_PID" 2>/dev/null; then
    echo "Spark failed to start. Last 20 lines of logs/spark.log:"
    tail -20 logs/spark.log
    exit 1
fi
echo "      Spark is running."

# ── Dashboard ──────────────────────────────────────────────────────────────────
echo "[7/7] Launching dashboard..."
echo ""
echo "  http://localhost:$DASHBOARD_PORT"
echo "  Ctrl+C to stop everything"
echo ""

streamlit run dashboard/app.py \
    --server.port "$DASHBOARD_PORT" \
    --server.address localhost
