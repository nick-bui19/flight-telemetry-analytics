#!/bin/bash

# Real-Time Flight Telemetry Dashboard Startup Script

# Source .env if present
if [ -f "$(dirname "$0")/.env" ]; then
    set -a
    source "$(dirname "$0")/.env"
    set +a
fi

echo "Starting Real-Time Flight Telemetry Dashboard..."
echo "=================================================="

# Set working directory
cd "$(dirname "$0")"

# Default MODE
MODE="${MODE:-simulator}"
echo "Mode: $MODE"

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check if Docker is running
echo "Checking Docker status..."
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker Desktop."
    open -a Docker
    echo "Waiting for Docker to start (30 seconds)..."
    sleep 30
fi

# Start Kafka infrastructure
echo "Starting Kafka infrastructure..."
cd kafka-setup
docker-compose up -d
sleep 10

# Check Kafka status
if docker ps | grep -q kafka; then
    echo "Kafka is running"
else
    echo "Kafka may not be fully ready yet"
fi

cd ..

# Check PostgreSQL connection
echo "Checking PostgreSQL connection..."
if psql -U "${POSTGRES_USER:-nickbui}" -d "${POSTGRES_DB:-flight_data}" -c "SELECT 1;" > /dev/null 2>&1; then
    echo "PostgreSQL connection successful"
else
    echo "PostgreSQL connection failed. Please check if PostgreSQL is running and flight_data DB exists."
    echo "Run: psql -U nickbui -c 'CREATE DATABASE flight_data;' && psql -U nickbui -d flight_data -f sql/init_flight_db.sql"
    exit 1
fi

# Start producer in background based on MODE
if [ "$MODE" = "opensky" ]; then
    echo "Starting OpenSky producer..."
    python producers/opensky_producer.py &
    PRODUCER_PID=$!
    echo "OpenSky producer PID: $PRODUCER_PID"
else
    echo "Starting flight simulator..."
    python producers/flight_simulator.py &
    PRODUCER_PID=$!
    echo "Simulator PID: $PRODUCER_PID"
fi

# Start Spark streaming in background
echo "Starting Spark streaming processor..."
bash run_spark.sh &
SPARK_PID=$!
echo "Spark PID: $SPARK_PID"

# Wait for data to start flowing
echo "Waiting for data pipeline to initialize (10 seconds)..."
sleep 10

# Launch dashboard
echo "Launching Streamlit dashboard..."
echo "Dashboard will be available at: http://localhost:${DASHBOARD_PORT:-8501}"
echo "Press Ctrl+C to stop all services"
echo ""

streamlit run dashboard/app.py \
    --server.port "${DASHBOARD_PORT:-8501}" \
    --server.address localhost

# Cleanup on exit
echo ""
echo "Shutting down services..."
kill $PRODUCER_PID 2>/dev/null
kill $SPARK_PID 2>/dev/null
cd kafka-setup
docker-compose down
echo "All services stopped"
