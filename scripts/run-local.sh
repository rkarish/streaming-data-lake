#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Run the mock data generator locally (outside Docker)
# =============================================================================
# Prerequisites:
#   - Python 3.12+
#   - Kafka running (e.g. via `docker compose up kafka -d`)
#
# Usage:
#   bash scripts/run-local.sh
#
# Override defaults with environment variables:
#   KAFKA_BOOTSTRAP_SERVERS=localhost:29092 EVENTS_PER_SECOND=5 bash scripts/run-local.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"
MOCK_DIR="${PROJECT_ROOT}/mock-data-gen"

# Start Kafka if not already running
echo "==> Ensuring Kafka is running..."
docker compose -f "${PROJECT_ROOT}/docker-compose.yml" up kafka -d

echo "==> Waiting for Kafka to be healthy..."
until docker compose -f "${PROJECT_ROOT}/docker-compose.yml" ps kafka --format json | grep -q '"Health":"healthy"'; do
  sleep 2
done
echo "==> Kafka is ready."

# Create virtual environment if it doesn't exist
if [ ! -d "${VENV_DIR}" ]; then
  echo "==> Creating virtual environment at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi

# Activate virtual environment
echo "==> Activating virtual environment..."
source "${VENV_DIR}/bin/activate"

# Install dependencies
echo "==> Installing mock-data-gen package..."
pip install --quiet "${MOCK_DIR}"

# Default to localhost when running outside Docker
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"
export EVENTS_PER_SECOND="${EVENTS_PER_SECOND:-10}"
export TOPIC_BID_REQUESTS="${TOPIC_BID_REQUESTS:-bid-requests}"

echo "==> Starting mock data generator (${EVENTS_PER_SECOND} events/sec to ${KAFKA_BOOTSTRAP_SERVERS})..."
echo "    Press Ctrl+C to stop."
echo ""

cd "${MOCK_DIR}"
python -m src.generator
