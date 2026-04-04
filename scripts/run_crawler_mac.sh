#!/usr/bin/env bash
# Local Mac crawler entrypoint.
# Runs a single batch against Railway MySQL via public TCP endpoint.
# Usage: ./scripts/run_crawler_mac.sh [batch_id]
# Example: ./scripts/run_crawler_mac.sh batch_a
set -euo pipefail

BATCH_ID="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# ── Railway MySQL credentials ────────────────────────────────────────────────
# Loaded from .env.railway (gitignored). Copy .env.railway.example to create it.
ENV_FILE="$PROJECT_DIR/.env.railway"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Copy .env.railway.example and fill in credentials." >&2
  exit 1
fi
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a
# ─────────────────────────────────────────────────────────────────────────────

export CRAWLER_CONFIG_PATH="$PROJECT_DIR/config/crawler_venues.toml"
export RAWHTML_BASE_URL="https://pub-442b944fa7de402c96d47a99c9e857ee.r2.dev"
export CRAWLER_ALERT_WEBHOOK_URL="https://hooks.zapier.com/hooks/catch/26720878/uxujrnk"
export APP_ENV="production"
export LOG_LEVEL="INFO"

source ~/.zshrc

cd "$PROJECT_DIR"

TIMESTAMP="$(date '+%Y-%m-%dT%H:%M:%S')"
if [[ -n "$BATCH_ID" ]]; then
  LOG_FILE="$LOG_DIR/crawler_${BATCH_ID}_${TIMESTAMP}.log"
  echo "[$TIMESTAMP] Starting batch: $BATCH_ID → Railway MySQL ($MYSQL_HOST:$MYSQL_PORT)" | tee "$LOG_FILE"
  python scripts/run_crawler_batch.py \
    --config "$CRAWLER_CONFIG_PATH" \
    --batch-id "$BATCH_ID" 2>&1 | tee -a "$LOG_FILE"
else
  LOG_FILE="$LOG_DIR/crawler_all_${TIMESTAMP}.log"
  echo "[$TIMESTAMP] Starting all batches → Railway MySQL ($MYSQL_HOST:$MYSQL_PORT)" | tee "$LOG_FILE"
  python scripts/run_crawler_batch.py \
    --config "$CRAWLER_CONFIG_PATH" 2>&1 | tee -a "$LOG_FILE"
fi

# Keep only last 30 log files
ls -t "$LOG_DIR"/crawler_*.log 2>/dev/null | tail -n +31 | xargs rm -f || true
