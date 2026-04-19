#!/usr/bin/env bash
# Local crawler entrypoint targeting local MySQL via .env.
# Runs a single batch or all batches.
# Usage: ./scripts/run_crawler_local.sh [batch_id]
set -euo pipefail

BATCH_ID="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# ── Local MySQL credentials ─────────────────────────────────────────────────
ENV_FILE="$PROJECT_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found." >&2
  exit 1
fi
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a
# ─────────────────────────────────────────────────────────────────────────────

export CRAWLER_CONFIG_PATH="$PROJECT_DIR/config/crawler_venues.toml"
export APP_ENV="development"
export LOG_LEVEL="INFO"

source ~/.zshrc

cd "$PROJECT_DIR"

TIMESTAMP="$(date '+%Y-%m-%dT%H:%M:%S')"
if [[ -n "$BATCH_ID" ]]; then
  LOG_FILE="$LOG_DIR/crawler_local_${BATCH_ID}_${TIMESTAMP}.log"
  echo "[$TIMESTAMP] Starting batch: $BATCH_ID → local MySQL ($MYSQL_HOST:$MYSQL_PORT)" | tee "$LOG_FILE"
  python scripts/run_crawler_batch.py \
    --config "$CRAWLER_CONFIG_PATH" \
    --batch-id "$BATCH_ID" 2>&1 | tee -a "$LOG_FILE"
else
  LOG_FILE="$LOG_DIR/crawler_local_all_${TIMESTAMP}.log"
  echo "[$TIMESTAMP] Starting all batches → local MySQL ($MYSQL_HOST:$MYSQL_PORT)" | tee "$LOG_FILE"
  python scripts/run_crawler_batch.py \
    --config "$CRAWLER_CONFIG_PATH" 2>&1 | tee -a "$LOG_FILE"
fi

# Keep only last 30 log files
ls -t "$LOG_DIR"/crawler_local_*.log 2>/dev/null | tail -n +31 | xargs rm -f || true
