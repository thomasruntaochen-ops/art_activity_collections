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
# Alert webhook disabled: the Zapier catch hook required a paid plan and 404'd
# on every delivery. Zero-row alerts are appended to logs/crawler_alerts.jsonl.
export CRAWLER_ALERT_WEBHOOK_URL=""
export APP_ENV="production"
export LOG_LEVEL="INFO"
# Unbuffer Python stdout so run_crawler_batch.py's orchestration lines
# ([crawler-batch] Running venue=…, Per-venue timeout, timeout/kill messages)
# stream to the log live instead of block-buffering behind `tee`.
export PYTHONUNBUFFERED=1

source ~/.zshrc

cd "$PROJECT_DIR"

# ── Single-run lock ──────────────────────────────────────────────────────────
# Two crawler runs hitting the same DB concurrently race on an unguarded insert
# and create duplicate sources/venues/activities (see scripts/dedupe_venues.py).
# `mkdir` is atomic and portable (macOS has no `flock`); the lock is keyed by the
# target DB so unrelated databases don't block each other.
LOCK_DIR="$LOG_DIR/.crawler-${MYSQL_HOST:-railway}-${MYSQL_PORT:-3306}.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "ERROR: another crawler run is active for ${MYSQL_HOST:-railway}:${MYSQL_PORT:-3306} ($LOCK_DIR)." >&2
  echo "       Exiting to avoid duplicate ingestion. Remove the lock dir if it is stale." >&2
  exit 1
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

TIMESTAMP="$(date '+%Y-%m-%dT%H:%M:%S')"

# Under launchd, stdout is logs/launchd_daily.log, and teeing the whole run into
# it kept a second, never-rotated copy of every crawl (376 MB by Nov 2026). Only
# echo the run to stdout when a terminal is watching; launchd's log still gets
# the "Starting" line and anything that fails before the run begins.
log_run() {
  if [[ -t 1 ]]; then
    tee -a "$LOG_FILE"
  else
    cat >> "$LOG_FILE"
  fi
}

# The batch exits 1 whenever any venue fails, which is most days. Capture that
# instead of letting `set -e` end the script here, or the log rotation below
# never runs (224 logs had piled up by Nov 2026).
run_status=0
if [[ -n "$BATCH_ID" ]]; then
  LOG_FILE="$LOG_DIR/crawler_${BATCH_ID}_${TIMESTAMP}.log"
  echo "[$TIMESTAMP] Starting batch: $BATCH_ID → Railway MySQL ($MYSQL_HOST:$MYSQL_PORT)" | tee "$LOG_FILE"
  python scripts/run_crawler_batch.py \
    --config "$CRAWLER_CONFIG_PATH" \
    --batch-id "$BATCH_ID" 2>&1 | log_run || run_status=$?
else
  LOG_FILE="$LOG_DIR/crawler_all_${TIMESTAMP}.log"
  echo "[$TIMESTAMP] Starting all batches → Railway MySQL ($MYSQL_HOST:$MYSQL_PORT)" | tee "$LOG_FILE"
  python scripts/run_crawler_batch.py \
    --config "$CRAWLER_CONFIG_PATH" 2>&1 | log_run || run_status=$?
fi
grep -h "^\[crawler-batch\] Completed" "$LOG_FILE" || true

# Keep only last 30 log files
ls -t "$LOG_DIR"/crawler_*.log 2>/dev/null | tail -n +31 | xargs rm -f || true

exit "$run_status"
