# Railway Configuration Reference

This file is the single source of truth for Railway service setup in this repo.

## Service Topology

- `mysql` (private)
- `api` (public FastAPI)
- `crawler` (cron/batch only)
- `frontend` (public Next.js)

Keep all 4 services in the same Railway project/environment.

## Project-Level Variables (shared)

Use Railway reference vars from the MySQL service:

- `MYSQL_HOST=${{MySQL.MYSQLHOST}}`
- `MYSQL_PORT=${{MySQL.MYSQLPORT}}`
- `MYSQL_USER=${{MySQL.MYSQLUSER}}`
- `MYSQL_PASSWORD=${{MySQL.MYSQLPASSWORD}}`
- `MYSQL_DB=${{MySQL.MYSQLDATABASE}}`

Optional:

- `MYSQL_DSN=mysql+pymysql://<user>:<pass>@<host>:<port>/<db>`

## API Service (`api`)

- Root directory: `.`
- Start command:
  - `bash -lc 'uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}'`
- Public domain: enabled

### API vars

- `API_ALLOWED_ORIGINS=https://<frontend-domain>`
  - Comma-separated list of browser origins allowed by CORS.
  - Must include the exact frontend domain (scheme + host, no trailing slash).
  - Add `http://localhost:3000` only for local development.
- `AUTH_ENABLED=false`
  - Master switch for JWT auth parsing/verification.
  - `false`: API accepts guest requests (still rate-limited).
  - `true`: API validates bearer JWT when present.
- `AUTH_REQUIRE_ALL_REQUESTS=false`
  - Controls whether unauthenticated requests are allowed when auth is enabled.
  - `false`: guests allowed, authenticated users get user-tier limits.
  - `true`: missing/invalid bearer token returns `401`.
- `AUTH_ISSUER=`
  - Expected JWT `iss` claim from your auth provider.
  - Required when `AUTH_ENABLED=true`.
- `AUTH_AUDIENCE=`
  - Expected JWT `aud` claim for this API.
  - Required when `AUTH_ENABLED=true`.
- `AUTH_JWKS_URL=`
  - URL for provider JWKS public keys (used to verify token signature).
  - Required when `AUTH_ENABLED=true`.
- `REDIS_URL=`
  - Redis connection string for shared rate limits across multiple API replicas.
  - Leave empty to use in-memory limiter (works but resets on restart/deploy and is per-instance).
- `RATE_LIMIT_GUEST_PER_MINUTE=30`
  - Guest/anonymous burst limit in 60-second window.
  - Applied by client IP when user is not authenticated.
- `RATE_LIMIT_GUEST_PER_DAY=500`
  - Guest/anonymous daily quota (UTC day window).
- `RATE_LIMIT_USER_PER_MINUTE=120`
  - Authenticated user burst limit in 60-second window.
- `RATE_LIMIT_USER_PER_DAY=5000`
  - Authenticated user daily quota (UTC day window).

## Crawler Service (`crawler`)

- Root directory: `.`
- Start command:
  - `bash scripts/run_crawler_guarded.sh`
- Public domain: disabled

### Crawler vars

- `RUN_CRAWLER=false` (safe default)
- `CRAWLER_CONFIG_PATH=config/crawler_venues.toml` (default config file for venue list)
- `CRAWLER_BATCH_ID=` (optional batch selector; runs only matching venues in config)
- `CRAWLER_COMMAND=` (optional manual override; if set, bypasses config-driven runner)
- `CRAWLER_FAIL_FAST=false` (optional; when `true`, stop the batch on the first venue failure)
- `CRAWLER_ALERT_WEBHOOK_URL=` (optional; receives JSON alert when a parser returns zero rows on `--commit`)
- `RAWHTML_BASE_URL=` (optional shared remote base URL for raw HTML, e.g. `<base>/met/latest_events.html`)

Config file:

- `config/crawler_venues.toml`
- Each venue entry includes:
  - `venue`
  - `enabled`
  - `parser_script_name`
  - `batch_id`
  - `use_base_url`
  - optional `local_cache_glob`
  - optional `args`
- Preview a batch selection locally:
  - `python scripts/run_crawler_batch.py --batch-id batch_a --list-only`

For cron-only operation:

- keep `RUN_CRAWLER=false` by default
- set `RUN_CRAWLER=true` in cron trigger
- set `CRAWLER_BATCH_ID` per trigger (for example `batch_a` and `batch_b`) to stagger runs

Safety behavior (all parser scripts):

- When `--commit` is set and total parsed rows are `0`, DB commit is aborted and process exits non-zero.
- If `CRAWLER_ALERT_WEBHOOK_URL` is set, the parser posts an alert payload to that webhook.
- MET parser auto-loads from `RAWHTML_BASE_URL/met/latest_events.html` when `RAWHTML_BASE_URL` is set.
- Batch runner continues to later venues after an individual venue failure, then exits non-zero at the end if any venue failed.
- Set `CRAWLER_FAIL_FAST=true` to restore first-failure abort behavior for debugging.
- Run a manual webhook test from Railway/local:
  - `python scripts/test_crawler_alert.py`

## Frontend Service (`frontend`)

- Root directory: `frontend`
- Build command:
  - `npm ci && npm run build`
- Start command:
  - `npm run start`
- Public domain: enabled

### Frontend vars

- `NEXT_PUBLIC_API_BASE_URL=https://<api-domain>`

## Required One-Time DB Setup

From laptop (using MySQL public endpoint credentials):

```bash
mysql --protocol=TCP --ssl-mode=REQUIRED \
  -h "<HOST>" -P "<PORT>" -u "<USER>" -p"<PASSWORD>" \
  -e "CREATE DATABASE IF NOT EXISTS \`<DB>\`;"

mysql --protocol=TCP --ssl-mode=REQUIRED \
  -h "<HOST>" -P "<PORT>" -u "<USER>" -p"<PASSWORD>" \
  "<DB>" < db/schema_latest.sql
```

## Change Checklist

When API domain changes:

1. Update `NEXT_PUBLIC_API_BASE_URL` in `frontend` service.
2. Update `API_ALLOWED_ORIGINS` in `api` service.
3. Redeploy both `frontend` and `api`.

When frontend domain changes:

1. Update `API_ALLOWED_ORIGINS` in `api`.
2. Redeploy `api`.

When enabling auth:

1. Set `AUTH_ENABLED=true`.
2. Fill `AUTH_ISSUER`, `AUTH_AUDIENCE`, `AUTH_JWKS_URL`.
3. Optionally set `AUTH_REQUIRE_ALL_REQUESTS=true`.
4. Redeploy `api`.

When moving to shared rate limits:

1. Add Railway Redis service.
2. Set `REDIS_URL` in `api`.
3. Redeploy `api`.

## Troubleshooting

`Invalid value for '--port': '$PORT'`:

- Use shell-wrapped start command with expansion:
  - `bash -lc 'uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}'`

`ConnectionRefusedError` to MySQL from service:

1. Verify service is using Railway reference vars, not localhost.
2. Temporarily run `python scripts/check_db_connection.py` as start command.
3. Redeploy and check logged runtime values.

## Common Railway CLI Commands

Install Railway CLI (local laptop):

```bash
# macOS (Homebrew)
brew install railway

# or with npm
npm install -g @railway/cli
```

Auth + link project:

```bash
railway login
railway link
```

Inspect environment/services:

```bash
railway status
railway variables --service crawler
```

View logs:

```bash
railway logs --service crawler
railway logs --service api
```

Restart/redeploy a service:

```bash
railway restart --service crawler
railway redeploy --service crawler
```

Run one-off command in a service container:

```bash
railway run --service crawler -- bash -lc 'python scripts/run_mfa_parser.py --commit'
```

SSH into a running container:

```bash
railway ssh --service crawler
```

Notes:

- If SSH returns "application is not running" or "scaled to zero", start/trigger the service first (`railway restart --service crawler`) and retry quickly.
- Crawler HTML cache is runtime filesystem data at `/app/data/html/...` and is ephemeral unless you mount a Railway Volume and set `--cache-dir` to that mount path.
