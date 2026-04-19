# Art Activity Collection

Pipeline and API to collect kids/teens art activities from museum and art center websites, store them in MySQL with free/paid/unknown price state, and expose searchable data for web/app frontends.

## Stack
- Python 3.11+
- FastAPI
- SQLAlchemy
- MySQL 8
- Optional Playwright for dynamic pages
- Optional LLM extraction fallback

## Project Structure
- `src/api`: FastAPI routes
- `src/core`: settings and shared config
- `src/db`: database engine/session
- `src/models`: SQLAlchemy models
- `src/schemas`: API and ingestion schemas
- `src/services`: domain services
- `src/crawlers`: source adapters + extraction pipeline
- `db`: schema and migrations
- `scripts`: utility scripts (seed/run jobs)
- `RAILWAY_CONFIG.md`: Railway service/variable checklist

## Quick Start
1. Create a virtual environment and install dependencies.
2. Copy `.env.example` to `.env` and update values.
3. Create MySQL database:
Homebrew service:
    brew update
    brew install mysql
    brew services start mysql
Test direct connection: 
    mysql -h 127.0.0.1 -P 3306 -u root -p
Ensure DB exists and schema is loaded (run in project root):
    mysql -h 127.0.0.1 -P 3306 -u root -p -e 'CREATE DATABASE IF NOT EXISTS art_activity_collection;'
    mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/schema_latest.sql
  
4. If you already have an older database and need incremental updates, run migrations in order:
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/001_init.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/002_add_activity_location_text.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/003_add_venue_indexes.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/004_add_activity_query_indexes.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/005_allow_paid_and_unknown_activity_price.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/002_add_activity_location_text.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/003_add_venue_indexes.sql`
   - `mysql -h 127.0.0.1 -P 3306 -u root -p art_activity_collection < db/migrations/004_add_activity_query_indexes.sql`
5. Run API:
   - `uvicorn src.main:app --reload`
6. Run frontend:
   - `cd frontend`
   - `npm install`
   - `cp .env.local.example .env.local`
   - `npm run dev`

## Running Crawlers

Config-driven venue list: `config/crawler_venues.toml` controls enabled venues, parser script path, and `batch_id`. Edit this file to enable/disable venues.

### Local MySQL (`scripts/run_crawler_local.sh`)
Sources `.env` for local MySQL credentials. No remote HTML fallback or Zapier alerts.
```bash
# Run all batches
./scripts/run_crawler_local.sh

# Run a single batch
./scripts/run_crawler_local.sh batch_a
```

### Railway MySQL (`scripts/run_crawler_mac.sh`)
Sources `.env.railway` for Railway MySQL credentials. Includes remote HTML fallback and Zapier alerts.
```bash
# Run all batches
./scripts/run_crawler_mac.sh

# Run a single batch
./scripts/run_crawler_mac.sh batch_a
```

### Useful commands
- Preview selected venues without running: `python3 scripts/run_crawler_batch.py --batch-id batch_a --list-only`
- Safety behavior: parser scripts abort DB writes when total parsed rows are `0` on `--commit`.
- Manual alert test: `python3 scripts/test_crawler_alert.py`
- Full Zapier setup: `ZAPIER_ALERT_SETUP.md`

## Railway Service Layout
- `mysql` service: private database.
- `api` service: FastAPI/uvicorn public endpoint for web + mobile apps.
- `frontend` service: Next.js website.

### `api` service
- Start command:
  - `bash -lc 'uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}'`
- Required vars (project-level or service-level):
  - `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`
  - or `MYSQL_DSN`
- Security vars:
  - `API_ALLOWED_ORIGINS=https://<frontend-domain>`
  - `AUTH_ENABLED=true|false`
  - `AUTH_REQUIRE_ALL_REQUESTS=true|false`
  - `AUTH_ISSUER`, `AUTH_AUDIENCE`, `AUTH_JWKS_URL` (required when `AUTH_ENABLED=true`)
  - `REDIS_URL` (optional but recommended for shared rate limiting)
  - `RATE_LIMIT_GUEST_PER_MINUTE`, `RATE_LIMIT_GUEST_PER_DAY`
  - `RATE_LIMIT_USER_PER_MINUTE`, `RATE_LIMIT_USER_PER_DAY`

### `frontend` service
- Root directory: `frontend`
- Build command:
  - `npm ci && npm run build`
- Start command:
  - `npm run start`
- Required var:
  - `NEXT_PUBLIC_API_BASE_URL=https://<api-domain>`


## Current Status
This scaffold includes:
- Free-only activity schema
- Optional LLM extraction metadata fields
- Basic activity query endpoint
- Crawler adapter interface and hardcoded extractor baseline

## MET Source Parser
- Source URL: `https://www.metmuseum.org/events?audience=teens&price=free&type=workshopsClasses`
- Fetches directly via Playwright (headless browser) to bypass Vercel bot detection.
- Dry run (fetch live and print parsed rows):
  - `python3 scripts/run_met_parser.py --fetch`
- Fetch live and commit to MySQL:
  - `python3 scripts/run_met_parser.py --fetch --commit`
- Parse from saved HTML file (offline fallback):
  - `python3 scripts/run_met_parser.py --input-html data/rawhtml/met/<file>.html`
- Dump normalized text for debugging:
  - `python3 scripts/run_met_parser.py --fetch --dump-text`

## MoMA Source Parser
- Teens URL: `https://www.moma.org/calendar/?happening_filter=For+teens`
- Kids URL: `https://www.moma.org/calendar/?happening_filter=For+kids`
- Remove all existing MoMA entries from DB:
  - `python3 scripts/run_moma_parser.py --clear`
- Parse directly from MoMA URLs (dry run):
  - `python3 scripts/run_moma_parser.py`
- Parse directly from MoMA URLs and commit to MySQL:
  - `python3 scripts/run_moma_parser.py --commit`

## Whitney Source Parser
- URL: `https://whitney.org/events?tags[]=courses_and_workshops&tags[]=teen_events`
- Remove all existing Whitney entries from DB:
  - `python3 scripts/run_whitney_parser.py --clear`
- Parse directly from Whitney URL (dry run):
  - `python3 scripts/run_whitney_parser.py`
- Parse from saved Whitney HTML (offline):
  - `python3 scripts/run_whitney_parser.py --input-html data/html/whitney/<file>.html`
- Parse directly from Whitney URL and commit to MySQL:
  - `python3 scripts/run_whitney_parser.py --commit`

## MFA Boston Source Parser
- Pages: `https://www.mfa.org/programs?page=0` through `https://www.mfa.org/programs?page=4`
- Notes:
  - Parser filters out guided tour events.
  - Parser also filters out events marked `tickets no longer available`.
  - Venue defaults to Boston, MA.
- Remove all existing MFA entries from DB:
  - `python3 scripts/run_mfa_parser.py --clear`
- Parse directly from MFA pages (dry run):
  - `python3 scripts/run_mfa_parser.py`
- Parse a custom page range:
  - `python3 scripts/run_mfa_parser.py --start-page 0 --end-page 4`
- Parse from saved MFA HTML files (offline):
  - `python3 scripts/run_mfa_parser.py --input-html-dir data/html/mfa`
  - expected filenames: `mfa_programs_page_0.html` ... `mfa_programs_page_4.html`
- Parse directly from MFA pages and commit to MySQL:
  - `python3 scripts/run_mfa_parser.py --commit`
