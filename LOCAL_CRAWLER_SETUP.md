# Local Mac Crawler Setup

Run the crawler on your Mac via launchd cron jobs, writing directly to Railway MySQL over its public TCP endpoint. This eliminates the Railway `crawler` service entirely.

## Prerequisites

- Python environment set up (see `CLAUDE.md`)
- Railway MySQL public endpoint available (`nozomi.proxy.rlwy.net:41048`)
- Railway MySQL credentials from Railway dashboard → MySQL service → Variables

## 1. Create `.env.railway`

Copy the example and fill in your Railway MySQL password:

```bash
cp .env.railway.example .env.railway
```

Edit `.env.railway` — it is gitignored and never committed:

```
MYSQL_HOST=nozomi.proxy.rlwy.net
MYSQL_PORT=41048
MYSQL_USER=root
MYSQL_PASSWORD=<your MYSQLPASSWORD from Railway>
MYSQL_DB=railway
```

## 2. Test a batch manually

```bash
source ~/.zshrc && bash scripts/run_crawler_mac.sh batch_e
```

Check output in `logs/crawler_batch_e_*.log`. Confirm rows were written to Railway MySQL.

## 3. Load launchd agents

Run once — persists across reboots:

```bash
for f in a b c d e; do
  launchctl load ~/Library/LaunchAgents/com.artactivity.crawler.batch_${f}.plist
done
```

Verify they are registered:

```bash
launchctl list | grep artactivity
```

## 4. Delete the Railway `crawler` service

Once local runs are confirmed working, remove the `crawler` service from Railway to stop billing for it.

## Schedule

| Batch | Venues | Schedule |
|-------|--------|----------|
| batch_a | 18 | Mon + Thu 2:00am |
| batch_b | 18 | Mon + Thu 3:00am |
| batch_c | 20 | Tue + Fri 2:00am |
| batch_d | 72 | Wed + Sat 2:00am |
| batch_e | 7  | Tue + Fri 3:00am |

## Managing launchd agents

**Unload (disable) an agent:**
```bash
launchctl unload ~/Library/LaunchAgents/com.artactivity.crawler.batch_a.plist
```

**Reload after editing a plist:**
```bash
launchctl unload ~/Library/LaunchAgents/com.artactivity.crawler.batch_a.plist
launchctl load   ~/Library/LaunchAgents/com.artactivity.crawler.batch_a.plist
```

**Trigger a batch immediately (for testing):**
```bash
launchctl start com.artactivity.crawler.batch_e
```

**Check last exit code:**
```bash
launchctl list com.artactivity.crawler.batch_e
```

## Logs

Logs are written to `logs/` in the project directory. Each run appends a timestamped file:

```
logs/crawler_batch_a_2026-04-03T02:00:00.log
logs/launchd_batch_a.log   ← stdout/stderr from launchd
```

The script keeps only the last 30 log files per batch automatically.

## Notes

- launchd **does not catch up** missed runs if the Mac was asleep. The job is simply skipped until the next scheduled time.
- To ensure jobs run: enable **Wake for network access** in System Settings → Battery → Options, or schedule batches during hours the Mac is reliably awake.
- To run a batch against local MySQL instead of Railway, override the env vars before calling the script:
  ```bash
  MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_PASSWORD= bash scripts/run_crawler_mac.sh batch_e
  ```
