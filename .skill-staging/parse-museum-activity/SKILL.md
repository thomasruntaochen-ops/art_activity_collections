---
name: parse-museum-activity
description: Implement new museum activity parsers in this repo for museums listed in `data/resoures/artmuseums/{STATE}.json`, working in bounded batches by state and museum count. Use when Codex is asked to build parser code for museums that do not yet have a `parser_script`, inspect official `event_links`, wire the parser into the existing MySQL ingestion flow, mark `fail_implement` for museums that cannot be parsed reliably, or record `failed_test_comment` when a new parser dry run parses 0 rows and should be skipped on later passes.
---

# Parse Museum Activity

Implement parser code for a limited batch of museums from a state JSON file, following the same split used by existing parsers: adapter logic under `src/crawlers/adapters/` and a runnable script under `scripts/`.

## Workflow

### 1. Bound the batch first

- Require two inputs: a US state (`CA` or `California`) and a museum count limit.
- Resolve the state to `data/resoures/artmuseums/{STATE}.json`.
- Work on at most the requested number of museums so the task stays within context limits.

Use the helper script to list pending museums:

```bash
python3 ~/.codex/skills/parse-museum-activity/scripts/museum_parser_queue.py \
  pending \
  --state California \
  --limit 3
```

Pending means the museum entry has none of `parser_script`, `fail_implement`, or `failed_test_comment`. If `failed_test_comment` is present, skip that venue on later parse requests unless the user explicitly asks to revisit it.

### 2. Read the existing parser style before writing code

- Read these files before implementing a new parser:
  - `scripts/run_moma_parser.py`
  - `src/crawlers/adapters/moma.py`
  - `scripts/run_whitney_parser.py`
  - `src/crawlers/adapters/whitney.py`
  - `scripts/run_mfa_parser.py`
  - `src/crawlers/adapters/mfa.py`
  - `src/crawlers/pipeline/types.py`
  - `src/crawlers/pipeline/runner.py`
  - `src/crawlers/pipeline/alerts.py`
  - `src/crawlers/extractors/filters.py`
- Match the existing conventions:
  - adapter module in `src/crawlers/adapters/<slug>.py`
  - runner script in `scripts/run_<slug>_parser.py`
  - `ExtractedActivity` output
  - dry-run printing plus optional `--commit`
  - `abort_commit_on_empty_parse(...)` before DB writes
  - `upsert_extracted_activities_with_stats(...)` for MySQL persistence

### 3. Inspect the museum event links with the search tool

- Read the museum's `event_links` from the state JSON first.
- Use the search tool and official pages to understand whether the site is:
  - server-rendered HTML
  - a calendar grid
  - a JSON/Next.js payload page
  - a listing page that links to detail pages
- Prefer official museum pages over secondary mirrors.
- If the page hides all required data behind opaque APIs, anti-bot walls, or scripts you cannot reliably reproduce, do not fake it. Mark `fail_implement` with a concrete reason.

### 4. Implement the parser in the repo's style

- Put page-fetch and parse logic in `src/crawlers/adapters/<slug>.py`.
- Put command-line orchestration in `scripts/run_<slug>_parser.py`.
- Reuse the existing structure where practical:
  - constants for venue name, city, state, timezone, source URLs
  - async fetch helpers with retries for live pages
  - optional HTML cache or text dump helpers when useful
  - JSON-first parsing, then DOM fallback
  - source-specific cleanup helpers only if a scoped `--clear` is safe
- If the museum has multiple relevant event pages, support them the way `run_moma_parser.py` supports multiple audiences.
- If the site uses a calendar, parse the calendar rather than scraping only visible teaser cards. Fresno Art Museum is a good example of a page where calendar parsing matters.

### 5. Apply the activity filters explicitly

- Drop events primarily about:
  - ticket or tickets
  - tour
  - registration
  - camp
  - free night
  - fundraising
  - admission, including free admission
  - exhibition
  - film
  - TV
  - reading
  - writing
  - open house
  - performance
  - music
- Prefer events clearly about:
  - talk
  - class
  - lecture
  - activity
  - workshop
  - lab
  - conversation
- Use title, description, and category text together.
- Let exclusion win when the event's primary purpose is disallowed.
- Let inclusion win when an excluded word appears only in boilerplate but the event itself is clearly a talk, class, workshop, lab, or conversation.

### 6. Handle partial data without inventing details

- Never invent age ranges. Use `age_min=None` and `age_max=None` when age is unknown.
- Never invent an end time. Use `end_at=None` when the page does not provide one.
- The current `ExtractedActivity` schema requires `start_at`, so truly empty dates cannot be stored as-is.
- If the page provides a date but no time, use local midnight for that date and preserve the raw time text in `description` when useful.
- If the page does not provide any reliable date at all, skip that item. If this affects the whole source, mark the museum `fail_implement` and explain why.

### 7. Wire integration points only when the parser is viable

- Add the new runner script to `config/crawler_venues.toml` only after a non-empty dry run succeeds.
- Add or update `src/crawlers/sources/registry.yaml` when the source should be known to the crawler registry.
- Keep new venues disabled by default if the parser still needs manual review.
- If the dry run parses 0 rows, do not add the parser script to the JSON, registry, or crawler venue config.

### 8. Validate before marking success

- Run the new parser in dry-run mode first.
- Confirm it emits non-empty parsed rows and that the rows look aligned with the inclusion and exclusion rules.
- If the dry run parses 0 rows, treat it as a failed test rather than a successful implementation. Record `failed_test_comment` with the concrete zero-row reason and leave `parser_script` unset.
- Only then run with `--commit` if the task explicitly includes DB writes and the environment is configured.

### 9. Update the state JSON

- On success, record the runner script path under `parser_script`.
- On implementation failure, record `fail_implement` with the specific blocking reason.
- On test failure where the new parser dry run parses 0 rows and the parser is not added, record `failed_test_comment` with the specific reason.
- Use the helper script so the JSON update is deterministic:

```bash
python3 ~/.codex/skills/parse-museum-activity/scripts/museum_parser_queue.py \
  mark-success \
  --state CA \
  --museum "Fresno Art Museum" \
  --parser-script "scripts/run_fresno_art_museum_parser.py"
```

```bash
python3 ~/.codex/skills/parse-museum-activity/scripts/museum_parser_queue.py \
  mark-failure \
  --state CA \
  --museum "Some Museum" \
  --reason "Calendar data is rendered behind an authenticated API with no stable HTML or JSON payload."
```

```bash
python3 ~/.codex/skills/parse-museum-activity/scripts/museum_parser_queue.py \
  mark-test-failure \
  --state CA \
  --museum "Some Museum" \
  --comment "Dry run parsed 0 rows because the listing endpoint returned only expired items and no event detail page exposed upcoming workshop dates."
```

## Quality Bar

- Follow the existing parser code style instead of introducing a new framework.
- Keep source-specific heuristics in the adapter file, not scattered across the repo.
- Avoid broad refactors while implementing a museum batch.
- Stop after the requested museum count even if more museums remain pending.
- Make failure reasons concrete enough that a later pass can decide whether to retry.

## Example Requests

- `Use $parse-museum-activity for CA and implement 2 museums.`
- `Use $parse-museum-activity for California and handle the next 3 unimplemented museums.`
