---
name: find-art-museums
description: Find official art museums in a US state, identify each museum's public event or activity listing pages, and update `data/resoures/artmuseums/{STATE}.json` incrementally. Use when Codex is asked to discover art museums by state, collect official museum event/calendar/program URLs, or extend an existing state JSON without reprocessing museums already saved.
---

# Find Art Museums

Use the search tool to discover official art museum sites for a US state and save verified museum event-page links into the repo's per-state JSON file.

## Workflow

### 1. Normalize the target

- Convert the state input to uppercase postal format such as `NY` or `CA`.
- Write to `data/resoures/artmuseums/{STATE}.json`.
- Keep the repo's existing directory spelling `resoures` even if the request says `resources` or `resourses`.

### 2. Read the existing state file first

- If `data/resoures/artmuseums/{STATE}.json` exists, load it before searching.
- Treat top-level keys as museums that are already covered.
- Do not search again for museums already present unless the user explicitly asks for a refresh.

### 3. Discover candidate museums

- Use the search tool, not terminal network commands.
- Start with broad discovery queries, then tighten to official domains.
- Good discovery queries:
  - `art museums in NY official site`
  - `site:*.org "art museum" "NY"`
  - `site:*.org "museum of art" "NY"`
  - `site:*.org "contemporary art museum" "NY"`
- Use secondary sources only to find names. Verify every saved museum against its official website before adding it.
- Exclude clear non-matches such as science museums, history museums, children's museums, commercial galleries, and event venues that are not art museums.

### 4. Find the right activity page

- Search each museum name with terms such as `events`, `calendar`, `programs`, `classes`, `workshops`, `family`, `kids`, and `teens`.
- Save only stable public listing pages that represent ongoing programming.
- Prefer URLs such as `/events`, `/calendar`, `/programs`, `/visit/calendar`, or official filtered listings for families, kids, or teens.
- Save multiple links only when the museum genuinely separates programming across multiple official pages.
- Do not save:
  - museum homepages
  - donate or membership pages
  - ticket checkout pages
  - press releases
  - single event detail pages when a stable listing page exists
- If no credible event or activity listing page can be found, skip that museum instead of guessing.

### 5. Build entries

- Use the official museum name as the JSON key.
- Capture the city from the official museum site when possible.
- Store only `http` or `https` URLs in `event_links`.
- Use this JSON shape:

```json
{
  "Museum Name": {
    "city": "City Name",
    "event_links": [
      "https://museum.example.org/events"
    ]
  }
}
```

### 6. Merge safely

- Create a temporary JSON file containing only the new museums discovered in this run.
- Merge it into the state file with `scripts/upsert_state_file.py`.
- Run the helper from the installed skill directory, for example:

```bash
python3 ~/.codex/skills/find-art-museums/scripts/upsert_state_file.py \
  --state NY \
  --incoming /tmp/ny_new_museums.json
```

- The helper creates `data/resoures/artmuseums` if needed and skips museum keys that already exist in the target file.

## Quality Bar

- Prefer official museum capitalization and naming over abbreviations.
- Deduplicate museums that share the same institution under slightly different spellings.
- Deduplicate `event_links` while preserving only official URLs.
- Keep entries minimal: only `city` and `event_links`.
- Leave the state file as valid, pretty-printed JSON with stable ordering.

## Example Requests

- `Use $find-art-museums to collect art museums in NY and save their event pages.`
- `Use $find-art-museums to extend CA.json with museums not already listed.`
