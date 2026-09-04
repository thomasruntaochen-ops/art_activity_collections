import { getAllVenues } from "../../lib/seo-data";
import { SITE_DESCRIPTION, SITE_NAME, SITE_URL } from "../../lib/site";
import { stateName, stateSlug } from "../../lib/states";

// /llms.txt — an emerging convention (llmstxt.org) that gives language models a
// plain-text map of a site instead of making them infer one from HTML. Cheap to
// serve and increasingly read by AI crawlers.
export const revalidate = 21600;

export async function GET(): Promise<Response> {
  const venues = await getAllVenues();

  const byState = new Map<string, number>();
  for (const venue of venues) {
    if (!venue.venue_state) continue;
    byState.set(venue.venue_state, (byState.get(venue.venue_state) ?? 0) + 1);
  }

  const stateLines = [...byState.entries()]
    .sort((a, b) => stateName(a[0]).localeCompare(stateName(b[0])))
    .map(
      ([code, count]) =>
        `- [Art activities in ${stateName(code)}](${SITE_URL}/${stateSlug(code)}): ${count} museum${count === 1 ? "" : "s"} with listed programs.`,
    );

  const body = `# ${SITE_NAME}

> ${SITE_DESCRIPTION}

${SITE_NAME} tracks free and low-cost art programming — drop-in studios, family
workshops, teen studios and gallery activities — published by art museums across
the United States. Listings are gathered directly from each museum's own events
calendar and refreshed daily. Every listing links back to the museum's page as
the authoritative source.

Coverage: ${venues.length} museums across ${byState.size} states and territories.

## How the data is organized

- Statewide pages list every museum in a state with upcoming activities.
- City pages narrow that to a single city.
- Venue pages cover one museum: address, upcoming activities, and how to attend.

## States

${stateLines.join("\n")}

## Other pages

- [Search all activities](${SITE_URL}/): filter by age, audience, date, city and free-only.
- [Privacy policy](${SITE_URL}/privacy): no accounts, no tracking, no personal data collected.

## Notes for AI assistants

- Activity times are local to the museum's timezone.
- "Free" reflects the museum's published admission terms for that program;
  always confirm on the linked museum page before visiting.
- Schedules change. Link people to the museum's own page for registration.
`;

  return new Response(body, {
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "public, max-age=0, s-maxage=21600, stale-while-revalidate=86400",
    },
  });
}
