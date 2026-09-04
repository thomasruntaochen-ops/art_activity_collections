// Server-side data access for the statically rendered SEO pages.
//
// This is deliberately separate from lib/api.ts: that module is used by the
// interactive client app and fetches with `cache: "no-store"` so the explorer
// always shows live results. These pages are crawled far more often than they
// change, so they fetch through Next's data cache with a revalidate window.
// That keeps hundreds of landing pages from stampeding the API, which rate
// limits guests to 180 requests/minute.
import type { Activity, AudienceSegment, VenueSummary } from "./types";

const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "").replace(/\/$/, "");

// The crawler refreshes once a day, so these windows are matched to it rather
// than to clock time. Anything shorter just re-queries the API for data that
// cannot have changed — and every server-side render on Railway comes from the
// one frontend container, so it all lands in a single per-IP rate-limit bucket.
const ACTIVITY_TTL_SECONDS = 60 * 60 * 6;
const CATALOG_TTL_SECONDS = 60 * 60 * 24;

class ApiUnavailableError extends Error {
  constructor(path: string, reason: string) {
    super(`Art activity API unavailable for ${path}: ${reason}`);
    this.name = "ApiUnavailableError";
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// A build renders every state at once, which would otherwise fire ~85 requests
// in a single burst. The API rate limits per IP and all server-side rendering
// arrives from the one frontend container, so the burst — not the total volume
// — is what trips the limiter. Capping concurrency spreads the same work under
// the limit instead of spiking through it.
const MAX_CONCURRENT_REQUESTS = 3;
let inFlight = 0;
const waiting: (() => void)[] = [];

async function acquire(): Promise<void> {
  if (inFlight < MAX_CONCURRENT_REQUESTS) {
    inFlight += 1;
    return;
  }
  await new Promise<void>((resolve) => waiting.push(resolve));
  inFlight += 1;
}

function release(): void {
  inFlight -= 1;
  waiting.shift()?.();
}

// Retries on 429, honouring Retry-After. A build prerenders every state, which
// is more unique requests than a restrictive per-minute limit allows in one
// window, so it must be able to wait out more than one window — four attempts
// covers roughly three minutes. Bounded so a genuinely broken API fails the
// deploy instead of hanging it.
const MAX_ATTEMPTS = 4;

async function fetchWithRetry(url: string, revalidate: number): Promise<Response> {
  let response: Response | null = null;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    await acquire();
    try {
      response = await fetch(url, { next: { revalidate } });
    } finally {
      release();
    }
    if (response.status !== 429 || attempt === MAX_ATTEMPTS) return response;

    const retryAfter = Number(response.headers.get("retry-after"));
    const waitSeconds = Number.isFinite(retryAfter) ? Math.min(Math.max(retryAfter, 1), 65) : 5;
    await sleep(waitSeconds * 1000);
  }
  // Unreachable: the loop always returns on its final attempt.
  return response as Response;
}

// `required: true` means the page cannot be rendered correctly without this
// data. Those callers must throw rather than fall back: an empty result would
// otherwise flow into notFound() and bake a 404 into the build for a state that
// really does have museums — far worse than a failed deploy, because a
// published 404 gets the page deindexed.
async function getJson<T>(
  path: string,
  revalidate: number,
  fallback: T,
  options: { required?: boolean } = {},
): Promise<T> {
  const fail = (reason: string): T => {
    if (options.required) throw new ApiUnavailableError(path, reason);
    return fallback;
  };

  if (!API_BASE_URL) return fail("NEXT_PUBLIC_API_BASE_URL is not set");
  try {
    const response = await fetchWithRetry(`${API_BASE_URL}${path}`, revalidate);
    if (!response.ok) return fail(`HTTP ${response.status}`);
    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof ApiUnavailableError) throw error;
    return fail(error instanceof Error ? error.message : "request failed");
  }
}

export type FilterOptions = { venues: string[]; states: string[]; cities: string[] };

export function getFilterOptions(stateCode?: string): Promise<FilterOptions> {
  const query = stateCode ? `?state=${encodeURIComponent(stateCode)}` : "";
  return getJson<FilterOptions>(`/api/activities/filter-options${query}`, CATALOG_TTL_SECONDS, {
    venues: [],
    states: [],
    cities: [],
  });
}

// Every SEO page is a slice of the same two national datasets, so they are
// fetched whole, once, and sliced in memory.
//
// The alternative — a query per state, city and venue — issued ~85 requests
// during a build and one per page at runtime, all from the single frontend
// container that the API rate limits as one guest IP. These two responses are
// small (roughly 60KB and 130KB), and because Next's data cache keys on the
// URL, all ~420 pages share these two cache entries instead of holding one
// each.
//
// Ceiling to watch: the venues endpoint caps `limit` at 300 and there are ~203
// venues today. Past 300 the catalog would silently truncate and pages would go
// missing, so that endpoint needs pagination before the directory grows that
// far.
export function getAllVenues(options: { required?: boolean } = {}): Promise<VenueSummary[]> {
  return getJson<VenueSummary[]>(
    "/api/activities/venues?limit=300",
    CATALOG_TTL_SECONDS,
    [],
    { required: options.required },
  );
}

// The API caps this endpoint at 200 rows after ordering by start time (see
// list_activities in src/services/activity_service.py). The cap applies to the
// whole result set, so activities CANNOT be fetched nationally and sliced in
// memory the way venues are: a national query returns only the 200 soonest
// events in the country, starving every state but the earliest few. Each scope
// therefore queries through the API's own filters.
export const ACTIVITY_RESULT_CAP = 200;

export function getUpcomingActivities(params: {
  state?: string;
  city?: string;
  venue?: string;
  required?: boolean;
} = {}): Promise<Activity[]> {
  // The API floors date_from to the start of the day, so a finer bucket would
  // only mint extra cache keys for identical results. One key per UTC day also
  // lines up with the once-a-day crawler.
  const day = new Date();
  day.setUTCHours(0, 0, 0, 0);

  const search = new URLSearchParams({ date_from: day.toISOString() });
  if (params.state) search.set("state", params.state);
  if (params.city) search.set("city", params.city);
  if (params.venue) search.set("venue", params.venue);

  return getJson<Activity[]>(
    `/api/activities?${search.toString()}`,
    ACTIVITY_TTL_SECONDS,
    [],
    { required: params.required },
  );
}

// Renders a total honestly when it may be the API's cap rather than the true
// count. Printing a flat "200" would assert a number we know might be short.
export function formatActivityTotal(count: number): string {
  return count >= ACTIVITY_RESULT_CAP ? `${ACTIVITY_RESULT_CAP}+` : String(count);
}

// --- in-memory venue slices ---
//
// Safe for venues only: the venues endpoint returns the full catalog in one
// call (203 of a 300 max today), so slicing it locally is exact.

export function venuesInState(venues: VenueSummary[], state: string): VenueSummary[] {
  return venues.filter((venue) => venue.venue_state === state);
}

export function venuesInCity(
  venues: VenueSummary[],
  state: string,
  city: string,
): VenueSummary[] {
  return venues.filter((venue) => venue.venue_state === state && venue.venue_city === city);
}

// Activities sort by start time so the soonest events lead the page — both for
// readers and for the Event structured data.
export function byStartTime(activities: Activity[]): Activity[] {
  return [...activities].sort((a, b) => a.start_at.localeCompare(b.start_at));
}

// Replaces the catalog's all-time counts with counts of the upcoming
// activities the page is already rendering, so the museum grid agrees with the
// activity list beside it. Museums with nothing upcoming are kept: dropping
// them would make pages appear and disappear between crawls.
export function withUpcomingCounts(
  catalog: VenueSummary[],
  activities: Activity[],
): VenueSummary[] {
  const total = new Map<string, number>();
  const free = new Map<string, number>();
  for (const activity of activities) {
    const name = activity.venue_name;
    if (!name) continue;
    total.set(name, (total.get(name) ?? 0) + 1);
    if (activity.is_free) free.set(name, (free.get(name) ?? 0) + 1);
  }
  return catalog.map((venue) => ({
    ...venue,
    activity_count: total.get(venue.venue_name) ?? 0,
    free_activity_count: free.get(venue.venue_name) ?? 0,
  }));
}
