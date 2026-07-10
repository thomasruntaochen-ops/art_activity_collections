import { API_BASE_URL } from "../config";
import type {
  Activity,
  ActivityFilterOptionFilters,
  ActivityFilterOptions,
  ActivityFilters,
  SuggestField,
  VenueSummary,
} from "./types";

// Resolve the configured API origin, trimming any trailing slash. Throws a
// friendly message if it hasn't been set yet so the UI can prompt for it.
function baseUrl(): string {
  const url = API_BASE_URL.replace(/\/$/, "");
  if (!url) {
    throw new Error(
      "No data source is set yet. Add your API URL in mobile/src/config.ts (API_BASE_URL).",
    );
  }
  return url;
}

async function getJson<T>(path: string, query?: URLSearchParams): Promise<T> {
  const qs = query?.toString();
  const url = `${baseUrl()}${path}${qs ? `?${qs}` : ""}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export async function fetchActivities(filters: ActivityFilters): Promise<Activity[]> {
  const params = new URLSearchParams();
  if (filters.age !== undefined) params.set("age", String(filters.age));
  if (filters.audience !== undefined) params.set("audience", filters.audience);
  if (filters.drop_in !== undefined) params.set("drop_in", String(filters.drop_in));
  if (filters.venue) params.set("venue", filters.venue);
  if (filters.city) params.set("city", filters.city);
  if (filters.state) params.set("state", filters.state);
  if (filters.date_from) params.set("date_from", filters.date_from);
  if (filters.date_to) params.set("date_to", filters.date_to);
  if (filters.free_only !== undefined) params.set("free_only", String(filters.free_only));
  return getJson<Activity[]>("/api/activities", params);
}

export async function fetchSuggestions(
  field: SuggestField,
  q: string,
  limit = 8,
): Promise<string[]> {
  const params = new URLSearchParams({ field, q, limit: String(limit) });
  return getJson<string[]>("/api/activities/suggestions", params);
}

export async function fetchFilterOptions(
  filters: ActivityFilterOptionFilters = {},
): Promise<ActivityFilterOptions> {
  const params = new URLSearchParams();
  if (filters.state) params.set("state", filters.state);
  if (filters.city) params.set("city", filters.city);
  if (filters.free_only !== undefined) params.set("free_only", String(filters.free_only));
  if (filters.audience !== undefined) params.set("audience", filters.audience);
  return getJson<ActivityFilterOptions>("/api/activities/filter-options", params);
}

export async function fetchVenueSummaries(params?: {
  state?: string;
  city?: string;
  date_from?: string;
  date_to?: string;
  free_only?: boolean;
  audience?: string;
  limit?: number;
}): Promise<VenueSummary[]> {
  const search = new URLSearchParams();
  if (params?.state) search.set("state", params.state);
  if (params?.city) search.set("city", params.city);
  if (params?.date_from) search.set("date_from", params.date_from);
  if (params?.date_to) search.set("date_to", params.date_to);
  if (params?.free_only !== undefined) search.set("free_only", String(params.free_only));
  if (params?.audience !== undefined) search.set("audience", params.audience);
  if (params?.limit !== undefined) search.set("limit", String(params.limit));
  return getJson<VenueSummary[]>("/api/activities/venues", search);
}
