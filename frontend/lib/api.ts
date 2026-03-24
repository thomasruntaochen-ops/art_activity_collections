import {
  Activity,
  ActivityFilterOptionFilters,
  ActivityFilterOptions,
  ActivityFilters,
  SuggestField,
  VenueSummary,
} from "./types";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000";

export async function fetchActivities(filters: ActivityFilters): Promise<Activity[]> {
  const params = new URLSearchParams();

  if (filters.age !== undefined) params.set("age", String(filters.age));
  if (filters.drop_in !== undefined) params.set("drop_in", String(filters.drop_in));
  if (filters.venue) params.set("venue", filters.venue);
  if (filters.city) params.set("city", filters.city);
  if (filters.state) params.set("state", filters.state);
  if (filters.date_from) params.set("date_from", filters.date_from);
  if (filters.date_to) params.set("date_to", filters.date_to);

  const query = params.toString();
  const url = `${API_BASE_URL}/api/activities${query ? `?${query}` : ""}`;

  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status}`);
  }
  return response.json();
}

export async function fetchSuggestions(field: SuggestField, q: string, limit = 8): Promise<string[]> {
  const params = new URLSearchParams({
    field,
    q,
    limit: String(limit),
  });
  const url = `${API_BASE_URL}/api/activities/suggestions?${params.toString()}`;

  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Suggestions request failed: ${response.status}`);
  }
  return response.json();
}

export async function fetchFilterOptions(
  filters: ActivityFilterOptionFilters = {},
): Promise<ActivityFilterOptions> {
  const params = new URLSearchParams();
  if (filters.state) params.set("state", filters.state);
  if (filters.city) params.set("city", filters.city);
  const query = params.toString();
  const url = `${API_BASE_URL}/api/activities/filter-options${query ? `?${query}` : ""}`;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Filter options request failed: ${response.status}`);
  }
  return response.json();
}

export async function fetchVenueSummaries(params?: {
  state?: string;
  city?: string;
  date_from?: string;
  date_to?: string;
  limit?: number;
}): Promise<VenueSummary[]> {
  const search = new URLSearchParams();
  if (params?.state) search.set("state", params.state);
  if (params?.city) search.set("city", params.city);
  if (params?.date_from) search.set("date_from", params.date_from);
  if (params?.date_to) search.set("date_to", params.date_to);
  if (params?.limit !== undefined) search.set("limit", String(params.limit));

  const query = search.toString();
  const url = `${API_BASE_URL}/api/activities/venues${query ? `?${query}` : ""}`;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Venue summaries request failed: ${response.status}`);
  }
  return response.json();
}
