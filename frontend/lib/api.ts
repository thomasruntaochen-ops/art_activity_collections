import { Activity, ActivityFilterOptions, ActivityFilters, SuggestField } from "./types";

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

export async function fetchFilterOptions(): Promise<ActivityFilterOptions> {
  const url = `${API_BASE_URL}/api/activities/filter-options`;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Filter options request failed: ${response.status}`);
  }
  return response.json();
}
