import { useQuery } from "@tanstack/react-query";
import { fetchVenueSummaries } from "../lib/api";
import { deriveDateRange, useFilters } from "../store/filters";

// Loads the venue explorer rows, scoped by the shared filters. The queryKey
// includes every filter so React Query refetches whenever a filter changes.
// Directory-wide museum count for the Explore header tagline. Deliberately
// unfiltered apart from the default "upcoming" window: the tagline describes the
// whole directory, so it must not move when the user narrows the filters. 300 is
// the API's ceiling for this endpoint.
export function useMuseumCount() {
  const { date_from } = deriveDateRange("upcoming");

  return useQuery({
    queryKey: ["museum-count"],
    queryFn: async () => {
      const rows = await fetchVenueSummaries({ date_from, limit: 300 });
      // Same name-merge the venue list uses, so the tagline and the museum list
      // can never disagree on how many museums there are.
      return new Set(rows.map((row) => row.venue_name.trim().toLowerCase())).size;
    },
  });
}

export function useVenues() {
  const state = useFilters((s) => s.state);
  const city = useFilters((s) => s.city);
  const audience = useFilters((s) => s.audience);
  const freeOnly = useFilters((s) => s.freeOnly);
  const rangeKey = useFilters((s) => s.rangeKey);
  const customFrom = useFilters((s) => s.customFrom);
  const customTo = useFilters((s) => s.customTo);
  const { date_from, date_to } = deriveDateRange(rangeKey, customFrom, customTo);

  return useQuery({
    queryKey: ["venues", state, city, audience, freeOnly, rangeKey, customFrom, customTo],
    queryFn: () =>
      fetchVenueSummaries({
        state: state || undefined,
        city: city || undefined,
        audience: audience || undefined,
        free_only: freeOnly,
        date_from,
        date_to,
        limit: 150,
      }),
  });
}
