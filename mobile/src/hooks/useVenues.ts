import { useQuery } from "@tanstack/react-query";
import { fetchVenueSummaries } from "../lib/api";
import { deriveDateRange, useFilters } from "../store/filters";

// Loads the venue explorer rows, scoped by the shared filters. The queryKey
// includes every filter so React Query refetches whenever a filter changes.
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
