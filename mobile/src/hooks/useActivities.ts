import { useQuery } from "@tanstack/react-query";
import { fetchActivities } from "../lib/api";
import { deriveDateRange, useFilters } from "../store/filters";

// Activities for a single venue, scoped by the shared audience / free-only /
// date filters (venue is the fixed identifier, so state/city aren't re-applied).
export function useVenueActivities(venueName: string) {
  const audience = useFilters((s) => s.audience);
  const freeOnly = useFilters((s) => s.freeOnly);
  const rangeKey = useFilters((s) => s.rangeKey);
  const customFrom = useFilters((s) => s.customFrom);
  const customTo = useFilters((s) => s.customTo);
  const { date_from, date_to } = deriveDateRange(rangeKey, customFrom, customTo);

  return useQuery({
    queryKey: ["activities", venueName, audience, freeOnly, rangeKey, customFrom, customTo],
    queryFn: () =>
      fetchActivities({
        venue: venueName,
        audience: audience || undefined,
        free_only: freeOnly,
        date_from,
        date_to,
      }),
    enabled: Boolean(venueName),
  });
}
