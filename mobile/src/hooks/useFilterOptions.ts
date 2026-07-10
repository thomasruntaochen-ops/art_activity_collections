import { useQuery } from "@tanstack/react-query";
import { fetchFilterOptions } from "../lib/api";
import { useFilters } from "../store/filters";

// Available states/cities/venues for the pickers. Pass a state to scope the
// city list to that state (mirrors the web explorer's cascading behavior).
export function useFilterOptions(stateForCities: string) {
  const freeOnly = useFilters((s) => s.freeOnly);
  const audience = useFilters((s) => s.audience);
  return useQuery({
    queryKey: ["filter-options", stateForCities, freeOnly, audience],
    queryFn: () =>
      fetchFilterOptions({
        state: stateForCities || undefined,
        free_only: freeOnly,
        audience: audience || undefined,
      }),
  });
}
