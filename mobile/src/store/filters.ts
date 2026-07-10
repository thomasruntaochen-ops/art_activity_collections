import { create } from "zustand";
import type { AudienceSegment } from "../lib/types";

// Date presets offered in the Filters screen. "upcoming" is the default (from
// now on, no end date); "custom" uses the user-picked customFrom/customTo dates.
export type RangeKey = "upcoming" | "today" | "7d" | "30d" | "90d" | "custom";

export const RANGE_LABELS: Record<RangeKey, string> = {
  upcoming: "Upcoming",
  today: "Today",
  "7d": "Next 7 days",
  "30d": "Next 30 days",
  "90d": "Next 3 months",
  custom: "Custom dates",
};

type FiltersState = {
  state: string; // "" = all states
  city: string; // "" = all cities
  audience: "" | AudienceSegment;
  freeOnly: boolean;
  rangeKey: RangeKey;
  customFrom: string | null; // ISO date, used when rangeKey === "custom"
  customTo: string | null;
  // "Near me" is client-side (not sent to the API): the user's location plus a
  // radius in miles. Active when both userLocation and radiusMiles are set.
  userLocation: { lat: number; lng: number } | null;
  radiusMiles: number | null;
  setState: (v: string) => void;
  setCity: (v: string) => void;
  setAudience: (v: "" | AudienceSegment) => void;
  setFreeOnly: (v: boolean) => void;
  setRange: (v: RangeKey) => void;
  setCustomFrom: (iso: string) => void;
  setCustomTo: (iso: string) => void;
  setUserLocation: (loc: { lat: number; lng: number } | null) => void;
  setRadiusMiles: (miles: number | null) => void;
  clearNearMe: () => void;
  reset: () => void;
};

// Shared filter state, read by the venue list, the detail screen, the map, and
// the Filters screen alike (the native counterpart to the web explorer's filter
// state living in one component).
export const useFilters = create<FiltersState>((set) => ({
  state: "",
  city: "",
  audience: "",
  freeOnly: false,
  rangeKey: "upcoming",
  customFrom: null,
  customTo: null,
  userLocation: null,
  radiusMiles: null,
  // Changing state clears city, since the chosen city may not belong to it.
  setState: (v) => set({ state: v, city: "" }),
  setCity: (v) => set({ city: v }),
  setAudience: (v) => set({ audience: v }),
  setFreeOnly: (v) => set({ freeOnly: v }),
  setRange: (v) => set({ rangeKey: v }),
  // Picking a custom date also switches the active range to "custom".
  setCustomFrom: (iso) => set({ customFrom: iso, rangeKey: "custom" }),
  setCustomTo: (iso) => set({ customTo: iso, rangeKey: "custom" }),
  setUserLocation: (loc) => set({ userLocation: loc }),
  setRadiusMiles: (miles) => set({ radiusMiles: miles }),
  clearNearMe: () => set({ radiusMiles: null }),
  reset: () =>
    set({
      state: "",
      city: "",
      audience: "",
      freeOnly: false,
      rangeKey: "upcoming",
      customFrom: null,
      customTo: null,
      radiusMiles: null,
    }),
}));

// Translate the active range into the API's date_from / date_to ISO strings.
export function deriveDateRange(
  rangeKey: RangeKey,
  customFrom?: string | null,
  customTo?: string | null,
): { date_from?: string; date_to?: string } {
  const now = new Date();
  const iso = (d: Date) => d.toISOString();

  if (rangeKey === "custom") {
    const out: { date_from?: string; date_to?: string } = {};
    if (customFrom) {
      const d = new Date(customFrom);
      d.setHours(0, 0, 0, 0);
      out.date_from = iso(d);
    }
    if (customTo) {
      const d = new Date(customTo);
      d.setHours(23, 59, 59, 999);
      out.date_to = iso(d);
    }
    if (!out.date_from && !out.date_to) return { date_from: iso(now) };
    return out;
  }

  if (rangeKey === "today") {
    const start = new Date(now);
    start.setHours(0, 0, 0, 0);
    const end = new Date(now);
    end.setHours(23, 59, 59, 999);
    return { date_from: iso(start), date_to: iso(end) };
  }

  const days = rangeKey === "7d" ? 7 : rangeKey === "30d" ? 30 : rangeKey === "90d" ? 90 : 0;
  if (days === 0) return { date_from: iso(now) }; // "upcoming"
  const end = new Date(now);
  end.setDate(end.getDate() + days);
  return { date_from: iso(now), date_to: iso(end) };
}

// Number of non-default filters, shown as a badge on the Filters button.
export function activeFilterCount(s: FiltersState): number {
  return (
    (s.state ? 1 : 0) +
    (s.city ? 1 : 0) +
    (s.audience ? 1 : 0) +
    (s.freeOnly ? 1 : 0) +
    (s.rangeKey !== "upcoming" ? 1 : 0)
  );
}
