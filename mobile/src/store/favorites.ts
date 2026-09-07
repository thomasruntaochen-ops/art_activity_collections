import AsyncStorage from "@react-native-async-storage/async-storage";
import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import { recordValueMoment } from "../lib/review";
import type { Activity, VenueSummary } from "../lib/types";

type FavoritesState = {
  // Keyed by venue_name / String(activity.id). Activities keep the full
  // snapshot so a saved item still renders after it leaves the API window.
  savedVenues: Record<string, VenueSummary>;
  savedActivities: Record<string, Activity>;
  toggleVenue: (venue: VenueSummary) => void;
  toggleActivity: (activity: Activity) => void;
};

// Persisted on-device favorites — no account needed. Survives app restarts via
// AsyncStorage; cleared only when the app is deleted.
export const useFavorites = create<FavoritesState>()(
  persist(
    (set, get) => ({
      savedVenues: {},
      savedActivities: {},
      toggleVenue: (venue) =>
        set((state) => {
          const next = { ...state.savedVenues };
          if (next[venue.venue_name]) delete next[venue.venue_name];
          else next[venue.venue_name] = venue;
          return { savedVenues: next };
        }),
      toggleActivity: (activity) => {
        const key = String(activity.id);
        const wasSaved = Boolean(get().savedActivities[key]);
        set((state) => {
          const next = { ...state.savedActivities };
          if (next[key]) delete next[key];
          else next[key] = activity;
          return { savedActivities: next };
        });
        // Saving counts as a value moment; un-saving obviously does not.
        if (!wasSaved) void recordValueMoment();
      },
    }),
    {
      name: "favorites",
      storage: createJSONStorage(() => AsyncStorage),
    },
  ),
);

export function useIsVenueSaved(venueName: string): boolean {
  return useFavorites((s) => Boolean(s.savedVenues[venueName]));
}

export function useIsActivitySaved(activityId: number): boolean {
  return useFavorites((s) => Boolean(s.savedActivities[String(activityId)]));
}
