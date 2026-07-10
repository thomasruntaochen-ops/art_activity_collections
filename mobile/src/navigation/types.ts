import type { VenueSummary } from "../lib/types";

export type RootStackParamList = {
  Explore: undefined;
  VenueDetail: { venue: VenueSummary };
  Filters: undefined;
  SelectOption: { field: "state" | "city"; title: string };
  Map: undefined;
  NearMe: undefined;
};
