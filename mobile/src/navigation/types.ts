import type { VenueSummary } from "../lib/types";

export type RootStackParamList = {
  Tabs: undefined;
  VenueDetail: { venue: VenueSummary };
  Filters: undefined;
  SelectOption: { field: "state" | "city"; title: string };
  Map: undefined;
  NearMe: undefined;
  Disclaimer: undefined;
};

export type TabParamList = {
  ExploreTab: undefined;
  SavedTab: undefined;
};
