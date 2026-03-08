export type Activity = {
  id: number;
  title: string;
  source_url: string;
  venue_name: string | null;
  location_text: string | null;
  venue_city: string | null;
  venue_state: string | null;
  activity_type: string | null;
  age_min: number | null;
  age_max: number | null;
  drop_in: boolean | null;
  registration_required: boolean | null;
  start_at: string;
  end_at: string | null;
  timezone: string;
  free_verification_status: string;
  extraction_method: string;
  status: string;
  confidence_score: string;
};

export type ActivityFilters = {
  age?: number;
  drop_in?: boolean;
  venue?: string;
  city?: string;
  state?: string;
  date_from?: string;
  date_to?: string;
};

export type SuggestField = "venue" | "city" | "state";

export type ActivityFilterOptions = {
  venues: string[];
  states: string[];
  cities: string[];
};
