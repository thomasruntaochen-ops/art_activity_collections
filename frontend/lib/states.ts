import { slugify } from "./slug";
// USPS state/territory codes → full display names. The API stores and filters
// by the code; the UI shows the full name.
const STATE_NAMES: Record<string, string> = {
  AL: "Alabama",
  AK: "Alaska",
  AZ: "Arizona",
  AR: "Arkansas",
  CA: "California",
  CO: "Colorado",
  CT: "Connecticut",
  DE: "Delaware",
  DC: "Washington, D.C.",
  FL: "Florida",
  GA: "Georgia",
  HI: "Hawaii",
  ID: "Idaho",
  IL: "Illinois",
  IN: "Indiana",
  IA: "Iowa",
  KS: "Kansas",
  KY: "Kentucky",
  LA: "Louisiana",
  ME: "Maine",
  MD: "Maryland",
  MA: "Massachusetts",
  MI: "Michigan",
  MN: "Minnesota",
  MS: "Mississippi",
  MO: "Missouri",
  MT: "Montana",
  NE: "Nebraska",
  NV: "Nevada",
  NH: "New Hampshire",
  NJ: "New Jersey",
  NM: "New Mexico",
  NY: "New York",
  NC: "North Carolina",
  ND: "North Dakota",
  OH: "Ohio",
  OK: "Oklahoma",
  OR: "Oregon",
  PA: "Pennsylvania",
  RI: "Rhode Island",
  SC: "South Carolina",
  SD: "South Dakota",
  TN: "Tennessee",
  TX: "Texas",
  UT: "Utah",
  VT: "Vermont",
  VA: "Virginia",
  WA: "Washington",
  WV: "West Virginia",
  WI: "Wisconsin",
  WY: "Wyoming",
  AS: "American Samoa",
  GU: "Guam",
  MP: "Northern Mariana Islands",
  PR: "Puerto Rico",
  VI: "U.S. Virgin Islands",
};

// Full name for a state code; unknown/empty values fall back to the raw input.
export function stateName(code: string | null | undefined): string {
  if (!code) return "";
  return STATE_NAMES[code.trim().toUpperCase()] ?? code;
}

// State slugs for the /[state] landing pages. The slug is built from the
// display name ("CA" -> "california", "DC" -> "washington-dc") so URLs read
// naturally; the API still filters by the two-letter code.
export function stateSlug(code: string): string {
  return slugify(stateName(code));
}

export function stateCodeFromSlug(slug: string): string | null {
  const match = Object.keys(STATE_NAMES).find((code) => stateSlug(code) === slug);
  return match ?? null;
}
