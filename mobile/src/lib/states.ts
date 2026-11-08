// USPS state/territory codes → full display names. The API stores and filters
// by the code; the UI shows the full name.
const US_STATE_NAMES: Record<string, string> = {
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

// Canadian province/territory codes (Canada Post). None collide with a USPS
// code, so both share the venue `state` field and the /[state] landing pages.
const PROVINCE_NAMES: Record<string, string> = {
  AB: "Alberta",
  BC: "British Columbia",
  MB: "Manitoba",
  NB: "New Brunswick",
  NL: "Newfoundland and Labrador",
  NS: "Nova Scotia",
  NT: "Northwest Territories",
  NU: "Nunavut",
  ON: "Ontario",
  PE: "Prince Edward Island",
  QC: "Quebec",
  SK: "Saskatchewan",
  YT: "Yukon",
};

const STATE_NAMES: Record<string, string> = { ...US_STATE_NAMES, ...PROVINCE_NAMES };

// ISO 3166-1 alpha-2 country for a state/province code. Unknown codes default
// to the US, where every venue lived before Canada was added.
export function countryCode(code: string | null | undefined): "US" | "CA" {
  return code && code.trim().toUpperCase() in PROVINCE_NAMES ? "CA" : "US";
}

export function isCanadianProvince(code: string | null | undefined): boolean {
  return countryCode(code) === "CA";
}

const CANADIAN_TERRITORY_CODES = new Set(["NT", "NU", "YT"]);

// "48 US states and territories" or, once Canadian venues exist,
// "48 US states and territories and 1 Canadian province".
export function regionCoverageLabel(codes: Iterable<string>): string {
  let us = 0;
  const canadian: string[] = [];
  for (const code of codes) {
    if (isCanadianProvince(code)) canadian.push(code.trim().toUpperCase());
    else us += 1;
  }
  const usLabel = `${us} US ${us === 1 ? "state or territory" : "states and territories"}`;
  if (canadian.length === 0) return usLabel;
  const caNoun =
    canadian.length > 1
      ? "provinces and territories"
      : CANADIAN_TERRITORY_CODES.has(canadian[0])
        ? "territory"
        : "province";
  const caLabel = `${canadian.length} Canadian ${caNoun}`;
  return us === 0 ? caLabel : `${usLabel} and ${caLabel}`;
}

// Full name for a state code; unknown/empty values fall back to the raw input.
export function stateName(code: string | null | undefined): string {
  if (!code) return "";
  return STATE_NAMES[code.trim().toUpperCase()] ?? code;
}
