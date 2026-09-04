// JSON-LD builders.
//
// Two audiences read this: Google, which turns Event markup into date-stamped
// event rich results, and AI crawlers (GPTBot, ClaudeBot, PerplexityBot), which
// lean on structured data because it states facts unambiguously instead of
// making them infer meaning from layout.
import { getVenueMedia } from "./venue-media";
import { absoluteUrl, SITE_DESCRIPTION, SITE_NAME, SITE_URL } from "./site";
import { stateName } from "./states";
import type { Activity, AudienceSegment, VenueSummary } from "./types";

type Json = Record<string, unknown>;

// Drops null/undefined/empty entries so the emitted graph never carries keys
// with nothing behind them, which validators flag.
function compact(input: Json): Json {
  return Object.fromEntries(
    Object.entries(input).filter(([, value]) => {
      if (value === null || value === undefined || value === "") return false;
      if (Array.isArray(value) && value.length === 0) return false;
      return true;
    }),
  );
}

// Activity times arrive as naive wall-clock in the venue's timezone
// ("2026-06-10T11:00:00"). schema.org wants a UTC offset and Google uses it to
// place events on a calendar, so resolve the zone's offset at that instant.
export function toIsoWithOffset(naive: string, timeZone: string): string {
  const instant = new Date(`${naive}Z`);
  if (Number.isNaN(instant.getTime())) return naive;
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone,
      timeZoneName: "longOffset",
    }).formatToParts(instant);
    const offset = (parts.find((part) => part.type === "timeZoneName")?.value ?? "").replace(
      /^GMT/,
      "",
    );
    return `${naive}${offset || "Z"}`;
  } catch {
    return naive;
  }
}

function ageRange(activity: Activity): string | null {
  const { age_min: min, age_max: max } = activity;
  if (min !== null && max !== null) return `${min}-${max}`;
  if (min !== null) return `${min}-`;
  if (max !== null) return `0-${max}`;
  return null;
}

const AUDIENCE_TYPES: Record<AudienceSegment, string | null> = {
  kids: "Children",
  teens: "Teenagers",
  teens_adults: "Teenagers and adults",
  adults: "Adults",
  all_ages: "Families",
  unknown: null,
};

// Many venue rows store only "City, ST" in the address column. Emitting that as
// a streetAddress duplicates addressLocality/addressRegion and tells consumers
// a street is known when it isn't, so it is kept only when it adds something.
export function streetAddress(
  raw: string | null | undefined,
  city: string | null,
  state: string | null,
): string | undefined {
  const value = raw?.trim();
  if (!value) return undefined;
  const cityState = [city, state].filter(Boolean).join(", ").toLowerCase();
  if (!cityState) return value;
  const normalized = value.toLowerCase().replace(/\s+/g, " ");
  if (normalized === cityState) return undefined;
  // No street number and nothing beyond the city/state it already repeats.
  if (!/\d/.test(value) && normalized.includes(cityState)) return undefined;
  return value;
}

// "a activity" and "a exhibition" both read as errors; pick the article and
// avoid restating the generic type.
function describeType(activityType: string | null): string {
  const word = activityType?.trim().toLowerCase();
  if (!word || word === "activity") return "an art activity";
  return `${/^[aeiou]/.test(word) ? "an" : "a"} ${word}`;
}

function placeLd(
  venueName: string | null,
  city: string | null,
  state: string | null,
  venue?: VenueSummary,
): Json {
  const media = getVenueMedia(venueName);
  return compact({
    "@type": "Place",
    name: venueName ?? undefined,
    url: media?.website,
    address: compact({
      "@type": "PostalAddress",
      streetAddress: streetAddress(venue?.venue_address, city, state),
      addressLocality: city ?? undefined,
      addressRegion: state ?? undefined,
      postalCode: venue?.venue_zip ?? undefined,
      addressCountry: "US",
    }),
    geo:
      venue?.venue_lat != null && venue?.venue_lng != null
        ? {
            "@type": "GeoCoordinates",
            latitude: venue.venue_lat,
            longitude: venue.venue_lng,
          }
        : undefined,
  });
}

export function buildEventLd(activity: Activity, venue?: VenueSummary): Json {
  const media = getVenueMedia(activity.venue_name);
  const city = activity.venue_city ?? venue?.venue_city ?? null;
  const state = activity.venue_state ?? venue?.venue_state ?? null;
  const where = [city, state].filter(Boolean).join(", ");

  return compact({
    "@type": "Event",
    name: activity.title,
    // A short factual summary gives AI crawlers something quotable and fills
    // Google's event description slot.
    description: [
      activity.title,
      describeType(activity.activity_type),
      activity.venue_name ? `at ${activity.venue_name}` : null,
      where ? `in ${where}` : null,
    ]
      .filter(Boolean)
      .join(" — "),
    startDate: toIsoWithOffset(activity.start_at, activity.timezone),
    endDate: activity.end_at ? toIsoWithOffset(activity.end_at, activity.timezone) : undefined,
    eventStatus: "https://schema.org/EventScheduled",
    eventAttendanceMode: "https://schema.org/OfflineEventAttendanceMode",
    location: placeLd(activity.venue_name, city, state, venue),
    image: media ? absoluteUrl(media.image_path) : undefined,
    url: activity.source_url,
    isAccessibleForFree: activity.is_free ?? undefined,
    // Free events still need an Offer for Google to show a price; omitting it
    // suppresses the rich result even when isAccessibleForFree is set.
    offers: activity.is_free
      ? {
          "@type": "Offer",
          price: "0",
          priceCurrency: "USD",
          availability: "https://schema.org/InStock",
          url: activity.source_url,
        }
      : undefined,
    organizer: activity.venue_name
      ? compact({
          "@type": "Organization",
          name: activity.venue_name,
          url: media?.website,
        })
      : undefined,
    typicalAgeRange: ageRange(activity) ?? undefined,
    audience: AUDIENCE_TYPES[activity.audience_segment]
      ? { "@type": "Audience", audienceType: AUDIENCE_TYPES[activity.audience_segment] }
      : undefined,
  });
}

export function buildMuseumLd(venue: VenueSummary, path: string): Json {
  const media = getVenueMedia(venue.venue_name);
  return compact({
    "@context": "https://schema.org",
    "@type": "Museum",
    name: venue.venue_name,
    url: absoluteUrl(path),
    sameAs: media?.website ? [media.website] : undefined,
    image: media ? absoluteUrl(media.image_path) : undefined,
    address: compact({
      "@type": "PostalAddress",
      streetAddress: streetAddress(venue.venue_address, venue.venue_city, venue.venue_state),
      addressLocality: venue.venue_city ?? undefined,
      addressRegion: venue.venue_state ?? undefined,
      postalCode: venue.venue_zip ?? undefined,
      addressCountry: "US",
    }),
    geo:
      venue.venue_lat != null && venue.venue_lng != null
        ? { "@type": "GeoCoordinates", latitude: venue.venue_lat, longitude: venue.venue_lng }
        : undefined,
    isAccessibleForFree: (venue.free_activity_count ?? 0) > 0 ? true : undefined,
  });
}

// A CollectionPage wrapping an ItemList of Events. This is the shape crawlers
// expect for a "list of things" page, and it lets the events be understood as
// belonging to the page rather than floating free.
export function buildCollectionLd(options: {
  name: string;
  description: string;
  path: string;
  activities: Activity[];
  venuesByName?: Map<string, VenueSummary>;
}): Json {
  const { name, description, path, activities, venuesByName } = options;
  return compact({
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name,
    description,
    url: absoluteUrl(path),
    isPartOf: { "@type": "WebSite", name: SITE_NAME, url: SITE_URL },
    mainEntity: {
      "@type": "ItemList",
      numberOfItems: activities.length,
      itemListElement: activities.map((activity, index) => ({
        "@type": "ListItem",
        position: index + 1,
        item: buildEventLd(activity, venuesByName?.get(activity.venue_name ?? "")),
      })),
    },
  });
}

export function buildBreadcrumbLd(trail: { name: string; path: string }[]): Json {
  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: trail.map((crumb, index) => ({
      "@type": "ListItem",
      position: index + 1,
      name: crumb.name,
      item: absoluteUrl(crumb.path),
    })),
  };
}

export function buildFaqLd(entries: { question: string; answer: string }[]): Json {
  return {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: entries.map((entry) => ({
      "@type": "Question",
      name: entry.question,
      acceptedAnswer: { "@type": "Answer", text: entry.answer },
    })),
  };
}

// Site-wide identity, emitted once from the root layout.
export function buildSiteLd(): Json {
  return {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "WebSite",
        "@id": `${SITE_URL}/#website`,
        name: SITE_NAME,
        description: SITE_DESCRIPTION,
        url: SITE_URL,
        inLanguage: "en-US",
        publisher: { "@id": `${SITE_URL}/#organization` },
      },
      {
        "@type": "Organization",
        "@id": `${SITE_URL}/#organization`,
        name: SITE_NAME,
        url: SITE_URL,
        description: SITE_DESCRIPTION,
        logo: { "@type": "ImageObject", url: absoluteUrl("/icon.svg") },
      },
    ],
  };
}

export function stateLabel(code: string): string {
  return stateName(code);
}
