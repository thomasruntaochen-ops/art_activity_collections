import type { MetadataRoute } from "next";

import { getAllVenues } from "../lib/seo-data";
import { absoluteUrl } from "../lib/site";
import { slugify } from "../lib/slug";
import { stateSlug } from "../lib/states";

// Rebuilt on the same cadence as the venue catalog it is derived from.
export const revalidate = 21600;

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const now = new Date();

  const staticEntries: MetadataRoute.Sitemap = [
    { url: absoluteUrl("/"), lastModified: now, changeFrequency: "daily", priority: 1 },
    { url: absoluteUrl("/privacy"), lastModified: now, changeFrequency: "yearly", priority: 0.2 },
  ];

  // One request covers the whole tree: every venue row carries its city and
  // state, so states, cities and venues all fall out of this single call
  // rather than one lookup per state.
  const venues = await getAllVenues();

  const stateCodes = new Set<string>();
  const cityKeys = new Set<string>();
  const venueEntries: MetadataRoute.Sitemap = [];

  for (const venue of venues) {
    const state = venue.venue_state;
    if (state) stateCodes.add(state);
    if (state && venue.venue_city) cityKeys.add(`${state}|${venue.venue_city}`);

    venueEntries.push({
      url: absoluteUrl(`/venue/${slugify(venue.venue_name)}`),
      lastModified: now,
      changeFrequency: "weekly",
      priority: 0.7,
    });
  }

  const stateEntries: MetadataRoute.Sitemap = [...stateCodes].map((code) => ({
    url: absoluteUrl(`/${stateSlug(code)}`),
    lastModified: now,
    changeFrequency: "daily",
    priority: 0.8,
  }));

  const cityEntries: MetadataRoute.Sitemap = [...cityKeys].map((key) => {
    const [code, city] = key.split("|");
    return {
      url: absoluteUrl(`/${stateSlug(code)}/${slugify(city)}`),
      lastModified: now,
      changeFrequency: "daily",
      priority: 0.6,
    };
  });

  // Sitemap entries must be fully-qualified URLs; unlike page metadata they
  // are not resolved against metadataBase.
  return [...staticEntries, ...stateEntries, ...cityEntries, ...venueEntries];
}
