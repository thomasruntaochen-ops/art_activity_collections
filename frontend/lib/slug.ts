// URL slugs for venues, cities and states.
//
// The rule matches the naming already used by the venue photos in
// `public/venue-photos` (see lib/venue-media.json), so slugs stay consistent
// with assets that were named by hand: "&" becomes "and", accents fold to
// ASCII, and every other non-alphanumeric run collapses to a single hyphen.
export function slugify(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

// Slugs are not stored anywhere, so a slug is resolved back to its display name
// by re-slugifying the candidates the API returns and matching. Callers pass
// the list they already fetched, which keeps this a pure lookup.
export function findBySlug<T>(items: T[], slug: string, name: (item: T) => string): T | undefined {
  return items.find((item) => slugify(name(item)) === slug);
}
