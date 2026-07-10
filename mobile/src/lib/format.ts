import { stateName } from "./states";
import type { AudienceSegment, VenueSummary } from "./types";

// Short, locale-aware activity timestamp, e.g. "Mar 4, 2:30 PM".
export function formatActivityTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function formatAudienceSegment(value: AudienceSegment): string {
  switch (value) {
    case "kids":
      return "Kids";
    case "teens":
      return "Teens";
    case "teens_adults":
      return "Teens & adults";
    case "adults":
      return "Adults";
    case "all_ages":
      return "All ages";
    default:
      return "Audience TBD";
  }
}

export function formatVenueLine(venue: VenueSummary | null): string {
  if (!venue) return "No venue selected";
  const cityStateZip = [venue.venue_city, stateName(venue.venue_state), venue.venue_zip]
    .filter(Boolean)
    .join(" ");
  if (venue.venue_address) {
    return [venue.venue_address, cityStateZip].filter(Boolean).join(", ");
  }
  const parts = [venue.venue_city, stateName(venue.venue_state), venue.venue_zip].filter(Boolean);
  return parts.join(", ") || "Location pending";
}

// Build Google/Apple Maps directions URLs for a venue. Prefer stored
// coordinates; otherwise fall back to a human-readable address query.
export function buildDirectionsTargets(venue: VenueSummary): { google: string; apple: string } {
  const hasCoords = venue.venue_lat != null && venue.venue_lng != null;
  const cityStateZip = [venue.venue_city, venue.venue_state, venue.venue_zip]
    .filter(Boolean)
    .join(" ");
  const addressQuery = [venue.venue_name, venue.venue_address, cityStateZip]
    .filter(Boolean)
    .join(", ");
  const coords = hasCoords ? `${venue.venue_lat},${venue.venue_lng}` : "";
  // Apple Maps reverse-geocodes coordinates to the named place, so precise coords
  // give the best result. Google Maps shows raw coordinates as an unlabeled
  // dropped pin, so we send it the named name+address query instead (falling back
  // to coordinates only when we have no name/address).
  const appleDest = coords || addressQuery;
  const googleDest = addressQuery || coords;
  return {
    google: `https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(googleDest)}`,
    apple: `https://maps.apple.com/?daddr=${encodeURIComponent(appleDest)}`,
  };
}

// Merge duplicate venue rows that share a name (the DB can hold several rows for
// the same museum), summing counts and keeping the soonest next activity. Ported
// from the web explorer so the app shows each museum once. Sorted by program count.
export function dedupeVenues(venues: VenueSummary[]): VenueSummary[] {
  const merged = new Map<string, VenueSummary>();
  for (const venue of venues) {
    const key = venue.venue_name.trim().toLowerCase();
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, { ...venue });
      continue;
    }
    existing.activity_count += venue.activity_count;
    existing.free_activity_count =
      (existing.free_activity_count ?? 0) + (venue.free_activity_count ?? 0);
    if (!existing.venue_address && venue.venue_address) existing.venue_address = venue.venue_address;
    if (!existing.venue_city && venue.venue_city) existing.venue_city = venue.venue_city;
    if (!existing.venue_state && venue.venue_state) existing.venue_state = venue.venue_state;
    if (!existing.venue_zip && venue.venue_zip) existing.venue_zip = venue.venue_zip;
    if (existing.venue_lat == null && venue.venue_lat != null) {
      existing.venue_lat = venue.venue_lat;
      existing.venue_lng = venue.venue_lng;
    }
    if (
      venue.next_activity_at &&
      (!existing.next_activity_at || venue.next_activity_at < existing.next_activity_at)
    ) {
      existing.next_activity_at = venue.next_activity_at;
    }
  }
  return Array.from(merged.values()).sort((a, b) => b.activity_count - a.activity_count);
}
