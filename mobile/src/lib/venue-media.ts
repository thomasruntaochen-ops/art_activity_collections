import { MEDIA_BASE_URL } from "../config";
import venueMedia from "./venue-media.json";

type VenueMediaEntry = {
  image_path: string;
  image_source_url: string;
  website: string;
};

export function getVenueMedia(venueName: string | null | undefined): VenueMediaEntry | null {
  if (!venueName) return null;
  return (venueMedia as Record<string, VenueMediaEntry>)[venueName] ?? null;
}

// Best available image URL for a venue card. Prefer our own hosted copy when a
// MEDIA_BASE_URL is configured; otherwise fall back to the venue's original
// source image so photos still appear without extra hosting.
export function getVenueImageUri(venueName: string | null | undefined): string | null {
  const media = getVenueMedia(venueName);
  if (!media) return null;
  if (MEDIA_BASE_URL) {
    return `${MEDIA_BASE_URL.replace(/\/$/, "")}${media.image_path}`;
  }
  return media.image_source_url || null;
}
