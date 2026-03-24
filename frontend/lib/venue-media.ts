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
