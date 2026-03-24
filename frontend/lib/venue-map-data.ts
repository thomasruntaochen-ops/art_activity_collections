import { VenueSummary } from "./types";

type Coordinates = {
  lat: number;
  lng: number;
};

export type ResolvedVenueCoordinates = VenueSummary & {
  resolvedLat: number;
  resolvedLng: number;
  usesStoredCoordinates: boolean;
};

const CITY_COORDINATES: Record<string, Coordinates> = {
  "Bakersfield, CA": { lat: 35.3733, lng: -119.0187 },
  "Berkeley, CA": { lat: 37.8715, lng: -122.273 },
  "Costa Mesa, CA": { lat: 33.6638, lng: -117.9047 },
  "Fresno, CA": { lat: 36.7378, lng: -119.7871 },
  "Los Angeles, CA": { lat: 34.0522, lng: -118.2437 },
  "Palm Springs, CA": { lat: 33.8303, lng: -116.5453 },
  "Pasadena, CA": { lat: 34.1478, lng: -118.1445 },
  "Sacramento, CA": { lat: 38.5816, lng: -121.4944 },
  "San Diego, CA": { lat: 32.7157, lng: -117.1611 },
  "San Jose, CA": { lat: 37.3382, lng: -121.8863 },
  "Santa Barbara, CA": { lat: 34.4208, lng: -119.6982 },
  "Fairfield, CT": { lat: 41.1408, lng: -73.2613 },
  "Greenwich, CT": { lat: 41.0262, lng: -73.6282 },
  "Hartford, CT": { lat: 41.7658, lng: -72.6734 },
  "New Haven, CT": { lat: 41.3083, lng: -72.9279 },
  "New London, CT": { lat: 41.3557, lng: -72.0995 },
  "Norwich, CT": { lat: 41.5243, lng: -72.0759 },
  "Old Lyme, CT": { lat: 41.3181, lng: -72.3306 },
  "Ridgefield, CT": { lat: 41.2815, lng: -73.4992 },
  "Storrs, CT": { lat: 41.8084, lng: -72.2495 },
  "Westport, CT": { lat: 41.1415, lng: -73.3579 },
  "Washington, DC": { lat: 38.9072, lng: -77.0369 },
  "Boston, MA": { lat: 42.3601, lng: -71.0589 },
  "Cambridge, MA": { lat: 42.3736, lng: -71.1097 },
  "Dennis, MA": { lat: 41.7354, lng: -70.1936 },
  "Fitchburg, MA": { lat: 42.5834, lng: -71.8023 },
  "Framingham, MA": { lat: 42.2793, lng: -71.4162 },
  "New Bedford, MA": { lat: 41.6362, lng: -70.9342 },
  "North Adams, MA": { lat: 42.7009, lng: -73.1087 },
  "Salem, MA": { lat: 42.5195, lng: -70.8967 },
  "Waltham, MA": { lat: 42.3765, lng: -71.2356 },
  "Williamstown, MA": { lat: 42.712, lng: -73.2037 },
  "Worcester, MA": { lat: 42.2626, lng: -71.8023 },
  "Baltimore, MD": { lat: 39.2904, lng: -76.6122 },
  "Easton, MD": { lat: 38.7743, lng: -76.0763 },
  "Hagerstown, MD": { lat: 39.6418, lng: -77.72 },
  "Atlantic City, NJ": { lat: 39.3643, lng: -74.4229 },
  "Hamilton, NJ": { lat: 40.2171, lng: -74.7429 },
  "New Brunswick, NJ": { lat: 40.4862, lng: -74.4518 },
  "Newark, NJ": { lat: 40.7357, lng: -74.1724 },
  "Princeton, NJ": { lat: 40.3573, lng: -74.6672 },
  "Buffalo, NY": { lat: 42.8864, lng: -78.8784 },
  "Katonah, NY": { lat: 41.2587, lng: -73.6857 },
  "Long Island City, NY": { lat: 40.7447, lng: -73.9485 },
  "New York, NY": { lat: 40.7128, lng: -74.006 },
  "Syracuse, NY": { lat: 43.0481, lng: -76.1474 },
  "Water Mill, NY": { lat: 40.9168, lng: -72.3468 },
  "Austin, TX": { lat: 30.2672, lng: -97.7431 },
  "Fort Worth, TX": { lat: 32.7555, lng: -97.3308 },
  "San Antonio, TX": { lat: 29.4241, lng: -98.4936 },
  "Abingdon, VA": { lat: 36.7098, lng: -81.9773 },
  "Arlington, VA": { lat: 38.8816, lng: -77.091 },
  "Charlottesville, VA": { lat: 38.0293, lng: -78.4767 },
  "Dayton, VA": { lat: 38.414, lng: -78.9395 },
  "Martinsville, VA": { lat: 36.6915, lng: -79.8725 },
  "Norfolk, VA": { lat: 36.8508, lng: -76.2859 },
  "Richmond, VA": { lat: 37.5407, lng: -77.436 },
  "Roanoke, VA": { lat: 37.2709, lng: -79.9414 },
  "Virginia Beach, VA": { lat: 36.8529, lng: -75.978 },
  "Williamsburg, VA": { lat: 37.2707, lng: -76.7075 },
};

const STATE_CENTERS: Record<string, Coordinates> = {
  CA: { lat: 36.7783, lng: -119.4179 },
  CT: { lat: 41.6032, lng: -73.0877 },
  DC: { lat: 38.9072, lng: -77.0369 },
  MA: { lat: 42.4072, lng: -71.3824 },
  MD: { lat: 39.0458, lng: -76.6413 },
  NJ: { lat: 40.0583, lng: -74.4057 },
  NY: { lat: 42.9134, lng: -75.5963 },
  TX: { lat: 31.9686, lng: -99.9018 },
  VA: { lat: 37.4316, lng: -78.6569 },
};

function hashString(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

function applyJitter(base: Coordinates, seed: string, magnitude: number): Coordinates {
  const hash = hashString(seed);
  const latOffset = (((hash % 1000) / 1000) - 0.5) * magnitude;
  const lngOffset = ((((hash >> 10) % 1000) / 1000) - 0.5) * magnitude * 1.25;
  return {
    lat: base.lat + latOffset,
    lng: base.lng + lngOffset,
  };
}

export function resolveVenueCoordinates(venue: VenueSummary): ResolvedVenueCoordinates {
  if (venue.venue_lat !== null && venue.venue_lng !== null) {
    return {
      ...venue,
      resolvedLat: venue.venue_lat,
      resolvedLng: venue.venue_lng,
      usesStoredCoordinates: true,
    };
  }

  const cityKey = venue.venue_city && venue.venue_state ? `${venue.venue_city}, ${venue.venue_state}` : null;
  if (cityKey && CITY_COORDINATES[cityKey]) {
    const jittered = applyJitter(CITY_COORDINATES[cityKey], venue.venue_name, 0.12);
    return {
      ...venue,
      resolvedLat: jittered.lat,
      resolvedLng: jittered.lng,
      usesStoredCoordinates: false,
    };
  }

  const stateFallback = venue.venue_state ? STATE_CENTERS[venue.venue_state] : null;
  const jittered = applyJitter(stateFallback ?? { lat: 39.8283, lng: -98.5795 }, venue.venue_name, 0.8);
  return {
    ...venue,
    resolvedLat: jittered.lat,
    resolvedLng: jittered.lng,
    usesStoredCoordinates: false,
  };
}

