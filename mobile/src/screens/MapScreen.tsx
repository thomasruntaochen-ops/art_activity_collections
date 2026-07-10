import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { useEffect, useMemo, useRef, useState } from "react";
import { StyleSheet, View } from "react-native";
import * as Location from "expo-location";
import MapView, { Marker } from "react-native-maps";
import { dedupeVenues } from "../lib/format";
import { stateName } from "../lib/states";
import { resolveVenueCoordinates } from "../lib/venue-map-data";
import { useVenues } from "../hooks/useVenues";
import type { RootStackParamList } from "../navigation/types";

// Fully zoomed-out continental US, used until we fit to the venue markers.
const USA_REGION = {
  latitude: 39.8283,
  longitude: -98.5795,
  latitudeDelta: 40,
  longitudeDelta: 40,
};

// Zoom tiers drive the dot size: longitudeDelta shrinks as the user zooms in.
// Custom dot markers (unlike Apple's default pins) are never auto-hidden by
// MapKit's decluttering, so every venue stays visible at every zoom level.
type Tier = "far" | "mid" | "near";
const DOT_SIZE: Record<Tier, number> = { far: 9, mid: 13, near: 18 };

function tierFor(longitudeDelta: number): Tier {
  if (longitudeDelta > 12) return "far";
  if (longitudeDelta > 2.5) return "mid";
  return "near";
}

type Props = NativeStackScreenProps<RootStackParamList, "Map">;

export function MapScreen({ navigation }: Props) {
  const { data } = useVenues();
  const mapRef = useRef<MapView>(null);
  const [tier, setTier] = useState<Tier>("far");

  const markers = useMemo(() => {
    return dedupeVenues(data ?? []).map((venue) => {
      const resolved = resolveVenueCoordinates(venue);
      return { venue, latitude: resolved.resolvedLat, longitude: resolved.resolvedLng };
    });
  }, [data]);

  // Ask for location so the native "you are here" blue dot can show.
  useEffect(() => {
    Location.requestForegroundPermissionsAsync().catch(() => {});
  }, []);

  const fitToMarkers = () => {
    if (markers.length > 0 && mapRef.current) {
      mapRef.current.fitToCoordinates(
        markers.map((m) => ({ latitude: m.latitude, longitude: m.longitude })),
        { edgePadding: { top: 70, right: 70, bottom: 70, left: 70 }, animated: false },
      );
    }
  };

  const size = DOT_SIZE[tier];

  return (
    <View style={styles.container}>
      <MapView
        ref={mapRef}
        style={StyleSheet.absoluteFill}
        initialRegion={USA_REGION}
        showsUserLocation
        showsMyLocationButton
        onMapReady={fitToMarkers}
        onRegionChangeComplete={(region) => {
          const next = tierFor(region.longitudeDelta);
          setTier((current) => (current === next ? current : next));
        }}
      >
        {markers.map((m) => (
          <Marker
            // Tier in the key remounts the marker when the size tier changes,
            // so the frozen (tracksViewChanges=false) snapshot gets re-taken.
            key={`${m.venue.venue_name}-${tier}`}
            coordinate={{ latitude: m.latitude, longitude: m.longitude }}
            title={m.venue.venue_name}
            description={[m.venue.venue_city, stateName(m.venue.venue_state)]
              .filter(Boolean)
              .join(", ")}
            onCalloutPress={() => navigation.navigate("VenueDetail", { venue: m.venue })}
            anchor={{ x: 0.5, y: 0.5 }}
            tracksViewChanges={false}
          >
            <View
              style={[
                styles.dot,
                { width: size, height: size, borderRadius: size / 2 },
              ]}
            />
          </Marker>
        ))}
      </MapView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  dot: {
    backgroundColor: "#ff3b30",
    borderWidth: 2,
    borderColor: "#ffffff",
    shadowColor: "#000",
    shadowOpacity: 0.25,
    shadowRadius: 2,
    shadowOffset: { width: 0, height: 1 },
  },
});
