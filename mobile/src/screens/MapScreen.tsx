import { Ionicons } from "@expo/vector-icons";
import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import * as Location from "expo-location";
import MapView, { Marker } from "react-native-maps";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { useVenueActivities } from "../hooks/useActivities";
import { dedupeVenues, formatActivityTime } from "../lib/format";
import { stateName } from "../lib/states";
import { haversineMiles, resolveVenueCoordinates } from "../lib/venue-map-data";
import { useVenues } from "../hooks/useVenues";
import type { RootStackParamList } from "../navigation/types";
import type { VenueSummary } from "../lib/types";
import { useFilters } from "../store/filters";
import { colors, fonts, radius, space } from "../theme";

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

type MapMarker = { venue: VenueSummary; latitude: number; longitude: number };

type Props = NativeStackScreenProps<RootStackParamList, "Map">;

export function MapScreen({ navigation }: Props) {
  const insets = useSafeAreaInsets();
  const { data } = useVenues();
  const mapRef = useRef<MapView>(null);
  const [tier, setTier] = useState<Tier>("far");
  const [selected, setSelected] = useState<MapMarker | null>(null);
  const userLocation = useFilters((s) => s.userLocation);
  const setUserLocation = useFilters((s) => s.setUserLocation);

  // Upcoming activities for the selected venue (respects the shared filters).
  const { data: selectedActivities, isLoading: activitiesLoading } = useVenueActivities(
    selected?.venue.venue_name ?? "",
  );

  const markers = useMemo<MapMarker[]>(() => {
    return dedupeVenues(data ?? []).map((venue) => {
      const resolved = resolveVenueCoordinates(venue);
      return { venue, latitude: resolved.resolvedLat, longitude: resolved.resolvedLng };
    });
  }, [data]);

  // Get a location fix so the blue dot and the "X mi away" line can show.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const { status } = await Location.requestForegroundPermissionsAsync();
        if (status !== "granted" || cancelled) return;
        const pos = await Location.getCurrentPositionAsync({});
        if (cancelled) return;
        setUserLocation({ lat: pos.coords.latitude, lng: pos.coords.longitude });
      } catch {
        // No location — the card simply omits the distance line.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [setUserLocation]);

  const fitToMarkers = () => {
    if (markers.length > 0 && mapRef.current) {
      mapRef.current.fitToCoordinates(
        markers.map((m) => ({ latitude: m.latitude, longitude: m.longitude })),
        { edgePadding: { top: 70, right: 70, bottom: 70, left: 70 }, animated: false },
      );
    }
  };

  const size = DOT_SIZE[tier];
  const distanceMiles =
    selected && userLocation
      ? haversineMiles(userLocation.lat, userLocation.lng, selected.latitude, selected.longitude)
      : null;
  const selectedLocation = selected
    ? [selected.venue.venue_city, stateName(selected.venue.venue_state)].filter(Boolean).join(", ")
    : "";

  return (
    <View style={styles.container}>
      <MapView
        ref={mapRef}
        style={StyleSheet.absoluteFill}
        initialRegion={USA_REGION}
        showsUserLocation
        showsMyLocationButton
        onMapReady={fitToMarkers}
        onPress={() => setSelected(null)}
        onRegionChangeComplete={(region) => {
          const next = tierFor(region.longitudeDelta);
          setTier((current) => (current === next ? current : next));
        }}
      >
        {markers.map((m) => {
          const isSelected = selected?.venue.venue_name === m.venue.venue_name;
          const dotSize = isSelected ? size + 8 : size;
          return (
            <Marker
              // Tier + selection in the key remount the marker so the frozen
              // (tracksViewChanges=false) snapshot gets re-taken on change.
              key={`${m.venue.venue_name}-${tier}-${isSelected ? "sel" : "dot"}`}
              coordinate={{ latitude: m.latitude, longitude: m.longitude }}
              onPress={(event) => {
                event.stopPropagation();
                setSelected(m);
              }}
              anchor={{ x: 0.5, y: 0.5 }}
              tracksViewChanges={false}
            >
              <View
                style={[
                  styles.dot,
                  isSelected && styles.dotSelected,
                  { width: dotSize, height: dotSize, borderRadius: dotSize / 2 },
                ]}
              />
            </Marker>
          );
        })}
      </MapView>

      {selected ? (
        <View style={[styles.card, { paddingBottom: Math.max(insets.bottom, space.md) }]}>
          <View style={styles.cardHeader}>
            <View style={styles.cardHeading}>
              <Text style={styles.cardTitle} numberOfLines={2}>
                {selected.venue.venue_name}
              </Text>
              <Text style={styles.cardMeta}>
                {selectedLocation || "Location pending"}
                {distanceMiles != null ? `  ·  ${distanceMiles.toFixed(1)} mi away` : ""}
              </Text>
            </View>
            <Pressable hitSlop={10} onPress={() => setSelected(null)}>
              <Ionicons name="close" size={22} color={colors.muted} />
            </Pressable>
          </View>

          <ScrollView style={styles.cardList} nestedScrollEnabled showsVerticalScrollIndicator>
            {activitiesLoading ? (
              <ActivityIndicator color={colors.gold} style={{ marginVertical: space.md }} />
            ) : (selectedActivities ?? []).length === 0 ? (
              <Text style={styles.cardEmpty}>No upcoming activities for this venue.</Text>
            ) : (
              (selectedActivities ?? []).map((activity) => (
                <View key={activity.id} style={styles.cardRow}>
                  <Text style={styles.cardRowTitle} numberOfLines={1}>
                    {activity.title}
                  </Text>
                  <View style={styles.cardRowMeta}>
                    <Text style={styles.cardRowTime}>{formatActivityTime(activity.start_at)}</Text>
                    {activity.is_free === true ? (
                      <Text style={styles.cardRowFree}>Free</Text>
                    ) : null}
                  </View>
                </View>
              ))
            )}
          </ScrollView>

          <Pressable
            style={styles.cardButton}
            onPress={() => navigation.navigate("VenueDetail", { venue: selected.venue })}
          >
            <Text style={styles.cardButtonText}>View museum & activities</Text>
            <Ionicons name="arrow-forward" size={16} color={colors.white} />
          </Pressable>
        </View>
      ) : null}
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
  dotSelected: {
    backgroundColor: colors.goldDeep,
    borderWidth: 3,
  },
  card: {
    position: "absolute",
    left: space.md,
    right: space.md,
    bottom: space.md,
    backgroundColor: colors.paper,
    borderRadius: radius.lg,
    borderColor: colors.line,
    borderWidth: 1,
    padding: space.lg,
    shadowColor: "#000",
    shadowOpacity: 0.18,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
  },
  cardHeader: { flexDirection: "row", alignItems: "flex-start", gap: space.sm },
  cardHeading: { flex: 1 },
  cardTitle: { fontFamily: fonts.serif, fontSize: 19, color: colors.ink },
  cardMeta: { fontFamily: fonts.sans, fontSize: 13, color: colors.muted, marginTop: 3 },
  cardList: { maxHeight: 168, marginTop: space.md },
  cardEmpty: { fontFamily: fonts.sans, fontSize: 13, color: colors.muted, paddingVertical: space.sm },
  cardRow: {
    paddingVertical: 9,
    borderBottomColor: colors.line,
    borderBottomWidth: 1,
  },
  cardRowTitle: { fontFamily: fonts.sans, fontSize: 14, fontWeight: "500", color: colors.ink },
  cardRowMeta: { flexDirection: "row", alignItems: "center", gap: 8, marginTop: 2 },
  cardRowTime: { fontFamily: fonts.sans, fontSize: 12, color: colors.muted },
  cardRowFree: { fontFamily: fonts.sans, fontSize: 12, fontWeight: "500", color: colors.goldDeep },
  cardButton: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 6,
    backgroundColor: colors.ink,
    borderRadius: radius.md,
    paddingVertical: space.md,
    marginTop: space.md,
  },
  cardButtonText: { fontFamily: fonts.sans, fontSize: 14, fontWeight: "500", color: colors.white },
});
