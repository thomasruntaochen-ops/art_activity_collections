import { useMemo, useState } from "react";
import {
  ActivityIndicator,
  FlatList,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { VenueCard } from "../components/VenueCard";
import { useVenues } from "../hooks/useVenues";
import { dedupeVenues } from "../lib/format";
import { stateName } from "../lib/states";
import { haversineMiles, resolveVenueCoordinates } from "../lib/venue-map-data";
import type { RootStackParamList } from "../navigation/types";
import { activeFilterCount, useFilters } from "../store/filters";
import { colors, fonts, radius, space } from "../theme";

type Props = NativeStackScreenProps<RootStackParamList, "Explore">;

export function ExploreScreen({ navigation }: Props) {
  const insets = useSafeAreaInsets();
  const { data, isLoading, isError, error, refetch, isRefetching } = useVenues();
  const activeCount = useFilters(activeFilterCount);
  const userLocation = useFilters((s) => s.userLocation);
  const radiusMiles = useFilters((s) => s.radiusMiles);
  const [search, setSearch] = useState("");

  const venues = useMemo(() => {
    let list = dedupeVenues(data ?? []);
    const query = search.trim().toLowerCase();
    if (query) {
      list = list.filter((venue) =>
        [venue.venue_name, venue.venue_city, venue.venue_state, stateName(venue.venue_state)]
          .filter(Boolean)
          .join(" ")
          .toLowerCase()
          .includes(query),
      );
    }
    // "Near me": keep venues within the chosen radius, ordered closest-first.
    if (userLocation && radiusMiles) {
      list = list
        .map((venue) => {
          const resolved = resolveVenueCoordinates(venue);
          const distance = haversineMiles(
            userLocation.lat,
            userLocation.lng,
            resolved.resolvedLat,
            resolved.resolvedLng,
          );
          return { venue, distance };
        })
        .filter((entry) => entry.distance <= radiusMiles)
        .sort((a, b) => a.distance - b.distance)
        .map((entry) => entry.venue);
    }
    return list;
  }, [data, search, userLocation, radiusMiles]);

  const nearMeActive = Boolean(userLocation && radiusMiles);

  return (
    <View style={[styles.container, { paddingTop: insets.top + space.md }]}>
      <Text style={styles.brand}>Art Museum Activities</Text>
      <Text style={styles.subtitle}>Museums with active art programs</Text>

      <View style={styles.searchRow}>
        <TextInput
          style={styles.search}
          placeholder="Search museums or cities..."
          placeholderTextColor={colors.muted}
          value={search}
          onChangeText={setSearch}
          autoCorrect={false}
          clearButtonMode="while-editing"
        />
        <Pressable style={styles.filterBtn} onPress={() => navigation.navigate("Filters")}>
          <Text style={styles.filterBtnText}>Filters</Text>
          {activeCount > 0 ? (
            <View style={styles.badge}>
              <Text style={styles.badgeText}>{activeCount}</Text>
            </View>
          ) : null}
        </Pressable>
      </View>

      <View style={styles.actionRow}>
        <Pressable style={styles.actionBtn} onPress={() => navigation.navigate("Map")}>
          <Text style={styles.actionText}>🗺  Map</Text>
        </Pressable>
        <Pressable
          style={[styles.actionBtn, nearMeActive && styles.actionBtnActive]}
          onPress={() => navigation.navigate("NearMe")}
        >
          <Text style={[styles.actionText, nearMeActive && styles.actionTextActive]}>
            ◎  {nearMeActive ? `Near me · ${radiusMiles} mi` : "Near me"}
          </Text>
        </Pressable>
      </View>

      {isLoading ? (
        <View style={styles.center}>
          <ActivityIndicator color={colors.gold} />
          <Text style={styles.note}>Loading venues…</Text>
        </View>
      ) : isError ? (
        <View style={styles.center}>
          <Text style={styles.errorTitle}>Couldn’t load venues</Text>
          <Text style={styles.note}>{(error as Error)?.message}</Text>
        </View>
      ) : (
        <FlatList
          data={venues}
          keyExtractor={(venue) => venue.venue_name}
          renderItem={({ item }) => (
            <VenueCard
              venue={item}
              onPress={() => navigation.navigate("VenueDetail", { venue: item })}
            />
          )}
          contentContainerStyle={{ paddingBottom: insets.bottom + space.xl }}
          showsVerticalScrollIndicator={false}
          onRefresh={refetch}
          refreshing={isRefetching}
          ListEmptyComponent={
            <Text style={styles.note}>
              {nearMeActive ? "No museums within this distance." : "No venues match these filters."}
            </Text>
          }
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: colors.paper,
    paddingHorizontal: space.lg,
  },
  brand: {
    fontFamily: fonts.serif,
    fontSize: 28,
    color: colors.ink,
  },
  subtitle: {
    fontFamily: fonts.sans,
    fontSize: 14,
    color: colors.muted,
    marginTop: 2,
    marginBottom: space.md,
  },
  searchRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: space.sm,
    marginBottom: space.sm,
  },
  search: {
    flex: 1,
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingHorizontal: space.lg,
    paddingVertical: space.md,
    fontFamily: fonts.sans,
    fontSize: 16,
    color: colors.ink,
  },
  filterBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    backgroundColor: colors.ink,
    borderRadius: radius.md,
    paddingHorizontal: space.lg,
    paddingVertical: space.md,
  },
  filterBtnText: { fontFamily: fonts.sans, fontSize: 15, fontWeight: "500", color: colors.white },
  badge: {
    minWidth: 18,
    height: 18,
    borderRadius: 9,
    backgroundColor: colors.gold,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 4,
  },
  badgeText: { fontFamily: fonts.sans, fontSize: 11, fontWeight: "600", color: colors.white },
  actionRow: {
    flexDirection: "row",
    gap: space.sm,
    marginBottom: space.md,
  },
  actionBtn: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingVertical: space.md,
  },
  actionBtnActive: { backgroundColor: colors.gold, borderColor: colors.goldDeep },
  actionText: { fontFamily: fonts.sans, fontSize: 15, fontWeight: "500", color: colors.ink },
  actionTextActive: { color: colors.white },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: space.sm,
    paddingHorizontal: space.xl,
  },
  errorTitle: {
    fontFamily: fonts.serif,
    fontSize: 18,
    color: colors.ink,
  },
  note: {
    fontFamily: fonts.sans,
    fontSize: 14,
    color: colors.muted,
    textAlign: "center",
  },
});
