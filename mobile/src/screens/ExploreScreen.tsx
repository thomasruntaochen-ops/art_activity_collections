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
import { Ionicons } from "@expo/vector-icons";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { useNavigation } from "@react-navigation/native";
import type { NativeStackNavigationProp } from "@react-navigation/native-stack";
import { VenueCard } from "../components/VenueCard";
import { useVenues } from "../hooks/useVenues";
import { dedupeVenues } from "../lib/format";
import { stateName } from "../lib/states";
import { haversineMiles, resolveVenueCoordinates } from "../lib/venue-map-data";
import type { RootStackParamList } from "../navigation/types";
import { RANGE_LABELS, activeFilterCount, deriveDateRange, useFilters } from "../store/filters";
import { colors, fonts, radius, space } from "../theme";

type Nav = NativeStackNavigationProp<RootStackParamList>;

export function ExploreScreen() {
  const insets = useSafeAreaInsets();
  const navigation = useNavigation<Nav>();
  const { data, isLoading, isError, error, refetch, isRefetching } = useVenues();
  const activeCount = useFilters(activeFilterCount);
  const userLocation = useFilters((s) => s.userLocation);
  const radiusMiles = useFilters((s) => s.radiusMiles);
  const rangeKey = useFilters((s) => s.rangeKey);
  const setRange = useFilters((s) => s.setRange);
  const customFrom = useFilters((s) => s.customFrom);
  const customTo = useFilters((s) => s.customTo);
  const [search, setSearch] = useState("");

  // Plain-words description of the active date window (hidden for the default
  // "upcoming"), so buttons like "This weekend" explain what they did.
  const dateStatus = useMemo(() => {
    if (rangeKey === "upcoming") return null;
    const fmt = (iso?: string) =>
      iso
        ? new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric" })
        : null;
    const { date_from, date_to } = deriveDateRange(rangeKey, customFrom, customTo);
    const from = fmt(date_from);
    const to = fmt(date_to);
    if (rangeKey === "today") return "Showing today only";
    if (rangeKey === "weekend") return `Showing this weekend · ${from} – ${to}`;
    if (rangeKey === "custom") {
      if (from && to) return `Showing ${from} – ${to}`;
      if (from) return `Showing from ${from}`;
      return `Showing until ${to}`;
    }
    return `Showing ${RANGE_LABELS[rangeKey].toLowerCase()} · ${from} – ${to}`;
  }, [rangeKey, customFrom, customTo]);

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
          <Ionicons name="map-outline" size={16} color={colors.ink} />
          <Text style={styles.actionText}>Map</Text>
        </Pressable>
        <Pressable
          style={[styles.actionBtn, nearMeActive && styles.actionBtnActive]}
          onPress={() => navigation.navigate("NearMe")}
        >
          <Ionicons
            name="locate-outline"
            size={16}
            color={nearMeActive ? colors.white : colors.ink}
          />
          <Text
            style={[styles.actionText, nearMeActive && styles.actionTextActive]}
            numberOfLines={1}
          >
            {nearMeActive ? `${radiusMiles} mi` : "Near me"}
          </Text>
        </Pressable>
        <Pressable
          style={[styles.actionBtn, rangeKey === "weekend" && styles.actionBtnActive]}
          onPress={() => setRange(rangeKey === "weekend" ? "upcoming" : "weekend")}
        >
          <Ionicons
            name="calendar-outline"
            size={16}
            color={rangeKey === "weekend" ? colors.white : colors.ink}
          />
          <Text
            style={[styles.actionText, rangeKey === "weekend" && styles.actionTextActive]}
            numberOfLines={1}
          >
            This weekend
          </Text>
        </Pressable>
      </View>

      {dateStatus ? (
        <View style={styles.statusRow}>
          <Text style={styles.statusText}>{dateStatus}</Text>
          <Pressable hitSlop={8} onPress={() => setRange("upcoming")}>
            <Text style={styles.statusClear}>✕</Text>
          </Pressable>
        </View>
      ) : null}

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
          ListFooterComponent={
            <View style={styles.footer}>
              <Text style={styles.footerText}>
                Activity details are gathered automatically from museum websites and may be out of
                date — always confirm dates, prices, ages, and registration with the venue before
                you go.
              </Text>
              <Pressable hitSlop={6} onPress={() => navigation.navigate("Disclaimer")}>
                <Text style={styles.footerLink}>Read full disclaimer</Text>
              </Pressable>
            </View>
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
    // Grow from natural content width (not equal thirds), so longer labels
    // like "This weekend" get the room they need instead of being squeezed.
    flexGrow: 1,
    flexShrink: 1,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 6,
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingVertical: space.md,
    paddingHorizontal: 10,
  },
  actionBtnActive: { backgroundColor: colors.gold, borderColor: colors.goldDeep },
  actionText: { fontFamily: fonts.sans, fontSize: 13.5, fontWeight: "500", color: colors.ink },
  actionTextActive: { color: colors.white },
  statusRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    backgroundColor: "rgba(200, 144, 49, 0.14)",
    borderRadius: radius.sm,
    paddingHorizontal: space.md,
    paddingVertical: 7,
    marginBottom: space.md,
  },
  statusText: { fontFamily: fonts.sans, fontSize: 13, color: colors.goldDeep, flex: 1 },
  statusClear: { fontFamily: fonts.sans, fontSize: 14, color: colors.goldDeep, paddingLeft: space.md },
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
  footer: {
    marginTop: space.md,
    paddingTop: space.lg,
    borderTopColor: colors.line,
    borderTopWidth: 1,
    gap: space.sm,
  },
  footerText: {
    fontFamily: fonts.sans,
    fontSize: 12.5,
    lineHeight: 18,
    color: colors.muted,
    textAlign: "center",
  },
  footerLink: {
    fontFamily: fonts.sans,
    fontSize: 13,
    fontWeight: "500",
    color: colors.goldDeep,
    textAlign: "center",
  },
});
