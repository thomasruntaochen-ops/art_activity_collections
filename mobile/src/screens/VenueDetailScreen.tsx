import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { ActivityIndicator, FlatList, Linking, Pressable, StyleSheet, Text, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { ActivityRow } from "../components/ActivityRow";
import { useVenueActivities } from "../hooks/useActivities";
import { buildDirectionsTargets, formatVenueLine } from "../lib/format";
import type { RootStackParamList } from "../navigation/types";
import { colors, fonts, radius, space } from "../theme";

type Props = NativeStackScreenProps<RootStackParamList, "VenueDetail">;

export function VenueDetailScreen({ route }: Props) {
  const insets = useSafeAreaInsets();
  const { venue } = route.params;
  const { data: activities, isLoading, isError, error } = useVenueActivities(venue.venue_name);
  const directions = buildDirectionsTargets(venue);
  const rows = activities ?? [];
  const freeCount = rows.filter((a) => a.is_free === true).length;

  return (
    <FlatList
      style={styles.container}
      contentContainerStyle={{ padding: space.lg, paddingBottom: insets.bottom + space.xl }}
      data={rows}
      keyExtractor={(a) => String(a.id)}
      renderItem={({ item }) => <ActivityRow activity={item} />}
      showsVerticalScrollIndicator={false}
      ListHeaderComponent={
        <View>
          <Text style={styles.eyebrow}>Venue activities</Text>
          <Text style={styles.title}>{venue.venue_name}</Text>
          <Text style={styles.location}>{formatVenueLine(venue)}</Text>
          <View style={styles.directions}>
            <Pressable style={styles.dirBtn} onPress={() => Linking.openURL(directions.apple)}>
              <Text style={styles.dirText}>Apple Maps</Text>
            </Pressable>
            <Pressable style={styles.dirBtn} onPress={() => Linking.openURL(directions.google)}>
              <Text style={styles.dirText}>Google Maps</Text>
            </Pressable>
          </View>
          {!isLoading && !isError ? (
            <Text style={styles.summary}>
              {rows.length} {rows.length === 1 ? "activity" : "activities"} · {freeCount} free
            </Text>
          ) : null}
          <Text style={styles.section}>Activities</Text>
          {isLoading ? <ActivityIndicator color={colors.gold} style={{ marginTop: space.md }} /> : null}
          {isError ? (
            <Text style={styles.note}>{(error as Error)?.message ?? "Couldn’t load activities."}</Text>
          ) : null}
        </View>
      }
      ListEmptyComponent={
        !isLoading && !isError ? (
          <Text style={styles.note}>No upcoming activities for this venue.</Text>
        ) : null
      }
    />
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: colors.paper },
  eyebrow: {
    fontFamily: fonts.sans,
    fontSize: 12,
    letterSpacing: 1,
    textTransform: "uppercase",
    color: colors.gold,
  },
  title: { fontFamily: fonts.serif, fontSize: 26, color: colors.ink, marginTop: 4 },
  location: { fontFamily: fonts.sans, fontSize: 14, color: colors.muted, marginTop: 4 },
  directions: { flexDirection: "row", gap: space.sm, marginTop: space.md },
  dirBtn: {
    flex: 1,
    alignItems: "center",
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingVertical: space.md,
  },
  dirText: { fontFamily: fonts.sans, fontSize: 14, fontWeight: "500", color: colors.goldDeep },
  summary: { fontFamily: fonts.sans, fontSize: 13, color: colors.muted, marginTop: space.md },
  section: {
    fontFamily: fonts.serif,
    fontSize: 18,
    color: colors.ink,
    marginTop: space.lg,
    marginBottom: space.sm,
  },
  note: { fontFamily: fonts.sans, fontSize: 14, color: colors.muted, marginTop: space.md },
});
