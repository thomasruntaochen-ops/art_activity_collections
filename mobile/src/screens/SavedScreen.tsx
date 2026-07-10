import { Ionicons } from "@expo/vector-icons";
import { useNavigation } from "@react-navigation/native";
import type { NativeStackNavigationProp } from "@react-navigation/native-stack";
import { useMemo } from "react";
import { ScrollView, StyleSheet, Text, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { ActivityRow } from "../components/ActivityRow";
import { VenueCard } from "../components/VenueCard";
import type { RootStackParamList } from "../navigation/types";
import { useFavorites } from "../store/favorites";
import { colors, fonts, space } from "../theme";

type Nav = NativeStackNavigationProp<RootStackParamList>;

// Everything the user has hearted, saved on this phone — no account needed.
export function SavedScreen() {
  const insets = useSafeAreaInsets();
  const navigation = useNavigation<Nav>();
  const savedVenues = useFavorites((s) => s.savedVenues);
  const savedActivities = useFavorites((s) => s.savedActivities);

  const venues = useMemo(
    () => Object.values(savedVenues).sort((a, b) => a.venue_name.localeCompare(b.venue_name)),
    [savedVenues],
  );
  const activities = useMemo(
    () =>
      Object.values(savedActivities).sort(
        (a, b) => new Date(a.start_at).getTime() - new Date(b.start_at).getTime(),
      ),
    [savedActivities],
  );
  const isEmpty = venues.length === 0 && activities.length === 0;

  return (
    <ScrollView
      style={styles.container}
      contentContainerStyle={{
        paddingTop: insets.top + space.md,
        paddingHorizontal: space.lg,
        paddingBottom: insets.bottom + space.xl,
      }}
      showsVerticalScrollIndicator={false}
    >
      <Text style={styles.brand}>Saved</Text>
      <Text style={styles.subtitle}>Your museums and activities, kept on this phone</Text>

      {isEmpty ? (
        <View style={styles.empty}>
          <Ionicons name="heart-outline" size={40} color={colors.muted} />
          <Text style={styles.note}>
            Nothing saved yet. Tap the heart on any museum or activity and it will show up here.
          </Text>
        </View>
      ) : null}

      {venues.length > 0 ? (
        <>
          <Text style={styles.section}>Museums</Text>
          {venues.map((venue) => (
            <VenueCard
              key={venue.venue_name}
              venue={venue}
              onPress={() => navigation.navigate("VenueDetail", { venue })}
            />
          ))}
        </>
      ) : null}

      {activities.length > 0 ? (
        <>
          <Text style={styles.section}>Activities</Text>
          {activities.map((activity) => (
            <ActivityRow key={activity.id} activity={activity} />
          ))}
        </>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: colors.paper },
  brand: { fontFamily: fonts.serif, fontSize: 28, color: colors.ink },
  subtitle: {
    fontFamily: fonts.sans,
    fontSize: 14,
    color: colors.muted,
    marginTop: 2,
    marginBottom: space.md,
  },
  section: {
    fontFamily: fonts.serif,
    fontSize: 18,
    color: colors.ink,
    marginTop: space.md,
    marginBottom: space.sm,
  },
  empty: { alignItems: "center", paddingVertical: 56, gap: space.sm },
  note: {
    fontFamily: fonts.sans,
    fontSize: 14,
    color: colors.muted,
    textAlign: "center",
    lineHeight: 20,
  },
});
