import { Ionicons } from "@expo/vector-icons";
import { Image } from "expo-image";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { getVenueImageUri } from "../lib/venue-media";
import { stateName } from "../lib/states";
import type { VenueSummary } from "../lib/types";
import { useFavorites, useIsVenueSaved } from "../store/favorites";
import { colors, fonts, radius, space } from "../theme";

type Props = {
  venue: VenueSummary;
  onPress?: () => void;
};

// A tappable venue card with the museum photo, a dark scrim for legibility, the
// name / location / program count, and a heart to save it — the native
// counterpart to the web explorer's `.venue-card`.
export function VenueCard({ venue, onPress }: Props) {
  const uri = getVenueImageUri(venue.venue_name);
  const toggleVenue = useFavorites((s) => s.toggleVenue);
  const saved = useIsVenueSaved(venue.venue_name);
  const location =
    [venue.venue_city, stateName(venue.venue_state)].filter(Boolean).join(", ") ||
    "Location pending";

  return (
    <Pressable
      style={({ pressed }) => [styles.card, pressed && styles.pressed]}
      onPress={onPress}
    >
      {uri ? (
        <Image source={{ uri }} style={StyleSheet.absoluteFill} contentFit="cover" transition={250} />
      ) : null}
      <View style={styles.scrim} />
      <Pressable style={styles.heart} hitSlop={10} onPress={() => toggleVenue(venue)}>
        <Ionicons
          name={saved ? "heart" : "heart-outline"}
          size={19}
          color={saved ? "#ff5d7d" : "rgba(255,255,255,0.92)"}
        />
      </Pressable>
      <View style={styles.content}>
        <Text style={styles.title} numberOfLines={2}>
          {venue.venue_name}
        </Text>
        <Text style={styles.meta}>{location}</Text>
        <Text style={styles.count}>
          {venue.activity_count} {venue.activity_count === 1 ? "program" : "programs"}
        </Text>
      </View>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    height: 168,
    borderRadius: radius.lg,
    overflow: "hidden",
    marginBottom: space.md,
    backgroundColor: colors.ink,
    justifyContent: "flex-end",
  },
  pressed: {
    opacity: 0.92,
  },
  scrim: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: colors.cardScrim,
  },
  heart: {
    position: "absolute",
    top: space.md,
    right: space.md,
    width: 34,
    height: 34,
    borderRadius: 17,
    backgroundColor: "rgba(22, 18, 14, 0.45)",
    alignItems: "center",
    justifyContent: "center",
  },
  content: {
    padding: space.lg,
  },
  title: {
    fontFamily: fonts.serif,
    fontSize: 21,
    color: colors.white,
    marginBottom: 4,
  },
  meta: {
    fontFamily: fonts.sans,
    fontSize: 13,
    color: "rgba(255,255,255,0.86)",
  },
  count: {
    fontFamily: fonts.sans,
    fontSize: 12,
    color: "rgba(255,255,255,0.78)",
    marginTop: 6,
  },
});
