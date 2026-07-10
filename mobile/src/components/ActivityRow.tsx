import { Ionicons } from "@expo/vector-icons";
import { Linking, Pressable, Share, StyleSheet, Text, View } from "react-native";
import { addActivityToCalendar } from "../lib/calendar";
import { formatActivityTime, formatAudienceSegment } from "../lib/format";
import type { Activity } from "../lib/types";
import { useFavorites, useIsActivitySaved } from "../store/favorites";
import { colors, fonts, radius, space } from "../theme";
import { MetaPill } from "./MetaPill";

// One activity in a venue's list. Tapping the row opens its source page (the
// museum's own listing); the icon buttons save it, add it to the calendar, or
// share it.
export function ActivityRow({ activity }: { activity: Activity }) {
  const toggleActivity = useFavorites((s) => s.toggleActivity);
  const saved = useIsActivitySaved(activity.id);
  const isPast = new Date(activity.start_at).getTime() < Date.now();

  const open = () => {
    if (activity.source_url) Linking.openURL(activity.source_url);
  };

  const share = () => {
    const lines = [
      activity.title,
      [formatActivityTime(activity.start_at), activity.venue_name].filter(Boolean).join(" · "),
      activity.source_url,
    ].filter(Boolean);
    Share.share({ message: lines.join("\n") });
  };

  return (
    <Pressable style={({ pressed }) => [styles.row, pressed && styles.pressed]} onPress={open}>
      <Text style={styles.title} numberOfLines={2}>
        {activity.title}
      </Text>
      <View style={styles.meta}>
        <Text style={styles.time}>{formatActivityTime(activity.start_at)}</Text>
        {isPast ? <MetaPill label="Past" /> : null}
        {activity.is_free === true ? <MetaPill label="Free" tone="free" /> : null}
        {activity.audience_segment !== "unknown" ? (
          <MetaPill label={formatAudienceSegment(activity.audience_segment)} />
        ) : null}
        <View style={styles.actions}>
          <Pressable hitSlop={8} onPress={() => toggleActivity(activity)}>
            <Ionicons
              name={saved ? "heart" : "heart-outline"}
              size={20}
              color={saved ? "#e0245e" : colors.muted}
            />
          </Pressable>
          <Pressable hitSlop={8} onPress={() => addActivityToCalendar(activity)}>
            <Ionicons name="calendar-outline" size={19} color={colors.muted} />
          </Pressable>
          <Pressable hitSlop={8} onPress={share}>
            <Ionicons name="share-outline" size={20} color={colors.muted} />
          </Pressable>
        </View>
      </View>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  row: {
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    padding: space.lg,
    marginBottom: space.sm,
  },
  pressed: { opacity: 0.7 },
  title: {
    fontFamily: fonts.sans,
    fontSize: 16,
    fontWeight: "500",
    color: colors.ink,
    marginBottom: 8,
  },
  meta: { flexDirection: "row", alignItems: "center", flexWrap: "wrap", gap: 8 },
  time: { fontFamily: fonts.sans, fontSize: 13, color: colors.muted },
  actions: {
    flexDirection: "row",
    alignItems: "center",
    gap: 14,
    marginLeft: "auto",
  },
});
