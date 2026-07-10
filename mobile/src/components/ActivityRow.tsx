import { Linking, Pressable, StyleSheet, Text, View } from "react-native";
import { formatActivityTime, formatAudienceSegment } from "../lib/format";
import type { Activity } from "../lib/types";
import { colors, fonts, radius, space } from "../theme";
import { MetaPill } from "./MetaPill";

// One activity in a venue's list. Tapping opens its source page (the museum's
// own listing) in the browser, mirroring the web app's external links.
export function ActivityRow({ activity }: { activity: Activity }) {
  const open = () => {
    if (activity.source_url) Linking.openURL(activity.source_url);
  };
  return (
    <Pressable style={({ pressed }) => [styles.row, pressed && styles.pressed]} onPress={open}>
      <Text style={styles.title} numberOfLines={2}>
        {activity.title}
      </Text>
      <View style={styles.meta}>
        <Text style={styles.time}>{formatActivityTime(activity.start_at)}</Text>
        {activity.is_free === true ? <MetaPill label="Free" tone="free" /> : null}
        {activity.audience_segment !== "unknown" ? (
          <MetaPill label={formatAudienceSegment(activity.audience_segment)} />
        ) : null}
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
});
