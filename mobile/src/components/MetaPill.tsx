import { StyleSheet, Text, View } from "react-native";
import { colors, fonts } from "../theme";

type Tone = "free" | "audience";

// Small rounded label used for "Free" and audience tags, matching the web pills.
export function MetaPill({ label, tone = "audience" }: { label: string; tone?: Tone }) {
  const isFree = tone === "free";
  return (
    <View style={[styles.pill, isFree ? styles.free : styles.audience]}>
      <Text style={[styles.text, isFree ? styles.freeText : styles.audienceText]}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  pill: { paddingHorizontal: 8, paddingVertical: 3, borderRadius: 999 },
  free: { backgroundColor: "rgba(200,144,49,0.2)" },
  audience: { backgroundColor: "rgba(101,84,69,0.12)" },
  text: { fontFamily: fonts.sans, fontSize: 12, fontWeight: "500" },
  freeText: { color: colors.goldDeep },
  audienceText: { color: colors.muted },
});
