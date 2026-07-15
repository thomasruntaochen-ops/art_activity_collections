import { Linking, ScrollView, StyleSheet, Text } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { PRIVACY_POLICY_URL } from "../config";
import { colors, fonts, space } from "../theme";

// Full disclaimer, ported verbatim from the website footer (site → app).
const ITEMS: { lead: string; body: string }[] = [
  {
    lead: "Independent directory.",
    body:
      "Art Museum Activities Explorer is an independent guide to art activities for all ages — kids, teens, and adults — and is not affiliated with, endorsed by, or sponsored by any museum or institution listed here.",
  },
  {
    lead: "Information may be inaccurate or out of date.",
    body:
      "Listings are collected automatically from public museum websites. Dates, times, locations, age ranges, registration rules, and availability can change or be removed at any time.",
  },
  {
    lead: "“Free” is not a guarantee.",
    body:
      "A “Free” label reflects what we found when the listing was collected; admission, materials, or registration fees may still apply. Confirm pricing with the venue.",
  },
  {
    lead: "Verify before attending.",
    body:
      "Check the museum’s official website or contact them directly to confirm an activity is still running and is suitable for you, your child, or your group.",
  },
  {
    lead: "External links.",
    body: "Links open third-party websites we don’t control and aren’t responsible for.",
  },
  {
    lead: "No warranty.",
    body:
      "This app is provided “as is,” without warranties of any kind, and we are not liable for any loss or inconvenience from reliance on this information.",
  },
];

export function DisclaimerScreen() {
  const insets = useSafeAreaInsets();
  return (
    <ScrollView
      style={styles.container}
      contentContainerStyle={{ padding: space.lg, paddingBottom: insets.bottom + space.xl }}
      showsVerticalScrollIndicator={false}
    >
      <Text style={styles.summary}>
        Activity details are gathered automatically from museum websites and may be out of date —
        always confirm dates, prices, ages, and registration with the venue before you go.
      </Text>
      {ITEMS.map((item) => (
        <Text key={item.lead} style={styles.item}>
          <Text style={styles.lead}>{item.lead}</Text> {item.body}
        </Text>
      ))}
      <Text style={styles.privacyLink} onPress={() => Linking.openURL(PRIVACY_POLICY_URL)}>
        Privacy Policy
      </Text>
      <Text style={styles.credit}>
        Designed and developed by Thomas R Chen.{"\n"}© 2026 Thomas R Chen. All rights
        reserved.
      </Text>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: colors.paper },
  summary: {
    fontFamily: fonts.sans,
    fontSize: 15,
    lineHeight: 22,
    color: colors.ink,
    marginBottom: space.lg,
  },
  item: {
    fontFamily: fonts.sans,
    fontSize: 14,
    lineHeight: 21,
    color: colors.muted,
    marginBottom: space.md,
  },
  lead: { fontWeight: "600", color: colors.ink },
  privacyLink: {
    fontFamily: fonts.sans,
    fontSize: 14,
    fontWeight: "600",
    color: colors.ink,
    textDecorationLine: "underline",
    marginTop: space.md,
  },
  credit: {
    fontFamily: fonts.sans,
    fontSize: 13,
    lineHeight: 20,
    color: colors.muted,
    marginTop: space.lg,
  },
});
