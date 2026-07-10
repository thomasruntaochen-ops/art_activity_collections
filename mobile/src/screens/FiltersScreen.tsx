import DateTimePicker from "@react-native-community/datetimepicker";
import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { Pressable, ScrollView, StyleSheet, Switch, Text, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { Chip } from "../components/Chip";
import { stateName } from "../lib/states";
import type { AudienceSegment } from "../lib/types";
import type { RootStackParamList } from "../navigation/types";
import { RANGE_LABELS, type RangeKey, useFilters } from "../store/filters";
import { colors, fonts, radius, space } from "../theme";

const RANGE_KEYS: RangeKey[] = ["upcoming", "today", "weekend", "7d", "30d", "90d", "custom"];
const AUDIENCE_OPTIONS: { value: "" | AudienceSegment; label: string }[] = [
  { value: "", label: "All audiences" },
  { value: "kids", label: "Kids" },
  { value: "teens", label: "Teens" },
  { value: "adults", label: "Adults" },
  { value: "all_ages", label: "All ages" },
];

type Props = NativeStackScreenProps<RootStackParamList, "Filters">;

export function FiltersScreen({ navigation }: Props) {
  const insets = useSafeAreaInsets();
  const filters = useFilters();

  return (
    <View style={styles.container}>
      <ScrollView contentContainerStyle={{ padding: space.lg, paddingBottom: 120 }}>
        <Text style={styles.h1}>Filters</Text>

        <Text style={styles.label}>When</Text>
        <View style={styles.chips}>
          {RANGE_KEYS.map((k) => (
            <Chip
              key={k}
              label={RANGE_LABELS[k]}
              active={filters.rangeKey === k}
              onPress={() => filters.setRange(k)}
            />
          ))}
        </View>

        {filters.rangeKey === "custom" ? (
          <View style={styles.customRow}>
            <View style={styles.dateField}>
              <Text style={styles.dateLabel}>From</Text>
              <DateTimePicker
                value={filters.customFrom ? new Date(filters.customFrom) : new Date()}
                mode="date"
                display="compact"
                onChange={(_event, d) => d && filters.setCustomFrom(d.toISOString())}
              />
            </View>
            <View style={styles.dateField}>
              <Text style={styles.dateLabel}>To</Text>
              <DateTimePicker
                value={
                  filters.customTo
                    ? new Date(filters.customTo)
                    : filters.customFrom
                      ? new Date(filters.customFrom)
                      : new Date()
                }
                mode="date"
                display="compact"
                minimumDate={filters.customFrom ? new Date(filters.customFrom) : undefined}
                onChange={(_event, d) => d && filters.setCustomTo(d.toISOString())}
              />
            </View>
          </View>
        ) : null}

        <Text style={styles.label}>Audience</Text>
        <View style={styles.chips}>
          {AUDIENCE_OPTIONS.map((o) => (
            <Chip
              key={o.value || "all"}
              label={o.label}
              active={filters.audience === o.value}
              onPress={() => filters.setAudience(o.value)}
            />
          ))}
        </View>

        <Text style={styles.label}>Location</Text>
        <Pressable
          style={styles.row}
          onPress={() => navigation.navigate("SelectOption", { field: "state", title: "State" })}
        >
          <Text style={styles.rowLabel}>State</Text>
          <Text style={styles.rowValue}>{stateName(filters.state) || "All states"}  ›</Text>
        </Pressable>
        <Pressable
          style={styles.row}
          onPress={() => navigation.navigate("SelectOption", { field: "city", title: "City" })}
        >
          <Text style={styles.rowLabel}>City</Text>
          <Text style={styles.rowValue}>{filters.city || "All cities"}  ›</Text>
        </Pressable>

        <View style={[styles.row, styles.switchRow]}>
          <Text style={styles.rowLabel}>Free only</Text>
          <Switch
            value={filters.freeOnly}
            onValueChange={filters.setFreeOnly}
            trackColor={{ true: colors.gold }}
          />
        </View>
      </ScrollView>

      <View style={[styles.footer, { paddingBottom: insets.bottom + space.md }]}>
        <Pressable style={styles.clearBtn} onPress={filters.reset}>
          <Text style={styles.clearText}>Clear all</Text>
        </Pressable>
        <Pressable style={styles.doneBtn} onPress={() => navigation.goBack()}>
          <Text style={styles.doneText}>Done</Text>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: colors.paper },
  h1: { fontFamily: fonts.serif, fontSize: 26, color: colors.ink, marginBottom: space.sm },
  label: {
    fontFamily: fonts.sans,
    fontSize: 12,
    letterSpacing: 1,
    textTransform: "uppercase",
    color: colors.muted,
    marginTop: space.lg,
    marginBottom: space.sm,
  },
  chips: { flexDirection: "row", flexWrap: "wrap", gap: space.sm },
  customRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    alignItems: "center",
    gap: space.lg,
    marginTop: space.md,
  },
  dateField: { flexDirection: "row", alignItems: "center", gap: space.sm },
  dateLabel: { fontFamily: fonts.sans, fontSize: 14, color: colors.muted },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingHorizontal: space.lg,
    paddingVertical: space.md,
    marginTop: space.sm,
  },
  switchRow: { marginTop: space.lg, paddingVertical: space.sm },
  rowLabel: { fontFamily: fonts.sans, fontSize: 16, color: colors.ink },
  rowValue: { fontFamily: fonts.sans, fontSize: 15, color: colors.muted },
  footer: {
    position: "absolute",
    left: 0,
    right: 0,
    bottom: 0,
    flexDirection: "row",
    gap: space.sm,
    paddingHorizontal: space.lg,
    paddingTop: space.md,
    backgroundColor: colors.paper,
    borderTopColor: colors.line,
    borderTopWidth: 1,
  },
  clearBtn: {
    flex: 1,
    alignItems: "center",
    paddingVertical: space.md,
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: colors.line,
  },
  clearText: { fontFamily: fonts.sans, fontSize: 15, color: colors.ink },
  doneBtn: {
    flex: 1,
    alignItems: "center",
    paddingVertical: space.md,
    borderRadius: radius.md,
    backgroundColor: colors.ink,
  },
  doneText: { fontFamily: fonts.sans, fontSize: 15, fontWeight: "500", color: colors.white },
});
