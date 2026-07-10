import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { useMemo, useState } from "react";
import { ActivityIndicator, FlatList, Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { useFilterOptions } from "../hooks/useFilterOptions";
import { stateName } from "../lib/states";
import type { RootStackParamList } from "../navigation/types";
import { useFilters } from "../store/filters";
import { colors, fonts, radius, space } from "../theme";

type Props = NativeStackScreenProps<RootStackParamList, "SelectOption">;

// A searchable list for picking a state or city. Writes the choice straight to
// the shared filter store, then returns to the Filters screen.
export function SelectOptionScreen({ route, navigation }: Props) {
  const insets = useSafeAreaInsets();
  const { field, title } = route.params;
  const selectedState = useFilters((s) => s.state);
  const setState = useFilters((s) => s.setState);
  const setCity = useFilters((s) => s.setCity);
  const current = useFilters((s) => (field === "state" ? s.state : s.city));
  // City options are scoped to the chosen state; state options aren't scoped.
  const { data, isLoading } = useFilterOptions(field === "city" ? selectedState : "");
  const [query, setQuery] = useState("");

  const allLabel = field === "state" ? "All states" : "All cities";
  // For states, options are USPS codes; display (and search) the full name but
  // keep the code as the stored value. Sort states alphabetically by full name.
  const display = (value: string) => (field === "state" ? stateName(value) : value);
  const options = useMemo(() => {
    let all = (field === "state" ? data?.states : data?.cities) ?? [];
    if (field === "state") {
      all = [...all].sort((a, b) => stateName(a).localeCompare(stateName(b)));
    }
    const q = query.trim().toLowerCase();
    return q
      ? all.filter((o) => `${o} ${display(o)}`.toLowerCase().includes(q))
      : all;
  }, [data, field, query]);

  const choose = (value: string) => {
    if (field === "state") setState(value);
    else setCity(value);
    navigation.goBack();
  };

  return (
    <View style={styles.container}>
      <TextInput
        style={styles.search}
        placeholder={`Search ${title.toLowerCase()}…`}
        placeholderTextColor={colors.muted}
        value={query}
        onChangeText={setQuery}
        autoCorrect={false}
        clearButtonMode="while-editing"
      />
      {isLoading ? (
        <ActivityIndicator color={colors.gold} style={{ marginTop: space.lg }} />
      ) : (
        <FlatList
          data={[allLabel, ...options]}
          keyExtractor={(item) => item}
          keyboardShouldPersistTaps="handled"
          contentContainerStyle={{ paddingBottom: insets.bottom + space.xl }}
          renderItem={({ item, index }) => {
            const value = index === 0 ? "" : item;
            const isSelected = value === current;
            const label = index === 0 ? item : display(item);
            return (
              <Pressable style={styles.row} onPress={() => choose(value)}>
                <Text style={[styles.rowText, isSelected && styles.selected]}>{label}</Text>
                {isSelected ? <Text style={styles.check}>✓</Text> : null}
              </Pressable>
            );
          }}
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: colors.paper, paddingHorizontal: space.lg, paddingTop: space.md },
  search: {
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingHorizontal: space.lg,
    paddingVertical: space.md,
    fontFamily: fonts.sans,
    fontSize: 16,
    color: colors.ink,
    marginBottom: space.sm,
  },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingVertical: space.md,
    borderBottomColor: colors.line,
    borderBottomWidth: 1,
  },
  rowText: { fontFamily: fonts.sans, fontSize: 16, color: colors.ink },
  selected: { color: colors.goldDeep, fontWeight: "500" },
  check: { fontFamily: fonts.sans, fontSize: 16, color: colors.goldDeep },
});
