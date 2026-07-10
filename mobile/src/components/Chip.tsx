import { Pressable, StyleSheet, Text } from "react-native";
import { colors, fonts } from "../theme";

// A selectable pill used for the date and audience choices in the Filters screen.
export function Chip({
  label,
  active,
  onPress,
}: {
  label: string;
  active: boolean;
  onPress: () => void;
}) {
  return (
    <Pressable onPress={onPress} style={[styles.chip, active && styles.active]}>
      <Text style={[styles.text, active && styles.activeText]}>{label}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  chip: {
    paddingHorizontal: 14,
    paddingVertical: 9,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: colors.line,
    backgroundColor: colors.paperStrong,
  },
  active: {
    backgroundColor: colors.gold,
    borderColor: colors.goldDeep,
  },
  text: { fontFamily: fonts.sans, fontSize: 14, color: colors.ink },
  activeText: { color: colors.white, fontWeight: "500" },
});
