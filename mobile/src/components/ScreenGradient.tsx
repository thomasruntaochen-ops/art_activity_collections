import { LinearGradient } from "expo-linear-gradient";
import { StyleSheet, type StyleProp, type ViewStyle } from "react-native";
import type { ReactNode } from "react";
import { gradient } from "../theme";

type Props = {
  children: ReactNode;
  style?: StyleProp<ViewStyle>;
};

// Full-bleed page wash shared by the content screens. It mirrors the web's
// `html` gradient (same paper base, graded warm-to-deep) so that with the
// section frames gone both platforms read as one continuous surface.
export function ScreenGradient({ children, style }: Props) {
  return (
    <LinearGradient
      colors={gradient.colors}
      locations={gradient.locations}
      style={[styles.fill, style]}
    >
      {children}
    </LinearGradient>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
});
