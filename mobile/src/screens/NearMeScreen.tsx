import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import * as Location from "expo-location";
import { useEffect, useState } from "react";
import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import type { RootStackParamList } from "../navigation/types";
import { useFilters } from "../store/filters";
import { colors, fonts, radius, space } from "../theme";

const RADII = [10, 25, 50];

type Status = "loading" | "ready" | "denied" | "error";

type Props = NativeStackScreenProps<RootStackParamList, "NearMe">;

export function NearMeScreen({ navigation }: Props) {
  const insets = useSafeAreaInsets();
  const setUserLocation = useFilters((s) => s.setUserLocation);
  const setRadiusMiles = useFilters((s) => s.setRadiusMiles);
  const clearNearMe = useFilters((s) => s.clearNearMe);
  const currentRadius = useFilters((s) => s.radiusMiles);
  const [status, setStatus] = useState<Status>("loading");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const { status: perm } = await Location.requestForegroundPermissionsAsync();
        if (perm !== "granted") {
          if (!cancelled) setStatus("denied");
          return;
        }
        const pos = await Location.getCurrentPositionAsync({});
        if (cancelled) return;
        setUserLocation({ lat: pos.coords.latitude, lng: pos.coords.longitude });
        setStatus("ready");
      } catch {
        if (!cancelled) setStatus("error");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [setUserLocation]);

  const pick = (miles: number) => {
    setRadiusMiles(miles);
    navigation.goBack();
  };
  const clearAndClose = () => {
    clearNearMe();
    navigation.goBack();
  };

  return (
    <View style={[styles.container, { paddingBottom: insets.bottom + space.lg }]}>
      <Text style={styles.h1}>Museums near me</Text>

      {status === "loading" ? (
        <View style={styles.center}>
          <ActivityIndicator color={colors.gold} />
          <Text style={styles.note}>Finding your location…</Text>
        </View>
      ) : status === "denied" ? (
        <Text style={styles.note}>
          Location access is off. Turn it on for the app in your iPhone Settings, then reopen this.
        </Text>
      ) : status === "error" ? (
        <Text style={styles.note}>We couldn’t get your location. Please try again.</Text>
      ) : (
        <>
          <Text style={styles.note}>Show museums within this distance, closest first.</Text>
          <View style={styles.options}>
            {RADII.map((mi) => {
              const active = currentRadius === mi;
              return (
                <Pressable
                  key={mi}
                  style={[styles.option, active && styles.optionActive]}
                  onPress={() => pick(mi)}
                >
                  <Text style={[styles.optionText, active && styles.optionTextActive]}>
                    Within {mi} miles
                  </Text>
                  <Text style={[styles.chev, active && styles.optionTextActive]}>›</Text>
                </Pressable>
              );
            })}
          </View>
        </>
      )}

      <Pressable style={styles.clear} onPress={clearAndClose}>
        <Text style={styles.clearText}>Any distance</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: colors.paper, padding: space.lg },
  h1: { fontFamily: fonts.serif, fontSize: 24, color: colors.ink, marginBottom: space.sm },
  center: { alignItems: "center", gap: space.sm, marginVertical: space.xl },
  note: { fontFamily: fonts.sans, fontSize: 15, color: colors.muted, marginBottom: space.md },
  options: { gap: space.sm },
  option: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    backgroundColor: colors.paperStrong,
    borderColor: colors.line,
    borderWidth: 1,
    borderRadius: radius.md,
    paddingHorizontal: space.lg,
    paddingVertical: space.lg,
  },
  optionActive: { backgroundColor: colors.gold, borderColor: colors.goldDeep },
  optionText: { fontFamily: fonts.sans, fontSize: 16, color: colors.ink },
  optionTextActive: { color: colors.white, fontWeight: "500" },
  chev: { fontFamily: fonts.sans, fontSize: 18, color: colors.muted },
  clear: {
    marginTop: space.lg,
    alignItems: "center",
    paddingVertical: space.md,
    borderRadius: radius.md,
    borderWidth: 1,
    borderColor: colors.line,
  },
  clearText: { fontFamily: fonts.sans, fontSize: 15, color: colors.ink },
});
