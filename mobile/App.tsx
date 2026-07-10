import { NavigationContainer } from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StatusBar } from "expo-status-bar";
import { SafeAreaProvider } from "react-native-safe-area-context";
import type { RootStackParamList } from "./src/navigation/types";
import { ExploreScreen } from "./src/screens/ExploreScreen";
import { FiltersScreen } from "./src/screens/FiltersScreen";
import { MapScreen } from "./src/screens/MapScreen";
import { NearMeScreen } from "./src/screens/NearMeScreen";
import { SelectOptionScreen } from "./src/screens/SelectOptionScreen";
import { VenueDetailScreen } from "./src/screens/VenueDetailScreen";
import { colors } from "./src/theme";

const queryClient = new QueryClient();
const Stack = createNativeStackNavigator<RootStackParamList>();

const headerTheme = {
  headerStyle: { backgroundColor: colors.paper },
  headerTintColor: colors.goldDeep,
  headerShadowVisible: false,
} as const;

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <SafeAreaProvider>
        <StatusBar style="dark" />
        <NavigationContainer>
          <Stack.Navigator
            screenOptions={{ headerShown: false, contentStyle: { backgroundColor: colors.paper } }}
          >
            <Stack.Screen name="Explore" component={ExploreScreen} />
            <Stack.Screen
              name="VenueDetail"
              component={VenueDetailScreen}
              options={{ headerShown: true, title: "", ...headerTheme }}
            />
            <Stack.Screen
              name="Filters"
              component={FiltersScreen}
              options={{ presentation: "modal" }}
            />
            <Stack.Screen
              name="SelectOption"
              component={SelectOptionScreen}
              options={({ route }) => ({ headerShown: true, title: route.params.title, ...headerTheme })}
            />
            <Stack.Screen
              name="Map"
              component={MapScreen}
              options={{ headerShown: true, title: "Map", ...headerTheme }}
            />
            <Stack.Screen
              name="NearMe"
              component={NearMeScreen}
              options={{ presentation: "modal" }}
            />
          </Stack.Navigator>
        </NavigationContainer>
      </SafeAreaProvider>
    </QueryClientProvider>
  );
}
