import { Ionicons } from "@expo/vector-icons";
import { NavigationContainer } from "@react-navigation/native";
import { createBottomTabNavigator } from "@react-navigation/bottom-tabs";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StatusBar } from "expo-status-bar";
import { SafeAreaProvider } from "react-native-safe-area-context";
import type { RootStackParamList, TabParamList } from "./src/navigation/types";
import { DisclaimerScreen } from "./src/screens/DisclaimerScreen";
import { ExploreScreen } from "./src/screens/ExploreScreen";
import { FiltersScreen } from "./src/screens/FiltersScreen";
import { MapScreen } from "./src/screens/MapScreen";
import { NearMeScreen } from "./src/screens/NearMeScreen";
import { SavedScreen } from "./src/screens/SavedScreen";
import { SelectOptionScreen } from "./src/screens/SelectOptionScreen";
import { VenueDetailScreen } from "./src/screens/VenueDetailScreen";
import { colors, fonts } from "./src/theme";

const queryClient = new QueryClient();
const Stack = createNativeStackNavigator<RootStackParamList>();
const Tab = createBottomTabNavigator<TabParamList>();

const headerTheme = {
  headerStyle: { backgroundColor: colors.paper },
  headerTintColor: colors.goldDeep,
  headerShadowVisible: false,
} as const;

function Tabs() {
  return (
    <Tab.Navigator
      screenOptions={{
        headerShown: false,
        tabBarActiveTintColor: colors.goldDeep,
        tabBarInactiveTintColor: colors.muted,
        tabBarStyle: { backgroundColor: colors.paper, borderTopColor: colors.line },
        tabBarLabelStyle: { fontFamily: fonts.sans, fontSize: 11 },
      }}
    >
      <Tab.Screen
        name="ExploreTab"
        component={ExploreScreen}
        options={{
          title: "Explore",
          tabBarIcon: ({ color, size, focused }) => (
            <Ionicons name={focused ? "compass" : "compass-outline"} size={size ?? 22} color={color} />
          ),
        }}
      />
      <Tab.Screen
        name="SavedTab"
        component={SavedScreen}
        options={{
          title: "Saved",
          tabBarIcon: ({ color, size, focused }) => (
            <Ionicons name={focused ? "heart" : "heart-outline"} size={size ?? 22} color={color} />
          ),
        }}
      />
    </Tab.Navigator>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <SafeAreaProvider>
        <StatusBar style="dark" />
        <NavigationContainer>
          <Stack.Navigator
            screenOptions={{ headerShown: false, contentStyle: { backgroundColor: colors.paper } }}
          >
            <Stack.Screen name="Tabs" component={Tabs} />
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
            <Stack.Screen
              name="Disclaimer"
              component={DisclaimerScreen}
              options={{
                presentation: "modal",
                headerShown: true,
                title: "Disclaimer",
                ...headerTheme,
              }}
            />
          </Stack.Navigator>
        </NavigationContainer>
      </SafeAreaProvider>
    </QueryClientProvider>
  );
}
