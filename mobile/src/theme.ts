import { Platform } from "react-native";

// Palette ported from the web app's globals.css :root tokens so the native app
// shares the same warm "paper + gold" identity.
export const colors = {
  paper: "#f5efe6",
  paperStrong: "#fbf8f2",
  ink: "#1f1b18",
  muted: "#7d7269",
  line: "rgba(101, 84, 69, 0.16)",
  lineStrong: "rgba(101, 84, 69, 0.28)",
  gold: "#c89031",
  goldDeep: "#8a6224",
  white: "#ffffff",
  cardScrim: "rgba(22, 18, 14, 0.55)",
};

// iOS ships these exact font families, so the app matches the site's typography.
export const fonts = {
  serif: Platform.select({ ios: "Iowan Old Style", default: "serif" }) as string,
  sans: Platform.select({ ios: "Avenir Next", default: "System" }) as string,
};

export const radius = { sm: 10, md: 16, lg: 22 };

export const space = { xs: 4, sm: 8, md: 12, lg: 16, xl: 24 };
