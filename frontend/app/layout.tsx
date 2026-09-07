import type { Metadata, Viewport } from "next";
import "leaflet/dist/leaflet.css";
import "./globals.css";
import { JsonLd } from "../components/json-ld";
import { APP_STORE_ID, SITE_DESCRIPTION, SITE_NAME, SITE_URL } from "../lib/site";
import { buildSiteLd } from "../lib/structured-data";

export const metadata: Metadata = {
  // metadataBase lets every page below declare canonical and Open Graph URLs
  // as plain relative paths and have them resolved to absolute ones.
  metadataBase: new URL(SITE_URL),
  title: {
    default: "Free Art Activities for Kids & Families at US Art Museums",
    // Section pages supply just their own name.
    template: `%s | ${SITE_NAME}`,
  },
  description: SITE_DESCRIPTION,
  applicationName: SITE_NAME,
  category: "Arts & Family",
  alternates: { canonical: "/" },
  // Emits <meta name="apple-itunes-app">, which is what draws Safari's Smart
  // App Banner on iOS. It only appears in Safari, so it supplements the visible
  // AppCallout rather than replacing it.
  itunes: { appId: APP_STORE_ID },
  openGraph: {
    type: "website",
    siteName: SITE_NAME,
    locale: "en_US",
    url: "/",
    title: "Free Art Activities for Kids & Families at US Art Museums",
    description: SITE_DESCRIPTION,
  },
  twitter: {
    card: "summary_large_image",
    title: "Free Art Activities for Kids & Families at US Art Museums",
    description: SITE_DESCRIPTION,
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      // Without these Google clips previews, which suppresses rich results and
      // large image cards in Discover.
      "max-image-preview": "large",
      "max-snippet": -1,
      "max-video-preview": -1,
    },
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <JsonLd data={buildSiteLd()} />
        {children}
      </body>
    </html>
  );
}
