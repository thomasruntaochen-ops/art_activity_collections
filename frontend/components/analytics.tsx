import Script from "next/script";
import { SITE_URL } from "../lib/site";

// Cookieless visitor analytics through Umami Cloud (https://cloud.umami.is).
//
// Renders nothing unless NEXT_PUBLIC_UMAMI_WEBSITE_ID is set, so local dev and
// any build without the variable send no data. NEXT_PUBLIC_* values are inlined
// at build time: after setting or changing it on Railway, redeploy the frontend.
const UMAMI_WEBSITE_ID = process.env.NEXT_PUBLIC_UMAMI_WEBSITE_ID?.trim();
const UMAMI_SCRIPT_URL = "https://cloud.umami.is/script.js";

// Only report from the canonical host and its www/apex twin. Even with the ID
// set, a copy served from another host (a Railway *.up.railway.app domain, a
// local production build) stays out of the numbers.
const SITE_HOST = new URL(SITE_URL).hostname.replace(/^www\./, "");
const TRACKED_DOMAINS = `${SITE_HOST},www.${SITE_HOST}`;

export function Analytics() {
  if (!UMAMI_WEBSITE_ID) return null;
  return (
    <Script
      src={UMAMI_SCRIPT_URL}
      strategy="afterInteractive"
      data-website-id={UMAMI_WEBSITE_ID}
      data-domains={TRACKED_DOMAINS}
      // The tracker counts client-side navigations by hooking pushState and
      // replaceState. The explorer rewrites ?view= with replaceState (on load,
      // and from the map/table toggle when it's enabled), so leaving the query
      // string out keeps those rewrites from showing up as extra page views.
      data-exclude-search="true"
    />
  );
}
