/**
 * App configuration.
 *
 * API_BASE_URL — where the app fetches museum data from. Set this to either:
 *   • your deployed API,            e.g. "https://your-api.up.railway.app"
 *   • or, when running the API on your Mac, your Mac's LAN IP + port,
 *     e.g. "http://192.168.1.23:8000"
 *     (use the LAN IP, NOT 127.0.0.1 — the phone needs to reach the Mac across wi-fi)
 *
 * MEDIA_BASE_URL — optional host serving the local /venue-photos copies. When
 * left empty, venue cards fall back to each venue's original image URL from
 * venue-media.json, so photos still show without any extra hosting.
 */
export const API_BASE_URL: string = "https://artactivityapi.up.railway.app";

export const MEDIA_BASE_URL: string = "https://artactivity.up.railway.app";

// Public privacy-policy page, linked from the Disclaimer screen and used in the
// App Store Connect "Privacy Policy URL" field.
export const PRIVACY_POLICY_URL: string = "https://artactivity.up.railway.app/privacy";
