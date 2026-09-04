// Canonical identity for every absolute URL the site emits: metadata, Open
// Graph tags, JSON-LD, robots.txt and the sitemap all resolve through here.
//
// Defaults to the production domain, so a deploy is correct without any extra
// configuration. NEXT_PUBLIC_SITE_URL overrides it for staging or a future
// rename; it must be the bare origin with no trailing slash.
//
// The canonical host includes the "www." prefix — whichever form you serve,
// every canonical tag, sitemap entry and Open Graph URL must agree with it, and
// the other form should 301 to it so search engines don't see two sites.
export const SITE_URL = (
  process.env.NEXT_PUBLIC_SITE_URL ?? "https://www.artmuseumactivities.com"
).replace(/\/+$/, "");

export const SITE_NAME = "Art Museum Activities";

export const SITE_DESCRIPTION =
  "Find free art activities, workshops, and drop-in studios for kids, teens, and families at art museums across the United States.";

// Joins a site-relative path onto SITE_URL. Absolute URLs pass through so the
// same helper can be used for outbound museum links.
export function absoluteUrl(path = "/"): string {
  if (/^https?:\/\//i.test(path)) return path;
  return `${SITE_URL}${path.startsWith("/") ? path : `/${path}`}`;
}
