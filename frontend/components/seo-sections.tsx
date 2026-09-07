import Link from "next/link";

import { APP_STORE_URL } from "../lib/site";
import { getVenueMedia } from "../lib/venue-media";
import { slugify } from "../lib/slug";
import { stateName, stateSlug } from "../lib/states";
import type { Activity, VenueSummary } from "../lib/types";

// Activity times are naive wall-clock in the museum's own timezone, so they are
// formatted from the literal parts. Converting through the server's timezone
// would shift every listing by the deployment region's offset.
export function formatActivityDate(naive: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/.exec(naive);
  if (!match) return naive;
  const [, year, month, day, hour, minute] = match;
  const asUtc = new Date(Date.UTC(+year, +month - 1, +day, +hour, +minute));
  const datePart = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    weekday: "short",
    month: "short",
    day: "numeric",
  }).format(asUtc);
  const timePart = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    hour: "numeric",
    minute: "2-digit",
  }).format(asUtc);
  return `${datePart} · ${timePart}`;
}

// Returns the whole word rather than a bare suffix. Writing {n} activit{"y"}
// in JSX emits separate text nodes ("activit", "y") separated by a React
// comment marker; browsers show that correctly, but naive HTML-to-text
// extractors — which is how several AI crawlers read a page — turn it into
// "activit y". Interpolating a complete word avoids that.
export function plural(count: number, singular: string, pluralForm?: string): string {
  return count === 1 ? singular : (pluralForm ?? `${singular}s`);
}

// The Smart App Banner declared in app/layout.tsx only draws in Safari on iOS,
// which leaves Chrome, Android and every desktop visitor with no route to the
// app at all. This is the visible counterpart, and it goes on the pages that
// actually receive search traffic rather than only on the homepage.
export function AppCallout() {
  return (
    <aside className="app-callout">
      <div>
        <p className="app-callout__title">Take these listings with you</p>
        <p className="app-callout__body">
          The free iPhone app maps what&rsquo;s near you, filters by age and date, and adds an
          activity to your calendar in one tap.
        </p>
      </div>
      <a
        className="app-callout__cta"
        href={APP_STORE_URL}
        target="_blank"
        rel="noopener"
      >
        Get the free iPhone app
      </a>
    </aside>
  );
}

export function Breadcrumbs({ trail }: { trail: { name: string; path: string }[] }) {
  return (
    <nav className="seo-crumbs" aria-label="Breadcrumb">
      {trail.map((crumb, index) => (
        <span key={crumb.path}>
          {index > 0 && <span className="seo-crumbs__sep">/</span>}
          {index === trail.length - 1 ? (
            <span aria-current="page">{crumb.name}</span>
          ) : (
            <Link href={crumb.path}>{crumb.name}</Link>
          )}
        </span>
      ))}
    </nav>
  );
}

// Renders the activity list as a description list of real, linked text. The
// point is that this exists in the served HTML: crawlers that don't run
// JavaScript see the actual programming, not an empty shell.
export function ActivityList({
  activities,
  showVenue = true,
}: {
  activities: Activity[];
  showVenue?: boolean;
}) {
  if (activities.length === 0) {
    return (
      <p className="seo-empty">
        No upcoming activities are listed right now. Listings refresh daily as museums publish
        their next season — check the museum links below for the latest schedule.
      </p>
    );
  }

  return (
    <ul className="seo-activities">
      {activities.map((activity) => (
        <li className="seo-activity" key={activity.id}>
          <a className="seo-activity__title" href={activity.source_url} rel="nofollow noopener">
            {activity.title}
          </a>
          <p className="seo-activity__meta">
            <time dateTime={activity.start_at}>{formatActivityDate(activity.start_at)}</time>
            {showVenue && activity.venue_name && (
              <>
                {" · "}
                <Link href={`/venue/${slugify(activity.venue_name)}`}>{activity.venue_name}</Link>
              </>
            )}
            {activity.venue_city && ` · ${activity.venue_city}`}
          </p>
          <p className="seo-activity__tags">
            {activity.is_free && <span className="seo-tag seo-tag--free">Free</span>}
            {activity.activity_type && <span className="seo-tag">{activity.activity_type}</span>}
            {activity.drop_in && <span className="seo-tag">Drop-in</span>}
            {activity.registration_required && (
              <span className="seo-tag">Registration required</span>
            )}
            {(activity.age_min !== null || activity.age_max !== null) && (
              <span className="seo-tag">
                Ages {activity.age_min ?? 0}
                {activity.age_max !== null ? `–${activity.age_max}` : "+"}
              </span>
            )}
          </p>
        </li>
      ))}
    </ul>
  );
}

export function VenueGrid({ venues }: { venues: VenueSummary[] }) {
  if (venues.length === 0) return null;
  return (
    <ul className="seo-venues">
      {venues.map((venue) => {
        const media = getVenueMedia(venue.venue_name);
        return (
          <li className="seo-venue" key={venue.venue_name}>
            <Link className="seo-venue__link" href={`/venue/${slugify(venue.venue_name)}`}>
              <span className="seo-venue__name">{venue.venue_name}</span>
              <span className="seo-venue__place">
                {[venue.venue_city, venue.venue_state].filter(Boolean).join(", ")}
              </span>
              <span className="seo-venue__count">
                {venue.activity_count > 0
                  ? `${venue.activity_count} upcoming${
                      (venue.free_activity_count ?? 0) > 0
                        ? ` · ${venue.free_activity_count} free`
                        : ""
                    }`
                  : "Check the museum calendar"}
              </span>
            </Link>
            {media?.website && (
              <a className="seo-venue__site" href={media.website} rel="nofollow noopener">
                Official site
              </a>
            )}
          </li>
        );
      })}
    </ul>
  );
}

export function CityLinks({ state, cities }: { state: string; cities: string[] }) {
  if (cities.length === 0) return null;
  return (
    <ul className="seo-chiplist">
      {cities.map((city) => (
        <li key={city}>
          <Link className="seo-chip" href={`/${stateSlug(state)}/${slugify(city)}`}>
            {city}
          </Link>
        </li>
      ))}
    </ul>
  );
}

export function StateLinks({ states }: { states: string[] }) {
  return (
    <ul className="seo-chiplist">
      {states.map((code) => (
        <li key={code}>
          <Link className="seo-chip" href={`/${stateSlug(code)}`}>
            {stateName(code)}
          </Link>
        </li>
      ))}
    </ul>
  );
}
