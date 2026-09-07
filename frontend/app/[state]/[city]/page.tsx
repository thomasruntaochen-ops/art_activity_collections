import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { JsonLd } from "../../../components/json-ld";
import {
  ActivityList,
  AppCallout,
  Breadcrumbs,
  plural,
  VenueGrid,
} from "../../../components/seo-sections";
import {
  byStartTime,
  formatActivityTotal,
  getAllVenues,
  getUpcomingActivities,
  venuesInCity,
  venuesInState,
  withUpcomingCounts,
} from "../../../lib/seo-data";
import { SITE_NAME } from "../../../lib/site";
import { slugify } from "../../../lib/slug";
import { stateCodeFromSlug, stateName } from "../../../lib/states";
import { buildBreadcrumbLd, buildCollectionLd } from "../../../lib/structured-data";
import type { VenueSummary } from "../../../lib/types";

export const revalidate = 3600;

// Cities are generated on demand rather than at build time: there are ~170 of
// them and the API rate limits guests to 180 requests a minute, so prerendering
// them all would trip the limiter during a build. Each page is cached once a
// crawler or visitor first requests it.
export const dynamicParams = true;
export async function generateStaticParams() {
  return [];
}

const MAX_ACTIVITIES = 60;

type Props = { params: { state: string; city: string } };

// Resolves the URL slug back to the API's display name. Slugs aren't stored, so
// the city list for the state is the source of truth for the match.
// required: an empty result would 404 a real city rather than surface the
// outage, and an on-demand 404 is cached and served to crawlers.
async function resolveCity(stateCode: string, citySlug: string) {
  const venues = venuesInState(await getAllVenues({ required: true }), stateCode);
  const match = venues.find(
    (venue) => venue.venue_city && slugify(venue.venue_city) === citySlug,
  );
  return match?.venue_city ?? null;
}

function describe(city: string, label: string, venueCount: number, activityCount: number): string {
  const tail =
    activityCount > 0
      ? ` — ${formatActivityTotal(activityCount)} upcoming ${activityCount === 1 ? "activity" : "activities"}`
      : "";
  return `Free art activities, workshops and drop-in studios for kids, teens and families at ${venueCount} art ${venueCount === 1 ? "museum" : "museums"} in ${city}, ${label}${tail}. Updated daily from each museum's own calendar.`;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const code = stateCodeFromSlug(params.state);
  if (!code) return {};
  const city = await resolveCity(code, params.city);
  if (!city) return {};

  const [allVenues, activities] = await Promise.all([
    getAllVenues(),
    getUpcomingActivities({ state: code, city }),
  ]);
  const venues = venuesInCity(allVenues, code, city);

  const label = stateName(code);
  const title = `Free Art Activities for Kids & Families in ${city}, ${label}`;
  const description = describe(city, label, venues.length, activities.length);

  return {
    title,
    description,
    alternates: { canonical: `/${params.state}/${params.city}` },
    openGraph: {
      title: `${title} | ${SITE_NAME}`,
      description,
      url: `/${params.state}/${params.city}`,
    },
    twitter: { title: `${title} | ${SITE_NAME}`, description },
  };
}

export default async function CityPage({ params }: Props) {
  const code = stateCodeFromSlug(params.state);
  if (!code) notFound();

  const city = await resolveCity(code, params.city);
  if (!city) notFound();

  const [allVenues, allActivities] = await Promise.all([
    getAllVenues({ required: true }),
    getUpcomingActivities({ state: code, city, required: true }),
  ]);
  const catalog = venuesInCity(allVenues, code, city);
  if (catalog.length === 0) notFound();
  const venues = withUpcomingCounts(catalog, allActivities);

  const label = stateName(code);
  const activities = byStartTime(allActivities).slice(0, MAX_ACTIVITIES);
  const venuesByName = new Map<string, VenueSummary>(
    venues.map((venue) => [venue.venue_name, venue]),
  );
  const freeCount = allActivities.filter((activity) => activity.is_free).length;

  const path = `/${params.state}/${params.city}`;
  const trail = [
    { name: "Home", path: "/" },
    { name: label, path: `/${params.state}` },
    { name: city, path },
  ];

  return (
    <main className="seo-page">
      <JsonLd
        data={[
          buildCollectionLd({
            name: `Free Art Activities for Kids & Families in ${city}, ${label}`,
            description: describe(city, label, venues.length, allActivities.length),
            path,
            activities,
            venuesByName,
          }),
          buildBreadcrumbLd(trail),
        ]}
      />

      <div className="seo-page__inner">
        <Breadcrumbs trail={trail} />

        <header className="seo-page__header">
          <p className="seo-page__eyebrow">{SITE_NAME}</p>
          <h1 className="seo-page__title">
            Art Activities for Kids &amp; Families in {city}
          </h1>
          <p className="seo-page__lede">
            {venues.length} {plural(venues.length, "art museum")} in {city}, {label} run
            workshops, drop-in studios and family programs
            {allActivities.length > 0 && (
              <>
                {" "}
                — {formatActivityTotal(allActivities.length)}{" "}
                {plural(allActivities.length, "activity", "activities")} coming up
                {freeCount > 0 && `, ${freeCount} of them free`}
              </>
            )}
            . Every listing links back to the museum&rsquo;s official page.
          </p>
        </header>

        <section className="seo-section">
          <h2>Upcoming art activities in {city}</h2>
          <ActivityList activities={activities} />
        </section>

        <AppCallout />

        <section className="seo-section">
          <h2>
            Art museums in {city}, {label}
          </h2>
          <VenueGrid venues={venues} />
        </section>

        <p className="seo-section__more">
          <Link href={`/${params.state}`}>See all art activities in {label}</Link>
        </p>
      </div>
    </main>
  );
}
