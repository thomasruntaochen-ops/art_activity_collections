import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";

import { JsonLd } from "../../components/json-ld";
import {
  ActivityList,
  AppCallout,
  Breadcrumbs,
  CityLinks,
  plural,
  VenueGrid,
} from "../../components/seo-sections";
import {
  byStartTime,
  formatActivityTotal,
  getAllVenues,
  getUpcomingActivities,
  venuesInState,
  withUpcomingCounts,
} from "../../lib/seo-data";
import { SITE_NAME } from "../../lib/site";
import { stateCodeFromSlug, stateName, stateSlug } from "../../lib/states";
import { buildBreadcrumbLd, buildCollectionLd } from "../../lib/structured-data";
import type { VenueSummary } from "../../lib/types";

export const revalidate = 3600;

// Only the 42 states that actually have museums get prerendered at build time.
// Anything else falls through to notFound(), so this dynamic segment can't
// swallow arbitrary paths and mint thin pages.
export async function generateStaticParams() {
  const venues = await getAllVenues();
  const codes = new Set(venues.map((venue) => venue.venue_state).filter(Boolean) as string[]);
  return [...codes].map((code) => ({ state: stateSlug(code) }));
}

// How many events to render. Enough to prove depth to a crawler without
// building a page nobody can read.
const MAX_ACTIVITIES = 60;

type Props = { params: { state: string } };

function describe(code: string, venueCount: number, activityCount: number): string {
  const label = stateName(code);
  const lead =
    activityCount > 0
      ? `${formatActivityTotal(activityCount)} upcoming free art ${activityCount === 1 ? "activity" : "activities"}`
      : "Free art activities";
  return `${lead}, workshops and drop-in studios for kids, teens and families at ${venueCount} art ${venueCount === 1 ? "museum" : "museums"} across ${label}. Updated daily from each museum's own calendar.`;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const code = stateCodeFromSlug(params.state);
  if (!code) return {};

  const [allVenues, activities] = await Promise.all([
    getAllVenues(),
    getUpcomingActivities({ state: code }),
  ]);
  const venues = venuesInState(allVenues, code);


  const label = stateName(code);
  const title = `Free Art Activities for Kids & Families in ${label}`;
  const description = describe(code, venues.length, activities.length);

  return {
    title,
    description,
    alternates: { canonical: `/${params.state}` },
    openGraph: {
      title: `${title} | ${SITE_NAME}`,
      description,
      url: `/${params.state}`,
    },
    twitter: { title: `${title} | ${SITE_NAME}`, description },
  };
}

export default async function StatePage({ params }: Props) {
  const code = stateCodeFromSlug(params.state);
  if (!code) notFound();

  // required: an API failure here would look like "this state has no museums"
  // and 404 a page that should exist. Failing the render is the safer error.
  const [allVenues, allActivities] = await Promise.all([
    getAllVenues({ required: true }),
    getUpcomingActivities({ state: code, required: true }),
  ]);
  const catalog = venuesInState(allVenues, code);
  const venues = withUpcomingCounts(catalog, allActivities);

  // Only reachable for a state the catalog no longer covers: generateStaticParams
  // builds this list from the same source, and the fetch above throws rather
  // than returning empty on failure.
  if (catalog.length === 0) notFound();

  const label = stateName(code);
  const activities = byStartTime(allActivities).slice(0, MAX_ACTIVITIES);
  const venuesByName = new Map<string, VenueSummary>(
    venues.map((venue) => [venue.venue_name, venue]),
  );
  const cities = [
    ...new Set(venues.map((venue) => venue.venue_city).filter(Boolean) as string[]),
  ].sort();
  const freeCount = allActivities.filter((activity) => activity.is_free).length;

  const path = `/${params.state}`;
  const trail = [
    { name: "Home", path: "/" },
    { name: label, path },
  ];

  return (
    <main className="seo-page">
      <JsonLd
        data={[
          buildCollectionLd({
            name: `Free Art Activities for Kids & Families in ${label}`,
            description: describe(code, venues.length, allActivities.length),
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
          <h1 className="seo-page__title">Art Activities for Kids &amp; Families in {label}</h1>
          <p className="seo-page__lede">
            {venues.length} {plural(venues.length, "art museum")} in {label} publish
            workshops, drop-in studios, and family programs
            {allActivities.length > 0 && (
              <>
                {" "}
                — {formatActivityTotal(allActivities.length)}{" "}
                {plural(allActivities.length, "activity", "activities")} coming up
                {freeCount > 0 && `, ${freeCount} of them free`}
              </>
            )}
            . Listings come straight from each museum&rsquo;s own events calendar and refresh
            daily.
          </p>
        </header>

        {cities.length > 1 && (
          <section className="seo-section">
            <h2>Browse by city</h2>
            <CityLinks state={code} cities={cities} />
          </section>
        )}

        <section className="seo-section">
          <h2>
            Upcoming art activities in {label}
            {activities.length > 0 && (
              <span className="seo-section__count">
                {activities.length} of {formatActivityTotal(allActivities.length)}
              </span>
            )}
          </h2>
          <ActivityList activities={activities} />
          {allActivities.length > activities.length && (
            <p className="seo-section__more">
              <Link href="/">Search all {formatActivityTotal(allActivities.length)} activities</Link>{" "}
              by age, date and audience.
            </p>
          )}
        </section>

        <AppCallout />

        <section className="seo-section">
          <h2>Art museums in {label}</h2>
          <VenueGrid venues={venues} />
        </section>
      </div>
    </main>
  );
}
