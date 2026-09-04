import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import { notFound } from "next/navigation";

import { JsonLd } from "../../../components/json-ld";
import { ActivityList, Breadcrumbs, plural } from "../../../components/seo-sections";
import { byStartTime, getAllVenues, getUpcomingActivities } from "../../../lib/seo-data";
import { SITE_NAME } from "../../../lib/site";
import { findBySlug, slugify } from "../../../lib/slug";
import { stateName, stateSlug } from "../../../lib/states";
import { getVenueMedia } from "../../../lib/venue-media";
import {
  buildBreadcrumbLd,
  buildEventLd,
  buildMuseumLd,
  streetAddress,
} from "../../../lib/structured-data";

export const revalidate = 3600;

// Like the city pages, generated on first request to stay inside the API's
// guest rate limit rather than issuing 200+ calls during a build.
export const dynamicParams = true;
export async function generateStaticParams() {
  return [];
}

type Props = { params: { slug: string } };

// required: see the city page — a failed lookup must not masquerade as an
// unknown venue and get cached as a 404.
async function resolveVenue(slug: string) {
  const venues = await getAllVenues({ required: true });
  return findBySlug(venues, slug, (venue) => venue.venue_name) ?? null;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const venue = await resolveVenue(params.slug);
  if (!venue) return {};

  const activities = await getUpcomingActivities({ venue: venue.venue_name });
  const where = [venue.venue_city, venue.venue_state && stateName(venue.venue_state)]
    .filter(Boolean)
    .join(", ");

  const title = `Art Activities & Workshops at ${venue.venue_name}`;
  const description = `${activities.length > 0 ? `${activities.length} upcoming art activities` : "Art activities"} for kids, teens and families at ${venue.venue_name}${where ? ` in ${where}` : ""} — workshops, drop-in studios and family programs, updated daily from the museum's own calendar.`;

  const media = getVenueMedia(venue.venue_name);

  return {
    title,
    description,
    alternates: { canonical: `/venue/${params.slug}` },
    openGraph: {
      title: `${title} | ${SITE_NAME}`,
      description,
      url: `/venue/${params.slug}`,
      type: "website",
      images: media ? [{ url: media.image_path, alt: venue.venue_name }] : undefined,
    },
    twitter: { title: `${title} | ${SITE_NAME}`, description },
  };
}

export default async function VenuePage({ params }: Props) {
  const venue = await resolveVenue(params.slug);
  if (!venue) notFound();

  const activities = byStartTime(
    await getUpcomingActivities({ venue: venue.venue_name, required: true }),
  );
  const media = getVenueMedia(venue.venue_name);
  const stateLabel = venue.venue_state ? stateName(venue.venue_state) : null;
  const freeCount = activities.filter((activity) => activity.is_free).length;
  // Omitted when the address column only repeats the city line below it.
  const street = streetAddress(venue.venue_address, venue.venue_city, venue.venue_state);

  const path = `/venue/${params.slug}`;
  const trail = [
    { name: "Home", path: "/" },
    ...(venue.venue_state
      ? [{ name: stateLabel as string, path: `/${stateSlug(venue.venue_state)}` }]
      : []),
    ...(venue.venue_state && venue.venue_city
      ? [
          {
            name: venue.venue_city,
            path: `/${stateSlug(venue.venue_state)}/${slugify(venue.venue_city)}`,
          },
        ]
      : []),
    { name: venue.venue_name, path },
  ];

  return (
    <main className="seo-page">
      <JsonLd
        data={[
          buildMuseumLd(venue, path),
          buildBreadcrumbLd(trail),
          // Events are emitted alongside the Museum entity so each program can
          // surface on its own in event results.
          ...activities.map((activity) => ({
            "@context": "https://schema.org",
            ...buildEventLd(activity, venue),
          })),
        ]}
      />

      <div className="seo-page__inner">
        <Breadcrumbs trail={trail} />

        <header className="seo-page__header">
          <p className="seo-page__eyebrow">
            {[venue.venue_city, stateLabel].filter(Boolean).join(", ")}
          </p>
          <h1 className="seo-page__title">Art Activities at {venue.venue_name}</h1>
          <p className="seo-page__lede">
            {activities.length > 0 ? (
              <>
                {activities.length} upcoming art{" "}
                {plural(activities.length, "activity", "activities")}
                {freeCount > 0 && `, ${freeCount} of them free`} for kids, teens and families at{" "}
                {venue.venue_name}
                {venue.venue_city && ` in ${venue.venue_city}`}.
              </>
            ) : (
              <>
                {venue.venue_name}
                {venue.venue_city && ` in ${venue.venue_city}`} runs art workshops and family
                programs. No dates are listed right now — the museum&rsquo;s own calendar has the
                latest schedule.
              </>
            )}
          </p>
        </header>

        {media && (
          <Image
            className="seo-page__photo"
            src={media.image_path}
            alt={venue.venue_name}
            width={1200}
            height={630}
            priority
          />
        )}

        <section className="seo-section">
          <h2>Visiting</h2>
          <dl className="seo-facts">
            {street && (
              <div>
                <dt>Address</dt>
                <dd>
                  {street}
                  {venue.venue_zip && ` ${venue.venue_zip}`}
                </dd>
              </div>
            )}
            {venue.venue_city && (
              <div>
                <dt>City</dt>
                <dd>
                  {venue.venue_state ? (
                    <Link
                      href={`/${stateSlug(venue.venue_state)}/${slugify(venue.venue_city)}`}
                    >
                      {venue.venue_city}, {stateLabel}
                    </Link>
                  ) : (
                    venue.venue_city
                  )}
                </dd>
              </div>
            )}
            {media?.website && (
              <div>
                <dt>Official site</dt>
                <dd>
                  <a href={media.website} rel="nofollow noopener">
                    {media.website.replace(/^https?:\/\//, "").replace(/\/$/, "")}
                  </a>
                </dd>
              </div>
            )}
          </dl>
        </section>

        <section className="seo-section">
          <h2>Upcoming activities at {venue.venue_name}</h2>
          <ActivityList activities={activities} showVenue={false} />
        </section>

        {venue.venue_state && (
          <p className="seo-section__more">
            <Link href={`/${stateSlug(venue.venue_state)}`}>
              More art activities across {stateLabel}
            </Link>
          </p>
        )}
      </div>
    </main>
  );
}
