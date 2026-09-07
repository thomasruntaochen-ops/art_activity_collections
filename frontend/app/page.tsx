import type { Metadata } from "next";
import Link from "next/link";

import ExplorerClient from "./explorer-client";
import { AppCallout } from "../components/seo-sections";
import { JsonLd } from "../components/json-ld";
import { getAllVenues } from "../lib/seo-data";
import { SITE_DESCRIPTION, SITE_NAME, SITE_URL } from "../lib/site";
import { stateName, stateSlug } from "../lib/states";
import { buildFaqLd } from "../lib/structured-data";

// The explorer's data is refetched in the browser on every visit, so this only
// governs the server-rendered directory summary below it.
export const revalidate = 21600;

const TITLE = "Free Art Activities for Kids & Families at US Art Museums";

export const metadata: Metadata = {
  title: { absolute: TITLE },
  description: SITE_DESCRIPTION,
  alternates: { canonical: "/" },
  openGraph: { url: "/", title: TITLE, description: SITE_DESCRIPTION },
};

export default async function HomePage() {
  const venues = await getAllVenues();

  // Group the directory by state so the footer can link out to every state
  // page. A sitemap alone is a weak discovery signal; real internal links are
  // how crawlers actually reach and weight these pages.
  const byState = new Map<string, number>();
  for (const venue of venues) {
    if (!venue.venue_state) continue;
    byState.set(venue.venue_state, (byState.get(venue.venue_state) ?? 0) + 1);
  }
  const states = [...byState.entries()].sort((a, b) =>
    stateName(a[0]).localeCompare(stateName(b[0])),
  );

  const museumCount = venues.length;
  const faq = [
    {
      question: "Are the art activities listed here free?",
      answer:
        "Many are. Each listing shows whether the museum publishes the activity as free, and you can filter to free-only activities. Admission terms change, so confirm on the museum's own page before you go.",
    },
    {
      question: "What ages are these art activities for?",
      answer:
        "Listings cover kids, teens, adults and all-ages family programs. You can filter by a specific age or by audience to see only activities that fit.",
    },
    {
      question: "How current are the listings?",
      answer: `${SITE_NAME} refreshes listings daily from each museum's official events calendar, and every activity links back to that museum's page as the authoritative source.`,
    },
    {
      question: "Which museums are covered?",
      answer: `${museumCount} art museums across ${byState.size} US states and territories, including major institutions and regional and university art museums.`,
    },
  ];

  return (
    <>
      <JsonLd
        data={[
          {
            "@context": "https://schema.org",
            "@type": "WebSite",
            name: SITE_NAME,
            url: SITE_URL,
            description: SITE_DESCRIPTION,
            // Lets Google offer a search box for the site directly in results.
            potentialAction: {
              "@type": "SearchAction",
              target: {
                "@type": "EntryPoint",
                urlTemplate: `${SITE_URL}/?q={search_term_string}`,
              },
              "query-input": "required name=search_term_string",
            },
          },
          buildFaqLd(faq),
        ]}
      />
      <ExplorerClient
        footerSlot={
          <>
          <AppCallout />
          <section className="seo-directory" aria-labelledby="browse-by-state">
            <h2 className="seo-directory__title" id="browse-by-state">
              Browse art museum activities by state
            </h2>
            <p className="seo-directory__lede">
              {museumCount} art museums across {byState.size} states and territories publish free
              and low-cost workshops, drop-in studios and family programs. Pick a state to see
              what&rsquo;s coming up near you.
            </p>
            <ul className="seo-directory__grid">
              {states.map(([code, count]) => (
                <li key={code}>
                  <Link className="seo-directory__link" href={`/${stateSlug(code)}`}>
                    {stateName(code)}
                    <span className="seo-directory__count">{count}</span>
                  </Link>
                </li>
              ))}
            </ul>

            <h2 className="seo-directory__title seo-directory__title--faq">
              Common questions
            </h2>
            <dl className="seo-directory__faq">
              {faq.map((entry) => (
                <div key={entry.question}>
                  <dt>{entry.question}</dt>
                  <dd>{entry.answer}</dd>
                </div>
              ))}
            </dl>
          </section>
          </>
        }
      />
    </>
  );
}
