import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Privacy Policy — Art Museum Activities",
  description:
    "Privacy policy for the Art Museum Activities app and website: no accounts, no tracking, and no personal data collected.",
};

export default function PrivacyPage() {
  return (
    <main className="legal-page">
      <div className="legal-page__inner">
        <p className="legal-page__eyebrow">Art Museum Activities</p>
        <h1 className="legal-page__title">Privacy Policy</h1>
        <p className="legal-page__updated">Effective July 15, 2026</p>

        <p className="legal-page__lede">
          Art Museum Activities — the iPhone app and this website — is a free guide to art
          activities at museums. The short version: <strong>we don&rsquo;t collect, store, sell,
          or share your personal information.</strong> There are no accounts, no ads, no
          trackers, and no analytics.
        </p>

        <section className="legal-page__section">
          <h2>Information we collect</h2>
          <p>
            None. You never sign up, log in, or give us your name, email address, or any other
            personal details to use the app or the website.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Location</h2>
          <p>
            The app&rsquo;s &ldquo;Near Me&rdquo; feature can use your device&rsquo;s location to
            sort museums by distance. This is optional — the app asks for permission first, and it
            works fine if you say no. Your location is used <strong>only on your device</strong>{" "}
            and is never sent to our servers or anyone else. You can turn access off anytime in
            Settings &rarr; Privacy &amp; Security &rarr; Location Services.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Calendar</h2>
          <p>
            If you use &ldquo;add to calendar,&rdquo; the app asks for calendar permission and
            creates the event directly on your device. We never read your existing calendar events
            and never transmit any calendar data.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Saved favorites and filters</h2>
          <p>
            Museums you save and filters you choose are stored only on your device. They never
            leave it, and deleting the app deletes them.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Server logs</h2>
          <p>
            When the app or website loads activity listings from our server, our hosting provider
            may keep standard technical logs (such as IP addresses and request times) to run and
            secure the service, as virtually all web services do. We don&rsquo;t use these logs to
            identify anyone.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Third-party services</h2>
          <p>
            Maps in the iPhone app are provided by Apple Maps; the map on this website loads tiles
            from CARTO and OpenStreetMap. Activity listings link out to museums&rsquo; official
            websites. These third parties have their own privacy policies, and we don&rsquo;t
            control them.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Children&rsquo;s privacy</h2>
          <p>
            This is a family-friendly guide meant for parents and caregivers planning museum
            visits. We don&rsquo;t knowingly collect personal information from anyone — children
            or adults.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Changes to this policy</h2>
          <p>
            If the app ever changes how it handles data, we&rsquo;ll update this page and the
            effective date above before the change takes effect.
          </p>
        </section>

        <section className="legal-page__section">
          <h2>Contact</h2>
          <p>
            Questions? Email{" "}
            <a className="legal-page__link" href="mailto:thomasruntaochen@gmail.com">
              thomasruntaochen@gmail.com
            </a>
            .
          </p>
        </section>

        <p className="legal-page__back">
          <Link className="legal-page__link" href="/">
            &larr; Back to Art Museum Activities
          </Link>
        </p>
      </div>
    </main>
  );
}
