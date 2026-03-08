"use client";

import { useEffect, useMemo, useState } from "react";
import { ActivityTable } from "../components/activity-table";
import { fetchActivities, fetchFilterOptions } from "../lib/api";
import { Activity } from "../lib/types";

export default function HomePage() {
  const [age, setAge] = useState<string>("");
  const [dropIn, setDropIn] = useState<string>("");
  const [venue, setVenue] = useState<string>("");
  const [city, setCity] = useState<string>("");
  const [state, setState] = useState<string>("");
  const [dateFrom, setDateFrom] = useState<string>("");
  const [dateTo, setDateTo] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [activities, setActivities] = useState<Activity[]>([]);
  const [venueOptions, setVenueOptions] = useState<string[]>([]);
  const [stateOptions, setStateOptions] = useState<string[]>([]);
  const [cityOptions, setCityOptions] = useState<string[]>([]);

  const summary = useMemo(() => `Showing ${activities.length} activity rows`, [activities.length]);

  useEffect(() => {
    const loadOptions = async () => {
      try {
        const options = await fetchFilterOptions();
        setVenueOptions(options.venues);
        setStateOptions(options.states);
        setCityOptions(options.cities);
      } catch {
        setVenueOptions([]);
        setStateOptions([]);
        setCityOptions([]);
      }
    };
    loadOptions();
  }, []);

  async function onSearch() {
    setLoading(true);
    setError("");
    try {
      const rows = await fetchActivities({
        age: age ? Number(age) : undefined,
        drop_in: dropIn === "" ? undefined : dropIn === "true",
        venue: venue || undefined,
        city: city || undefined,
        state: state || undefined,
        date_from: dateFrom ? new Date(`${dateFrom}T00:00:00`).toISOString() : undefined,
        date_to: dateTo ? new Date(`${dateTo}T23:59:59`).toISOString() : undefined,
      });
      setActivities(rows);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="container">
      <header>
        <h1>Art Activity Collection</h1>
        <p>kids/teen art activities</p>
      </header>

      <section className="filters">
        <label>
          Age
          <input
            type="number"
            min={0}
            max={120}
            value={age}
            onChange={(e) => setAge(e.target.value)}
          />
        </label>

        <label>
          Drop-in
          <select value={dropIn} onChange={(e) => setDropIn(e.target.value)}>
            <option value="">Any</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </select>
        </label>

        <label>
          Venue
          <select value={venue} onChange={(e) => setVenue(e.target.value)}>
            <option value="">Any</option>
            {venueOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>

        <label>
          City
          <select value={city} onChange={(e) => setCity(e.target.value)}>
            <option value="">Any</option>
            {cityOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>

        <label>
          State
          <select value={state} onChange={(e) => setState(e.target.value)}>
            <option value="">Any</option>
            {stateOptions.map((code) => (
              <option key={code} value={code}>
                {code}
              </option>
            ))}
          </select>
        </label>

        <label>
          Date From
          <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
        </label>

        <label>
          Date To
          <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
        </label>

        <button onClick={onSearch} disabled={loading}>
          {loading ? "Loading..." : "Search"}
        </button>
      </section>

      <section className="summary">
        <span>{summary}</span>
        {error ? <span className="error">Error: {error}</span> : null}
      </section>

      <ActivityTable activities={activities} />
    </main>
  );
}
