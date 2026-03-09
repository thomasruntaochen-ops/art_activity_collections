"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ActivityTable } from "../components/activity-table";
import { fetchActivities, fetchFilterOptions } from "../lib/api";
import { Activity, ActivityFilterOptions } from "../lib/types";

function formatDateInput(date: Date): string {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

export default function HomePage() {
  const [dateBounds, setDateBounds] = useState<{
    defaultFrom: string;
    defaultTo: string;
    minFrom: string;
  } | null>(null);
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
  const [allOptions, setAllOptions] = useState<ActivityFilterOptions>({
    venues: [],
    states: [],
    cities: [],
  });
  const [venueOptions, setVenueOptions] = useState<string[]>([]);
  const [stateOptions, setStateOptions] = useState<string[]>([]);
  const [cityOptions, setCityOptions] = useState<string[]>([]);
  const optionsRequestIdRef = useRef(0);

  const summary = useMemo(() => `Showing ${activities.length} activity rows`, [activities.length]);

  useEffect(() => {
    const today = new Date();
    const nextBounds = {
      defaultFrom: formatDateInput(today),
      defaultTo: formatDateInput(addDays(today, 30)),
      minFrom: formatDateInput(addDays(today, -3)),
    };
    setDateBounds(nextBounds);
    setDateFrom(nextBounds.defaultFrom);
    setDateTo(nextBounds.defaultTo);
  }, []);

  useEffect(() => {
    const loadInitialOptions = async () => {
      try {
        const options = await fetchFilterOptions();
        setAllOptions(options);
        setVenueOptions(options.venues);
        setStateOptions(options.states);
        setCityOptions(options.cities);
      } catch {
        setAllOptions({ venues: [], states: [], cities: [] });
        setVenueOptions([]);
        setStateOptions([]);
        setCityOptions([]);
      }
    };
    loadInitialOptions();
  }, []);

  useEffect(() => {
    if (
      allOptions.venues.length === 0 &&
      allOptions.states.length === 0 &&
      allOptions.cities.length === 0
    ) {
      return;
    }

    const requestId = optionsRequestIdRef.current + 1;
    optionsRequestIdRef.current = requestId;

    const loadDependentOptions = async () => {
      try {
        const [stateScoped, cityScoped, combinedScoped] = await Promise.all([
          state ? fetchFilterOptions({ state }) : Promise.resolve(allOptions),
          city ? fetchFilterOptions({ city }) : Promise.resolve(allOptions),
          state || city ? fetchFilterOptions({ state: state || undefined, city: city || undefined }) : Promise.resolve(allOptions),
        ]);

        if (optionsRequestIdRef.current !== requestId) {
          return;
        }

        const nextCityOptions = state ? stateScoped.cities : allOptions.cities;
        const nextStateOptions = city ? cityScoped.states : allOptions.states;
        const nextVenueOptions = state || city ? combinedScoped.venues : allOptions.venues;

        setCityOptions(nextCityOptions);
        setStateOptions(nextStateOptions);
        setVenueOptions(nextVenueOptions);

        setCity((current) => (current && !nextCityOptions.includes(current) ? "" : current));
        setState((current) => (current && !nextStateOptions.includes(current) ? "" : current));
        setVenue((current) => (current && !nextVenueOptions.includes(current) ? "" : current));
      } catch {
        if (optionsRequestIdRef.current !== requestId) {
          return;
        }
        setCityOptions(allOptions.cities);
        setStateOptions(allOptions.states);
        setVenueOptions(allOptions.venues);
      }
    };

    loadDependentOptions();
  }, [allOptions, city, state]);

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
          <input
            type="date"
            min={dateBounds?.minFrom}
            value={dateFrom}
            onChange={(e) =>
              setDateFrom(
                dateBounds && e.target.value < dateBounds.minFrom ? dateBounds.minFrom : e.target.value,
              )
            }
          />
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
