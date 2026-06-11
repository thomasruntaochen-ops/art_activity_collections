"use client";

import type { CSSProperties } from "react";
import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useState } from "react";
import { ActivityTable } from "../components/activity-table";
import { fetchActivities, fetchFilterOptions, fetchVenueSummaries } from "../lib/api";
import { getVenueMedia } from "../lib/venue-media";
import type { Activity, AudienceSegment, VenueSummary } from "../lib/types";

type MapViewportMode = "fit" | "focus" | "usa";
type ViewMode = "map" | "table";

const AUDIENCE_OPTIONS: { value: "" | AudienceSegment; label: string }[] = [
  { value: "", label: "All audiences" },
  { value: "kids", label: "Kids" },
  { value: "teens", label: "Teens" },
  { value: "adults", label: "Adults" },
  { value: "all_ages", label: "All ages" },
  { value: "unknown", label: "Unknown" },
];

function hashString(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

function formatActivityTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatDateInput(value: Date): string {
  const year = value.getFullYear();
  const month = `${value.getMonth() + 1}`.padStart(2, "0");
  const day = `${value.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function toStartOfDayIso(value: string): string | undefined {
  return value ? new Date(`${value}T00:00:00`).toISOString() : undefined;
}

function toEndOfDayIso(value: string): string | undefined {
  return value ? new Date(`${value}T23:59:59`).toISOString() : undefined;
}

function formatAgeRange(min: number | null, max: number | null): string {
  if (min === null && max === null) return "Age TBD";
  if (min !== null && max !== null) return `Ages ${min}-${max}`;
  if (min !== null) return `Ages ${min}+`;
  return `Up to ${max}`;
}

function formatAudienceSegment(value: AudienceSegment): string {
  switch (value) {
    case "kids":
      return "Kids";
    case "teens":
      return "Teens";
    case "adults":
      return "Adults";
    case "all_ages":
      return "All ages";
    default:
      return "Audience TBD";
  }
}

function getFreeLabel(activity: Activity): string {
  if (activity.is_free === true) {
    return activity.free_verification_status === "confirmed" ? "Free" : "Likely free";
  }
  if (activity.free_verification_status === "uncertain") {
    return "Price unclear";
  }
  return "Check price";
}

function getFreeTone(activity: Activity): string {
  if (activity.is_free === true && activity.free_verification_status === "confirmed") {
    return "confirmed";
  }
  if (activity.is_free === true) {
    return "inferred";
  }
  if (activity.free_verification_status === "uncertain") {
    return "warning";
  }
  return "neutral";
}

function formatVenueLine(venue: VenueSummary | null): string {
  if (!venue) return "No venue selected";
  const cityStateZip = [venue.venue_city, venue.venue_state, venue.venue_zip].filter(Boolean).join(" ");
  if (venue.venue_address) {
    return [venue.venue_address, cityStateZip].filter(Boolean).join(", ");
  }
  const parts = [venue.venue_city, venue.venue_state, venue.venue_zip].filter(Boolean);
  return parts.join(", ") || "Location pending";
}

function buildVenueCardStyle(seed: string, imageUrl?: string | null): CSSProperties {
  const hue = hashString(seed) % 360;
  const secondaryHue = (hue + 42) % 360;
  const layers = [
    "linear-gradient(180deg, rgba(22, 18, 14, 0.1), rgba(22, 18, 14, 0.76))",
  ];
  if (imageUrl) {
    layers.push(`url("${imageUrl}")`);
  } else {
    layers.push(`linear-gradient(135deg, hsl(${hue}, 42%, 22%), hsl(${secondaryHue}, 36%, 12%))`);
  }
  return {
    backgroundImage: layers.join(", "),
    backgroundSize: imageUrl ? "auto, cover" : "auto",
    backgroundPosition: "center",
    backgroundRepeat: imageUrl ? "no-repeat, no-repeat" : "no-repeat",
  };
}

const VenueMap = dynamic(
  () => import("../components/venue-map").then((module) => module.VenueMap),
  {
    ssr: false,
    loading: () => <div className="venue-map__loading">Loading interactive map...</div>,
  },
);

export default function HomePage() {
  const [viewMode, setViewMode] = useState<ViewMode>("map");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [venues, setVenues] = useState<VenueSummary[]>([]);
  const [stateOptions, setStateOptions] = useState<string[]>([]);
  const [cityOptions, setCityOptions] = useState<string[]>([]);
  const [venueOptions, setVenueOptions] = useState<string[]>([]);
  const [selectedVenueName, setSelectedVenueName] = useState("");
  const [tableVenueName, setTableVenueName] = useState("");
  const [selectedState, setSelectedState] = useState("");
  const [selectedCity, setSelectedCity] = useState("");
  const [searchValue, setSearchValue] = useState("");
  const [ageFilter, setAgeFilter] = useState("");
  const [audienceFilter, setAudienceFilter] = useState<"" | AudienceSegment>("");
  const [dropInFilter, setDropInFilter] = useState("");
  const [dateFrom, setDateFrom] = useState(() => formatDateInput(new Date()));
  const [dateTo, setDateTo] = useState("");
  const [freeOnly, setFreeOnly] = useState(false);
  const [selectedActivities, setSelectedActivities] = useState<Activity[]>([]);
  const [activityError, setActivityError] = useState("");
  const [activityLoading, setActivityLoading] = useState(false);
  const [tableActivities, setTableActivities] = useState<Activity[]>([]);
  const [tableError, setTableError] = useState("");
  const [tableLoading, setTableLoading] = useState(false);
  const [mapViewportMode, setMapViewportMode] = useState<MapViewportMode>("fit");
  const [mobilePane, setMobilePane] = useState<"map" | "activities">("map");
  const dateFromIso = useMemo(() => toStartOfDayIso(dateFrom), [dateFrom]);
  const dateToIso = useMemo(() => toEndOfDayIso(dateTo), [dateTo]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    if (params.get("view") === "table") {
      setViewMode("table");
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    if (viewMode === "map") {
      url.searchParams.delete("view");
    } else {
      url.searchParams.set("view", viewMode);
    }
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, [viewMode]);

  useEffect(() => {
    let cancelled = false;

    async function loadOptions() {
      try {
        const audience = audienceFilter || undefined;
        const basePromise = fetchFilterOptions({ free_only: freeOnly, audience });
        const [baseOptions, stateScopedOptions, cityScopedOptions, combinedOptions] = await Promise.all([
          basePromise,
          selectedState ? fetchFilterOptions({ state: selectedState, free_only: freeOnly, audience }) : basePromise,
          selectedCity ? fetchFilterOptions({ city: selectedCity, free_only: freeOnly, audience }) : basePromise,
          selectedState || selectedCity
            ? fetchFilterOptions({
                state: selectedState || undefined,
                city: selectedCity || undefined,
                free_only: freeOnly,
                audience,
              })
            : basePromise,
        ]);
        if (cancelled) return;
        const nextCities = selectedState ? stateScopedOptions.cities : baseOptions.cities;
        const nextStates = selectedCity ? cityScopedOptions.states : baseOptions.states;
        setCityOptions(nextCities);
        setStateOptions(nextStates);
        setVenueOptions(combinedOptions.venues);
        setSelectedCity((current) => (current && !nextCities.includes(current) ? "" : current));
        setSelectedState((current) => (current && !nextStates.includes(current) ? "" : current));
        setTableVenueName((current) => (current && !combinedOptions.venues.includes(current) ? "" : current));
      } catch {
        if (cancelled) return;
        setCityOptions([]);
        setStateOptions([]);
        setVenueOptions([]);
      }
    }

    loadOptions();
    return () => {
      cancelled = true;
    };
  }, [selectedCity, selectedState, freeOnly, audienceFilter]);

  useEffect(() => {
    let cancelled = false;

    async function loadExplorer() {
      setLoading(true);
      setError("");
      try {
        const venueRows = await fetchVenueSummaries({
          state: selectedState || undefined,
          city: selectedCity || undefined,
          date_from: dateFromIso,
          date_to: dateToIso,
          free_only: freeOnly,
          audience: audienceFilter || undefined,
          limit: 150,
        });
        if (cancelled) return;
        setVenues(venueRows);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Unable to load venue explorer");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadExplorer();
    return () => {
      cancelled = true;
    };
  }, [selectedState, selectedCity, dateFromIso, dateToIso, freeOnly, audienceFilter]);

  const filteredVenues = useMemo(() => {
    const query = searchValue.trim().toLowerCase();
    return venues.filter((venue) => {
      const searchText = [venue.venue_name, venue.venue_city, venue.venue_state]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      const matchesSearch = query ? searchText.includes(query) : true;
      return matchesSearch;
    });
  }, [searchValue, venues]);

  useEffect(() => {
    setMapViewportMode("fit");
  }, [selectedState, selectedCity, searchValue, dateFromIso, dateToIso, freeOnly, audienceFilter]);

  useEffect(() => {
    if (filteredVenues.length === 0) {
      if (selectedVenueName) {
        setSelectedVenueName("");
      }
      return;
    }
    if (!filteredVenues.some((venue) => venue.venue_name === selectedVenueName)) {
      setSelectedVenueName(filteredVenues[0].venue_name);
    }
  }, [filteredVenues, selectedVenueName]);

  const handleSelectVenue = useCallback((venueName: string) => {
    setSelectedVenueName(venueName);
    setMapViewportMode("focus");
  }, []);

  const handleResetMapView = useCallback(() => {
    setMapViewportMode("usa");
  }, []);

  const selectedVenue = useMemo(
    () => filteredVenues.find((venue) => venue.venue_name === selectedVenueName) ?? null,
    [filteredVenues, selectedVenueName],
  );

  useEffect(() => {
    if (!selectedVenueName) {
      setSelectedActivities([]);
      setActivityError("");
      return;
    }

    let cancelled = false;

    async function loadVenueActivities() {
      setActivityLoading(true);
      setActivityError("");
      try {
        const rows = await fetchActivities({
          venue: selectedVenueName,
          city: selectedCity || undefined,
          state: selectedState || undefined,
          date_from: dateFromIso,
          date_to: dateToIso,
          free_only: freeOnly,
          audience: audienceFilter || undefined,
        });
        if (cancelled) return;
        setSelectedActivities(rows);
      } catch (err) {
        if (cancelled) return;
        setActivityError(err instanceof Error ? err.message : "Unable to load venue activities");
        setSelectedActivities([]);
      } finally {
        if (!cancelled) {
          setActivityLoading(false);
        }
      }
    }

    loadVenueActivities();
    return () => {
      cancelled = true;
    };
  }, [selectedVenueName, selectedCity, selectedState, dateFromIso, dateToIso, freeOnly, audienceFilter]);

  useEffect(() => {
    if (viewMode !== "table") {
      return;
    }

    let cancelled = false;

    async function loadTableActivities() {
      setTableLoading(true);
      setTableError("");
      try {
        const rows = await fetchActivities({
          age: ageFilter ? Number(ageFilter) : undefined,
          drop_in: dropInFilter === "" ? undefined : dropInFilter === "true",
          venue: tableVenueName || undefined,
          city: selectedCity || undefined,
          state: selectedState || undefined,
          date_from: dateFromIso,
          date_to: dateToIso,
          free_only: freeOnly,
          audience: audienceFilter || undefined,
        });
        if (cancelled) return;
        setTableActivities(rows);
      } catch (err) {
        if (cancelled) return;
        setTableError(err instanceof Error ? err.message : "Unable to load activity table");
        setTableActivities([]);
      } finally {
        if (!cancelled) {
          setTableLoading(false);
        }
      }
    }

    loadTableActivities();
    return () => {
      cancelled = true;
    };
  }, [
    viewMode,
    tableVenueName,
    selectedCity,
    selectedState,
    ageFilter,
    audienceFilter,
    dropInFilter,
    dateFromIso,
    dateToIso,
    freeOnly,
  ]);

  function handleViewChange(nextView: ViewMode) {
    if (nextView === "table" && !tableVenueName && selectedVenueName) {
      setTableVenueName(selectedVenueName);
    }
    setViewMode(nextView);
  }

  const selectedLocation = formatVenueLine(selectedVenue);
  const visibleVenueCount = filteredVenues.length;
  const stateLabel = selectedState || "All states";
  const tableSummary = tableLoading
    ? "Loading matching activities..."
    : `${tableActivities.length} activities matching the current filters`;

  return (
    <main className="explorer-shell">
      <header className="explorer-topbar">
        <div className="explorer-brand">Art Museum Activities Explorer</div>
        <div className="explorer-headersearch">
          {viewMode === "map" ? (
            <label className="explorer-search">
              <span className="sr-only">Search venues</span>
              <input
                type="search"
                value={searchValue}
                onChange={(event) => setSearchValue(event.target.value)}
                placeholder="Search museums or cities..."
              />
            </label>
          ) : (
            <div className="explorer-toolbar__note">
              Switch between the live venue map and a detailed activity table without leaving the page.
            </div>
          )}
        </div>

        <div className="view-switch" role="tablist" aria-label="Explorer view">
          <button
            type="button"
            className={`view-switch__button${viewMode === "map" ? " is-active" : ""}`}
            onClick={() => handleViewChange("map")}
          >
            Map
          </button>
          <button
            type="button"
            className={`view-switch__button${viewMode === "table" ? " is-active" : ""}`}
            onClick={() => handleViewChange("table")}
          >
            Table
          </button>
        </div>
      </header>

      <section className="explorer-filterbar">
        <label className="explorer-filterbar__control">
          <span>State</span>
          <select value={selectedState} onChange={(event) => setSelectedState(event.target.value)}>
            <option value="">All states</option>
            {stateOptions.map((state) => (
              <option key={state} value={state}>
                {state}
              </option>
            ))}
          </select>
        </label>

        <label className="explorer-filterbar__control">
          <span>City</span>
          <select value={selectedCity} onChange={(event) => setSelectedCity(event.target.value)}>
            <option value="">All cities</option>
            {cityOptions.map((city) => (
              <option key={city} value={city}>
                {city}
              </option>
            ))}
          </select>
        </label>

        <label className="explorer-filterbar__control">
          <span>Audience</span>
          <select
            value={audienceFilter}
            onChange={(event) => setAudienceFilter(event.target.value as "" | AudienceSegment)}
          >
            {AUDIENCE_OPTIONS.map((option) => (
              <option key={option.value || "all"} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="explorer-filterbar__control">
          <span>From</span>
          <input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} />
        </label>

        <label className="explorer-filterbar__control">
          <span>To</span>
          <input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} />
        </label>

        <label className={`explorer-toggle${freeOnly ? " is-active" : ""}`}>
          <input
            type="checkbox"
            checked={freeOnly}
            onChange={(event) => setFreeOnly(event.target.checked)}
          />
          <span>Free only</span>
        </label>
      </section>

      {viewMode === "map" ? (
      <section className={`explorer-content is-pane-${mobilePane}`}>
        <aside className="explorer-sidebar">
          <div className="explorer-sidebar__heading">
            <h1>Venue Explorer</h1>
            <p>{visibleVenueCount} museums with active art programs</p>
          </div>

          <div className="explorer-sidebar__list">
            <div className="explorer-sidebar__list-head">
              <span>{stateLabel}</span>
              <span>{visibleVenueCount} visible</span>
            </div>

            {loading ? <p className="status-note">Loading venues...</p> : null}
            {error ? <p className="status-note is-error">{error}</p> : null}
            {!loading && !error && filteredVenues.length === 0 ? (
              <p className="status-note">No venues match this filter.</p>
            ) : null}

            <div className="explorer-sidebar__track">
            {filteredVenues.map((venue) => {
              const isActive = venue.venue_name === selectedVenueName;
              const venueMedia = getVenueMedia(venue.venue_name);
              return (
                <button
                  key={venue.venue_name}
                  type="button"
                  className={`venue-card${isActive ? " is-active" : ""}`}
                  style={buildVenueCardStyle(venue.venue_name, venueMedia?.image_path)}
                  onClick={() => handleSelectVenue(venue.venue_name)}
                >
                  <span className="venue-card__content">
                    <span className="venue-card__title">{venue.venue_name}</span>
                    <span className="venue-card__meta">
                      {[venue.venue_city, venue.venue_state].filter(Boolean).join(", ") || "Location pending"}
                    </span>
                    <span className="venue-card__count">{venue.activity_count} programs</span>
                  </span>
                </button>
              );
            })}
            </div>
          </div>
        </aside>

        <div className="explorer-paneswitch view-switch" role="tablist" aria-label="Map or activities view">
          <button
            type="button"
            className={`view-switch__button${mobilePane === "map" ? " is-active" : ""}`}
            onClick={() => setMobilePane("map")}
          >
            Map
          </button>
          <button
            type="button"
            className={`view-switch__button${mobilePane === "activities" ? " is-active" : ""}`}
            onClick={() => setMobilePane("activities")}
          >
            Activities
          </button>
        </div>

        <aside className="explorer-detail">
          <p className="eyebrow">Venue Activities</p>
          <h2>{selectedVenue?.venue_name ?? "Select a museum"}</h2>
          <p className="explorer-detail__location">{selectedLocation}</p>

          <div className="explorer-detail__section">
            <div className="explorer-detail__section-head">
              <h3>Activities</h3>
              <span>{activityLoading ? "Updating..." : `${selectedActivities.length} loaded`}</span>
            </div>

            {activityError ? <p className="status-note is-error">{activityError}</p> : null}
            {!activityError && !activityLoading && selectedActivities.length === 0 ? (
              <p className="status-note">No activities available for this venue.</p>
            ) : null}

            {selectedActivities.length > 0 ? (
              <div className="activity-list">
                {selectedActivities.map((activity) => (
                  <a
                    key={activity.id}
                    className="activity-listing"
                    href={activity.source_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    <span className="activity-listing__title">{activity.title}</span>
                    <span className="activity-listing__meta">
                      <span className="activity-listing__time">{formatActivityTime(activity.start_at)}</span>
                      <span className={`meta-pill meta-pill--${getFreeTone(activity)}`}>{getFreeLabel(activity)}</span>
                      <span className="meta-pill meta-pill--neutral">
                        {formatAgeRange(activity.age_min, activity.age_max)}
                      </span>
                      <span className="meta-pill meta-pill--neutral">
                        {formatAudienceSegment(activity.audience_segment)}
                      </span>
                      {activity.activity_type ? (
                        <span className="meta-pill meta-pill--soft">{activity.activity_type}</span>
                      ) : null}
                    </span>
                  </a>
                ))}
              </div>
            ) : null}
          </div>
        </aside>

        <section className="explorer-map">
          <div className="explorer-map__frame">
            <div className="explorer-map__surface is-live">
              <VenueMap
                venues={filteredVenues}
                selectedVenueName={selectedVenueName}
                viewportMode={mapViewportMode}
                onSelectVenue={handleSelectVenue}
                onResetView={handleResetMapView}
              />
            </div>
          </div>
        </section>
      </section>
      ) : (
        <section className="table-shell">
          <div className="table-shell__header">
            <div>
              <p className="eyebrow">Detailed Activity Index</p>
              <h1>Activity Table</h1>
              <p className="table-shell__lead">{tableSummary}</p>
            </div>

            <div className="table-shell__actions">
              <button
                type="button"
                className="directions-toggle"
                onClick={() => {
                  if (selectedVenueName) {
                    setTableVenueName(selectedVenueName);
                  }
                }}
                disabled={!selectedVenueName}
              >
                Use Map Selection
              </button>
              <button
                type="button"
                className="directions-toggle"
                onClick={() => setTableVenueName("")}
                disabled={!tableVenueName}
              >
                Clear Venue
              </button>
            </div>
          </div>

          <div className="table-controls">
            <label className="explorer-filterbar__control">
              <span>Venue</span>
              <select value={tableVenueName} onChange={(event) => setTableVenueName(event.target.value)}>
                <option value="">All venues</option>
                {venueOptions.map((venue) => (
                  <option key={venue} value={venue}>
                    {venue}
                  </option>
                ))}
              </select>
            </label>

            <label className="explorer-filterbar__control">
              <span>Audience</span>
              <select
                value={audienceFilter}
                onChange={(event) => setAudienceFilter(event.target.value as "" | AudienceSegment)}
              >
                {AUDIENCE_OPTIONS.map((option) => (
                  <option key={option.value || "all"} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="explorer-filterbar__control">
              <span>Age</span>
              <input
                type="number"
                min={0}
                max={120}
                value={ageFilter}
                onChange={(event) => setAgeFilter(event.target.value)}
                placeholder="Any age"
              />
            </label>

            <label className="explorer-filterbar__control">
              <span>Drop-in</span>
              <select value={dropInFilter} onChange={(event) => setDropInFilter(event.target.value)}>
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </select>
            </label>
          </div>

          {tableError ? <p className="status-note is-error">{tableError}</p> : null}
          {tableLoading ? <p className="status-note">Loading activities...</p> : <ActivityTable activities={tableActivities} />}
        </section>
      )}
    </main>
  );
}
