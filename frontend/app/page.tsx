"use client";

import type { CSSProperties } from "react";
import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ActivityTable } from "../components/activity-table";
import { AppleMapsIcon, GoogleMapsIcon } from "../components/map-icons";
import { fetchActivities, fetchFilterOptions, fetchVenueSummaries } from "../lib/api";
import { getVenueMedia } from "../lib/venue-media";
import { haversineMiles, resolveVenueCoordinates } from "../lib/venue-map-data";
import type { Activity, AudienceSegment, VenueSummary } from "../lib/types";

type MapViewportMode = "fit" | "focus" | "usa";
type ViewMode = "map" | "table";
type UserLocation = { lat: number; lng: number };

// "Near me" distance choices (miles) offered in the mobile location panel.
const LOCATION_RANGES = [10, 25, 50];

const AUDIENCE_OPTIONS: { value: "" | AudienceSegment; label: string }[] = [
  { value: "", label: "All audiences" },
  { value: "kids", label: "Kids" },
  // "Teens" and "Adults" each surface teens_adults activities (see the API's
  // _audience_segments), so a separate "Teens & adults" filter is redundant.
  { value: "teens", label: "Teens" },
  { value: "adults", label: "Adults" },
  { value: "all_ages", label: "All ages" },
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

function addMonths(value: Date, months: number): Date {
  const next = new Date(value);
  next.setMonth(next.getMonth() + months);
  return next;
}

function toStartOfDayIso(value: string): string | undefined {
  return value ? new Date(`${value}T00:00:00`).toISOString() : undefined;
}

function toEndOfDayIso(value: string): string | undefined {
  return value ? new Date(`${value}T23:59:59`).toISOString() : undefined;
}

function formatAudienceSegment(value: AudienceSegment): string {
  switch (value) {
    case "kids":
      return "Kids";
    case "teens":
      return "Teens";
    case "teens_adults":
      return "Teens & adults";
    case "adults":
      return "Adults";
    case "all_ages":
      return "All ages";
    default:
      return "Audience TBD";
  }
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

// Build Google/Apple Maps directions URLs for a venue. Prefer stored
// coordinates for accuracy; otherwise fall back to a human-readable address
// query so the maps app can geocode it.
function buildDirectionsTargets(venue: VenueSummary): { google: string; apple: string } {
  const hasCoords = venue.venue_lat != null && venue.venue_lng != null;
  const cityStateZip = [venue.venue_city, venue.venue_state, venue.venue_zip].filter(Boolean).join(" ");
  const addressQuery = [venue.venue_name, venue.venue_address, cityStateZip].filter(Boolean).join(", ");
  const destination = hasCoords ? `${venue.venue_lat},${venue.venue_lng}` : addressQuery;
  const encoded = encodeURIComponent(destination);
  return {
    google: `https://www.google.com/maps/dir/?api=1&destination=${encoded}`,
    apple: `https://maps.apple.com/?daddr=${encoded}`,
  };
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
  const [dateTo, setDateTo] = useState(() => formatDateInput(addMonths(new Date(), 1)));
  const [freeOnly, setFreeOnly] = useState(false);
  const [selectedActivities, setSelectedActivities] = useState<Activity[]>([]);
  const [activityError, setActivityError] = useState("");
  const [activityLoading, setActivityLoading] = useState(false);
  const [tableActivities, setTableActivities] = useState<Activity[]>([]);
  const [tableError, setTableError] = useState("");
  const [tableLoading, setTableLoading] = useState(false);
  const [mapViewportMode, setMapViewportMode] = useState<MapViewportMode>("fit");
  const [isMapOpen, setIsMapOpen] = useState(false);
  const [isFiltersOpen, setIsFiltersOpen] = useState(false);
  const [userLocation, setUserLocation] = useState<UserLocation | null>(null);
  const [locationRange, setLocationRange] = useState<number | null>(null);
  const [isLocationPanelOpen, setIsLocationPanelOpen] = useState(false);
  const [locationLoading, setLocationLoading] = useState(false);
  const [locationError, setLocationError] = useState("");
  const detailRef = useRef<HTMLElement | null>(null);
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
    // Keep Next's App Router routing state on the entry. Replacing it with `{}`
    // strands the entry, so navigating back to it (e.g. Back to close the map
    // overlay) makes the router remount the page and lose the selected venue.
    window.history.replaceState(
      { ...window.history.state },
      "",
      `${url.pathname}${url.search}${url.hash}`,
    );
  }, [viewMode]);

  // Lock background scroll while a full-screen mobile overlay (map or filters)
  // is open. The "near me" panel slides inline and keeps the page scrollable.
  useEffect(() => {
    if (typeof document === "undefined") return;
    document.body.style.overflow = isMapOpen || isFiltersOpen ? "hidden" : "";
    return () => {
      document.body.style.overflow = "";
    };
  }, [isMapOpen, isFiltersOpen]);

  // Let Esc and the device Back gesture close the full-screen mobile map overlay
  // instead of navigating away from the page.
  useEffect(() => {
    if (!isMapOpen || typeof window === "undefined") return;
    // Push a marker history entry so the device Back button/gesture closes the
    // map overlay instead of leaving the page. We reuse an existing marker entry
    // (closing via the button leaves it in place) so repeated open/close cycles
    // don't stack up entries.
    //
    // Crucially we never call `window.history.back()` ourselves: in the Next App
    // Router that popstate remounts the whole page, which would reset the
    // selected venue (so "View activities" always fell back to the first venue).
    const alreadyMarked = (window.history.state as { mapOverlay?: boolean } | null)?.mapOverlay === true;
    if (!alreadyMarked) {
      window.history.pushState({ ...window.history.state, mapOverlay: true }, "");
    }
    const handlePop = () => setIsMapOpen(false);
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setIsMapOpen(false);
    };
    window.addEventListener("popstate", handlePop);
    window.addEventListener("keydown", handleKey);
    return () => {
      window.removeEventListener("popstate", handlePop);
      window.removeEventListener("keydown", handleKey);
    };
  }, [isMapOpen]);

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

  // The DB can hold several venue rows for the same museum (e.g. duplicate
  // "Blanden Memorial Art Museum" records). Their summaries arrive as separate
  // rows with split counts; merge them by name so each museum shows once with a
  // unique identity. Duplicate names would otherwise collide as React keys and
  // break list reconciliation (stale cards surviving a search filter).
  const dedupedVenues = useMemo(() => {
    const merged = new Map<string, VenueSummary>();
    for (const venue of venues) {
      const key = venue.venue_name.trim().toLowerCase();
      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, { ...venue });
        continue;
      }
      existing.activity_count += venue.activity_count;
      existing.free_activity_count = (existing.free_activity_count ?? 0) + (venue.free_activity_count ?? 0);
      if (!existing.venue_address && venue.venue_address) existing.venue_address = venue.venue_address;
      if (!existing.venue_city && venue.venue_city) existing.venue_city = venue.venue_city;
      if (!existing.venue_state && venue.venue_state) existing.venue_state = venue.venue_state;
      if (!existing.venue_zip && venue.venue_zip) existing.venue_zip = venue.venue_zip;
      if (existing.venue_lat == null && venue.venue_lat != null) {
        existing.venue_lat = venue.venue_lat;
        existing.venue_lng = venue.venue_lng;
      }
      if (
        venue.next_activity_at &&
        (!existing.next_activity_at || venue.next_activity_at < existing.next_activity_at)
      ) {
        existing.next_activity_at = venue.next_activity_at;
      }
    }
    return Array.from(merged.values()).sort((a, b) => b.activity_count - a.activity_count);
  }, [venues]);

  const filteredVenues = useMemo(() => {
    const query = searchValue.trim().toLowerCase();
    const matches = dedupedVenues.filter((venue) => {
      const searchText = [venue.venue_name, venue.venue_city, venue.venue_state]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      const matchesSearch = query ? searchText.includes(query) : true;
      return matchesSearch;
    });

    // "Near me": keep venues within the chosen radius of the user and order them
    // closest-first. Venues without stored coordinates fall back to their
    // city/state centroid via resolveVenueCoordinates.
    if (userLocation && locationRange) {
      return matches
        .map((venue) => {
          const resolved = resolveVenueCoordinates(venue);
          const distance = haversineMiles(
            userLocation.lat,
            userLocation.lng,
            resolved.resolvedLat,
            resolved.resolvedLng,
          );
          return { venue, distance };
        })
        .filter((entry) => entry.distance <= locationRange)
        .sort((a, b) => a.distance - b.distance)
        .map((entry) => entry.venue);
    }

    return matches;
  }, [searchValue, dedupedVenues, userLocation, locationRange]);

  useEffect(() => {
    setMapViewportMode("fit");
  }, [selectedState, selectedCity, searchValue, dateFromIso, dateToIso, freeOnly, audienceFilter, locationRange]);

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

  const requestLocation = useCallback(() => {
    if (typeof navigator === "undefined" || !navigator.geolocation) {
      setLocationError("This browser can't share your location, so museums near you aren't available here.");
      return;
    }
    // Browsers only expose geolocation in a secure context (HTTPS or localhost).
    // Over a plain-HTTP LAN address (e.g. http://192.168.x.x:3000 on a phone) the
    // call just fails, so flag that up front instead of showing a vague error.
    if (typeof window !== "undefined" && window.isSecureContext === false) {
      setLocationError("Location needs a secure connection — open this site over HTTPS (or on localhost).");
      return;
    }
    setLocationLoading(true);
    setLocationError("");
    navigator.geolocation.getCurrentPosition(
      (position) => {
        setUserLocation({ lat: position.coords.latitude, lng: position.coords.longitude });
        setLocationLoading(false);
      },
      (error) => {
        const message =
          error.code === error.PERMISSION_DENIED
            ? "Location is turned off for this site. Turn on location access for your browser (and your phone's location services), then tap Try again."
            : error.code === error.POSITION_UNAVAILABLE
              ? "We can't find your location. Make sure location services are turned on for your device, then tap Try again."
              : error.code === error.TIMEOUT
                ? "Finding your location took too long. Tap Try again."
                : "We couldn't access your location. Turn on location access, then tap Try again.";
        setLocationError(message);
        setLocationLoading(false);
      },
      { enableHighAccuracy: true, timeout: 10000, maximumAge: 60000 },
    );
  }, []);

  const handleLocateClick = useCallback(() => {
    // Toggle the inline panel; kick off a location request the first time it opens.
    if (isLocationPanelOpen) {
      setIsLocationPanelOpen(false);
      return;
    }
    setIsLocationPanelOpen(true);
    if (!userLocation) {
      requestLocation();
    }
  }, [isLocationPanelOpen, requestLocation, userLocation]);

  const handlePickRange = useCallback(
    (range: number) => {
      setLocationRange(range);
      // Picking a distance finishes the flow: slide the panel back up.
      setIsLocationPanelOpen(false);
      if (!userLocation && !locationLoading) {
        requestLocation();
      }
    },
    [locationLoading, requestLocation, userLocation],
  );

  const clearLocationFilter = useCallback(() => {
    setLocationRange(null);
    setIsLocationPanelOpen(false);
  }, []);

  // Clear every active filter back to its default. Wired to the "Clear all"
  // button in the Filters group (inline on desktop, in the sheet on mobile).
  const handleResetFilters = useCallback(() => {
    setSelectedState("");
    setSelectedCity("");
    setAudienceFilter("");
    setFreeOnly(false);
    setSearchValue("");
    setAgeFilter("");
    setDropInFilter("");
    setTableVenueName("");
    setDateFrom(formatDateInput(new Date()));
    setDateTo(formatDateInput(addMonths(new Date(), 1)));
    setLocationRange(null);
    setIsLocationPanelOpen(false);
  }, []);

  // From a map popup: close the map, select the venue, and bring its card +
  // activities into view in the explorer below.
  const handleViewVenueActivities = useCallback((venueName: string) => {
    setSelectedVenueName(venueName);
    setMapViewportMode("focus");
    setIsMapOpen(false);
    if (typeof window !== "undefined") {
      window.setTimeout(() => {
        document
          .querySelector(".venue-card.is-active")
          ?.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
        detailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 150);
    }
  }, []);

  // Try to locate the user when the map opens so the "you are here" marker can
  // appear without an explicit tap on the locate button. Guards prevent
  // re-prompting once we have a fix, are mid-request, or already errored.
  useEffect(() => {
    if (isMapOpen && !userLocation && !locationLoading && !locationError) {
      requestLocation();
    }
  }, [isMapOpen, userLocation, locationLoading, locationError, requestLocation]);

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

  // Unused while the Map/Table switch is removed and the table view is disabled.
  // Retained for when the table view is re-enabled.
  // function handleViewChange(nextView: ViewMode) {
  //   if (nextView === "table" && !tableVenueName && selectedVenueName) {
  //     setTableVenueName(selectedVenueName);
  //   }
  //   // The locate button only lives in map view, so collapse the panel on switch.
  //   if (nextView !== "map") {
  //     setIsLocationPanelOpen(false);
  //   }
  //   setViewMode(nextView);
  // }

  const selectedLocation = formatVenueLine(selectedVenue);
  const directionsTargets = useMemo(
    () => (selectedVenue ? buildDirectionsTargets(selectedVenue) : null),
    [selectedVenue],
  );
  const advancedFilterCount =
    (selectedState ? 1 : 0) + (selectedCity ? 1 : 0) + (audienceFilter ? 1 : 0) + (freeOnly ? 1 : 0);
  const activitySummary = useMemo(() => {
    const counts = {
      total: selectedActivities.length,
      free: 0,
      kids: 0,
      teens: 0,
      teensAdults: 0,
      adults: 0,
      allAges: 0,
    };
    for (const activity of selectedActivities) {
      if (activity.is_free === true) counts.free += 1;
      switch (activity.audience_segment) {
        case "kids":
          counts.kids += 1;
          break;
        case "teens":
          counts.teens += 1;
          break;
        case "teens_adults":
          counts.teensAdults += 1;
          break;
        case "adults":
          counts.adults += 1;
          break;
        case "all_ages":
          counts.allAges += 1;
          break;
      }
    }
    return counts;
  }, [selectedActivities]);
  const tableSummary = tableLoading
    ? "Loading matching activities..."
    : `${tableActivities.length} activities matching the current filters`;

  return (
    <main
      className={`explorer-shell${isMapOpen ? " is-map-open" : ""}${isFiltersOpen ? " is-filters-open" : ""}`}
    >
      <header className="explorer-topbar">
        <div className="explorer-brand">Art Museum Activities Explorer</div>
        <div className="explorer-headersearch">
          {viewMode === "map" ? (
            <>
              <label className="explorer-search">
                <span className="sr-only">Search venues</span>
                <input
                  type="search"
                  value={searchValue}
                  onChange={(event) => setSearchValue(event.target.value)}
                  placeholder="Search museums or cities..."
                />
              </label>
              <button
                type="button"
                className={`explorer-locate${locationRange ? " is-active" : ""}`}
                onClick={handleLocateClick}
                aria-label="Find museums near me"
                title="Find museums near me"
              >
                <span aria-hidden="true">◎</span>
              </button>
            </>
          ) : (
            <div className="explorer-toolbar__note">
              Switch between the live venue map and a detailed activity table without leaving the page.
            </div>
          )}
        </div>

        {/* Map/Table view switch removed — the Activity Table view is disabled.
            Retained (commented) for future use.
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
        */}
      </header>

      {/* Inline "near me" panel: slides down under the search/locate row instead
          of opening a modal, so it pushes the content below it down rather than
          covering it. Mile choices are hidden when location is unavailable. */}
      <div className={`location-panel${isLocationPanelOpen ? " is-open" : ""}`}>
        <div className="location-panel__inner">
          <div className="location-panel__card">
            <div className="location-panel__header">
              <p className="location-panel__title">Museums near me</p>
              <button
                type="button"
                className="location-panel__close"
                onClick={() => setIsLocationPanelOpen(false)}
                aria-label="Close"
              >
                ✕
              </button>
            </div>
            {locationError ? (
              <div className="location-panel__notice">
                <p className="status-note is-error">{locationError}</p>
                <button type="button" className="location-panel__retry" onClick={requestLocation}>
                  Try again
                </button>
              </div>
            ) : (
              <>
                <p className="location-panel__hint">Show museums within this distance, closest first.</p>
                {locationLoading ? <p className="status-note">Finding your location…</p> : null}
                <div className="location-panel__options">
                  {LOCATION_RANGES.map((range) => (
                    <button
                      key={range}
                      type="button"
                      className={`location-range${locationRange === range ? " is-active" : ""}`}
                      onClick={() => handlePickRange(range)}
                    >
                      <span>Within {range} mi</span>
                      <span aria-hidden="true">›</span>
                    </button>
                  ))}
                </div>
                {locationRange ? (
                  <button type="button" className="location-panel__clear" onClick={clearLocationFilter}>
                    Clear distance filter
                  </button>
                ) : null}
              </>
            )}
          </div>
        </div>
      </div>

      <section className="explorer-filterbar">
        {/* `display: contents` (desktop) keeps these inline in the grid; on mobile
            this wrapper becomes a bottom sheet opened by the Filters button. */}
        <div className="filters-advanced">
          <p className="filters-advanced__title">Filters</p>

          <label className="explorer-filterbar__control ctl-state">
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

          <label className="explorer-filterbar__control ctl-city">
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

          <label className="explorer-filterbar__control ctl-audience">
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

          <label className={`explorer-toggle ctl-free${freeOnly ? " is-active" : ""}`}>
            <input
              type="checkbox"
              checked={freeOnly}
              onChange={(event) => setFreeOnly(event.target.checked)}
            />
            <span>Free only</span>
          </label>

          <button type="button" className="filters-advanced__reset" onClick={handleResetFilters}>
            Clear all
          </button>

          <button type="button" className="filters-advanced__done" onClick={() => setIsFiltersOpen(false)}>
            Done
          </button>
        </div>

        <label className="explorer-filterbar__control ctl-from">
          <span>From</span>
          <input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} />
        </label>

        <label className="explorer-filterbar__control ctl-to">
          <span>To</span>
          <input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} />
        </label>

        <button type="button" className="filterbar__sheet-trigger" onClick={() => setIsFiltersOpen(true)}>
          Filters
          {advancedFilterCount > 0 ? <span className="filterbar__badge">{advancedFilterCount}</span> : null}
        </button>

        <button type="button" className="filterbar__map-trigger" onClick={() => setIsMapOpen(true)}>
          <span aria-hidden="true">🗺</span> Map view
        </button>
      </section>

      {isFiltersOpen ? (
        <div className="sheet-backdrop" onClick={() => setIsFiltersOpen(false)} aria-hidden="true" />
      ) : null}

      {/* Map/Table switch removed; the Activity Table view below is disabled
          (kept, not rendered). The map view always shows. */}
      <section className="explorer-content">
        <aside className="explorer-sidebar">
          <div className="explorer-sidebar__heading">
            <h1>Venue Explorer</h1>
            <p>Museums with active art programs</p>
            {locationRange ? (
              <span className="explorer-sidebar__nearby">
                {userLocation
                  ? `Nearest first · within ${locationRange} mi`
                  : locationError
                    ? "Location unavailable"
                    : `Locating you · within ${locationRange} mi`}
                <button type="button" onClick={clearLocationFilter}>
                  Clear
                </button>
              </span>
            ) : null}
          </div>

          <div className="explorer-sidebar__list">
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

        <aside className="explorer-detail" ref={detailRef}>
          <p className="eyebrow">Venue Activities</p>
          <h2>{selectedVenue?.venue_name ?? "Select a museum"}</h2>
          <p className="explorer-detail__location">{selectedLocation}</p>
          {selectedVenue && directionsTargets ? (
            <div className="detail-directions">
              <span className="detail-directions__label">Directions</span>
              <a
                className="directions-link"
                href={directionsTargets.google}
                target="_blank"
                rel="noreferrer"
                aria-label="Open directions in Google Maps"
                title="Open in Google Maps"
              >
                <GoogleMapsIcon className="directions-link__icon" />
                <span className="directions-link__text">Google Maps</span>
              </a>
              <a
                className="directions-link"
                href={directionsTargets.apple}
                target="_blank"
                rel="noreferrer"
                aria-label="Open directions in Apple Maps"
                title="Open in Apple Maps"
              >
                <AppleMapsIcon className="directions-link__icon" />
                <span className="directions-link__text">Apple Maps</span>
              </a>
            </div>
          ) : null}
          {selectedVenue ? (
            <p className="explorer-detail__summary">
              <span>{activitySummary.total} total</span>
              <span>{activitySummary.free} free</span>
              {activitySummary.kids > 0 ? <span>{activitySummary.kids} kids</span> : null}
              {activitySummary.teens > 0 ? <span>{activitySummary.teens} teens</span> : null}
              {activitySummary.teensAdults > 0 ? <span>{activitySummary.teensAdults} teens & adults</span> : null}
              {activitySummary.adults > 0 ? <span>{activitySummary.adults} adults</span> : null}
              {activitySummary.allAges > 0 ? <span>{activitySummary.allAges} all ages</span> : null}
            </p>
          ) : null}

          <div className="explorer-detail__section">
            <div className="explorer-detail__section-head">
              <h3>Activities</h3>
              {activityLoading ? <span>Updating...</span> : null}
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
                      {activity.is_free === true ? (
                        <span className="meta-pill meta-pill--confirmed">Free</span>
                      ) : null}
                      {activity.audience_segment !== "unknown" ? (
                        <span className={`meta-pill audience-pill audience-pill--${activity.audience_segment}`}>
                          {formatAudienceSegment(activity.audience_segment)}
                        </span>
                      ) : null}
                    </span>
                  </a>
                ))}
              </div>
            ) : null}
          </div>
        </aside>

        <section className="explorer-map">
          <button
            type="button"
            className="explorer-map__close"
            onClick={() => setIsMapOpen(false)}
            aria-label="Close map"
          >
            ✕
          </button>
          <div className="explorer-map__frame">
            <div className="explorer-map__surface is-live">
              <VenueMap
                venues={filteredVenues}
                selectedVenueName={selectedVenueName}
                viewportMode={mapViewportMode}
                onSelectVenue={handleSelectVenue}
                onResetView={handleResetMapView}
                active={isMapOpen}
                userLocation={userLocation}
                onViewVenueActivities={handleViewVenueActivities}
              />
            </div>
          </div>
        </section>
      </section>
      {/* Activity Table view — disabled, kept for future use. To re-enable, restore the
          `viewMode === "map" ? (...) : (...)` ternary around the map section above and
          this table section.
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
      */}

      <footer className="site-footer">
        <div className="site-footer__inner">
          <p className="site-footer__summary">
            Activity details are gathered automatically from museum websites and may be out of
            date — always confirm dates, prices, ages, and registration with the venue before you
            go.
          </p>
          <details className="site-footer__details">
            <summary className="site-footer__toggle">Read full disclaimer</summary>
            <div className="site-footer__full">
              <p className="site-footer__heading">Disclaimer</p>
              <ul className="site-footer__list">
                <li>
                  <strong>Independent directory.</strong> Art Museum Activities Explorer is an
                  independent guide to art activities for all ages — kids, teens, and adults — and
                  is not affiliated with, endorsed by, or sponsored by any museum or institution
                  listed here.
                </li>
                <li>
                  <strong>Information may be inaccurate or out of date.</strong> Listings are
                  collected automatically from public museum websites. Dates, times, locations,
                  age ranges, registration rules, and availability can change or be removed at any
                  time.
                </li>
                <li>
                  <strong>&ldquo;Free&rdquo; is not a guarantee.</strong> A &ldquo;Free&rdquo;
                  label reflects what we found when the listing was collected; admission,
                  materials, or registration fees may still apply. Confirm pricing with the venue.
                </li>
                <li>
                  <strong>Verify before attending.</strong> Check the museum&rsquo;s official
                  website or contact them directly to confirm an activity is still running and is
                  suitable for you, your child, or your group.
                </li>
                <li>
                  <strong>External links.</strong> Links open third-party websites we don&rsquo;t
                  control and aren&rsquo;t responsible for.
                </li>
                <li>
                  <strong>No warranty.</strong> This site is provided &ldquo;as is,&rdquo; without
                  warranties of any kind, and we are not liable for any loss or inconvenience from
                  reliance on this information.
                </li>
              </ul>
            </div>
          </details>
        </div>
      </footer>
    </main>
  );
}
