"use client";

import L, { type LayerGroup, type Map as LeafletMap, type TileLayer } from "leaflet";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { resolveVenueCoordinates, type ResolvedVenueCoordinates } from "../lib/venue-map-data";
import { VenueSummary } from "../lib/types";

type Props = {
  venues: VenueSummary[];
  selectedVenueName: string;
  viewportMode: "fit" | "focus" | "usa";
  onSelectVenue: (venueName: string) => void;
  onResetView: () => void;
  // True while the mobile full-screen overlay shows the map. Used to force a
  // Leaflet resize when the container goes from hidden to full-screen.
  active?: boolean;
  // The user's resolved geolocation, drawn as a distinct "you are here" marker.
  userLocation?: { lat: number; lng: number } | null;
  // Invoked from a venue popup link to jump back to that venue's activities.
  onViewVenueActivities?: (venueName: string) => void;
};

// When a venue is selected we nudge in to this zoom instead of diving all the way
// to street level, so neighbouring venues stay on screen for context.
const FOCUS_MIN_ZOOM = 8;
const SELECTED_ACCENT = "#1a73e8";

// The default, fully zoomed-out view of the continental US.
const USA_CENTER: [number, number] = [39.8283, -98.5795];
const USA_ZOOM = 4;

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNextActivity(iso: string | null): string | null {
  if (!iso) return null;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return null;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function buildVenueSummaryHtml(venue: ResolvedVenueCoordinates): string {
  const location = [venue.venue_city, venue.venue_state].filter(Boolean).join(", ") || "Location pending";
  const programs = `${venue.activity_count} live ${venue.activity_count === 1 ? "program" : "programs"}`;
  const freeCount = venue.free_activity_count ?? 0;
  const freePrograms = `${freeCount} free ${freeCount === 1 ? "program" : "programs"}`;
  const next = formatNextActivity(venue.next_activity_at);
  return [
    `<strong class="venue-map__summary-name">${escapeHtml(venue.venue_name)}</strong>`,
    `<span class="venue-map__summary-location">${escapeHtml(location)}</span>`,
    `<span class="venue-map__summary-count">${escapeHtml(programs)}</span>`,
    `<span class="venue-map__summary-free">${escapeHtml(freePrograms)}</span>`,
    next ? `<span class="venue-map__summary-next">Next: ${escapeHtml(next)}</span>` : "",
  ]
    .filter(Boolean)
    .join("");
}

// Build the selected venue's popup as a real DOM node (not an HTML string) so we
// can wire a click handler onto the "view activities" link.
function buildVenueSummaryPopup(
  venue: ResolvedVenueCoordinates,
  onViewVenueActivities?: (venueName: string) => void,
): HTMLElement {
  const container = document.createElement("div");
  container.className = "venue-map__summary-body";
  container.innerHTML = buildVenueSummaryHtml(venue);
  if (onViewVenueActivities) {
    const link = document.createElement("button");
    link.type = "button";
    link.className = "venue-map__summary-link";
    link.textContent = "View activities →";
    link.addEventListener("click", () => onViewVenueActivities(venue.venue_name));
    container.appendChild(link);
  }
  return container;
}

export function VenueMap({
  venues,
  selectedVenueName,
  viewportMode,
  onSelectVenue,
  onResetView,
  active,
  userLocation,
  onViewVenueActivities,
}: Props) {
  const [containerNode, setContainerNode] = useState<HTMLDivElement | null>(null);
  const mapRef = useRef<LeafletMap | null>(null);
  const markersRef = useRef<LayerGroup | null>(null);
  const primaryTilesRef = useRef<TileLayer | null>(null);
  const fallbackTilesRef = useRef<TileLayer | null>(null);
  const usingFallbackTilesRef = useRef(false);
  const tileErrorCountRef = useRef(0);
  const resolvedVenues = useMemo(() => venues.map(resolveVenueCoordinates), [venues]);
  const [mapNotice, setMapNotice] = useState("");
  const [mapInitError, setMapInitError] = useState("");

  useEffect(() => {
    if (!containerNode || mapRef.current) return;

    let resizeObserver: ResizeObserver | null = null;

    try {
      const map = L.map(containerNode, {
        zoomControl: false,
        attributionControl: true,
        minZoom: 3,
        maxZoom: 18,
        zoomSnap: 1,
        zoomDelta: 1,
        wheelPxPerZoomLevel: 100,
        wheelDebounceTime: 60,
        fadeAnimation: true,
        markerZoomAnimation: true,
      });

      L.control.zoom({ position: "bottomright" }).addTo(map);
      L.control.scale({ position: "bottomleft", imperial: true, metric: false }).addTo(map);

      const primaryTiles = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: "abcd",
        maxZoom: 18,
        maxNativeZoom: 18,
        detectRetina: true,
        crossOrigin: true,
      });
      const fallbackTiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 18,
        maxNativeZoom: 18,
        detectRetina: true,
        crossOrigin: true,
      });

      primaryTiles.on("load", () => {
        tileErrorCountRef.current = 0;
        if (!usingFallbackTilesRef.current) {
          setMapNotice("");
        }
      });
      primaryTiles.on("tileerror", () => {
        tileErrorCountRef.current += 1;
        if (usingFallbackTilesRef.current || tileErrorCountRef.current < 3) return;
        usingFallbackTilesRef.current = true;
        setMapNotice("Map tiles switched to fallback mode.");
        if (map.hasLayer(primaryTiles)) {
          map.removeLayer(primaryTiles);
        }
        fallbackTiles.addTo(map);
      });
      fallbackTiles.on("load", () => {
        if (usingFallbackTilesRef.current) {
          setMapNotice("Map tiles switched to fallback mode.");
        }
      });
      fallbackTiles.on("tileerror", () => {
        if (usingFallbackTilesRef.current) {
          setMapNotice("Map tiles are unavailable right now.");
        }
      });

      primaryTiles.addTo(map);

      map.setView(USA_CENTER, USA_ZOOM);
      mapRef.current = map;
      markersRef.current = L.layerGroup().addTo(map);
      primaryTilesRef.current = primaryTiles;
      fallbackTilesRef.current = fallbackTiles;
      setMapInitError("");

      resizeObserver = new ResizeObserver((entries) => {
        const entry = entries[0];
        if (!entry) return;
        const { width, height } = entry.contentRect;
        if (width <= 0 || height <= 0) return;
        map.invalidateSize({ animate: false });
      });
      resizeObserver.observe(containerNode);
      requestAnimationFrame(() => {
        map.invalidateSize({ animate: false });
      });
    } catch (error) {
      setMapInitError(error instanceof Error ? error.message : "Unable to initialize the map.");
    }

    return () => {
      resizeObserver?.disconnect();
      markersRef.current?.clearLayers();
      primaryTilesRef.current?.off();
      fallbackTilesRef.current?.off();
      mapRef.current?.remove();
      mapRef.current = null;
      markersRef.current = null;
      primaryTilesRef.current = null;
      fallbackTilesRef.current = null;
    };
  }, [containerNode]);

  // Pan/zoom the map to match the current selection — but only when the map is
  // actually on screen. On mobile the map lives in a `display:none` container
  // until the overlay opens; running Leaflet's view math on a 0x0 map throws
  // (the "client-side exception"), so bail out until it has a real size.
  const applyView = useCallback(() => {
    const map = mapRef.current;
    if (!map) return;
    const size = map.getSize();
    if (size.x === 0 || size.y === 0) return;

    if (resolvedVenues.length === 0) {
      map.setView(USA_CENTER, USA_ZOOM);
      return;
    }

    if (viewportMode === "usa") {
      map.flyTo(USA_CENTER, USA_ZOOM, { duration: 0.6 });
      return;
    }

    const bounds = L.latLngBounds(resolvedVenues.map((venue) => L.latLng(venue.resolvedLat, venue.resolvedLng)));
    if (viewportMode === "fit" && bounds.isValid()) {
      map.fitBounds(bounds, {
        padding: [40, 40],
        maxZoom: 8,
        animate: true,
        duration: 0.35,
      });
      return;
    }

    const selectedVenue = resolvedVenues.find((venue) => venue.venue_name === selectedVenueName) ?? null;
    if (selectedVenue) {
      // Nudge in just enough to centre the pick without dropping its neighbours.
      const nextZoom = Math.max(map.getZoom(), FOCUS_MIN_ZOOM);
      map.flyTo([selectedVenue.resolvedLat, selectedVenue.resolvedLng], nextZoom, {
        duration: 0.45,
      });
    }
  }, [resolvedVenues, selectedVenueName, viewportMode]);

  useEffect(() => {
    const map = mapRef.current;
    const layerGroup = markersRef.current;
    if (!map || !layerGroup) return;

    layerGroup.clearLayers();
    const mapSize = map.getSize();
    const hasSize = mapSize.x > 0 && mapSize.y > 0;

    resolvedVenues.forEach((venue) => {
      const isSelected = venue.venue_name === selectedVenueName;
      const halo = L.circleMarker([venue.resolvedLat, venue.resolvedLng], {
        radius: isSelected ? 18 : 9,
        color: isSelected ? "rgba(26, 115, 232, 0.22)" : "rgba(26, 115, 232, 0.08)",
        fillColor: isSelected ? "rgba(26, 115, 232, 0.22)" : "rgba(26, 115, 232, 0.08)",
        fillOpacity: isSelected ? 0.28 : 0.08,
        weight: 0,
        interactive: false,
      });
      const marker = L.circleMarker([venue.resolvedLat, venue.resolvedLng], {
        radius: isSelected ? 9.5 : 5,
        color: "#ffffff",
        fillColor: isSelected ? SELECTED_ACCENT : "#5f87c7",
        fillOpacity: 1,
        weight: isSelected ? 2.5 : 1.25,
      });

      if (isSelected) {
        marker.bindPopup(buildVenueSummaryPopup(venue, onViewVenueActivities), {
          className: "venue-map__summary",
          offset: [0, -8],
          autoPan: false,
          closeButton: true,
        });
      } else {
        marker.bindTooltip(escapeHtml(venue.venue_name), {
          direction: "top",
          offset: [0, -10],
          opacity: 0.98,
          className: "venue-map__tooltip",
        });
      }

      marker.on("click", () => {
        onSelectVenue(venue.venue_name);
      });

      halo.addTo(layerGroup);
      marker.addTo(layerGroup);

      if (isSelected) {
        marker.bringToFront();
        // Surface the venue summary in the highlighted dot once the user has
        // actively picked it; on the initial "fit" view we just keep it red.
        // Skipped while the map is hidden (mobile) — opening a popup on a 0x0
        // map throws.
        if (viewportMode === "focus" && hasSize) {
          marker.openPopup();
        }
      }
    });

    // "You are here" marker: a translucent accuracy ring under a solid blue dot.
    if (userLocation) {
      const ring = L.circleMarker([userLocation.lat, userLocation.lng], {
        radius: 16,
        color: "rgba(26, 115, 232, 0.18)",
        fillColor: "rgba(26, 115, 232, 0.18)",
        fillOpacity: 0.4,
        weight: 0,
        interactive: false,
      });
      const dot = L.circleMarker([userLocation.lat, userLocation.lng], {
        radius: 7,
        color: "#ffffff",
        fillColor: SELECTED_ACCENT,
        fillOpacity: 1,
        weight: 3,
      });
      dot.bindTooltip("Your location", {
        direction: "top",
        offset: [0, -10],
        opacity: 0.98,
        className: "venue-map__tooltip",
      });
      ring.addTo(layerGroup);
      dot.addTo(layerGroup);
    }

    applyView();
  }, [applyView, onSelectVenue, onViewVenueActivities, resolvedVenues, selectedVenueName, userLocation, viewportMode]);

  // The mobile overlay reveals the map by switching its container from
  // `display: none` to full-screen. Recompute size and re-run the view math we
  // skipped while it was hidden.
  useEffect(() => {
    if (!active) return;
    const map = mapRef.current;
    if (!map) return;
    const frame = requestAnimationFrame(() => {
      map.invalidateSize({ animate: false });
      applyView();
    });
    return () => cancelAnimationFrame(frame);
  }, [active, applyView]);

  if (resolvedVenues.length === 0) {
    return <div className="venue-map__loading">No venues available for the current filter.</div>;
  }

  return (
    <div className="venue-map">
      <div ref={setContainerNode} className="venue-map__canvas" />
      <button
        type="button"
        className="venue-map__reset"
        onClick={onResetView}
        title="Zoom out to the entire US"
        aria-label="Zoom out to the entire US"
      >
        <span aria-hidden="true">⤢</span>
        View entire US
      </button>
      {mapInitError ? <div className="venue-map__notice">Map failed to initialize: {mapInitError}</div> : null}
      {mapNotice ? <div className="venue-map__notice">{mapNotice}</div> : null}
    </div>
  );
}
