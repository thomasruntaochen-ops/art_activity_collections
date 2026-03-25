"use client";

import L, { type LayerGroup, type Map as LeafletMap, type TileLayer } from "leaflet";
import { useEffect, useMemo, useRef, useState } from "react";
import { resolveVenueCoordinates } from "../lib/venue-map-data";
import { VenueSummary } from "../lib/types";

type Props = {
  venues: VenueSummary[];
  selectedVenueName: string;
  viewportMode: "fit" | "focus";
  onSelectVenue: (venueName: string) => void;
};

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function VenueMap({ venues, selectedVenueName, viewportMode, onSelectVenue }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<LeafletMap | null>(null);
  const markersRef = useRef<LayerGroup | null>(null);
  const primaryTilesRef = useRef<TileLayer | null>(null);
  const fallbackTilesRef = useRef<TileLayer | null>(null);
  const usingFallbackTilesRef = useRef(false);
  const tileErrorCountRef = useRef(0);
  const resolvedVenues = useMemo(() => venues.map(resolveVenueCoordinates), [venues]);
  const [mapNotice, setMapNotice] = useState("");
  const [mapInitError, setMapInitError] = useState("");
  const [debugLines, setDebugLines] = useState<string[]>([]);

  function pushDebug(message: string) {
    const timestamp = new Date().toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    setDebugLines((current) => [...current.slice(-5), `${timestamp} ${message}`]);
  }

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    let resizeObserver: ResizeObserver | null = null;

    try {
      pushDebug("Container ready");
      const map = L.map(containerRef.current, {
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
      pushDebug("Leaflet initialized");

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
        pushDebug("Primary tiles loaded");
        if (!usingFallbackTilesRef.current) {
          setMapNotice("");
        }
      });
      primaryTiles.on("tileerror", () => {
        tileErrorCountRef.current += 1;
        pushDebug(`Primary tile error ${tileErrorCountRef.current}`);
        if (usingFallbackTilesRef.current || tileErrorCountRef.current < 3) return;
        usingFallbackTilesRef.current = true;
        setMapNotice("Map tiles switched to fallback mode.");
        pushDebug("Switching to fallback tiles");
        if (map.hasLayer(primaryTiles)) {
          map.removeLayer(primaryTiles);
        }
        fallbackTiles.addTo(map);
      });
      fallbackTiles.on("load", () => {
        if (usingFallbackTilesRef.current) {
          setMapNotice("Map tiles switched to fallback mode.");
          pushDebug("Fallback tiles loaded");
        }
      });
      fallbackTiles.on("tileerror", () => {
        if (usingFallbackTilesRef.current) {
          setMapNotice("Map tiles are unavailable right now.");
          pushDebug("Fallback tile error");
        }
      });

      primaryTiles.addTo(map);
      pushDebug("Primary tiles requested");

      map.setView([39.8283, -98.5795], 4);
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
      resizeObserver.observe(containerRef.current);
      requestAnimationFrame(() => {
        map.invalidateSize({ animate: false });
      });
    } catch (error) {
      pushDebug("Leaflet init failed");
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
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const layerGroup = markersRef.current;
    if (!map || !layerGroup) return;

    layerGroup.clearLayers();
    const bounds = L.latLngBounds([]);

    resolvedVenues.forEach((venue) => {
      const isSelected = venue.venue_name === selectedVenueName;
      const halo = L.circleMarker([venue.resolvedLat, venue.resolvedLng], {
        radius: isSelected ? 16 : 9,
        color: isSelected ? "rgba(26, 115, 232, 0.20)" : "rgba(26, 115, 232, 0.08)",
        fillColor: isSelected ? "rgba(26, 115, 232, 0.20)" : "rgba(26, 115, 232, 0.08)",
        fillOpacity: isSelected ? 0.24 : 0.08,
        weight: 0,
        interactive: false,
      });
      const marker = L.circleMarker([venue.resolvedLat, venue.resolvedLng], {
        radius: isSelected ? 7.5 : 5,
        color: "#ffffff",
        fillColor: isSelected ? "#1a73e8" : "#5f87c7",
        fillOpacity: 1,
        weight: isSelected ? 2 : 1.25,
      });

      marker.bindTooltip(escapeHtml(venue.venue_name), {
        direction: "top",
        offset: [0, -10],
        opacity: 0.98,
        className: isSelected ? "venue-map__tooltip is-selected" : "venue-map__tooltip",
      });
      marker.bindPopup(
        [
          `<strong>${escapeHtml(venue.venue_name)}</strong>`,
          escapeHtml([venue.venue_city, venue.venue_state].filter(Boolean).join(", ") || "Location pending"),
          escapeHtml(`${venue.activity_count} live programs`),
          escapeHtml(venue.usesStoredCoordinates ? "Using stored coordinates" : "Using fallback coordinates"),
        ].join("<br />"),
      );

      marker.on("click", () => {
        onSelectVenue(venue.venue_name);
      });

      halo.addTo(layerGroup);
      marker.addTo(layerGroup);
      bounds.extend([venue.resolvedLat, venue.resolvedLng]);

      if (isSelected) {
        marker.openTooltip();
        marker.bringToFront();
      }
    });

    if (resolvedVenues.length === 0) {
      pushDebug("No resolved venues for current filter");
      map.setView([39.8283, -98.5795], 4);
      return;
    }

    pushDebug(`Rendered ${resolvedVenues.length} markers`);

    const selectedVenue = resolvedVenues.find((venue) => venue.venue_name === selectedVenueName) ?? null;
    if (viewportMode === "fit" && bounds.isValid()) {
      pushDebug("Fitting bounds to visible venues");
      map.fitBounds(bounds, {
        padding: [40, 40],
        maxZoom: 8,
        animate: true,
        duration: 0.35,
      });
      return;
    }

    if (selectedVenue) {
      pushDebug(`Focusing ${selectedVenue.venue_name}`);
      map.flyTo([selectedVenue.resolvedLat, selectedVenue.resolvedLng], Math.max(map.getZoom(), 15), {
        duration: 0.45,
      });
    }
  }, [onSelectVenue, resolvedVenues, selectedVenueName, viewportMode]);

  if (resolvedVenues.length === 0) {
    return <div className="venue-map__loading">No venues available for the current filter.</div>;
  }

  return (
    <div className="venue-map">
      <div ref={containerRef} className="venue-map__canvas" />
      <div className="venue-map__debug">
        {debugLines.length > 0 ? (
          debugLines.map((line) => (
            <span key={line}>{line}</span>
          ))
        ) : (
          <span>Waiting for map lifecycle...</span>
        )}
      </div>
      {mapInitError ? <div className="venue-map__notice">Map failed to initialize: {mapInitError}</div> : null}
      {mapNotice ? <div className="venue-map__notice">{mapNotice}</div> : null}
    </div>
  );
}
