"use client";

import { Activity } from "../lib/types";

type Props = {
  activities: Activity[];
};

let entityDecoder: HTMLTextAreaElement | null = null;

function decodeHtmlEntities(value: string): string {
  if (!value.includes("&")) return value;
  if (typeof document === "undefined") return value;
  if (entityDecoder === null) {
    entityDecoder = document.createElement("textarea");
  }
  entityDecoder.innerHTML = value;
  return entityDecoder.value;
}

function formatAgeRange(min: number | null, max: number | null): string {
  if (min === null && max === null) return "Age TBD";
  if (min !== null && max !== null) return `Ages ${min}-${max}`;
  if (min !== null) return `Ages ${min}+`;
  return `Up to ${max}`;
}

function formatDate(value: string): string {
  const midnightMatch = value.match(/T00:00(?::00(?:\.0+)?)?(?:Z|[+-]\d{2}:\d{2})?$/);
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  if (midnightMatch) {
    return date.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }
  return date.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function getFreeLabel(activity: Activity): string {
  if (activity.is_free === true) {
    return activity.free_verification_status === "confirmed" ? "Free confirmed" : "Likely free";
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

export function ActivityTable({ activities }: Props) {
  if (activities.length === 0) {
    return <p className="empty">No activities found for this filter.</p>;
  }

  return (
    <div className="table-wrap">
      <table className="activity-table">
        <thead>
          <tr>
            <th>Title</th>
            <th>Date</th>
            <th>Venue</th>
            <th>Location</th>
            <th>City</th>
            <th>State</th>
            <th>Age</th>
            <th>Type</th>
            <th>Drop-in</th>
            <th>Registration</th>
            <th>Free</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          {activities.map((activity) => (
            <tr key={activity.id}>
              <td className="activity-table__title-cell">{decodeHtmlEntities(activity.title)}</td>
              <td>{formatDate(activity.start_at)}</td>
              <td>{activity.venue_name ?? "-"}</td>
              <td>{activity.location_text ?? "-"}</td>
              <td>{activity.venue_city ?? "-"}</td>
              <td>{activity.venue_state ?? "-"}</td>
              <td>
                <span className="meta-pill meta-pill--neutral">{formatAgeRange(activity.age_min, activity.age_max)}</span>
              </td>
              <td>
                {activity.activity_type ? (
                  <span className="meta-pill meta-pill--soft">{activity.activity_type}</span>
                ) : (
                  "-"
                )}
              </td>
              <td>
                <span className="meta-pill meta-pill--neutral">
                  {activity.drop_in === null ? "Unknown" : activity.drop_in ? "Drop-in" : "Scheduled"}
                </span>
              </td>
              <td>
                <span className="meta-pill meta-pill--neutral">
                  {activity.registration_required === null
                    ? "Unknown"
                    : activity.registration_required
                      ? "Required"
                      : "Optional"}
                </span>
              </td>
              <td>
                <span className={`meta-pill meta-pill--${getFreeTone(activity)}`}>{getFreeLabel(activity)}</span>
              </td>
              <td>
                <a className="table-link" href={activity.source_url} target="_blank" rel="noreferrer">
                  View source
                </a>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
