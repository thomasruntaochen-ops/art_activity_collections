"use client";

import { Activity } from "../lib/types";

type Props = {
  activities: Activity[];
};

function formatAgeRange(min: number | null, max: number | null): string {
  if (min === null && max === null) return "Any";
  if (min !== null && max !== null) return `${min}-${max}`;
  if (min !== null) return `${min}+`;
  return `<=${max}`;
}

function formatDate(value: string): string {
  const date = new Date(value);
  return date.toLocaleString();
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
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          {activities.map((activity) => (
            <tr key={activity.id}>
              <td>{activity.title}</td>
              <td>{formatDate(activity.start_at)}</td>
              <td>{activity.venue_name ?? "-"}</td>
              <td>{activity.location_text ?? "-"}</td>
              <td>{activity.venue_city ?? "-"}</td>
              <td>{activity.venue_state ?? "-"}</td>
              <td>{formatAgeRange(activity.age_min, activity.age_max)}</td>
              <td>{activity.activity_type ?? "-"}</td>
              <td>{activity.drop_in === null ? "?" : activity.drop_in ? "Yes" : "No"}</td>
              <td>
                {activity.registration_required === null
                  ? "?"
                  : activity.registration_required
                    ? "Required"
                    : "Not required"}
              </td>
              <td>
                <a href={activity.source_url} target="_blank" rel="noreferrer">
                  Link
                </a>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
