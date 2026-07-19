// Date presets shared with the native app's Filters screen (mobile/src/store/
// filters.ts). "upcoming" is the default (from now on, no end date); "weekend"
// covers Friday–Sunday of the current week; "custom" uses the user-picked
// From/To dates.
export type RangeKey = "upcoming" | "today" | "weekend" | "7d" | "30d" | "90d" | "custom";

export const RANGE_KEYS: RangeKey[] = ["upcoming", "today", "weekend", "7d", "30d", "90d", "custom"];

export const RANGE_LABELS: Record<RangeKey, string> = {
  upcoming: "Upcoming",
  today: "Today",
  weekend: "This weekend",
  "7d": "Next 7 days",
  "30d": "Next 30 days",
  "90d": "Next 3 months",
  custom: "Custom dates",
};

// Translate the active range into the API's date_from / date_to ISO strings.
// customFrom/customTo are the `YYYY-MM-DD` values of the web date inputs.
export function deriveDateRange(
  rangeKey: RangeKey,
  customFrom?: string | null,
  customTo?: string | null,
): { date_from?: string; date_to?: string } {
  const now = new Date();
  const iso = (d: Date) => d.toISOString();

  if (rangeKey === "custom") {
    const out: { date_from?: string; date_to?: string } = {};
    if (customFrom) out.date_from = iso(new Date(`${customFrom}T00:00:00`));
    if (customTo) out.date_to = iso(new Date(`${customTo}T23:59:59`));
    if (!out.date_from && !out.date_to) return { date_from: iso(now) };
    return out;
  }

  if (rangeKey === "today") {
    const start = new Date(now);
    start.setHours(0, 0, 0, 0);
    const end = new Date(now);
    end.setHours(23, 59, 59, 999);
    return { date_from: iso(start), date_to: iso(end) };
  }

  if (rangeKey === "weekend") {
    // Sunday that ends this weekend (getDay(): 0=Sun … 5=Fri, 6=Sat).
    const day = now.getDay();
    const sunday = new Date(now);
    sunday.setDate(sunday.getDate() + (day === 0 ? 0 : 7 - day));
    sunday.setHours(23, 59, 59, 999);
    // Mon–Thu → start Friday 00:00; Fri/Sat/Sun → we're in the window, start now.
    let start = now;
    if (day >= 1 && day <= 4) {
      start = new Date(now);
      start.setDate(start.getDate() + (5 - day));
      start.setHours(0, 0, 0, 0);
    }
    return { date_from: iso(start), date_to: iso(sunday) };
  }

  const days = rangeKey === "7d" ? 7 : rangeKey === "30d" ? 30 : rangeKey === "90d" ? 90 : 0;
  if (days === 0) return { date_from: iso(now) }; // "upcoming"
  const end = new Date(now);
  end.setDate(end.getDate() + days);
  return { date_from: iso(now), date_to: iso(end) };
}
