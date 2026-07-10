import * as Calendar from "expo-calendar";
import { Alert } from "react-native";
import type { Activity } from "./types";

// Find a calendar we can write to: the default one when available, otherwise
// the first modifiable event calendar on the device.
async function getWritableCalendarId(): Promise<string | null> {
  try {
    const def = await Calendar.getDefaultCalendarAsync();
    if (def?.id) return def.id;
  } catch {
    // fall through to scanning all calendars
  }
  const all = await Calendar.getCalendarsAsync(Calendar.EntityTypes.EVENT);
  return all.find((c) => c.allowsModifications)?.id ?? null;
}

// One-tap "add to calendar" for an activity. Handles the permission prompt and
// reports the outcome with a small alert.
export async function addActivityToCalendar(activity: Activity): Promise<void> {
  try {
    const { status } = await Calendar.requestCalendarPermissionsAsync();
    if (status !== "granted") {
      Alert.alert(
        "Calendar access needed",
        "Turn on calendar access in Settings → Privacy → Calendars to add activities.",
      );
      return;
    }
    const calendarId = await getWritableCalendarId();
    if (!calendarId) {
      Alert.alert("No calendar found", "We couldn’t find a calendar to add this event to.");
      return;
    }
    const start = new Date(activity.start_at);
    const end = activity.end_at
      ? new Date(activity.end_at)
      : new Date(start.getTime() + 60 * 60 * 1000);
    await Calendar.createEventAsync(calendarId, {
      title: activity.title,
      startDate: start,
      endDate: end,
      location: activity.venue_name ?? undefined,
      notes: activity.source_url,
    });
    Alert.alert("Added to calendar", `“${activity.title}” is on your calendar.`);
  } catch {
    Alert.alert("Couldn’t add event", "Something went wrong adding this to your calendar.");
  }
}
