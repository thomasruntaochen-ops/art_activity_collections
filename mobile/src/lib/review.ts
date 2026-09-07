import AsyncStorage from "@react-native-async-storage/async-storage";
import * as StoreReview from "expo-store-review";

// Apple shows the rating sheet at most three times per user per year and
// silently swallows every call beyond that, so the prompts are a scarce
// resource. These thresholds spend them on people who have actually got
// something out of the app rather than on first launch.
const MOMENTS_KEY = "review:valueMoments";
const LAST_PROMPT_KEY = "review:lastPromptedAt";
const MOMENTS_BEFORE_ASKING = 3;
const MIN_DAYS_BETWEEN_PROMPTS = 120;

const DAY_MS = 24 * 60 * 60 * 1000;

// Called when the user does something that means the app worked for them —
// adding an activity to their calendar, or saving one. Counts silently until
// the threshold, then asks once and resets.
//
// Never awaited by callers and never throws: a rating prompt must not be able
// to interrupt or fail the action that triggered it.
export async function recordValueMoment(): Promise<void> {
  try {
    // False on a simulator and anywhere the native API is missing, which also
    // keeps the counter from being burned during development.
    if (!(await StoreReview.hasAction())) return;

    const lastPrompted = Number(await AsyncStorage.getItem(LAST_PROMPT_KEY));
    if (lastPrompted && Date.now() - lastPrompted < MIN_DAYS_BETWEEN_PROMPTS * DAY_MS) {
      return;
    }

    const moments = Number(await AsyncStorage.getItem(MOMENTS_KEY)) + 1;
    if (!Number.isFinite(moments) || moments < MOMENTS_BEFORE_ASKING) {
      await AsyncStorage.setItem(MOMENTS_KEY, String(Number.isFinite(moments) ? moments : 1));
      return;
    }

    // Recorded before asking, not after: if the sheet throws or the app is
    // killed while it is up, the next moment should not immediately re-ask.
    await AsyncStorage.multiSet([
      [MOMENTS_KEY, "0"],
      [LAST_PROMPT_KEY, String(Date.now())],
    ]);
    await StoreReview.requestReview();
  } catch {
    // Nothing here is worth surfacing to the user.
  }
}
