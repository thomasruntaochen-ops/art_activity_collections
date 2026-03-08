#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.pipeline.alerts import send_crawler_alert


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Send a test crawler alert using CRAWLER_ALERT_WEBHOOK_URL."
    )
    parser.add_argument(
        "--title",
        default="Crawler alert test",
        help="Alert title.",
    )
    parser.add_argument(
        "--message",
        default="Manual test alert from scripts/test_crawler_alert.py",
        help="Alert message body.",
    )
    parser.add_argument(
        "--details-json",
        default="{}",
        help='Extra JSON object payload for details, e.g. \'{"env":"railway"}\'.',
    )
    parser.add_argument(
        "--allow-missing-webhook",
        action="store_true",
        help="Exit 0 even when CRAWLER_ALERT_WEBHOOK_URL is unset.",
    )
    args = parser.parse_args()

    webhook_url = os.getenv("CRAWLER_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url and not args.allow_missing_webhook:
        print(
            "CRAWLER_ALERT_WEBHOOK_URL is empty. "
            "Set it first or use --allow-missing-webhook."
        )
        return 2

    try:
        details = json.loads(args.details_json)
        if not isinstance(details, dict):
            print("--details-json must decode to a JSON object.")
            return 2
    except json.JSONDecodeError as exc:
        print(f"Invalid --details-json: {exc}")
        return 2

    send_crawler_alert(
        title=args.title,
        message=args.message,
        details=details,
    )
    print("Test alert call completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
