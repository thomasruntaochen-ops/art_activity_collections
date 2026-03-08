import json
import os
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def send_crawler_alert(
    *,
    title: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    payload = {
        "title": title,
        "message": message,
        "details": details or {},
        "timestamp_utc": _utc_now_iso(),
    }

    print(f"[ALERT] {title}: {message}", file=sys.stderr)
    if details:
        print(
            f"[ALERT] details={json.dumps(details, ensure_ascii=True, sort_keys=True)}",
            file=sys.stderr,
        )

    webhook_url = os.getenv("CRAWLER_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    body = json.dumps(payload).encode("utf-8")
    req = Request(
        webhook_url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=10) as response:
            status = getattr(response, "status", response.getcode())
        print(f"[ALERT] webhook delivered status={status}", file=sys.stderr)
    except Exception as exc:
        print(f"[ALERT] webhook delivery failed: {exc}", file=sys.stderr)


def abort_commit_on_empty_parse(
    *,
    parser_name: str,
    commit_requested: bool,
    parsed_count: int,
    source_url: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    if not commit_requested or parsed_count > 0:
        return

    context: dict[str, Any] = {"parser": parser_name, "parsed_count": parsed_count}
    if source_url:
        context["source_url"] = source_url
    if details:
        context.update(details)

    send_crawler_alert(
        title="Crawler parse returned 0 rows",
        message="Aborting DB commit to avoid destructive empty updates.",
        details=context,
    )
    raise SystemExit(2)
