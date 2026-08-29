import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _local_alert_log_path() -> Path:
    override = os.getenv("CRAWLER_ALERT_LOG_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "logs" / "crawler_alerts.jsonl"


def _append_local_alert(payload: dict[str, Any]) -> None:
    """Persist every alert to disk.

    The webhook is best-effort and has failed silently for long stretches; this
    file is the durable record, so `crawler_alerts.jsonl` can be grepped even
    when no webhook is configured or the endpoint is dead.
    """
    path = _local_alert_log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
    except Exception as exc:  # never let alerting break the crawl
        print(f"[ALERT] local alert log write failed: {exc}", file=sys.stderr)


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

    _append_local_alert(payload)

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
    candidates_found: int | None = None,
) -> bool:
    """Decide whether a commit may proceed, alerting when an empty parse looks broken.

    Returns True when the caller should commit, False when it should skip the
    commit quietly. Raises SystemExit(2) — as it always has — when an empty parse
    looks like a broken parser.

    `candidates_found` is how many candidate events the parser saw *before*
    filtering. When it is positive, keeping zero rows just means the venue has
    nothing matching our audience criteria right now, which is normal and not
    worth an alert. When it is 0, or when the parser did not report at all, we
    fall back to alerting: the parser may well be dead.
    """
    if not commit_requested or parsed_count > 0:
        return True

    if candidates_found is not None and candidates_found > 0:
        print(
            f"[{parser_name}] 0 rows kept from {candidates_found} candidate events; "
            "nothing matches the audience filters right now. Skipping commit "
            "(no alert: the parser is reading the page fine).",
            file=sys.stderr,
        )
        return False

    context: dict[str, Any] = {"parser": parser_name, "parsed_count": parsed_count}
    if candidates_found is not None:
        context["candidates_found"] = candidates_found
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
