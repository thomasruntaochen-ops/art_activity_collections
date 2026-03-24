#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import func, select

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

FRONTEND_ROOT = PROJECT_ROOT / "frontend"
from src.db.session import SessionLocal
from src.models.activity import Activity, Source, Venue

PUBLIC_IMAGE_DIR = FRONTEND_ROOT / "public" / "venue-photos"
MANIFEST_PATH = FRONTEND_ROOT / "lib" / "venue-media.json"
USER_AGENT = "DigitalCuratorVenueAssets/1.0 (+https://example.local)"
REQUEST_TIMEOUT = 20

MANUAL_ADDRESS_OVERRIDES: dict[str, dict[str, str]] = {
    "The Metropolitan Museum of Art": {
        "address": "1000 Fifth Avenue",
        "city": "New York",
        "state": "NY",
        "zip": "10028",
        "website": "https://www.metmuseum.org",
    }
}


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def normalize_root_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def image_extension_from_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    mime_type = content_type.split(";", 1)[0].strip().lower()
    if mime_type in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    if mime_type == "image/png":
        return ".png"
    if mime_type == "image/webp":
        return ".webp"
    extension = mimetypes.guess_extension(mime_type)
    return extension if extension not in {None, ".jpe"} else ".jpg"


def fetch_html(url: str) -> str | None:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None
    if "text/html" not in response.headers.get("content-type", "").lower():
        return None
    return response.text


def score_image_candidate(url: str, attrs: dict[str, Any] | None = None) -> int:
    lower_url = url.lower()
    score = 0
    attrs = attrs or {}
    if lower_url.endswith(".svg"):
        return -100
    if any(token in lower_url for token in ("logo", "icon", "favicon", "sprite")):
        score -= 60
    if any(token in lower_url for token in ("hero", "banner", "museum", "building", "facade", "visit", "home")):
        score += 25
    if any(token in lower_url for token in ("jpg", "jpeg", "png", "webp")):
        score += 10
    width = str(attrs.get("width") or "")
    height = str(attrs.get("height") or "")
    if width.isdigit() and int(width) >= 800:
        score += 10
    if height.isdigit() and int(height) >= 450:
        score += 10
    alt_text = str(attrs.get("alt") or "").lower()
    if "logo" in alt_text:
        score -= 40
    if any(token in alt_text for token in ("museum", "building", "gallery", "exterior", "facade")):
        score += 15
    return score


def extract_image_candidates(page_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[int, str]] = []

    for meta in soup.find_all("meta"):
        key = (meta.get("property") or meta.get("name") or "").strip().lower()
        content = (meta.get("content") or "").strip()
        if not content:
            continue
        if key in {"og:image", "twitter:image", "twitter:image:src"}:
            absolute = urljoin(page_url, content)
            candidates.append((score_image_candidate(absolute), absolute))

    for image in soup.find_all("img", src=True):
        absolute = urljoin(page_url, image["src"])
        attrs = {
            "width": image.get("width"),
            "height": image.get("height"),
            "alt": image.get("alt"),
        }
        candidates.append((score_image_candidate(absolute, attrs), absolute))

    deduped: dict[str, int] = {}
    for score, url in candidates:
        if url not in deduped or score > deduped[url]:
            deduped[url] = score
    return [url for url, _ in sorted(deduped.items(), key=lambda item: item[1], reverse=True) if _ > -50]


def download_image(url: str, file_stem: str) -> str | None:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    content_type = response.headers.get("content-type", "")
    if not content_type.lower().startswith("image/"):
        return None

    extension = image_extension_from_content_type(content_type)
    if extension not in {".jpg", ".jpeg", ".png", ".webp"}:
        return None

    PUBLIC_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    normalized_extension = ".jpg" if extension == ".jpeg" else extension
    file_path = PUBLIC_IMAGE_DIR / f"{file_stem}{normalized_extension}"
    file_path.write_bytes(response.content)
    return f"/venue-photos/{file_path.name}"


def choose_source_url(website: str | None) -> str | None:
    root = normalize_root_url(website)
    return root


def build_venue_source_index() -> dict[int, dict[str, str | None]]:
    stmt = (
        select(
            Venue.id,
            Venue.name,
            Venue.city,
            Venue.state,
            Venue.address,
            Venue.zip,
            Venue.website,
            Source.base_url,
            func.count(Activity.id).label("activity_count"),
        )
        .join(Activity, Activity.venue_id == Venue.id, isouter=True)
        .join(Source, Source.id == Activity.source_id, isouter=True)
        .group_by(Venue.id, Source.base_url)
        .order_by(Venue.id.asc(), func.count(Activity.id).desc(), Source.base_url.asc())
    )

    results: dict[int, dict[str, str | None]] = {}
    with SessionLocal() as db:
        for row in db.execute(stmt):
            if row.id in results:
                continue
            results[row.id] = {
                "name": row.name,
                "city": row.city,
                "state": row.state,
                "address": row.address,
                "zip": row.zip,
                "website": row.website,
                "source_base_url": row.base_url,
            }
    return results


def apply_db_backfill(*, commit: bool) -> dict[str, str]:
    updated_websites: dict[str, str] = {}
    with SessionLocal() as db:
        venues = db.scalars(select(Venue).order_by(Venue.id.asc())).all()
        source_rows = db.execute(
            select(Venue.id, Source.base_url, func.count(Activity.id).label("activity_count"))
            .join(Activity, Activity.venue_id == Venue.id)
            .join(Source, Source.id == Activity.source_id)
            .group_by(Venue.id, Source.base_url)
            .order_by(Venue.id.asc(), func.count(Activity.id).desc(), Source.base_url.asc())
        ).all()

        best_source_by_venue: dict[int, str] = {}
        for row in source_rows:
            if row.id not in best_source_by_venue and row.base_url:
                best_source_by_venue[row.id] = normalize_root_url(row.base_url) or row.base_url

        for venue in venues:
            best_source_url = best_source_by_venue.get(venue.id)
            if (not venue.website) and best_source_url:
                venue.website = best_source_url
                updated_websites[venue.name] = best_source_url

            override = MANUAL_ADDRESS_OVERRIDES.get(venue.name)
            if override:
                venue.address = override["address"]
                venue.city = override["city"]
                venue.state = override["state"]
                venue.zip = override["zip"]
                venue.website = override["website"]
                updated_websites[venue.name] = override["website"]

        if commit:
            db.commit()
        else:
            db.rollback()
    return updated_websites


def build_manifest() -> dict[str, dict[str, str]]:
    venue_rows = build_venue_source_index()
    manifest: dict[str, dict[str, str]] = {}
    image_failures: defaultdict[str, list[str]] = defaultdict(list)

    for row in venue_rows.values():
        venue_name = row["name"] or ""
        if not venue_name or venue_name in manifest:
            continue
        website = row["website"] or row["source_base_url"] or MANUAL_ADDRESS_OVERRIDES.get(venue_name, {}).get("website")
        root_url = choose_source_url(website)
        if not root_url:
            continue

        image_public_path: str | None = None
        image_source_url: str | None = None
        html = fetch_html(root_url)
        if html:
            for candidate in extract_image_candidates(root_url, html):
                public_path = download_image(candidate, slugify(venue_name))
                if public_path:
                    image_public_path = public_path
                    image_source_url = candidate
                    break
                image_failures[venue_name].append(candidate)

        if image_public_path:
            manifest[venue_name] = {
                "image_path": image_public_path,
                "image_source_url": image_source_url or root_url,
                "website": root_url,
            }

    MANIFEST_PATH.write_text(json.dumps(dict(sorted(manifest.items())), indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill venue website/address metadata and build local venue image assets.")
    parser.add_argument("--commit", action="store_true", help="Persist DB updates for venue website/address fields.")
    args = parser.parse_args()

    updated_websites = apply_db_backfill(commit=args.commit)
    manifest = build_manifest()
    print(
        json.dumps(
            {
                "commit": args.commit,
                "website_updates": len(updated_websites),
                "image_entries": len(manifest),
                "manifest_path": str(MANIFEST_PATH.relative_to(PROJECT_ROOT)),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
