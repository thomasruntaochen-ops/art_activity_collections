#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_venue_presentation_assets import (  # noqa: E402
    MANIFEST_PATH,
    PUBLIC_IMAGE_DIR,
    download_image,
    extract_image_candidates,
    fetch_html,
    normalize_root_url,
    slugify,
)

CONFIG_PATH = PROJECT_ROOT / "config" / "crawler_venues.toml"
REGISTRY_PATH = PROJECT_ROOT / "src" / "crawlers" / "sources" / "registry.yaml"
ADAPTER_DIR = PROJECT_ROOT / "src" / "crawlers" / "adapters"


@dataclass(frozen=True)
class VenueMediaCandidate:
    venue_name: str
    urls: tuple[str, ...]


def literal_string(value: ast.AST) -> str | None:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def literal_string_tuple(value: ast.AST) -> tuple[str, ...]:
    text = literal_string(value)
    if text:
        return (text,)
    if isinstance(value, (ast.Tuple, ast.List)):
        values = [literal_string(item) for item in value.elts]
        return tuple(item for item in values if item)
    return ()


def load_enabled_adapter_paths() -> set[Path]:
    config = tomllib.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    paths: set[Path] = set()
    for venue in config.get("venues", []):
        if not venue.get("enabled"):
            continue
        script_name = str(venue.get("parser_script_name") or "")
        adapter_name = Path(script_name).stem.removeprefix("run_").removesuffix("_parser")
        adapter_path = ADAPTER_DIR / f"{adapter_name}.py"
        if adapter_path.exists():
            paths.add(adapter_path)
    return paths


def load_registry_urls() -> dict[str, list[str]]:
    registry_text = REGISTRY_PATH.read_text(encoding="utf-8")
    urls_by_token: dict[str, list[str]] = {}
    current_name = ""
    current_adapter = ""
    for raw_line in registry_text.splitlines():
        line = raw_line.strip()
        if line.startswith("- name:"):
            current_name = line.split(":", 1)[1].strip()
            current_adapter = ""
        elif line.startswith("adapter_type:"):
            current_adapter = line.split(":", 1)[1].strip()
        elif line.startswith("base_url:"):
            url = line.split(":", 1)[1].strip()
            for token in {current_name, current_adapter}:
                if token:
                    urls_by_token.setdefault(token, []).append(url)
    return urls_by_token


def collect_module_constants(tree: ast.Module) -> tuple[dict[str, str], dict[str, tuple[str, ...]]]:
    strings: dict[str, str] = {}
    string_sets: dict[str, tuple[str, ...]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        if not names:
            continue
        string_value = literal_string(node.value)
        tuple_value = literal_string_tuple(node.value)
        for name in names:
            if string_value is not None:
                strings[name] = string_value
            if tuple_value:
                string_sets[name] = tuple_value
    return strings, string_sets


def resolve_name(value: ast.AST, strings: dict[str, str]) -> str | None:
    text = literal_string(value)
    if text:
        return text
    if isinstance(value, ast.Name):
        return strings.get(value.id)
    return None


def resolve_urls(value: ast.AST, strings: dict[str, str], string_sets: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    text = literal_string(value)
    if text:
        return (text,)
    if isinstance(value, ast.Name):
        if value.id in string_sets:
            return string_sets[value.id]
        if value.id in strings:
            return (strings[value.id],)
    return literal_string_tuple(value)


def extract_call_candidates(tree: ast.Module, strings: dict[str, str], string_sets: dict[str, tuple[str, ...]]) -> list[VenueMediaCandidate]:
    candidates: list[VenueMediaCandidate] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        kwargs = {keyword.arg: keyword.value for keyword in node.keywords if keyword.arg}
        if "venue_name" not in kwargs:
            continue
        venue_name = resolve_name(kwargs["venue_name"], strings)
        if not venue_name:
            continue
        urls: list[str] = []
        for key in ("website", "list_url", "api_url", "url"):
            if key in kwargs:
                urls.extend(resolve_urls(kwargs[key], strings, string_sets))
        for key in ("list_urls", "source_prefixes", "category_urls"):
            if key in kwargs:
                urls.extend(resolve_urls(kwargs[key], strings, string_sets))
        candidates.append(VenueMediaCandidate(venue_name=venue_name, urls=dedupe_urls(urls)))
    return candidates


def dedupe_urls(urls: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    output: list[str] = []
    for url in urls:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc or url in seen:
            continue
        seen.add(url)
        output.append(url)
    return tuple(output)


def extract_single_candidate(adapter_path: Path, strings: dict[str, str], string_sets: dict[str, tuple[str, ...]], registry_urls: dict[str, list[str]]) -> list[VenueMediaCandidate]:
    names = [value for key, value in strings.items() if key.endswith("VENUE_NAME")]
    if not names:
        return []
    urls: list[str] = []
    for key, value in strings.items():
        if key.endswith("_URL") or key.endswith("_URL_PREFIX") or "URL" in key:
            urls.append(value)
    for key, values in string_sets.items():
        if "URL" in key:
            urls.extend(values)
    adapter_token = adapter_path.stem
    for token, token_urls in registry_urls.items():
        if adapter_token in token or token.startswith(adapter_token.removesuffix("_museum")):
            urls.extend(token_urls)
    return [VenueMediaCandidate(venue_name=name, urls=dedupe_urls(urls)) for name in dict.fromkeys(names)]


def collect_candidates() -> dict[str, VenueMediaCandidate]:
    registry_urls = load_registry_urls()
    candidates: dict[str, VenueMediaCandidate] = {}
    for adapter_path in sorted(load_enabled_adapter_paths()):
        tree = ast.parse(adapter_path.read_text(encoding="utf-8"))
        strings, string_sets = collect_module_constants(tree)
        module_candidates = extract_call_candidates(tree, strings, string_sets)
        if not module_candidates:
            module_candidates = extract_single_candidate(adapter_path, strings, string_sets, registry_urls)
        for candidate in module_candidates:
            roots = [normalize_root_url(url) or url for url in candidate.urls]
            urls = dedupe_urls(tuple(candidate.urls) + tuple(root for root in roots if root))
            if candidate.venue_name in candidates:
                merged = dedupe_urls(candidates[candidate.venue_name].urls + urls)
                candidates[candidate.venue_name] = VenueMediaCandidate(candidate.venue_name, merged)
            else:
                candidates[candidate.venue_name] = VenueMediaCandidate(candidate.venue_name, urls)
    return candidates


def load_manifest() -> dict[str, dict[str, str]]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def try_download_for_candidate(candidate: VenueMediaCandidate) -> dict[str, str] | None:
    for page_url in candidate.urls:
        html = fetch_html(page_url)
        if not html:
            continue
        for image_url in extract_image_candidates(page_url, html):
            public_path = download_image(image_url, slugify(candidate.venue_name))
            if public_path:
                return {
                    "image_path": public_path,
                    "image_source_url": image_url,
                    "website": normalize_root_url(page_url) or page_url,
                }
    return None


def main() -> int:
    manifest = load_manifest()
    candidates = collect_candidates()
    added: dict[str, dict[str, str]] = {}
    failed: dict[str, tuple[str, ...]] = {}

    PUBLIC_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    for name, candidate in sorted(candidates.items()):
        if name in manifest:
            continue
        media = try_download_for_candidate(candidate)
        if media:
            manifest[name] = media
            added[name] = media
        else:
            failed[name] = candidate.urls

    MANIFEST_PATH.write_text(json.dumps(dict(sorted(manifest.items())), indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "candidate_count": len(candidates),
                "added_count": len(added),
                "failed_count": len(failed),
                "added": added,
                "failed": failed,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
