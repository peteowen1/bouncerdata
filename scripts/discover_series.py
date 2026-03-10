"""Auto-discover ESPN Cricinfo series from multiple sources.

Scrapes Cricinfo to find series not yet in series_list.csv. Uses the same
Playwright + stealth approach as the main scraper (Akamai Bot Manager).

Data sources (tiers):
  Tier 1: Parquet scan — read series metadata from existing _match.parquet files (offline, fast)
  Tier 2: Live scores SSR — /live-cricket-score __NEXT_DATA__ (most reliable web source)
  Tier 3: Schedule pages — /cricket/schedule/* with API response interception (broadest coverage)

Usage:
  # Dry run — just print discoveries (local with system Chrome):
  python scripts/discover_series.py --system-chrome --dry-run

  # Auto-update CSV (all tiers):
  python scripts/discover_series.py --system-chrome --update --scan-parquets --cricinfo-dir ../cricinfo

  # Parquet scan only (no browser needed):
  python scripts/discover_series.py --scan-parquets --cricinfo-dir ../cricinfo --skip-web --update

  # Schedule pages only (testing):
  python scripts/discover_series.py --system-chrome --schedule-only --dry-run

  # CI (Playwright's bundled Chromium under Xvfb):
  xvfb-run --auto-servernum python scripts/discover_series.py --update --scan-parquets --cricinfo-dir cricinfo
"""

import sys
import io
import os
import time
import json
import argparse
from pathlib import Path
from urllib.parse import unquote

sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
)
sys.stderr = io.TextIOWrapper(
    sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
)

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from series_cache import (
    load_csv_cache, scan_parquets_for_series, merge_series, write_csv_cache,
    CSV_FIELDS, MAX_INNINGS,
    normalize_format, infer_gender, CLASS_ID_MAP, FORMAT_STRING_MAP,
)

# ============================================================
# Configuration — portable defaults relative to script location
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SERIES_LIST = SCRIPT_DIR / "series_list.csv"
DEFAULT_CRICINFO_DIR = SCRIPT_DIR.parent / "cricinfo"

stealth = Stealth()

LIVE_SCORES_URL = "https://www.espncricinfo.com/live-cricket-score"
SCHEDULE_URLS = [
    "https://www.espncricinfo.com/cricket/schedule/upcoming",
    "https://www.espncricinfo.com/cricket/schedule/past-results",
]


# ============================================================
# Helpers
# ============================================================

def extract_next_data(page):
    """Extract and parse __NEXT_DATA__ from the current page."""
    nd_text = page.evaluate("""
        () => {
            const el = document.getElementById('__NEXT_DATA__');
            return el ? el.textContent : null;
        }
    """)
    if not nd_text:
        return None
    try:
        return json.loads(nd_text)
    except json.JSONDecodeError:
        return None


def detect_format(obj):
    """Detect cricket format from a match or series object.
    Returns 't20i', 'odi', 'test', or None.

    Uses the canonical normalize_format() from series_cache for class ID
    and format string mapping, with name-based heuristics as fallback.
    """
    # Canonical detection via class ID and format string
    result = normalize_format(
        fmt_str=obj.get("format", ""),
        class_id=obj.get("internationalClassId"),
    )
    if result:
        return result

    # Name-based heuristic fallback (not in canonical function since it's
    # series-discovery-specific and too aggressive for general use)
    name = obj.get("longName", "") or obj.get("name", "") or obj.get("title", "") or ""
    name_upper = name.upper()
    if "T20" in name_upper or "IPL" in name_upper or "BBL" in name_upper or "CPL" in name_upper:
        return "t20i"
    if "ODI" in name_upper or "ONE-DAY" in name_upper or "ONE DAY" in name_upper or "50" in name_upper:
        return "odi"
    if "TEST" in name_upper or "SHEFFIELD" in name_upper or "RANJI" in name_upper or "TROPHY" in name_upper:
        return "test"

    return None


def detect_gender(obj):
    """Detect gender from a match or series object. Returns 'male' or 'female'.

    Uses the canonical infer_gender() from series_cache.
    """
    name = obj.get("longName", "") or obj.get("name", "") or obj.get("title", "") or ""
    slug = obj.get("slug", "")
    gender_field = obj.get("gender", "")
    return infer_gender(name=name, slug=slug, gender_field=gender_field)


def build_series_entry(series_id, name, slug, fmt, gender, season=None):
    """Build a CSV-compatible series entry dict."""
    if slug:
        url = f"https://www.espncricinfo.com/series/{slug}-{series_id}"
    else:
        url = f"https://www.espncricinfo.com/series/{series_id}"

    return {
        "series_id": str(series_id),
        "name": name or f"Series {series_id}",
        "url": url,
        "season": unquote(season) if season else "",
        "format": fmt or "test",
        "max_innings": str(MAX_INNINGS.get(fmt, 4)),
        "gender": gender or "male",
    }


def _add_series(dest, series_id, entry):
    """Add a series entry if not already present. Returns True if new."""
    if series_id not in dest:
        dest[series_id] = entry
        return True
    return False


# ============================================================
# Data extraction from match/series objects
# ============================================================

def _extract_series_from_matches(matches):
    """Extract unique series from a list of match objects."""
    seen = {}
    for m in matches:
        series = m.get("series") or {}
        series_id = series.get("objectId") or series.get("id")
        if not series_id:
            continue
        series_id = str(series_id)
        if series_id in seen:
            continue

        slug = series.get("slug", "")
        name = series.get("longName") or series.get("name") or ""

        fmt = detect_format(m) or detect_format(series)
        gender = detect_gender(m) if m.get("gender") else detect_gender(series)
        season = series.get("season") or m.get("season") or ""

        seen[series_id] = build_series_entry(series_id, name, slug, fmt, gender, season)

    return list(seen.values())


def _extract_key_series(edition_details):
    """Extract series from editionDetails.keySeriesItems."""
    items = edition_details.get("keySeriesItems", [])
    result = []
    for item in items:
        if item.get("type") != "SERIES":
            continue
        series = item.get("series") or {}
        series_id = series.get("objectId") or series.get("id")
        if not series_id:
            continue
        slug = series.get("slug", "")
        name = item.get("title") or series.get("longName") or series.get("name") or ""
        fmt = detect_format(series) or detect_format(item)
        gender = detect_gender(series) or detect_gender(item)
        season = series.get("season", "")
        result.append(build_series_entry(str(series_id), name, slug, fmt, gender, season))
    return result


def _extract_trending_series(edition_details):
    """Extract series from editionDetails.trendingMatches."""
    trending = edition_details.get("trendingMatches", {}).get("matches", [])
    return _extract_series_from_matches(trending)


# ============================================================
# Tier 2: Live scores page (SSR — existing logic)
# ============================================================

def discover_from_live_scores(page):
    """Discover series from /live-cricket-score page.

    This page has content.matches in __NEXT_DATA__ (SSR), unlike schedule pages
    which load matches client-side via API.
    """
    try:
        print(f"  Navigating to {LIVE_SCORES_URL}")
        page.goto(LIVE_SCORES_URL, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)

        title = page.title()
        if "access denied" in title.lower():
            print(f"    Blocked by Akamai")
            return [], []

        nd = extract_next_data(page)
        if not nd:
            print(f"    No __NEXT_DATA__ found")
            return [], []

        props = nd.get("props", {})
        app_data = props.get("appPageProps", {}).get("data", {})
        edition = props.get("editionDetails", {})

        content = app_data.get("content", {})
        matches = content.get("matches", []) if isinstance(content, dict) else []
        match_series = _extract_series_from_matches(matches) if matches else []
        print(f"    content.matches: {len(matches)} matches -> {len(match_series)} series")

        key_series = _extract_key_series(edition)
        print(f"    keySeriesItems: {len(key_series)} series")

        trending_series = _extract_trending_series(edition)
        print(f"    trendingMatches: {len(trending_series)} series")

        all_from_matches = match_series + trending_series
        return all_from_matches, key_series

    except Exception as e:
        import traceback
        print(f"    Error in live scores discovery: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return [], []


# ============================================================
# Tier 3: Schedule pages (API interception)
# ============================================================

def discover_from_schedule_pages(page):
    """Discover series from /cricket/schedule/* pages.

    These pages load match data client-side via hs-consumer-api. We intercept
    the API responses to extract series metadata. Falls back to __NEXT_DATA__
    if interception fails.
    """
    all_series = []
    intercepted_matches = []

    def on_response(response):
        """Capture schedule API responses."""
        url = response.url
        if "hs-consumer-api" not in url:
            return
        # Schedule API endpoints contain /schedule or /matches
        if "/schedule" not in url and "/matches" not in url:
            return
        try:
            body = response.json()
            # The API returns match collections — extract matches from various shapes
            matches = _extract_matches_from_api(body)
            intercepted_matches.extend(matches)
        except Exception as exc:
            print(f"    Warning: Failed to parse schedule API response: {exc}", file=sys.stderr)

    for schedule_url in SCHEDULE_URLS:
        try:
            page_label = "upcoming" if "upcoming" in schedule_url else "past-results"
            print(f"  Navigating to schedule/{page_label}")

            intercepted_matches.clear()
            page.on("response", on_response)

            page.goto(schedule_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)  # Wait for client-side API calls

            title = page.title()
            if "access denied" in title.lower():
                print(f"    Blocked by Akamai")
                page.remove_listener("response", on_response)
                continue

            # Scroll down to trigger lazy-loaded content (adaptive like main scraper)
            prev_match_count = len(intercepted_matches)
            for scroll_i in range(20):
                page.keyboard.press("End")
                time.sleep(1)
                if len(intercepted_matches) > prev_match_count:
                    prev_match_count = len(intercepted_matches)
                elif scroll_i >= 2:
                    break  # No new data after at least 3 scrolls

            page.remove_listener("response", on_response)

            # Extract series from intercepted API responses
            if intercepted_matches:
                api_series = _extract_series_from_matches(intercepted_matches)
                print(f"    API interception: {len(intercepted_matches)} matches -> {len(api_series)} series")
                all_series.extend(api_series)
            else:
                # Fallback: try __NEXT_DATA__
                nd = extract_next_data(page)
                if nd:
                    props = nd.get("props", {})
                    app_data = props.get("appPageProps", {}).get("data", {})
                    content = app_data.get("content", {})
                    matches = content.get("matches", []) if isinstance(content, dict) else []
                    if matches:
                        nd_series = _extract_series_from_matches(matches)
                        print(f"    __NEXT_DATA__ fallback: {len(matches)} matches -> {len(nd_series)} series")
                        all_series.extend(nd_series)
                    else:
                        # Try collections (schedule pages sometimes use this structure)
                        collections = content.get("collections", []) if isinstance(content, dict) else []
                        for coll in collections:
                            coll_matches = coll.get("matches", [])
                            if coll_matches:
                                coll_series = _extract_series_from_matches(coll_matches)
                                all_series.extend(coll_series)
                        if collections:
                            total = sum(len(c.get("matches", [])) for c in collections)
                            print(f"    __NEXT_DATA__ collections: {total} matches -> {len(all_series)} series")
                        else:
                            print(f"    No data found on {page_label} page")
                else:
                    print(f"    No __NEXT_DATA__ on {page_label} page")

        except Exception as e:
            print(f"    Error on {page_label}: {e}")
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass

    return all_series


def _extract_matches_from_api(body):
    """Extract match objects from various API response shapes."""
    matches = []

    # Shape 1: { matches: [...] }
    if isinstance(body, dict) and "matches" in body:
        raw = body["matches"]
        if isinstance(raw, list):
            matches.extend(raw)

    # Shape 2: { content: { matches: [...] } }
    if isinstance(body, dict) and "content" in body:
        content = body["content"]
        if isinstance(content, dict) and "matches" in content:
            raw = content["matches"]
            if isinstance(raw, list):
                matches.extend(raw)

    # Shape 3: { collections: [{ matches: [...] }, ...] }
    if isinstance(body, dict):
        collections = body.get("collections", [])
        if isinstance(collections, list):
            for coll in collections:
                if isinstance(coll, dict) and "matches" in coll:
                    raw = coll["matches"]
                    if isinstance(raw, list):
                        matches.extend(raw)

    # Shape 4: top-level array
    if isinstance(body, list):
        for item in body:
            if isinstance(item, dict) and "series" in item:
                matches.append(item)

    return matches


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Discover new ESPN Cricinfo series not yet in series_list.csv"
    )
    parser.add_argument(
        "--series-list",
        type=str,
        default=os.environ.get("CRICINFO_SERIES_LIST", str(DEFAULT_SERIES_LIST)),
        help="Path to series_list.csv",
    )
    parser.add_argument(
        "--system-chrome",
        action="store_true",
        help="Use system Chrome instead of Playwright's bundled Chromium",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Write updated series_list.csv (full rewrite with gender column)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be added without writing",
    )
    parser.add_argument(
        "--season",
        type=str,
        help="Season label for new entries (e.g. '2025/26')",
    )
    # Tier 1: Parquet scan
    parser.add_argument(
        "--scan-parquets",
        action="store_true",
        help="Enable Tier 1: scan _match.parquet files for series metadata",
    )
    parser.add_argument(
        "--cricinfo-dir",
        type=str,
        default=os.environ.get("CRICINFO_OUTPUT_DIR", str(DEFAULT_CRICINFO_DIR)),
        help="Path to cricinfo/ data directory (for parquet scanning)",
    )
    # Tier 3: Schedule pages
    parser.add_argument(
        "--skip-schedule",
        action="store_true",
        help="Skip Tier 3: schedule page scraping",
    )
    parser.add_argument(
        "--schedule-only",
        action="store_true",
        help="Only run Tier 3: schedule page scraping (skip live scores)",
    )
    # Skip all web scraping
    parser.add_argument(
        "--skip-web",
        action="store_true",
        help="Skip all web scraping (Tier 2 + Tier 3), only use parquets + CSV",
    )
    args = parser.parse_args()

    csv_path = Path(args.series_list)

    # Load existing CSV cache
    csv_cache = load_csv_cache(csv_path)
    existing_ids = set(csv_cache.keys())
    print(f"Loaded {len(existing_ids)} existing series from {csv_path.name}")

    # ── Tier 1: Parquet scan (no browser needed) ──
    parquet_series = {}
    if args.scan_parquets:
        print(f"\n{'='*60}")
        print("Tier 1: Scanning parquet files for series metadata")
        print(f"{'='*60}")
        parquet_series = scan_parquets_for_series(args.cricinfo_dir)
        new_from_parquets = len(set(parquet_series.keys()) - existing_ids)
        print(f"  Found {len(parquet_series)} series in parquets ({new_from_parquets} new)")

    # ── Tier 2 + 3: Web discovery (needs browser) ──
    web_discovered = {}  # series_id → entry dict

    if not args.skip_web:
        launch_opts = {
            "headless": False,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if args.system_chrome:
            launch_opts["channel"] = "chrome"

        with sync_playwright() as p:
            browser = p.chromium.launch(**launch_opts)
            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            stealth.apply_stealth_sync(context)
            page = context.new_page()

            # Tier 2: Live scores (SSR)
            if not args.schedule_only:
                print(f"\n{'='*60}")
                print("Tier 2: Discovering series from live scores page")
                print(f"{'='*60}")
                match_series, key_series = discover_from_live_scores(page)
                for s in match_series:
                    _add_series(web_discovered, s["series_id"], s)
                for s in key_series:
                    _add_series(web_discovered, s["series_id"], s)

            # Tier 3: Schedule pages
            if not args.skip_schedule:
                print(f"\n{'='*60}")
                print("Tier 3: Discovering series from schedule pages")
                print(f"{'='*60}")
                schedule_series = discover_from_schedule_pages(page)
                for s in schedule_series:
                    _add_series(web_discovered, s["series_id"], s)

            browser.close()

    # ── Merge all sources ──
    merged = merge_series(csv_cache, parquet_series, web_discovered)

    # Apply season override to newly discovered entries
    new_series = {}
    for sid, entry in merged.items():
        if sid not in existing_ids:
            if args.season:
                entry["season"] = args.season
            new_series[sid] = entry

    # ── Report ──
    print(f"\n{'='*60}")
    print(f"Discovery Summary")
    print(f"{'='*60}")
    print(f"  CSV existing:      {len(existing_ids)}")
    print(f"  From parquets:     {len(parquet_series)}")
    print(f"  From web:          {len(web_discovered)}")
    print(f"  Total merged:      {len(merged)}")
    print(f"  NEW series:        {len(new_series)}")

    if new_series:
        print(f"\nNew series to add:")
        for sid in sorted(new_series.keys(), key=lambda x: int(x), reverse=True):
            s = new_series[sid]
            print(f"  {s['series_id']:>10}  {s['format']:<5}  {s.get('gender', 'male'):<7}  {s['name']}")

        if args.update and not args.dry_run:
            write_csv_cache(merged, csv_path)
            print(f"\nWrote {len(merged)} series to {csv_path.name} (full rewrite with gender column)")
        elif args.dry_run:
            print(f"\n(dry run - no changes written)")
        else:
            print(f"\nRun with --update to write these to {csv_path.name}")
    else:
        print(f"\nNo new series found.")
        # Still rewrite CSV if --update to add gender column
        if args.update and not args.dry_run:
            write_csv_cache(merged, csv_path)
            print(f"Rewrote {csv_path.name} with gender column")

    print()

    # Signal failure if web discovery was attempted but found nothing
    # (possible Akamai block affecting all pages)
    if not args.skip_web and len(web_discovered) == 0 and not args.scan_parquets:
        print("WARNING: Web discovery found zero series (possible Akamai block)", file=sys.stderr)
        return -1

    return len(new_series)


if __name__ == "__main__":
    result = main()
    sys.exit(1 if result < 0 else 0)
