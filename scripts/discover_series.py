"""Auto-discover ESPN Cricinfo series from live scores and schedule pages.

Scrapes Cricinfo to find series not yet in series_list.csv. Uses the same
Playwright + stealth approach as the main scraper (Akamai Bot Manager).

Data sources (from /live-cricket-score __NEXT_DATA__):
1. content.matches — live/recent matches with series metadata (SSR, most reliable)
2. editionDetails.keySeriesItems — Cricinfo-featured series (IPL, WPL, WTC, etc.)
3. editionDetails.trendingMatches — trending matches with series refs

Note: /cricket/schedule/* pages load data client-side via hs-consumer-api but
the responses are not reliably interceptable (service worker caching). The
live-scores page provides the same current-season coverage via SSR data.

Usage:
  # Dry run — just print discoveries (local with system Chrome):
  python scripts/discover_series.py --system-chrome --dry-run

  # Auto-update CSV:
  python scripts/discover_series.py --system-chrome --update

  # CI (Playwright's bundled Chromium under Xvfb):
  xvfb-run --auto-servernum python scripts/discover_series.py --update
"""

import sys
import io
import os
import time
import json
import argparse
import csv
from pathlib import Path

sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
)
sys.stderr = io.TextIOWrapper(
    sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
)

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# ============================================================
# Configuration — portable defaults relative to script location
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SERIES_LIST = SCRIPT_DIR / "series_list.csv"

stealth = Stealth()

# internationalClassId → format string (same mapping as main scraper)
FORMAT_FROM_CLASS_ID = {1: "test", 2: "odi", 3: "t20i"}

# Format string variants → normalized
FORMAT_NORMALIZE = {
    "TEST": "test", "ODI": "odi", "T20I": "t20i", "T20": "t20i",
    "MDM": "test", "ODM": "odi", "IT20": "t20i",
}

MAX_INNINGS = {"test": 4, "odi": 2, "t20i": 2}

LIVE_SCORES_URL = "https://www.espncricinfo.com/live-cricket-score"


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
    Returns 't20i', 'odi', 'test', or None."""
    # Priority 1: internationalClassId
    class_id = obj.get("internationalClassId")
    if class_id and class_id in FORMAT_FROM_CLASS_ID:
        return FORMAT_FROM_CLASS_ID[class_id]

    # Priority 2: format string
    fmt_str = obj.get("format", "")
    if fmt_str and fmt_str.upper() in FORMAT_NORMALIZE:
        return FORMAT_NORMALIZE[fmt_str.upper()]

    # Priority 3: series name heuristics
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
    """Detect gender from a match or series object. Returns 'male' or 'female'."""
    gender = obj.get("gender", "")
    if gender:
        g = gender.lower()
        if g in ("male", "female"):
            return g

    # Heuristic: check name/slug for women/female keywords
    name = obj.get("longName", "") or obj.get("name", "") or obj.get("slug", "") or obj.get("title", "") or ""
    name_lower = name.lower()
    if any(kw in name_lower for kw in ("women", "female", "wbbl", "wpl", "wodi", "wt20")):
        return "female"

    return "male"


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
        "season": season or "",
        "format": fmt or "test",
        "max_innings": str(MAX_INNINGS.get(fmt, 4)),
    }


def _add_series(dest, series_id, entry):
    """Add a series entry if not already present. Returns True if new."""
    if series_id not in dest:
        dest[series_id] = entry
        return True
    return False


# ============================================================
# Data extraction
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

        # Use match-level metadata for format/gender (more reliable than series-level)
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
# Page-level discovery functions
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

        # Source 1: content.matches (live/recent matches)
        content = app_data.get("content", {})
        matches = content.get("matches", []) if isinstance(content, dict) else []
        match_series = _extract_series_from_matches(matches) if matches else []
        print(f"    content.matches: {len(matches)} matches -> {len(match_series)} series")

        # Source 2: keySeriesItems (featured series)
        key_series = _extract_key_series(edition)
        print(f"    keySeriesItems: {len(key_series)} series")

        # Source 3: trendingMatches
        trending_series = _extract_trending_series(edition)
        print(f"    trendingMatches: {len(trending_series)} series")

        # Combine match-based + trending
        all_from_matches = match_series + trending_series
        return all_from_matches, key_series

    except Exception as e:
        print(f"    Error: {e}")
        return [], []


# ============================================================
# CSV operations
# ============================================================

def load_existing_series_ids(csv_path):
    """Load set of known series IDs from series_list.csv."""
    ids = set()
    if not Path(csv_path).exists():
        return ids
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row.get("series_id", "").strip().strip('"')
            if sid:
                ids.add(sid)
    return ids


def append_to_csv(new_series, csv_path):
    """Append new series entries to series_list.csv."""
    fieldnames = ["series_id", "name", "url", "season", "format", "max_innings"]
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        for s in new_series:
            writer.writerow(s)


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
        help="Auto-append new series to series_list.csv",
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
    args = parser.parse_args()

    csv_path = Path(args.series_list)
    existing_ids = load_existing_series_ids(csv_path)
    print(f"Loaded {len(existing_ids)} existing series from {csv_path.name}")

    # Browser launch
    launch_opts = {
        "headless": False,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    if args.system_chrome:
        launch_opts["channel"] = "chrome"

    all_discovered = {}  # series_id → entry dict (dedup across sources)

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_opts)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        stealth.apply_stealth_sync(context)
        page = context.new_page()

        # Live scores page: SSR matches + keySeriesItems + trending
        print(f"\n{'='*60}")
        print("Discovering series from live scores page")
        print(f"{'='*60}")
        match_series, key_series = discover_from_live_scores(page)
        for s in match_series:
            _add_series(all_discovered, s["series_id"], s)
        for s in key_series:
            _add_series(all_discovered, s["series_id"], s)

        browser.close()

    # Compare with existing
    new_series = []
    for sid, entry in sorted(all_discovered.items(), key=lambda x: int(x[0]), reverse=True):
        if sid not in existing_ids:
            if args.season:
                entry["season"] = args.season
            new_series.append(entry)

    # Report
    print(f"\n{'='*60}")
    print(f"Discovery Summary")
    print(f"{'='*60}")
    print(f"  Total discovered: {len(all_discovered)}")
    print(f"  Already in CSV:   {len(all_discovered) - len(new_series)}")
    print(f"  NEW series:       {len(new_series)}")

    if new_series:
        print(f"\nNew series to add:")
        for s in new_series:
            print(f"  {s['series_id']:>10}  {s['format']:<5}  {s['name']}")

        if args.update and not args.dry_run:
            append_to_csv(new_series, csv_path)
            print(f"\nAppended {len(new_series)} new series to {csv_path.name}")
        elif args.dry_run:
            print(f"\n(dry run - no changes written)")
        else:
            print(f"\nRun with --update to append these to {csv_path.name}")
    else:
        print(f"\nNo new series found.")

    print()
    return len(new_series)


if __name__ == "__main__":
    sys.exit(0 if main() >= 0 else 1)
