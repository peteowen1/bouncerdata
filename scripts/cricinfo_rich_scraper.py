"""
Cricinfo Ball-by-Ball Scraper using Playwright + Stealth.

Extracts three tables per match:
1. **Balls** (_balls.parquet): Full ball-by-ball data with Hawkeye fields
   - Wagon wheel (wagonX, wagonY, wagonZone)
   - Pitch map (pitchLine, pitchLength)
   - Shot analysis (shotType, shotControl)
   - Win probability (predictions.score, predictions.winProbability)
   - Player IDs, running totals, commentary text
2. **Match** (_match.parquet): Match-level metadata
   - Venue, toss, result, dates, umpires, officials, format, teams, captains, POTM
3. **Innings** (_innings.parquet): Batting scorecards with player details
   - Per-batsman per-innings rows with DOB, batting/bowling style, playing role

Output structure: cricinfo/{format}_{gender}/{match_id}_{table}.parquet

Strategy:
1. Launch Chrome (headed via Xvfb in CI, or system Chrome locally) with playwright-stealth
2. Navigate to ball-by-ball commentary page
3. Extract SSR data from __NEXT_DATA__ (first ~20 balls + match metadata + innings)
4. Scroll up/down to trigger IntersectionObserver pagination
5. Capture API responses via page.on('response')
6. Switch innings via dropdown and repeat
7. Combine SSR + API data, deduplicate, save as parquet

Usage:
  # CI (uses Playwright's bundled Chromium, run under xvfb-run):
  xvfb-run --auto-servernum python cricinfo_rich_scraper.py --max-matches 50

  # Local Windows/Mac (uses system Chrome):
  python cricinfo_rich_scraper.py --system-chrome --series 1502138 --max-matches 5

  # Override paths:
  python cricinfo_rich_scraper.py --output-dir /tmp/cricinfo --series-list my_series.csv
"""
import sys
import io
import os
import time
import json
import argparse
import csv

sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
)
sys.stderr = io.TextIOWrapper(
    sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
)

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from pathlib import Path

from series_cache import build_series_list

# ============================================================
# Configuration — portable defaults relative to script location
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = SCRIPT_DIR.parent / "cricinfo"
DEFAULT_SERIES_LIST = SCRIPT_DIR / "series_list.csv"

stealth = Stealth()

ERROR_LOG_COLUMNS = [
    "timestamp", "match_id", "series_id", "series_name", "format",
    "teams", "innings_expected", "innings_scraped", "failed_innings",
    "error_type", "error_message",
]


def log_scrape_error(output_dir, **kwargs):
    """Append one row to cricinfo/scrape_errors.csv."""
    log_path = Path(output_dir) / "scrape_errors.csv"
    write_header = not log_path.exists()
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ERROR_LOG_COLUMNS)
        if write_header:
            writer.writeheader()
        row = {col: kwargs.get(col, "") for col in ERROR_LOG_COLUMNS}
        writer.writerow(row)


def _detect_gender_from_series(series_obj, name=""):
    """Detect gender from series __NEXT_DATA__ object or name."""
    # Direct field (most reliable)
    gender = series_obj.get("gender", "")
    if gender and gender.lower() in ("male", "female"):
        return gender.lower()
    # Slug check
    slug = series_obj.get("slug", "")
    if "women" in slug.lower():
        return "female"
    # Name heuristic
    if name:
        lower = name.lower()
        if any(kw in lower for kw in ("women", "female", "wbbl", "wpl", "wodi", "wt20")):
            return "female"
    return None


def discover_matches(page, series_id, series_url=None, series_name=None,
                     series_format=None, series_gender=None):
    """Discover all matches in a series from the schedule page.

    Returns (finished_matches, all_fixtures):
        finished_matches: list of dicts for scraping (FINISHED/POST only)
        all_fixtures: list of dicts for fixtures table (all states including UPCOMING)
    """
    if not series_url:
        series_url = f"https://www.espncricinfo.com/series/{series_id}"

    url = series_url + "/match-schedule-fixtures-and-results"

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(1.5)

        nd_text = page.evaluate(
            """
            () => {
                const el = document.getElementById('__NEXT_DATA__');
                return el ? el.textContent : null;
            }
        """
        )
        if not nd_text:
            print(f"    No __NEXT_DATA__ on schedule page")
            return [], []

        nd = json.loads(nd_text)
    except Exception as e:
        print(f"    Error loading schedule page: {e}")
        return [], []

    try:
        data = nd.get("props", {}).get("appPageProps", {}).get("data", {})
        content = data.get("content", {})
        series_obj = data.get("series", {})
        matches_raw = content.get("matches", [])

        series_slug = series_obj.get("slug", "")
        # Use series-level metadata, with page data as override
        s_name = series_name or series_obj.get("longName") or series_obj.get("name") or ""
        s_format = series_format or ""
        # Detect gender from page data (more reliable than CSV)
        page_gender = _detect_gender_from_series(series_obj, s_name)
        s_gender = page_gender or series_gender or "male"

        finished_matches = []
        all_fixtures = []

        for m in matches_raw:
            match_id = m.get("objectId") or m.get("id")
            if not match_id:
                continue
            match_id = str(match_id)
            state = m.get("state", "")
            slug = m.get("slug", "")

            teams_raw = m.get("teams", [])
            team1 = teams_raw[0].get("team", {}) if len(teams_raw) > 0 else {}
            team2 = teams_raw[1].get("team", {}) if len(teams_raw) > 1 else {}

            ground = m.get("ground") or {}
            country = ground.get("country") or {}

            # Build fixture entry (all states)
            fixture = {
                "match_id": match_id,
                "series_id": str(series_id),
                "series_name": s_name,
                "format": s_format,
                "gender": s_gender,
                "status": state,
                "start_date": m.get("startDate", ""),
                "start_time": m.get("startTime", ""),
                "title": m.get("title", ""),
                "team1": team1.get("longName", ""),
                "team1_abbrev": team1.get("abbreviation", ""),
                "team2": team2.get("longName", ""),
                "team2_abbrev": team2.get("abbreviation", ""),
                "venue": ground.get("name", ""),
                "country": country.get("name", ""),
                "status_text": m.get("statusText", ""),
                "winner_team_id": str(m.get("winnerTeamId", "")) if m.get("winnerTeamId") else "",
            }
            all_fixtures.append(fixture)

            # Build scraping entry (finished only)
            if state in ("FINISHED", "POST"):
                finished_matches.append({
                    "match_id": match_id,
                    "slug": slug,
                    "series_slug": series_slug,
                    "series_id": str(series_id),
                    "title": m.get("title", ""),
                    "teams": [
                        t.get("team", {}).get("abbreviation", "?")
                        for t in teams_raw
                    ],
                })

        return finished_matches, all_fixtures
    except Exception as e:
        print(f"    Error parsing schedule: {e}")
        return [], []


def scrape_match_commentary(browser, context, page, match_url, max_innings=2):
    """Scrape all ball-by-ball data for a match using scroll-based pagination.

    Returns dict with:
        balls: list of ball dicts (rich or basic)
        has_rich: bool - whether wagonX/predictions data is available
        match_meta: dict - match-level metadata (venue, toss, result, etc.)
        innings_data: list of dicts - batting scorecards with player details
        scorecard: dict - scorecard data if available (fallback when no rich data)
    """

    # Set up response interceptor
    api_responses = []

    def on_response(response):
        if "hs-consumer-api" in response.url and "/comments" in response.url:
            try:
                body = response.json()
                api_responses.append(body)
            except Exception as exc:
                print(
                    f"  Warning: Failed to parse API response: {exc}", file=sys.stderr
                )

    page.on("response", on_response)

    try:
        page.goto(
            match_url + "/ball-by-ball-commentary",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        time.sleep(1.5)
    except Exception as e:
        page.remove_listener("response", on_response)
        raise e

    title = page.title()
    if "access denied" in title.lower():
        page.remove_listener("response", on_response)
        # Retry once with a fresh context
        print(f"      Akamai block detected, retrying with fresh context...")
        try:
            context2 = browser.new_context(
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            stealth.apply_stealth_sync(context2)
            page2 = context2.new_page()

            api_responses2 = []

            def on_response2(response):
                if "hs-consumer-api" in response.url and "/comments" in response.url:
                    try:
                        body = response.json()
                        api_responses2.append(body)
                    except Exception as exc:
                        print(
                            f"  Warning: Failed to parse API response: {exc}",
                            file=sys.stderr,
                        )

            page2.on("response", on_response2)

            time.sleep(3)  # Wait before retry
            page2.goto(
                match_url + "/ball-by-ball-commentary",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            time.sleep(1.5)

            title2 = page2.title()
            if "access denied" in title2.lower():
                page2.remove_listener("response", on_response2)
                context2.close()
                raise Exception("Blocked by Akamai (retry also failed)")

            # Continue scraping with the new page/context
            result = _scrape_innings_loop(page2, api_responses2, max_innings)
            page2.remove_listener("response", on_response2)
            context2.close()
            return result
        except Exception as retry_err:
            raise Exception(f"Blocked by Akamai: {retry_err}")

    result = _scrape_innings_loop(page, api_responses, max_innings)
    page.remove_listener("response", on_response)
    return result


def _scrape_innings_loop(page, api_responses, max_innings):
    """Core innings scraping loop, shared by initial attempt and retry."""

    # Early check: does this match have rich ball-by-ball data?
    # Also extract match format for auto-classification
    initial_check = page.evaluate(
        """
        () => {
            try {
                const nd = JSON.parse(document.getElementById('__NEXT_DATA__').textContent);
                const data = nd.props.appPageProps.data;
                const content = data.content;
                const match = data.match || {};
                const comments = content.comments || [];
                const hasRich = comments.some(c => c.wagonX != null || c.predictions != null);
                const hasBalls = comments.some(c => c.overNumber != null);
                return {
                    hasRich, hasBalls, commentCount: comments.length,
                    matchFormat: match.format,
                    internationalClassId: match.internationalClassId,
                    gender: match.gender,
                    slug: match.slug || '',
                    teams: (match.teams || []).map(t => t.team?.abbreviation || ''),
                };
            } catch(e) {
                return { error: e.message };
            }
        }
    """
    )

    has_rich = initial_check.get("hasRich", False)
    has_balls = initial_check.get("hasBalls", False)
    detected_format = _detect_format(initial_check)
    detected_gender = _detect_gender(initial_check)

    # Extract match metadata and innings data (zero extra network calls)
    match_meta = extract_match_metadata(page)
    innings_data = extract_innings_data(page)

    if not has_balls:
        scorecard = _extract_scorecard(page)
        return {"balls": [], "has_rich": False, "scorecard": scorecard,
                "match_meta": match_meta, "innings_data": innings_data,
                "detected_format": detected_format, "detected_gender": detected_gender}

    if not has_rich:
        print(f"      (no rich data - scraping basic ball-by-ball)")

    # Discover available innings from the dropdown
    available_innings = _discover_innings(page)
    if not available_innings:
        available_innings = [
            {"title": f"Innings {i+1}", "index": i} for i in range(max_innings)
        ]

    all_balls = []
    innings_failures = []

    for innings_idx, innings_item in enumerate(available_innings):
        if innings_idx > 0:
            api_responses.clear()
            try:
                _switch_to_innings(page, innings_item["title"])
                time.sleep(2)
            except Exception as e:
                print(f"      Innings switch to '{innings_item['title']}' failed: {e}")
                innings_failures.append({
                    "innings": innings_idx + 1,
                    "title": innings_item["title"],
                    "error_type": "innings_switch_timeout",
                    "error_message": str(e),
                })
                continue

        # Get initial data for current innings
        ssr_balls = []
        inn_num = innings_idx + 1

        if innings_idx == 0:
            ssr_data = page.evaluate(
                """
                () => {
                    try {
                        const nd = JSON.parse(document.getElementById('__NEXT_DATA__').textContent);
                        const content = nd.props.appPageProps.data.content;
                        return {
                            comments: content.comments || [],
                            nextInningOver: content.nextInningOver,
                            currentInningNumber: content.currentInningNumber,
                        };
                    } catch(e) {
                        return { error: e.message };
                    }
                }
            """
            )
            if ssr_data.get("error") or not ssr_data.get("comments"):
                err_detail = ssr_data.get('error', 'empty')
                print(
                    f"      Innings {innings_idx+1}: No SSR data ({err_detail})"
                )
                innings_failures.append({
                    "innings": innings_idx + 1,
                    "title": innings_item["title"],
                    "error_type": "no_ssr_data",
                    "error_message": err_detail,
                })
                continue
            ssr_balls = [
                c for c in ssr_data["comments"] if c.get("overNumber") is not None
            ]
            inn_num = ssr_data.get("currentInningNumber", innings_idx + 1)

        # Clear api_responses before scrolling
        api_responses.clear()

        # Dismiss overlays and trigger pagination via scrolling
        _dismiss_overlays(page)

        # Scroll pagination: keyboard End/Home triggers IntersectionObserver
        prev_count = 0
        stale_rounds = 0
        max_scrolls = 200

        for i in range(max_scrolls):
            if i % 2 == 0:
                page.keyboard.press("End")
            else:
                page.keyboard.press("Home")
                time.sleep(0.1)
                page.keyboard.press("End")

            time.sleep(0.7)

            if i % 15 == 14:
                _dismiss_overlays(page)

            curr_count = len(api_responses)
            if curr_count > prev_count:
                prev_count = curr_count
                stale_rounds = 0
                last = api_responses[-1]
                if last.get("nextInningOver") is None:
                    break
            else:
                stale_rounds += 1
                if stale_rounds >= 5:
                    break

        # Combine SSR + API balls, deduplicate by id
        seen_ids = set()
        innings_balls = []
        for ball in ssr_balls:
            bid = ball.get("id")
            if bid and bid not in seen_ids:
                seen_ids.add(bid)
                innings_balls.append(ball)
        for resp in api_responses:
            for ball in resp.get("comments", []):
                if ball.get("overNumber") is not None:
                    bid = ball.get("id")
                    if bid and bid not in seen_ids:
                        seen_ids.add(bid)
                        innings_balls.append(ball)

        innings_balls.sort(
            key=lambda b: (b.get("overNumber", 0), b.get("ballNumber", 0))
        )

        if innings_balls and innings_idx > 0:
            inn_num = innings_balls[0].get("inningNumber", innings_idx + 1)

        rich_count = sum(1 for b in innings_balls if b.get("wagonX") is not None)
        overs = sorted(set(b.get("overNumber") for b in innings_balls))
        if not innings_balls:
            if innings_idx > 0:
                print(f"      Innings switch: no ball data captured")
                innings_failures.append({
                    "innings": innings_idx + 1,
                    "title": innings_item["title"],
                    "error_type": "no_ball_data",
                    "error_message": "Innings switch succeeded but no balls captured",
                })
            continue

        over_range = f"{min(overs)}-{max(overs)}" if overs else "none"
        print(
            f"      Innings {inn_num}: {len(innings_balls)} balls, overs {over_range}, rich={rich_count}/{len(innings_balls)}, pages={len(api_responses)}"
        )

        all_balls.extend(innings_balls)

    innings_scraped = len(set(b.get("inningNumber") for b in all_balls)) if all_balls else 0
    return {"balls": all_balls, "has_rich": has_rich, "scorecard": None,
            "match_meta": match_meta, "innings_data": innings_data,
            "detected_format": detected_format, "detected_gender": detected_gender,
            "innings_expected": len(available_innings), "innings_scraped": innings_scraped,
            "innings_failures": innings_failures}


FORMAT_MAP = {
    # internationalClassId -> our directory name
    1: "test",
    2: "odi",
    3: "t20i",
    # match.format string -> our directory name
    "TEST": "test",
    "ODI": "odi",
    "T20I": "t20i",
    "T20": "t20i",
    "MDM": "test",   # Multi-day match
    "ODM": "odi",    # One-day match (domestic)
    "IT20": "t20i",  # International T20
}


def _detect_format(initial_check):
    """Detect match format from __NEXT_DATA__ metadata.
    Returns 't20i', 'odi', 'test', or None if undetectable."""
    # Prefer internationalClassId (most reliable)
    class_id = initial_check.get("internationalClassId")
    if class_id and class_id in FORMAT_MAP:
        return FORMAT_MAP[class_id]
    # Fall back to match.format string
    fmt_str = initial_check.get("matchFormat")
    if fmt_str and fmt_str.upper() in FORMAT_MAP:
        return FORMAT_MAP[fmt_str.upper()]
    return None


def _detect_gender(initial_check):
    """Detect match gender from __NEXT_DATA__ metadata.
    Returns 'male', 'female', or None if undetectable."""
    # Direct gender field (most reliable)
    gender = initial_check.get("gender")
    if gender:
        return gender.lower() if gender.lower() in ("male", "female") else None
    # Heuristic: check team abbreviations for -W suffix (e.g. IND-W, AUS-W)
    teams = initial_check.get("teams", [])
    if teams and all(t.endswith("-W") for t in teams if t):
        return "female"
    # Heuristic: check slug for "women" keyword
    slug = initial_check.get("slug", "")
    if "women" in slug.lower():
        return "female"
    if teams:
        return None  # Can't determine gender from team data alone
    return None


def extract_match_metadata(page):
    """Extract match-level metadata from __NEXT_DATA__.

    Returns a flat dict suitable for a single-row parquet table, or None on failure.
    Fields come from data.match + data.content.supportInfo.
    """
    raw = page.evaluate(
        """
        () => {
            try {
                const nd = JSON.parse(document.getElementById('__NEXT_DATA__').textContent);
                const data = nd.props.appPageProps.data;
                const match = data.match || {};
                const support = (data.content || {}).supportInfo || {};
                const teams = match.teams || [];
                const umpires = match.umpires || [];
                const tvUmpires = match.tvUmpires || [];
                const matchReferees = match.matchReferees || [];
                const potm = (support.playersOfTheMatch || [])[0] || {};
                const ground = match.ground || {};
                const t0 = teams[0] || {};
                const t1 = teams[1] || {};

                return {
                    match_id: match.objectId,
                    title: match.title,
                    series_id: (match.series || {}).objectId,
                    series_name: (match.series || {}).longName,
                    format: match.format,
                    international_class_id: match.internationalClassId,
                    gender: match.gender,
                    start_date: match.startDate,
                    end_date: match.endDate,
                    start_time: match.startTime,
                    status: match.status,
                    status_text: match.statusText,
                    slug: match.slug,
                    ground_id: ground.objectId,
                    ground_name: ground.name,
                    ground_long_name: ground.longName,
                    country_name: (ground.country || {}).name,
                    city_name: (ground.town || {}).name,
                    toss_winner_team_id: match.tossWinnerTeamId,
                    toss_winner_choice: match.tossWinnerChoice,
                    winner_team_id: match.winnerTeamId,
                    scheduled_overs: match.scheduledOvers,
                    hawkeye_source: match.hawkeyeSource,
                    ball_by_ball_source: match.ballByBallSource,
                    team1_id: (t0.team || {}).objectId,
                    team1_name: (t0.team || {}).longName,
                    team1_abbreviation: (t0.team || {}).abbreviation,
                    team1_captain_id: (t0.captain || {}).objectId,
                    team1_is_home: t0.isHome,
                    team2_id: (t1.team || {}).objectId,
                    team2_name: (t1.team || {}).longName,
                    team2_abbreviation: (t1.team || {}).abbreviation,
                    team2_captain_id: (t1.captain || {}).objectId,
                    team2_is_home: t1.isHome,
                    umpire1_id: (umpires[0] || {}).objectId,
                    umpire1_name: (umpires[0] || {}).longName,
                    umpire2_id: (umpires[1] || {}).objectId,
                    umpire2_name: (umpires[1] || {}).longName,
                    tv_umpire_id: (tvUmpires[0] || {}).objectId,
                    tv_umpire_name: (tvUmpires[0] || {}).longName,
                    match_referee_id: (matchReferees[0] || {}).objectId,
                    match_referee_name: (matchReferees[0] || {}).longName,
                    potm_player_id: (potm.player || {}).objectId,
                    potm_player_name: (potm.player || {}).longName,
                };
            } catch(e) {
                return null;
            }
        }
    """
    )
    return raw


def extract_innings_data(page):
    """Extract innings summaries with batting scorecards and player details.

    Returns a list of dicts (one row per batsman per innings), or empty list on failure.
    Fields come from data.content.innings[].inningBatsmen[].
    """
    raw = page.evaluate(
        """
        () => {
            try {
                const nd = JSON.parse(document.getElementById('__NEXT_DATA__').textContent);
                const data = nd.props.appPageProps.data;
                const innings = (data.content || {}).innings || [];
                const rows = [];

                for (const inn of innings) {
                    const team = inn.team || {};
                    const batsmen = inn.inningBatsmen || [];

                    for (const bat of batsmen) {
                        const player = bat.player || {};
                        rows.push({
                            innings_number: inn.inningNumber,
                            team_id: team.objectId,
                            team_name: team.longName,
                            total_runs: inn.runs,
                            total_wickets: inn.wickets,
                            total_overs: inn.overs,
                            player_id: player.objectId,
                            player_name: player.longName,
                            player_dob: player.dateOfBirth,
                            batting_style: (player.battingStyles || [])[0] || null,
                            bowling_style: (player.bowlingStyles || [])[0] || null,
                            playing_role: player.playingRole,
                            runs: bat.runs,
                            balls_faced: bat.ballsFaced,
                            fours: bat.fours,
                            sixes: bat.sixes,
                            strike_rate: bat.strikerate || bat.strikeRate,
                            is_not_out: bat.isNotOut,
                            batting_position: bat.battingPosition,
                        });
                    }
                }
                return rows;
            } catch(e) {
                return [];
            }
        }
    """
    )
    return raw if isinstance(raw, list) else []


def _extract_scorecard(page):
    """Extract scorecard/metadata from __NEXT_DATA__ when no ball-by-ball data is available."""
    return page.evaluate(
        """
        () => {
            try {
                const nd = JSON.parse(document.getElementById('__NEXT_DATA__').textContent);
                const data = nd.props.appPageProps.data;
                const match = data.match || {};
                const content = data.content || {};

                return {
                    matchId: match.objectId,
                    title: match.title,
                    status: match.statusText,
                    teams: (match.teams || []).map(t => ({
                        id: t.team?.objectId,
                        name: t.team?.longName,
                        abbreviation: t.team?.abbreviation,
                    })),
                    innings: (match.innings || []).map(i => ({
                        inningNumber: i.inningNumber,
                        team: i.team?.abbreviation,
                        runs: i.runs,
                        wickets: i.wickets,
                        overs: i.overs,
                    })),
                    ground: match.ground ? {
                        name: match.ground.name,
                        country: match.ground.country?.name,
                    } : null,
                    startDate: match.startDate,
                    format: match.format,
                };
            } catch(e) {
                return { error: e.message };
            }
        }
    """
    )


def _dismiss_overlays(page):
    """Remove marketing overlays (CleverTap, cookie banners) that block clicks."""
    page.evaluate(
        """
        () => {
            const overlays = document.querySelectorAll('.wzrk-overlay, #wzrk_wrapper, [class*="wzrk"]');
            for (const el of overlays) el.remove();
            const banners = document.querySelectorAll('[class*="cookie"], [class*="consent"], [id*="cookie"]');
            for (const el of banners) el.style.display = 'none';
        }
    """
    )


def _find_innings_button(page):
    """Find the innings/team filter button.
    T20I/ODI pages have a short team abbrev button (e.g. 'PAK').
    Test pages have a full innings label (e.g. 'AUS 2nd Innings')."""
    return page.evaluate(
        """
        () => {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const text = btn.innerText.trim();
                if (text.includes('Innings')) {
                    const rect = btn.getBoundingClientRect();
                    if (rect.height > 10 && rect.width > 30) {
                        return { text, x: rect.x + rect.width/2, y: rect.y + rect.height/2, style: 'test' };
                    }
                }
            }
            for (const btn of buttons) {
                const text = btn.innerText.trim();
                if (/^[A-Z][A-Z0-9-]{1,7}$/.test(text)) {
                    const rect = btn.getBoundingClientRect();
                    if (rect.height > 15 && rect.width > 30) {
                        return { text, x: rect.x + rect.width/2, y: rect.y + rect.height/2, style: 'limited' };
                    }
                }
            }
            return null;
        }
    """
    )


def _discover_innings(page):
    """Discover all available innings from the dropdown."""
    try:
        _dismiss_overlays(page)
        page.evaluate("window.scrollTo(0, 500)")
        time.sleep(0.5)

        btn_info = _find_innings_button(page)
        if not btn_info:
            return []

        page.mouse.click(btn_info["x"], btn_info["y"])
        time.sleep(1.0)

        tippy = page.locator(".tippy-box")
        if not tippy.count():
            return []

        items = tippy.locator("li[title]").all()
        result = []
        current_title = btn_info["text"]
        for li in items:
            title = (li.get_attribute("title") or "").strip()
            if title:
                result.append({"title": title})

        page.keyboard.press("Escape")
        time.sleep(0.3)

        # Reorder: put the currently-displayed innings first
        reordered = []
        rest = []
        for item in result:
            if item["title"] == current_title:
                reordered.insert(0, item)
            else:
                rest.append(item)
        reordered.extend(rest)

        return reordered
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return []


def _switch_to_innings(page, target_title):
    """Switch to a specific innings by clicking its dropdown item."""
    for attempt in range(3):
        _dismiss_overlays(page)

        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)
        page.evaluate("window.scrollTo(0, 500)")
        time.sleep(0.5)

        btn_info = _find_innings_button(page)
        if not btn_info:
            if attempt < 2:
                time.sleep(1)
                continue
            raise Exception("Could not find innings dropdown button")

        page.mouse.click(btn_info["x"], btn_info["y"])
        time.sleep(1.0)

        tippy = page.locator(".tippy-box")
        if not tippy.count():
            if attempt < 2:
                page.evaluate("window.scrollTo(0, 0)")
                time.sleep(0.3)
                page.evaluate("window.scrollTo(0, 400)")
                time.sleep(0.5)
                btn_info2 = _find_innings_button(page)
                if btn_info2:
                    page.mouse.click(btn_info2["x"], btn_info2["y"])
                    time.sleep(1.0)
                    if page.locator(".tippy-box").count():
                        tippy = page.locator(".tippy-box")
                    else:
                        continue
                else:
                    continue
            else:
                raise Exception("Tippy dropdown did not appear")

        # Click the target innings item via JS (Playwright locator clicks
        # are unreliable here — the tippy can close during locator resolution)
        clicked = page.evaluate(
            """
            (target) => {
                const tippy = document.querySelector('.tippy-box');
                if (!tippy) return 'no_tippy';
                const items = tippy.querySelectorAll('li[title]');
                for (const li of items) {
                    const title = (li.getAttribute('title') || '').trim();
                    if (title === target || title.includes(target) || target.includes(title)) {
                        const div = li.querySelector('div');
                        if (div) div.click();
                        else li.click();
                        return 'ok';
                    }
                }
                return 'not_found';
            }
        """,
            target_title,
        )

        if clicked == "ok":
            return target_title
        elif clicked == "no_tippy":
            if attempt < 2:
                continue
            raise Exception("Tippy dropdown closed before click")
        else:
            page.keyboard.press("Escape")
            raise Exception(f"Could not find '{target_title}' in dropdown")

    raise Exception(f"Failed to switch to '{target_title}' after 3 attempts")


def flatten_ball(ball):
    """Flatten a ball dict for tabular output."""
    pred = ball.get("predictions") or {}

    # Extract dismissal text (structured string)
    dismissal_text_obj = ball.get("dismissalText") or {}
    dismissal_text = dismissal_text_obj.get("long") if isinstance(dismissal_text_obj, dict) else None

    # Extract first event (DRS reviews, dropped catches, etc.)
    events = ball.get("events") or []
    first_event = events[0] if events else {}
    event_type = first_event.get("type")  # e.g. "DRS_REVIEW", "DROPPED_CATCH"

    return {
        "id": ball.get("id"),
        "inningNumber": ball.get("inningNumber"),
        "overNumber": ball.get("overNumber"),
        "ballNumber": ball.get("ballNumber"),
        "oversActual": ball.get("oversActual"),
        "oversUnique": ball.get("oversUnique"),
        "totalRuns": ball.get("totalRuns"),
        "batsmanRuns": ball.get("batsmanRuns"),
        "isFour": ball.get("isFour"),
        "isSix": ball.get("isSix"),
        "isWicket": ball.get("isWicket"),
        "dismissalType": ball.get("dismissalType"),
        "dismissalText": dismissal_text,
        "wides": ball.get("wides"),
        "noballs": ball.get("noballs"),
        "byes": ball.get("byes"),
        "legbyes": ball.get("legbyes"),
        "penalties": ball.get("penalties"),
        "wagonX": ball.get("wagonX"),
        "wagonY": ball.get("wagonY"),
        "wagonZone": ball.get("wagonZone"),
        "pitchLine": ball.get("pitchLine"),
        "pitchLength": ball.get("pitchLength"),
        "shotType": ball.get("shotType"),
        "shotControl": ball.get("shotControl"),
        "batsmanPlayerId": ball.get("batsmanPlayerId"),
        "bowlerPlayerId": ball.get("bowlerPlayerId"),
        "nonStrikerPlayerId": ball.get("nonStrikerPlayerId"),
        "outPlayerId": ball.get("outPlayerId"),
        "totalInningRuns": ball.get("totalInningRuns"),
        "totalInningWickets": ball.get("totalInningWickets"),
        "predicted_score": pred.get("score"),
        "win_probability": pred.get("winProbability"),
        "event_type": event_type,
        "drs_successful": first_event.get("isSuccessful") if event_type == "DRS_REVIEW" else None,
        "title": ball.get("title"),
        "timestamp": ball.get("timestamp"),
    }


def save_all_tables(balls, match_meta, innings_data, match_id, format_dir, output_dir):
    """Save balls, match, and innings tables as separate parquets.

    Returns dict with paths of saved files.
    """
    outdir = Path(output_dir) / format_dir
    outdir.mkdir(parents=True, exist_ok=True)
    saved = {}

    import pyarrow as pa
    import pyarrow.parquet as pq

    # Balls table
    if balls:
        flat = [flatten_ball(b) for b in balls]
        table = pa.Table.from_pylist(flat)
        outpath = outdir / f"{match_id}_balls.parquet"
        pq.write_table(table, outpath)
        saved["balls"] = str(outpath)

    # Match metadata table (single row)
    if match_meta:
        table = pa.Table.from_pylist([match_meta])
        outpath = outdir / f"{match_id}_match.parquet"
        pq.write_table(table, outpath)
        saved["match"] = str(outpath)

    # Innings table (one row per batsman per innings)
    if innings_data:
        table = pa.Table.from_pylist(innings_data)
        outpath = outdir / f"{match_id}_innings.parquet"
        pq.write_table(table, outpath)
        saved["innings"] = str(outpath)

    return saved


# ============================================================
# Fixtures table
# ============================================================

FIXTURE_COLUMNS = [
    "match_id", "series_id", "series_name", "format", "gender",
    "status", "start_date", "start_time", "title",
    "team1", "team1_abbrev", "team2", "team2_abbrev",
    "venue", "country", "status_text", "winner_team_id",
    "has_ball_by_ball",
]


def save_fixtures(all_fixtures, output_dir):
    """Save/update fixtures.parquet with all discovered matches.

    Merges new fixtures with any existing file, deduplicating by match_id
    (latest status wins for updated matches).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    outpath = Path(output_dir) / "fixtures.parquet"

    # Normalize fixtures to consistent schema
    normalized = []
    for f in all_fixtures:
        row = {col: f.get(col, "") for col in FIXTURE_COLUMNS}
        # has_ball_by_ball defaults to False, will be updated later
        if row["has_ball_by_ball"] == "":
            row["has_ball_by_ball"] = False
        normalized.append(row)

    if not normalized:
        return

    # Load existing fixtures if present
    existing = {}
    if outpath.exists():
        try:
            old_table = pq.read_table(outpath)
            for i in range(old_table.num_rows):
                mid = str(old_table.column("match_id")[i].as_py())
                row = {}
                for col in old_table.column_names:
                    row[col] = old_table.column(col)[i].as_py()
                existing[mid] = row
        except Exception:
            pass  # If existing file is corrupt, start fresh

    # Merge: new fixtures overwrite existing (by match_id)
    for row in normalized:
        mid = row["match_id"]
        if mid in existing:
            # Preserve has_ball_by_ball from existing if new doesn't set it
            old = existing[mid]
            if old.get("has_ball_by_ball") and not row.get("has_ball_by_ball"):
                row["has_ball_by_ball"] = old["has_ball_by_ball"]
        existing[mid] = row

    # Write merged fixtures
    all_rows = list(existing.values())
    # Ensure all rows have all columns
    for row in all_rows:
        for col in FIXTURE_COLUMNS:
            if col not in row:
                row[col] = False if col == "has_ball_by_ball" else ""

    table = pa.Table.from_pylist(all_rows)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    pq.write_table(table, outpath)
    return str(outpath)


def mark_fixtures_scraped(output_dir, match_ids):
    """Update has_ball_by_ball=True for scraped match IDs in fixtures.parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    outpath = Path(output_dir) / "fixtures.parquet"
    if not outpath.exists() or not match_ids:
        return

    try:
        table = pq.read_table(outpath)
        rows = []
        scraped_set = set(str(mid) for mid in match_ids)
        for i in range(table.num_rows):
            row = {}
            for col in table.column_names:
                row[col] = table.column(col)[i].as_py()
            if str(row.get("match_id")) in scraped_set:
                row["has_ball_by_ball"] = True
            rows.append(row)
        pq.write_table(pa.Table.from_pylist(rows), outpath)
    except Exception:
        pass


def _infer_gender(name):
    """Infer gender from series name."""
    if not name:
        return "male"
    lower = name.lower()
    if any(kw in lower for kw in ("women", "female", "wbbl", "wpl", "wodi", "wt20", "women's")):
        return "female"
    return "male"


def load_series_list(series_list_path, format_filter=None, max_series=10):
    """Load series from series_list.csv, filtered by format."""
    series = []
    with open(series_list_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if format_filter and row.get("format") != format_filter:
                continue
            # Infer gender if not in CSV
            if not row.get("gender"):
                row["gender"] = _infer_gender(row.get("name", ""))
            series.append(row)

    # Most recent first (higher series_id = newer)
    series.sort(key=lambda s: int(s.get("series_id", 0)), reverse=True)
    return series[:max_series]


def main():
    parser = argparse.ArgumentParser(description="Cricinfo Ball-by-Ball Scraper")
    parser.add_argument("--series", type=int, nargs="*", help="Specific series IDs")
    parser.add_argument(
        "--format", choices=["t20i", "odi", "test"], help="Filter by format"
    )
    parser.add_argument(
        "--max-series", type=int, default=3, help="Max series per format"
    )
    parser.add_argument(
        "--max-matches", type=int, default=50, help="Max matches per series"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.environ.get("CRICINFO_OUTPUT_DIR", str(DEFAULT_OUTPUT)),
        help="Output directory for scraped data (default: ../cricinfo relative to script)",
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
        "--force",
        action="store_true",
        help="Re-scrape matches even if output files already exist",
    )
    parser.add_argument(
        "--scan-parquets",
        action="store_true",
        help="Use build_series_list() to auto-discover series from parquets + CSV",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    series_list_path = Path(args.series_list)

    # Determine which series to scrape
    if args.series:
        # Explicit series IDs: look up in merged series list
        merged = build_series_list(
            csv_path=str(series_list_path),
            cricinfo_dir=str(output_dir) if args.scan_parquets else None,
        )
        target_series = []
        for sid in args.series:
            sid_str = str(sid)
            if sid_str in merged:
                target_series.append(merged[sid_str])
            else:
                fallback_fmt = args.format or "t20i"
                if not args.format:
                    print(f"  WARNING: Series {sid} not in CSV and no --format specified, defaulting to '{fallback_fmt}'")
                target_series.append(
                    {
                        "series_id": sid_str,
                        "format": fallback_fmt,
                        "name": f"Series {sid}",
                        "url": "",
                        "gender": "male",
                    }
                )
    elif args.scan_parquets:
        # Auto-discover: use build_series_list with parquet scanning
        merged = build_series_list(
            csv_path=str(series_list_path),
            cricinfo_dir=str(output_dir),
        )
        all_entries = sorted(merged.values(),
                             key=lambda s: int(s.get("series_id", 0)), reverse=True)
        if args.format:
            all_entries = [s for s in all_entries if s.get("format") == args.format]
            target_series = all_entries[:args.max_series]
        else:
            target_series = []
            for fmt in ["t20i", "odi", "test"]:
                fmt_entries = [s for s in all_entries if s.get("format") == fmt]
                target_series.extend(fmt_entries[:args.max_series])
    elif args.format:
        target_series = load_series_list(
            series_list_path, format_filter=args.format, max_series=args.max_series
        )
    else:
        target_series = []
        for fmt in ["t20i", "odi", "test"]:
            target_series.extend(
                load_series_list(
                    series_list_path, format_filter=fmt, max_series=args.max_series
                )
            )

    print(f"Target: {len(target_series)} series")
    for s in target_series:
        print(f"  {s['series_id']}: {s['name']} ({s['format']})")
    print()

    # Browser launch options
    launch_opts = {
        "headless": False,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    if args.system_chrome:
        launch_opts["channel"] = "chrome"

    all_fixtures = []  # Collect fixtures across all series
    scraped_match_ids = []  # Track successfully scraped matches

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_opts)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        stealth.apply_stealth_sync(context)

        page = context.new_page()

        total_matches = 0
        total_balls = 0
        total_rich = 0

        for series_info in target_series:
            series_id = series_info["series_id"]
            fmt = series_info.get("format", "t20i")
            gender = series_info.get("gender", "male")
            max_innings = int(
                series_info.get("max_innings", 4 if fmt == "test" else 2)
            )
            print(f"\n{'='*60}")
            print(f"Series: {series_info['name']} (id={series_id}, format={fmt}, gender={gender})")
            print(f"{'='*60}")

            finished_matches, series_fixtures = discover_matches(
                page, series_id,
                series_url=series_info.get("url") or None,
                series_name=series_info.get("name"),
                series_format=fmt,
                series_gender=gender,
            )

            # Collect all fixtures (including upcoming) for the fixtures table
            if series_fixtures:
                all_fixtures.extend(series_fixtures)
                upcoming_count = sum(1 for f in series_fixtures if f["status"] not in ("FINISHED", "POST"))
                print(f"  Fixtures: {len(series_fixtures)} total ({upcoming_count} upcoming)")

            if not finished_matches:
                print(f"  No completed matches to scrape")
                continue

            print(f"  Found {len(finished_matches)} completed matches")
            finished_matches = finished_matches[: args.max_matches]

            for match in finished_matches:
                match_id = match["match_id"]
                teams = " vs ".join(match["teams"][:2])
                print(f"\n  Match {match_id}: {teams}")

                # Skip if already scraped in ANY format dir (unless --force)
                if not args.force:
                    already_scraped = False
                    for check_fmt in ["t20i_male", "t20i_female", "odi_male", "odi_female", "test_male", "test_female"]:
                        check_dir = output_dir / check_fmt
                        if check_dir.exists() and list(check_dir.glob(f"{match_id}_balls.*")):
                            already_scraped = True
                            break
                    if already_scraped:
                        print(f"    Already scraped, skipping")
                        scraped_match_ids.append(match_id)  # Already has ball-by-ball
                        continue

                match_url = f"https://www.espncricinfo.com/series/{match['series_slug']}/{match['slug']}-{match_id}"

                try:
                    t0 = time.time()
                    result = scrape_match_commentary(
                        browser, context, page, match_url, max_innings=max_innings
                    )
                    elapsed = time.time() - t0

                    # Use detected format/gender, fall back to CSV values
                    save_fmt = result.get("detected_format") or fmt
                    save_gender = result.get("detected_gender")
                    if not save_gender:
                        save_gender = "male"
                        print(f"    WARNING: Could not detect gender, defaulting to 'male'")
                    format_dir = f"{save_fmt}_{save_gender}"

                    if result.get("detected_format") and result["detected_format"] != fmt:
                        print(f"    (auto-detected format: {result['detected_format']}, CSV said: {fmt})")

                    balls = result["balls"]
                    match_meta = result.get("match_meta")
                    innings_data = result.get("innings_data")

                    if balls or match_meta or innings_data:
                        saved = save_all_tables(
                            balls, match_meta, innings_data,
                            match_id, format_dir, output_dir
                        )
                        tables_saved = list(saved.keys())
                        if balls:
                            rich = sum(
                                1 for b in balls if b.get("wagonX") is not None
                            )
                            label = "rich" if result["has_rich"] else "basic"
                            print(
                                f"    -> Saved {len(balls)} balls ({rich} {label}) + tables {tables_saved} to {format_dir}/ [{elapsed:.0f}s]"
                            )
                            total_matches += 1
                            total_balls += len(balls)
                            total_rich += rich
                            scraped_match_ids.append(match_id)
                        else:
                            print(
                                f"    -> Saved metadata only (tables {tables_saved}) to {format_dir}/ [{elapsed:.0f}s]"
                            )
                    elif result.get("scorecard"):
                        sc = result["scorecard"]
                        print(
                            f"    -> Scorecard only: {sc.get('title', 'unknown')} [{elapsed:.0f}s]"
                        )
                    else:
                        print(f"    No data found [{elapsed:.0f}s]")

                    # Log any innings failures to CSV
                    for fail in result.get("innings_failures", []):
                        log_scrape_error(
                            output_dir,
                            timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
                            match_id=match_id,
                            series_id=series_id,
                            series_name=series_info.get("name", ""),
                            format=result.get("detected_format") or fmt,
                            teams=teams,
                            innings_expected=result.get("innings_expected", ""),
                            innings_scraped=result.get("innings_scraped", ""),
                            failed_innings=fail["innings"],
                            error_type=fail["error_type"],
                            error_message=fail["error_message"][:200],
                        )
                except Exception as e:
                    print(f"    ERROR: {e}")
                    log_scrape_error(
                        output_dir,
                        timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
                        match_id=match_id,
                        series_id=series_id,
                        series_name=series_info.get("name", ""),
                        format=fmt,
                        teams=teams,
                        error_type="match_error",
                        error_message=str(e)[:200],
                    )

                time.sleep(1)

        browser.close()

    # Save fixtures table (all matches: completed + upcoming)
    if all_fixtures:
        fixtures_path = save_fixtures(all_fixtures, output_dir)
        if scraped_match_ids:
            mark_fixtures_scraped(output_dir, scraped_match_ids)
        fixture_upcoming = sum(1 for f in all_fixtures if f["status"] not in ("FINISHED", "POST"))
        print(f"\nFixtures: saved {len(all_fixtures)} matches ({fixture_upcoming} upcoming) to {fixtures_path}")

    print(f"\n{'='*60}")
    print(f"DONE: {total_matches} matches, {total_balls} balls, {total_rich} rich")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
