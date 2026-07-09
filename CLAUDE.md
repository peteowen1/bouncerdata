# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Workflow
- Work on `dev` branch, not directly on `main`
- Large data files are gitignored - distributed via GitHub Releases

## Directory Structure

```
bouncerdata/
├── .github/workflows/     <- cricsheet-daily.yml, foxsports-daily.yml, cricinfo-daily.yml, build-blog-data.yml
├── scripts/               <- Local scripts (R, Python, shell)
│   ├── cricinfo_scraper.py       <- Cricinfo Python scraper (Playwright + stealth)
│   ├── combine_cricinfo_parquets.py <- Merge per-match → format-level combined parquets
│   ├── kill_scrapers.py          <- Kill all scraper processes + Playwright Chrome
│   ├── discover_series.py        <- Auto-discover new series from Cricinfo schedule pages
│   ├── series_cache.py           <- Series caching utility
│   ├── run_scraper.sh            <- Shell wrapper (ensures Chrome cleanup on kill)
│   ├── series_list.csv           <- Series to scrape (format, name, URL per series)
│   ├── build_blog_data.R         <- Build data for blog/website
│   └── oracle_vm/                <- VM deployment script (migrate_partitions.R)
├── bouncer.duckdb         <- Main database (~18GB, gitignored)
├── models/                <- Trained XGBoost models (.ubj/.rds, gitignored)
├── cricinfo/              <- Cricinfo scraped data (gitignored)
│   ├── {format}_{gender}/ <- Per-match tables: *_balls.parquet, *_match.parquet, *_innings.parquet
│   └── _archive/          <- Old scraper data (commentary, rich)
├── fox_cricket/           <- Fox Sports scraped data (gitignored)
├── json_files/            <- Raw Cricsheet JSON (gitignored, re-downloadable)
├── parquet/               <- Exported parquet files for releases
├── source/                <- Source skill/player parquets feeding blog data builds
├── blog/                  <- Aggregated leaderboard/ranking parquets for blog/website
├── manifests/             <- Cricsheet manifest files
├── checkpoints/           <- Pipeline checkpoint files
└── temp_*/                <- Temporary working directories (gitignored)
```

**Gitignored (large/regenerable):** `*.duckdb`, `models/*.ubj`, `models/*.rds`, `cricinfo/`, `fox_cricket/`, `json_files/`, `temp_*/`
These files are distributed via GitHub Releases, not tracked in git.

## Release Tags

| Tag | Content | Source |
|-----|---------|--------|
| `cricsheet` | Cricsheet parquet files + manifest.json | Automated daily |
| `foxsports` | Fox Sports combined parquets per format | Automated daily |
| `cricinfo` | Cricinfo per-match tables (balls, match metadata, innings scorecards) | Automated daily |
| `player_rating` | Per-delivery player skill indices for all formats (~530MB) | After pipeline run |
| `team_rating` | Per-delivery team skill indices for all formats (~530MB) | After pipeline run |
| `venue_rating` | Per-delivery venue skill indices for all formats (~350MB) | After pipeline run |
| `core` | Full database exports (matches, deliveries, players) | Manual |
| `predictions-cache` | Cached pipeline aggregates (~50MB) for GHA predictions | Uploaded from bouncer |

## Local Scripts

```bash
# Export DuckDB tables to parquet (run from bouncerdata/)
Rscript scripts/export_parquets.R

# Upload parquets to a release
Rscript scripts/upload_to_release.R
Rscript scripts/upload_core_release.R     # Full core release (matches, deliveries, players)

# Create release assets (parquets + manifest)
Rscript scripts/create_release_assets.R
Rscript scripts/full_export_and_upload.R  # End-to-end export + upload

# Download and classify Cricsheet JSON
Rscript scripts/sync_cricsheet.R
Rscript scripts/download_json_archives.R  # Download raw JSON archives

# Check for new data
Rscript scripts/check_updates.R

# Build player data for blog/website
Rscript scripts/build_player_details.R   # Player detail pages
Rscript scripts/build_player_lookup.R    # Player search/lookup data

# Discover new Cricinfo series (uses Playwright + stealth)
python scripts/discover_series.py --system-chrome --dry-run   # Preview only
python scripts/discover_series.py --system-chrome --update     # Append to CSV
```

## Automated Data Scraping (GitHub Actions)

Workflows live in THIS repo (`.github/workflows/`) so `GITHUB_TOKEN` has release upload permission.

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| `cricsheet-daily.yml` | 7 AM UTC | Incremental Cricsheet sync |
| `foxsports-daily.yml` | 10 AM UTC | Fox Sports scraping with headless Chrome |
| `cricinfo-daily.yml` | 12 PM UTC | ESPN Cricinfo ball-by-ball Hawkeye data via Playwright |
| `build-blog-data.yml` | Manual dispatch | Aggregate skill data → Cloudflare R2 for blog/website |

**Build Blog Data:**
- Downloads skill parquets from `player_rating`, `team_rating`, `venue_rating` releases
- Aggregates into compact leaderboard/ranking parquets (16 files: 12 leaderboard [batting/bowling/teams/venues × format] + 3 ball-by-ball [balls-{t20i,odi,test}] + player-names.parquet)
- Uploads to Cloudflare R2 bucket `inthegame-data` via `wrangler`
- Trigger: `gh workflow run build-blog-data.yml --repo peteowen1/bouncerdata --ref dev`

**Cricsheet Daily:**
- Downloads `recently_added_7_json.zip`, parses new matches, merges with existing parquets
- Uploads to release tag `cricsheet`
- Uses `piggyback` R package for GitHub release uploads
- Workaround: Forces `jsonlite` parser (RcppSimdJson has array simplification issues)

**Fox Sports Daily:**
- Uses `browser-actions/setup-chrome` + `chromote` for headless browser automation
- Extracts userkey from Fox Sports API via browser network intercept
- Scrapes formats: BBL, WBBL, TEST, T20I, WT20I, ODI, WODI
- Uploads combined parquets to release tag `foxsports`

**Cricinfo Daily:**
- Auto-discovers new series via `discover_series.py` pre-step (`continue-on-error: true`)
- Uses Playwright + stealth in headed mode (via Xvfb) to scrape ESPN Cricinfo
- Produces 3 tables per match: `_balls` (Hawkeye), `_match` (metadata), `_innings` (scorecards)
- Output: `cricinfo/{format}_{gender}/{match_id}_{table}.parquet`
- Uploads to release tag `cricinfo`

**Manual Triggers:**
```bash
# Trigger Cricsheet sync
gh workflow run cricsheet-daily.yml --repo peteowen1/bouncerdata

# Force full rebuild (downloads all_json.zip ~93MB)
gh workflow run cricsheet-daily.yml --repo peteowen1/bouncerdata -f force_full_rebuild=true

# Fox Sports with specific formats/years
gh workflow run foxsports-daily.yml --repo peteowen1/bouncerdata -f formats=BBL,TEST -f years=2024,2025

# Cricinfo scrape (optional: specify series IDs and/or format filter)
gh workflow run cricinfo-daily.yml --repo peteowen1/bouncerdata -f series_ids=1455609 -f format=test
```

**Troubleshooting:**
```bash
gh run list --workflow=cricsheet-daily.yml --repo peteowen1/bouncerdata
gh run view <run_id> --log --repo peteowen1/bouncerdata
```
Failed runs auto-create GitHub issues with labels `workflow-failure` + `cricsheet`/`foxsports`

## Parquet File Naming

Deliveries and matches are partitioned by `{match_type}_{gender}`:
- `deliveries_T20_male.parquet`, `deliveries_Test_female.parquet`
- `matches_ODI_male.parquet`, etc.

Core tables: `players.parquet`, `team_elo.parquet`, `manifest.json`

## Database Schema (bouncer.duckdb)

Uses DuckDB schemas: `cricsheet.*`, `cricinfo.*`, `main.*` (see `bouncer/CLAUDE.md` for full column details).

- **cricsheet.matches**: Match metadata (teams, venue, outcome)
- **cricsheet.deliveries**: Ball-by-ball data (partitioned by match_type/gender in parquet exports)
- **cricsheet.players**: Player registry with IDs
- **cricsheet.match_innings**: Innings summaries
- **cricsheet.innings_powerplays**: Powerplay periods
- **cricinfo.matches**: Match metadata (Hawkeye source info)
- **cricinfo.balls**: Ball-by-ball with Hawkeye fields
- **cricinfo.innings**: Batting scorecards
- **cricinfo.fixtures**: Schedule/results index
- **main.team_elo**: Team ELO ratings per match
- **main.{format}_player_skill**: Player skill indices (test, odi, t20)
- **main.{format}_3way_elo**: 3-way ELO ratings (batter, bowler, venue)
- **main.{format}_team_skill**: Team batting/bowling skill indices
- **main.{format}_venue_skill**: Venue run/wicket/boundary rates
- **main.{format}_score_projection**: Projected scores per delivery

See `bouncer/CLAUDE.md` for full schema details including column names and delivery ID format.

## Required Secrets (GitHub Actions)

| Secret | Used By | Purpose |
|--------|---------|---------|
| `GITHUB_TOKEN` | All workflows | Release uploads (auto-provided) |
| `WORKFLOW_PAT` | cricsheet-daily, cricinfo-daily | Cross-repo release uploads + repository_dispatch |
| `CLOUDFLARE_R2_TOKEN` | build-blog-data | Cloudflare R2 API token |
| `CLOUDFLARE_ACCOUNT_ID` | build-blog-data | Cloudflare account ID |

## Key R Dependencies

Workflows install from GitHub: `peteowen1/bouncer@dev`

Core packages: `arrow`, `piggyback`, `httr2`, `jsonlite`, `chromote` (Fox Sports only)
