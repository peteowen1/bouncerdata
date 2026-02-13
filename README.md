# bouncerdata

Data repository for the [bouncer](https://github.com/peteowen1/bouncer) cricket analytics R package.

## Relationship to bouncer Package

This repository stores the cricket database and trained models. The `bouncer` R package manages all data operations:

```r
library(bouncer)

# Install data from GitHub releases (recommended)
install_bouncerdata_from_release()

# Or download fresh from Cricsheet
install_bouncer_data(formats = c("t20", "odi"))

# Query the database
matches <- query_matches(match_type = "t20", season = "2024")
```

**You don't need to interact with this directory directly** - use bouncer's helper functions.

## Contents

| File/Directory | Size | Description |
|----------------|------|-------------|
| `bouncer.duckdb` | ~8 GB | Main DuckDB database |
| `models/` | ~50 MB | Trained XGBoost models (.ubj, .rds) |
| `fox_cricket/` | ~100 MB | Fox Sports ball-by-ball data |
| `parquet/` | Variable | Exported parquet files for releases |

## Database Overview

The DuckDB database contains cricket data from [Cricsheet](https://cricsheet.org) and Fox Sports:

### Core Tables

| Table | ~Rows | Description |
|-------|-------|-------------|
| `matches` | 15,000+ | Match metadata - teams, venue, outcome, officials |
| `deliveries` | 6,000,000+ | Ball-by-ball data - runs, wickets, extras, context |
| `players` | 25,000+ | Player registry - name, country, DOB, styles |
| `match_innings` | 50,000+ | Innings summaries - totals, declarations, targets |
| `innings_powerplays` | 30,000+ | Powerplay periods per innings |

### Rating & Skill Tables

| Table | Description |
|-------|-------------|
| `team_elo` | Game-level team ELO ratings |
| `player_elo_history` | Historical player ELO by match |
| `t20/odi/test_player_skill` | Per-delivery player skill indices |
| `t20/odi/test_team_skill` | Per-delivery team skill indices |
| `t20/odi/test_venue_skill` | Per-delivery venue characteristics |

### Projection & Prediction Tables

| Table | Description |
|-------|-------------|
| `t20/odi/test_score_projection` | Per-delivery score projections |
| `projection_params` | Optimized projection parameters |
| `pre_match_features` | Features for pre-match prediction models |
| `pre_match_predictions` | Model predictions with outcomes |
| `simulation_results` | Monte Carlo simulation outputs |

**Full schema documentation:** See bouncer's [database-schema vignette](https://peteowen1.github.io/bouncer/articles/database-schema.html)

## GitHub Releases

Data is distributed via GitHub Releases with these tags:

| Tag | Content | Updated |
|-----|---------|---------|
| `cricsheet` | Cricsheet parquet files + manifest.json | Daily (automated) |
| `foxsports` | Fox Sports combined parquets per format | Daily (automated) |
| `core` | Full database exports (matches, deliveries, players) | Manual |

### Parquet File Naming

Deliveries and matches are partitioned by format and gender:
- `deliveries_T20_male.parquet`
- `deliveries_Test_female.parquet`
- `matches_ODI_male.parquet`

Core files: `players.parquet`, `team_elo.parquet`, `manifest.json`

## Automated Data Updates

GitHub Actions workflows run daily:

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| `cricsheet-daily.yml` | 7 AM UTC | Incremental Cricsheet sync |
| `foxsports-daily.yml` | 10 AM UTC | Fox Sports scraping |

### Manual Triggers

```bash
# Trigger Cricsheet sync
gh workflow run cricsheet-daily.yml --repo peteowen1/bouncerdata

# Force full rebuild
gh workflow run cricsheet-daily.yml --repo peteowen1/bouncerdata -f force_full_rebuild=true

# Fox Sports specific formats
gh workflow run foxsports-daily.yml --repo peteowen1/bouncerdata -f formats=BBL,TEST -f years=2024,2025
```

## Data Sources

- **Cricsheet**: [cricsheet.org](https://cricsheet.org) - Open cricket data under ODC-BY license
- **Fox Sports**: Scraped via headless browser for Australian competitions

## Local Development

If working on the data pipeline:

```bash
# Export tables to parquet
Rscript scripts/export_parquets.R

# Upload to release
RELEASE_TYPE=core Rscript scripts/upload_to_release.R
```

## Related

- [bouncer package](https://github.com/peteowen1/bouncer) - Cricket analytics R package
- [Documentation](https://peteowen1.github.io/bouncer/) - Full package documentation
- [Getting Started](https://peteowen1.github.io/bouncer/articles/getting-started.html) - Installation and first steps
- [Database Schema](https://peteowen1.github.io/bouncer/articles/database-schema.html) - Complete table reference
- [Function Reference](https://peteowen1.github.io/bouncer/reference/) - All exported functions
