# bouncerdata

This directory stores cricket data for the `bouncer` R package.

## Contents

When you run `install_bouncer_data()` from the bouncer package, cricket data will be stored here:

- `bouncer.duckdb` - DuckDB database with cricket match data
- `parquet/` - Parquet files for efficient storage (optional, created in future versions)

## Usage

You don't need to interact with this directory directly. The `bouncer` package manages all data operations:

```r
library(bouncer)

# Install cricket data (creates/updates database)
install_bouncer_data(
  formats = c("odi", "t20i"),
  leagues = c("ipl")
)

# Query data
matches <- query_matches(match_type = "odi", season = "2024")
```

## Database Schema

The DuckDB database contains:

- **matches**: Match metadata (teams, venue, outcome, etc.)
- **deliveries**: Ball-by-ball data
- **players**: Player registry
- **match_innings**: Innings summaries
- **player_elo_history**: Player ELO ratings over time

## Data Source

All data comes from [Cricsheet](https://cricsheet.org).

## Size

The database size depends on how much data you download:
- ODI + T20I + IPL (2018-present): ~500MB
- All formats (all years): ~2-3GB
