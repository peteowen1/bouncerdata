#!/usr/bin/env Rscript
# daily_scrape.R - Daily Cricsheet sync and parquet upload for bouncerdata
#
# This script runs on Oracle Cloud VM to:
# 1. Check Cricsheet for new data (ETag caching)
# 2. Download new JSON files
# 3. Update the DuckDB database
# 4. Re-export parquet files
# 5. Upload to GitHub Release
#
# Prerequisites:
# - bouncer package installed via micromamba
# - GITHUB_PAT environment variable set
# - R_ZIPCMD set to /usr/bin/zip
#
# Usage: Run via cron at 7 AM UTC daily
#   0 7 * * * /home/opc/bouncer-scraper/run_scrape.sh

library(bouncer)
library(piggyback)
library(cli)
library(httr2)
library(jsonlite)
library(duckdb)
library(arrow)

# ============================================================
# CONFIGURATION
# ============================================================

REPO <- "peteowen1/bouncerdata"
DATA_DIR <- Sys.getenv("BOUNCER_DATA_DIR", "~/bouncer-scraper/data")
DB_PATH <- file.path(DATA_DIR, "bouncer.duckdb")
PARQUET_DIR <- file.path(DATA_DIR, "parquet")
ETAG_CACHE <- file.path(DATA_DIR, "etag_cache.json")

# Cricsheet URLs
CRICSHEET_BASE <- "https://cricsheet.org/downloads"
CRICSHEET_ALL_JSON <- paste0(CRICSHEET_BASE, "/all_json.zip")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

#' Check if Cricsheet has new data using ETag
check_cricsheet_updates <- function() {
  cli_h2("Checking Cricsheet for updates")

  # Load cached ETag
  old_etag <- NULL
  if (file.exists(ETAG_CACHE)) {
    cache <- fromJSON(ETAG_CACHE)
    old_etag <- cache$etag
    cli_alert_info("Cached ETag: {old_etag}")
  }

  # HEAD request to check current ETag
  resp <- request(CRICSHEET_ALL_JSON) |>
    req_method("HEAD") |>
    req_timeout(30) |>
    req_perform()

  new_etag <- resp_header(resp, "ETag")
  cli_alert_info("Current ETag: {new_etag}")

  if (!is.null(old_etag) && old_etag == new_etag) {
    cli_alert_success("No new data available")
    return(FALSE)
  }

  cli_alert_info("New data available!")

  # Save new ETag
  write_json(list(etag = new_etag, checked_at = Sys.time()), ETAG_CACHE, auto_unbox = TRUE)

  return(TRUE)
}

#' Download and extract Cricsheet data
download_cricsheet_data <- function() {
  cli_h2("Downloading Cricsheet data")

  json_dir <- file.path(DATA_DIR, "json_files")
  dir.create(json_dir, showWarnings = FALSE, recursive = TRUE)

  # Download ZIP
  zip_path <- tempfile(fileext = ".zip")
  cli_alert_info("Downloading all_json.zip...")

  resp <- request(CRICSHEET_ALL_JSON) |>
    req_timeout(600) |>
    req_perform(path = zip_path)

  size_mb <- file.size(zip_path) / 1024 / 1024
  cli_alert_success("Downloaded: {round(size_mb, 1)} MB")

  # Extract
  cli_alert_info("Extracting...")
  unzip(zip_path, exdir = json_dir, overwrite = TRUE)
  file.remove(zip_path)

  n_files <- length(list.files(json_dir, pattern = "\\.json$", recursive = TRUE))
  cli_alert_success("Extracted {n_files} JSON files")

  return(json_dir)
}

#' Update DuckDB database with new matches
update_database <- function(json_dir) {
  cli_h2("Updating database")

  json_files <- list.files(json_dir, pattern = "\\.json$",
                           full.names = TRUE, recursive = TRUE)

  cli_alert_info("Found {length(json_files)} JSON files")

  # Initialize or connect to database
  if (!file.exists(DB_PATH)) {
    cli_alert_info("Creating new database...")
    initialize_bouncer_database(db_path = DB_PATH, overwrite = TRUE)
  }

  # Batch load matches (handles duplicates)
  cli_alert_info("Loading matches into database...")
  batch_load_matches(json_files, db_path = DB_PATH, progress = TRUE)

  # Get stats
  con <- dbConnect(duckdb(), DB_PATH, read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE))

  n_matches <- dbGetQuery(con, "SELECT COUNT(*) as n FROM matches")$n
  n_deliveries <- dbGetQuery(con, "SELECT COUNT(*) as n FROM deliveries")$n

  cli_alert_success("Database updated: {n_matches} matches, {n_deliveries} deliveries")
}

#' Export tables to parquet (unified structure)
export_parquets <- function() {
  cli_h2("Exporting to Parquet")

  dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

  con <- dbConnect(duckdb(), DB_PATH, read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE))

  tables <- dbListTables(con)
  export_count <- 0

  # ============================================================
  # CORE TABLES (unified)
  # ============================================================

  # Matches
  data <- dbGetQuery(con, "SELECT * FROM matches")
  if (nrow(data) > 0) {
    write_parquet(data, file.path(PARQUET_DIR, "matches.parquet"), compression = "zstd")
    export_count <- export_count + 1
    cli_alert_success("  matches.parquet: {nrow(data)} rows")
  }

  # Deliveries (with team_type from matches)
  data <- dbGetQuery(con, "
    SELECT d.*, m.team_type
    FROM deliveries d
    LEFT JOIN matches m ON d.match_id = m.match_id
  ")
  if (nrow(data) > 0) {
    write_parquet(data, file.path(PARQUET_DIR, "deliveries.parquet"), compression = "zstd")
    export_count <- export_count + 1
    cli_alert_success("  deliveries.parquet: {nrow(data)} rows")
  }

  # Players
  if ("players" %in% tables) {
    data <- dbGetQuery(con, "SELECT * FROM players")
    if (nrow(data) > 0) {
      write_parquet(data, file.path(PARQUET_DIR, "players.parquet"), compression = "zstd")
      export_count <- export_count + 1
    }
  }

  # Team ELO
  if ("team_elo" %in% tables) {
    data <- dbGetQuery(con, "SELECT * FROM team_elo")
    if (nrow(data) > 0) {
      write_parquet(data, file.path(PARQUET_DIR, "team_elo.parquet"), compression = "zstd")
      export_count <- export_count + 1
    }
  }

  # ============================================================
  # RATING TABLES (in subdirectories)
  # ============================================================

  # Player ratings
  player_dir <- file.path(PARQUET_DIR, "player_rating")
  dir.create(player_dir, showWarnings = FALSE)
  for (tbl in c("test_player_skill", "odi_player_skill", "t20_player_skill")) {
    if (tbl %in% tables) {
      data <- dbGetQuery(con, sprintf("SELECT * FROM %s", tbl))
      if (nrow(data) > 0) {
        write_parquet(data, file.path(player_dir, paste0(tbl, ".parquet")), compression = "zstd")
        export_count <- export_count + 1
      }
    }
  }

  # Team ratings
  team_dir <- file.path(PARQUET_DIR, "team_rating")
  dir.create(team_dir, showWarnings = FALSE)
  for (tbl in c("test_team_skill", "odi_team_skill", "t20_team_skill")) {
    if (tbl %in% tables) {
      data <- dbGetQuery(con, sprintf("SELECT * FROM %s", tbl))
      if (nrow(data) > 0) {
        write_parquet(data, file.path(team_dir, paste0(tbl, ".parquet")), compression = "zstd")
        export_count <- export_count + 1
      }
    }
  }

  # Venue ratings
  venue_dir <- file.path(PARQUET_DIR, "venue_rating")
  dir.create(venue_dir, showWarnings = FALSE)
  for (tbl in c("test_venue_skill", "odi_venue_skill", "t20_venue_skill")) {
    if (tbl %in% tables) {
      data <- dbGetQuery(con, sprintf("SELECT * FROM %s", tbl))
      if (nrow(data) > 0) {
        write_parquet(data, file.path(venue_dir, paste0(tbl, ".parquet")), compression = "zstd")
        export_count <- export_count + 1
      }
    }
  }

  cli_alert_success("Exported {export_count} parquet files")
}

#' Upload parquets to GitHub Release using gh CLI
#' Uploads to 4 separate releases: core, player_rating, team_rating, venue_rating
upload_to_github <- function() {
  cli_h2("Uploading to GitHub")

  # Release configurations
  releases <- list(
    list(
      tag = "core",
      title = "Core Data",
      files = c("matches.parquet", "deliveries.parquet", "players.parquet", "team_elo.parquet"),
      dir = PARQUET_DIR
    ),
    list(
      tag = "player_rating",
      title = "Player Ratings",
      files = c("test_player_skill.parquet", "odi_player_skill.parquet", "t20_player_skill.parquet"),
      dir = file.path(PARQUET_DIR, "player_rating")
    ),
    list(
      tag = "team_rating",
      title = "Team Ratings",
      files = c("test_team_skill.parquet", "odi_team_skill.parquet", "t20_team_skill.parquet"),
      dir = file.path(PARQUET_DIR, "team_rating")
    ),
    list(
      tag = "venue_rating",
      title = "Venue Ratings",
      files = c("test_venue_skill.parquet", "odi_venue_skill.parquet", "t20_venue_skill.parquet"),
      dir = file.path(PARQUET_DIR, "venue_rating")
    )
  )

  for (rel in releases) {
    cli_alert_info("Uploading to {rel$tag}...")

    # Get full paths to files that exist
    file_paths <- file.path(rel$dir, rel$files)
    existing_files <- file_paths[file.exists(file_paths)]

    if (length(existing_files) == 0) {
      cli_alert_warning("  No files found for {rel$tag}, skipping")
      next
    }

    # Build gh command - delete and recreate release to ensure clean state
    files_arg <- paste(shQuote(existing_files), collapse = " ")

    # Try to delete existing release first (ignore errors if it doesn't exist)
    system2("gh", c("release", "delete", rel$tag, "--repo", REPO, "--yes"),
            stdout = FALSE, stderr = FALSE)

    # Create release with files
    cmd <- sprintf(
      "gh release create %s %s --repo %s --title %s --notes %s",
      rel$tag,
      files_arg,
      REPO,
      shQuote(rel$title),
      shQuote(sprintf("Updated: %s", format(Sys.time(), "%Y-%m-%d %H:%M UTC")))
    )

    result <- system(cmd)

    if (result == 0) {
      cli_alert_success("  Uploaded {length(existing_files)} files to {rel$tag}")
    } else {
      cli_alert_danger("  Failed to upload {rel$tag}")
    }
  }

  cli_alert_success("Upload complete!")
  cli_alert_info("Releases: https://github.com/{REPO}/releases")
}

# ============================================================
# MAIN EXECUTION
# ============================================================

cli_h1("Bouncer Daily Scrape")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")

# Ensure data directory exists
dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)

# Step 1: Check for updates
has_updates <- check_cricsheet_updates()

if (!has_updates) {
  cli_alert_success("No updates needed. Exiting.")
  quit(save = "no", status = 0)
}

# Step 2: Download new data
json_dir <- download_cricsheet_data()

# Step 3: Update database
update_database(json_dir)

# Step 4: Export parquets
export_parquets()

# Step 5: Upload to GitHub
upload_to_github()

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
