#!/usr/bin/env Rscript
# daily_scrape.R - Daily Cricsheet sync and parquet upload for bouncerdata
#
# This script runs on Oracle Cloud VM to:
# 1. Check Cricsheet for new data (ETag caching)
# 2. Download new JSON files
# 3. Parse JSON directly to data frames (NO DuckDB needed!)
# 4. Export to parquet files
# 5. Upload to GitHub Release
#
# Prerequisites:
# - bouncer package installed (duckdb NOT required)
# - arrow package installed
# - gh CLI installed and authenticated
# - GITHUB_PAT environment variable set
#
# Usage: Run via cron at 7 AM UTC daily
#   0 7 * * * /home/opc/bouncer-scraper/run_scrape.sh

library(bouncer)
library(arrow)
library(cli)
library(httr2)
library(jsonlite)

# ============================================================
# CONFIGURATION
# ============================================================

REPO <- "peteowen1/bouncerdata"
DATA_DIR <- Sys.getenv("BOUNCER_DATA_DIR", "~/bouncer-scraper/data")
PARQUET_DIR <- file.path(DATA_DIR, "parquet")
JSON_DIR <- file.path(DATA_DIR, "json_files")
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

  dir.create(JSON_DIR, showWarnings = FALSE, recursive = TRUE)

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
  unzip(zip_path, exdir = JSON_DIR, overwrite = TRUE)
  file.remove(zip_path)

  n_files <- length(list.files(JSON_DIR, pattern = "\\.json$", recursive = TRUE))
  cli_alert_success("Extracted {n_files} JSON files")

  return(JSON_DIR)
}

#' Parse all JSON files directly to data frames (NO DuckDB!)
parse_all_matches <- function(json_dir) {
  cli_h2("Parsing JSON files")

  json_files <- list.files(json_dir, pattern = "\\.json$",
                           full.names = TRUE, recursive = TRUE)

  cli_alert_info("Found {length(json_files)} JSON files to parse")

  # Pre-allocate lists for combining
  all_matches <- vector("list", length(json_files))
  all_deliveries <- vector("list", length(json_files))
  all_players <- vector("list", length(json_files))

  # Progress bar
  pb <- cli_progress_bar("Parsing", total = length(json_files))

  for (i in seq_along(json_files)) {
    tryCatch({
      parsed <- parse_cricsheet_json(json_files[i])

      all_matches[[i]] <- parsed$match_info
      all_deliveries[[i]] <- parsed$deliveries
      all_players[[i]] <- parsed$players

    }, error = function(e) {
      cli_alert_warning("Failed to parse {basename(json_files[i])}: {e$message}")
    })

    cli_progress_update(id = pb)
  }

  cli_progress_done(id = pb)

  # Combine all data frames
  cli_alert_info("Combining data frames...")

  matches_df <- do.call(rbind, Filter(function(x) nrow(x) > 0, all_matches))
  deliveries_df <- do.call(rbind, Filter(function(x) nrow(x) > 0, all_deliveries))
  players_df <- do.call(rbind, Filter(function(x) nrow(x) > 0, all_players))

  # Deduplicate players
  players_df <- players_df[!duplicated(players_df$player_id), ]

  cli_alert_success("Parsed: {nrow(matches_df)} matches, {nrow(deliveries_df)} deliveries, {nrow(players_df)} players")

  list(
    matches = matches_df,
    deliveries = deliveries_df,
    players = players_df
  )
}

#' Export tables to parquet (unified structure)
export_parquets <- function(data) {
  cli_h2("Exporting to Parquet")

  dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)
  export_count <- 0

  # Matches
  if (!is.null(data$matches) && nrow(data$matches) > 0) {
    write_parquet(data$matches, file.path(PARQUET_DIR, "matches.parquet"), compression = "zstd")
    export_count <- export_count + 1
    cli_alert_success("  matches.parquet: {nrow(data$matches)} rows")
  }

  # Deliveries (add team_type from matches)
  if (!is.null(data$deliveries) && nrow(data$deliveries) > 0) {
    # Join team_type from matches
    deliveries_with_type <- merge(
      data$deliveries,
      data$matches[, c("match_id", "team_type")],
      by = "match_id",
      all.x = TRUE
    )
    write_parquet(deliveries_with_type, file.path(PARQUET_DIR, "deliveries.parquet"), compression = "zstd")
    export_count <- export_count + 1
    cli_alert_success("  deliveries.parquet: {nrow(deliveries_with_type)} rows")
  }

  # Players
  if (!is.null(data$players) && nrow(data$players) > 0) {
    write_parquet(data$players, file.path(PARQUET_DIR, "players.parquet"), compression = "zstd")
    export_count <- export_count + 1
    cli_alert_success("  players.parquet: {nrow(data$players)} rows")
  }

  cli_alert_success("Exported {export_count} parquet files")
}

#' Upload parquets to GitHub Release using gh CLI
upload_to_github <- function() {
  cli_h2("Uploading to GitHub")

  # Core release only (no ratings - those require DuckDB/full pipeline)
  files <- c("matches.parquet", "deliveries.parquet", "players.parquet")
  file_paths <- file.path(PARQUET_DIR, files)
  existing_files <- file_paths[file.exists(file_paths)]

  if (length(existing_files) == 0) {
    cli_alert_warning("No parquet files found to upload")
    return(invisible(FALSE))
  }

  cli_alert_info("Uploading {length(existing_files)} files to 'core' release...")

  # Build file arguments
  files_arg <- paste(shQuote(existing_files), collapse = " ")

  # Delete existing release (ignore errors if doesn't exist)
  system2("gh", c("release", "delete", "core", "--repo", REPO, "--yes"),
          stdout = FALSE, stderr = FALSE)

  # Create release with files
  cmd <- sprintf(
    "gh release create core %s --repo %s --title %s --notes %s",
    files_arg,
    REPO,
    shQuote("Core Data"),
    shQuote(sprintf("Updated: %s", format(Sys.time(), "%Y-%m-%d %H:%M UTC")))
  )

  result <- system(cmd)

  if (result == 0) {
    cli_alert_success("Uploaded {length(existing_files)} files to core release")
  } else {
    cli_alert_danger("Failed to upload to GitHub")
    return(invisible(FALSE))
  }

  cli_alert_success("Upload complete!")
  cli_alert_info("Release: https://github.com/{REPO}/releases/tag/core")

  invisible(TRUE)
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

# Step 3: Parse JSON to data frames (NO DuckDB!)
data <- parse_all_matches(json_dir)

# Step 4: Export parquets
export_parquets(data)

# Step 5: Upload to GitHub
upload_to_github()

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
