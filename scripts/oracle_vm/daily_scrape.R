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
library(piggyback)

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
# Use recently_added_7 (added in last 7 days) for efficient daily updates
# This catches matches added late, not just recently played
CRICSHEET_RECENT_JSON <- paste0(CRICSHEET_BASE, "/recently_added_7_json.zip")

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
  resp <- request(CRICSHEET_RECENT_JSON) |>
    req_method("HEAD") |>
    req_timeout(30) |>
    req_perform()

  new_etag <- resp_header(resp, "ETag")
  if (is.null(new_etag)) new_etag <- ""
  cli_alert_info("Current ETag: {new_etag}")

  # Compare ETags (both must be non-empty strings to match)
  old_etag_str <- if (is.null(old_etag)) "" else old_etag
  if (nchar(old_etag_str) > 0 && nchar(new_etag) > 0 && old_etag_str == new_etag) {
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

  resp <- request(CRICSHEET_RECENT_JSON) |>
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

#' Upload parquets to GitHub Release using piggyback
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

  # Ensure release exists (piggyback will create if needed)
  tryCatch({
    pb_release_create(repo = REPO, tag = "recent")
    cli_alert_info("Created 'core' release")
  }, error = function(e) {
    # Release already exists, that's fine
    cli_alert_info("Release 'core' already exists")
  })

  # Upload each file (overwrites existing)
  for (file_path in existing_files) {
    cli_alert_info("  Uploading {basename(file_path)}...")
    tryCatch({
      pb_upload(
        file = file_path,
        repo = REPO,
        tag = "recent",
        overwrite = TRUE
      )
      cli_alert_success("  Uploaded {basename(file_path)}")
    }, error = function(e) {
      cli_alert_danger("  Failed to upload {basename(file_path)}: {e$message}")
    })
  }

  cli_alert_success("Upload complete!")
  cli_alert_info("Release: https://github.com/{REPO}/releases/tag/recent")

  invisible(TRUE)
}

#' Download existing parquet data from GitHub
download_existing_data <- function() {
  cli_h2("Downloading existing data from GitHub")

  existing_data <- list(matches = NULL, deliveries = NULL, players = NULL)

  for (file in c("matches.parquet", "deliveries.parquet", "players.parquet")) {
    local_path <- file.path(PARQUET_DIR, file)

    tryCatch({
      pb_download(
        file = file,
        repo = REPO,
        tag = "recent",
        dest = PARQUET_DIR,
        overwrite = TRUE
      )

      if (file.exists(local_path)) {
        name <- gsub("\\.parquet$", "", file)
        existing_data[[name]] <- read_parquet(local_path)
        cli_alert_success("  {file}: {nrow(existing_data[[name]])} rows")
      }
    }, error = function(e) {
      cli_alert_warning("  {file}: not found (first run?)")
    })
  }

  existing_data
}

#' Merge new data with existing data (deduplicating by match_id)
merge_data <- function(existing, new_data) {
  cli_h2("Merging with existing data")

  result <- list()

  # Merge matches
  if (!is.null(existing$matches) && nrow(existing$matches) > 0) {
    # Get new match IDs not in existing
    new_match_ids <- setdiff(new_data$matches$match_id, existing$matches$match_id)
    new_matches <- new_data$matches[new_data$matches$match_id %in% new_match_ids, ]
    result$matches <- rbind(existing$matches, new_matches)
    cli_alert_info("  Matches: {nrow(existing$matches)} existing + {nrow(new_matches)} new = {nrow(result$matches)}")
  } else {
    result$matches <- new_data$matches
    cli_alert_info("  Matches: {nrow(result$matches)} (all new)")
  }

  # Merge deliveries
  if (!is.null(existing$deliveries) && nrow(existing$deliveries) > 0) {
    new_match_ids <- setdiff(unique(new_data$deliveries$match_id), unique(existing$deliveries$match_id))
    new_deliveries <- new_data$deliveries[new_data$deliveries$match_id %in% new_match_ids, ]
    result$deliveries <- rbind(existing$deliveries, new_deliveries)
    cli_alert_info("  Deliveries: {nrow(existing$deliveries)} existing + {nrow(new_deliveries)} new = {nrow(result$deliveries)}")
  } else {
    result$deliveries <- new_data$deliveries
    cli_alert_info("  Deliveries: {nrow(result$deliveries)} (all new)")
  }

  # Merge players (dedupe by player_id)
  if (!is.null(existing$players) && nrow(existing$players) > 0) {
    all_players <- rbind(existing$players, new_data$players)
    result$players <- all_players[!duplicated(all_players$player_id), ]
    cli_alert_info("  Players: {nrow(result$players)} unique")
  } else {
    result$players <- new_data$players[!duplicated(new_data$players$player_id), ]
    cli_alert_info("  Players: {nrow(result$players)} (all new)")
  }

  result
}

# ============================================================
# MAIN EXECUTION
# ============================================================

cli_h1("Bouncer Daily Scrape")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
cli_alert_info("Using: recently_added_7_json (added in last 7 days)")

# Ensure data directory exists
dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

# Step 1: Check for updates
has_updates <- check_cricsheet_updates()

if (!has_updates) {
  cli_alert_success("No updates needed. Exiting.")
  quit(save = "no", status = 0)
}

# Step 2: Download new JSON data (last 7 days)
# NOTE: Like panna, we do incremental-only updates on the VM.
# The full "core" dataset was seeded from a local machine.
# VM just uploads recent matches to "recent" tag.
json_dir <- download_cricsheet_data()

# Step 3: Parse JSON to data frames
new_data <- parse_all_matches(json_dir)

# Step 4: Export parquets (just recent data)
export_parquets(new_data)

# Step 5: Upload to "recent" tag (not "core")
upload_to_github()

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
