# upload_to_release.R
# Upload parquet files to GitHub Releases
#
# Supports multiple release types:
#   - "core"          : Core data (matches, deliveries, players, team_elo)
#   - "player_rating" : Player skill indices (all formats)
#   - "team_rating"   : Team skill indices (all formats)
#   - "venue_rating"  : Venue skill indices (all formats)
#
# Prerequisites:
# - Run export_parquets.R first to create parquet_output/
# - Set GITHUB_PAT environment variable or use .github_pat file
#
# Usage:
#   RELEASE_TYPE <- "core"  # or "player_rating", "team_rating", "venue_rating"
#   source("scripts/upload_to_release.R")

library(piggyback)
library(cli)

# Configuration
REPO <- "peteowen1/bouncerdata"
PARQUET_DIR <- "parquet_output"

# Get release type from environment or default to "core"
RELEASE_TYPE <- Sys.getenv("RELEASE_TYPE", "core")

# Release configurations
RELEASE_CONFIG <- list(
  "core" = list(
    tag = "core",
    name = "Core Data",
    dir = PARQUET_DIR,
    pattern = "^(matches|deliveries|players|team_elo|manifest)\\.(parquet|json)$",
    description = "Core cricket data updated daily: matches, deliveries, players, team_elo"
  ),
  "player_rating" = list(
    tag = "player_rating",
    name = "Player Ratings",
    dir = file.path(PARQUET_DIR, "player_rating"),
    pattern = "\\.parquet$",
    description = "Per-delivery player skill indices for all formats (T20, ODI, Test)"
  ),
  "team_rating" = list(
    tag = "team_rating",
    name = "Team Ratings",
    dir = file.path(PARQUET_DIR, "team_rating"),
    pattern = "\\.parquet$",
    description = "Per-delivery team skill indices for all formats (T20, ODI, Test)"
  ),
  "venue_rating" = list(
    tag = "venue_rating",
    name = "Venue Ratings",
    dir = file.path(PARQUET_DIR, "venue_rating"),
    pattern = "\\.parquet$",
    description = "Per-delivery venue skill indices for all formats (T20, ODI, Test)"
  )
)

# Set up GitHub token
setup_github_token <- function() {
  if (Sys.getenv("GITHUB_PAT") != "") {
    cli_alert_success("Using GITHUB_PAT from environment")
    return(invisible(NULL))
  }

  pat_file <- path.expand("~/.github_pat")
  if (file.exists(pat_file)) {
    pat <- trimws(readLines(pat_file, n = 1, warn = FALSE))
    Sys.setenv(GITHUB_PAT = pat)
    cli_alert_success("Using token from ~/.github_pat")
    return(invisible(NULL))
  }

  cli_abort("No GitHub token found. Set GITHUB_PAT or create ~/.github_pat")
}

# Validate release type
if (!RELEASE_TYPE %in% names(RELEASE_CONFIG)) {
  cli_abort("Invalid RELEASE_TYPE: {RELEASE_TYPE}. Must be one of: {paste(names(RELEASE_CONFIG), collapse=', ')}")
}

config <- RELEASE_CONFIG[[RELEASE_TYPE]]

# Main execution
cli_h1("Uploading to GitHub Release: {config$name}")
cli_alert_info("Release type: {RELEASE_TYPE}")

# Check directory exists
if (!dir.exists(config$dir)) {
  cli_abort("Directory not found: {config$dir}\nRun export_parquets.R first!")
}

# Get files to upload
all_files <- list.files(config$dir, full.names = TRUE)
parquet_files <- all_files[grepl(config$pattern, basename(all_files))]

if (length(parquet_files) == 0) {
  cli_abort("No matching files found in {config$dir}")
}

cli_alert_info("Found {length(parquet_files)} files to upload")

# Calculate total size
total_size <- sum(file.size(parquet_files))
cli_alert_info("Total size: {round(total_size / 1024 / 1024, 1)} MB")

# Setup token
setup_github_token()

release_tag <- config$tag
cli_alert_info("Release tag: {release_tag}")

# Check if release exists
cli_h2("Managing Release")

existing_releases <- tryCatch(
  pb_releases(repo = REPO),
  error = function(e) data.frame()
)

if (release_tag %in% existing_releases$tag_name) {
  cli_alert_info("Release {release_tag} exists - will overwrite assets")
} else {
  cli_alert_info("Creating new release: {release_tag}")

  pb_new_release(
    repo = REPO,
    tag = release_tag,
    name = config$name,
    body = paste0(
      "## ", config$name, "\n\n",
      config$description, "\n\n",
      "**Last updated:** ", format(Sys.time(), "%Y-%m-%d %H:%M UTC"), "\n\n",
      "### Files\n",
      paste0("- `", basename(parquet_files), "`\n", collapse = ""),
      "\n### Usage\n",
      "```r\n",
      "library(bouncer)\n",
      if (RELEASE_TYPE == "core") {
        "matches <- load_matches(source = \"remote\")\ndeliveries <- load_deliveries(source = \"remote\")"
      } else {
        paste0("# Download from: https://github.com/", REPO, "/releases/tag/", release_tag)
      },
      "\n```"
    )
  )

  cli_alert_success("Release created")
}

# Upload files
cli_h2("Uploading Files")

for (i in seq_along(parquet_files)) {
  file_path <- parquet_files[i]
  file_name <- basename(file_path)
  size_mb <- round(file.size(file_path) / 1024 / 1024, 1)

  cli_alert_info("[{i}/{length(parquet_files)}] Uploading {file_name} ({size_mb} MB)...")

  tryCatch({
    pb_upload(
      file = file_path,
      repo = REPO,
      tag = release_tag,
      overwrite = TRUE
    )
    cli_alert_success("  Uploaded: {file_name}")
  }, error = function(e) {
    cli_alert_danger("  Failed: {file_name} - {e$message}")
  })
}

# Summary
cli_h2("Upload Complete")
cli_alert_success("Uploaded {length(parquet_files)} files to {REPO}")
cli_alert_info("Release URL: https://github.com/{REPO}/releases/tag/{release_tag}")

# Verify
release_assets <- pb_list(repo = REPO, tag = release_tag)
cli_alert_info("Release contains {nrow(release_assets)} assets")
