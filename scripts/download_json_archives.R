# download_json_archives.R
# Download JSON archives from latest GitHub Release

library(httr2)
library(jsonlite)
library(zip)
library(cli)

# Configuration
REPO <- "peteowen1/bouncerdata"
OUTPUT_DIR <- "json_temp"

#' Get latest release info from GitHub API
get_latest_release <- function() {
  cli_h2("Getting latest release info")

  url <- sprintf("https://api.github.com/repos/%s/releases/latest", REPO)

  # Use GITHUB_TOKEN if available (for rate limiting)
  token <- Sys.getenv("GITHUB_TOKEN")

  req <- request(url) |>
    req_headers(Accept = "application/vnd.github.v3+json")

  if (nzchar(token)) {
    req <- req |> req_auth_bearer_token(token)
  }

  resp <- req_perform(req)
  release <- resp_body_json(resp)

  cli_alert_info("Latest release: {release$tag_name}")
  cli_alert_info("Published: {release$published_at}")
  cli_alert_info("Assets: {length(release$assets)}")

  release
}

#' Download a release asset
download_asset <- function(asset, output_dir) {
  name <- asset$name
  url <- asset$browser_download_url
  size_mb <- asset$size / 1024 / 1024

  cli_alert_info("Downloading {name} ({round(size_mb, 1)} MB)...")

  dest_path <- file.path(output_dir, name)

  # Use GITHUB_TOKEN for private repos
  token <- Sys.getenv("GITHUB_TOKEN")

  req <- request(url) |>
    req_timeout(600)

  if (nzchar(token)) {
    req <- req |> req_auth_bearer_token(token)
  }

  req |>
    req_progress() |>
    req_perform(path = dest_path)

  cli_alert_success("Downloaded: {dest_path}")

  dest_path
}

#' Extract ZIP files
extract_zips <- function(zip_files, output_dir) {
  cli_h2("Extracting ZIP files")

  json_dir <- file.path(output_dir, "json_files")
  dir.create(json_dir, showWarnings = FALSE, recursive = TRUE)

  total_files <- 0L

  for (zip_file in zip_files) {
    if (!grepl("\\.zip$", zip_file)) next

    cli_alert_info("Extracting {basename(zip_file)}...")

    # Get folder name from zip name (e.g., short_form_male_club.zip -> short_form_male_club)
    folder_name <- tools::file_path_sans_ext(basename(zip_file))
    folder_path <- file.path(json_dir, folder_name)
    dir.create(folder_path, showWarnings = FALSE, recursive = TRUE)

    unzip(zip_file, exdir = folder_path)

    n_files <- length(list.files(folder_path, pattern = "\\.json$"))
    total_files <- total_files + n_files
    cli_alert_success("{folder_name}: {n_files} files")
  }

  cli_alert_success("Total: {total_files} JSON files extracted")

  json_dir
}

# Main execution
cli_h1("Downloading JSON Archives from GitHub Release")

# Create output directory
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# Get latest release
release <- get_latest_release()

# Filter to only ZIP files (data archives, not manifest)
zip_assets <- Filter(function(a) grepl("\\.zip$", a$name), release$assets)

cli_alert_info("Found {length(zip_assets)} ZIP archives to download")

# Download each asset
downloaded_files <- character()
for (asset in zip_assets) {
  path <- download_asset(asset, OUTPUT_DIR)
  downloaded_files <- c(downloaded_files, path)
}

# Extract all ZIPs
json_dir <- extract_zips(downloaded_files, OUTPUT_DIR)

cli_alert_success("JSON files ready in: {json_dir}")
