# check_updates.R
# Check if Cricsheet has new data using HTTP HEAD + ETag caching

library(httr2)
library(jsonlite)
library(cli)

# Cricsheet all_json.zip URL
CRICSHEET_URL <- "https://cricsheet.org/downloads/all_json.zip"

# Cache file for ETag
CACHE_FILE <- "manifests/etag_cache.json"

#' Read cached ETag from file
read_cached_etag <- function() {
  if (file.exists(CACHE_FILE)) {
    cache <- fromJSON(CACHE_FILE)
    return(cache$etag)
  }
  NULL
}

#' Write ETag to cache file
write_etag_cache <- function(etag, last_modified) {
  dir.create(dirname(CACHE_FILE), showWarnings = FALSE, recursive = TRUE)
  cache <- list(
    etag = etag,
    last_modified = last_modified,
    checked_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )
  write_json(cache, CACHE_FILE, auto_unbox = TRUE, pretty = TRUE)
}

#' Check Cricsheet for updates using HTTP HEAD
check_cricsheet_updates <- function() {
  cli_h1("Checking Cricsheet for updates")

  # HTTP HEAD request to check headers without downloading
  cli_alert_info("Sending HEAD request to {.url {CRICSHEET_URL}}")

  resp <- tryCatch({
    request(CRICSHEET_URL) |>
      req_method("HEAD") |>
      req_timeout(30) |>
      req_perform()
  }, error = function(e) {
    cli_alert_danger("Failed to check Cricsheet: {e$message}")
    return(NULL)
  })

  if (is.null(resp)) {
    # On error, assume update needed
    return(list(needs_update = TRUE, reason = "HEAD request failed"))
  }

  # Extract headers
  current_etag <- resp_header(resp, "ETag")
  current_modified <- resp_header(resp, "Last-Modified")
  content_length <- resp_header(resp, "Content-Length")

  cli_alert_info("ETag: {current_etag %||% 'not provided'}")
  cli_alert_info("Last-Modified: {current_modified %||% 'not provided'}")
  cli_alert_info("Content-Length: {content_length %||% 'not provided'}")

  # Compare with cached ETag
  cached_etag <- read_cached_etag()

  if (is.null(cached_etag)) {
    cli_alert_warning("No cached ETag found - first run or cache cleared")
    needs_update <- TRUE
    reason <- "No previous cache"
  } else if (is.null(current_etag)) {
    cli_alert_warning("Server did not return ETag - falling back to download")
    needs_update <- TRUE
    reason <- "No ETag from server"
  } else if (current_etag != cached_etag) {
    cli_alert_success("ETag changed! Update available.")
    cli_alert_info("Old: {cached_etag}")
    cli_alert_info("New: {current_etag}")
    needs_update <- TRUE
    reason <- "ETag changed"
  } else {
    cli_alert_success("No changes detected (ETag unchanged)")
    needs_update <- FALSE
    reason <- "ETag unchanged"
  }

  # Update cache if we have new ETag
  if (!is.null(current_etag)) {
    write_etag_cache(current_etag, current_modified)
  }

  list(
    needs_update = needs_update,
    reason = reason,
    etag = current_etag,
    last_modified = current_modified,
    content_length = content_length
  )
}

# Main execution
cli_h1("Cricsheet Update Check")

# Check for force flag from environment
force_download <- Sys.getenv("FORCE_DOWNLOAD", "false") == "true"

if (force_download) {
  cli_alert_warning("Force download requested - skipping update check")
  needs_update <- TRUE
} else {
  result <- check_cricsheet_updates()
  needs_update <- result$needs_update
  cli_alert_info("Result: {result$reason}")
}

# Set environment variable for GitHub Actions
if (Sys.getenv("GITHUB_ACTIONS") == "true") {
  # Write to GITHUB_ENV file
  env_file <- Sys.getenv("GITHUB_ENV")
  if (nzchar(env_file)) {
    cat(sprintf("NEEDS_UPDATE=%s\n", tolower(as.character(needs_update))),
        file = env_file, append = TRUE)
    cat(sprintf("RELEASE_DATE=%s\n", format(Sys.Date(), "%Y.%m.%d")),
        file = env_file, append = TRUE)
  }
}

cli_alert_info("Needs update: {needs_update}")
