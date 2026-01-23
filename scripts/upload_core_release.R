#!/usr/bin/env Rscript
# upload_core_release.R - Upload local parquet files to GitHub "core" release
#
# Use this script to:
#   1. Do a full refresh of the core release from local data
#   2. Upload after making local changes to parquet files
#   3. Fix/replace corrupted release data
#
# Usage:
#   Rscript upload_core_release.R                    # Use default parquet dir
#   Rscript upload_core_release.R /path/to/parquets  # Use custom path
#
# Requirements:
#   - GitHub PAT with repo access (set via GITHUB_PAT env var or gh auth)
#   - piggyback package

library(piggyback)
library(cli)

# ============================================================
# CONFIGURATION
# ============================================================

REPO <- "peteowen1/bouncerdata"

# Default parquet directory (relative to bouncerverse/)
DEFAULT_PARQUET_DIR <- file.path(

  dirname(dirname(getwd())),  # Go up from scripts/ to bouncerdata/

  "parquet"
)

# Allow override via command line arg
args <- commandArgs(trailingOnly = TRUE)
PARQUET_DIR <- if (length(args) > 0) args[1] else DEFAULT_PARQUET_DIR

# Normalize path
PARQUET_DIR <- normalizePath(PARQUET_DIR, mustWork = FALSE)

# ============================================================
# VALIDATION
# ============================================================

cli_h1("Upload Core Release")
cli_alert_info("Repository: {REPO}")
cli_alert_info("Parquet directory: {PARQUET_DIR}")

if (!dir.exists(PARQUET_DIR)) {
  cli_abort("Parquet directory not found: {PARQUET_DIR}")
}

# Find parquet files
parquet_files <- list.files(PARQUET_DIR, pattern = "\\.parquet$", full.names = TRUE)

if (length(parquet_files) == 0) {
  cli_abort("No parquet files found in {PARQUET_DIR}")
}

# Show what we're uploading
cli_h2("Files to upload")
total_size <- 0
for (f in parquet_files) {
  size_mb <- file.size(f) / 1024 / 1024
  total_size <- total_size + size_mb
  cli_alert_info("  {basename(f)}: {round(size_mb, 1)} MB")
}
cli_alert_success("Total: {length(parquet_files)} files, {round(total_size, 1)} MB")

# ============================================================
# CONFIRMATION
# ============================================================

cli_h2("Confirm upload")
cli_alert_warning("This will REPLACE the current core release!")

if (interactive()) {
  response <- readline("Continue? [y/N]: ")
  if (!tolower(response) %in% c("y", "yes")) {
    cli_alert_info("Aborted by user")
    quit(save = "no", status = 0)
  }
} else {
  cli_alert_info("Running non-interactively, proceeding...")
}

# ============================================================
# CREATE ZIP
# ============================================================

cli_h2("Creating ZIP archive")

zip_file <- tempfile(fileext = ".zip")
if (file.exists(zip_file)) file.remove(zip_file)

old_wd <- getwd()
on.exit(setwd(old_wd), add = TRUE)
setwd(PARQUET_DIR)

# Create zip with relative paths
rel_files <- basename(parquet_files)
zip_result <- zip(zip_file, files = rel_files, flags = "-rq")

if (zip_result != 0) {
  cli_abort("Failed to create ZIP archive")
}

zip_size <- file.size(zip_file) / 1024 / 1024
cli_alert_success("Created ZIP: {round(zip_size, 1)} MB")

# ============================================================
# UPLOAD TO GITHUB
# ============================================================

cli_h2("Uploading to GitHub")

# Ensure release exists
tryCatch({
  pb_release_create(repo = REPO, tag = "core")
  cli_alert_info("Created 'core' release")
  Sys.sleep(2)
}, error = function(e) {
  cli_alert_info("Release 'core' already exists")
})

# Upload with retry
cli_alert_info("Uploading bouncerdata-parquet.zip...")

success <- FALSE
for (attempt in 1:3) {
  success <- tryCatch({
    pb_upload(
      file = zip_file,
      repo = REPO,
      tag = "core",
      name = "bouncerdata-parquet.zip",
      overwrite = TRUE
    )
    TRUE
  }, error = function(e) {
    if (attempt < 3) {
      cli_alert_warning("Attempt {attempt} failed: {e$message}")
      cli_alert_info("Retrying in 5 seconds...")
      Sys.sleep(5)
    } else {
      cli_alert_danger("Final attempt failed: {e$message}")
    }
    FALSE
  })

  if (success) break
}

# ============================================================
# SUMMARY
# ============================================================

cli_h2("Summary")

if (success) {
  cli_alert_success("Upload complete!")
  cli_alert_info("Release URL: https://github.com/{REPO}/releases/tag/core")
  cli_alert_info("Files: {length(parquet_files)} parquet files")
  cli_alert_info("Size: {round(zip_size, 1)} MB")
} else {
  cli_alert_danger("Upload failed after 3 attempts")
  cli_alert_info("Check your GitHub PAT and network connection")
  quit(save = "no", status = 1)
}

# Cleanup
file.remove(zip_file)
cli_alert_success("Done!")
