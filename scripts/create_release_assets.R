# create_release_assets.R
# Create ZIP files for each data folder for GitHub Release

library(jsonlite)
library(zip)
library(cli)

# Configuration
INPUT_DIR <- "classified_json"
OUTPUT_DIR <- "release_assets"

# Data folders
DATA_FOLDERS <- c(
  "long_form_male_international",
  "long_form_male_club",
  "long_form_female_international",
  "long_form_female_club",
  "short_form_male_international",
  "short_form_male_club",
  "short_form_female_international",
  "short_form_female_club"
)

#' Create ZIP archive for a folder
create_folder_zip <- function(folder_name) {
  folder_path <- file.path(INPUT_DIR, folder_name)

  if (!dir.exists(folder_path)) {
    cli_alert_warning("Folder not found: {folder_path}")
    return(NULL)
  }

  # Get all JSON files in folder
  json_files <- list.files(folder_path, pattern = "\\.json$", full.names = TRUE)

  if (length(json_files) == 0) {
    cli_alert_info("{folder_name}: empty (0 files)")
    # Create empty placeholder
    placeholder <- file.path(OUTPUT_DIR, paste0(folder_name, ".txt"))
    writeLines(paste0("No matches in ", folder_name), placeholder)
    return(NULL)
  }

  # Create ZIP
  zip_name <- paste0(folder_name, ".zip")
  zip_path <- file.path(OUTPUT_DIR, zip_name)

  cli_alert_info("Creating {zip_name} ({length(json_files)} files)...")

  # Use zipr for cross-platform compatibility
  zipr(zip_path, json_files, include_directories = FALSE)

  # Get file size
  size_mb <- file.size(zip_path) / 1024 / 1024

  cli_alert_success("{zip_name}: {round(size_mb, 1)} MB")

  list(
    folder = folder_name,
    zip_path = zip_path,
    file_count = length(json_files),
    size_bytes = file.size(zip_path)
  )
}

#' Create manifest with all release info
create_manifest <- function(zip_info_list) {
  manifest <- list(
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    release_date = format(Sys.Date(), "%Y.%m.%d"),
    folders = lapply(zip_info_list, function(info) {
      if (is.null(info)) return(NULL)
      list(
        name = info$folder,
        file_count = info$file_count,
        size_bytes = info$size_bytes
      )
    })
  )

  # Remove NULLs
  manifest$folders <- Filter(Negate(is.null), manifest$folders)

  # Calculate totals
  manifest$total_matches <- sum(sapply(manifest$folders, `[[`, "file_count"))
  manifest$total_size_bytes <- sum(sapply(manifest$folders, `[[`, "size_bytes"))

  manifest_path <- file.path(OUTPUT_DIR, "manifest.json")
  write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE)

  cli_alert_success("Created manifest.json")

  manifest
}

# Main execution
cli_h1("Creating Release Assets")

# Create output directory
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# Create ZIP for each folder
zip_info_list <- list()
for (folder in DATA_FOLDERS) {
  zip_info_list[[folder]] <- create_folder_zip(folder)
}

# Create manifest
manifest <- create_manifest(zip_info_list)

# Summary
cli_h2("Release Summary")
cli_alert_info("Total matches: {manifest$total_matches}")
cli_alert_info("Total size: {round(manifest$total_size_bytes / 1024 / 1024, 1)} MB")
cli_alert_info("Assets in: {OUTPUT_DIR}/")

# List files
cli_h3("Release Assets")
for (f in list.files(OUTPUT_DIR)) {
  size <- file.size(file.path(OUTPUT_DIR, f))
  cli_alert_info("{f}: {round(size / 1024 / 1024, 2)} MB")
}

cli_alert_success("Release assets ready!")
