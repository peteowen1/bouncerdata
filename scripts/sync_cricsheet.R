# sync_cricsheet.R
# Download Cricsheet data and organize into 8 folders

library(httr2)
library(jsonlite)
library(zip)
library(cli)

# Configuration
CRICSHEET_URL <- "https://cricsheet.org/downloads/all_json.zip"
TEMP_DIR <- "json_temp"
OUTPUT_DIR <- "classified_json"

DATA_FOLDERS <- c(
  "long_form_male_international", "long_form_male_club",
  "long_form_female_international", "long_form_female_club",
  "short_form_male_international", "short_form_male_club",
  "short_form_female_international", "short_form_female_club"
)

# Classification constants
FORMAT_LONG_FORM <- c("Test", "MDM")
MATCH_TYPE_INTERNATIONAL <- c("Test", "ODI", "IT20")

#' Classify a match into one of 8 folders
classify_match <- function(match_type, gender) {
  format_cat <- if (match_type %in% FORMAT_LONG_FORM) "long_form" else "short_form"
  type_cat <- if (match_type %in% MATCH_TYPE_INTERNATIONAL) "international" else "club"
  paste(format_cat, gender %||% "male", type_cat, sep = "_")
}

#' Download Cricsheet all_json.zip
download_cricsheet <- function() {
  cli_h2("Downloading Cricsheet data")

  dir.create(TEMP_DIR, showWarnings = FALSE, recursive = TRUE)
  zip_path <- file.path(TEMP_DIR, "all_json.zip")

  cli_alert_info("Downloading from {.url {CRICSHEET_URL}}")
  cli_alert_info("This may take a few minutes (~93MB)...")

  request(CRICSHEET_URL) |>
    req_user_agent("bouncerdata GitHub Action") |>
    req_timeout(600) |>  # 10 minute timeout
    req_progress() |>
    req_perform(path = zip_path)

  cli_alert_success("Downloaded to {.file {zip_path}}")
  zip_path
}

#' Extract and classify JSON files
extract_and_classify <- function(zip_path) {
  cli_h2("Extracting and classifying matches")

  # Create output directories
  for (folder in DATA_FOLDERS) {
    dir.create(file.path(OUTPUT_DIR, folder), showWarnings = FALSE, recursive = TRUE)
  }

  # Extract all files to temp location
  extract_dir <- file.path(TEMP_DIR, "extracted")
  dir.create(extract_dir, showWarnings = FALSE, recursive = TRUE)

  cli_alert_info("Extracting ZIP file...")
  unzip(zip_path, exdir = extract_dir)

  # Find all JSON files
  json_files <- list.files(extract_dir, pattern = "\\.json$",
                            full.names = TRUE, recursive = TRUE)

  cli_alert_info("Found {length(json_files)} JSON files")

  # Track statistics
  folder_counts <- setNames(rep(0L, length(DATA_FOLDERS)), DATA_FOLDERS)
  errors <- 0L
  new_matches <- 0L
  changed_matches <- 0L

  # Process each file
  cli_progress_bar("Classifying matches", total = length(json_files))

  for (json_file in json_files) {
    cli_progress_update()

    tryCatch({
      # Read just enough to classify
      json_data <- fromJSON(json_file, simplifyVector = FALSE)
      info <- json_data$info

      match_type <- info$match_type %||% "T20"
      gender <- info$gender %||% "male"

      # Classify
      folder <- classify_match(match_type, gender)

      # Copy to classified folder
      dest_path <- file.path(OUTPUT_DIR, folder, basename(json_file))

      # Check if file is new or changed
      if (!file.exists(dest_path)) {
        new_matches <- new_matches + 1L
      } else if (file.size(json_file) != file.size(dest_path)) {
        changed_matches <- changed_matches + 1L
      }

      file.copy(json_file, dest_path, overwrite = TRUE)
      folder_counts[folder] <- folder_counts[folder] + 1L

    }, error = function(e) {
      errors <<- errors + 1L
    })
  }

  cli_progress_done()

  # Report results
  cli_h3("Classification Results")
  for (folder in DATA_FOLDERS) {
    cli_alert_info("{folder}: {folder_counts[folder]} matches")
  }

  cli_alert_success("Total: {sum(folder_counts)} matches")
  cli_alert_info("New: {new_matches} | Changed: {changed_matches} | Errors: {errors}")

  # Set environment variables for GitHub Actions
  if (Sys.getenv("GITHUB_ACTIONS") == "true") {
    env_file <- Sys.getenv("GITHUB_ENV")
    if (nzchar(env_file)) {
      cat(sprintf("NEW_MATCH_COUNT=%d\n", new_matches),
          file = env_file, append = TRUE)
      cat(sprintf("CHANGED_MATCH_COUNT=%d\n", changed_matches),
          file = env_file, append = TRUE)
      cat(sprintf("TOTAL_MATCH_COUNT=%d\n", sum(folder_counts)),
          file = env_file, append = TRUE)
    }
  }

  list(
    folder_counts = folder_counts,
    new_matches = new_matches,
    changed_matches = changed_matches,
    errors = errors
  )
}

# Main execution
cli_h1("Cricsheet Data Sync")

# Download
zip_path <- download_cricsheet()

# Extract and classify
result <- extract_and_classify(zip_path)

cli_alert_success("Sync complete!")
