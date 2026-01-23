#!/usr/bin/env Rscript
# migrate_partitions.R - One-time migration to 3-way partitioning
#
# Converts from 6-way partitioning (by match_type only):
#   deliveries_Test.parquet, deliveries_ODI.parquet, etc.
#
# To 16-way partitioning (match_type x gender x team_type):
#   deliveries_Test_male_international.parquet
#   deliveries_T20_female_club.parquet
#   etc.
#
# Run this ONCE before deploying the new daily_scrape_lowmem.R

library(arrow)
library(dplyr)
library(cli)

# ============================================================
# CONFIGURATION
# ============================================================

DATA_DIR <- Sys.getenv("BOUNCER_DATA_DIR", "~/bouncer-scraper/data")
PARQUET_DIR <- file.path(DATA_DIR, "parquet")
BACKUP_DIR <- file.path(DATA_DIR, "backup_old_partitions")

OLD_MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")

# ============================================================
# MIGRATION
# ============================================================

cli_h1("Migrating to 3-way partitioning")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")

# Check if migration already done
new_partition_files <- list.files(PARQUET_DIR, pattern = "deliveries_.*_.*_.*\\.parquet$")
if (length(new_partition_files) > 0) {
  cli_alert_warning("Found {length(new_partition_files)} files with new partition naming")
  cli_alert_warning("Migration may have already been done. Exiting.")
  quit(save = "no", status = 0)
}

# Load matches to get gender and team_type for each match_id
cli_h2("Loading match metadata")
matches_path <- file.path(PARQUET_DIR, "matches.parquet")

if (!file.exists(matches_path)) {
  cli_alert_danger("No matches.parquet found!")
  quit(save = "no", status = 1)
}

matches <- read_parquet(matches_path, col_select = c("match_id", "match_type", "gender", "team_type"))
cli_alert_success("Loaded {nrow(matches)} matches")

# Create lookup: match_id -> partition_key
match_lookup <- setNames(
  paste(matches$match_type, matches$gender, matches$team_type, sep = "_"),
  matches$match_id
)
cli_alert_info("Created partition lookup for {length(match_lookup)} matches")

# Backup old files
cli_h2("Backing up old partition files")
dir.create(BACKUP_DIR, showWarnings = FALSE, recursive = TRUE)

for (mt in OLD_MATCH_TYPES) {
  old_file <- file.path(PARQUET_DIR, paste0("deliveries_", mt, ".parquet"))
  if (file.exists(old_file)) {
    backup_file <- file.path(BACKUP_DIR, paste0("deliveries_", mt, ".parquet"))
    file.copy(old_file, backup_file, overwrite = TRUE)
    cli_alert_success("Backed up: deliveries_{mt}.parquet")
  }
}

# Process each old partition file
cli_h2("Re-partitioning deliveries")

for (mt in OLD_MATCH_TYPES) {
  old_file <- file.path(PARQUET_DIR, paste0("deliveries_", mt, ".parquet"))

  if (!file.exists(old_file)) {
    cli_alert_info("Skipping {mt} (no file)")
    next
  }

  cli_alert_info("Processing {mt}...")

  # Read the old file
  del <- read_parquet(old_file)
  cli_alert_info("  Read {nrow(del)} deliveries")

  # Add partition_key based on match_id lookup
  del$partition_key <- match_lookup[del$match_id]

  # Handle any matches not in lookup (shouldn't happen, but be safe)
  missing_lookup <- is.na(del$partition_key)
  if (any(missing_lookup)) {
    cli_alert_warning("  {sum(missing_lookup)} deliveries have no match lookup - using match_type only")
    del$partition_key[missing_lookup] <- paste(mt, "unknown", "unknown", sep = "_")
  }

  # Split by partition_key and write new files
  partitions <- unique(del$partition_key)
  cli_alert_info("  Splitting into {length(partitions)} partitions")

  for (pk in partitions) {
    subset_del <- del[del$partition_key == pk, ]
    new_file <- file.path(PARQUET_DIR, paste0("deliveries_", pk, ".parquet"))

    if (file.exists(new_file)) {
      existing <- read_parquet(new_file, as_data_frame = FALSE)
      combined <- concat_tables(existing, arrow_table(subset_del))
      write_parquet(combined, new_file, compression = "zstd")
      cli_alert_success("    {pk}: appended {nrow(subset_del)} -> {combined$num_rows} total")
    } else {
      write_parquet(subset_del, new_file, compression = "zstd")
      cli_alert_success("    {pk}: {nrow(subset_del)} rows (new)")
    }
  }

  file.remove(old_file)
  cli_alert_info("  Removed old deliveries_{mt}.parquet")
  gc()
}

# Summary
cli_h2("Migration complete!")

new_files <- list.files(PARQUET_DIR, pattern = "deliveries_.*\\.parquet$")
cli_alert_success("Created {length(new_files)} partition files:")

for (f in sort(new_files)) {
  size_mb <- file.size(file.path(PARQUET_DIR, f)) / 1024 / 1024
  cli_alert_info("  {f}: {round(size_mb, 1)} MB")
}

cli_alert_info("Old files backed up to: {BACKUP_DIR}")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
