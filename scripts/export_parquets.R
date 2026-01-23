# export_parquets.R
# Export DuckDB tables to Parquet files
#
# Exports tables split by match_type/gender/team_type:
#   - matches_{match_type}_{gender}_{team_type}.parquet
#   - deliveries_{match_type}_{gender}_{team_type}.parquet
# Plus unified tables:
#   - players.parquet, team_elo.parquet, skill indices

library(duckdb)
library(arrow)
library(jsonlite)
library(cli)

# Null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Configuration
DB_PATH <- "bouncer.duckdb"
OUTPUT_DIR <- "parquet"

#' Export a table to Parquet
export_table <- function(con, table_name, output_path) {
  cli_alert_info("Exporting {basename(output_path)}...")

  data <- DBI::dbGetQuery(con, sprintf("SELECT * FROM %s", table_name))

  if (nrow(data) == 0) {
    cli_alert_warning("  No data to export for {basename(output_path)}")
    return(NULL)
  }

  write_parquet(data, output_path, compression = "zstd")

  size_mb <- file.size(output_path) / 1024 / 1024
  cli_alert_success("  {basename(output_path)}: {format(nrow(data), big.mark=',')} rows, {round(size_mb, 1)} MB")

  list(name = basename(output_path), rows = nrow(data), size_bytes = file.size(output_path))
}

#' Export multiple skill tables
export_skill_tables <- function(con, tables, prefix, available_tables, export_info) {
  for (tbl in tables) {
    if (tbl %in% available_tables) {
      export_info[[tbl]] <- export_table(con, tbl, file.path(OUTPUT_DIR, paste0(tbl, ".parquet")))
    }
  }
  export_info
}

#' Create manifest for parquet files
create_manifest <- function(export_info) {
  manifest <- list(
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    files = export_info,
    total_rows = sum(sapply(export_info, function(x) x$rows %||% 0)),
    total_size_bytes = sum(sapply(export_info, function(x) x$size_bytes %||% 0))
  )

  write_json(manifest, file.path(OUTPUT_DIR, "manifest.json"), auto_unbox = TRUE, pretty = TRUE)
  cli_alert_success("Created manifest.json")
  manifest
}

#' Print file sizes for a category
print_file_sizes <- function(files, label) {
  cli_h3(label)
  for (f in files) {
    size_mb <- file.size(file.path(OUTPUT_DIR, f)) / 1024 / 1024
    cli_alert_info("  {f}: {round(size_mb, 2)} MB")
  }
}

# Main execution
cli_h1("Exporting Tables to Parquet (Split by match_type/gender/team_type)")

# Create output directory
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# Connect to database
if (!file.exists(DB_PATH)) {
  cli_abort("Database not found: {DB_PATH}")
}

con <- dbConnect(duckdb(), DB_PATH, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE))

export_info <- list()
tables <- DBI::dbListTables(con)

# ============================================================
# MATCHES & DELIVERIES (split by match_type/gender/team_type)
# ============================================================
cli_h2("Exporting matches and deliveries (split)")

# Get all unique combinations
combos <- DBI::dbGetQuery(con, "
  SELECT DISTINCT match_type, gender, team_type
  FROM matches
  WHERE match_type IS NOT NULL
    AND gender IS NOT NULL
    AND team_type IS NOT NULL
  ORDER BY match_type, gender, team_type
")

cli_alert_info("Found {nrow(combos)} unique combinations")

for (i in seq_len(nrow(combos))) {
  mt <- combos$match_type[i]
  gen <- combos$gender[i]
  tt <- combos$team_type[i]

  # Create filename: matches_T20_male_international.parquet
  suffix <- paste(mt, gen, tt, sep = "_")

  # Export matches for this combination

  matches_query <- sprintf("
    SELECT * FROM matches
    WHERE match_type = '%s' AND gender = '%s' AND team_type = '%s'
  ", mt, gen, tt)

  matches_data <- DBI::dbGetQuery(con, matches_query)

  if (nrow(matches_data) > 0) {
    matches_path <- file.path(OUTPUT_DIR, paste0("matches_", suffix, ".parquet"))
    write_parquet(matches_data, matches_path, compression = "zstd")
    size_mb <- file.size(matches_path) / 1024 / 1024
    cli_alert_success("  matches_{suffix}: {format(nrow(matches_data), big.mark=',')} rows, {round(size_mb, 2)} MB")

    export_info[[paste0("matches_", suffix)]] <- list(
      name = paste0("matches_", suffix, ".parquet"),
      rows = nrow(matches_data),
      size_bytes = file.size(matches_path)
    )
  }

  # Export deliveries for this combination (join to get team_type from matches)
  # Note: deliveries already has gender, so only add team_type
  deliveries_query <- sprintf("
    SELECT d.*, m.team_type
    FROM deliveries d
    INNER JOIN matches m ON d.match_id = m.match_id
    WHERE m.match_type = '%s' AND m.gender = '%s' AND m.team_type = '%s'
  ", mt, gen, tt)

  deliveries_data <- DBI::dbGetQuery(con, deliveries_query)

  if (nrow(deliveries_data) > 0) {
    deliveries_path <- file.path(OUTPUT_DIR, paste0("deliveries_", suffix, ".parquet"))
    write_parquet(deliveries_data, deliveries_path, compression = "zstd")
    size_mb <- file.size(deliveries_path) / 1024 / 1024
    cli_alert_success("  deliveries_{suffix}: {format(nrow(deliveries_data), big.mark=',')} rows, {round(size_mb, 1)} MB")

    export_info[[paste0("deliveries_", suffix)]] <- list(
      name = paste0("deliveries_", suffix, ".parquet"),
      rows = nrow(deliveries_data),
      size_bytes = file.size(deliveries_path)
    )
  }
}

# ============================================================
# UNIFIED TABLES (players, team_elo)
# ============================================================
cli_h2("Exporting unified tables")

# Players (global registry)
export_info$players <- export_table(
  con, "players",
  file.path(OUTPUT_DIR, "players.parquet")
)

# Team ELO (per-match ratings, all formats)
if ("team_elo" %in% tables) {
  export_info$team_elo <- export_table(
    con, "team_elo",
    file.path(OUTPUT_DIR, "team_elo.parquet")
  )
}

# ============================================================
# SKILL INDEX TABLES
# ============================================================
cli_h2("Exporting skill index tables")

skill_tables <- c(
  "test_player_skill", "odi_player_skill", "t20_player_skill",
  "test_team_skill", "odi_team_skill", "t20_team_skill",
  "test_venue_skill", "odi_venue_skill", "t20_venue_skill"
)
export_info <- export_skill_tables(con, skill_tables, "", tables, export_info)

# ============================================================
# CREATE MANIFEST
# ============================================================
cli_h2("Creating manifest")
manifest <- create_manifest(Filter(Negate(is.null), export_info))

# ============================================================
# SUMMARY
# ============================================================
cli_h2("Export Complete")

parquet_files <- list.files(OUTPUT_DIR, pattern = "\\.parquet$")
cli_alert_info("Total files: {length(parquet_files)}")
cli_alert_info("Total rows: {format(manifest$total_rows, big.mark=',')}")
cli_alert_info("Total size: {round(manifest$total_size_bytes / 1024 / 1024, 1)} MB")

print_file_sizes(grep("^matches_", parquet_files, value = TRUE), "Matches (split by match_type/gender/team_type)")
print_file_sizes(grep("^deliveries_", parquet_files, value = TRUE), "Deliveries (split by match_type/gender/team_type)")
print_file_sizes(grep("^(players|team_elo)\\.parquet$", parquet_files, value = TRUE), "Unified Tables")
print_file_sizes(grep("_skill\\.parquet$", parquet_files, value = TRUE), "Skill Indices")

cli_alert_success("Parquet export complete! {length(parquet_files)} files created.")
