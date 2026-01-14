# export_parquets.R
# Export DuckDB tables to Parquet files
#
# Exports unified tables:
#   - matches.parquet (all matches)
#   - deliveries.parquet (all ball-by-ball data)
#   - players.parquet, team_elo.parquet, skill indices

library(duckdb)
library(arrow)
library(jsonlite)
library(cli)

# Configuration
DB_PATH <- "bouncer.duckdb"
OUTPUT_DIR <- "parquet_output"

#' Export a table to Parquet
#' @param con DuckDB connection
#' @param table_name Table name in database
#' @param output_path Output file path
export_table <- function(con, table_name, output_path) {
  query <- sprintf("SELECT * FROM %s", table_name)

  cli_alert_info("Exporting {basename(output_path)}...")

  # Read data
  data <- DBI::dbGetQuery(con, query)

  if (nrow(data) == 0) {
    cli_alert_warning("  No data to export for {basename(output_path)}")
    return(NULL)
  }

  # Write to Parquet with zstd compression
  write_parquet(data, output_path, compression = "zstd")

  size_mb <- file.size(output_path) / 1024 / 1024
  cli_alert_success("  {basename(output_path)}: {format(nrow(data), big.mark=',')} rows, {round(size_mb, 1)} MB")

  list(
    name = basename(output_path),
    rows = nrow(data),
    size_bytes = file.size(output_path)
  )
}

#' Create manifest for parquet files
create_manifest <- function(export_info) {
  manifest <- list(
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    files = export_info
  )

  manifest$total_rows <- sum(sapply(export_info, function(x) x$rows %||% 0))
  manifest$total_size_bytes <- sum(sapply(export_info, function(x) x$size_bytes %||% 0))

  manifest_path <- file.path(OUTPUT_DIR, "manifest.json")
  write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE)

  cli_alert_success("Created manifest.json")
  manifest
}

# Null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Main execution
cli_h1("Exporting Tables to Parquet (Unified)")

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
# CORE TABLES (unified - no splits)
# ============================================================
cli_h2("Exporting core tables")

# Matches (all matches in one file)
export_info$matches <- export_table(
  con, "matches",
  file.path(OUTPUT_DIR, "matches.parquet")
)

# Deliveries (all ball-by-ball data in one file)
# Join with matches to include team_type for filtering
cli_alert_info("Exporting deliveries.parquet...")
deliveries_query <- "
  SELECT d.*, m.team_type
  FROM deliveries d
  LEFT JOIN matches m ON d.match_id = m.match_id
"
deliveries_data <- DBI::dbGetQuery(con, deliveries_query)

if (nrow(deliveries_data) > 0) {
  deliveries_path <- file.path(OUTPUT_DIR, "deliveries.parquet")
  write_parquet(deliveries_data, deliveries_path, compression = "zstd")
  size_mb <- file.size(deliveries_path) / 1024 / 1024
  cli_alert_success("  deliveries.parquet: {format(nrow(deliveries_data), big.mark=',')} rows, {round(size_mb, 1)} MB")
  export_info$deliveries <- list(
    name = "deliveries.parquet",
    rows = nrow(deliveries_data),
    size_bytes = file.size(deliveries_path)
  )
} else {
  cli_alert_warning("  No deliveries data to export")
  export_info$deliveries <- NULL
}

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
# SKILL INDEX TABLES (separate directories for separate releases)
# ============================================================
cli_h2("Exporting skill index tables")

# Create subdirectories for each rating type
rating_dirs <- c("player_rating", "team_rating", "venue_rating")
for (rating_dir in rating_dirs) {
  dir.create(file.path(OUTPUT_DIR, rating_dir), showWarnings = FALSE, recursive = TRUE)
}

# Player skill tables -> player_rating
player_skill_tables <- c("test_player_skill", "odi_player_skill", "t20_player_skill")
for (tbl in player_skill_tables) {
  if (tbl %in% tables) {
    export_info[[tbl]] <- export_table(
      con, tbl,
      file.path(OUTPUT_DIR, "player_rating", paste0(tbl, ".parquet"))
    )
  }
}

# Team skill tables -> team_rating
team_skill_tables <- c("test_team_skill", "odi_team_skill", "t20_team_skill")
for (tbl in team_skill_tables) {
  if (tbl %in% tables) {
    export_info[[tbl]] <- export_table(
      con, tbl,
      file.path(OUTPUT_DIR, "team_rating", paste0(tbl, ".parquet"))
    )
  }
}

# Venue skill tables -> venue_rating
venue_skill_tables <- c("test_venue_skill", "odi_venue_skill", "t20_venue_skill")
for (tbl in venue_skill_tables) {
  if (tbl %in% tables) {
    export_info[[tbl]] <- export_table(
      con, tbl,
      file.path(OUTPUT_DIR, "venue_rating", paste0(tbl, ".parquet"))
    )
  }
}

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

# List files by category
cli_h3("Core Tables")
for (f in grep("^(matches|deliveries|players|team_elo)\\.parquet$", parquet_files, value = TRUE)) {
  size <- file.size(file.path(OUTPUT_DIR, f))
  cli_alert_info("  {f}: {round(size / 1024 / 1024, 2)} MB")
}

cli_h3("Skill Indices (9 tables)")
for (f in grep("_skill\\.parquet$", parquet_files, value = TRUE)) {
  size <- file.size(file.path(OUTPUT_DIR, f))
  cli_alert_info("  {f}: {round(size / 1024 / 1024, 2)} MB")
}

cli_alert_success("Parquet export complete! {length(parquet_files)} files created.")
