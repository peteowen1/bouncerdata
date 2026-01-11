# export_parquets.R
# Export DuckDB tables to Parquet files

library(duckdb)
library(arrow)
library(jsonlite)
library(cli)

# Configuration
DB_PATH <- "bouncer.duckdb"
OUTPUT_DIR <- "parquet_output"

# Format classification for splitting deliveries
FORMAT_LONG_FORM <- c("Test", "MDM")

#' Export a table to Parquet
export_table <- function(con, table_name, output_path, where_clause = NULL) {
  query <- sprintf("SELECT * FROM %s", table_name)
  if (!is.null(where_clause)) {
    query <- paste(query, "WHERE", where_clause)
  }

  cli_alert_info("Exporting {basename(output_path)}...")

  # Read data
  data <- DBI::dbGetQuery(con, query)

  if (nrow(data) == 0) {
    cli_alert_warning("  No data to export for {table_name}")
    return(NULL)
  }

  # Write to Parquet
  write_parquet(data, output_path, compression = "zstd")

  size_mb <- file.size(output_path) / 1024 / 1024
  cli_alert_success("  {basename(output_path)}: {nrow(data)} rows, {round(size_mb, 1)} MB")

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

# Main execution
cli_h1("Exporting Tables to Parquet")

# Create output directory
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# Connect to database
if (!file.exists(DB_PATH)) {
  cli_abort("Database not found: {DB_PATH}")
}

con <- dbConnect(duckdb(), DB_PATH, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE))

export_info <- list()

# 1. Export matches table
export_info$matches <- export_table(
  con, "matches",
  file.path(OUTPUT_DIR, "matches.parquet")
)

# 2. Export players table
export_info$players <- export_table(
  con, "players",
  file.path(OUTPUT_DIR, "players.parquet")
)

# 3. Export deliveries split by format
cli_h2("Exporting deliveries (split by format)")

# Long form deliveries
export_info$deliveries_long_form <- export_table(
  con, "deliveries",
  file.path(OUTPUT_DIR, "deliveries_long_form.parquet"),
  where_clause = sprintf("match_type IN ('%s')", paste(FORMAT_LONG_FORM, collapse = "', '"))
)

# Short form deliveries
export_info$deliveries_short_form <- export_table(
  con, "deliveries",
  file.path(OUTPUT_DIR, "deliveries_short_form.parquet"),
  where_clause = sprintf("match_type NOT IN ('%s')", paste(FORMAT_LONG_FORM, collapse = "', '"))
)

# 4. Check for skill index tables (if they exist)
cli_h2("Checking for skill index tables")

tables <- DBI::dbListTables(con)

skill_tables <- c(
  "test_player_skill", "odi_player_skill", "t20_player_skill",
  "test_team_skill", "odi_team_skill", "t20_team_skill",
  "test_venue_skill", "odi_venue_skill", "t20_venue_skill",
  "team_elo"
)

for (tbl in skill_tables) {
  if (tbl %in% tables) {
    export_info[[tbl]] <- export_table(
      con, tbl,
      file.path(OUTPUT_DIR, paste0(tbl, ".parquet"))
    )
  } else {
    cli_alert_info("Table {tbl} not found (skipping)")
  }
}

# Create manifest
cli_h2("Creating manifest")
manifest <- create_manifest(Filter(Negate(is.null), export_info))

# Summary
cli_h2("Export Complete")
cli_alert_info("Total files: {length(list.files(OUTPUT_DIR, pattern = '\\.parquet$'))}")
cli_alert_info("Total rows: {manifest$total_rows}")
cli_alert_info("Total size: {round(manifest$total_size_bytes / 1024 / 1024, 1)} MB")

# List files
cli_h3("Parquet Files")
for (f in list.files(OUTPUT_DIR)) {
  size <- file.size(file.path(OUTPUT_DIR, f))
  cli_alert_info("{f}: {round(size / 1024 / 1024, 2)} MB")
}

cli_alert_success("Parquet export complete!")
