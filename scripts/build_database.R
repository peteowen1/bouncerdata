# build_database.R
# Build DuckDB database from JSON files
# This is a simplified version for GitHub Actions
# For full features, use bouncer::install_all_bouncer_data()

library(duckdb)
library(jsonlite)
library(cli)

# Configuration
JSON_DIR <- "json_temp/json_files"
DB_PATH <- "bouncer.duckdb"
BATCH_SIZE <- 500

#' Parse a single Cricsheet JSON file (simplified)
parse_json_simple <- function(file_path) {
  json <- fromJSON(file_path, simplifyVector = FALSE)
  match_id <- tools::file_path_sans_ext(basename(file_path))
  info <- json$info

  # Match info
  teams <- if (is.list(info$teams)) unlist(info$teams) else info$teams
  dates <- if (is.list(info$dates)) unlist(info$dates) else info$dates

  match_info <- data.frame(
    match_id = match_id,
    match_type = info$match_type %||% NA_character_,
    match_date = if (length(dates) > 0) as.Date(dates[1]) else NA,
    venue = info$venue %||% NA_character_,
    city = info$city %||% NA_character_,
    gender = info$gender %||% "male",
    team1 = if (length(teams) >= 1) teams[1] else NA_character_,
    team2 = if (length(teams) >= 2) teams[2] else NA_character_,
    season = as.character(info$season %||% NA),
    stringsAsFactors = FALSE
  )

  # Deliveries (simplified - just core fields)
  deliveries <- list()
  del_idx <- 0

  innings <- json$innings
  if (!is.null(innings)) {
    for (inn_num in seq_along(innings)) {
      inning <- innings[[inn_num]]
      batting_team <- inning$team %||% NA_character_

      overs <- inning$overs
      if (!is.null(overs)) {
        for (over_data in overs) {
          over_num <- over_data$over %||% 0L
          balls <- over_data$deliveries

          if (!is.null(balls)) {
            for (ball_num in seq_along(balls)) {
              ball <- balls[[ball_num]]
              del_idx <- del_idx + 1

              runs <- ball$runs
              is_wicket <- !is.null(ball$wickets) && length(ball$wickets) > 0

              deliveries[[del_idx]] <- data.frame(
                delivery_id = sprintf("%s_%d_%03d_%02d", match_id, inn_num, over_num, ball_num),
                match_id = match_id,
                match_type = info$match_type %||% NA_character_,
                match_date = match_info$match_date,
                gender = info$gender %||% "male",
                batting_team = batting_team,
                innings = inn_num,
                over = over_num,
                ball = ball_num,
                batter_id = ball$batter %||% NA_character_,
                bowler_id = ball$bowler %||% NA_character_,
                runs_batter = as.integer(runs$batter %||% 0L),
                runs_total = as.integer(runs$total %||% 0L),
                is_wicket = is_wicket,
                stringsAsFactors = FALSE
              )
            }
          }
        }
      }
    }
  }

  deliveries_df <- if (length(deliveries) > 0) {
    do.call(rbind, deliveries)
  } else {
    data.frame()
  }

  # Players
  players_data <- info$players
  players <- list()
  if (!is.null(players_data)) {
    for (team_name in names(players_data)) {
      for (p in players_data[[team_name]]) {
        players[[length(players) + 1]] <- data.frame(
          player_id = p,
          player_name = p,
          country = team_name,
          stringsAsFactors = FALSE
        )
      }
    }
  }

  players_df <- if (length(players) > 0) {
    unique(do.call(rbind, players))
  } else {
    data.frame()
  }

  list(
    match_info = match_info,
    deliveries = deliveries_df,
    players = players_df
  )
}

#' Initialize database schema
init_database <- function(con) {
  cli_alert_info("Creating database schema...")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS matches (
      match_id VARCHAR PRIMARY KEY,
      match_type VARCHAR,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,
      team1 VARCHAR,
      team2 VARCHAR,
      season VARCHAR
    )
  ")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS deliveries (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      gender VARCHAR,
      batting_team VARCHAR,
      innings INTEGER,
      over INTEGER,
      ball INTEGER,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      runs_batter INTEGER,
      runs_total INTEGER,
      is_wicket BOOLEAN
    )
  ")

  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS players (
      player_id VARCHAR PRIMARY KEY,
      player_name VARCHAR,
      country VARCHAR
    )
  ")

  cli_alert_success("Schema created")
}

# Main execution
cli_h1("Building DuckDB Database")

# Find all JSON files
json_files <- list.files(JSON_DIR, pattern = "\\.json$",
                          full.names = TRUE, recursive = TRUE)

cli_alert_info("Found {length(json_files)} JSON files")

# Initialize database
if (file.exists(DB_PATH)) {
  file.remove(DB_PATH)
}

con <- dbConnect(duckdb(), DB_PATH)
on.exit(dbDisconnect(con, shutdown = TRUE))

init_database(con)

# Process in batches
n_batches <- ceiling(length(json_files) / BATCH_SIZE)
cli_alert_info("Processing in {n_batches} batches of {BATCH_SIZE}")

success_count <- 0L
error_count <- 0L

for (batch_num in seq_len(n_batches)) {
  start_idx <- (batch_num - 1) * BATCH_SIZE + 1
  end_idx <- min(batch_num * BATCH_SIZE, length(json_files))
  batch_files <- json_files[start_idx:end_idx]

  cli_alert_info("Batch {batch_num}/{n_batches} ({length(batch_files)} files)")

  batch_matches <- list()
  batch_deliveries <- list()
  batch_players <- list()

  for (f in batch_files) {
    tryCatch({
      parsed <- parse_json_simple(f)
      batch_matches[[length(batch_matches) + 1]] <- parsed$match_info
      if (nrow(parsed$deliveries) > 0) {
        batch_deliveries[[length(batch_deliveries) + 1]] <- parsed$deliveries
      }
      if (nrow(parsed$players) > 0) {
        batch_players[[length(batch_players) + 1]] <- parsed$players
      }
      success_count <- success_count + 1L
    }, error = function(e) {
      error_count <<- error_count + 1L
    })
  }

  # Combine and insert
  if (length(batch_matches) > 0) {
    all_matches <- do.call(rbind, batch_matches)
    all_matches <- all_matches[!duplicated(all_matches$match_id), ]
    DBI::dbAppendTable(con, "matches", all_matches)
  }

  if (length(batch_deliveries) > 0) {
    all_deliveries <- do.call(rbind, batch_deliveries)
    all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
    DBI::dbAppendTable(con, "deliveries", all_deliveries)
  }

  if (length(batch_players) > 0) {
    all_players <- do.call(rbind, batch_players)
    all_players <- all_players[!duplicated(all_players$player_id), ]
    # Use INSERT OR IGNORE for players
    DBI::dbExecute(con, "INSERT OR IGNORE INTO players SELECT * FROM all_players",
                   list(all_players))
  }
}

# Summary
cli_h2("Database Build Complete")
cli_alert_success("Processed: {success_count} matches")
if (error_count > 0) {
  cli_alert_warning("Errors: {error_count}")
}

# Show counts
n_matches <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM matches")$n
n_deliveries <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM deliveries")$n
n_players <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM players")$n

cli_alert_info("Matches: {n_matches}")
cli_alert_info("Deliveries: {n_deliveries}")
cli_alert_info("Players: {n_players}")

db_size <- file.size(DB_PATH) / 1024 / 1024
cli_alert_info("Database size: {round(db_size, 1)} MB")

cli_alert_success("Database ready: {DB_PATH}")
