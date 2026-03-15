# build_player_lookup.R — Extract player_id → player_name mapping from balls data.
# The balls endpoint uses different IDs than innings, so we parse the "title" field
# which has "BowlerName to BatsmanName" per delivery.
# Run from bouncerdata root: Rscript scripts/build_player_lookup.R

library(arrow)
library(dplyr)

cat("Building player ID → name lookup from balls title field...\n")

formats <- c("t20i_male", "t20i_female", "odi_male", "odi_female",
             "test_male", "test_female")

all_players <- list()
for (fmt in formats) {
  f <- file.path("cricinfo", "combined", paste0("cricinfo_balls_", fmt, ".parquet"))
  if (!file.exists(f)) {
    cat("  Skipping", fmt, "(not found)\n")
    next
  }
  balls <- read_parquet(f, col_select = c("batsman_player_id", "bowler_player_id", "title"))

  # Parse title: "BowlerName to BatsmanName"
  parts <- strsplit(balls$title, " to ", fixed = TRUE)
  bowler_names <- sapply(parts, function(p) if (length(p) >= 2) p[1] else NA_character_)
  batsman_names <- sapply(parts, function(p) if (length(p) >= 2) p[2] else NA_character_)

  # Build batsman lookup
  bat_lookup <- data.frame(
    player_id = balls$batsman_player_id,
    player_name = batsman_names,
    stringsAsFactors = FALSE
  ) |> filter(!is.na(player_id), !is.na(player_name)) |> distinct()

  # Build bowler lookup
  bowl_lookup <- data.frame(
    player_id = balls$bowler_player_id,
    player_name = bowler_names,
    stringsAsFactors = FALSE
  ) |> filter(!is.na(player_id), !is.na(player_name)) |> distinct()

  combined <- bind_rows(bat_lookup, bowl_lookup) |> distinct()
  all_players[[fmt]] <- combined
  cat("  ", fmt, ":", nrow(combined), "unique player-ID pairs\n")
}

player_lookup <- bind_rows(all_players) |>
  distinct(player_id, player_name)

# Handle duplicates — keep most common name per ID
dups <- player_lookup |> group_by(player_id) |> filter(n() > 1) |> ungroup()
if (nrow(dups) > 0) {
  cat("  Resolving", length(unique(dups$player_id)), "IDs with multiple names...\n")
  # Count occurrences across all raw data
  all_raw <- bind_rows(all_players)
  player_lookup <- all_raw |>
    count(player_id, player_name) |>
    group_by(player_id) |>
    slice_max(n, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(player_id, player_name) |>
    arrange(player_id)
}

dir.create("blog", showWarnings = FALSE)
write_parquet(player_lookup, "blog/player-names.parquet")
cat("player-names:", nrow(player_lookup), "players\n")
