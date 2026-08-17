library(arrow)
library(dplyr)

dir.create("blog", showWarnings = FALSE)

min_balls_batting  <- c(t20 = 100, odi = 300, test = 500)
min_balls_bowling  <- c(t20 = 100, odi = 300, test = 500)

# Load player metadata (cricsheet names + cricinfo enrichment)
players_path <- "source/players.parquet"
details_path <- "source/bouncer_player_details.parquet"

if (file.exists(players_path)) {
  players <- read_parquet(players_path) |>
    select(player_id, player_name)
  cat(sprintf("Loaded player registry: %d players\n", nrow(players)))
} else {
  warning("players.parquet not found — player names will be IDs")
  players <- NULL
}

if (file.exists(details_path)) {
  details <- read_parquet(details_path) |>
    select(player_id, country, full_name, dob, batting_style, bowling_style)
  cat(sprintf("Loaded player details: %d players (%d enriched)\n",
              nrow(details), sum(!is.na(details$full_name))))
} else {
  warning("bouncer_player_details.parquet not found — no enrichment")
  details <- NULL
}

# Build player metadata lookup (join players + details)
if (!is.null(players)) {
  player_meta <- players
  if (!is.null(details)) {
    player_meta <- player_meta |>
      left_join(details, by = "player_id")
  }
} else {
  player_meta <- NULL
}

for (fmt in c("t20", "odi", "test")) {
  cat(sprintf("Processing %s...\n", toupper(fmt)))

  # Player skill (batting + bowling from same file)
  ps_path <- sprintf("source/%s_player_skill.parquet", fmt)
  if (!file.exists(ps_path)) stop("Missing source file: ", ps_path)
  ps <- read_parquet(ps_path)
  required_cols <- c("batter_id", "batter_balls_faced", "batter_scoring_index",
                     "batter_survival_rate", "bowler_id", "bowler_balls_bowled",
                     "bowler_economy_index", "bowler_strike_rate")
  missing <- setdiff(required_cols, names(ps))
  if (length(missing)) stop("Missing columns in ", ps_path, ": ", paste(missing, collapse = ", "))

  batting <- ps |>
    group_by(batter_id) |>
    slice_max(batter_balls_faced, n = 1, with_ties = FALSE) |>
    ungroup() |>
    filter(batter_balls_faced >= min_balls_batting[fmt]) |>
    select(player_id = batter_id, scoring_index = batter_scoring_index,
           survival_rate = batter_survival_rate, balls_faced = batter_balls_faced)

  if (!is.null(player_meta)) {
    batting <- batting |>
      left_join(player_meta, by = "player_id") |>
      mutate(player_name = coalesce(player_name, player_id)) |>
      select(player = player_name, country, full_name, dob, batting_style,
             scoring_index, survival_rate, balls_faced)
  } else {
    batting <- batting |> rename(player = player_id)
  }

  batting <- batting |> arrange(desc(scoring_index))
  write_parquet(batting, sprintf("blog/%s-batting.parquet", fmt))
  cat(sprintf("  %s batting: %d players\n", fmt, nrow(batting)))

  bowling <- ps |>
    group_by(bowler_id) |>
    slice_max(bowler_balls_bowled, n = 1, with_ties = FALSE) |>
    ungroup() |>
    filter(bowler_balls_bowled >= min_balls_bowling[fmt]) |>
    select(player_id = bowler_id, economy_index = bowler_economy_index,
           strike_rate = bowler_strike_rate, balls_bowled = bowler_balls_bowled)

  if (!is.null(player_meta)) {
    bowling <- bowling |>
      left_join(player_meta, by = "player_id") |>
      mutate(player_name = coalesce(player_name, player_id)) |>
      select(player = player_name, country, full_name, dob, bowling_style,
             economy_index, strike_rate, balls_bowled)
  } else {
    bowling <- bowling |> rename(player = player_id)
  }

  bowling <- bowling |> arrange(economy_index)
  write_parquet(bowling, sprintf("blog/%s-bowling.parquet", fmt))
  cat(sprintf("  %s bowling: %d players\n", fmt, nrow(bowling)))

  rm(ps); gc()

  # Team skill
  ts <- read_parquet(sprintf("source/%s_team_skill.parquet", fmt))
  teams <- ts |>
    group_by(batting_team_id) |>
    slice_max(batting_team_balls, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(team = batting_team_id, batting_runs_skill = batting_team_runs_skill,
           batting_wicket_skill = batting_team_wicket_skill,
           bowling_runs_skill = bowling_team_runs_skill,
           bowling_wicket_skill = bowling_team_wicket_skill,
           balls = batting_team_balls) |>
    arrange(desc(batting_runs_skill))
  write_parquet(teams, sprintf("blog/%s-teams.parquet", fmt))
  cat(sprintf("  %s teams: %d teams\n", fmt, nrow(teams)))
  rm(ts); gc()

  # Venue skill
  vs <- read_parquet(sprintf("source/%s_venue_skill.parquet", fmt))
  venues <- vs |>
    group_by(venue) |>
    slice_max(venue_balls, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(venue, run_rate = venue_run_rate, wicket_rate = venue_wicket_rate,
           boundary_rate = venue_boundary_rate, dot_rate = venue_dot_rate,
           balls = venue_balls) |>
    arrange(desc(run_rate))
  write_parquet(venues, sprintf("blog/%s-venues.parquet", fmt))
  cat(sprintf("  %s venues: %d venues\n", fmt, nrow(venues)))
  rm(vs); gc()
}

# Build balls parquets (ball-by-ball data for match pages)
# Also builds player-names.parquet lookup from title field
balls_cols <- c("match_id", "title", "innings_number", "over_number", "ball_number",
                "overs_actual", "total_runs", "batsman_runs", "is_four", "is_six",
                "is_wicket", "dismissal_type", "dismissal_text", "wides", "noballs",
                "wagon_x", "wagon_y", "batsman_player_id", "bowler_player_id",
                "total_innings_runs", "total_innings_wickets", "predicted_score",
                "win_probability")

all_player_pairs <- list()

for (fmt in c("t20i", "odi", "test")) {
  tryCatch({
    balls_path <- sprintf("cricinfo/combined/cricinfo_balls_%s_male.parquet", fmt)
    if (!file.exists(balls_path)) {
      cat(sprintf("Skipping %s balls — file not found: %s\n", fmt, balls_path))
      next
    }

    balls <- read_parquet(balls_path)
    available <- intersect(balls_cols, names(balls))
    missing <- setdiff(balls_cols, names(balls))
    if (length(missing)) {
      cat(sprintf("  %s balls: missing columns (will be NA): %s\n", fmt, paste(missing, collapse = ", ")))
    }

    # Collect player ID→name pairs from title field ("BowlerName to BatsmanName")
    if ("title" %in% names(balls) && "batsman_player_id" %in% names(balls)) {
      parts <- strsplit(balls$title, " to ", fixed = TRUE)
      bowler_names <- vapply(parts, function(p) if (length(p) >= 2) p[1] else NA_character_, character(1))
      batsman_names <- vapply(parts, function(p) if (length(p) >= 2) p[2] else NA_character_, character(1))
      all_player_pairs[[paste0(fmt, "_bat")]] <- data.frame(
        player_id = balls$batsman_player_id, player_name = batsman_names, stringsAsFactors = FALSE
      ) |> filter(!is.na(player_id), !is.na(player_name)) |> distinct()
      all_player_pairs[[paste0(fmt, "_bowl")]] <- data.frame(
        player_id = balls$bowler_player_id, player_name = bowler_names, stringsAsFactors = FALSE
      ) |> filter(!is.na(player_id), !is.na(player_name)) |> distinct()
    }

    balls <- balls |> select(all_of(available))
    out_path <- sprintf("blog/balls-%s.parquet", fmt)
    write_parquet(balls, out_path)
    cat(sprintf("  %s balls: %d rows, %d cols -> %s\n", fmt, nrow(balls), ncol(balls), out_path))
    rm(balls); gc()
  }, error = function(e) {
    warning(sprintf("Failed to build %s balls parquet: %s", fmt, conditionMessage(e)))
  })
}

# Build player-names.parquet from collected title pairs
tryCatch({
  player_lookup <- bind_rows(all_player_pairs) |>
    count(player_id, player_name) |>
    group_by(player_id) |>
    slice_max(n, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(player_id, player_name) |>
    arrange(player_id)
  write_parquet(player_lookup, "blog/player-names.parquet")
  cat(sprintf("  player-names: %d players\n", nrow(player_lookup)))
}, error = function(e) {
  warning("Failed to build player-names.parquet: ", conditionMessage(e))
})

# ---------------------------------------------------------------------------
# Player Rating v2 (bouncerverse D-P16 to D-P29)
#
# The opponent- and competition-adjusted rating, covering men's and women's
# T20 and ODI. Published straight through rather than recomputed: the parquets
# on `player-rating-v2` are already validated at publish time (bucket
# completeness, rank 1..N, no duplicate player per bucket, anchor players
# where they belong), so re-deriving anything here would only add a second
# place for it to go wrong.
#
# Unlike the skill tables above these already carry `player_name`, so they do
# NOT depend on the registry join. They also carry the two columns needed to
# read a rating honestly:
#   average             the traditional number, for orientation
#   effective_matches   how much evidence is behind it after decay -- a rating
#                       whose effective_matches is below its bucket's prior is
#                       mostly population mean, and the page should be able to
#                       say so rather than hide it behind a filter.
rating_v2_path <- "source/player_rating_v2.parquet"
value_v2_path  <- "source/player_value_v2.parquet"

if (file.exists(rating_v2_path)) {
  rv <- read_parquet(rating_v2_path)

  # Guard the shape rather than trusting it: this file is produced by a
  # different repo on a different schedule, and a silently-renamed column
  # would publish an empty table on a green run.
  need <- c("format", "gender", "role", "rank", "player_id", "player_name",
            "rating", "average", "main_comp", "matches", "balls",
            "effective_matches")
  missing <- setdiff(need, names(rv))
  if (length(missing)) {
    stop("player_rating_v2.parquet is missing column(s): ",
         paste(missing, collapse = ", "),
         "\n  Produced by bouncer's 01_build_player_ratings_v2.R; check that ",
         "the release asset matches the current schema.")
  }
  if (nrow(rv) == 0L) stop("player_rating_v2.parquet is empty.")

  # player_id is carried even though nothing reads it yet, because `player` is
  # NOT unique and the front-end links by it. Two bucket-roles ship two rows
  # with the same name, and in both cases they are genuinely two different
  # people that D-P28 deliberately refused to merge:
  #   odi-female batter  E Jones        ids 971cb321 and "E Jones"
  #   t20-male   bowler  Harmeet Singh  ids 0bf15e52 and 2a72fd4f
  # Without the id the page shows a name twice and points both rows at one
  # player. This is the find_player() lesson at the presentation layer: a name
  # is not an identifier, and the place that discovers it is always downstream.
  rv <- rv |>
    mutate(bucket = paste0(tolower(format), "-", gender)) |>
    select(bucket, role, rank, player_id, player = player_name, rating, average,
           main_comp, matches, balls, effective_matches, last_match, as_at) |>
    arrange(bucket, role, rank)
  stopifnot(!anyNA(rv$player_id))
  write_parquet(rv, "blog/player-ratings-v2.parquet")
  cat(sprintf("  ratings v2: %d rows across %d bucket-roles (%d names shared by 2+ ids)\n",
              nrow(rv), dplyr::n_distinct(rv$bucket, rv$role),
              sum(rv |> count(bucket, role, player) |> pull(n) > 1)))

  if (file.exists(value_v2_path)) {
    # player_id for the same reason as the rating table above: two names are
    # shared by two different players here too.
    vv <- read_parquet(value_v2_path) |>
      mutate(bucket = paste0(tolower(format), "-", gender)) |>
      select(bucket, rank, player_id, player = player_name, total_value,
             bat_value, bowl_value, matches, bat_balls, bowl_balls,
             calibrated, as_at) |>
      arrange(bucket, rank)
    stopifnot(!anyNA(vv$player_id))
    write_parquet(vv, "blog/player-values-v2.parquet")
    cat(sprintf("  values v2:  %d rows across %d buckets\n",
                nrow(vv), dplyr::n_distinct(vv$bucket)))
  } else {
    cat("  values v2:  absent, skipping (ratings still published)\n")
  }
} else {
  # Not fatal: the v2 release is newer than this script's other inputs, so an
  # older bouncerdata checkout can legitimately lack it. Loud, though -- a
  # missing rating file must not read as "there are no ratings".
  cat("  ratings v2: source/player_rating_v2.parquet ABSENT -- v2 tables not published\n")
}
