library(arrow)
library(dplyr)

dir.create("blog", showWarnings = FALSE)

min_balls_batting  <- c(t20 = 100, odi = 300, test = 500)
min_balls_bowling  <- c(t20 = 100, odi = 300, test = 500)

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
    select(player = batter_id, scoring_index = batter_scoring_index,
           survival_rate = batter_survival_rate, balls_faced = batter_balls_faced) |>
    arrange(desc(scoring_index))
  write_parquet(batting, sprintf("blog/%s_batting.parquet", fmt))
  cat(sprintf("  %s batting: %d players\n", fmt, nrow(batting)))

  bowling <- ps |>
    group_by(bowler_id) |>
    slice_max(bowler_balls_bowled, n = 1, with_ties = FALSE) |>
    ungroup() |>
    filter(bowler_balls_bowled >= min_balls_bowling[fmt]) |>
    select(player = bowler_id, economy_index = bowler_economy_index,
           strike_rate = bowler_strike_rate, balls_bowled = bowler_balls_bowled) |>
    arrange(economy_index)
  write_parquet(bowling, sprintf("blog/%s_bowling.parquet", fmt))
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
  write_parquet(teams, sprintf("blog/%s_teams.parquet", fmt))
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
  write_parquet(venues, sprintf("blog/%s_venues.parquet", fmt))
  cat(sprintf("  %s venues: %d venues\n", fmt, nrow(venues)))
  rm(vs); gc()
}
