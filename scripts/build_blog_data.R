library(arrow)
library(dplyr)

dir.create("blog", showWarnings = FALSE)

# Qualification thresholds. Batting and bowling share the same values today;
# kept as one vector so they cannot drift apart silently.
min_balls_qualifying <- c(t20 = 100, odi = 300, test = 500)

# Load player metadata (cricsheet names + cricinfo enrichment)
players_path <- "source/players.parquet"
details_path <- "source/bouncer_player_details.parquet"

# The player registry is REQUIRED, not best-effort.
#
# Its download step in build-blog-data.yml is continue-on-error, so a
# transient GitHub Releases failure leaves this file absent. Warning and
# carrying on published a live blog whose player column held raw UUIDs
# instead of names, with a green workflow run and no signal that anything
# had gone wrong. A missing registry now stops the build before the R2
# upload step can run.
if (!file.exists(players_path)) {
  stop("Required source file missing: ", players_path, "\n",
       "  The blog tables key on player names; without the registry every ",
       "row would publish a raw player_id.\n",
       "  Re-run the 'Download player metadata' step, or check the ",
       "cricsheet release.")
}
players <- read_parquet(players_path) |>
  select(player_id, player_name)
cat(sprintf("Loaded player registry: %d players\n", nrow(players)))
if (nrow(players) == 0) {
  stop("Player registry is empty: ", players_path)
}

if (file.exists(details_path)) {
  details <- tryCatch({
    d <- read_parquet(details_path) |>
      select(player_id, country, full_name, dob, batting_style, bowling_style)
    cat(sprintf("Loaded player details: %d players (%d enriched)\n",
                nrow(d), sum(!is.na(d$full_name))))
    d
  }, error = function(e) {
    warning("bouncer_player_details.parquet exists but failed to read (",
            conditionMessage(e), ") — no enrichment")
    NULL
  })
} else {
  warning("bouncer_player_details.parquet not found — no enrichment")
  details <- NULL
}

# Build player metadata lookup (join players + details). `details` stays
# optional -- it only adds country/dob/style columns, so its absence degrades
# the page rather than corrupting it. That degrade only actually holds if
# player_meta always has those columns to select downstream -- caught by
# review (#66): a NULL `details` (missing OR unreadable) used to leave
# player_meta without country/full_name/dob/batting_style/bowling_style, and
# the batting/bowling select() blocks below reference those names
# unconditionally, so the "degrades gracefully" claim was false -- it still
# crashed a few lines later with a confusing "column doesn't exist" error.
# Pad with NA columns here, once, so every downstream reference is safe.
detail_cols <- c("country", "full_name", "dob", "batting_style", "bowling_style")
player_meta <- players
if (!is.null(details)) {
  player_meta <- player_meta |>
    left_join(details, by = "player_id")
} else {
  player_meta[detail_cols] <- NA_character_
}

# The registry existing is not the same as it COVERING the leaderboard. A
# player absent from it still falls back to a raw ID via coalesce(), so a
# stale registry publishes UUIDs for exactly the newest players. Fail if more
# than a small fraction of a table is unnamed.
max_unnamed_frac <- 0.02

report_unnamed <- function(tbl, ids, label) {
  unnamed <- sum(!(tbl$player %in% player_meta$player_name))
  frac <- if (nrow(tbl) > 0) unnamed / nrow(tbl) else 0
  if (unnamed == 0) return(invisible(NULL))
  msg <- sprintf("%s: %d of %d rows (%.1f%%) have no name in the registry",
                 label, unnamed, nrow(tbl), 100 * frac)
  if (frac > max_unnamed_frac) {
    stop(msg, "\n  Refusing to publish raw player IDs. Check that ",
         "players.parquet is current.")
  }
  cat(sprintf("  WARNING: %s\n", msg))
  summary_file <- Sys.getenv("GITHUB_STEP_SUMMARY")
  if (nzchar(summary_file)) {
    cat(sprintf("- **WARNING**: %s\n", msg), file = summary_file, append = TRUE)
  }
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
    filter(batter_balls_faced >= min_balls_qualifying[fmt]) |>
    select(player_id = batter_id, scoring_index = batter_scoring_index,
           survival_rate = batter_survival_rate, balls_faced = batter_balls_faced)

  batting <- batting |>
    left_join(player_meta, by = "player_id") |>
    mutate(player_name = coalesce(player_name, player_id)) |>
    select(player = player_name, country, full_name, dob, batting_style,
           scoring_index, survival_rate, balls_faced)
  report_unnamed(batting, ps$batter_id, sprintf("%s batting", fmt))

  batting <- batting |> arrange(desc(scoring_index))
  write_parquet(batting, sprintf("blog/%s-batting.parquet", fmt))
  cat(sprintf("  %s batting: %d players\n", fmt, nrow(batting)))

  bowling <- ps |>
    group_by(bowler_id) |>
    slice_max(bowler_balls_bowled, n = 1, with_ties = FALSE) |>
    ungroup() |>
    filter(bowler_balls_bowled >= min_balls_qualifying[fmt]) |>
    select(player_id = bowler_id, economy_index = bowler_economy_index,
           strike_rate = bowler_strike_rate, balls_bowled = bowler_balls_bowled)

  bowling <- bowling |>
    left_join(player_meta, by = "player_id") |>
    mutate(player_name = coalesce(player_name, player_id)) |>
    select(player = player_name, country, full_name, dob, bowling_style,
           economy_index, strike_rate, balls_bowled)
  report_unnamed(bowling, ps$bowler_id, sprintf("%s bowling", fmt))

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
  # Every column the select() below reads must be named here, or the guard is
  # decorative: a column renamed upstream then fails inside dplyr::select() with
  # "can't subset columns that don't exist" and no mention of which repo
  # produced the file. last_match and as_at were selected but not asserted.
  need <- c("format", "gender", "role", "rank", "player_id", "player_name",
            "rating", "average", "main_comp", "matches", "balls",
            "effective_matches", "last_match", "as_at")
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

  # Enrich from the same id-keyed crosswalk the skill tables above use, so the
  # page can filter by country and show style badges and age without matching
  # on names. The join is on player_id and never on the name -- which matters
  # here more than usual, because the ids that ARE a bare name (D-P28's
  # deliberately unmerged split careers) simply fail to match and come back
  # NA. That is the honest outcome; a name join would instead attach one real
  # player's country and date of birth to a different player's rating.
  if (!is.null(player_meta) && "country" %in% names(player_meta)) {
    before <- nrow(rv)
    rv <- rv |>
      left_join(
        player_meta |>
          select(player_id, country, full_name, dob, batting_style, bowling_style) |>
          distinct(player_id, .keep_all = TRUE),
        by = "player_id")
    # A crosswalk with a duplicated player_id would silently multiply rows and
    # break rank 1..N. distinct() above prevents it; this proves it.
    stopifnot(nrow(rv) == before)
    cat(sprintf("  ratings v2: %d/%d rows enriched with country\n",
                sum(!is.na(rv$country)), nrow(rv)))
  } else {
    cat("  ratings v2: no player_meta available -- publishing without country/style/age\n")
  }

  write_parquet(rv, "blog/player-ratings-v2.parquet")
  cat(sprintf("  ratings v2: %d rows across %d bucket-roles (%d names shared by 2+ ids)\n",
              nrow(rv), dplyr::n_distinct(rv$bucket, rv$role),
              sum(rv |> count(bucket, role, player) |> pull(n) > 1)))

  if (file.exists(value_v2_path)) {
    # Guarded the same way as the rating table, and for a sharper reason: the
    # upload step globs blog/*.parquet unconditionally, so a present-but-empty
    # value file would be written here and then published OVER the good copy
    # already on R2. "Downloaded successfully" does not mean "has rows".
    vv_raw <- read_parquet(value_v2_path)
    vv_need <- c("format", "gender", "rank", "player_id", "player_name",
                 "total_value", "bat_value", "bowl_value", "matches",
                 "bat_balls", "bowl_balls", "calibrated", "as_at")
    vv_missing <- setdiff(vv_need, names(vv_raw))
    if (length(vv_missing)) {
      stop("player_value_v2.parquet is missing column(s): ",
           paste(vv_missing, collapse = ", "),
           "\n  Produced by bouncer's 01_build_player_ratings_v2.R; check that ",
           "the release asset matches the current schema.")
    }
    if (nrow(vv_raw) == 0L) stop("player_value_v2.parquet is empty.")

    # player_id for the same reason as the rating table above: two names are
    # shared by two different players here too.
    vv <- vv_raw |>
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

# ---------------------------------------------------------------------------
# Player Rating TSA (bouncerverse D-P51)
#
# A separate, lambda-free lens on the same rating-v2 pipeline: prices a
# wicket by its effect on the match's own projected-final-score curve
# instead of a flat lambda. Alongside player-ratings-v2, not replacing it --
# Pete's call. Same non-recompute philosophy as v2 above: the parquet on
# `player-rating-tsa` is already anchor-checked at publish time
# (02_build_player_ratings_tsa.R in bouncer), so re-deriving anything here
# would only add a second place for it to go wrong.
#
# Same schema shape as player_rating_v2 (store_player_rating_v2() writes both
# through .rating_v2_schema), so this block mirrors that one closely, all
# formats -- no value-table analogue yet, that's the only difference.
#
# Test WAS excluded here on the reasoning that TSA has no fixed ball
# allocation to project against in Test. That reasoning no longer holds:
# bouncer `dev` c/e171cca (2026-09-04, bouncerverse D-P65) shipped a two-stage
# expected-overs projection specifically to give Test TSA a real rating, and
# it is now live and anchor-checked (`test`/`male` in main.player_rating_tsa,
# same check_anchor() gate every other bucket already passes through
# 02_build_player_ratings_tsa.R). The hard stop() this block used to have on
# any Test row would abort the WHOLE build the first time this release
# republishes with Test data in it -- removed rather than left to fire.
#
# `format='TEST'` rows here still are NOT wired into the cricket frontend
# (cricket/player-ratings.qmd's slice() calls only cover t20/odi buckets) --
# publishing them here is safe (unused rows, nothing reads them) but does not
# by itself put Test on the site. That is a separate frontend change.
rating_tsa_path <- "source/player_rating_tsa.parquet"

if (file.exists(rating_tsa_path)) {
  rt <- read_parquet(rating_tsa_path)

  need <- c("format", "gender", "role", "rank", "player_id", "player_name",
            "rating", "average", "main_comp", "matches", "balls",
            "effective_matches", "last_match", "as_at")
  missing <- setdiff(need, names(rt))
  if (length(missing)) {
    stop("player_rating_tsa.parquet is missing column(s): ",
         paste(missing, collapse = ", "),
         "\n  Produced by bouncer's 02_build_player_ratings_tsa.R; check that ",
         "the release asset matches the current schema.")
  }
  if (nrow(rt) == 0L) stop("player_rating_tsa.parquet is empty.")

  # Same player_id caveat as v2 above: `player` is not unique, and the
  # front-end links by id.
  rt <- rt |>
    mutate(bucket = paste0(tolower(format), "-", gender)) |>
    select(bucket, role, rank, player_id, player = player_name, rating, average,
           main_comp, matches, balls, effective_matches, last_match, as_at) |>
    arrange(bucket, role, rank)
  stopifnot(!anyNA(rt$player_id))

  if (!is.null(player_meta) && "country" %in% names(player_meta)) {
    before <- nrow(rt)
    rt <- rt |>
      left_join(
        player_meta |>
          select(player_id, country, full_name, dob, batting_style, bowling_style) |>
          distinct(player_id, .keep_all = TRUE),
        by = "player_id")
    stopifnot(nrow(rt) == before)
    cat(sprintf("  ratings tsa: %d/%d rows enriched with country\n",
                sum(!is.na(rt$country)), nrow(rt)))
  } else {
    cat("  ratings tsa: no player_meta available -- publishing without country/style/age\n")
  }

  write_parquet(rt, "blog/player-rating-tsa.parquet")
  cat(sprintf("  ratings tsa: %d rows across %d bucket-roles\n",
              nrow(rt), dplyr::n_distinct(rt$bucket, rt$role)))
} else {
  # Not fatal, same reasoning as v2 above -- TSA is newer than this script's
  # other inputs.
  cat("  ratings tsa: source/player_rating_tsa.parquet ABSENT -- TSA table not published\n")
}
