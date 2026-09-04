# build_player_details.R
# Build enriched player details by crosswalking cricsheet ↔ cricinfo
#
# Requires: bouncer.duckdb with cricsheet + cricinfo schemas
# Produces: blog/bouncer_player_details.parquet
#
# The crosswalk matches players across datasets using:
#   - Shared match_id (both cover same matches)
#   - Same batting team within that match
#   - Same surname (last token of player name)
#   - Ranked by number of co-occurrences (2+ = confident match)
#
# `country` is NOT taken from cricsheet.players (that column is first-seen
# team, so it reads club/franchise names for most players -- bouncerverse#77,
# D-P58). It is also not sourced from cricinfo: cricinfo's own team_name has
# the same problem, since it scrapes domestic Hawkeye competitions too (a
# player's modal cricinfo team is often still a domestic side). Instead it is
# derived from cricsheet.matches.team_type, which IS a genuine
# international/club flag from the raw data -- a player's modal team across
# matches where team_type = 'international' is their real nationality, and a
# player with no international appearance gets NA rather than a fabricated
# country (2026-09-01 decision, D-P63; see docs/DECISIONS.md).
#
# Run locally or whenever new cricinfo data is ingested.

library(duckdb)
library(arrow)
library(dplyr)

DB_PATH <- "bouncer.duckdb"
if (!file.exists(DB_PATH)) stop("Database not found: ", DB_PATH)

con <- dbConnect(duckdb(), DB_PATH, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE))

dir.create("blog", showWarnings = FALSE)

cat("Building player details with cricinfo enrichment...\n")

# Step 1: Build crosswalk via shared matches + team + surname
cat("  Building cricsheet ↔ cricinfo crosswalk...\n")
crosswalk <- dbGetQuery(con, "
  WITH cs_batters AS (
    SELECT DISTINCT match_id, batter_id, batting_team
    FROM cricsheet.deliveries
    WHERE match_id IN (SELECT DISTINCT match_id FROM cricinfo.innings)
  ),
  raw_matches AS (
    SELECT
      cb.batter_id AS cs_id,
      ci.player_id AS ci_id,
      COUNT(DISTINCT cb.match_id) AS shared_matches
    FROM cs_batters cb
    JOIN cricsheet.players cs ON cb.batter_id = cs.player_id
    JOIN cricinfo.innings ci ON cb.match_id = ci.match_id
      AND cb.batting_team = ci.team_name
      AND SPLIT_PART(cs.player_name, ' ', -1) = SPLIT_PART(ci.player_name, ' ', -1)
    GROUP BY cb.batter_id, ci.player_id
  ),
  ranked AS (
    -- `ci_id` is the explicit tiebreaker: without it, a cricsheet player who
    -- matches two cricinfo ids on the same number of shared matches is assigned
    -- one of them arbitrarily and differently on each run, which changes their
    -- name/DOB/style enrichment AND their fallback nationality. Same defect
    -- class as the two ranking queries below.
    SELECT cs_id, ci_id, shared_matches,
      ROW_NUMBER() OVER (PARTITION BY cs_id ORDER BY shared_matches DESC, ci_id ASC) AS rn
    FROM raw_matches
  )
  SELECT cs_id, ci_id, shared_matches
  FROM ranked
  WHERE rn = 1 AND shared_matches >= 2
")
cat(sprintf("  Crosswalk: %d players mapped (>= 2 shared matches)\n", nrow(crosswalk)))

# Step 2: Get cricinfo metadata (deduplicated per player)
cat("  Fetching cricinfo metadata...\n")
ci_meta <- dbGetQuery(con, "
  SELECT
    player_id AS ci_id,
    MAX(player_name) AS full_name,
    MAX(player_dob) AS dob_raw,
    MAX(batting_style) AS batting_style,
    MAX(bowling_style) AS bowling_style
  FROM cricinfo.innings
  GROUP BY player_id
")

# Step 3: Parse DOB from Python dict string (e.g. {'year': 1990, 'month': 3, 'date': 15}) to ISO date
parse_dob <- function(x) {
  if (is.na(x) || x == "" || !grepl("year", x)) return(NA_character_)
  year <- as.integer(sub(".*'year':\\s*(\\d+).*", "\\1", x))
  month <- as.integer(sub(".*'month':\\s*(\\d+).*", "\\1", x))
  day <- as.integer(sub(".*'date':\\s*(\\d+).*", "\\1", x))
  if (is.na(year) || is.na(month) || is.na(day)) return(NA_character_)
  sprintf("%04d-%02d-%02d", year, month, day)
}
ci_meta$dob <- sapply(ci_meta$dob_raw, parse_dob, USE.NAMES = FALSE)
n_raw <- sum(!is.na(ci_meta$dob_raw) & ci_meta$dob_raw != "")
n_parsed <- sum(!is.na(ci_meta$dob))
if (n_raw > 0 && n_parsed < n_raw * 0.5) {
  warning(sprintf("DOB parsing: only %d/%d parsed — format may have changed", n_parsed, n_raw))
}
ci_meta$dob_raw <- NULL

# Step 4: Get all cricsheet players
cat("  Fetching cricsheet player registry...\n")
cs_players <- dbGetQuery(con, "
  SELECT player_id, player_name
  FROM cricsheet.players
")

# Drop the bare-name fallback rows (bouncerverse#75): registry entries whose
# player_id IS the name string, created when ingestion could not resolve a
# player against registry$people. They carry no deliveries and no metadata, but
# 3,281 of them duplicate a REAL player's name, so anything that displays or
# looks up by name gets a phantom twin. The permanent fix is the scoped delete
# in the SIBLING REPO at bouncer/data-raw/data-acquisition/delete_orphan_player_names.R
# (written, tested, not yet run) -- bouncerdata has no data-raw/ of its own, so
# the path needs the repo prefix to be findable from here. This filter keeps them
# out of the published artefact meanwhile.
n_before <- nrow(cs_players)
cs_players <- cs_players[cs_players$player_id != cs_players$player_name, , drop = FALSE]
n_junk <- n_before - nrow(cs_players)
cat(sprintf("  Dropped %d bare-name fallback rows (#75); %d real players remain\n",
            n_junk, nrow(cs_players)))
if (nrow(cs_players) < 0.5 * n_before) {
  stop(sprintf("Bare-name filter removed %.0f%% of the registry - refusing to publish",
               100 * n_junk / n_before))
}

# Step 4b: Derive real nationality from international appearances (not the
# registry's first-seen-team `country`, and not cricinfo -- see header note).
cat("  Deriving nationality from international appearances...\n")
# `team_type = 'international'` also covers composite invitational sides, which
# are not nationalities. Only three exist in the corpus (ICC World XI 7 matches,
# Asia XI 5, Africa XI 5), but excluding them matters: cricsheet withholds all
# Afghanistan data (0 matches), so Rashid Khan's ONLY international appearances
# are 33 deliveries for ICC World XI, and the modal rule was about to publish
# that as his country at rank 3 of the T20 male board. He now falls to NA, which
# is honest -- the Afghan players are unfixable from this source, not mislabelled.
COMPOSITE_SIDES <- c("ICC World XI", "Asia XI", "Africa XI")
# Single source of truth: build the SQL IN-list FROM the R constant rather than
# repeating the literals. They were duplicated in three queries, which meant the
# "leaked" assertion below could only ever catch a typo between the copies.
.composite_sql <- paste0(
  "(", paste(sprintf("'%s'", gsub("'", "''", COMPOSITE_SIDES)), collapse = ", "), ")")

nationality <- dbGetQuery(con, sprintf("
  WITH appearances AS (
    SELECT batter_id AS player_id, batting_team AS team
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON d.match_id = m.match_id
    WHERE m.team_type = 'international'
      AND d.batting_team NOT IN %1$s
    UNION ALL
    SELECT bowler_id AS player_id, bowling_team AS team
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON d.match_id = m.match_id
    WHERE m.team_type = 'international'
      AND d.bowling_team NOT IN %1$s
  ),
  counted AS (
    SELECT player_id, team, COUNT(*) AS n
    FROM appearances
    GROUP BY player_id, team
  ),
  ranked AS (
    -- `team` is the explicit tiebreaker. Without it this ORDER BY is
    -- non-deterministic on ties and the published file changes between runs
    -- (measured: five builds, five different md5s, via the cricinfo fallback
    -- query which had the same defect). 0 players are currently tied here, but
    -- that is a property of today's data, not a guarantee.
    SELECT player_id, team,
      ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY n DESC, team ASC) AS rn
    FROM counted
  )
  SELECT player_id, team AS country
  FROM ranked
  WHERE rn = 1
", .composite_sql))
cat(sprintf("  Nationality resolved for %d players (%.0f%% of registry)\n",
            nrow(nationality), 100 * nrow(nationality) / nrow(cs_players)))

# The whole nationality step is one predicate away from silently producing
# nothing: everything below keys off `m.team_type = 'international'`, and if that
# value is ever renamed or recased upstream the query returns zero rows without
# erroring, `country` lands 100% NA, and the file still publishes. That is the
# failure this repo has already shipped once (a column present, correctly typed
# and 0% populated for months). Fail here instead.
if (!nrow(nationality)) {
  stop("Nationality query returned 0 rows -- check that cricsheet.matches.team_type ",
       "still uses the value 'international'. Refusing to publish an empty country column.")
}

# Step 4c: fall back to cricinfo for nationalities cricsheet cannot supply.
#
# Cricsheet withholds entire nations at source -- Afghanistan has 0 matches in
# cricsheet.matches, so no Afghan player can ever get a nationality from step 4b,
# however many internationals they have played. Cricinfo DOES carry them (798
# Afghanistan innings rows, 60 players).
#
# Cricinfo's team_name cannot be trusted on its own -- it scrapes domestic
# competitions too, so a player's modal cricinfo team is often a franchise (the
# reason the header rules cricinfo out as the primary source). The fallback is
# therefore restricted to team names already known to be NATIONAL sides: the set
# cricsheet itself flags team_type='international', minus the composite sides,
# plus Afghanistan (which by construction can never appear in a cricsheet-derived
# set). A franchise name cannot pass that filter.
#
# Deliberately narrow: it is not a general enrichment path, it is a patch for
# nations missing at source. The count it fills is logged at runtime rather than
# quoted here, because it moves as cricinfo coverage grows.
national_sides <- dbGetQuery(con, sprintf("
  SELECT DISTINCT team FROM (
    SELECT team1 AS team FROM cricsheet.matches WHERE team_type = 'international'
    UNION ALL
    SELECT team2 AS team FROM cricsheet.matches WHERE team_type = 'international'
  ) WHERE team NOT IN %s
", .composite_sql))$team
national_sides <- union(national_sides, "Afghanistan")

# Filter to national sides BEFORE ranking, not after, and break ties explicitly.
#
# The previous shape (rank every cricinfo team by appearances, take the modal
# one, THEN drop it if it is not a national side) had two faults, both found by
# running this script three times and getting three different files:
#
#   1. NON-DETERMINISM. `ROW_NUMBER() OVER (... ORDER BY n DESC)` has no
#      tiebreaker, and 423 of 9,326 cricinfo players (4.5%) are tied at the top.
#      DuckDB resolved those ties differently on each run, so the published
#      artefact varied run to run -- measured at 20/21/22/23/24 fallback players
#      across five builds, with the country mix changing too. The main cricsheet
#      nationality query has 0 ties today, but it gets the same explicit
#      tiebreaker below, because "no ties" is a property of today's data, not a
#      guarantee.
#
#   2. IT ASKED THE WRONG QUESTION. Ranking first meant a player whose modal
#      cricinfo team is their franchise got NO country even when they had plenty
#      of national appearances -- the tie decided whether they were represented
#      at all. The question here is "does cricinfo show a national side for this
#      player", so restrict to national sides and take the modal one of those.
#      Age-group and A sides ("Afghanistan Under-19s", "Afghanistan A") are not
#      in national_sides, so they are excluded by the same filter.
ci_counts <- dbGetQuery(con, "
  SELECT player_id AS ci_id, team_name, COUNT(*) AS n
  FROM cricinfo.innings GROUP BY 1, 2
")
ci_counts <- ci_counts[ci_counts$team_name %in% national_sides, , drop = FALSE]
ci_counts <- ci_counts[order(ci_counts$ci_id, -ci_counts$n, ci_counts$team_name), ]
ci_nationality <- ci_counts[!duplicated(ci_counts$ci_id), c("ci_id", "team_name")]
names(ci_nationality)[names(ci_nationality) == "team_name"] <- "ci_country"

fallback <- merge(crosswalk[, c("cs_id", "ci_id")], ci_nationality, by = "ci_id")
fallback <- fallback[!fallback$cs_id %in% nationality$player_id, , drop = FALSE]
if (nrow(fallback)) {
  cat(sprintf("  Cricinfo fallback filled %d players cricsheet could not: %s\n",
              nrow(fallback),
              paste(sprintf("%s x%d", names(sort(table(fallback$ci_country), decreasing = TRUE)),
                            sort(table(fallback$ci_country), decreasing = TRUE)),
                    collapse = ", ")))
  nationality <- rbind(
    nationality,
    data.frame(player_id = fallback$cs_id, country = fallback$ci_country,
               stringsAsFactors = FALSE)
  )
}
stopifnot(!anyDuplicated(nationality$player_id))

# Validate the FINAL country set -- after the cricinfo fallback has merged in,
# not before it. The earlier version of this ran before the rbind above, so
# fallback-sourced countries were never checked at all.
#
# Two separate checks, because they catch different things:
#
#   `leaked` is only an assertion that the SQL exclusion above did its job. It
#   cannot catch a NEW invitational side, because it compares against the very
#   list the query already excluded. That is all it claims to be.
#
#   `suspect` is the one that actually protects against a new World XI-type
#   fixture, which is the real risk -- cricsheet withholding Afghanistan is what
#   made a 33-delivery ICC World XI stint Rashid Khan's modal international team
#   in the first place, and nothing stops a future "Cricket All-Stars" or
#   "World Select XI" doing the same to somebody else. It is a stop(), not a
#   warning(): publishing a made-up nationality on a leaderboard is worse than
#   failing a build. Verified against all 108 real country values in the corpus
#   on 2026-09-03 -- none of them match this pattern, so it cannot fire
#   spuriously on a genuine nation.
leaked <- intersect(unique(nationality$country), COMPOSITE_SIDES)
if (length(leaked)) {
  stop("Composite side survived the SQL exclusion: ", paste(leaked, collapse = ", "),
       " -- the NOT IN filter is not working.")
}
suspect <- setdiff(
  grep("\\bXI\\b|World|Invitation|Select|Combined|ICC", unique(nationality$country),
       value = TRUE),
  COMPOSITE_SIDES)
if (length(suspect)) {
  stop("Country value(s) look like a composite/invitational side, not a nation: ",
       paste(suspect, collapse = ", "),
       "\n  If this is a real nation, widen the pattern. If it is an invitational ",
       "side, add it to COMPOSITE_SIDES -- it must not be published as anyone's ",
       "nationality.")
}

# Step 5: Join everything
cat("  Joining metadata...\n")
details <- cs_players |>
  left_join(nationality, by = "player_id") |>
  left_join(crosswalk, by = c("player_id" = "cs_id")) |>
  left_join(ci_meta, by = "ci_id") |>
  transmute(
    player_id,
    player_name,
    country,
    full_name,
    dob,
    batting_style,
    bowling_style
  )

# Summary
n_enriched <- sum(!is.na(details$full_name))
n_dob <- sum(!is.na(details$dob))
n_bat <- sum(!is.na(details$batting_style))
n_country <- sum(!is.na(details$country))
cat(sprintf("  Total players: %d\n", nrow(details)))
cat(sprintf("  With real nationality: %d (%.0f%%)\n", n_country, 100 * n_country / nrow(details)))
cat(sprintf("  With cricinfo enrichment: %d (%.0f%%)\n", n_enriched, 100 * n_enriched / nrow(details)))
cat(sprintf("  With DOB: %d | Batting style: %d\n", n_dob, n_bat))

# Coverage floor, not a presence check. `country` can be present, correctly typed
# and 0% populated while every other check in this script passes -- and this
# artefact in particular went six months without a rebuild (bouncerdata#70), so a
# collapse here would sit unnoticed and feed straight into build_blog_data.R's
# player_meta join, which also only tests for the column's presence.
#
# Floor is deliberately well below the observed value rather than near it: 65.8%
# on 2026-09-03 (8,501 of 12,915). 40% leaves room for the genuine drift of more
# uncapped domestic players entering the registry, while still catching the
# failure mode that matters, which is a collapse toward zero rather than a slow
# decline.
MIN_COUNTRY_COVERAGE <- 0.40
if (n_country < MIN_COUNTRY_COVERAGE * nrow(details)) {
  stop(sprintf(
    paste0("country coverage collapsed to %.1f%% (%d of %d), below the %.0f%% floor ",
           "-- refusing to publish. Check cricsheet.matches.team_type and the ",
           "cricinfo fallback's national_sides list before overriding this."),
    100 * n_country / nrow(details), n_country, nrow(details),
    100 * MIN_COUNTRY_COVERAGE))
}

# Sort before writing. DuckDB does not guarantee row order, so without this the
# same data lands in a different order on each run and the published artefact's
# bytes change even when nothing about its content did -- which makes
# "did this actually change?" unanswerable by checksum, both for a human and for
# the cricinfo-daily unchanged-asset check.
details <- details[order(details$player_id), , drop = FALSE]

write_parquet(details, "blog/bouncer_player_details.parquet")
cat("  Written: blog/bouncer_player_details.parquet\n")
cat("Done.\n")
