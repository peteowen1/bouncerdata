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
    SELECT cs_id, ci_id, shared_matches,
      ROW_NUMBER() OVER (PARTITION BY cs_id ORDER BY shared_matches DESC) AS rn
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
# in data-raw/data-acquisition/delete_orphan_player_names.R (written, tested,
# not yet run); this filter keeps them out of the published artefact meanwhile.
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
nationality <- dbGetQuery(con, "
  WITH appearances AS (
    SELECT batter_id AS player_id, batting_team AS team
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON d.match_id = m.match_id
    WHERE m.team_type = 'international'
      AND d.batting_team NOT IN ('ICC World XI', 'Asia XI', 'Africa XI')
    UNION ALL
    SELECT bowler_id AS player_id, bowling_team AS team
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON d.match_id = m.match_id
    WHERE m.team_type = 'international'
      AND d.bowling_team NOT IN ('ICC World XI', 'Asia XI', 'Africa XI')
  ),
  counted AS (
    SELECT player_id, team, COUNT(*) AS n
    FROM appearances
    GROUP BY player_id, team
  ),
  ranked AS (
    SELECT player_id, team,
      ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY n DESC) AS rn
    FROM counted
  )
  SELECT player_id, team AS country
  FROM ranked
  WHERE rn = 1
")
cat(sprintf("  Nationality resolved for %d players (%.0f%% of registry)\n",
            nrow(nationality), 100 * nrow(nationality) / nrow(cs_players)))

# Guard: a composite side must never reach the country column. If cricsheet adds
# another one (a new World XI fixture), fail loudly here rather than publish it
# as somebody's nationality.
leaked <- intersect(unique(nationality$country), COMPOSITE_SIDES)
if (length(leaked)) {
  stop("Composite side leaked into country: ", paste(leaked, collapse = ", "))
}
suspect <- grep("XI$|World|ICC", unique(nationality$country), value = TRUE)
if (length(suspect)) {
  warning("country values that look like composite sides, not nations: ",
          paste(suspect, collapse = ", "))
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
# Deliberately narrow: this fills 20 players, 13 of them Afghan. It is not a
# general enrichment path, it is a patch for nations missing at source.
national_sides <- dbGetQuery(con, "
  SELECT DISTINCT team FROM (
    SELECT team1 AS team FROM cricsheet.matches WHERE team_type = 'international'
    UNION ALL
    SELECT team2 AS team FROM cricsheet.matches WHERE team_type = 'international'
  ) WHERE team NOT IN ('ICC World XI', 'Asia XI', 'Africa XI')
")$team
national_sides <- union(national_sides, "Afghanistan")

ci_nationality <- dbGetQuery(con, "
  WITH counted AS (
    SELECT player_id AS ci_id, team_name, COUNT(*) AS n
    FROM cricinfo.innings GROUP BY 1, 2
  ),
  ranked AS (
    SELECT ci_id, team_name, n,
      ROW_NUMBER() OVER (PARTITION BY ci_id ORDER BY n DESC) AS rn
    FROM counted
  )
  SELECT ci_id, team_name AS ci_country FROM ranked WHERE rn = 1
")
ci_nationality <- ci_nationality[ci_nationality$ci_country %in% national_sides, , drop = FALSE]

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

write_parquet(details, "blog/bouncer_player_details.parquet")
cat("  Written: blog/bouncer_player_details.parquet\n")
cat("Done.\n")
