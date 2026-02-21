#!/usr/bin/env python3
"""Combine per-match Cricinfo parquets into format/gender-level combined files.

Reads per-match parquets from cricinfo/{format}_{gender}/ and produces combined
parquet files suitable for GitHub release upload and remote R loader access.

Ball-by-ball files get camelCase → snake_case column renaming so R never sees
camelCase. Match and innings files are already snake_case from the scraper.

Usage:
    python scripts/combine_cricinfo_parquets.py --cricinfo-dir cricinfo
    python scripts/combine_cricinfo_parquets.py --cricinfo-dir cricinfo --formats t20i_male odi_male
"""

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# camelCase → snake_case mapping for ball-by-ball parquets
BALLS_COLUMN_MAP = {
    "id": "id",
    "inningNumber": "innings_number",
    "overNumber": "over_number",
    "ballNumber": "ball_number",
    "oversActual": "overs_actual",
    "oversUnique": "overs_unique",
    "totalRuns": "total_runs",
    "batsmanRuns": "batsman_runs",
    "isFour": "is_four",
    "isSix": "is_six",
    "isWicket": "is_wicket",
    "dismissalType": "dismissal_type",
    "dismissalText": "dismissal_text",
    "wides": "wides",
    "noballs": "noballs",
    "byes": "byes",
    "legbyes": "legbyes",
    "penalties": "penalties",
    "wagonX": "wagon_x",
    "wagonY": "wagon_y",
    "wagonZone": "wagon_zone",
    "pitchLine": "pitch_line",
    "pitchLength": "pitch_length",
    "shotType": "shot_type",
    "shotControl": "shot_control",
    "batsmanPlayerId": "batsman_player_id",
    "bowlerPlayerId": "bowler_player_id",
    "nonStrikerPlayerId": "non_striker_player_id",
    "outPlayerId": "out_player_id",
    "totalInningRuns": "total_innings_runs",
    "totalInningWickets": "total_innings_wickets",
    "predicted_score": "predicted_score",
    "win_probability": "win_probability",
    "event_type": "event_type",
    "drs_successful": "drs_successful",
    "title": "title",
    "timestamp": "timestamp",
}


def rename_balls_columns(table):
    """Rename camelCase ball columns to snake_case using the mapping."""
    new_names = []
    for col in table.column_names:
        new_names.append(BALLS_COLUMN_MAP.get(col, col))
    return table.rename_columns(new_names)


def unify_and_concat(tables):
    """Concatenate tables with potentially mismatched schemas.

    Handles the common case where some tables have null-type columns (all values
    null) while others have concrete types (string, bool, etc.). Builds a unified
    schema preferring non-null types, then casts all tables before concatenating.
    """
    if not tables:
        return None

    # Build unified schema: for each field name, pick the first non-null type
    all_fields = {}  # name → type
    field_order = []
    for t in tables:
        for field in t.schema:
            if field.name not in all_fields:
                all_fields[field.name] = field.type
                field_order.append(field.name)
            elif all_fields[field.name] == pa.null():
                # Upgrade from null to a concrete type
                all_fields[field.name] = field.type

    unified_schema = pa.schema(
        [pa.field(name, all_fields[name]) for name in field_order]
    )

    # Cast each table to the unified schema
    unified_tables = []
    for t in tables:
        columns = {}
        for field in unified_schema:
            if field.name in t.column_names:
                col = t.column(field.name)
                if col.type != field.type:
                    col = col.cast(field.type)
                columns[field.name] = col
            else:
                # Missing column — fill with nulls
                columns[field.name] = pa.nulls(t.num_rows, type=field.type)
        unified_tables.append(
            pa.table(columns, schema=unified_schema)
        )

    return pa.concat_tables(unified_tables)


def combine_table_type(cricinfo_dir, format_gender, table_type, output_dir):
    """Combine all per-match parquets of a given type into one combined file.

    Args:
        cricinfo_dir: Path to cricinfo/ directory
        format_gender: e.g. "t20i_male"
        table_type: "balls", "match", or "innings"
        output_dir: Where to write combined parquets

    Returns:
        Number of rows in combined file, or 0 if no data.
    """
    data_dir = Path(cricinfo_dir) / format_gender
    if not data_dir.exists():
        return 0

    pattern = f"*_{table_type}.parquet"
    files = sorted(data_dir.glob(pattern))
    if not files:
        return 0

    # For match/innings files, only include those that have corresponding balls data.
    # The scraper saves match metadata for all matches in a series, but only creates
    # balls/innings files for matches it actually scrapes ball-by-ball.
    if table_type in ("match", "innings"):
        ball_ids = {
            f.stem.rsplit("_balls", 1)[0]
            for f in data_dir.glob("*_balls.parquet")
        }
        files = [f for f in files if f.stem.rsplit(f"_{table_type}", 1)[0] in ball_ids]
        if not files:
            return 0

    tables = []
    for f in files:
        try:
            t = pq.read_table(f)
            # Extract match_id from filename: {match_id}_{type}.parquet
            match_id = f.stem.rsplit(f"_{table_type}", 1)[0]

            # Validate match_id is numeric (Cricinfo match IDs are always integers).
            # Guards against filenames containing the table_type substring in the ID.
            if not match_id.isdigit():
                print(f"  Warning: Skipping {f.name} — extracted match_id '{match_id}' is not numeric", file=sys.stderr)
                continue

            if table_type == "balls":
                t = rename_balls_columns(t)
                # Add match_id column (balls parquets don't have it)
                t = t.append_column(
                    "match_id",
                    pa.array([match_id] * t.num_rows, type=pa.string()),
                )
            elif table_type == "innings":
                # Innings parquets don't have match_id either
                if "match_id" not in t.column_names:
                    t = t.append_column(
                        "match_id",
                        pa.array([match_id] * t.num_rows, type=pa.string()),
                    )

            tables.append(t)
        except Exception as e:
            print(f"  Warning: Failed to read {f}: {e}", file=sys.stderr)

    if not tables:
        return 0

    # Unify schemas (some files have null-type columns, others have concrete types)
    combined = unify_and_concat(tables)

    # Output naming: cricinfo_{type}_{format}_{gender}.parquet
    # e.g. cricinfo_balls_t20i_male.parquet
    out_name = f"cricinfo_{table_type}_{format_gender}.parquet"
    out_path = Path(output_dir) / out_name
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write: temp file + rename prevents corruption on crash/sync conflict
    tmp_path = out_path.with_suffix('.parquet.tmp')
    pq.write_table(combined, tmp_path)
    tmp_path.rename(out_path)

    return combined.num_rows


def main():
    parser = argparse.ArgumentParser(
        description="Combine per-match Cricinfo parquets into combined files"
    )
    parser.add_argument(
        "--cricinfo-dir",
        default="cricinfo",
        help="Path to cricinfo data directory (default: cricinfo)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for combined parquets (default: {cricinfo-dir}/combined)",
    )
    parser.add_argument(
        "--formats",
        nargs="*",
        default=None,
        help="Specific format_gender dirs to process (default: all found)",
    )
    args = parser.parse_args()

    cricinfo_dir = Path(args.cricinfo_dir)
    output_dir = Path(args.output_dir) if args.output_dir else cricinfo_dir / "combined"

    if not cricinfo_dir.exists():
        print(f"Error: {cricinfo_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    # Discover format_gender directories
    if args.formats:
        format_genders = args.formats
    else:
        format_genders = []
        for d in sorted(cricinfo_dir.iterdir()):
            if d.is_dir() and "_" in d.name and d.name != "combined" and not d.name.startswith("_"):
                format_genders.append(d.name)

    if not format_genders:
        print("No format_gender directories found", file=sys.stderr)
        sys.exit(0)

    print(f"Combining parquets for: {', '.join(format_genders)}")
    total_files = 0

    for fg in format_genders:
        for table_type in ("balls", "match", "innings"):
            n_rows = combine_table_type(cricinfo_dir, fg, table_type, output_dir)
            if n_rows > 0:
                out_name = f"cricinfo_{table_type}_{fg}.parquet"
                print(f"  {out_name}: {n_rows:,} rows")
                total_files += 1

    print(f"\nCombined {total_files} parquet files into {output_dir}")


if __name__ == "__main__":
    main()
