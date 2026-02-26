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
            else:
                # Promote types when they conflict to avoid cast errors
                existing = all_fields[field.name]
                incoming = field.type
                if existing == incoming:
                    pass
                elif pa.types.is_integer(existing) and pa.types.is_floating(incoming):
                    all_fields[field.name] = incoming  # int→float
                elif pa.types.is_floating(existing) and pa.types.is_integer(incoming):
                    pass  # keep float
                elif pa.types.is_string(existing) or pa.types.is_string(incoming):
                    all_fields[field.name] = pa.string()  # any conflict with string→string
                elif pa.types.is_large_string(existing) or pa.types.is_large_string(incoming):
                    all_fields[field.name] = pa.large_string()

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
                    try:
                        col = col.cast(field.type)
                    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                        # Fallback: cast to string if types are truly incompatible
                        col = col.cast(pa.string())
                columns[field.name] = col
            else:
                # Missing column — fill with nulls
                columns[field.name] = pa.nulls(t.num_rows, type=field.type)
        unified_tables.append(
            pa.table(columns, schema=unified_schema)
        )

    return pa.concat_tables(unified_tables)


def extract_match_id(filepath, table_type):
    """Extract numeric match_id from a per-match parquet filename.

    Returns the match_id string, or None if the filename doesn't parse correctly.
    """
    match_id = filepath.stem.rsplit(f"_{table_type}", 1)[0]
    return match_id if match_id.isdigit() else None


def combine_table_type(cricinfo_dir, format_gender, table_type, output_dir, merge=False):
    """Combine all per-match parquets of a given type into one combined file.

    Args:
        cricinfo_dir: Path to cricinfo/ directory
        format_gender: e.g. "t20i_male"
        table_type: "balls", "match", or "innings"
        output_dir: Where to write combined parquets
        merge: If True and a combined file already exists, merge new per-match
               data into it instead of rebuilding from scratch.

    Returns:
        Number of rows in combined file, or 0 if no data.
    """
    data_dir = Path(cricinfo_dir) / format_gender
    out_name = f"cricinfo_{table_type}_{format_gender}.parquet"
    out_path = Path(output_dir) / out_name

    # In merge mode, we can return existing data even if no per-match dir exists
    existing_table = None
    existing_ids = set()
    if merge and out_path.exists():
        try:
            existing_table = pq.read_table(out_path)
            if "match_id" in existing_table.column_names:
                # Convert to strings — filename-extracted IDs are always strings,
                # but the scraper may store match_id as int in match/innings tables
                existing_ids = set(str(x) for x in existing_table.column("match_id").to_pylist())
        except Exception as e:
            print(f"  Warning: Failed to read existing {out_name}, doing full rebuild: {e}", file=sys.stderr)
            existing_table = None
            existing_ids = set()

    if not data_dir.exists():
        return existing_table.num_rows if existing_table is not None else 0

    pattern = f"*_{table_type}.parquet"
    files = sorted(data_dir.glob(pattern))
    if not files and existing_table is None:
        return 0

    # For match/innings files, only include those that have corresponding balls data.
    # This ensures combined files only contain matches with full ball-by-ball coverage,
    # even though the scraper may save match and innings metadata independently.
    if table_type in ("match", "innings"):
        ball_ids = {
            f.stem.rsplit("_balls", 1)[0]
            for f in data_dir.glob("*_balls.parquet")
        }
        files = [f for f in files if f.stem.rsplit(f"_{table_type}", 1)[0] in ball_ids]

    # In merge mode, skip per-match files already in the existing combined file
    if existing_ids:
        files = [f for f in files if extract_match_id(f, table_type) not in existing_ids]
        if not files:
            # Nothing new to add
            return existing_table.num_rows

    tables = []
    read_failures = 0
    for f in files:
        try:
            t = pq.read_table(f)
            # Extract match_id from filename: {match_id}_{type}.parquet
            match_id = extract_match_id(f, table_type)

            # Validate match_id is numeric (Cricinfo match IDs are always integers).
            # Guards against filenames containing the table_type substring in the ID.
            if match_id is None:
                print(f"  Warning: Skipping {f.name} — extracted match_id is not numeric", file=sys.stderr)
                read_failures += 1
                continue

            # Ensure every table has a valid match_id column from the filename.
            # Balls parquets never have it; innings/match may have it but it can
            # be null-typed if the scraper didn't capture it from the API.
            match_id_arr = pa.array([match_id] * t.num_rows, type=pa.string())
            if table_type == "balls":
                t = rename_balls_columns(t)
                t = t.append_column("match_id", match_id_arr)
            elif "match_id" not in t.column_names or t.schema.field("match_id").type == pa.null():
                if "match_id" in t.column_names:
                    t = t.drop("match_id")
                t = t.append_column("match_id", match_id_arr)

            tables.append(t)
        except Exception as e:
            read_failures += 1
            print(f"  Warning: Failed to read {f}: {e}", file=sys.stderr)

    if read_failures:
        print(f"  Warning: {read_failures}/{len(files)} files failed to read for {format_gender}/{table_type}", file=sys.stderr)

    if not tables and existing_table is None:
        return 0

    if not tables and existing_table is not None:
        # No new per-match files, nothing to write
        return existing_table.num_rows

    # Unify schemas (some files have null-type columns, others have concrete types)
    new_combined = unify_and_concat(tables)

    # Merge with existing data if present
    if existing_table is not None and new_combined is not None:
        combined = unify_and_concat([existing_table, new_combined])
        new_count = new_combined.num_rows
        print(f"  Merged {new_count:,} new rows into existing {existing_table.num_rows:,} rows")
    else:
        combined = new_combined

    if combined is None:
        return 0

    # Output naming: cricinfo_{type}_{format}_{gender}.parquet
    # e.g. cricinfo_balls_t20i_male.parquet
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write: temp file + rename prevents corruption on crash/sync conflict
    tmp_path = out_path.with_suffix('.parquet.tmp')
    pq.write_table(combined, tmp_path)
    tmp_path.replace(out_path)

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
    parser.add_argument(
        "--merge",
        action="store_true",
        default=False,
        help="Merge new per-match data into existing combined files instead of rebuilding",
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
        print("Warning: No format_gender directories found", file=sys.stderr)
        sys.exit(1)

    mode = "merge" if args.merge else "full rebuild"
    print(f"Combining parquets ({mode}) for: {', '.join(format_genders)}")
    total_files = 0

    for fg in format_genders:
        for table_type in ("balls", "match", "innings"):
            n_rows = combine_table_type(cricinfo_dir, fg, table_type, output_dir, merge=args.merge)
            if n_rows > 0:
                out_name = f"cricinfo_{table_type}_{fg}.parquet"
                print(f"  {out_name}: {n_rows:,} rows")
                total_files += 1

    print(f"\nCombined {total_files} parquet files into {output_dir}")


if __name__ == "__main__":
    main()
