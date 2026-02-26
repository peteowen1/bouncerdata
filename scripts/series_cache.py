"""Shared module for building and maintaining the series_list.csv cache.

Merges series metadata from multiple sources:
  Tier 1: Parquet scan  — read series_id/name/format/gender from _match.parquet files (offline, fast)
  Tier 2: Live scores   — SSR data from Cricinfo (existing discover_series.py logic)
  Tier 3: Schedule pages — client-side API interception for upcoming/past results (new)

Usage:
    from series_cache import build_series_list

    # Rebuild from parquets + existing CSV (no browser needed):
    series = build_series_list(csv_path="scripts/series_list.csv",
                                cricinfo_dir="../cricinfo")

    # With web discoveries merged in:
    series = build_series_list(csv_path="scripts/series_list.csv",
                                cricinfo_dir="../cricinfo",
                                web_discoveries={...})
"""

import csv
import re
import sys
from pathlib import Path

# ============================================================
# CSV field names (gender column is new, backwards-compatible)
# ============================================================
CSV_FIELDS = ["series_id", "name", "url", "season", "format", "max_innings", "gender"]

MAX_INNINGS = {"test": 4, "odi": 2, "t20i": 2}

# Format directory prefix → normalized format string
FORMAT_DIR_MAP = {
    "t20i": "t20i",
    "odi": "odi",
    "test": "test",
}


# ============================================================
# Tier 1: Parquet scan
# ============================================================

def scan_parquets_for_series(cricinfo_dir):
    """Scan _match.parquet files to discover series metadata.

    Reads only the columns needed (series_id, series_name, format, gender,
    international_class_id) using pyarrow column projection — no full file loads.

    Returns dict keyed by series_id with entry dicts.
    """
    import pyarrow.parquet as pq

    cricinfo_path = Path(cricinfo_dir)
    if not cricinfo_path.exists():
        return {}

    series = {}
    match_files = list(cricinfo_path.glob("*/*_match.parquet"))
    if not match_files:
        return {}

    # Columns we want from _match.parquet (not all will exist in every file)
    target_cols = {"series_id", "series_name", "format", "gender",
                   "international_class_id", "start_date"}

    for mf in match_files:
        try:
            # Read only available target columns
            schema = pq.read_schema(mf)
            available = [c for c in target_cols if c in schema.names]
            if "series_id" not in available:
                continue

            table = pq.read_table(mf, columns=available)
            for i in range(table.num_rows):
                sid = str(table.column("series_id")[i].as_py())
                if not sid or sid == "None":
                    continue
                if sid in series:
                    continue

                name = _col_val(table, "series_name", i) or ""
                fmt_raw = _col_val(table, "format", i) or ""
                gender = _col_val(table, "gender", i) or ""
                class_id = _col_val(table, "international_class_id", i)

                # Detect format from directory name as fallback
                dir_name = mf.parent.name  # e.g. "t20i_male"
                dir_fmt = dir_name.split("_")[0] if "_" in dir_name else ""
                dir_gender = dir_name.split("_")[1] if "_" in dir_name else ""

                fmt = _normalize_format(fmt_raw, class_id) or FORMAT_DIR_MAP.get(dir_fmt)
                if not gender:
                    gender = dir_gender or "male"
                gender = gender.lower()

                # Infer season from start_date if available
                start_date = _col_val(table, "start_date", i) or ""
                season = _season_from_date(start_date)

                url = f"https://www.espncricinfo.com/series/{sid}"

                series[sid] = {
                    "series_id": sid,
                    "name": name,
                    "url": url,
                    "season": season,
                    "format": fmt or "test",
                    "max_innings": str(MAX_INNINGS.get(fmt, 4)),
                    "gender": gender,
                }
        except Exception as e:
            print(f"  Warning: Could not read {mf.name}: {e}", file=sys.stderr)
            continue

    return series


def _col_val(table, col_name, row_idx):
    """Safely get a column value from a pyarrow table."""
    if col_name not in table.column_names:
        return None
    val = table.column(col_name)[row_idx].as_py()
    return val


def _normalize_format(fmt_str, class_id=None):
    """Normalize format strings and class IDs to t20i/odi/test."""
    CLASS_ID_MAP = {1: "test", 2: "odi", 3: "t20i"}
    if class_id and class_id in CLASS_ID_MAP:
        return CLASS_ID_MAP[class_id]
    if not fmt_str:
        return None
    fmt_upper = str(fmt_str).upper()
    FORMAT_MAP = {
        "TEST": "test", "ODI": "odi", "T20I": "t20i", "T20": "t20i",
        "MDM": "test", "ODM": "odi", "IT20": "t20i",
    }
    return FORMAT_MAP.get(fmt_upper)


def _season_from_date(date_str):
    """Infer season string from ISO date (e.g. '2025-11-01' -> '2025/26')."""
    if not date_str or len(date_str) < 4:
        return ""
    try:
        year = int(date_str[:4])
        month = int(date_str[5:7]) if len(date_str) >= 7 else 1
        # Cricket season logic:
        #   Aug-Dec: current season spans two years (e.g. "2025/26")
        #   Jan-Apr: belongs to previous season (e.g. "2024/25")
        #   May-Jul: mid-year season, just the year (e.g. "2025")
        if month >= 8:
            return f"{year}/{str(year + 1)[-2:]}"
        else:
            return f"{year - 1}/{str(year)[-2:]}" if month <= 4 else str(year)
    except (ValueError, IndexError):
        return ""


# ============================================================
# CSV cache operations
# ============================================================

def load_csv_cache(csv_path):
    """Load existing series_list.csv as dict keyed by series_id.

    Handles CSVs with or without the gender column (backwards-compatible).
    """
    series = {}
    path = Path(csv_path)
    if not path.exists():
        return series

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row.get("series_id", "").strip().strip('"')
            if not sid:
                continue
            entry = {
                "series_id": sid,
                "name": row.get("name", "").strip().strip('"'),
                "url": row.get("url", "").strip().strip('"'),
                "season": row.get("season", "").strip().strip('"'),
                "format": row.get("format", "test").strip().strip('"'),
                "max_innings": row.get("max_innings", "4").strip().strip('"'),
                "gender": row.get("gender", "").strip().strip('"'),
            }
            # Infer gender from name if not present in CSV
            if not entry["gender"]:
                entry["gender"] = _infer_gender_from_name(entry["name"])
            series[sid] = entry

    return series


def _infer_gender_from_name(name):
    """Infer gender from series name using keyword heuristics."""
    if not name:
        return "male"
    lower = name.lower()
    female_keywords = ("women", "female", "wbbl", "wpl", "wodi", "wt20",
                       "women's", "w t20", "w odi")
    if any(kw in lower for kw in female_keywords):
        return "female"
    return "male"


def merge_series(*sources):
    """Merge multiple series dicts, preferring non-empty values from later sources.

    Args:
        *sources: dicts keyed by series_id

    Returns:
        Merged dict keyed by series_id.
    """
    merged = {}
    for source in sources:
        for sid, entry in source.items():
            if sid not in merged:
                merged[sid] = dict(entry)
            else:
                # Later source fills in blanks but doesn't overwrite existing values
                existing = merged[sid]
                for key, val in entry.items():
                    if val and not existing.get(key):
                        existing[key] = val
    return merged


def write_csv_cache(series, csv_path):
    """Write full series dict to CSV (overwrites).

    Sorted by series_id descending (newest first).
    """
    path = Path(csv_path)
    entries = sorted(series.values(), key=lambda s: int(s.get("series_id", 0)), reverse=True)

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_ALL,
                                extrasaction="ignore")
        writer.writeheader()
        for entry in entries:
            # Ensure all fields have at least empty string
            row = {field: entry.get(field, "") for field in CSV_FIELDS}
            if not row["max_innings"]:
                row["max_innings"] = str(MAX_INNINGS.get(row.get("format"), 4))
            if not row["gender"]:
                row["gender"] = _infer_gender_from_name(row.get("name", ""))
            writer.writerow(row)


def build_series_list(csv_path, cricinfo_dir=None, web_discoveries=None):
    """Main entry point: build a merged series list from all available sources.

    Args:
        csv_path: Path to series_list.csv (read + write)
        cricinfo_dir: Path to cricinfo/ data dir for parquet scanning (optional)
        web_discoveries: Dict of series_id -> entry from web scraping (optional)

    Returns:
        Merged dict keyed by series_id.
    """
    # Source 1: Existing CSV cache (base layer)
    csv_cache = load_csv_cache(csv_path)

    # Source 2: Parquet scan (fills in any missing series from existing data)
    parquet_series = {}
    if cricinfo_dir:
        parquet_series = scan_parquets_for_series(cricinfo_dir)

    # Source 3: Web discoveries (freshest metadata)
    web = web_discoveries or {}

    # Merge: CSV base <- parquets <- web (later sources fill gaps)
    merged = merge_series(csv_cache, parquet_series, web)

    return merged
