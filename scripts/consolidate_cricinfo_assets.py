#!/usr/bin/env python3
"""One-time consolidation migration for the `cricinfo` GitHub release.

CONTEXT (see FABLE-REVIEW.md finding H7, follow-up review 2026-07-10)
----------------------------------------------------------------------
cricinfo-daily.yml uploads 3 per-match parquet assets per newly-scraped match
(`{format}_{gender}__{match_id}_{balls,match,innings}.parquet`) IN ADDITION TO
18 "combined" bundle assets (`cricinfo_{table}_{format}_{gender}.parquet`, one
per format/gender/table-type) that already contain every match's data merged
together (keyed on `match_id`). By 2026-07-10 the release held 985/1000 assets
-- almost entirely per-match files -- because nothing ever prunes them.

The combined/bundle assets are the ONLY assets any consumer actually reads
remotely (`bouncer::load_cricinfo_remote()` in bouncer/R/cricinfo_data.R only
ever downloads `cricinfo_*.parquet`). Per-match assets exist purely so the
next day's workflow run can restore local per-match files -- which the
scraper uses only to check "have I already scraped this match?"
(`_get_scraped_match_ids()` in cricinfo_scraper.py, a filesystem scan for
`*_balls.parquet`). That check only needs FILE PRESENCE, not content, and the
combined parquets already carry every match's `match_id` -- so once a
per-match asset's match_id is verified present in the corresponding combined
asset, the per-match asset is pure redundancy that only costs cap headroom.

This script is a MANUAL, human-run migration. It is NOT invoked by any
workflow and never runs automatically. It:

  1. Lists all assets currently on the release.
  2. Downloads the 18 combined/bundle assets + fixtures.parquet (small; a few
     hundred MB at most) and reads each bundle's `match_id` column.
  3. Classifies every per-match asset as:
       SAFE        -- its match_id is present in the matching combined bundle
                       for its (format, gender, table_type) -- safe to delete.
       UNSAFE      -- no matching bundle found, or match_id missing from it
                       -- NEVER deleted; needs investigation (that match's
                       data may only exist in the per-match asset).
       SKIP_RECENT -- updated within --min-age-hours (default 24h); skipped
                       so we never race a concurrently-running scrape.
  4. Writes a full JSON audit report to disk (always, even in dry-run mode).
  5. In dry-run mode (the default): reports counts only, deletes nothing.
  6. In --execute mode (requires --yes too): deletes SAFE assets one at a
     time via `gh release delete-asset`, logging every deletion to the audit
     report. Bundle/fixtures assets are structurally excluded from deletion
     (the regexes that classify SAFE candidates only ever match the
     per-match naming pattern).

WHY THIS DOESN'T FIX THE ROOT CAUSE BY ITSELF
----------------------------------------------
This migration only reclaims headroom that already exists. The workflow will
still upload 3 new per-match assets per newly-scraped match going forward, so
the count will climb back toward the cap over time (just much more slowly,
since it currently sits at ~965 per-match assets and this migration should
remove nearly all of them). Permanently stopping per-match asset growth
requires restoring the scraper's local "already scraped" state from the
combined bundles instead of from per-match release assets on every run --
that is a deeper workflow rework (see FABLE-REVIEW.md's "Deeper refactors"
list, C4/H7/H9/M11) and is deliberately OUT OF SCOPE for this script. Re-run
this migration periodically (or once that rework lands) to stay under the
950-asset guard added to cricinfo-daily.yml.

PREREQUISITES
--------------
- `gh` CLI installed and authenticated with access to peteowen1/bouncerdata
  (`gh auth status`), with `repo` scope (asset deletion needs write access).
- Python packages: pyarrow (already in scripts/requirements-cricinfo.txt).
- Run from anywhere; the script only uses temp directories.

USAGE
-----
    # 1. Dry run first. ALWAYS do this before --execute. Review the report.
    python scripts/consolidate_cricinfo_assets.py

    # 2. Inspect the JSON report it writes (path printed at the end), especially
    #    the "unsafe" section -- every entry there is a match NOT deleted.

    # 3. Execute for real, in small batches to start (recommended):
    python scripts/consolidate_cricinfo_assets.py --execute --yes --batch-size 100

    # 4. Re-run (dry-run, then --execute) until "safe" count reaches 0, or run
    #    with no --batch-size to do it all in one pass once you trust it:
    python scripts/consolidate_cricinfo_assets.py --execute --yes

Flags:
    --repo OWNER/REPO      Default: peteowen1/bouncerdata
    --tag TAG               Release tag. Default: cricinfo
    --execute                Actually delete SAFE assets (default: dry-run/report only)
    --yes                    Required alongside --execute as an explicit confirmation
    --batch-size N            Delete at most N assets this run (default: unlimited)
    --min-age-hours N          Skip assets updated within the last N hours (default: 24)
    --report-dir PATH          Where to write the JSON audit report (default: cwd)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required (pip install -r scripts/requirements-cricinfo.txt)", file=sys.stderr)
    sys.exit(1)


FORMATS = ("t20i", "odi", "test")
GENDERS = ("male", "female")
TABLE_TYPES = ("balls", "match", "innings")

# {format}_{gender}__{match_id}_{table_type}.parquet
PER_MATCH_RE = re.compile(
    r"^(?P<format>t20i|odi|test)_(?P<gender>male|female)__(?P<match_id>\d+)_(?P<table>balls|match|innings)\.parquet$"
)
# Legacy backwards-compat naming the workflow's restore step still handles:
# {format}__{match_id}_{table_type}.parquet (no gender, assumed male)
PER_MATCH_LEGACY_RE = re.compile(
    r"^(?P<format>t20i|odi|test)__(?P<match_id>\d+)_(?P<table>balls|match|innings)\.parquet$"
)
# cricinfo_{table_type}_{format}_{gender}.parquet
BUNDLE_RE = re.compile(
    r"^cricinfo_(?P<table>balls|match|innings)_(?P<format>t20i|odi|test)_(?P<gender>male|female)\.parquet$"
)
FIXTURES_NAME = "fixtures.parquet"


def run_gh(args, **kwargs):
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True, **kwargs)


def list_release_assets(repo: str, tag: str) -> list[dict]:
    result = run_gh(["api", f"repos/{repo}/releases/tags/{tag}"])
    data = json.loads(result.stdout)
    return data["assets"]


def download_bundle_assets(repo: str, tag: str, dest_dir: Path) -> None:
    """Download only the combined/bundle assets + fixtures.parquet (small)."""
    run_gh([
        "release", "download", tag,
        "--repo", repo,
        "-D", str(dest_dir),
        "--pattern", "cricinfo_*.parquet",
        "--clobber",
    ])
    try:
        run_gh([
            "release", "download", tag,
            "--repo", repo,
            "-D", str(dest_dir),
            "--pattern", FIXTURES_NAME,
            "--clobber",
        ])
    except subprocess.CalledProcessError:
        pass  # fixtures.parquet may not exist; not required for this script


def load_bundle_match_ids(bundle_dir: Path) -> dict[tuple[str, str, str], set[str]]:
    """Returns {(format, gender, table_type): {match_id, ...}} read from bundles."""
    bundle_ids: dict[tuple[str, str, str], set[str]] = {}
    for f in bundle_dir.glob("cricinfo_*.parquet"):
        m = BUNDLE_RE.match(f.name)
        if not m:
            continue
        key = (m.group("format"), m.group("gender"), m.group("table"))
        try:
            col = pq.read_table(f, columns=["match_id"]).column("match_id")
        except Exception as e:
            print(f"  WARNING: could not read match_id from {f.name}: {e}", file=sys.stderr)
            continue
        bundle_ids[key] = {str(x) for x in col.to_pylist()}
        print(f"  Bundle {f.name}: {len(bundle_ids[key]):,} match_ids")
    return bundle_ids


def classify_assets(assets: list[dict], bundle_ids: dict, min_age_hours: int):
    now = dt.datetime.now(dt.timezone.utc)
    safe, unsafe, skip_recent, kept = [], [], [], []

    for a in assets:
        name = a["name"]
        m = PER_MATCH_RE.match(name)
        gender = None
        if m:
            fmt, gender, match_id, table = m.group("format"), m.group("gender"), m.group("match_id"), m.group("table")
        else:
            m2 = PER_MATCH_LEGACY_RE.match(name)
            if m2:
                fmt, match_id, table = m2.group("format"), m2.group("match_id"), m2.group("table")
                gender = "male"
            else:
                # Not a per-match asset (bundle, fixtures, or unrecognized) -- never touch it.
                kept.append({"name": name, "reason": "not a per-match asset (bundle/fixtures/unrecognized)"})
                continue

        updated_at = a.get("updated_at")
        if updated_at:
            updated_dt = dt.datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            age_hours = (now - updated_dt).total_seconds() / 3600.0
            if age_hours < min_age_hours:
                skip_recent.append({"name": name, "match_id": match_id, "age_hours": round(age_hours, 1)})
                continue

        key = (fmt, gender, table)
        ids = bundle_ids.get(key)
        if ids is not None and match_id in ids:
            safe.append({
                "name": name, "format": fmt, "gender": gender,
                "match_id": match_id, "table": table,
                "size": a.get("size"), "updated_at": updated_at,
            })
        else:
            reason = "no bundle found for (format,gender,table)" if ids is None else "match_id not present in bundle"
            unsafe.append({"name": name, "format": fmt, "gender": gender, "match_id": match_id, "table": table, "reason": reason})

    return safe, unsafe, skip_recent, kept


def delete_assets(repo: str, tag: str, safe: list[dict], batch_size: int | None) -> tuple[list[str], list[dict]]:
    deleted, failed = [], []
    targets = safe if batch_size is None else safe[:batch_size]
    for entry in targets:
        name = entry["name"]
        # Defensive: never delete anything that isn't a verified per-match asset name.
        if not (PER_MATCH_RE.match(name) or PER_MATCH_LEGACY_RE.match(name)):
            failed.append({"name": name, "error": "refused: name did not match per-match pattern"})
            continue
        try:
            run_gh(["release", "delete-asset", tag, name, "--repo", repo, "--yes"])
            deleted.append(name)
            print(f"  Deleted {name}")
        except subprocess.CalledProcessError as e:
            failed.append({"name": name, "error": e.stderr.strip() if e.stderr else str(e)})
            print(f"  FAILED to delete {name}: {e.stderr}", file=sys.stderr)
    return deleted, failed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default="peteowen1/bouncerdata")
    parser.add_argument("--tag", default="cricinfo")
    parser.add_argument("--execute", action="store_true", help="Actually delete SAFE assets (default: dry-run)")
    parser.add_argument("--yes", action="store_true", help="Required alongside --execute to confirm")
    parser.add_argument("--batch-size", type=int, default=None, help="Delete at most N assets this run")
    parser.add_argument("--min-age-hours", type=int, default=24, help="Skip assets updated more recently than this")
    parser.add_argument("--report-dir", default=".", help="Directory to write the JSON audit report")
    args = parser.parse_args()

    if args.execute and not args.yes:
        print("ERROR: --execute requires --yes (explicit confirmation this touches the LIVE release).", file=sys.stderr)
        sys.exit(1)

    print(f"Listing assets on {args.repo} release '{args.tag}'...")
    assets = list_release_assets(args.repo, args.tag)
    print(f"  {len(assets)} assets currently on the release")

    tmp_dir = Path(tempfile.mkdtemp(prefix="cricinfo_consolidate_"))
    try:
        print("Downloading combined/bundle assets to verify against...")
        download_bundle_assets(args.repo, args.tag, tmp_dir)
        bundle_ids = load_bundle_match_ids(tmp_dir)
        if not bundle_ids:
            print("ERROR: no combined bundle assets could be read — refusing to classify anything as safe to delete.", file=sys.stderr)
            sys.exit(1)

        safe, unsafe, skip_recent, kept = classify_assets(assets, bundle_ids, args.min_age_hours)

        print("\n--- Classification summary ---")
        print(f"  SAFE to delete (verified in a bundle):  {len(safe)}")
        print(f"  UNSAFE (needs investigation, kept):      {len(unsafe)}")
        print(f"  SKIP_RECENT (< {args.min_age_hours}h old, kept): {len(skip_recent)}")
        print(f"  Bundles/fixtures/unrecognized (kept):    {len(kept)}")
        if unsafe:
            print("\n  First 20 UNSAFE assets (NOT deleted, investigate these):")
            for e in unsafe[:20]:
                print(f"    {e['name']}: {e['reason']}")

        report = {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "repo": args.repo,
            "tag": args.tag,
            "total_assets_before": len(assets),
            "min_age_hours": args.min_age_hours,
            "executed": False,
            "safe_count": len(safe),
            "unsafe_count": len(unsafe),
            "skip_recent_count": len(skip_recent),
            "safe": safe,
            "unsafe": unsafe,
            "skip_recent": skip_recent,
        }

        if args.execute:
            n_to_delete = len(safe) if args.batch_size is None else min(args.batch_size, len(safe))
            print(f"\n--execute set: deleting {n_to_delete} of {len(safe)} SAFE assets from the LIVE release now...")
            deleted, failed = delete_assets(args.repo, args.tag, safe, args.batch_size)
            report["executed"] = True
            report["deleted"] = deleted
            report["deleted_count"] = len(deleted)
            report["delete_failed"] = failed
            print(f"\nDeleted {len(deleted)} assets, {len(failed)} failures.")
            print(f"Release asset count should now be approximately {len(assets) - len(deleted)}.")
        else:
            print("\nDry run only — nothing was deleted. Re-run with --execute --yes once you've reviewed the report.")

        report_path = Path(args.report_dir) / f"cricinfo_consolidation_report_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        report_path.write_text(json.dumps(report, indent=2))
        print(f"\nFull audit report written to: {report_path}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
