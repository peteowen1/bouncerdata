---
name: consolidate-cricinfo-assets
description: Consolidate/dedupe Cricinfo GitHub release assets when the asset cap is being approached. Use when asked to clean up cricinfo assets, consolidate cricinfo releases, or when a release nears its asset-count limit.
---

## Cricinfo asset cap consolidation

GitHub caps releases at 1000 assets. Per-match Cricinfo uploads (3 assets/match, never pruned) pushed the `cricinfo` release to 985/1000 by 2026-07-10, one bad week away from silently dropping new matches and forcing daily re-scrapes.

`cricinfo-daily.yml` has an early "Check cricinfo release asset-count headroom" step that fails loudly (with a pointer to `bouncerdata/CLAUDE.md`) once the release hits 950 assets, and per-file upload failures fail the job instead of warning.

Only the 18 combined bundle assets + `fixtures.parquet` are ever read remotely (`bouncer::load_cricinfo_remote()`); per-match assets exist solely so the next day's run can restore local "already scraped" state. This means old per-match assets are safe to delete once their `match_id` is verified present in the matching bundle.

**When the guard trips**, run the one-time consolidation migration manually (never automated, never run by CI):

```bash
# 1. Dry run — review the JSON report it writes, especially "unsafe" entries
python scripts/consolidate_cricinfo_assets.py

# 2. Execute for real once you trust the report (start with a small batch)
python scripts/consolidate_cricinfo_assets.py --execute --yes --batch-size 100

# 3. Repeat (dry-run, then --execute) until the guard has enough headroom again
python scripts/consolidate_cricinfo_assets.py --execute --yes
```

See `scripts/consolidate_cricinfo_assets.py`'s module docstring for full details (classification logic, safety checks, `--min-age-hours` protection against racing a concurrent scrape).

This migration only reclaims existing headroom — it does not stop per-match assets from growing again over time. Permanently fixing that requires restoring the scraper's "already scraped" state from the combined bundles instead of from per-match release assets each run — tracked as a deeper refactor in `../docs/reviews/FABLE-REVIEW.md` (H7/C4/H9/M11), not yet implemented.
