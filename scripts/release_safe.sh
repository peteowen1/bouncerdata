#!/usr/bin/env bash
# release_safe.sh -- release-asset uploads that never delete before they
# replace, and restores that never read a failed download as "absent".
#
# VENDORED from pannadata/scripts/versebus.sh 1.3.0 (vb_sh_list_assets,
# vb_sh_safe_upload, vb_sh_restore, vb_sh_upload_all -- byte-identical; keep
# them in sync by diffing the two files), plus vb_sh_restore_all below, which
# is bouncerdata's own. Same temp-name scheme (vbnew-...--<name>) across
# verses, so a stranded copy reads the same everywhere.
#
# Source it from a workflow step: `source scripts/release_safe.sh`.
# Tests: scripts/tests/test_release_safe.sh.
RELEASE_SAFE_VERSION="1.0.0"

# Never `gh release upload --clobber`: clobber DELETES the existing asset
# before it uploads, so an upload that then fails (wheather, 2026-09-17: HTTP
# 500 on a 115 MB asset) leaves the release without the file. Every
# accumulating writer here reads its asset back next run, and a missing one
# reads as "first run" -- the next run publishes a cut-down file over the
# history. vb_sh_safe_upload keeps the old asset until a verified
# replacement is on the release. Audit: vault/plans/CLOBBER-AUDIT-2026-10-08.md.
VB_SH_TMP_PREFIX="vbnew-"

# vb_sh_list_assets <repo> <tag>
# Prints the release's asset array as JSON, trimmed to the fields used here.
# Non-zero if it can't be fetched. Trimmed because the full listing carries
# an uploader object per asset: opta-latest's 134 assets came to more than
# Linux's 128 KB limit for one environment string, and a caller that
# exported it broke every later exec ("Argument list too long", 2026-10-08).
# Keep VB_SH_ASSETS_JSON a plain shell variable; never export it.
vb_sh_list_assets() {
  gh api "repos/$1/releases/tags/$2" --jq '[.assets[] | {id, name, size, state, created_at}]'
}

# vb_sh_safe_upload <repo> <tag> <file>
# Uploads <file> under a temporary asset name (vbnew-<run>-<attempt>-<pid>--
# <name>), confirms it reached state "uploaded" at the local byte size, and
# only then deletes the old <name> (plus any temp copies left by earlier
# runs) and renames the new asset to <name>. Prints "OK <name>" or
# "FAIL <name> <why>". Failure before the delete leaves the old asset
# untouched; failure of the final rename leaves the data on the release
# under its temp name, where vb_sh_restore finds it.
vb_sh_safe_upload() {
  local repo="$1" tag="$2" f="$3"
  local name size tmpname tmpd assets new_id attempt id old_ids
  name=$(basename "$f")
  [ -f "$f" ] || { echo "FAIL $name local file missing"; return 1; }
  size=$(stat -c%s "$f")
  # Unique per call: a second upload of the same file in one shell must not
  # collide with a temp copy the first one stranded.
  tmpname="${VB_SH_TMP_PREFIX}${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$${RANDOM}${RANDOM}--${name}"

  # gh names an asset after the file's basename, so stage it under the temp
  # name. Hard link where possible: the events files run to ~300 MB.
  tmpd=$(mktemp -d) || { echo "FAIL $name could not create a staging dir"; return 1; }
  ln "$f" "$tmpd/$tmpname" 2>/dev/null || cp "$f" "$tmpd/$tmpname" || {
    rm -rf "$tmpd"; echo "FAIL $name could not stage $tmpname"; return 1; }
  if ! gh release upload "$tag" "$tmpd/$tmpname" --repo "$repo"; then
    rm -rf "$tmpd"
    echo "FAIL $name upload failed (old asset untouched)"
    return 1
  fi
  rm -rf "$tmpd"

  # The listing can lag an upload by seconds (versebus.R saw a stale size
  # for ~95s on 2026-07-16), so poll before declaring the upload bad.
  new_id=""
  for attempt in 1 2 3 4 5; do
    if assets=$(vb_sh_list_assets "$repo" "$tag"); then
      # A jq failure leaves new_id empty, which only ever means "keep the old
      # asset" -- never a delete.
      new_id=$(jq -r --arg n "$tmpname" --argjson s "$size" \
        'first(.[] | select(.name == $n and .state == "uploaded" and .size == $s) | .id) // empty' <<<"$assets") || new_id=""
      [ -n "$new_id" ] && break
    fi
    [ "$attempt" -lt 5 ] && sleep $((attempt * 3))
  done
  if [ -z "$new_id" ]; then
    echo "FAIL $name $tmpname never listed as uploaded at $size bytes (old asset untouched)"
    return 1
  fi

  # Delete the old asset and any stale temp copies of it. A failed delete of
  # the real <name> stops here: the rename below would collide with it.
  if ! old_ids=$(jq -r --arg n "$name" --arg t "$tmpname" --arg p "$VB_SH_TMP_PREFIX" \
    '.[] | select(.name == $n or (.name != $t and (.name | startswith($p)) and (.name | endswith("--" + $n)))) | "\(.id) \(.name)"' <<<"$assets"); then
    echo "FAIL $name could not read the listing to find the old asset; new copy left as $tmpname"
    return 1
  fi
  while read -r id old_name; do
    [ -n "$id" ] || continue
    if ! gh api -X DELETE "repos/${repo}/releases/assets/${id}" >/dev/null; then
      if [ "$old_name" = "$name" ]; then
        echo "FAIL $name could not delete the old asset; new copy left as $tmpname"
        return 1
      fi
      echo "::warning::could not delete stale temp asset $old_name" >&2
    fi
  done <<<"$old_ids"

  for attempt in 1 2 3; do
    if gh api -X PATCH "repos/${repo}/releases/assets/${new_id}" -f name="$name" >/dev/null; then
      echo "OK $name"
      return 0
    fi
    [ "$attempt" -lt 3 ] && sleep $((attempt * 5))
  done
  echo "FAIL $name rename failed; the data is on the release as $tmpname (vb_sh_restore falls back to it)"
  return 1
}

# vb_sh_restore <repo> <tag> <name> <dir>
# Downloads <name> into <dir>. If <name> is absent but an unswapped temp
# copy from vb_sh_safe_upload is on the release, downloads the newest one
# and saves it as <dir>/<name>. Checks the downloaded size against the
# listing. Uses $VB_SH_ASSETS_JSON as the listing when set (callers that
# restore many files list once). Returns:
#   0  restored
#   2  not on the release at all (neither the name nor a temp copy)
#   1  on the release but could not be downloaded intact -- callers must
#      treat this as fatal, never as "absent"
vb_sh_restore() {
  local repo="$1" tag="$2" name="$3" dir="$4"
  local assets src want attempt got
  assets="${VB_SH_ASSETS_JSON:-}"
  if [ -z "$assets" ]; then
    assets=$(vb_sh_list_assets "$repo" "$tag") || return 1
  fi
  # Every jq failure here returns 1: an empty answer from a broken jq must
  # never read as "absent" (it did once, see vb_sh_list_assets).
  src=$(jq -r --arg n "$name" \
    '[.[] | select(.state == "uploaded" and .name == $n)] | .[0].name // empty' <<<"$assets") || return 1
  if [ -z "$src" ]; then
    # Listed under its real name but not "uploaded" (a killed upload) is a
    # broken asset, not an absent one.
    local half
    half=$(jq -r --arg n "$name" '[.[] | select(.name == $n)] | length' <<<"$assets") || return 1
    if [ "$half" != "0" ]; then
      echo "::error::$name is on ${repo}@${tag} but not in state uploaded" >&2
      return 1
    fi
    src=$(jq -r --arg n "$name" --arg p "$VB_SH_TMP_PREFIX" \
      '[.[] | select(.state == "uploaded" and (.name | startswith($p)) and (.name | endswith("--" + $n)))]
       | sort_by(.created_at) | last | .name // empty' <<<"$assets") || return 1
    [ -n "$src" ] && echo "::warning::$name is missing from ${repo}@${tag}; restoring the unswapped upload $src" >&2
  fi
  [ -n "$src" ] || return 2
  want=$(jq -r --arg n "$src" '[.[] | select(.name == $n)] | .[0].size' <<<"$assets") || return 1

  mkdir -p "$dir" || return 1
  for attempt in 1 2 3; do
    if gh release download "$tag" --repo "$repo" --pattern "$src" --dir "$dir" --clobber; then
      got=$(stat -c%s "$dir/$src" 2>/dev/null || echo -1)
      if [ "$got" = "$want" ]; then
        [ "$src" = "$name" ] || mv -f "$dir/$src" "$dir/$name" || return 1
        return 0
      fi
      echo "::warning::$src downloaded at $got bytes, listing says $want (attempt $attempt)" >&2
    fi
    [ "$attempt" -lt 3 ] && sleep $((attempt * 5))
  done
  return 1
}

# vb_sh_upload_all <repo> <tag> <file> [<file> ...]
# vb_sh_safe_upload on each file, one at a time. Prints "OK <name>" or
# "FAIL <name> ..." per file to stdout -- the caller greps/counts failures
# (panna's epv-pipeline.yml greps '^FAIL'). Never aborts on an individual
# failure and always returns 0: callers run `out=$(vb_sh_upload_all ...)`
# under `set -e`, where a non-zero return would kill the step before the
# FAIL lines are read. The caller decides whether to gate downstream steps
# (verify, manifest) on the failure count.
vb_sh_upload_all() {
  local repo="$1" tag="$2"; shift 2
  local f
  for f in "$@"; do
    [ -f "$f" ] || continue
    vb_sh_safe_upload "$repo" "$tag" "$f" || true
  done
  return 0
}

# vb_sh_restore_all <repo> <tag> <dir> <suffix>
# Restores EVERY asset on the release whose name ends in <suffix> into <dir>,
# and proves it: each file must arrive at the size the listing gives. A bulk
# download goes first (fast); anything it missed or truncated is retried one
# by one through vb_sh_restore. A data file stranded under a temp name by an
# interrupted vb_sh_safe_upload rename is restored under its real name.
# Uses $VB_SH_ASSETS_JSON as the listing when set. Prints a summary and
# returns 0 only when every listed file is present and the right size --
# never treat a non-zero return as "nothing to restore".
vb_sh_restore_all() {
  local repo="$1" tag="$2" dir="$3" suffix="$4"
  local assets names n want got failed=0 restored=0
  assets="${VB_SH_ASSETS_JSON:-}"
  if [ -z "$assets" ]; then
    assets=$(vb_sh_list_assets "$repo" "$tag") || { echo "::error::could not list ${repo}@${tag}"; return 1; }
  fi
  # Real names, plus the real name of any stranded temp copy whose real name
  # is no longer listed.
  names=$(jq -r --arg s "$suffix" --arg p "$VB_SH_TMP_PREFIX" '
    [.[] | select(.state == "uploaded") | .name] as $all
    | ($all | map(select((startswith($p) | not) and endswith($s)))) as $real
    | ($all | map(select(startswith($p)) | sub("^[^-]+-[^-]+-[^-]+-[^-]+--"; "") | select(endswith($s)))) as $tmp
    | ($real + ($tmp - $real)) | unique | .[]' <<<"$assets") || { echo "::error::could not read the ${repo}@${tag} listing"; return 1; }

  mkdir -p "$dir" || return 1
  if [ -n "$names" ]; then
    gh release download "$tag" --repo "$repo" --pattern "*${suffix}" --dir "$dir" --clobber \
      || echo "::warning::bulk download of ${repo}@${tag} did not complete; checking every file individually"
  fi

  while IFS= read -r n; do
    [ -n "$n" ] || continue
    want=$(jq -r --arg n "$n" '[.[] | select(.name == $n and .state == "uploaded")] | .[0].size // empty' <<<"$assets")
    got=$(stat -c%s "$dir/$n" 2>/dev/null || echo -1)
    if [ -n "$want" ] && [ "$got" = "$want" ]; then
      restored=$((restored + 1))
      continue
    fi
    if VB_SH_ASSETS_JSON="$assets" vb_sh_restore "$repo" "$tag" "$n" "$dir"; then
      restored=$((restored + 1))
    else
      echo "::error::$n is on ${repo}@${tag} but could not be restored intact"
      failed=$((failed + 1))
    fi
  done <<<"$names"

  # Temp copies the bulk download pulled in under their temp names.
  rm -f "$dir/${VB_SH_TMP_PREFIX}"* 2>/dev/null
  echo "vb_sh_restore_all: $restored restored, $failed failed (${repo}@${tag}, *${suffix})"
  [ "$failed" -eq 0 ]
}
