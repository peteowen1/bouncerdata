#!/usr/bin/env bash
# scripts/tests/test_release_safe.sh -- tests for scripts/release_safe.sh.
# The fake release and the swap/restore cases are copied from
# pannadata/scripts/tests/test_versebus.sh (the functions they test are
# vendored byte-identical); the vb_sh_restore_all cases are bouncerdata's own.
# Plain bash, no network: `gh` is a shell function backed by real files.
#
# Run: bash scripts/tests/test_release_safe.sh

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../release_safe.sh"

pass_count=0
fail_count=0
pass() { echo "PASS: $1"; pass_count=$((pass_count + 1)); }
fail() { echo "FAIL: $1"; fail_count=$((fail_count + 1)); }
check() { local desc="$1"; shift; if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi; }
check_not() { local desc="$1"; shift; if "$@" >/dev/null 2>&1; then fail "$desc"; else pass "$desc"; fi; }

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
echo "dummy content" > "$tmpdir/a.parquet"

# No real waiting in tests (safe upload / restore back off between retries).
sleep() { :; }
# Windows jq emits CRLF, which breaks the size comparisons; Linux CI is
# unaffected. pipefail keeps jq's own exit status.
jq() { command jq "$@" | tr -d '\r'; }

# ---------------------------------------------------------------------------
# Fake release: a stateful `gh` that keeps assets as real files plus a JSON
# listing, so the upload -> list -> delete -> rename and restore paths run
# for real. Failure switches (set to 1 to trigger):
#   FAKE_UPLOAD_FAIL FAKE_LIST_FAIL FAKE_PATCH_FAIL FAKE_DELETE_FAIL
#   FAKE_DOWNLOAD_FAIL FAKE_DOWNLOAD_TRUNC FAKE_UPLOAD_STUCK (upload lands
#   in state "starter", i.e. never completes)
# ---------------------------------------------------------------------------
fake_reset() {
  FAKE_DIR="$tmpdir/fake_release_$RANDOM$RANDOM"
  mkdir -p "$FAKE_DIR/files"
  echo '[]' > "$FAKE_DIR/assets.json"
  FAKE_NEXT_ID=1
  FAKE_UPLOAD_FAIL=0 FAKE_LIST_FAIL=0 FAKE_PATCH_FAIL=0 FAKE_DELETE_FAIL=0
  FAKE_DOWNLOAD_FAIL=0 FAKE_DOWNLOAD_TRUNC=0 FAKE_UPLOAD_STUCK=0
}
fake_put() {  # fake_put <name> <content> -- seed an asset directly
  printf '%s' "$2" > "$FAKE_DIR/files/$1"
  _fake_add "$1" "$(stat -c%s "$FAKE_DIR/files/$1")" uploaded
}
_fake_add() {
  local tmp="$FAKE_DIR/assets.tmp"
  command jq --arg n "$1" --argjson s "$2" --arg st "$3" --argjson id "$FAKE_NEXT_ID" \
    '. + [{id: $id, name: $n, size: $s, state: $st, created_at: ("2026-10-08T00:00:" + ($id | tostring | if length < 2 then "0" + . else . end) + "Z")}]' \
    "$FAKE_DIR/assets.json" > "$tmp" && mv "$tmp" "$FAKE_DIR/assets.json"
  FAKE_NEXT_ID=$((FAKE_NEXT_ID + 1))
}
fake_names() { command jq -r '[.[].name] | sort | join(" ")' "$FAKE_DIR/assets.json" | tr -d '\r'; }
fake_content() { cat "$FAKE_DIR/files/$1" 2>/dev/null; }

gh() {
  local tmp="$FAKE_DIR/assets.tmp"
  if [ "$1" = "api" ] && [ "$2" = "-X" ]; then
    local method="$3" id="${4##*/}"
    local name
    name=$(command jq -r --argjson id "$id" '.[] | select(.id == $id) | .name' "$FAKE_DIR/assets.json" | tr -d '\r')
    [ -n "$name" ] || return 1
    if [ "$method" = "DELETE" ]; then
      [ "$FAKE_DELETE_FAIL" = 1 ] && return 1
      rm -f "$FAKE_DIR/files/$name"
      command jq --argjson id "$id" 'map(select(.id != $id))' "$FAKE_DIR/assets.json" > "$tmp" && mv "$tmp" "$FAKE_DIR/assets.json"
      return 0
    elif [ "$method" = "PATCH" ]; then
      [ "$FAKE_PATCH_FAIL" = 1 ] && return 1
      local new="${6#name=}"
      mv "$FAKE_DIR/files/$name" "$FAKE_DIR/files/$new"
      command jq --argjson id "$id" --arg n "$new" 'map(if .id == $id then .name = $n else . end)' "$FAKE_DIR/assets.json" > "$tmp" && mv "$tmp" "$FAKE_DIR/assets.json"
      echo '{}'
      return 0
    fi
    return 1
  elif [ "$1" = "api" ]; then
    [ "$FAKE_LIST_FAIL" = 1 ] && return 1
    cat "$FAKE_DIR/assets.json"
    return 0
  elif [ "$1" = "release" ] && [ "$2" = "upload" ]; then
    [ "$FAKE_UPLOAD_FAIL" = 1 ] && return 1
    local f="$4" name
    name=$(basename "$f")
    cp "$f" "$FAKE_DIR/files/$name"
    if [ "$FAKE_UPLOAD_STUCK" = 1 ]; then
      _fake_add "$name" "$(stat -c%s "$f")" starter
    else
      _fake_add "$name" "$(stat -c%s "$f")" uploaded
    fi
    return 0
  elif [ "$1" = "release" ] && [ "$2" = "download" ]; then
    [ "$FAKE_DOWNLOAD_FAIL" = 1 ] && return 1
    local pattern="" dir="" prev=""
    for a in "$@"; do
      [ "$prev" = "--pattern" ] && pattern="$a"
      [ "$prev" = "--dir" ] && dir="$a"
      prev="$a"
    done
    if [[ "$pattern" == *"*"* ]]; then
      # Bulk glob download. FAKE_BULK_SKIP names one file to "lose" mid-batch
      # (gh then exits non-zero, like a real partial failure).
      local g rc=0
      for g in "$FAKE_DIR/files/"$pattern; do
        [ -f "$g" ] || continue
        if [ "$(basename "$g")" = "${FAKE_BULK_SKIP:-}" ]; then rc=1; continue; fi
        if [ "$FAKE_DOWNLOAD_TRUNC" = 1 ]; then
          head -c 1 "$g" > "$dir/$(basename "$g")"
        else
          cp "$g" "$dir/"
        fi
      done
      return $rc
    fi
    [ -f "$FAKE_DIR/files/$pattern" ] || return 1
    if [ "$FAKE_DOWNLOAD_TRUNC" = 1 ]; then
      head -c 1 "$FAKE_DIR/files/$pattern" > "$dir/$pattern"
    else
      cp "$FAKE_DIR/files/$pattern" "$dir/$pattern"
    fi
    return 0
  fi
  echo "unexpected gh invocation: $*" >&2
  return 1
}
# Keep a copy so a section can swap in a one-off gh and switch back.
eval "$(declare -f gh | sed '1s/^gh /gh_fake /')"
fake_reset

# ---------------------------------------------------------------------------
# 6. vb_sh_safe_upload / vb_sh_restore: the old asset must survive every
#    failure before the swap (audit: vault/plans/CLOBBER-AUDIT-2026-10-08.md).
# ---------------------------------------------------------------------------
echo "new content" > "$tmpdir/a.parquet"

# 6a. Happy path: replaced in place, one asset, no temp leftovers.
fake_reset
fake_put a.parquet "old content"
check "safe upload succeeds over an existing asset" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_names)" = "a.parquet" ] && [ "$(fake_content a.parquet)" = "new content" ]; then
  pass "safe upload: one asset named a.parquet holding the new content"
else
  fail "safe upload: release holds '$(fake_names)', content '$(fake_content a.parquet)'"
fi

# 6b. Upload fails -> old asset untouched.
fake_reset
fake_put a.parquet "old content"
FAKE_UPLOAD_FAIL=1
check_not "safe upload returns non-zero when the upload fails" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_names)" = "a.parquet" ] && [ "$(fake_content a.parquet)" = "old content" ]; then
  pass "failed upload leaves the old asset in place"
else
  fail "failed upload changed the release: '$(fake_names)'"
fi

# 6c. Upload never completes (state stays "starter") -> old asset untouched.
fake_reset
fake_put a.parquet "old content"
FAKE_UPLOAD_STUCK=1
check_not "safe upload returns non-zero when the upload never completes" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_content a.parquet)" = "old content" ]; then
  pass "incomplete upload leaves the old asset in place"
else
  fail "incomplete upload deleted or changed the old asset"
fi

# 6d. Deleting the old asset fails -> old asset untouched, FAIL.
fake_reset
fake_put a.parquet "old content"
FAKE_DELETE_FAIL=1
check_not "safe upload returns non-zero when the old asset can't be deleted" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_content a.parquet)" = "old content" ]; then
  pass "failed delete leaves the old asset in place"
else
  fail "failed delete still lost the old asset"
fi

# 6e. Rename fails after the delete -> FAIL, and vb_sh_restore recovers the
#     new data from the temp copy.
fake_reset
fake_put a.parquet "old content"
FAKE_PATCH_FAIL=1
check_not "safe upload returns non-zero when the final rename fails" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
FAKE_PATCH_FAIL=0
restore_dir="$tmpdir/restore_6e"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$restore_dir" >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 0 ] && [ "$(cat "$restore_dir/a.parquet" 2>/dev/null)" = "new content" ]; then
  pass "restore falls back to the unswapped temp copy"
else
  fail "restore after a failed rename: rc=$rc, content '$(cat "$restore_dir/a.parquet" 2>/dev/null)'"
fi

# 6f. The next successful upload clears the stranded temp copy.
check "safe upload succeeds after a stranded temp copy" \
  vb_sh_safe_upload "test/fixture" "test-tag" "$tmpdir/a.parquet"
if [ "$(fake_names)" = "a.parquet" ]; then
  pass "stranded temp copy cleaned up by the next upload"
else
  fail "release still holds '$(fake_names)' after the next upload"
fi

# 6g. vb_sh_restore return codes: absent -> 2, download failure -> 1,
#     truncated download -> 1, never 0 on a bad file.
fake_reset
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 2 ] && pass "restore returns 2 for an absent asset" || fail "restore returned $rc for an absent asset, expected 2"
fake_put a.parquet "old content"
FAKE_DOWNLOAD_FAIL=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 when a listed asset can't be downloaded" || fail "restore returned $rc on a download failure, expected 1"
FAKE_DOWNLOAD_FAIL=0; FAKE_DOWNLOAD_TRUNC=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 on a truncated download" || fail "restore returned $rc on a truncated download, expected 1"
FAKE_DOWNLOAD_TRUNC=0; FAKE_LIST_FAIL=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 when the listing fails (never 'absent')" || fail "restore returned $rc on a listing failure, expected 1"
# An asset under its real name in a non-uploaded state (killed upload) is
# broken, not absent.
rc=0; VB_SH_ASSETS_JSON='[{"id":1,"name":"a.parquet","size":0,"state":"starter","created_at":"x"}]' \
  vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 for a half-uploaded asset (never 'absent')" || fail "restore returned $rc for a half-uploaded asset, expected 1"
# A listing jq can't parse (the 2026-10-08 dev dry run: jq died with
# "Argument list too long" and the old code read that as absent).
FAKE_LIST_FAIL=0
rc=0; VB_SH_ASSETS_JSON="not json" vb_sh_restore "test/fixture" "test-tag" a.parquet "$tmpdir/r6g" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ] && pass "restore returns 1 when jq fails on the listing (never 'absent')" || fail "restore returned $rc when jq failed, expected 1"

# 6g2. The listing is trimmed to the fields used, so it stays far below the
#      128 KB per-variable limit even for opta-latest's 134 assets.
#      This fake returns the raw release object (with an uploader per asset,
#      as GitHub does) and applies the caller's --jq, like gh api.
gh() {
  command jq -c '{assets: map(. + {uploader: {login: "x", id: 1}, label: ""})}' "$FAKE_DIR/assets.json" \
    | command jq -c "$4"
}
listing=$(vb_sh_list_assets "test/fixture" "test-tag")
if command jq -e 'length == 1 and all(.[]; (keys | sort) == ["created_at","id","name","size","state"])' <<<"$listing" >/dev/null; then
  pass "vb_sh_list_assets keeps only id/name/size/state/created_at"
else
  fail "vb_sh_list_assets returned extra or missing fields: $listing"
fi
gh() { gh_fake "$@"; }

# 6h. vb_sh_upload_all keeps the OK/FAIL line format panna's epv-pipeline.yml
#     greps ('^FAIL').
fake_reset
out=$(vb_sh_upload_all "test/fixture" "test-tag" "$tmpdir/a.parquet" 2>/dev/null)
grep -qx "OK a.parquet" <<<"$out" && pass "upload_all prints 'OK <name>'" || fail "upload_all printed '$out'"
FAKE_UPLOAD_FAIL=1
out=$(vb_sh_upload_all "test/fixture" "test-tag" "$tmpdir/a.parquet" 2>/dev/null)
grep -q "^FAIL a.parquet" <<<"$out" && pass "upload_all prints 'FAIL <name> ...'" || fail "upload_all printed '$out'"
# panna's epv-pipeline.yml runs `out=$(vb_sh_upload_all ...)` under
# `set -euo pipefail`; a failing LAST file must not kill the step before the
# FAIL line is read.
if out=$(set -e; vb_sh_upload_all "test/fixture" "test-tag" "$tmpdir/a.parquet" 2>/dev/null) \
   && grep -q "^FAIL a.parquet" <<<"$out"; then
  pass "upload_all returns 0 with a failing last file, so set -e callers still read FAIL"
else
  fail "upload_all aborted a set -e caller on a failing last file"
fi


# ---------------------------------------------------------------------------
# 7. vb_sh_restore_all -- bouncerdata's whole-release restore.
# ---------------------------------------------------------------------------
# 7a. Everything listed arrives at the listed size.
fake_reset
fake_put t20i_male__1_balls.parquet "per-match"
fake_put cricinfo_balls_t20i_male.parquet "bundle content"
fake_put notes.txt "not parquet"
d="$tmpdir/r7a"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore_all "o/r" "cricinfo" "$d" .parquet >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 0 ] && [ -f "$d/cricinfo_balls_t20i_male.parquet" ] && [ -f "$d/t20i_male__1_balls.parquet" ] && [ ! -f "$d/notes.txt" ]; then
  pass "restore_all restores every listed .parquet and nothing else"
else
  fail "restore_all on a healthy release: rc=$rc, dir: $(ls "$d" 2>/dev/null | tr '\n' ' ')"
fi

# 7b. A file the bulk download dropped is fetched individually.
fake_reset
fake_put t20i_male__1_balls.parquet "per-match"
fake_put cricinfo_balls_t20i_male.parquet "bundle content"
FAKE_BULK_SKIP=cricinfo_balls_t20i_male.parquet
d="$tmpdir/r7b"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore_all "o/r" "cricinfo" "$d" .parquet >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 0 ] && [ "$(cat "$d/cricinfo_balls_t20i_male.parquet" 2>/dev/null)" = "bundle content" ]; then
  pass "restore_all recovers a file the bulk download dropped"
else
  fail "restore_all after a partial bulk download: rc=$rc"
fi
FAKE_BULK_SKIP=""

# 7c. A listed file that cannot be downloaded at all fails the restore.
fake_reset
fake_put cricinfo_balls_t20i_male.parquet "bundle content"
FAKE_DOWNLOAD_FAIL=1
d="$tmpdir/r7c"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore_all "o/r" "cricinfo" "$d" .parquet >/dev/null 2>&1 || rc=$?
[ "$rc" -ne 0 ] && pass "restore_all fails when a listed file can't be downloaded (the old '|| true' hid this)" \
  || fail "restore_all returned 0 although nothing could be downloaded"
FAKE_DOWNLOAD_FAIL=0

# 7d. A truncated download fails the restore.
fake_reset
fake_put cricinfo_balls_t20i_male.parquet "bundle content"
FAKE_DOWNLOAD_TRUNC=1
d="$tmpdir/r7d"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore_all "o/r" "cricinfo" "$d" .parquet >/dev/null 2>&1 || rc=$?
[ "$rc" -ne 0 ] && pass "restore_all fails on a truncated download" || fail "restore_all accepted a truncated file"
FAKE_DOWNLOAD_TRUNC=0

# 7e. A bundle stranded under a temp name comes back under its real name,
#     and no temp-named file is left in the restore dir.
fake_reset
fake_put "vbnew-77-1-123--cricinfo_match_odi_male.parquet" "stranded bundle"
d="$tmpdir/r7e"
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore_all "o/r" "cricinfo" "$d" .parquet >/dev/null 2>&1 || rc=$?
if [ "$rc" -eq 0 ] && [ "$(cat "$d/cricinfo_match_odi_male.parquet" 2>/dev/null)" = "stranded bundle" ] \
   && [ -z "$(ls "$d" | grep '^vbnew-')" ]; then
  pass "restore_all restores a stranded temp copy under its real name"
else
  fail "stranded temp copy: rc=$rc, dir: $(ls "$d" 2>/dev/null | tr '\n' ' ')"
fi

# 7f. A listing failure is a failure, never "nothing to restore".
fake_reset
FAKE_LIST_FAIL=1
rc=0; VB_SH_ASSETS_JSON="" vb_sh_restore_all "o/r" "cricinfo" "$tmpdir/r7f" .parquet >/dev/null 2>&1 || rc=$?
[ "$rc" -ne 0 ] && pass "restore_all fails when the listing fails" || fail "restore_all read a listing failure as empty"
FAKE_LIST_FAIL=0

# ---------------------------------------------------------------------------
# 8. No workflow in this repo uploads with --clobber (comment lines excluded).
# ---------------------------------------------------------------------------
clobber_uploads=$(grep -n 'release upload.*--clobber' "$SCRIPT_DIR"/../../.github/workflows/cricinfo-daily.yml "$SCRIPT_DIR/../release_safe.sh" 2>/dev/null \
  | grep -v ':[0-9]*:[[:space:]]*#' || true)
if [ -z "$clobber_uploads" ]; then
  pass "no 'gh release upload --clobber' left in cricinfo-daily.yml or release_safe.sh"
else
  fail "delete-first uploads remain: $clobber_uploads"
fi

echo ""
echo "TOTALS: $pass_count passed, $fail_count failed"
[ "$fail_count" -eq 0 ]
