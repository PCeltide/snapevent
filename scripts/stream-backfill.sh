#!/usr/bin/env bash
#
# stream-backfill.sh - capture a series' FULL trade history over a date range and
# stream it to Google Drive day-by-day, keeping local disk flat.
#
# Covers BOTH Kalshi data tiers automatically, so a single run spans the whole
# range regardless of where the moving ~3-month historical cutoff currently sits:
#   * ARCHIVE tier (GET /historical/*) -> markets that SETTLED before the cutoff
#   * LIVE    tier (GET /markets*)      -> markets that settled after the cutoff
# Each tier is asked for the full range and naturally returns only its half (the
# other half comes back empty = a clean no-op), so there is no cutoff to compute
# and no seam to reason about. A given market is in exactly one tier, so the two
# passes never double-fetch it.
#
# Per chunk (default 1 day): backfill -> kdp-process (Parquet) -> tar.gz ->
# rclone upload -> verify -> DELETE local. Peak local disk ~= one day's trades.
# Resumable: completed <tier>:<day> chunks are recorded and skipped on re-run.
# All endpoints are public/no-auth; only rclone (Drive) credentials are needed.
#
# TARGET: the Linux server (GNU coreutils `date`, rclone, rust toolchain, repo).
# Run under tmux/systemd for unattended use.
#
# Usage (override any via env):
#   SERIES=KXBTCD MIN_CLOSE=2026-01-01 MAX_CLOSE=2026-06-03 \
#   REMOTE=remote:kdp/btc-trades/all-2026 \
#   bash scripts/stream-backfill.sh 2>&1 | tee ~/stream-backfill.log
#
set -euo pipefail

SERIES="${SERIES:-KXBTCD}"
MIN_CLOSE="${MIN_CLOSE:-2026-01-01}"     # inclusive (UTC date / rfc3339 / unix)
MAX_CLOSE="${MAX_CLOSE:-2026-06-03}"     # exclusive
CHUNK_DAYS="${CHUNK_DAYS:-1}"            # one Drive tarball per this many days
MIN_VOLUME="${MIN_VOLUME:-1}"           # skip never-traded strikes (0 = keep all)
RATE="${RATE:-10}"                      # GET/s (token bucket; <= Basic budget)
FORMAT="${FORMAT:-parquet}"             # parquet | feather
REPO="${REPO:-$HOME/snapevent}"
WORK="${WORK:-$HOME/kdp-stream-work}"   # local scratch; one chunk lives here at a time
REMOTE="${REMOTE:?REMOTE must be set, e.g. remote:kdp/btc-trades/all-2026}"
RCLONE_CONF="${RCLONE_CONF:-}"          # optional rclone --config path

cd "$REPO"
export PATH="$HOME/.cargo/bin:$PATH"
mkdir -p "$WORK"
DONE="$WORK/.chunks-done"; touch "$DONE"
RC=(rclone); [ -n "$RCLONE_CONF" ] && RC=(rclone --config "$RCLONE_CONF")
log() { echo "[$(date -u '+%F %T')Z] $*"; }

log "building release binaries (kdp-cli, kdp-process)..."
cargo build --release -p kdp-cli -p kdp-process
CLI="$REPO/target/release/kdp-cli"
PROC="$REPO/target/release/kdp-process"
log "series=$SERIES range=[$MIN_CLOSE,$MAX_CLOSE) chunk=${CHUNK_DAYS}d remote=$REMOTE"

# Enumerate the ARCHIVE tier once (its filters have no close-time param), so the
# per-day archive backfills read it via --markets-file instead of re-paginating.
META="$WORK/meta-archive"
if [ ! -s "$META/markets.jsonl" ]; then
  log "enumerating $SERIES archive markets once (logs earliest_close = inception)..."
  "$CLI" backfill --series "$SERIES" --historical --discover-only \
    --min-close "$MIN_CLOSE" --max-close "$MAX_CLOSE" --out "$META" --rate "$RATE"
  [ -s "$META/markets.jsonl" ] && "${RC[@]}" copy "$META/markets.jsonl" \
    "$REMOTE/_meta/markets-archive.jsonl" 2>/dev/null || \
    log "  (archive enumeration empty or _meta upload skipped)"
fi

process_upload_chunk() {  # $1=tier  $2=day-label  $3=capture-dir
  local tier="$1" label="$2" cdir="$3" pdir tarf
  pdir="${cdir}_processed"; tarf="$WORK/${label}.${tier}.tar.gz"
  # Count strike subdirs without a SIGPIPE-prone `find | grep -q` pipe: under
  # `set -o pipefail`, `grep -q` closes the pipe on first match and `find` dies
  # with SIGPIPE (141), which pipefail propagates -> the chunk is wrongly seen as
  # empty and the upload is skipped. Drain into a count instead.
  local ndirs
  ndirs=$(find "$cdir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
  if [ "$ndirs" -gt 0 ]; then
    "$PROC" --in "$cdir" --out "$pdir" --all --format "$FORMAT"
    tar czf "$tarf" -C "$pdir" .
    "${RC[@]}" copy "$tarf" "$REMOTE/"
    [ -f "$cdir/markets.jsonl" ] && "${RC[@]}" copy "$cdir/markets.jsonl" \
      "$REMOTE/_meta/${label}.${tier}.markets.jsonl"
    "${RC[@]}" lsf "$REMOTE/$(basename "$tarf")" >/dev/null   # verify it landed
    log "  $tier $label: uploaded $(basename "$tarf") ($(du -h "$tarf" | cut -f1))"
  else
    log "  $tier $label: no traded markets in window"
  fi
  rm -rf "$cdir" "$pdir" "$tarf"
}

run_tier() {  # $1 = archive | live
  local tier="$1" start end step lo hi label cdir
  # Archive tier reads the one-shot enumeration; if it matched nothing (e.g. the
  # whole range is post-cutoff), there is no markets.jsonl and nothing to do.
  if [ "$tier" = archive ] && [ ! -s "$META/markets.jsonl" ]; then
    log "  archive: no enumerated markets (range fully post-cutoff?); skipping tier"
    return 0
  fi
  start=$(date -u -d "$MIN_CLOSE" +%s); end=$(date -u -d "$MAX_CLOSE" +%s)
  step=$(( CHUNK_DAYS * 86400 )); lo=$start
  while [ "$lo" -lt "$end" ]; do
    hi=$(( lo + step )); [ "$hi" -gt "$end" ] && hi=$end
    label=$(date -u -d "@$lo" +%F)
    if grep -qxF "$tier:$label" "$DONE"; then lo=$hi; continue; fi
    cdir="$WORK/${tier}_${label}"; rm -rf "$cdir"
    log "$tier $label: backfill [$lo,$hi)..."
    if [ "$tier" = archive ]; then
      "$CLI" backfill --markets-file "$META/markets.jsonl" --historical \
        --min-close "$lo" --max-close "$hi" --min-volume "$MIN_VOLUME" \
        --out "$cdir" --rate "$RATE" --resume
    else
      "$CLI" backfill --series "$SERIES" \
        --min-close "$lo" --max-close "$hi" --chunk "${CHUNK_DAYS}d" \
        --min-volume "$MIN_VOLUME" --out "$cdir" --rate "$RATE" --resume
    fi
    process_upload_chunk "$tier" "$label" "$cdir"
    echo "$tier:$label" >> "$DONE"
    lo=$hi
  done
}

log "=== ARCHIVE tier (markets settled before the cutoff) ==="; run_tier archive
log "=== LIVE tier (markets settled after the cutoff) ==="; run_tier live
log "ALL DONE for $SERIES [$MIN_CLOSE,$MAX_CLOSE). Drive: $REMOTE"
log "Verify locally after pulling a day: bash scripts/verify-hourly-days.sh <extracted-dir> $SERIES"
