#!/usr/bin/env bash
#
# kdp-archive.sh [session] [remote_prefix]
#   <session>                  -> archive just that one session NOW (event-driven, after a stop)
#   <session> <remote_prefix>  -> as above, but upload under <remote_prefix>/<session>
#                                 instead of the default $KDP_RCLONE_REMOTE/<session>
#                                 (per-event-set storage namespace, e.g. remote:kdp/cup-2026)
#   (no arg)                   -> sweep: archive every SETTLED session (nightly catch-all)
#
# Per session: process raw JSONL -> Parquet, report gaps + completeness, upload
# the Parquet AND a raw tar.gz to Google Drive (both checksum-verified), then
# (opt-in) prune local raw. Deletes raw ONLY after every check passes; aborts
# before any deletion otherwise. The Parquet is uploaded even if a ticker is
# "incomplete" (still useful) — but pruning then keeps the raw for review.
#
set -Eeuo pipefail

ENV_FILE="${KDP_ENV_FILE:-/etc/kdp/kdp.env}"
_prune_override="${KDP_PRUNE:-}"
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
[[ -n "$_prune_override" ]] && KDP_PRUNE="$_prune_override"

: "${KDP_BIN:=/opt/kdp/bin}"
: "${KDP_DATA_DIR:=/var/lib/kdp/data}"
: "${KDP_PROC_DIR:=/var/lib/kdp/processed}"
: "${KDP_RCLONE_REMOTE:?KDP_RCLONE_REMOTE must be set (e.g. remote:kdp) - see deploy/kdp.env.example}"
: "${KDP_BACKUP_RAW:=1}"
: "${KDP_PRUNE:=0}"
: "${KDP_HOT_DAYS:=7}"
: "${KDP_ALERT_WEBHOOK:=}"
: "${KDP_SETTLE_MINUTES:=10}"
# Google Drive enforces a per-minute API query quota. A per-event folder holds
# ~26 strikes x several files, so an unthrottled copy/check bursts past it and
# 403s ("Quota exceeded ... Queries per minute"). Cap rclone's transaction rate
# and lean on low-level retries (with backoff) so every Drive call self-paces.
: "${KDP_RCLONE_TPSLIMIT:=4}"
RCLONE_PACE=(--tpslimit "$KDP_RCLONE_TPSLIMIT" --low-level-retries 10)

log()    { printf '[%s] kdp-archive: %s\n' "$(date -u +%FT%TZ)" "$*"; }
# ntfy POST with headers: $1=priority $2=tags(emoji) $3=message body.
_ntfy()  { [[ -n "$KDP_ALERT_WEBHOOK" ]] || return 0; curl -fsS -m 15 -H "Title: kdp-archive" -H "Priority: $1" -H "Tags: $2" --data-binary "kdp-archive: $3" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true; }
# Routine progress -> low priority (delivered silently, no buzz). Failures -> urgent + warning.
notify() { log "$*";        _ntfy low    floppy_disk "$*"; }
alert()  { log "ALERT: $*"; _ntfy urgent warning     "ALERT: $*"; }
trap 'alert "unexpected abort at line $LINENO (no data deleted past this point)"' ERR

command -v rclone >/dev/null || { alert "rclone not installed"; exit 1; }
command -v jq     >/dev/null || { alert "jq not installed"; exit 1; }

# archive_one <session> <explicit|sweep> [remote_prefix]
archive_one() {
  local session="$1" mode="$2" remote_prefix="${3:-}"
  # Empty/absent 3rd arg -> fall back to the env default (a passed empty string
  # must NOT blank the remote -- ${3:-} above yields "" when the arg is absent).
  [[ -n "$remote_prefix" ]] || remote_prefix="$KDP_RCLONE_REMOTE"
  local sdir="$KDP_DATA_DIR/$session" remote="$remote_prefix/$session" proc="$KDP_PROC_DIR/$session"

  [[ -d "$sdir" ]] || { log "$session: no data dir; skip"; return 0; }
  [[ -e "$sdir/.archived" ]] && { log "$session: already archived; skip"; return 0; }
  if systemctl is-active --quiet "kdp-capture@${session}.service" 2>/dev/null; then
    log "$session: capture still active; skip"; return 0
  fi
  if [[ "$mode" == "sweep" ]]; then
    [[ -e "$sdir/.done" ]] || { log "$session: no .done; skip"; return 0; }
    if find "$sdir" -type f -name '*.jsonl' -newermt "-${KDP_SETTLE_MINUTES} minutes" -print -quit | grep -q .; then
      log "$session: recent writes; not settled; skip"; return 0
    fi
  fi

  log "$session: processing"
  install -d "$proc"
  "$KDP_BIN/kdp-process" --in "$sdir" --out "$proc" || { alert "$session: kdp-process failed; keeping raw"; return 0; }

  # Refuse to archive a session that produced ZERO tickers. This happens when a
  # restart re-resolves an already-finished+pruned event and re-captures 0
  # records (the market is settled): kdp-process finds no capture files and emits
  # no manifests. Uploading the resulting empty processed/ + empty raw tar would
  # CLOBBER a prior good archive on Drive (rclone copy/copyto overwrite by name).
  # Bail BEFORE any upload -- a no-data session must never touch Drive. (See the
  # prune/marker clobber post-mortem in docs/dev-context/open-items.md.)
  if ! compgen -G "$proc/*/manifest.json" >/dev/null; then
    log "$session: 0 tickers processed (no capture files); nothing to archive -- skip, no upload"
    return 0
  fi

  # Gap + completeness summary across the session's tickers.
  local incomplete=0 gaps=0 g c
  for m in "$proc"/*/manifest.json; do
    [[ -f "$m" ]] || continue
    c="$(jq -r '.complete // false' "$m")"; [[ "$c" == "true" ]] || incomplete=$((incomplete+1))
    g="$(jq -r '.counts.gaps // 0' "$m")"; gaps=$((gaps + g))
  done
  local quality; quality="$( ((gaps==0)) && echo GAPLESS || echo "${gaps} gap-marker(s)" )"
  quality+="$( ((incomplete==0)) && echo ", complete" || echo ", ${incomplete} INCOMPLETE ticker(s)" )"
  notify "$session: processed ($quality)"

  # Curate: keep only genuinely two-sided strikes in the processed (Parquet)
  # upload. The raw tar.gz (below) still backs up ALL captured strikes to Drive,
  # so a one-sided strike's data is never truly lost (just kept out of the
  # curated working set). A manifest without the field (e.g. a non-hourly
  # session) defaults to kept.
  #
  # NB: do NOT use `.two_sided // true` here -- jq's `//` treats BOTH null AND
  # `false` as "empty", so it returns `true` for a genuinely one-sided strike,
  # silently dropping nothing. Test `== false` explicitly so only a real `false`
  # is curated out; null/missing (non-hourly) still defaults to kept.
  local dropped=0
  for m in "$proc"/*/manifest.json; do
    [[ -f "$m" ]] || continue
    if [[ "$(jq -r 'if .two_sided == false then "drop" else "keep" end' "$m")" == "drop" ]]; then
      rm -rf -- "$(dirname "$m")"
      dropped=$((dropped + 1))
    fi
  done
  log "$session: curated processed set (dropped $dropped one-sided strike(s))"

  log "$session: uploading parquet"
  rclone copy  "$proc" "$remote/processed" --checksum --transfers 4 --retries 5 "${RCLONE_PACE[@]}" || { alert "$session: parquet upload failed; keeping raw"; return 0; }
  rclone check "$proc" "$remote/processed" --checksum --one-way "${RCLONE_PACE[@]}" || { alert "$session: parquet verify failed; keeping raw"; return 0; }

  local raw_ok=1
  if [[ "$KDP_BACKUP_RAW" == "1" ]]; then
    local tb; tb="$(mktemp --suffix=.tar.gz)"
    if tar -C "$KDP_DATA_DIR" --exclude='.done' --exclude='.archived' -czf "$tb" "$session" \
       && rclone copyto "$tb" "$remote/raw/$session.tar.gz" --checksum --retries 5 "${RCLONE_PACE[@]}"; then
      local lm rm; lm="$(md5sum "$tb" | cut -d' ' -f1)"; rm="$(rclone md5sum "$remote/raw/$session.tar.gz" "${RCLONE_PACE[@]}" | awk '{print $1}')"
      [[ -n "$rm" && "$lm" == "$rm" ]] || raw_ok=0
    else
      raw_ok=0
    fi
    rm -f "$tb"
    (( raw_ok == 0 )) && { alert "$session: raw backup failed/unverified; keeping raw"; return 0; }
    log "$session: raw backup verified on Drive"
  fi

  : > "$sdir/.archived"

  if [[ "$KDP_PRUNE" == "1" && ( "$KDP_BACKUP_RAW" != "1" || "$raw_ok" == "1" ) ]] && (( incomplete == 0 )); then
    log "$session: pruning local raw"
    # Remove the raw ticker SUBDIRS but KEEP the session dir + .done/.archived
    # markers. A full `rm -rf $session` would delete the .archived marker we just
    # wrote, defeating the scheduler's already_archived restart-guard -- a restart
    # within the event's arm window would then re-arm the finished event (see the
    # prune/marker clobber post-mortem in open-items). The empty marker stub is
    # reaped by the nightly sweep once it ages past the arm window (below).
    find "${KDP_DATA_DIR:?}/$session" -mindepth 1 -maxdepth 1 -type d -exec rm -rf -- {} +
  else
    log "$session: archived; local raw kept (KDP_PRUNE=$KDP_PRUNE, incomplete=$incomplete)"
  fi
  notify "$session: archived to Drive ($remote)"
  return 0
}

[[ -d "$KDP_DATA_DIR" ]] || { log "no data dir $KDP_DATA_DIR; nothing to do"; exit 0; }

if (( $# >= 1 )); then
  # Optional 2nd arg: a Drive-namespace prefix for this event-set (the scheduler
  # passes e.g. remote:kdp/cup-2026); absent -> the env's KDP_RCLONE_REMOTE.
  archive_one "$1" explicit "${2:-}" || true
else
  shopt -s nullglob
  for sdir in "$KDP_DATA_DIR"/*/; do
    archive_one "$(basename "$sdir")" sweep || true
  done
  # Age out the local processed hot-cache (Drive holds the durable copy).
  if [[ -d "$KDP_PROC_DIR" ]]; then
    while IFS= read -r -d '' d; do
      log "hot-cache expire $(basename "$d")"; rm -rf -- "$d"
    done < <(find "$KDP_PROC_DIR" -mindepth 1 -maxdepth 1 -type d -mtime "+${KDP_HOT_DAYS}" -print0 2>/dev/null)
  fi
  # Reap archived-marker stubs. Pruning keeps an empty session dir (.archived +
  # .done) so the scheduler's restart-guard sees a finished event as done DURING
  # its arm window (<= max_hours, ~8h). Once a stub ages past that window the
  # guard never consults it again, so it is pure residue -- remove stubs (a dir
  # with no ticker SUBDIRS left, only marker files) older than the hot-cache
  # horizon. Keeps local disk bounded without ever reaping a marker still in use.
  while IFS= read -r -d '' d; do
    [[ -e "$d/.archived" ]] || continue
    find "$d" -mindepth 1 -maxdepth 1 -type d -print -quit | grep -q . && continue
    log "marker-stub reap $(basename "$d")"; rm -rf -- "$d"
  done < <(find "$KDP_DATA_DIR" -mindepth 1 -maxdepth 1 -type d -mtime "+${KDP_HOT_DAYS}" -print0 2>/dev/null)
fi

log "done."
