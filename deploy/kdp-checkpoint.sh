#!/usr/bin/env bash
# kdp-checkpoint.sh <session> [remote_prefix]
#
# Daily raw checkpoint for a long-window in-flight session: an ADDITIVE rclone
# copy of the session's raw JSONL to <prefix>/<session>/raw-inflight/. Spawned
# by the capture supervisor's --checkpoint-cmd just after each UTC day
# rotation, so a box loss costs at most today's partial day (design 1.3).
#
# Safety net only -- the authoritative archive stays kdp-archive.sh, which
# purges raw-inflight/ as its LAST step, only after the settle-time raw tar
# checksum-verifies on Drive. A failed checkpoint alerts (ntfy) but never
# touches capture. Local day-files are NOT pruned here (the settle-time
# process needs the full raw).
set -Eeuo pipefail

ENV_FILE="${KDP_ENV_FILE:-/etc/kdp/kdp.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
: "${KDP_DATA_DIR:=/var/lib/kdp/data}"
# NB: required even when an explicit [remote_prefix] arg is passed -- the only
# production caller (the universe supervisor) already refuses to start without
# it, so this guard just keeps a manual half-configured invocation loud.
: "${KDP_RCLONE_REMOTE:?KDP_RCLONE_REMOTE must be set (remote:path, e.g. remote:kdp)}"
: "${KDP_RCLONE_TPSLIMIT:=4}"

log() { printf '%s kdp-checkpoint: %s\n' "$(date -u +%FT%TZ)" "$*"; }
_ntfy() {
  [[ -n "${KDP_ALERT_WEBHOOK:-}" ]] || return 0
  curl -fsS -m 15 -H "Title: kdp-checkpoint" -H "Priority: $1" -H "Tags: $2" \
       --data-binary "kdp-checkpoint: $3" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true
}

session="${1:?usage: kdp-checkpoint.sh <session> [remote_prefix]}"
prefix="${2:-$KDP_RCLONE_REMOTE}"
sdir="$KDP_DATA_DIR/$session"
remote="$prefix/$session/raw-inflight"
lock="$KDP_DATA_DIR/.locks/$session.lock"

if [[ ! -d "$sdir" ]]; then
  log "$session: no session dir at $sdir; nothing to checkpoint"
  exit 0
fi

# Serialize against kdp-archive.sh's raw-inflight purge + local prune of this
# same session (see that script's "checkpoint lock" comment). Non-blocking on
# purpose: the archive holding the lock means this session is being made
# durable right now, which is exactly what the checkpoint is a stand-in for --
# so there is nothing left to checkpoint. Skipping is the correct outcome, not
# a degraded one.
command -v flock >/dev/null || { log "flock not installed (util-linux); refusing to checkpoint unlocked"; exit 1; }
# umask 0 so a lock file created by a ROOT run of kdp-archive.sh (one `sudo
# kdp-archive.sh` during an incident is a plausible human move) is still
# writable by the kdp user this runs as. Without it `exec 9>` fails with
# Permission denied and the script dies under `set -e` BEFORE it can alert --
# a silently dropped raw checkpoint, which is the safety net, not the archive.
install -d "$KDP_DATA_DIR/.locks"
_um="$(umask)"; umask 0000; exec 9>"$lock"; umask "$_um"
if ! flock -n 9; then
  log "$session: archive in progress (lock held); skipping this checkpoint"
  exit 0
fi

if rclone ${RCLONE_CONFIG:+--config "$RCLONE_CONFIG"} copy "$sdir" "$remote" \
     --exclude '.done' --exclude '.archived' --exclude '.remote-prefix' --local-no-check-updated \
     --transfers 4 --retries 3 --low-level-retries 10 \
     --tpslimit "$KDP_RCLONE_TPSLIMIT" \
     --stats-one-line --stats 0; then
  log "$session: raw checkpoint synced to $remote"
else
  log "$session: raw checkpoint FAILED"
  _ntfy urgent warning "$session: daily raw checkpoint failed (raw remains local-only)"
fi
