#!/usr/bin/env bash
#
# kdp-rawsync.sh <session>
#   Rolling, ADDITIVE backup of an in-flight capture session's raw JSONL to
#   Google Drive, so losing the box mid-event can't lose un-archived data.
#
#   - Read-only w.r.t. capture: it only `rclone copy`s the on-disk JSONL; it
#     never touches the live writer.
#   - Runs on a short timer (kdp-rawsync@<session>.timer) for the life of the
#     capture. Once the capture unit goes inactive it does ONE final sync and
#     stops its own timer (so it cleans itself up after the event).
#   - SAFETY NET, not the authoritative archive: kdp-archive.sh still produces
#     the verified Parquet + raw tar.gz at settlement. The in-flight copy lands
#     at $KDP_RCLONE_REMOTE/<session>/raw-inflight/ and may be pruned once the session's
#     archive is verified.
#
# Run as root (it stops its own timer). Usage: kdp-rawsync.sh <session>
#
set -Eeuo pipefail
session="${1:?usage: kdp-rawsync.sh <session>}"

ENV_FILE="${KDP_ENV_FILE:-/etc/kdp/kdp.env}"
# shellcheck source=/dev/null
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
: "${KDP_DATA_DIR:=/var/lib/kdp/data}"
: "${KDP_RCLONE_REMOTE:?KDP_RCLONE_REMOTE must be set (e.g. remote:kdp) - see deploy/kdp.env.example}"
: "${RCLONE_CONFIG:=/home/kdp/.config/rclone/rclone.conf}"
export RCLONE_CONFIG

log() { printf '[%s] kdp-rawsync: %s\n' "$(date -u +%FT%TZ)" "$*"; }

command -v rclone >/dev/null || { log "rclone not installed"; exit 1; }

sdir="$KDP_DATA_DIR/$session"
remote="$KDP_RCLONE_REMOTE/$session/raw-inflight"
[[ -d "$sdir" ]] || { log "$session: no data dir; nothing to sync"; exit 0; }

# Additive copy ONLY (no --delete): never removes anything already on Drive.
# Run rclone AS the kdp user so OAuth-token refreshes are written back to the
# kdp-owned config (running as root would rewrite rclone.conf as root and lock
# the kdp-run archiver out). --local-no-check-updated stops false "corrupted on
# transfer" errors when copying a file the live capture is still appending to;
# the next cycle re-copies the now-larger file, so nothing is missed.
sync_now() {
  sudo -u kdp rclone --config "$RCLONE_CONFIG" copy "$sdir" "$remote" \
    --exclude '.done' --exclude '.archived' --local-no-check-updated \
    --transfers 4 --retries 3 --low-level-retries 10 \
    --stats-one-line --stats 0 2>&1 | tail -3 || true
}

if systemctl is-active --quiet "kdp-capture@${session}.service"; then
  log "$session: capture active -> rolling sync of in-flight raw to $remote"
  sync_now
else
  log "$session: capture inactive -> final sync, then stopping rolling backup"
  sync_now
  systemctl stop "kdp-rawsync@${session}.timer" 2>/dev/null || true
fi
log "$session: sync cycle done"
