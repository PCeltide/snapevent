#!/usr/bin/env bash
#
# kdp-health.sh — lightweight watchdog. Alerts (via KDP_ALERT_WEBHOOK) on:
#   - low free disk on the data volume,
#   - any failed kdp-* systemd unit,
#   - an ACTIVE capture session that has stopped receiving data (stuck beyond
#     the in-process reconnect's reach).
# Run frequently from a timer. Read-only; never deletes anything.
#
set -Eeuo pipefail

ENV_FILE="${KDP_ENV_FILE:-/etc/kdp/kdp.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
: "${KDP_DATA_DIR:=/var/lib/kdp/data}"
: "${KDP_ALERT_WEBHOOK:=}"
: "${KDP_DISK_FLOOR_GIB:=5}"
: "${KDP_STALE_SECONDS:=180}"   # active capture with no new jsonl this long = stuck
: "${KDP_ERROR_WINDOW_MIN:=6}"  # journal lookback for the ERROR-rate check (timer fires every 5min)
: "${KDP_ERROR_FLOOR:=5}"       # more ERROR lines than this in the window = alert (0 disables)
: "${KDP_ALERT_THROTTLE_SEC:=3600}"                  # suppress an identical ntfy push re-sent within this window
: "${KDP_ALERT_STATE_DIR:=/var/lib/kdp/alert-state}" # per-alert last-sent timestamps live here

log() { printf '[%s] kdp-health: %s\n' "$(date -u +%FT%TZ)" "$*"; }

# ntfy POST with headers: $1=priority $2=tags(emoji) $3=message body.
_ntfy() {
  [[ -n "$KDP_ALERT_WEBHOOK" ]] || return 0
  curl -fsS -m 15 -H "Title: kdp-health" -H "Priority: $1" -H "Tags: $2" \
       --data-binary "kdp-health: $3" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true
}

# De-duplicated alert. Always logs; pushes to ntfy at most once per throttle
# window per distinct SUBJECT. The key normalises the message to letters only so
# a drifting metric (disk GiB, ages in seconds) can't defeat dedup by changing
# the number every run -- the reason a single stuck unit fired 186 identical
# pushes in 24h. A genuinely different subject (low disk vs. a failed unit) still
# fires. Known tradeoff of stripping digits: subjects differing ONLY in digits
# share a key, so two capture sessions stale at once alert once per window, not
# twice. See runbook-server.md ("Alert throttling").
alert() {
  log "ALERT: $*"
  [[ -n "$KDP_ALERT_WEBHOOK" ]] || return 0
  mkdir -p "$KDP_ALERT_STATE_DIR" 2>/dev/null || true
  local key f now last
  key=$(printf '%s' "$*" | tr -cd 'A-Za-z ' | cksum | cut -d' ' -f1)
  f="$KDP_ALERT_STATE_DIR/$key"
  now=$(date +%s)
  if [[ -f "$f" ]]; then
    last=$(cat "$f" 2>/dev/null || echo 0); last=${last:-0}
    if (( now - last < KDP_ALERT_THROTTLE_SEC )); then
      log "(ntfy throttled: same alert sent $((now-last))s ago; window ${KDP_ALERT_THROTTLE_SEC}s)"
      return 0
    fi
  fi
  printf '%s' "$now" > "$f" 2>/dev/null || true
  _ntfy urgent warning "$*"
}

problems=0

# 1. Disk free on the data volume.
avail_gib="$(df -BG --output=avail "$KDP_DATA_DIR" 2>/dev/null | tail -1 | tr -dc '0-9' || true)"
if [[ -n "$avail_gib" ]] && (( avail_gib < KDP_DISK_FLOOR_GIB )); then
  alert "low disk: ${avail_gib} GiB free on $KDP_DATA_DIR (floor ${KDP_DISK_FLOOR_GIB} GiB)"
  problems=$((problems+1))
fi

# 2. Any failed kdp-* unit.
if systemctl --failed --no-legend 2>/dev/null | grep -q 'kdp-'; then
  failed="$(systemctl --failed --no-legend | awk '/kdp-/{print $1}' | tr '\n' ' ')"
  alert "failed systemd unit(s): $failed"
  problems=$((problems+1))
fi

# 3. Active capture sessions must be receiving data.
while IFS= read -r unit; do
  [[ -n "$unit" ]] || continue
  session="${unit#kdp-capture@}"; session="${session%.service}"
  sdir="$KDP_DATA_DIR/$session"
  newest="$(find "$sdir" -type f -name '*.jsonl' -printf '%T@\n' 2>/dev/null | sort -n | tail -1 || true)"
  if [[ -z "$newest" ]]; then
    alert "capture $session is active but has produced no jsonl yet"
    problems=$((problems+1)); continue
  fi
  age=$(( $(date +%s) - ${newest%.*} ))
  if (( age > KDP_STALE_SECONDS )); then
    alert "capture $session stale: no new data for ${age}s (> ${KDP_STALE_SECONDS}s)"
    problems=$((problems+1))
  fi
done < <(systemctl list-units 'kdp-capture@*' --state=active --no-legend --plain 2>/dev/null | awk '{print $1}')

# 4. ERROR lines in the kdp journal.
#
# Until 2026-08-09 this watchdog looked only at disk, failed units and stale
# capture -- so it reported "ok" straight through 1,062 rclone ERRORs at the
# UTC rollover (the checkpoint/prune race, fixed in kdp-archive.sh). That run
# was harmless, but an rclone failure that DID matter would have been just as
# silent. Nothing here parses the errors; the point is that a burst of them
# reaches a human at all.
#
# Text match, not --priority=err: rclone/kdp-process errors arrive as service
# stderr, which journald records at the unit's SyslogLevel (info), so a
# priority filter sees none of them. Both `ERROR` (rclone) and tracing's
# `ERROR` level match the same token.
#
# Our OWN alert lines are excluded. alert() logs "kdp-health: ALERT: <msg>" and
# the msg below contains the word ERROR, so without the filter this check
# counts its own output: bounded (a 5-min timer against a 6-min window yields
# at most 2 self-lines, so floor 5 cannot self-sustain) but it would latch on
# permanently at a floor of 2 or below. Excluding the lines removes the
# constraint instead of documenting it -- kdp-health logs only its own alerts
# and "ok", so nothing real is dropped.
_err_lines() {
  journalctl -u 'kdp-*' --since "-${KDP_ERROR_WINDOW_MIN}min" --no-pager -o cat 2>/dev/null \
    | grep -v 'kdp-health:' | grep 'ERROR' || true
}
if (( KDP_ERROR_FLOOR > 0 )); then
  errs="$(_err_lines | grep -c '' || true)"
  errs="${errs:-0}"
  if (( errs > KDP_ERROR_FLOOR )); then
    worst="$(_err_lines | head -1 | cut -c1-160 || true)"
    alert "${errs} ERROR line(s) in the kdp journal over ${KDP_ERROR_WINDOW_MIN}min (floor ${KDP_ERROR_FLOOR}); first: ${worst}"
    problems=$((problems+1))
  fi
fi

if (( problems == 0 )); then
  log "ok"
  # Self-heal notice: if we were alerting recently (throttle state present) and
  # everything is clear now, send ONE all-clear and reset the throttle state so
  # the next distinct failure alerts immediately rather than being throttled.
  if [[ -n "$KDP_ALERT_WEBHOOK" && -d "$KDP_ALERT_STATE_DIR" ]] && compgen -G "$KDP_ALERT_STATE_DIR/*" >/dev/null 2>&1; then
    rm -f "$KDP_ALERT_STATE_DIR"/* 2>/dev/null || true
    _ntfy default white_check_mark "recovered -- all checks passing"
  fi
else
  log "$problems problem(s) reported"
fi
