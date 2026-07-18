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
