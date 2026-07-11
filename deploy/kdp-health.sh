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

log()   { printf '[%s] kdp-health: %s\n' "$(date -u +%FT%TZ)" "$*"; }
alert() {
  log "ALERT: $*"
  [[ -n "$KDP_ALERT_WEBHOOK" ]] && curl -fsS -m 15 --data-binary "kdp-health: $*" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true
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

if (( problems == 0 )); then log "ok"; else log "$problems problem(s) reported"; fi
