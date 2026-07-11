#!/usr/bin/env bash
#
# kdp-settlewatch.sh — gracefully stop a capture session shortly after its target
# market(s) settle. Polls the PUBLIC Kalshi /markets endpoint; when every target
# ticker reaches a terminal status, waits a short grace period (to capture the
# final convergence) then `systemctl stop`s the capture (SIGINT -> clean shutdown).
#
# Bias is toward NOT stopping early (missing data is unrecoverable):
#   - only a RECOGNISED terminal status counts; transient/unknown/unreadable
#     statuses keep capturing;
#   - a hard backstop (KDP_SETTLE_MAXHOURS) guarantees it never runs forever.
#
# Run as root (it calls `systemctl stop`). Usage:
#   kdp-settlewatch.sh <session> <series_ticker> <ticker[,ticker...]>
#   e.g.  kdp-settlewatch.sh cup-final KXCUP KXCUP-26-XX,KXCUP-26-YY
#
set -Eeuo pipefail
session="${1:?usage: kdp-settlewatch.sh <session> <series_ticker> <ticker,ticker>}"
series="${2:?need series_ticker}"
tickers="${3:?need target tickers}"

ENV_FILE="${KDP_ENV_FILE:-/etc/kdp/kdp.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
: "${KDP_KALSHI_BASE:=https://api.elections.kalshi.com/trade-api/v2}"
: "${KDP_SETTLE_TERMINAL:=closed settled finalized determined}"  # statuses that mean "done"
: "${KDP_SETTLE_GRACE:=180}"     # keep capturing this many seconds after settlement
: "${KDP_SETTLE_POLL:=30}"       # poll interval (s)
: "${KDP_SETTLE_MAXHOURS:=10}"   # hard backstop — never run longer than this
: "${KDP_ALERT_WEBHOOK:=}"

log()    { printf '[%s] settlewatch: %s\n' "$(date -u +%FT%TZ)" "$*"; }
notify() { log "$*";        [[ -n "$KDP_ALERT_WEBHOOK" ]] && curl -fsS -m 15 --data-binary "settlewatch: $*" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true; }
alert()  { log "ALERT: $*"; [[ -n "$KDP_ALERT_WEBHOOK" ]] && curl -fsS -m 15 --data-binary "settlewatch: ALERT: $*" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true; }

# Gracefully stop the capture, then immediately kick off the per-session archive
# (process -> verify -> Drive). `systemctl stop` blocks until the unit is fully
# stopped, so the archive runs against a settled session.
stop_and_archive() {
  systemctl stop "kdp-capture@${session}.service" || true
  if systemctl start --no-block "kdp-archive@${session}.service" 2>/dev/null; then
    notify "$session: stopped gracefully -> archive started (process -> verify -> Google Drive)"
  else
    alert "$session: stopped, but could not start kdp-archive@${session}; run 'kdp-archive.sh $session' manually"
  fi
}

command -v jq   >/dev/null || { alert "jq not installed";   exit 1; }
command -v curl >/dev/null || { alert "curl not installed"; exit 1; }

IFS=',' read -ra TK <<< "$tickers"
deadline=$(( $(date +%s) + KDP_SETTLE_MAXHOURS * 3600 ))

is_terminal() {  # 0 if $1 is a recognised terminal status
  local s
  for s in $KDP_SETTLE_TERMINAL; do [[ "$1" == "$s" ]] && return 0; done
  return 1
}

all_settled() {  # 0 only if EVERY target ticker is terminal
  local resp st t
  resp="$(curl -fsS -m 20 "$KDP_KALSHI_BASE/markets?series_ticker=$series&limit=1000" 2>/dev/null)" || return 1
  for t in "${TK[@]}"; do
    st="$(jq -r --arg t "$t" '.markets[] | select(.ticker==$t) | .status // empty' <<<"$resp" 2>/dev/null || echo '')"
    [[ -n "$st" ]] || return 1     # unreadable -> assume still trading (conservative)
    is_terminal "$st" || return 1  # still trading
  done
  return 0
}

log "watching {$tickers} (series $series) for $session; terminal={$KDP_SETTLE_TERMINAL}, grace=${KDP_SETTLE_GRACE}s, backstop=${KDP_SETTLE_MAXHOURS}h"
while (( $(date +%s) < deadline )); do
  if all_settled; then
    log "all targets terminal; capturing ${KDP_SETTLE_GRACE}s more, then stopping"
    sleep "$KDP_SETTLE_GRACE"
    stop_and_archive
    exit 0
  fi
  sleep "$KDP_SETTLE_POLL"
done
alert "$session: ${KDP_SETTLE_MAXHOURS}h backstop reached without confirmed settlement; stopping + archiving anyway"
stop_and_archive
