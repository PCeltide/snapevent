#!/usr/bin/env bash
# kdp-digest.sh -- nightly capture-health digest over the processed hot cache.
#
# Sweeps every manifest.json modified in the last KDP_DIGEST_WINDOW_HOURS
# (default 24) under KDP_PROC_DIR and pushes ONE ntfy summary:
#   all green  -> Priority: low (no buzz; a quiet "still alive" receipt)
#   any problem-> Priority: urgent (incomplete tickers, verify mismatches,
#                 underflows, or uptime below KDP_DIGEST_UPTIME_FLOOR)
#   ZERO manifests in the window -> urgent (a dead capture/archive/timer must
#                 never read as silence-is-fine)
# Uptime is 1 - hole_us/span_us from the manifest stamps (same clamped-hole
# rule as kdp-data coverage(); pre-stamp manifests lack the fields and are
# skipped by the uptime metric, never counted as 0 or 1).
set -Eeuo pipefail

ENV_FILE="${KDP_ENV_FILE:-/etc/kdp/kdp.env}"
if [[ -f "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi
: "${KDP_PROC_DIR:=/var/lib/kdp/processed}"
: "${KDP_DATA_DIR:=/var/lib/kdp/data}"
: "${KDP_DIGEST_UPTIME_FLOOR:=0.999}"
: "${KDP_DIGEST_WINDOW_HOURS:=24}"
# How many below-floor tickers are tolerated before the digest goes urgent.
# NOT a sensitivity reduction: min uptime, the worst ticker's name, and the
# below-floor count are in the body of EVERY digest, green or urgent -- only
# the urgent flag moves. See the "why this is a count, not a min" note below.
: "${KDP_DIGEST_LOW_UPTIME_MAX:=5}"
# A malformed env value must not silently kill the digest under set -e -- a
# dead digest with no push is the exact failure class this script exists to
# surface. The floor must be ONE valid JSON-ish number ("1.2.3" or "." would
# blow up jq --argjson); the window must be a positive integer (anything else
# breaks the $((...)) arithmetic below).
[[ "$KDP_DIGEST_UPTIME_FLOOR" =~ ^[0-9]*\.?[0-9]+$ ]] || KDP_DIGEST_UPTIME_FLOOR=0.999
[[ "$KDP_DIGEST_WINDOW_HOURS" =~ ^[1-9][0-9]*$ ]] || KDP_DIGEST_WINDOW_HOURS=24
[[ "$KDP_DIGEST_LOW_UPTIME_MAX" =~ ^[0-9]+$ ]] || KDP_DIGEST_LOW_UPTIME_MAX=5

log() { printf '%s kdp-digest: %s\n' "$(date -u +%FT%TZ)" "$*"; }
_ntfy() {
  [[ -n "${KDP_ALERT_WEBHOOK:-}" ]] || return 0
  curl -fsS -m 15 -H "Title: kdp-digest" -H "Priority: $1" -H "Tags: $2" \
       --data-binary "$3" "$KDP_ALERT_WEBHOOK" >/dev/null 2>&1 || true
}

mapfile -t manifests < <(find "$KDP_PROC_DIR" -name manifest.json \
  -mmin -"$(( KDP_DIGEST_WINDOW_HOURS * 60 ))" 2>/dev/null | sort)

if (( ${#manifests[@]} == 0 )); then
  msg="NO manifests processed in the last ${KDP_DIGEST_WINDOW_HOURS}h -- capture/archive/timer may be dead"
  log "$msg"
  _ntfy urgent rotating_light "kdp-digest: $msg"
  exit 0
fi

# Session count from the path layout <proc>/<session>/<ticker>/manifest.json.
sessions=$(printf '%s\n' "${manifests[@]}" | awk -F/ '{print $(NF-2)}' | sort -u | wc -l)

summary="$(jq -s --argjson floor "$KDP_DIGEST_UPTIME_FLOOR" '
  {
    tickers: length,
    incomplete: ([ .[] | select(.complete != true) ] | length),
    gaps: (([.[].counts.gaps] | add) // 0),
    verify_mismatches: (([.[].verify_mismatches] | add) // 0),
    mismatch_tickers: ([ .[] | select((.verify_mismatches // 0) > 0) | .ticker ] | sort),
    underflows: (([.[].underflows] | add) // 0),
    with_uptime: ([ .[] | select((.span_us // 0) > 0 and .hole_us != null) ] | length),
    worst: ([ .[] | select((.span_us // 0) > 0 and .hole_us != null)
              | {t: .ticker, u: (1 - (.hole_us / .span_us))} ] | min_by(.u)),
    low_uptime: ([ .[] | select((.span_us // 0) > 0 and .hole_us != null)
                   | select((1 - (.hole_us / .span_us)) < $floor) ] | length)
  }' "${manifests[@]}")"

incomplete=$(jq -r '.incomplete' <<<"$summary")
mismatches=$(jq -r '.verify_mismatches' <<<"$summary")
underflows=$(jq -r '.underflows' <<<"$summary")
low_uptime=$(jq -r '.low_uptime' <<<"$summary")
# Name the worst ticker, not just its number. "min uptime 34.95%" over ~400
# tickers costs a human an ssh and a jq sweep to find out which one; the name
# usually answers it outright (an illiquid wing strike vs an ATM peer).
min_uptime=$(jq -r 'if .worst == null then "n/a" else (.worst.u * 100 | tostring | .[0:8]) + "% (" + .worst.t + ")" end' <<<"$summary")
# Same reasoning for the mismatch trigger: a verify mismatch is the honesty gate
# (exact full-state REST-vs-replay inequality) and it SYNTHESIZES a gap marker,
# so it is usually the CAUSE of that ticker's low uptime, not a coincidence
# beside it. Naming the tickers is what makes the alert actionable -- the next
# step is always `verify_outcomes.parquet` on those exact tickers. Capped at 5
# so a systemic day cannot blow up the push body.
mismatch_names=$(jq -r 'if (.mismatch_tickers | length) == 0 then "" else
    " [" + (.mismatch_tickers[0:5] | join(", "))
    + (if (.mismatch_tickers | length) > 5 then ", +" + ((.mismatch_tickers | length) - 5 | tostring) + " more" else "" end)
    + "]" end' <<<"$summary")
tickers=$(jq -r '.tickers' <<<"$summary")
gaps=$(jq -r '.gaps' <<<"$summary")
disk_avail=$(df -k --output=avail "$KDP_DATA_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
disk_gib=$(( ${disk_avail:-0} / 1048576 ))

body="last ${KDP_DIGEST_WINDOW_HOURS}h: ${sessions} sessions / ${tickers} tickers; min uptime ${min_uptime}; ${low_uptime} below floor ${KDP_DIGEST_UPTIME_FLOOR}; gaps ${gaps}; incomplete ${incomplete}; verify mismatches ${mismatches}${mismatch_names}; underflows ${underflows}; disk free ${disk_gib} GiB"
log "$body"

# Why the uptime trigger is a COUNT, not the min. uptime = 1 - hole_us/span_us,
# so on an illiquid wing strike (~1k book events over an hour) a single gap
# marker is a large fraction of the span -- 34.95% was measured on
# KXETHD-26AUG0821-T1909.99 while its ATM peer T1914.99 had 51,430 events,
# hole_us=0 and identical first/last recv_ts to the microsecond. The min is
# therefore dominated by illiquidity, not by capture health: at floor 0.999
# over ~400 tickers it fires most days, which is the alert-fatigue shape.
# The measured background of such tickers is 1/1/5/1/2 per day (Aug 1/2/3/7/9),
# while a real systemic failure hits dozens to hundreds at once -- so a count
# separates the two cleanly where the min cannot. Everything is still reported
# every night; only the buzz threshold moved.
if (( incomplete > 0 || mismatches > 0 || underflows > 0 || low_uptime > KDP_DIGEST_LOW_UPTIME_MAX )); then
  _ntfy urgent warning "kdp-digest PROBLEMS -- $body"
else
  _ntfy low white_check_mark "kdp-digest all green -- $body"
fi
