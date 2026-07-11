#!/usr/bin/env bash
#
# verify-hourly-days.sh - completeness check for an HOURLY Kalshi series.
#
# For a processed/backfilled tree (one dir per strike ticker, e.g.
# KXBTCD-26APR0100-T66799.99), reports how many DISTINCT hourly events (00-23)
# were captured per calendar day, flagging days that are not a full 24. Kalshi
# maintenance days are commonly 22-23 hours; anything below that is suspect.
#
# The date/hour are read from the ticker name: <SERIES>-<YY><MON><DD><HH>-T<strike>.
# That date/hour is the market's ET label.
#
# WINDOWING FOOTGUN (read this before panicking at "INCOMPLETE"):
# stream-backfill.sh writes one tarball per day windowed on close_time in
# [day, day+1) UTC, but the hour labels above are ET. So a single day's tree
# splits each ET-labeled day across TWO adjacent tarballs -> run on one day and
# two ET-days will (correctly) read partial. To check real completeness, extract
# CONSECUTIVE days into one dir and pass --stitch: interior ET-days then show 24
# (or 22-23 on a Kalshi maintenance day), and only the range's first/last ET-day
# are expected-partial ("edge"), not flagged INCOMPLETE.
#
# Usage: bash scripts/verify-hourly-days.sh [--stitch] <dir> [SERIES]
#   single day:  bash scripts/verify-hourly-days.sh data_kxbtcd_processed KXBTCD
#   stitched:    mkdir d && tar xzf 2026-03-15.*.tar.gz 2026-03-16.*.tar.gz -C d
#                bash scripts/verify-hourly-days.sh --stitch d KXBTCD
#
set -euo pipefail

STITCH=0
args=()
for a in "$@"; do
  case "$a" in
    --stitch) STITCH=1 ;;
    *)        args+=("$a") ;;
  esac
done
DIR="${args[0]:?usage: verify-hourly-days.sh [--stitch] <dir> [SERIES]}"
SERIES="${args[1]:-KXBTCD}"

if [ "$STITCH" -eq 1 ]; then
  echo "NOTE: --stitch on. Counting hours per ET-labeled day across the whole tree;"
  echo "the chronological first/last day are range edges (partial expected, not a gap)."
else
  echo "NOTE: hour labels are ET but day-tarballs are windowed on close_time in UTC,"
  echo "so a single day's tree shows two PARTIAL ET-days by design. For real"
  echo "completeness, extract consecutive days into one dir and pass --stitch."
fi

ls "$DIR" \
 | grep -oE "${SERIES}-[0-9]{2}[A-Z]{3}[0-9]{4}" \
 | sed "s/${SERIES}-//" \
 | awk '{ print substr($0,1,7), substr($0,8,2) }' \
 | sort -u \
 | awk '{ c[$1]++ } END { for (d in c) print d, c[d] }' \
 | awk 'BEGIN{split("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC",mm," ");for(i in mm)mo[mm[i]]=i}
        {y=substr($1,1,2);m=mo[substr($1,3,3)];d=substr($1,6,2);
         printf "%s%02d%s %s %s\n",y,m,d,$1,$2}' \
 | sort \
 | awk -v stitch="$STITCH" '{
        n++; lab[n]=$2; hrs[n]=$3; sumh+=$3;
     } END {
        for (i=1;i<=n;i++) {
          flag="";
          if (stitch && (i==1 || i==n)) { edges++; flag="  <-- edge (partial expected)"; }
          else if (hrs[i]==24)          full++;
          else if (hrs[i]>=22)          maint++;
          else                        { low++; flag="  <-- INCOMPLETE (<22)"; }
          printf "  %s : %2d hours%s\n", lab[i], hrs[i], flag;
        }
        printf "\n%d days | %d full(24) | %d maint(22-23) | %d incomplete(<22)",
               n, full+0, maint+0, low+0;
        if (stitch) printf " | %d edges(partial ok)", edges+0;
        printf " | %d hourly-events total\n", sumh;
     }'
