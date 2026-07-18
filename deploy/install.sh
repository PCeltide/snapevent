#!/usr/bin/env bash
#
# deploy/install.sh — provision kdp on a fresh Ubuntu server. Run as root from
# the cloned repo root:   sudo bash deploy/install.sh
#
# Idempotent. Does the NON-secret setup (packages, user, dirs, binaries, units,
# scripts, ntp, firewall) and then prints exactly which SECRET files you still
# need to place by hand (they are deliberately not in git).
#
set -Eeuo pipefail
[[ $EUID -eq 0 ]] || { echo "run as root: sudo bash deploy/install.sh"; exit 1; }
REPO="$(cd "$(dirname "$0")/.." && pwd)"
say() { printf '\n==> %s\n' "$*"; }

say "packages"
apt-get update -qq
apt-get install -y -qq chrony jq rclone ufw tar gzip curl ca-certificates >/dev/null

say "user + directories"
id kdp &>/dev/null || useradd --system --create-home --home-dir /home/kdp --shell /usr/sbin/nologin kdp
install -d -o kdp -g kdp /opt/kdp/bin /var/lib/kdp /var/lib/kdp/data /var/lib/kdp/processed
# Alert throttle state (kdp-health runs as kdp; a root-owned dir here would make
# every throttle write fail silently and restore the push spam it prevents).
install -d -o kdp -g kdp /var/lib/kdp/alert-state
install -d -m 750 -o kdp -g kdp /etc/kdp /etc/kdp/sessions /etc/kdp/schedules
install -d -m 700 -o kdp -g kdp /home/kdp/.config /home/kdp/.config/rclone

say "binaries"
if [[ -x "$REPO/target/release/kdp-cli" && -x "$REPO/target/release/kdp-process" ]]; then
  install -o kdp -g kdp -m 755 "$REPO/target/release/kdp-cli"     /opt/kdp/bin/kdp-cli
  install -o kdp -g kdp -m 755 "$REPO/target/release/kdp-process" /opt/kdp/bin/kdp-process
else
  echo "  !! binaries not built yet — run:  (cd $REPO && cargo build --release --workspace)  then re-run this script"
fi

say "scripts + systemd units"
install -o kdp -g kdp -m 755 \
  "$REPO/deploy/kdp-archive.sh" "$REPO/deploy/kdp-health.sh" "$REPO/deploy/kdp-settlewatch.sh" \
  "$REPO/deploy/kdp-rawsync.sh" \
  /opt/kdp/bin/
install -m 644 \
  "$REPO/deploy/kdp-capture@.service" \
  "$REPO/deploy/kdp-archive.service" "$REPO/deploy/kdp-archive.timer" "$REPO/deploy/kdp-archive@.service" \
  "$REPO/deploy/kdp-health.service"  "$REPO/deploy/kdp-health.timer" \
  "$REPO/deploy/kdp-rawsync@.service" "$REPO/deploy/kdp-rawsync@.timer" \
  "$REPO/deploy/kdp-hourly.service" \
  "$REPO/deploy/kdp-scheduled.service" \
  /etc/systemd/system/

say "schedule files"
# Canonical JSONL schedules (one event-set per file). Don't clobber a file the
# operator may have edited on the box; copy any that are missing.
if compgen -G "$REPO/deploy/schedules/*.jsonl" >/dev/null; then
  for f in "$REPO/deploy/schedules/"*.jsonl; do
    dest="/etc/kdp/schedules/$(basename "$f")"
    if [[ ! -f "$dest" ]]; then
      install -o kdp -g kdp -m 644 "$f" "$dest"
      echo "  installed $(basename "$f")"
    else
      echo "  kept existing $(basename "$f") (not overwritten)"
    fi
  done
fi

say "config + ntp + firewall"
if [[ ! -f /etc/kdp/kdp.env ]]; then
  install -o kdp -g kdp -m 600 "$REPO/deploy/kdp.env.example" /etc/kdp/kdp.env
  echo "  created /etc/kdp/kdp.env from the example — EDIT IT (KALSHI_API_KEY_ID, KDP_TICKERS, KDP_RCLONE_REMOTE, KDP_ALERT_WEBHOOK)"
fi
timedatectl set-ntp true || true
ufw allow OpenSSH >/dev/null 2>&1 || true
ufw --force enable >/dev/null 2>&1 || true

systemctl daemon-reload
systemctl enable --now kdp-archive.timer kdp-health.timer || true

# --- report what secrets are still missing (NOT in git) -------------------
say "remaining manual steps (sensitive — migrate these, never commit):"
todo=0
if [[ ! -f /etc/kdp/kalshi_private_key.pem ]]; then
  echo "  [ ] RSA private key  ->  /etc/kdp/kalshi_private_key.pem   (chmod 600, chown kdp:kdp)"; todo=1
fi
if grep -q '^KALSHI_API_KEY_ID=00000000' /etc/kdp/kdp.env 2>/dev/null; then
  echo "  [ ] set KALSHI_API_KEY_ID and KDP_TICKERS in /etc/kdp/kdp.env"; todo=1
fi
if [[ ! -s /home/kdp/.config/rclone/rclone.conf ]]; then
  echo "  [ ] rclone Google Drive credential  ->  /home/kdp/.config/rclone/rclone.conf   (runbook section 4)"; todo=1
fi
if (( todo == 0 )); then
  echo "  none — all secrets present. Start a capture:  systemctl start kdp-capture@<session>"
else
  echo "  ...then:  systemctl start kdp-capture@<session>"
fi
# Report each optional unit's REAL state — a re-run on a live box was claiming
# "(DISABLED)" about units that were enabled and running.
unit_note() {  # $1 = unit, $2 = hint to print only when it's disabled
  if systemctl is-enabled --quiet "$1" 2>/dev/null; then
    echo "  $1 installed, ENABLED ($(systemctl is-active "$1" 2>/dev/null || true)) — restart it to pick up the new binary."
  else
    echo "  $1 installed (DISABLED). $2"
  fi
}
unit_note kdp-hourly.service    "Start forward hourly capture with: systemctl enable --now kdp-hourly"
unit_note kdp-scheduled.service "Set KDP_SCHEDULE_FILE in kdp.env, then: systemctl enable --now kdp-scheduled"
echo
echo "done."
