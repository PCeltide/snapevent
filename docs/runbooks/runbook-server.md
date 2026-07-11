# Runbook — server deployment (capture 24/7 → process → Google Drive archive)

How to run kdp as an always-available data-collection service on a cheap US-East
VPS, with the compressed output archived to Google Drive and local disk reclaimed
safely. Copy-paste oriented. The deploy artifacts referenced here live in
[`/deploy`](../../deploy/).

> **L2 order-book history is live-only and unrecoverable (ADR-003).** Everything
> below optimizes for one thing: capture stays up, and **no raw is ever deleted
> until it is processed, verified, AND backed up to Drive.**

---

## The decision (what to buy / how it's shaped)

| Choice | Value | Why |
|---|---|---|
| **Host** | Hetzner Cloud **CPX21** (3 vCPU / 4 GB / 80 GB NVMe), **~$15/mo** | Capture is featherweight; the 80 GB NVMe (vs CPX11's 40 GB) is the real reason — hot cache + arrow-processing scratch above the disk-guard floor. 4 GB covers Parquet spikes. |
| **Location** | **Ashburn, VA (US-East)** | Next to Kalshi's AWS `us-east-1` → lowest RTT, fewest reconnects, most reliable capture (and best trading latency later). |
| **OS** | Ubuntu 24.04 LTS | Stable, systemd, easy Rust + rclone. |
| **Storage tiering** | local NVMe (hot) → **Google Drive** (cold archive via `rclone`) | Drive is free space you already have; the data compresses ~12×, so it's tiny. Never write live capture to Drive — only the finished output + a raw backup. |
| **Strategy path** | resize → **CPX31** (4/8/160, ~$26/mo) for a co-located strategy *process*, or split to a 2nd box | Keep the capture feed untouchable; strategy is a separate process/repo (kdp has **no trading logic, ever**). |

**Before you commit:** spin up a throwaway instance in Ashburn and measure the real
path to Kalshi — `mtr -rwz api.elections.kalshi.com` (lowest RTT + zero loss wins).
Five minutes, removes all guesswork.

---

## Architecture

```
                 ┌─────────────────── Hetzner CPX21 (Ashburn) ───────────────────┐
  Kalshi  ──WSS─▶│  kdp-cli capture  ──▶  /var/lib/kdp/data/<session>/  (raw JSONL,   │
  us-east-1 REST │  (systemd, Restart=always)        local NVMe, hot)                  │
                 │        │ nightly timer                                              │
                 │        ▼                                                            │
                 │  kdp-process  ──▶  /var/lib/kdp/processed/<session>/ (Parquet, hot) │
                 │        │                                                            │
                 │        ├── rclone copy (verified) ─────────────────┐               │
                 │        └── tar+gzip raw, rclone copy (verified) ──┐ │               │
                 │        ▼ (only after BOTH verified, opt-in)       │ │               │
                 │   prune local raw  ◀─────────────────────────────┘ │               │
                 └────────────────────────────────────────────────────┼───────────────┘
                                                                       ▼
                                              remote storage: $KDP_RCLONE_REMOTE/<session>/
                                                 ├── processed/  (Parquet + manifest)
                                                 └── raw/<session>.tar.gz  (cold backup)
```

**Why per-session, not per-day:** book reconstruction must carry across the
capture stream (a day-file can start mid-stream with a delta, not a snapshot — see
the cross-file test in `kdp-process`). So the immutable unit is a **capture
session** (starts at a snapshot, ends when you stop capture), not a calendar day.
Each session is processed **once** and uploaded immutably — no cumulative
reprocess, no overwrite-with-partial-data trap. (Continuous 24/7 capture of a
fixed market set is a one-long-session variant; see *Continuous mode* below.)

---

## Sensitive files — migrate these (they are NOT in git)

A fresh `git clone` of `PCeltide/snapevent` gives you **everything
except secrets** (`.env`, keys, `data_*/`, rclone config are all git-ignored). To
operate, place exactly these on the server by hand — none of them ever go in git:

| Secret | Source | Destination on server | Perms |
|---|---|---|---|
| **RSA private key** (Kalshi auth) | `kalshi_private_key.pem` (repo root on the dev box, git-ignored) | `/etc/kdp/kalshi_private_key.pem` | `600`, `kdp:kdp` |
| **`KALSHI_API_KEY_ID`** (UUID) | your dev `.env` | a line in `/etc/kdp/kdp.env` | `600`, `kdp:kdp` |
| **rclone Drive credential** | *generated on the server* (see §4) | `/home/kdp/.config/rclone/rclone.conf` (or a service-account JSON) | `600`, `kdp:kdp` |
| *(optional)* alert webhook URL | — | `KDP_ALERT_WEBHOOK` in `/etc/kdp/kdp.env` | `600` |

Move the two files over an encrypted channel (`scp`, or paste into an editor over
SSH); never email them. The rclone credential is **created on the server**, not
carried from the dev box (the OAuth token can be minted from any browser — §4).

> **OWNERSHIP FOOTGUN — these MUST be `kdp:kdp`, not `root` (bit us at Phase C go-live).**
> `/etc/kdp/kdp.env` and `/home/kdp/.config/rclone/rclone.conf` are read by the **`kdp`-user**
> archive child. If either is `root`-owned, the archive aborts at `set -Eeuo pipefail` with
> "Permission denied" — **silently, every hour** (capture still works, because systemd reads
> the EnvironmentFile as root before dropping privs, so the symptom is "data piles up locally,
> never reaches Drive"). **Any time you edit either file as root, re-`chown kdp:kdp` it.**
> Verify: `stat -c '%U:%G %n' /etc/kdp/kdp.env /home/kdp/.config/rclone/rclone.conf`.

## Fresh-server bring-up — ordered (this is the handoff script)

A clean server is operational in these steps; each maps to a detailed section below:

```
1. Provision the CPX21 (Ashburn, Ubuntu 24.04), SSH in as root.   # §1
2. git clone https://github.com/PCeltide/snapevent.git
3. cd snapevent && cargo build --release --workspace    # §2 (installs rustup first)
4. sudo bash deploy/install.sh        # packages, kdp user, dirs, binaries, units, timers  # §5
5. Place the 3 secrets (table above): RSA key, KALSHI_API_KEY_ID, rclone Drive.  # §3, §4
6. Edit /etc/kdp/kdp.env: KDP_TICKERS (tomorrow's match), KDP_RCLONE_REMOTE, alert webhook.
7. TEST end-to-end on a throwaway live market BEFORE the real match (see "Smoke test").
8. systemctl start kdp-capture@<session>    # at game time; stop after.
```

`install.sh` is idempotent and prints a checklist of any secret still missing, so
the operator (or a future Claude session) always knows precisely what's left.

## Smoke test (do this before the real match)

Validate the whole pipeline on any liquid live market (e.g. a current MLB game or
a BTC strike) so the *first* real run isn't your real target event:

```bash
# ~10-minute live capture into a throwaway session:
systemctl start kdp-capture@smoketest
sleep 600 && systemctl stop kdp-capture@smoketest
# force an archive run now (report-only) and watch it process + upload + verify:
sudo -u kdp KDP_PRUNE=0 /opt/kdp/bin/kdp-archive.sh
rclone lsf "$KDP_RCLONE_REMOTE/smoketest/processed"         # confirm Parquet landed on Drive
/opt/kdp/bin/kdp-process --head /var/lib/kdp/processed/smoketest/<TICKER>/book_top.parquet
```
Green across all of that = you're ready for the match. Delete the smoketest folder
locally and on Drive afterward.

---

## 1. Provision

1. Create the CPX21 in **Ashburn**, Ubuntu 24.04, add your SSH key.
2. SSH in and **start a tmux session** for all the interactive setup —
   `tmux new -s kdp-setup` — so a dropped connection can't kill the multi-minute
   `cargo build` or the smoke test (detach `Ctrl-b d`, reattach `tmux attach -t
   kdp-setup`). The capture/archive *services* run under systemd, so they don't
   need tmux — they survive disconnects and reboots on their own.
3. First-login hardening:
   ```bash
   apt update && apt -y upgrade
   apt -y install chrony jq rclone ufw build-essential pkg-config libssl-dev curl tar gzip
   timedatectl set-ntp true              # accurate recv_ts
   ufw default deny incoming && ufw default allow outgoing && ufw allow OpenSSH && ufw --force enable
   useradd --system --create-home --home-dir /home/kdp --shell /usr/sbin/nologin kdp
   install -d -o kdp -g kdp /var/lib/kdp /var/lib/kdp/data /var/lib/kdp/processed /opt/kdp/bin
   install -d -m 750 -o kdp -g kdp /etc/kdp /etc/kdp/sessions
   ```
   Only **outbound** is needed (WSS + HTTPS to Kalshi); no inbound except SSH.

## 2. Build & install kdp

Build on the box (or cross-compile and copy the two binaries):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
git clone https://github.com/PCeltide/snapevent.git /tmp/kdp && cd /tmp/kdp
cargo build --release --workspace
install -o kdp -g kdp -m 755 target/release/kdp-cli      /opt/kdp/bin/kdp-cli
install -o kdp -g kdp -m 755 target/release/kdp-process  /opt/kdp/bin/kdp-process
```
(Or copy the whole repo's `deploy/` dir to `/opt/kdp/deploy` for the units/scripts.)

## 3. Credentials

```bash
# RSA private key (the kalshi_private_key.pem PEM), root→kdp, locked down:
install -o kdp -g kdp -m 600 /path/to/kalshi_private_key.pem /etc/kdp/kalshi_private_key.pem
# Environment file (see deploy/kdp.env.example):
install -o kdp -g kdp -m 600 deploy/kdp.env.example /etc/kdp/kdp.env
$EDITOR /etc/kdp/kdp.env     # set KALSHI_API_KEY_ID, KDP_TICKERS, KDP_RCLONE_REMOTE, KDP_ALERT_WEBHOOK
```

## 4. Google Drive via rclone

Authorize once (do the OAuth on a desktop with a browser, paste the token back —
`rclone authorize "drive"`), or use a **service account** JSON (best for a
Workspace / Shared Drive). Store the config where the kdp user can read it:
```bash
# after `rclone config` (remote name "gdrive", type drive):
install -d -o kdp -g kdp -m 700 /home/kdp/.config/rclone
install -o kdp -g kdp -m 600 ~/.config/rclone/rclone.conf /home/kdp/.config/rclone/rclone.conf
sudo -u kdp rclone lsd <your-remote>:        # smoke test
```
Set `KDP_RCLONE_REMOTE=<your rclone remote:path>` (e.g. `remote:kdp`) in
`/etc/kdp/kdp.env`. REQUIRED — the archive/rawsync scripts refuse to run
without it (no default; a deploy that skips this line fails loudly, never
uploads to a wrong path).

## 5. Install the services

One shot (does steps 1's packages/user/dirs, installs binaries + units + scripts,
enables the timers, and reports any missing secret) — **idempotent**:

```bash
sudo bash deploy/install.sh
```

(It only does non-secret setup; secrets from §3/§4 are placed by hand.) If you
prefer to do it manually instead:

```bash
install -m 644 deploy/kdp-capture@.service deploy/kdp-archive.service \
               deploy/kdp-archive.timer deploy/kdp-archive@.service \
               deploy/kdp-health.service deploy/kdp-health.timer /etc/systemd/system/
install -o kdp -g kdp -m 755 deploy/kdp-archive.sh deploy/kdp-health.sh \
               deploy/kdp-settlewatch.sh /opt/kdp/bin/
systemctl daemon-reload
systemctl enable --now kdp-archive.timer kdp-health.timer
```

---

## Operate

**Capture an event** (the session name `%i` becomes the dir + Drive folder):
```bash
# tickers come from KDP_TICKERS in kdp.env, or override per session:
#   echo 'KDP_TICKERS=KXCUPMATCH-26JUN121330AAABBB-AAA,...-BBB' > /etc/kdp/sessions/2026-06-12-AAABBB.env
systemctl start kdp-capture@2026-06-12-AAABBB          # before the match
journalctl -u kdp-capture@2026-06-12-AAABBB -f          # watch it
systemctl stop  kdp-capture@2026-06-12-AAABBB          # after the match
```
On stop it writes a `.done` marker; the nightly **archive** timer then processes
that settled session, uploads Parquet **and** a raw backup to Drive (both
checksum-verified), and — once you opt in (`KDP_PRUNE=1`) — prunes the local raw.

**Scheduling a capture window** (unattended start/stop) — use transient systemd
timers. **The server runs on UTC; convert from local time** (IST = UTC+5:30, so
19:00 IST = 13:30 UTC). First set the session's tickers, then arm:

```bash
# 1. set the tickers for this session (overrides KDP_TICKERS for kdp-capture@<s>):
echo 'KDP_TICKERS=<comma,separated,markets>' > /etc/kdp/sessions/<session>.env
# 2. start ~30 min before the first event; stop generously after settlement:
systemd-run --on-calendar='YYYY-MM-DD 13:00:00 UTC' --unit=kdp-start-<session> \
  systemctl start kdp-capture@<session>
systemd-run --on-calendar='YYYY-MM-DD 19:30:00 UTC' --unit=kdp-stop-<session> \
  systemctl stop  kdp-capture@<session>
systemctl list-timers 'kdp-*'        # confirm both are armed
```
(`systemctl stop` is an explicit stop, so `Restart=always` does **not** fight it;
ExecStopPost marks the session `.done` for the archiver.)

> **For a can't-miss, unrecoverable event, do not trust a bare schedule.** Arm it
> **and** be present ~15 min before the first event to confirm data is actually
> flowing (`journalctl -u kdp-capture@<session> -f` shows snapshots + deltas). If
> the timer misfired (wrong date/UTC, typo), start it by hand. Starting ~30 min
> early gives margin to notice and fix before the action.

**Settle → graceful stop → immediate process → upload (the whole flow, automatic).**
`systemctl stop` triggers the capture's clean shutdown (the unit sets
`KillSignal=SIGINT` → flush + final report, not a hard kill). Run the **settlement
watcher** alongside capture; when all targets reach a terminal status it waits a
short grace (to capture the final convergence), **gracefully stops capture, and
immediately fires the per-session archive** (`kdp-archive@<session>`: process →
report gaps/completeness → upload Parquet + raw to Drive, verified → opt-in prune):

```bash
# polls the public /markets endpoint; on settlement: grace -> stop -> archive:
systemd-run --unit=kdp-settle-<session> \
  /opt/kdp/bin/kdp-settlewatch.sh <session> <SERIES> <ticker[,ticker...]>
```

So once you start capture + the watcher, the end of the event is hands-off:
**settles → stops cleanly → Parquet on Drive within minutes.** (The nightly
`kdp-archive.timer` is now just a catch-all for any session not archived this way,
e.g. a manual stop.) You can also archive any session on demand:
`systemctl start kdp-archive@<session>` or `kdp-archive.sh <session>`.

Layer the safety nets so they all bias toward *not* losing data:
1. **watcher** → graceful stop ~3 min after settlement (the normal path);
2. its **hard backstop** (`KDP_SETTLE_MAXHOURS`, default 10 h) if it never sees a
   terminal status (e.g. an unexpected API status string);
3. the **scheduled hard-stop** timer above (set generously, hours past expected
   settlement) as the final net.

The watcher is conservative: only a *recognised* terminal status
(`closed`/`settled`/`finalized`/`determined`) stops it; a transient blip or
unreadable status keeps capturing. **Confirm the real status strings during the
smoke test** (`curl -s "$KALSHI/markets?series_ticker=<S>" | jq '.markets[].status'`)
and adjust `KDP_SETTLE_TERMINAL` if Kalshi uses different values.

**Rolling in-flight backup (mid-event safety net).** Capture writes raw JSONL to
local NVMe in real time, but the verified Parquet + raw `tar.gz` only reach Drive
at the session-end archive — so until then the only copy of an in-progress session
is this box's disk. For a can't-miss event, arm the rolling backup alongside
capture so the un-archived raw is mirrored to Drive every ~3 min:

```bash
systemctl start kdp-rawsync@<session>.timer   # additive rclone copy -> $KDP_RCLONE_REMOTE/<session>/raw-inflight/
```

It runs `rclone` **as the kdp user** (additive, no `--delete`; `--local-no-check-updated`
so copying an actively-appended file doesn't false-error), never touches the live
writer, and self-stops after capture goes inactive (one final sync). The
`raw-inflight/` copy is a safety net only — the authoritative, checksum-verified
archive is still produced by `kdp-archive.sh` at settlement — and may be pruned
once that session's archive is verified.

**Backfill the trade tape after an event:**
```bash
sudo -u kdp /opt/kdp/bin/kdp-cli backfill --series KXCUPMATCH --out /var/lib/kdp/data/2026-06-12-AAABBB
```

**Pull data back to analyze:**
```bash
rclone copy "$KDP_RCLONE_REMOTE/2026-06-12-AAABBB/processed" ./local && \
  /opt/kdp/bin/kdp-process --head ./local/<TICKER>/book_top.parquet --rows 20
```

**Run the archive manually / dry-run** (prune off by default — it only *reports*
what it would delete until you set `KDP_PRUNE=1`):
```bash
sudo -u kdp KDP_PRUNE=0 /opt/kdp/bin/kdp-archive.sh
```

---

## Data-safety contract (why this won't lose data)

The archive script (`deploy/kdp-archive.sh`) deletes raw **only** when *all* hold,
and aborts before any deletion otherwise:
1. the session's capture unit is **inactive** (never touches an in-flight session);
2. `kdp-process` succeeded and every `manifest.complete == true`;
3. the Parquet copy to Drive **and** a `rclone check` checksum verify both passed;
4. a `tar.gz` of the raw was uploaded to Drive **and** verified (cold backup);
5. `KDP_PRUNE=1` is explicitly set (default `0` = report-only until you trust it).

So even after pruning, **both** the compressed Parquet and the original raw live
on Drive. Local is just a hot cache.

## Connectivity handling (what the code already does)

In-process resilience is already built in (see `kdp-kalshi::ws::session`):
auto-reconnect with exponential backoff (500 ms → 30 s) + jitter, 45 s idle
detection, ping/pong, inline `Gap{Reconnect}`/`Gap{SeqJump}` markers (never silent),
never-drop backpressure, and a disk guard. The server adds the missing layer:
**`Restart=always`** (survives crashes/reboots) + **monitoring** (`kdp-health`
alerts on: capture down, low disk, recent gap markers). Process-restart downtime
is *not* auto-marked as a gap yet — a future enhancement.

## Continuous mode (always-capturing a fixed market set)

For ambient 24/7 capture (not per-event), run one long-lived
`kdp-capture@live` session. Because it never goes `.done`, the per-session archiver
won't touch it. Options: (a) **roll** the session daily (stop+start at a quiet
moment — brief gap, gives the archiver a settled session), or (b) keep enough local
disk and back up raw to Drive without pruning. Fully-incremental archival of a
never-ending session (windowed/checkpoint processing at snapshot boundaries) is a
future `kdp-process` enhancement.

## When strategy arrives

- **Co-locate (cheap):** `hcloud server change-type <id> cpx31` (in-place, ~1 min
  reboot), run the strategy as a **separate** systemd service with capture given
  CPU/IO priority. Capture must never be blocked by strategy logic.
- **Split (preferred once trading is real):** keep CPX21 as the untouchable capture
  box; provision a separate strategy box you can size, deploy, and cycle freely
  (a great on/off / Spot candidate). kdp provides the data; the strategy (separate
  repo — `kdp` has **no trading logic, ever**) consumes it.

## Cost

| Item | ~Monthly |
|---|---|
| CPX21 capture box (always-on) | ~$15 |
| Google Drive | $0 (existing) |
| **Total to start** | **~$15** |
| Later: CPX31 if co-locating strategy | ~$26 |
