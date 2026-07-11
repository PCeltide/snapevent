# Runbook — forward continuous hourly KXBTCD L2 capture (`kdp-hourly`)

How to operate + verify our own never-stopping hourly Bitcoin L2 + trade capture
(Phase C). This is the permanent, clean L2 source (a third-party reconstruction
was evaluated and rejected for crossed order books — see decisions.md; ours is
the venue's real book). **Capture + store only (ADR-003).**

## What it is

One long-lived supervisor, `kdp-cli capture-hourly`, run as `kdp-hourly.service`
(Restart=always). Each hour it:

1. **Pre-arms** (~30s early) the market opening at the next top-of-hour, taken from
   Kalshi's pre-listed `initialized` ladder (the event with the full ladder, not a
   stub). Multi-day KXBTCD variants are filtered out (life must be ~1h).
2. **Captures the full ladder AFTER open** — waits ~60s (`--open-settle`) for the
   ladder to finish listing, then subscribes to ALL ~188 strikes (default `--band 0`).
   The two-sided curation keeps the ATM trail as spot moves; no band-tracking / no
   resubscribe (the ladder spans ~+/-$9k, far wider than an hour's BTC move; one WS
   connection handles all 188 — verified). `--band N` optionally caps to +/-N
   near-money strikes around the ATM (a load limiter; ATM from real post-open prices,
   spot-anchored, NOT the median — see footguns).
3. **Captures** L2 + trades for the band via the shared `capture_session`
   (reconnect/gap-hardened `run_session`), one WS connection, into a per-hour
   session dir `<KDP_DATA_DIR>/<event_ticker>/`.
4. **Detects settlement** (polls `/markets` until every band ticker is terminal),
   waits `KDP_SETTLE_GRACE` (180s) to catch the convergence, stops that hour.
5. **Archives in the background** — spawns `kdp-archive.sh <event_ticker>`:
   `kdp-process` -> **curate two-sided** -> verified Drive upload -> opt-in prune.
   Runs concurrently with the NEXT hour, which is already capturing -> **gap-free
   seam** (two short-overlapping WS connections, ~210s overlap).
6. **Maintenance** (no market opening at a boundary, e.g. the 02-05 ET window) ->
   logs `no hourly market ... waiting` + retries next boundary. No crash, no spin.

**Curation (the discard rule):** a strike is kept iff it ever showed BOTH a yes-ask
(`yes_ask_micro`) and a no-ask (`yes_bid_micro`) — a genuine two-way market.
One-sided-all-hour strikes are dropped from the curated Parquet; the **raw tar of
the full band still backs up to Drive** (no true loss). Implemented as
`manifest.two_sided` (kdp-process) + a drop loop in `kdp-archive.sh`.

## Operate

```bash
# Foreground dry-run (dev / first hour), prune OFF, no Drive needed if --archive-cmd "":
sudo -u kdp KDP_PRUNE=0 /opt/kdp/bin/kdp-cli capture-hourly \
  --series KXBTCD --band 25 --out /var/lib/kdp/data \
  --archive-cmd /opt/kdp/bin/kdp-archive.sh

# As a service (go-live):
sudo systemctl enable --now kdp-hourly
journalctl -u kdp-hourly -f          # watch hours arm -> capture -> settle -> archive
sudo systemctl stop kdp-hourly       # SIGINT: stop arming new hours, drain in-flight
```

**Graceful shutdown:** on SIGINT (`systemctl stop` / Ctrl-C) the supervisor stops
arming new hours and signals each in-flight hour to stop capturing now; each then
writes a `.done` marker and spawns its archive before the process exits (bounded
~30s/hour). If `systemctl` kills the spawned archive child mid-run, the `.done`
marker means the nightly `kdp-archive.timer` sweep picks that hour up — no orphaned
local data.

**Flags** (all have defaults): `--series KXBTCD`, `--band 25`, `--out`
(KDP_DATA_DIR), `--grace 180`, `--poll 30`, `--idle 45`, `--max-hours 2`,
`--pre-arm 30`, `--listing-grace 300`, `--open-settle 60` (wait this long past
open for the full ladder before selecting the band — see footguns), `--buffer
8192`, `--archive-cmd /opt/kdp/bin/kdp-archive.sh` (empty string = capture only,
no archive). Service flags come from `/etc/kdp/kdp.env` (`KDP_HOURLY_SERIES`,
`KDP_HOURLY_BAND`, `KDP_DATA_DIR`, `KDP_SETTLE_GRACE/POLL`).

**Listing timing (why `--listing-grace`):** Kalshi does not reliably pre-list the
next hour before its open — at pre-arm time the market may not exist yet. The
supervisor therefore retries discovery (windowed on the hour's close_time) from
`pre-arm` until `boundary + listing-grace` (300s) before concluding it's a real
maintenance gap. So a hour listed just-in-time is still caught (capture starts when
it appears, slightly into the hour); a genuine gap resolves to "maintenance; waiting"
after the grace window.

## Verify a captured hour

- **Session dir** `<KDP_DATA_DIR>/<event_ticker>/` has ~50 strike subdirs, each with
  `orderbook/` + `trade/` JSONL.
- **Drive** `$KDP_RCLONE_REMOTE/<event_ticker>/`: `processed/` holds Parquet for the
  **two-sided strikes only**; `raw/<event_ticker>.tar.gz` holds **all** band strikes.
  `rclone lsf $KDP_RCLONE_REMOTE/<event_ticker>/processed/ | wc -l` (curated count) <= band.
- **Seam (gap-free):** the next hour's earliest `book_top` row is at/just after its
  open. `kdp-process --head .../<strike>/book_top.parquet --rows 5`.
- **Curation worked:** `journalctl -u kdp-hourly` shows
  `curated processed set (dropped N one-sided strike(s))`; deep OTM/ITM strikes are
  absent from `processed/` but present in the raw tar.
- **Gaps:** a healthy hour is `GAPLESS` in the archive notify; `Gap{Reconnect}` at the
  seam edges can be expected.

## Known checks / footguns

- **ATM band must be selected AFTER open — NOT at pre-arm (the go-live bug, fixed
  2026-06-09).** At pre-arm the event is `initialized`: (a) it has NO prices, so
  ATM-by-price is impossible and the old code fell back to the **ladder median**
  (which on BTC sits ~$7k OTM — the ladder is wide and not centered on spot); and
  (b) the near-money strikes **don't exist yet** — Kalshi lists only a partial
  high-strike set pre-open and adds the near-money strikes (with live prices) in
  the first ~minute after open. Result of the old behavior: every hour captured
  deep-OTM strikes, 0 trades, `yes_ask` pinned at $0.01. **Fix:** `await_full_ladder`
  waits past `--open-settle` (default 60s) for the ladder to stop growing, then
  `select_band` picks the near-money band from real live prices (spot anchor from
  an adjacent open hour as fallback). **Smoke check:** the `hour armed` log shows
  `anchor=Some(~spot)` and the subscribed strikes straddle spot (yes_ask ~0.50 at
  the center, not 0.01). We intentionally lose the first ~minute of each hour (those
  strikes don't exist earlier). Tunable: `--open-settle` (raise if a slow hour lists
  the ladder late; the stable-count poll caps at `open-settle + 60`).
- **EXPECTED daily "gap" at 4-5PM ET — NOT a bug.** KXBTCD has hourly + daily + weekly +
  monthly + annual contracts; the higher cadence wins at a shared expiry, so the hourly
  whose expiry is 5PM ET (the 4-5PM ET hour, closes 21:00Z in summer) is **never opened** —
  the daily owns it (the weekly on Fridays, the monthly at month-end). The supervisor logs
  `no hourly market opening within grace (maintenance/gap); waiting` for that one hour each
  day and resumes the next hour. Do not "fix" this. (Fast-follow: optionally capture the
  daily/weekly substitute for that slot — see open-items; monthly/annual are ignored.)
- **Pre-arm is now discovery-only; subscription waits for `--open-settle`.** `--pre-arm`
  just starts hour discovery ~30s early to confirm the event is listed; the actual
  band selection + WS subscribe happens after open (see the ATM footgun above), once
  the near-money ladder exists. So a pre-open `initialized` subscribe is no longer
  attempted at all.
- **Band drift (only matters if you cap with `--band N`):** the default `--band 0`
  captures ALL strikes, so spot can never escape it — no drift, no resubscribe needed.
  Only a finite `--band N` can be out-run by a big intra-hour move; raise N if so. (One
  WS connection handles all 188 strikes — 376 subs, verified 101 + all snapshots — so
  capping is purely a load choice, not a subscription-cap necessity.)
- **Per-hour Drive folders:** one folder per event (~8,760/yr). Fine for now; a daily
  rollup is a possible later refinement.
- **Disk:** disk guard floors at 3 GiB; per-hour archive+prune keeps local flat. Enable
  `KDP_PRUNE=1` only after a day of verified curated uploads (raw backs up first).
