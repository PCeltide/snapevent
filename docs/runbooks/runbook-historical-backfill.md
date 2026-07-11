# Runbook — full-range trade backfill, streamed to Drive

How to capture a series' **entire** trade history over a date range and stream it
to Google Drive day-by-day, keeping local disk flat. Built for the all-2026 KXBTCD
pull; generic over any series. **Run on the server** (steadier internet, always-on,
rclone + toolchain already there). The driver is
**`scripts/stream-backfill.sh`**.

## Why a single run covers everything

Kalshi splits data at a moving ~3-month **historical cutoff** (`GET /historical/cutoff`,
keyed on `market_settled_ts`):
- `GET /markets` + `GET /markets/trades` (**live** tier) return only data *after* the cutoff.
- `GET /historical/markets` + `GET /historical/trades` (**archive** tier) hold everything *before* it.

`stream-backfill.sh` runs **both tiers over the full range**. Each tier naturally
returns only its half (the other half comes back empty = a clean no-op), and a given
market is in exactly one tier — so there's no cutoff to compute, no seam to patch, and
no double-fetch. All endpoints are **public/no-auth**; only rclone (Drive) is needed.

## Prerequisites (server)

- Repo on `main`, up to date: `cd ~/snapevent && git pull`.
- `~/.cargo/bin` on PATH; rclone configured with the `gdrive:` remote (or pass
  `RCLONE_CONF=/path/to/rclone.conf`). GNU coreutils `date` (standard on Ubuntu).
- No Kalshi credentials required.

## Run it

```bash
cd ~/snapevent
tmux new -s bf            # so it survives disconnects (detach: Ctrl-b d)

SERIES=KXBTCD \
MIN_CLOSE=2026-01-01 \
MAX_CLOSE=2026-06-03 \
CHUNK_DAYS=1 \
MIN_VOLUME=1 \
RATE=10 \
REMOTE=remote:kdp/btc-trades/all-2026 \
bash scripts/stream-backfill.sh 2>&1 | tee ~/stream-backfill.log
```

Per day, per tier, it does: `backfill` -> `kdp-process` (Parquet) -> `tar.gz` ->
`rclone copy` to `REMOTE/<day>.<tier>.tar.gz` -> verify it landed -> **delete local**.
Peak local disk ~= one day. The archive enumeration runs once up front; its log line
`historical discover complete ... earliest_close=...` prints the series' **true inception**
(if later than MIN_CLOSE, that's simply the real floor — the series didn't exist earlier).

**Resumable:** safe to Ctrl-C / re-run; completed `<tier>:<day>` chunks are skipped
(recorded in `$WORK/.chunks-done`), the in-flight day re-fetches cleanly. The in-client
**transient-retry** (reset/429/5xx + backoff) means a dropped connection won't abort it.

Tuning: `CHUNK_DAYS` trades peak-disk vs per-day overhead (1 = one tarball per day, easy
to verify); `RATE` 10 GET/s is conservative; `MIN_VOLUME=0` keeps untraded strikes too.

## Resulting Drive layout

```
REMOTE/
  _meta/markets-archive.jsonl        full archive enumeration (+ inception in the run log)
  _meta/<day>.<tier>.markets.jsonl   per-day strike metadata (close/settle/result/volume)
  <YYYY-MM-DD>.archive.tar.gz        Parquet for that day's pre-cutoff markets
  <YYYY-MM-DD>.live.tar.gz           Parquet for that day's post-cutoff markets
```

Most days have exactly one of `.archive`/`.live`; only days near the cutoff may have
both. Extract a day's tarball -> `<ticker>/trades.parquet` (+ manifest.json) per strike.

## Verify completeness

After the run (or on any day), pull + extract a few days and run the hourly-completeness
check — it reports distinct hourly events (00-23) per day:

```bash
mkdir -p /tmp/chk && cd /tmp/chk
rclone copy remote:kdp/btc-trades/all-2026/2026-04-15.live.tar.gz .
mkdir -p d && tar xzf 2026-04-15.live.tar.gz -C d
bash ~/snapevent/scripts/verify-hourly-days.sh d KXBTCD
```

Expect **24 hours** on normal days. KXBTCD has a **weekly maintenance window** — every
7th day (observed: Thu) is missing hours **02-05 ET** = **20 hours**, which is correct,
not a gap (those markets don't exist; check `markets.jsonl` to confirm). Range edges
(MIN/MAX_CLOSE) are partial by design. Anything else < 24 with no `markets.jsonl` reason
is a real shortfall — re-run that day (it's resumable).
