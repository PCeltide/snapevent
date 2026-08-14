# Changelog

All notable, consumer-visible changes to this tool are documented here, one
section per public release. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/) (pre-1.0: minor bump = new features, patch bump
= fixes). The public repo's git history is one squashed commit per release,
so this file is the authoritative change record.

## [Unreleased]

## [0.3.0] - 2026-08-14

### Fixed

- **Verify checks no longer report false mismatches on quiet order books.** The
  verify engine absorbs the race between the REST orderbook poll and the
  WebSocket delta stream by matching a REST observation against any replayed
  book state within a two-sided 5-second window. But the window retained only
  states established by book events *inside* those 5 seconds, and a book state
  holds until the next event — so on a strike that had not traded for minutes,
  the state the REST read actually saw had been evicted and there was nothing
  left to match against. A REST read stale by as little as a millisecond then
  produced a `mismatch`, which synthesizes a gap marker, which opens a coverage
  hole that (absent a later snapshot) runs to the end of the session. Measured
  in production at roughly one ticker per day, each losing most of its reported
  uptime. The window now also retains the newest state from before its edge —
  the one still in force there — so the tolerance applies on quiet books as it
  always did on busy ones. `match_lag_us` is clamped to the window edge, so it
  still never exceeds the documented tolerance.

- **Settlement detection now works.** The capture supervisor's settlement
  watcher polled `GET /markets?series_ticker=…`, read one 1000-row page, and
  discarded the cursor — so a unit's target markets were essentially never in
  the response, `all_settled` could never return true, and every capture unit
  ran to its `--max-hours` backstop instead of stopping when its markets
  settled. It now polls `GET /markets?event_ticker=…`: a capture cohort is an
  event, so the response is exactly the target set. The conservative "a market
  absent from the response is not terminal" rule is unchanged — it is the query
  scope that makes it safe. A truncated response, or one that comes back empty
  because the unit's event ticker is not a real API event ticker, now warns and
  refuses to conclude settlement rather than failing quietly.
- **`capture-universe` no longer misses the first minutes of every market.**
  `--status` defaulted to `open`, so a cohort was invisible to discovery until
  the instant it opened and the earliest possible arm was the following sweep.
  The default is now `open,unopened`, and the `--arm-lead-min` gate (default
  30) arms hourly cohorts off the API's `open_time` — so capture is connected
  and subscribed before the first order lands. Pass `--status open` for the
  previous behaviour. `--min-volume` is no longer applied to the `unopened`
  sweep (an unopened market has no volume, so the filter silently discarded
  every pre-open cohort), and the per-sweep page bound rises from 10 to 30
  pages, since an unopened listing runs 6-11 pages against an open listing's 1.
- **`capture-universe` no longer subscribes a ticker twice.** The per-status
  sweeps were concatenated without dedup, and Kalshi's status indexes are not
  disjoint at a transition: a cohort swept moments after it opens comes back
  from both the `open` and the still-stale `unopened` listing. First occurrence
  wins, so the freshest view of a transitioning market is the one kept.

### Added

- **Python bindings for the full-depth replay** — new `crates/kdp-load-py`
  (pyo3/maturin, abi3 ≥3.11) exposes `kdp-load` as the `kdp_load` module:
  `Loader(dir, allow_incomplete=False)` with the same R3 completeness gate
  as `kdp-data` (typed `IncompleteData`), `events()` / `between(t0, t1)`
  iterators of integer-unit dicts, and `book_at(t_us)` — the full ladder at
  any instant with `suspect_gaps` carrying unresolved capture holes. Build
  with `scripts/check-load-py.ps1` (Rust toolchain required); worked
  example `python/examples/full_depth_replay.py` runs against the committed
  fixture. The crate lives outside the cargo workspace (pyo3's generated
  FFI vs the workspace `forbid(unsafe_code)` lint); it contains no
  hand-written unsafe.
- **`kdp-cli capture-universe`** — declarative breadth capture: sweep a set
  of series, group non-terminal markets into settlement cohorts, arm each
  new one over the existing capture/settle/archive spine, bounded by
  `--max-units`. Bounded-window instances via `--until <RFC3339 |
  YYYY-MM-DD>` (a bare date is inclusive) or `--for <duration>` (resolved to
  an absolute bound at launch, so a restart can never extend the window);
  an arm-at-inferred-start gate (`--arm-lead-min`, default 30) defers a
  cohort with a ticker-inferred start until shortly before it; clash-slot
  substitution (`--clash-sub on|off`, default `on`) arms the daily/weekly
  contract that owns a shared-expiry hourly slot in its final ~70 minutes
  instead of leaving it a hole (Long/monthly-annual cadence is always
  skipped); `--checkpoint-cmd` runs a daily raw checkpoint on long windows.
  See `docs/runbooks/runbook-universe.md`.
- **`kdp-cli catalog --series`** drill-down now ends with a ready-to-paste
  `capture-universe` suggestion (unbounded, `--until`-bounded, and the
  `/etc/kdp/universes/<name>.env` opt-in form).
- **`manifest.json` gains `span_us`/`hole_us`** (schema-additive, no version
  bump) — capture span and unioned hole time, the same accounting
  `kdp-data`'s `coverage()` uses, so uptime (`1 - hole_us/span_us`) can be
  read straight off a manifest without a Python dependency. See the data
  guide §9 for the null/0/absent semantics.
- **Deploy: universe supervisor + digest.** `kdp-universe@.service` (one
  instance per universe, `Restart=on-failure`), `deploy/universes/
  crypto.env.example`, `kdp-checkpoint.sh` (daily raw checkpoint to
  `raw-inflight/`), and `kdp-digest.timer`/`kdp-digest.service` (nightly
  06:30 UTC capture-health ntfy summary — `KDP_DIGEST_UPTIME_FLOOR`,
  `KDP_DIGEST_WINDOW_HOURS`). `kdp-archive.sh` now purges `raw-inflight/`
  as its last step, only after the raw tar checksum-verifies on Drive.
- **CI: `load-py` job** — builds and smoke-tests the `kdp-load` Python
  bindings on every push (cherry-picked ahead of its crate landing).

### Fixed

- **`capture-universe`'s remote-prefix join.** A bare `universe-<name>`
  prefix is a local filesystem path to rclone (no `:` means local, not a
  remote), so an unset-remote run would have silently archived to the
  server's own disk instead of Drive. Never deployed — caught before the
  first `kdp-universe@` unit went live. Now joined under
  `$KDP_RCLONE_REMOTE/universe-<name>`, and the supervisor refuses to start
  when `--archive-cmd`/`--checkpoint-cmd` are configured but the remote
  is unset.
- **Cadence windows widened for DST** (review I-1). The normal daily
  measures exactly 1500 min (25h, ET-anchored) — the old inclusive top edge
  with zero headroom — and the annual fall-back-day daily measures 1560 min
  (26h, real 2025 contract), which classified `Other` and silently defeated
  clash-slot substitution. Daily is now `1380..=1620`, Weekly `9360..=10860`.
- **Sweep-recovered sessions keep their Drive namespace** (review I-2). The
  supervisor persists the universe prefix as a `.remote-prefix` marker in
  the session dir; the nightly `kdp-archive.sh` sweep (and manual no-prefix
  invocations) read it before falling back to the bare env remote, so a
  session recovered by the sweep still lands under `universe-<name>/` and
  its `raw-inflight/` checkpoints are still purged.
- **A windowed instance rebooted past its bound no-ops cleanly** (review
  M-2): a bound already past at launch counts as "bound reached" (loud
  warn, exit 0) instead of an error that `Restart=on-failure` +
  `StartLimitIntervalSec=0` would turn into an infinite 5s crash loop.
- **`--arm-lead-min` is clamped to `[0, 10080]`** (review M-1), closing an
  implicit `chrono::Duration::minutes` overflow-panic path on absurd
  operator input.
- **`kdp-digest.sh` survives malformed env values** (review M-3): a
  non-JSON-number uptime floor or non-numeric window-hours now falls back
  to the default instead of killing the digest before any ntfy push.
- **`LimitNOFILE=65536` on every capture unit** (found live, 2026-08-01
  rollout): one JSONL fd stays open per (ticker, channel) for a unit's
  life and cohorts overlap ~continuously, so a breadth capture blows
  through systemd's default 1024 soft limit — `capture-universe` over
  KXETHD+KXBTC halted a unit with `Too many open files` 16s after arming
  (the failure was loud and the unit drained/archived what it had;
  no-silent-failures held). `kdp-hourly` was latently exposed at the same
  default for two months.

## [0.2.1] - 2026-07-19

### Added

- **`kdp-load` runnable replay tour** — `cargo run -p kdp-load --example
  replay_tour` walks the full-depth replay API (open → completeness verdict →
  merged typed stream → point-in-time ladder via `between`) against the
  crate's committed fixture, so it works before you've captured anything;
  point it at any processed ticker directory with an argument. The data
  guide's `kdp-load` section now also documents that REST `verify`
  observations are not replay events (take "capture end" from the manifest,
  never from the stream's last timestamp).

### Fixed

- **`kdp-data` now indexes the day tarballs the backfill actually writes.**
  `DatasetIndex.build` only matched `YYYY-MM-DD.tar.gz`, but
  `scripts/stream-backfill.sh` emits tier-suffixed names
  (`<day>.archive.tar.gz` / `<day>.live.tar.gz`), so every real backfill tar
  was silently absent from `tar_entries()`. Both tiered shapes now match
  (found exercising the loaders against a real backfill archive); a
  cutoff-seam day's two tiers extract to separate directories.
- **Deploy alerting no longer spams on a persistent failure.** `kdp-health`
  re-pushed an identical webhook alert on every timer tick for as long as a
  problem lasted (one stuck unit produced 186 identical pushes in 24 h). It
  now logs every occurrence but pushes each distinct subject at most once per
  `KDP_ALERT_THROTTLE_SEC` (default 3600), sends one "recovered" all-clear on
  the first fully-clean run, and tags pushes with ntfy `Title`/`Priority`/
  `Tags` headers (failures urgent, routine archive progress silent). New
  knobs: `KDP_ALERT_THROTTLE_SEC`, `KDP_ALERT_STATE_DIR`.
- `deploy/install.sh` reported optional units as `(DISABLED)` even when they
  were enabled and running; it now reports their real state.

## [0.2.0] - 2026-07-12

### Added

- **`kdp-cli catalog`** — answer "what should I capture?" without knowing
  Kalshi: browse categories → series → live markets, ranked by traded
  volume (`catalog`, `catalog --category NAME`, `catalog --series TICKER`);
  the series drill-down shows the live picture (open markets, lifetime +
  24h volume, top movers) and ends in a ready-to-paste `capture-universe`
  command. Public endpoints, no credentials needed.
- **REST order-book cross-verification** (capture hardening): capture now
  periodically fetches the venue's own REST order book for every session
  ticker (`--verify-interval`, default 900 s, `0` disables; batched, ≤100
  tickers/request) and persists each observation inline; `kdp-process` diffs
  it against the replayed book within a ±5 s tolerance window and writes a
  new `verify` table (`matched` / `mismatch` / `skipped_gap` / `truncated`).
  A mismatch also synthesizes a `gaps` row (`reason: "verify_mismatch"`) and
  a warning — catching venue-side emission bugs and decode/replay bugs that
  sequence tracking structurally cannot.
- **Book underflow counter**: a delta driving a price level strictly below
  zero during replay is now counted (`underflows` in the manifest and on the
  replay `Book`) instead of being pruned silently.
- **Manifest fields** `verify_checks` / `verify_mismatches` /
  `verify_skipped` / `underflows` and `counts.verify`; `kdp-data`'s
  `coverage()` and `DatasetIndex.to_frame()` surface them as nullable columns
  (`null` for output processed before these existed — never a fabricated 0).
  None of them affect `complete`, which keeps its exact meaning
  (capture-to-table structural fidelity; verification is the capture-to-venue
  axis).

### Changed

- **Capture JSONL envelope version is now 2** (adds the `verify` record
  kind). v1 files remain fully readable; a pre-0.2 `kdp-process` binary
  refuses v2 captures loudly (`UnsupportedVersion`) instead of misreading
  them — rebuild both binaries together when updating a capture host.
  Processed-output schema is unchanged (`PROCESSED_SCHEMA_VERSION` stays 1;
  the new table and manifest fields are additive).

## [0.1.0] - 2026-07-11

Initial public release.

### Added

- **Capture** (`kdp-cli`): live L2 order books (snapshots + deltas) + trades
  over WebSocket into append-only JSONL, with reconnect, per-channel sequence
  tracking, and inline gap markers — nothing is ever silently dropped.
- **Supervisors**: `capture-hourly` (laddered products, hour after hour),
  `capture-scheduled` (pre-scheduled events from a declarative JSONL
  schedule), `capture-universe` (every market matching a series filter, with
  periodic re-discovery) — arm, capture, settle, archive, hands-off.
- **Backfill** (`kdp-cli backfill`): historical trade tape over REST, per
  ticker or whole series, windowed, rate-limited, resumable; `discover`
  enumerates markets.
- **Processing** (`kdp-process`): raw JSONL to per-ticker columnar tables —
  lossless `book_events`, derived `book_top`/`trades`/`gaps`, and a
  `manifest.json` with an honest `complete` flag. Parquet (ZSTD) or Feather.
  All money is scaled integers (micro-dollars / centi-contracts), never
  floats.
- **Replay** (`kdp-load`, Rust library): deterministic, time-ordered typed
  event stream over processed directories — effective-timestamp merge, trade
  dedup, point-in-time book replay with explicit gap poisoning.
- **Python access** (`kdp-data`): dataset index, R3-gated Polars loaders,
  lazy day-tar extraction, and `coverage()`/`holes()` dataset-trust
  reporting (span, unioned hole time, uptime, unresolved gaps).
- **Deployment reference** (`deploy/`): systemd units + archive scripts for
  a 24/7 capture server with verified rclone uploads and disk reclamation.
- **CI**: fmt + clippy(-D warnings) + tests and ruff + pytest, on Linux and
  Windows.
- Dual license: MIT OR Apache-2.0.
