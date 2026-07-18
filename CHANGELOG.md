# Changelog

All notable, consumer-visible changes to this tool are documented here, one
section per public release. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/) (pre-1.0: minor bump = new features, patch bump
= fixes). The public repo's git history is one squashed commit per release,
so this file is the authoritative change record.

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
