# Changelog

All notable, consumer-visible changes to this tool are documented here, one
section per public release. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/) (pre-1.0: minor bump = new features, patch bump
= fixes). The public repo's git history is one squashed commit per release,
so this file is the authoritative change record.

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
