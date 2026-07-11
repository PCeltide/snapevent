# snapevent

[![CI](https://github.com/PCeltide/snapevent/actions/workflows/ci.yml/badge.svg)](https://github.com/PCeltide/snapevent/actions/workflows/ci.yml)

Capture and storage tooling for Kalshi market data — live **L2 order books** over
WebSocket and historical **trade tape** over REST — written in Rust, plus an
offline processor that turns the raw capture into compact, analysis-ready
columnar files, a deterministic Rust replay library, and a Python dataset
package for research access.

**This repo only captures and stores data. There is no trading logic, ever.** It
is the data foundation for order-flow research.

> **Status:** capture, processing, replay, and the Python access layer are all
> live (**238 Rust tests + 34 Python tests**, gates green on Linux + Windows CI).
> It captures live L2 + trades (single sessions, hourly ladders, scheduled
> events, or a whole market universe), backfills the trade tape, processes raw
> JSONL into compressed Parquet/Feather, replays books deterministically, and
> reports dataset coverage. L2 order-book history exists **only** on the live
> feed (Kalshi sells no historical L2), so capture uptime + gap detection are
> first-class concerns (ADR-003).

## What it does

| Stage | Tool | What |
|---|---|---|
| **Capture** | `kdp-cli capture` | Live L2 order books (snapshots + deltas) + trades over WebSocket → append-only JSONL, with reconnect + inline gap markers. |
| **Supervise** | `kdp-cli capture-hourly` / `capture-scheduled` / `capture-universe` | Long-running supervisors over the same capture spine: hourly laddered products, pre-scheduled events from a JSONL schedule, or every market matching a series filter — arm → capture → settle → archive, hands-off. |
| **Backfill** | `kdp-cli backfill` | Historical trade tape over REST (cursor-paginated), per ticker or whole `--series`. |
| **Discover** | `kdp-cli discover` | Enumerate markets in a series / by status (find tickers to capture or backfill). |
| **Process** | `kdp-process` | Raw JSONL → per-ticker columnar tables (lossless `book_events`, derived `book_top`, `trades`, `gaps`, + `manifest.json`). Parquet (default) or Feather. |
| **Replay** | `kdp-load` (Rust lib) | Deterministic, time-ordered typed event stream over processed dirs: effective-timestamp merge, trade dedup, point-in-time book replay. |
| **Analyze** | `python/` (kdp-data) | Dataset index + Polars loaders + `coverage()`/`holes()` trustworthiness reporting. See [python/README.md](python/README.md). |

**New to the processed data?** Read the **[Data Guide](docs/data-guide.md)** — a
from-scratch tour of the order-book model and every output table, grounded in real
captured rows, including how to view the files quickly.

## Research context

Independent work confirms the premise this tool is built on: Kalshi exposes
no historical L2 order-book data, so live capture is the only way it ever
exists (ADR-003). Marriott (Fordham University, 2026), *Reconstructing Full
Limit Order Books for Kalshi from WebSocket Streams*, documents the same
capture-or-lose-it reality venue-wide; their dataset is available on request
and their pipeline is bespoke — this tool is an open replication vehicle for
the capture their conclusion invites. On why these markets are worth studying
at all, see Diercks, Katz & Wright, *Kalshi and the rise of macro markets*,
FEDS Working Paper 2026-010.

Design decisions are recorded as ADRs:

- [ADR-001 — Language choice (Rust)](docs/adr/ADR-001-language-choice.md)
- [ADR-002 — Storage strategy (JSONL → Parquet, realized via arrow-rs)](docs/adr/ADR-002-storage-strategy.md)
- [ADR-003 — Capture vs backfill (live L2, trades-only history)](docs/adr/ADR-003-capture-vs-backfill.md)
- [ADR-004 — Storage fidelity (lossless scaled integers)](docs/adr/ADR-004-storage-fidelity-scaled-integers.md)


## Workspace layout

```
snapevent/
├─ Cargo.toml                 # workspace manifest + pinned shared deps
├─ rust-toolchain.toml        # pins stable toolchain (no nightly)
├─ scripts/check.ps1          # Rust gate: fmt + clippy(-D warnings) + test
├─ scripts/check-py.ps1       # Python gate: ruff + pytest (via uv)
├─ docs/
│  ├─ data-guide.md           # << consumer's guide to the processed data
│  ├─ adr/                    # architecture decision records (1-4)
│  ├─ runbooks/               # operate/verify how-tos (server, capture, backfill)
│  └─ dev-context/            # living status, decisions, conventions
├─ deploy/                    # systemd units + archive scripts for a 24/7 server
├─ python/                    # kdp-data: dataset index + Polars loaders + coverage
└─ crates/
   ├─ kdp-core    (lib)  pure serde + chrono domain types, no I/O
   ├─ kdp-kalshi  (lib)  Kalshi REST + WebSocket + RSA-PSS auth -> kdp-core types
   ├─ kdp-store   (lib)  append-only JSONL writer with daily rotation
   ├─ kdp-cli     (bin)  capture / supervise / backfill / discover driver
   ├─ kdp-process (bin)  offline JSONL -> compressed columnar processor
   └─ kdp-load    (lib)  deterministic typed replay over processed dirs
```

Dependency direction: `kdp-cli` → `kdp-kalshi` → `kdp-core`; `kdp-cli` →
`kdp-store` → `kdp-core`; `kdp-process` → `kdp-core`; `kdp-load` → `kdp-core`
only (leaf library). The pure types crate depends on nothing in the workspace,
and `kdp-kalshi` must not depend on `kdp-store` (the protocol client stays
pure; capture orchestration lives in the CLI).

## Prerequisites

- **Rust stable, 1.80+** (bootstrapped on 1.96). Install via
  [rustup](https://rustup.rs/); the pinned toolchain is auto-installed on first
  `cargo` invocation.
- On **Windows**, the MSVC toolchain needs the **Visual C++ Build Tools** (linker).
  TLS uses the platform-native stack (schannel) — no OpenSSL needed.
- Components: `rustfmt`, `clippy`. For IDE use: `rustup component add rust-analyzer`.

## Build, lint, test

```sh
cargo build --workspace
cargo clippy --all-targets -- -D warnings   # lint; warnings are errors
cargo test --all
```

Or run all three as one gate (pre-commit / CI):

```powershell
powershell -File scripts/check.ps1
```

## Quick start

```sh
# Smoke: toolchain + real public wire (no credentials needed).
cargo run -p kdp-cli -- hello
cargo run -p kdp-cli -- probe          # hits public /markets, reports count

# Discover markets in a series (e.g. IPL games), then capture / backfill.
cargo run -p kdp-cli -- discover --series KXIPLGAME --status open
cargo run -p kdp-cli -- capture  --tickers <TICKER[,TICKER...]> --out data/   # live L2, needs auth
cargo run -p kdp-cli -- backfill --series KXIPLGAME --out data/               # trade tape

# Process the raw capture into compressed columnar tables.
cargo run -p kdp-process -- --in data/                 # -> data_processed/
cargo run -p kdp-process -- --in data/ --format feather
# Peek at any output table as JSON (no Python/DuckDB needed):
cargo run -p kdp-process -- --head data_processed/<TICKER>/book_top.parquet --rows 20
```

Beyond one-off sessions, the CLI runs hands-off supervisors — `capture-hourly`
(a laddered product, hour after hour), `capture-scheduled` (pre-scheduled
events from a JSONL schedule; see
[the runbook](docs/runbooks/runbook-scheduled-capture.md)), and
`capture-universe` (every market matching a series filter, re-discovering as
new ones list) — plus `ws-probe` for a raw WebSocket smoke test. Run
`cargo run -p kdp-cli` with no args for the full usage text.

The end-to-end lifecycle: **capture (live) → backfill (tape) → process →** check
each ticker's `manifest.json` for **`"complete": true` → then the raw JSONL is
safe to delete** (the lossless `book_events` table reproduces the full book). See
the [Data Guide](docs/data-guide.md) for what every file and column means.

## Run it on a server (24/7 + archive to remote storage)

To host data collection on a small US-East VPS — capture as an always-on
`systemd` service, nightly process → verified upload to any rclone remote
(Google Drive, S3, …) → reclaim local disk — see the
**[server runbook](docs/runbooks/runbook-server.md)** and the turnkey
**[`deploy/`](deploy/)** artifacts (recommended box: Hetzner CPX21 in
Ashburn, VA, ~$15/mo).

## Credentials

Public endpoints (`probe`, trade backfill, `discover`) need no auth. Live capture
and other authenticated endpoints read from the environment (never committed):

| Variable | Meaning |
|---|---|
| `KALSHI_API_KEY_ID` | Kalshi API key id (UUID). Lives in `.env` at the repo root. |
| `KDP_KALSHI_PRIVATE_KEY_PATH` | Path to the RSA private key PEM (e.g. `kalshi_private_key.pem` at the repo root). |

The CLI loads `.env` via `dotenvy`. Auth is **RSA-PSS-SHA256** request signing
(salt = digest length) over `ts_ms + METHOD + path`, required even for public WS
channels. `.env`, `*.key`, and `*.pem` are git-ignored, and the `data_*/`
capture output directories are too.

### Getting Kalshi API credentials (one-time)

Kalshi authenticates with an RSA keypair you register:

1. Generate a 2048-bit keypair (the private key never leaves your machine):

   ```sh
   openssl genrsa -out kalshi_private_key.pem 2048
   openssl rsa -in kalshi_private_key.pem -pubout -out kalshi_public_key.pem
   ```

2. In the Kalshi web app: **Account → Settings → API Keys → Create key** →
   paste the contents of `kalshi_public_key.pem`. Kalshi displays the new
   key's **API key id** (a UUID) — copy it. (Kalshi can instead generate the
   keypair for you; download the private PEM from that dialog if so.)
3. Wire the environment — `.env` at the repo root (git-ignored):

   ```sh
   KALSHI_API_KEY_ID=<the uuid>
   KDP_KALSHI_PRIVATE_KEY_PATH=kalshi_private_key.pem
   ```

## Conventions

- **Stable Rust only** — no nightly features; `unsafe_code` is forbidden workspace-wide.
- **No `unwrap()`/`expect()`/`panic!`** outside `#[cfg(test)]`. No silent failures:
  an unparseable value becomes a persisted `RawFallback` (never a dropped message
  or a silent zero); gaps are both an on-disk record and a `tracing::warn!`.
- **Money is fixed-point integers, never `f64`** — price = micro-dollars, size =
  centi-contracts, parsed by integer string surgery (ADR-004).
- **Observability from day one** — diagnostics go through `tracing`
  (`#[tracing::instrument]` on the I/O crates' public functions).
- **Shared dep versions** are pinned once in `[workspace.dependencies]`.
- **Formatting:** `max_width = 100` (`imports_granularity = "Crate"` is advisory —
  nightly-only).

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this work
by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
