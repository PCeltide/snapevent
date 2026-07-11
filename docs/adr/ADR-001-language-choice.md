# ADR-001 — Language choice: Rust

- **Status:** Accepted
- **Date:** 2026-05-30
- **Deciders:** project owner

## Context

`snapevent` (kdp) captures and stores Kalshi market data: live L2
order books over WebSocket and historical trade tape over REST. The workload is
long-running, latency-sensitive on the capture path, and parsing-heavy
(decoding a high-rate binary-ish JSON protocol into typed records without
dropping messages). A later phase adds offline analytics over the captured data.

There is an existing internal Python live-trading codebase that proves the
domain is tractable in Python. The choice here is what to build the *data
capture/storage* tooling in.

Candidates considered: **Python** (familiarity, fastest iteration, reuse of the
existing client), and **Rust** (type safety, performance headroom, strong
async/IO story).

## Decision

Build kdp in **Rust** (stable toolchain, pinned via `rust-toolchain.toml`; no
nightly features permitted).

## Rationale

- **Type safety for protocol parsing.** The capture path decodes order-book
  snapshots, deltas, and trades across three channels. Rust's enums + `serde`
  make malformed/partial messages a compile-time-shaped problem rather than a
  runtime `KeyError` discovered in production at 2am. The shared wire/storage
  contract lives in one dependency-light crate (`kdp-core`).
- **Performance headroom.** A single process should sustain many concurrent
  market subscriptions without GC pauses or GIL contention. Rust + `tokio`
  gives predictable, allocation-conscious throughput with room to grow.
- **Phase 2 analytics.** The Rust/Arrow ecosystem (Polars, `arrow`, `parquet`)
  is first-class, so the offline JSONL→Parquet derivation and subsequent
  analytics stay in one language and one type system.
- **Learning goal.** The owner explicitly wants to deepen Rust proficiency; a
  greenfield, well-scoped data tool is a good vehicle.

## Consequences

**Accepted costs:**

- **Slower iteration** than Python — compile times and a stricter compiler mean
  more up-front friction per change. Mitigated by small crates and `cargo
  check` in the inner loop.
- **Learning curve** — ownership/borrowing/async will occasionally dominate
  effort early on. Accepted deliberately as part of the learning goal.
- **No code reuse** from the internal Python live-trading client. In particular the
  RSA-PSS-SHA256 request signing must be **reimplemented from scratch** against
  Kalshi's published spec (the Python version may be read for the auth *pattern*
  only — see project constraints).

**Implications adopted:**

- Stable Rust only. Anything that would require nightly (e.g. the
  `imports_granularity` rustfmt option) is treated as advisory, not a build
  dependency.
- MSRV is tracked at 1.80 (`clippy.toml`) even though the pinned toolchain is
  newer, to avoid silently adopting very new APIs.
- `unsafe_code` is forbidden workspace-wide.

## Alternatives considered

- **Python.** Fastest path and reuses the existing client, but gives up the
  type-safety and throughput properties that motivated a dedicated capture tool,
  and would keep Phase 2 analytics in the pandas (rather than Arrow-native)
  world. Rejected for the capture/storage tool; Python remains fine for ad-hoc
  exploration.
