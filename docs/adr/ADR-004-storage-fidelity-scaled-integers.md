# ADR-004 — Storage fidelity: lossless scaled integers + capture envelope

- **Status:** Accepted
- **Date:** 2026-05-30
- **Deciders:** project owner + Claude (Phase 2 brainstorming)
- **Supersedes:** the integer-cent assumption baked into the Phase-1 `kdp-core`
  types (`price: u8` cents, `quantity: u32`, `delta: i32`).

## Context

Phase 1 modeled order-book prices as integer cents (`u8`, `1..=99`) and quantities
as whole integers. Phase 2 verification against Kalshi's live protocol
(docs.kalshi.com, 2026-05-30) showed the wire format is **fixed-point decimal
strings**, not integer cents:

- Orderbook snapshot levels: `yes_dollars_fp` / `no_dollars_fp` as
  `[["0.0800","300.00"], …]` — price is a **decimal-dollar string with up to 6
  decimal places**; quantity is a **fixed-point string with 2 decimals**
  (fractional contracts exist via Kalshi's `_fp` representation).
- Orderbook delta: `price_dollars` (string), `delta_fp` (string, signed, 2 dp).
- REST trades: `yes_price_dollars`/`no_price_dollars` (up to 6 dp), `count_fp` (2 dp).

The Phase-1 `u8`-cents / `u32`-count model **cannot represent** sub-cent prices or
fractional sizes — it would silently lose precision. For a capture tool whose
entire purpose is a faithful record for future research, silent precision loss is
unacceptable.

A second issue: snapshots carry **no timestamp** on the wire, and downstream replay
needs to know where reconnect/sequence-gap holes sit in the stream.

## Decision

### 1. Lossless scaled-integer units (in `kdp-core::units`)

- **Price → `MicroDollars(u32)`**: dollars × 1_000_000. Captures the wire's 6-dp
  precision exactly. Binary-market range ≈ `10_000..=990_000`; `u32` (max ~4.29e9)
  has ample headroom.
- **Size → centi-contracts**: contracts × 100. Resting quantity is **unsigned**
  `RestingQty(u64)`; signed change is `QtyDelta(i64)`. Captures the 2-dp `_fp`
  precision exactly.
- Parsing is done by **integer string surgery** (split on `.`, validate digits,
  scale) — **never via `f64`**, so no floating-point rounding ever touches money.
- A value with more fractional precision than the scale, or non-numeric input, is
  a **`FixedPointError`** — the caller persists a `RawFallback` rather than
  dropping or silently zeroing. (Contrast the Python reference's `fp_to_int`, which
  silently returns 0 — deliberately not copied.)

### 2. Capture envelope (in `kdp-core::envelope`)

Every JSONL line is an `Envelope { v: u16, recv_ts: Timestamp, seq: Option<u64>,
sid: Option<i64>, kind: RecordKind }` where `RecordKind` is an internally-tagged
enum (`#[serde(tag="kind", content="data")]`) over
`Snapshot | Delta | Trade | Gap | Raw`:

- `recv_ts` — receive wall-clock (UTC), always present (solves the missing
  snapshot timestamp).
- `seq` / `sid` — the per-subscription sequence + server subscription id, for gap
  reasoning and correlation.
- `Gap(GapMarker)` — an **inline** marker (reason ∈ SeqJump | Reconnect |
  Resubscribe, `last_seq`, `observed_seq`, ticker, channel, detail) so downstream
  replay sees the hole at its exact position in the log.
- `Raw(RawFallback)` — preserves the original `msg` JSON verbatim plus the decode
  error, so nothing is ever lost.
- `v` — schema version (starts at 1) for forward evolution.

## Rationale

- **Fidelity first.** Captured data feeds future order-flow research; exact prices
  and sizes (and exact delta arithmetic for book reconstruction) matter. Integers
  are exact and replay-safe; string-surgery parsing is lossless.
- **Inline envelope > side gap-file.** A separate gap stream would force
  timestamp-merging to locate a hole relative to deltas and isn't crash-atomic with
  the data file. Inline keeps the hole positionally exact in the single append-only
  log — the whole point of a gap marker (ADR-003).
- **Never drop.** `RawFallback` upholds "no silent failures": an unexpected or
  over-precise wire value is captured for later inspection, not discarded.

## Consequences

- The Phase-1 `kdp-core` types change shape (breaking) — acceptable, nothing
  persists data yet.
- JSONL lines are slightly larger (envelope wrapper + integer fields) — negligible
  vs. the fidelity gained; ADR-002's offline Parquet derivation reclaims it.
- `kdp-store` stays **generic** (`T: Serialize`): the `Envelope` is just a `T`; the
  store learns no domain types.
- Downstream consumers parse one tagged object per line and branch on `kind`;
  `MicroDollars`/centi-contracts convert to decimals only at presentation/analysis.

## Alternatives considered

- **Raw decimal strings on every record.** Maximum fidelity, zero parse risk, but
  weak typing and defers all numeric work (incl. delta replay arithmetic) to the
  analytics phase. Rejected: loses the typed-replay benefit; the integer form is
  equally lossless given the known scales.
- **Store both raw string and parsed integer.** Maximum safety but ~2× the
  fields/disk for little gain now. Rejected for the capture build; `RawFallback`
  already preserves the raw form when parsing fails.
- **Keep integer cents (`u8`).** Rejected: silently lossy against the verified wire
  format (sub-cent prices, fractional sizes).
