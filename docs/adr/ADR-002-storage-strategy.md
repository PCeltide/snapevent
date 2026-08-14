# ADR-002 — Storage strategy: JSONL now, Parquet later

- **Status:** Accepted
- **Date:** 2026-05-30
- **Deciders:** project owner

## Context

The capture path produces a high volume of small, append-only records (order-book
snapshots, deltas, trades) that must be persisted durably and cheaply, with low
write complexity on the hot path. A later phase needs the same data in a
columnar format for efficient analytical scans.

Two distinct needs, different priorities:

1. **Capture-time write:** simple, durable, crash-safe, schema-flexible.
2. **Analysis-time read:** compact, columnar, fast to scan.

Trying to satisfy both with one on-the-hot-path format (e.g. writing Parquet
live) couples capture reliability to a complex writer and fixed schema.

## Decision

Two-stage storage:

- **Phase 1 — capture:** append-only **JSONL**, one JSON document per line,
  rotated **daily per `(ticker, channel)`**. Layout:
  `<base>/<ticker>/<channel>/<YYYY-MM-DD>.jsonl`. Implemented in `kdp-store`
  (`JsonlWriter`).
- **Phase 2 — analytics:** **batch-derive Parquet** from the JSONL offline
  (via Polars), as a separate non-hot-path job. JSONL remains the source of
  truth; Parquet is a derived, regenerable artifact.

## Rationale

- **Schema evolution is free in JSONL.** Adding a field to a record doesn't
  require migrating existing files or coordinating a writer schema change —
  old lines simply lack the field. Critical while the wire model is still
  settling.
- **Replay is trivial.** Line-oriented append-only files are the simplest
  possible event log: re-read top to bottom to reconstruct state, `tail -f` to
  watch live, `wc -l` to count. No index, no footer, no reader library needed.
- **Write durability is simple.** Open-append-flush per record (or per small
  batch) is easy to reason about and crash-safe at line granularity: a torn
  final line is detectable and discardable; everything before it is intact.
- **Daily per-stream rotation** bounds file size, makes retention/cleanup a
  per-file decision, and aligns naturally with day-partitioned analytics.
- **Bulk Parquet conversion runs offline.** The expensive, schema-fixing,
  CPU-heavy columnar encode happens in a batch job where failures are
  retryable and don't threaten live capture.

## Consequences

- **Storage is larger on disk** in JSONL than columnar/compressed Parquet.
  Accepted for Phase 1; the Parquet derivation reclaims this for analytics, and
  raw JSONL can be compressed (gzip/zstd) or aged out per file once derived.
- **Analytical queries are not run directly on JSONL** — they target the derived
  Parquet. Acceptable because analysis is a distinct, later, offline phase.
- **A derivation job must exist** before Phase 2 analytics. Tracked as Phase 2
  work; out of scope for the bootstrap.
- The `JsonlWriter` flushes per append for durability; if profiling later shows
  this is a bottleneck, batched flushing with bounded loss windows is the
  obvious tuning knob (an explicit follow-up, not a silent default).

## Alternatives considered

- **Write Parquet live.** Best read performance, but couples capture to a
  complex writer and a fixed schema, and complicates crash recovery (open
  Parquet files need clean finalization). Rejected for the hot path.
- **Embedded DB (SQLite/DuckDB) live.** Adds query power but also a write
  bottleneck, locking semantics, and a less trivial replay/inspection story
  than plain append-only files. Rejected for capture; DuckDB may be used as a
  *reader* over the derived Parquet later.

## Update (2026-05-30) — Phase 3 realized: arrow-rs, not Polars; Parquet default + Feather option

The "Phase 2 — analytics" derivation above is now built as the **`kdp-process`**
binary crate (isolated so the heavy columnar deps never touch the lean capture
tool). Two implementation refinements to the original decision:

- **Engine: `arrow-rs` (`arrow` + `parquet` v58), not Polars.** The job only
  needs to build typed columns into a `RecordBatch` and encode it; it does no
  DataFrame algebra. `arrow-rs` is the layer Polars itself sits on, so using it
  directly gives explicit schema/nullability control and a focused dependency
  surface without the DataFrame layer. Polars remains a fine *reader* of the
  output. (The output is plain Arrow/Parquet, so pandas/polars/duckdb all read
  it natively.)
- **Format: Parquet by default, Feather (`--format feather`) optional.** Both are
  written from the *same* `RecordBatch`. Parquet wins the capture-and-archive
  goal (dictionary+RLE+ZSTD → far smaller; column stats → predicate/row-group
  pushdown; archival standard). Feather (Arrow IPC) is offered for fast zero-copy
  reload at the cost of size. Measured on the 5-min MLB L2 capture, the lossless
  `book_events` table was **~12x** smaller as Parquet than the raw orderbook
  JSONL, and ~3.8x smaller than the same table as Feather — confirming the
  default.

**Lossless replacement (the point of the phase).** The derived Parquet is no
longer merely "a regenerable artifact": the `book_events` table is a **lossless**
flattened mutation log that fully reconstructs every order-book state, and raw
fallbacks/gaps are preserved in their own tables. Each ticker's `manifest.json`
carries a `complete` flag (true iff every source line decoded **and** no raw
fallbacks remain — i.e. the structured tables are the whole story); when true, the
raw JSONL is safe to delete. See `docs/data-guide.md` for the table schemas and
the replay contract.
