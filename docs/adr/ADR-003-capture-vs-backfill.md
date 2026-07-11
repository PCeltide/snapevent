# ADR-003 — Capture vs backfill: live WebSocket L2, REST trades-only history

- **Status:** Accepted
- **Date:** 2026-05-30
- **Deciders:** project owner

## Context

We want the richest possible record of market microstructure: the full L2 order
book through time, plus the trade tape. Kalshi exposes two relevant surfaces:

- A **WebSocket** feed for live data, with separate channels for order-book
  snapshots, order-book deltas, and trades.
- A **REST** API, including `/markets/trades`, which returns *historical
  trades*.

Crucially, **Kalshi exposes no historical L2 endpoint.** There is no way to ask
the API "what did the order book look like last Tuesday." Only the live feed
ever carries book state.

## Decision

- **Live capture (the primary path):** subscribe over **WebSocket** to all
  three channels — order-book **snapshots**, order-book **deltas**, and
  **trades** — and persist each as it arrives. This is the only way to obtain L2
  history, so it must run continuously to be useful.
- **Backfill (secondary):** use REST `/markets/trades` to recover the
  **historical trade tape** for past windows (e.g. to fill a gap when the
  capture process was down, or to bootstrap trades before capture began).

## Rationale

- L2 reconstruction requires an initial snapshot plus the ordered delta stream;
  both exist only on the live feed. If we don't capture them as they happen,
  they are gone.
- The trade tape, by contrast, *is* queryable after the fact via REST, so trades
  (and only trades) can be backfilled.

## Consequences

- **Backfill is trades-only. Order-book context for past windows is permanently
  lost.** If capture was not running during a period, the L2 book for that
  period cannot be recovered from any source — we can recover the trades that
  printed, but not the resting liquidity around them. **This is a fundamental
  Kalshi data limitation, not a limitation of this tool**, and it cannot be
  engineered around. The only mitigation is to maximize live-capture uptime.
- **Capture uptime is the dominant data-quality lever.** Operational priorities
  follow: robust reconnection, gap detection (sequence/heartbeat tracking), and
  alerting when a subscription drops, are first-class concerns for the capture
  service. A dropped subscription is silent, unrecoverable L2 data loss.
- **Two ingestion code paths** must exist: a streaming WebSocket consumer
  (snapshots + deltas + trades) and a paginated REST trade reader. They share
  the `kdp-core` types and the `kdp-store` sink but differ in transport and
  liveness.
- Snapshots are captured alongside deltas (not deltas alone) so the book can be
  reconstructed from any snapshot forward without needing the entire delta
  history since subscription — and to self-heal after a missed delta.

## Status of implementation

Both paths are **deferred beyond the bootstrap**. Phase 1 ships only the public,
no-auth REST `/markets` smoke probe. The WebSocket module (`kdp-kalshi::ws`) and
authenticated REST (`kdp-kalshi::auth`) are documented placeholders; the live
capture service is Phase 2.
