//! The typed replay event — what a consumer's backtest engine actually pulls.
//!
//! Integer units only (ADR-004): prices are `MicroDollars`, sizes centi-
//! contracts. The files' convenience float columns are never read. A snapshot
//! arrives GROUPED (all rows of one `event_idx`, both sides) so consumers
//! never re-implement the grouping. Trades carry explicit provenance
//! (`TradeSource`) instead of leaving the null-`seq` writer convention for
//! every consumer to re-derive.

use kdp_core::{BookSide, MicroDollars, QtyDelta, RestingQty, Side};

/// Effective market timestamp, microseconds since epoch (the R1 clock):
/// `recv_ts_us` for WS-originated rows, `event_ts_us` (execution time) for
/// REST-backfilled trades — never fetch time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveTs(pub i64);

/// One resting price level (integer units).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    /// Price in micro-dollars.
    pub price: MicroDollars,
    /// Resting quantity in centi-contracts.
    pub qty: RestingQty,
}

/// Where a trade print came from. WS prints were observed live (authoritative
/// `recv_ts`); REST prints were backfilled after the fact (authoritative
/// execution time, approximate placement against WS book rows — the exchange
/// clock runs ~1.2s fast, see the data guide).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSource {
    Ws,
    RestBackfill,
}

/// One event in the merged, deterministic, time-ordered replay stream.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplayEvent {
    /// A full book snapshot (grouped: every level of both sides). `synthetic`
    /// is true only for the leading opener a `between(t0, _)` range emits.
    Snapshot {
        ts: EffectiveTs,
        event_idx: i64,
        yes: Vec<Level>,
        no: Vec<Level>,
        synthetic: bool,
    },
    /// An incremental book mutation: signed change at (side, price).
    Delta {
        ts: EffectiveTs,
        event_idx: i64,
        side: Side,
        price: MicroDollars,
        delta: QtyDelta,
    },
    /// A trade print, with provenance.
    Trade {
        ts: EffectiveTs,
        price: MicroDollars,
        count: RestingQty,
        taker_side: Side,
        taker_book_side: Option<BookSide>,
        trade_id: Option<String>,
        source: TradeSource,
    },
    /// A capture hole, in-band at its timestamp: the consumer must treat the
    /// book as suspect until the next (re-anchoring) snapshot.
    Gap {
        ts: EffectiveTs,
        reason: String,
        detail: String,
    },
}

impl ReplayEvent {
    /// The event's effective market timestamp (non-decreasing across a stream).
    pub fn ts(&self) -> EffectiveTs {
        match self {
            ReplayEvent::Snapshot { ts, .. }
            | ReplayEvent::Delta { ts, .. }
            | ReplayEvent::Trade { ts, .. }
            | ReplayEvent::Gap { ts, .. } => *ts,
        }
    }
}
