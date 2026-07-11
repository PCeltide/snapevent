//! The domain records kdp captures: order-book snapshots, deltas, and trades.
//!
//! These are the decoded, typed forms of Kalshi's market-data messages, built
//! from the lossless [`crate::units`] (prices in micro-dollars, sizes in
//! centi-contracts) and keyed by [`crate::ids`] primitives. They are the
//! payloads carried inside a stored [`crate::envelope::Envelope`]; this module
//! defines their shape and serde contract only — decoding from the wire lives in
//! `kdp-kalshi`, persistence in `kdp-store`.

use serde::{Deserialize, Serialize};

use crate::ids::{BookSide, Side, Ticker, Timestamp};
use crate::units::{MicroDollars, QtyDelta, RestingQty};

/// A single resting price level in the order book.
///
/// `price` is in micro-dollars and `quantity` in centi-contracts (see
/// [`crate::units`]); both serialize as bare integers in the stored JSONL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceLevel {
    /// Price of this level, in micro-dollars.
    pub price: MicroDollars,
    /// Resting quantity at this level, in centi-contracts.
    pub quantity: RestingQty,
}

/// A full L2 order-book snapshot for one market at one instant.
///
/// Emitted when a WebSocket subscription starts (and re-emitted after any
/// sequence gap or reconnect). [`OrderbookDelta`]s are applied on top of the
/// most recent snapshot to reconstruct the live book. The wire snapshot carries
/// no timestamp, so `ts` is the receive time stamped at the edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderbookSnapshot {
    /// Market this book belongs to.
    pub ticker: Ticker,
    /// When the snapshot was observed (receive time).
    pub ts: Timestamp,
    /// Resting orders on the "yes" side, by price level.
    pub yes: Vec<PriceLevel>,
    /// Resting orders on the "no" side, by price level.
    pub no: Vec<PriceLevel>,
}

/// An incremental change to a single order-book price level.
///
/// `delta` is the signed change in resting quantity (centi-contracts) at
/// (`side`, `price`): positive when liquidity is added, negative when removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderbookDelta {
    /// Market this change belongs to.
    pub ticker: Ticker,
    /// When the change was observed.
    pub ts: Timestamp,
    /// Outcome book side the change applies to.
    pub side: Side,
    /// Price level (micro-dollars) that changed.
    pub price: MicroDollars,
    /// Signed change in resting quantity at this level (centi-contracts).
    pub delta: QtyDelta,
}

/// An executed trade printed on the public tape.
///
/// `taker_book_side` and `trade_id` are optional because not every source
/// (live WS vs REST tape) reports them; absent values are omitted from the
/// stored JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Trade {
    /// Market the trade occurred in.
    pub ticker: Ticker,
    /// When the trade printed.
    pub ts: Timestamp,
    /// Execution price, in micro-dollars.
    pub price: MicroDollars,
    /// Number of contracts traded, in centi-contracts.
    pub count: RestingQty,
    /// Outcome side the aggressor (taker) was on.
    pub taker_side: Side,
    /// Book side the taker hit (bid/ask), when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taker_book_side: Option<BookSide>,
    /// Kalshi's trade identifier, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{BookSide, Side, Ticker, Timestamp};
    use crate::units::{MicroDollars, QtyDelta, RestingQty};
    use chrono::{DateTime, Utc};

    fn sample_ts() -> Timestamp {
        // Fixed instant so the test is deterministic (no wall-clock reads).
        "2026-05-30T12:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("valid RFC3339")
            .into()
    }

    #[test]
    fn snapshot_round_trips_through_json() {
        let snap = OrderbookSnapshot {
            ticker: Ticker("KXTEST-26MAY30-T1".to_string()),
            ts: sample_ts(),
            yes: vec![PriceLevel {
                price: MicroDollars(600_000),
                quantity: RestingQty(10_000),
            }],
            no: vec![PriceLevel {
                price: MicroDollars(400_000),
                quantity: RestingQty(25_000),
            }],
        };
        let json = serde_json::to_string(&snap).expect("serialize");
        let back: OrderbookSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(snap, back);
    }

    #[test]
    fn price_level_serializes_units_as_bare_integers() {
        let level = PriceLevel {
            price: MicroDollars::parse("0.0800").expect("price"),
            quantity: RestingQty::parse("300.00").expect("qty"),
        };
        let value = serde_json::to_value(level).expect("to_value");
        assert_eq!(
            value,
            serde_json::json!({ "price": 80_000, "quantity": 30_000 })
        );
    }

    #[test]
    fn delta_round_trips_with_signed_quantity() {
        let delta = OrderbookDelta {
            ticker: Ticker("KXTEST".to_string()),
            ts: sample_ts(),
            side: Side::Yes,
            price: MicroDollars::parse("0.0800").expect("price"),
            delta: QtyDelta::parse("-5.00").expect("delta"),
        };
        let json = serde_json::to_string(&delta).expect("serialize");
        let back: OrderbookDelta = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(delta, back);
        assert_eq!(back.delta, QtyDelta(-500), "signed quantity preserved");
    }

    #[test]
    fn trade_round_trips_with_optional_fields_present() {
        let trade = Trade {
            ticker: Ticker("KXTEST".to_string()),
            ts: sample_ts(),
            price: MicroDollars::parse("0.5000").expect("price"),
            count: RestingQty::parse("12.00").expect("count"),
            taker_side: Side::Yes,
            taker_book_side: Some(BookSide::Bid),
            trade_id: Some("abc-123".to_string()),
        };
        let json = serde_json::to_string(&trade).expect("serialize");
        let back: Trade = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(trade, back);
    }

    #[test]
    fn trade_round_trips_with_optional_fields_absent() {
        let trade = Trade {
            ticker: Ticker("KXTEST".to_string()),
            ts: sample_ts(),
            price: MicroDollars(500_000),
            count: RestingQty(1_200),
            taker_side: Side::No,
            taker_book_side: None,
            trade_id: None,
        };
        let json = serde_json::to_string(&trade).expect("serialize");
        let back: Trade = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(trade, back);
    }
}
