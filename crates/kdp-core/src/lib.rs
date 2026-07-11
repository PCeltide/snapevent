//! Shared, I/O-free domain types for the Kalshi data pipeline.
//!
//! Everything in this crate is pure `serde` + `chrono` (+ `thiserror` for the
//! fixed-point parse error): no network, no disk, no async runtime. These types
//! are the wire/storage contract shared by [`kdp_kalshi`](../kdp_kalshi/index.html)
//! (which decodes them from Kalshi's REST/WebSocket feeds) and `kdp-store`
//! (which persists them as JSONL). Keeping them dependency-light and I/O-free
//! means they can be reused from tests, analytics, and future crates without
//! pulling in a runtime.
//!
//! Modules:
//! - [`ids`] — [`Ticker`], [`Timestamp`], [`Side`], [`BookSide`].
//! - [`units`] — lossless scaled-integer money/size ([`MicroDollars`],
//!   [`RestingQty`], [`QtyDelta`]) + fixed-point parsers ([`FixedPointError`]).
//! - [`records`] — [`OrderbookSnapshot`], [`OrderbookDelta`], [`Trade`],
//!   [`PriceLevel`].
//! - [`book`] — pure order-book replay ([`Book`], [`Top`]): the one home of
//!   the snapshot/delta reconstruction rule (used by kdp-process and kdp-load).
//!
//! For ergonomics the most-used types are re-exported at the crate root, so
//! downstream code can write `kdp_core::Ticker` rather than `kdp_core::ids::Ticker`.

pub mod book;
pub mod envelope;
pub mod ids;
pub mod records;
pub mod units;

pub use book::{Book, Top, ONE_DOLLAR_MICRO};
pub use envelope::{
    Envelope, EnvelopeError, GapMarker, GapReason, RawFallback, RecordKind, ENVELOPE_VERSION,
};
pub use ids::{BookSide, Side, Ticker, Timestamp};
pub use records::{OrderbookDelta, OrderbookSnapshot, PriceLevel, Trade};
pub use units::{FixedPointError, MicroDollars, QtyDelta, RestingQty};
