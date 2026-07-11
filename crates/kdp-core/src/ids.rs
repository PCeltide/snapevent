//! Identifier and enumeration primitives shared across kdp records.
//!
//! These are the small, I/O-free building blocks every record is keyed and
//! tagged by: the market [`Ticker`], the normalized-to-UTC [`Timestamp`], and
//! the market-structure enums [`Side`] (yes/no book) and [`BookSide`]
//! (bid/ask). Keeping them here lets [`crate::records`] and [`crate::envelope`]
//! share one definition with one wire representation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A Kalshi market ticker, e.g. `"KXBTCD-25MAY3017-T1.5"`.
///
/// Thin newtype over `String` so a ticker can never be silently confused with
/// an arbitrary string at a call site. `#[serde(transparent)]` keeps the wire
/// representation a bare JSON string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ticker(pub String);

impl Ticker {
    /// Borrow the underlying ticker string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Ticker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A UTC instant.
///
/// Newtype over [`chrono::DateTime<Utc>`] so timestamps serialize consistently
/// as RFC 3339 regardless of how Kalshi delivered them. We normalise to UTC at
/// the edge so everything downstream is timezone-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub DateTime<Utc>);

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
}

/// Which side of a Kalshi binary market a quantity sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// The "yes" book.
    Yes,
    /// The "no" book.
    No,
}

/// Which side of the book an order rests on: the bid (buy) or the ask (sell).
///
/// Distinct from [`Side`] (which `yes`/`no` outcome book): a trade carries both
/// the taker's outcome side and, when Kalshi reports it, the book side the taker
/// hit. Wire form is lowercase `"bid"`/`"ask"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookSide {
    /// The buy side of the book.
    Bid,
    /// The sell side of the book.
    Ask,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_is_a_transparent_string() {
        let t = Ticker("ABC".to_string());
        assert_eq!(serde_json::to_string(&t).expect("serialize"), "\"ABC\"");
    }

    #[test]
    fn side_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&Side::Yes).expect("serialize"),
            "\"yes\""
        );
        assert_eq!(
            serde_json::to_string(&Side::No).expect("serialize"),
            "\"no\""
        );
    }

    #[test]
    fn book_side_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&BookSide::Bid).expect("serialize"),
            "\"bid\""
        );
        assert_eq!(
            serde_json::to_string(&BookSide::Ask).expect("serialize"),
            "\"ask\""
        );
    }
}
