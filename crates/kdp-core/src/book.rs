//! Pure order-book reconstruction from snapshot + delta records.
//!
//! Replays Kalshi's binary-market book: `yes` levels are bids to buy YES, `no`
//! levels are bids to buy NO (a `no` bid at price *p* is a YES *ask* at
//! `$1 − p`). The book is `price_micro -> qty_centi` per side. This is the
//! analytic core (no I/O, so it lives in kdp-core) and the ONE home of the
//! replay rule: `kdp-process` drives it to produce the `book_top` time series,
//! and `kdp-load` drives it for point-in-time book reconstruction (the R7
//! synthetic opening snapshot). Moved here from `kdp-process/src/replay.rs`.

use std::collections::BTreeMap;

use crate::{OrderbookDelta, OrderbookSnapshot, PriceLevel, Side};

/// One dollar in micro-dollars (the `yes_ask = 1 − no_bid` complement).
pub const ONE_DOLLAR_MICRO: u32 = 1_000_000;

/// Reconstructed binary-market order book.
#[derive(Debug, Default, Clone)]
pub struct Book {
    /// YES-side resting bids: price (micro-$) -> resting qty (centi-contracts).
    pub yes: BTreeMap<u32, i64>,
    /// NO-side resting bids: price (micro-$) -> resting qty (centi-contracts).
    pub no: BTreeMap<u32, i64>,
    /// Running total of YES resting size (centi-contracts), maintained
    /// incrementally so `top()` is O(1) rather than O(levels) per event.
    /// Invariant: equals `yes.values().sum()` — kept in sync by `apply_*` only;
    /// if you mutate `yes`/`no` directly, recompute these too.
    pub yes_total: i64,
    /// Running total of NO resting size (centi-contracts). See [`Book::yes_total`].
    pub no_total: i64,
    /// Count of strictly-negative residuals seen by [`Book::apply_change`] (a
    /// delta drove a level below zero — a consistency signal, not a legitimate
    /// full cancel). Cumulative for the life of the `Book`; deliberately NOT
    /// reset by [`Book::apply_snapshot_levels`] (it is a session-level diagnostic).
    pub underflows: u64,
}

/// Insert a snapshot's price levels into `map` and accumulate their sizes into
/// `total` (the `insert` return handles an unexpected duplicate price so `total`
/// stays equal to the sum of the final map values).
fn load_levels(map: &mut BTreeMap<u32, i64>, total: &mut i64, levels: &[PriceLevel]) {
    for level in levels {
        let q = level.quantity.0 as i64;
        match map.insert(level.price.0, q) {
            Some(old) => *total += q - old,
            None => *total += q,
        }
    }
}

impl Book {
    /// Reset the book to a fresh snapshot's levels.
    pub fn apply_snapshot(&mut self, snap: &OrderbookSnapshot) {
        self.apply_snapshot_levels(&snap.yes, &snap.no);
    }

    /// Reset the book from bare level slices (same rule as [`Book::apply_snapshot`],
    /// for callers that hold levels without a full record — e.g. kdp-load).
    pub fn apply_snapshot_levels(&mut self, yes: &[PriceLevel], no: &[PriceLevel]) {
        self.yes.clear();
        self.no.clear();
        self.yes_total = 0;
        self.no_total = 0;
        load_levels(&mut self.yes, &mut self.yes_total, yes);
        load_levels(&mut self.no, &mut self.no_total, no);
    }

    /// Apply an incremental delta: add the signed change at (side, price); drop
    /// the level when its resting quantity reaches zero (or below).
    pub fn apply_delta(&mut self, delta: &OrderbookDelta) {
        self.apply_change(delta.side, delta.price.0, delta.delta.0);
    }

    /// The bare-values form of [`Book::apply_delta`] (price in micro-$, signed
    /// change in centi-contracts) — one implementation of the mutation rule.
    pub fn apply_change(&mut self, side: Side, price_micro: u32, delta_centi: i64) {
        let (side, total) = match side {
            Side::Yes => (&mut self.yes, &mut self.yes_total),
            Side::No => (&mut self.no, &mut self.no_total),
        };
        let entry = side.entry(price_micro).or_insert(0);
        *entry += delta_centi;
        *total += delta_centi;
        let new = *entry;
        let underflowed = new < 0;
        if new <= 0 {
            // Level pruned: drop its (<= 0) residual from the running total.
            *total -= new;
            side.remove(&price_micro);
        }
        if underflowed {
            self.underflows += 1;
        }
    }

    /// The current top of book (best YES bid/ask + book-wide totals).
    pub fn top(&self) -> Top {
        let yes_bid = self.yes.iter().next_back().map(|(&p, &q)| (p, q));
        let no_bid = self.no.iter().next_back().map(|(&p, &q)| (p, q));
        Top {
            yes_bid_micro: yes_bid.map(|(p, _)| p),
            yes_bid_sz_centi: yes_bid.map(|(_, q)| q).unwrap_or(0),
            // best YES ask = $1 − best NO bid.
            yes_ask_micro: no_bid.map(|(p, _)| ONE_DOLLAR_MICRO.saturating_sub(p)),
            yes_ask_sz_centi: no_bid.map(|(_, q)| q).unwrap_or(0),
            yes_levels: self.yes.len() as i64,
            no_levels: self.no.len() as i64,
            yes_total_centi: self.yes_total,
            no_total_centi: self.no_total,
        }
    }
}

/// Top-of-book + book-wide aggregates at one instant.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Top {
    /// Best YES bid price (micro-$), if any.
    pub yes_bid_micro: Option<u32>,
    /// Size at the best YES bid (centi-contracts).
    pub yes_bid_sz_centi: i64,
    /// Best YES ask price (micro-$) = `$1 − best NO bid`, if any.
    pub yes_ask_micro: Option<u32>,
    /// Size at the best YES ask (centi-contracts).
    pub yes_ask_sz_centi: i64,
    /// Number of YES price levels.
    pub yes_levels: i64,
    /// Number of NO price levels.
    pub no_levels: i64,
    /// Total resting YES size (centi-contracts).
    pub yes_total_centi: i64,
    /// Total resting NO size (centi-contracts).
    pub no_total_centi: i64,
}

impl Top {
    /// Mid price in micro-$ (mean of bid/ask), if both sides are present.
    pub fn mid_micro(&self) -> Option<u32> {
        match (self.yes_bid_micro, self.yes_ask_micro) {
            (Some(b), Some(a)) => Some((b + a) / 2),
            _ => None,
        }
    }

    /// Spread in micro-$ (`ask − bid`), if both sides are present.
    pub fn spread_micro(&self) -> Option<i64> {
        match (self.yes_bid_micro, self.yes_ask_micro) {
            (Some(b), Some(a)) => Some(a as i64 - b as i64),
            _ => None,
        }
    }

    /// Book-wide resting-size imbalance in `[-1, 1]`
    /// (`(yes − no) / (yes + no)`), if there is any resting size.
    pub fn imbalance(&self) -> Option<f64> {
        let total = self.yes_total_centi + self.no_total_centi;
        if total == 0 {
            None
        } else {
            Some((self.yes_total_centi - self.no_total_centi) as f64 / total as f64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MicroDollars, PriceLevel, QtyDelta, RestingQty, Ticker, Timestamp};
    use chrono::{DateTime, Utc};

    fn ts() -> Timestamp {
        "2026-05-30T00:00:00Z"
            .parse::<DateTime<Utc>>()
            .unwrap()
            .into()
    }

    fn level(price: u32, qty: u64) -> PriceLevel {
        PriceLevel {
            price: MicroDollars(price),
            quantity: RestingQty(qty),
        }
    }

    fn snapshot(yes: Vec<PriceLevel>, no: Vec<PriceLevel>) -> OrderbookSnapshot {
        OrderbookSnapshot {
            ticker: Ticker("K".into()),
            ts: ts(),
            yes,
            no,
        }
    }

    fn delta(side: Side, price: u32, d: i64) -> OrderbookDelta {
        OrderbookDelta {
            ticker: Ticker("K".into()),
            ts: ts(),
            side,
            price: MicroDollars(price),
            delta: QtyDelta(d),
        }
    }

    #[test]
    fn snapshot_sets_top_of_book_and_yes_ask_from_no_side() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(
            vec![level(450_000, 8_003_183), level(440_000, 100)],
            vec![level(540_000, 8_427_534), level(530_000, 50)],
        ));
        let top = book.top();
        assert_eq!(top.yes_bid_micro, Some(450_000)); // $0.45
        assert_eq!(top.yes_bid_sz_centi, 8_003_183);
        assert_eq!(top.yes_ask_micro, Some(460_000)); // $1 - $0.54
        assert_eq!(top.yes_ask_sz_centi, 8_427_534);
        assert_eq!(top.mid_micro(), Some(455_000));
        assert_eq!(top.spread_micro(), Some(10_000)); // 1 cent
        assert_eq!(top.yes_levels, 2);
        assert_eq!(top.no_levels, 2);
    }

    #[test]
    fn delta_adds_to_a_level() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(vec![level(450_000, 100)], vec![]));
        book.apply_delta(&delta(Side::Yes, 450_000, 200)); // +2.00 contracts
        assert_eq!(book.yes[&450_000], 300);
    }

    #[test]
    fn delta_removes_a_level_when_it_hits_zero() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(vec![level(410_000, 2_051)], vec![]));
        book.apply_delta(&delta(Side::Yes, 410_000, -2_051));
        assert!(!book.yes.contains_key(&410_000), "emptied level is dropped");
        assert_eq!(book.top().yes_bid_micro, None);
    }

    #[test]
    fn delta_at_a_new_better_price_moves_the_top() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(vec![level(450_000, 100)], vec![]));
        book.apply_delta(&delta(Side::Yes, 460_000, 500)); // better bid
        assert_eq!(book.top().yes_bid_micro, Some(460_000));
        assert_eq!(book.top().yes_bid_sz_centi, 500);
    }

    #[test]
    fn imbalance_reflects_resting_size() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(
            vec![level(450_000, 300)],
            vec![level(540_000, 100)],
        ));
        // (300 - 100) / 400 = 0.5
        assert_eq!(book.top().imbalance(), Some(0.5));
        let empty = Book::default();
        assert_eq!(empty.top().imbalance(), None);
    }

    #[test]
    fn exact_zero_removal_does_not_count_as_underflow() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(vec![level(410_000, 2_051)], vec![]));
        book.apply_delta(&delta(Side::Yes, 410_000, -2_051)); // lands exactly on 0
        assert_eq!(book.underflows, 0);
    }

    #[test]
    fn delta_driving_a_level_below_zero_counts_as_underflow() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(vec![level(410_000, 2_051)], vec![]));
        book.apply_delta(&delta(Side::Yes, 410_000, -3_000)); // overshoots below 0
        assert_eq!(book.underflows, 1);
    }

    #[test]
    fn two_separate_underflows_accumulate() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(
            vec![level(410_000, 100), level(420_000, 100)],
            vec![],
        ));
        book.apply_delta(&delta(Side::Yes, 410_000, -300));
        book.apply_delta(&delta(Side::Yes, 420_000, -500));
        assert_eq!(book.underflows, 2);
    }

    #[test]
    fn apply_snapshot_levels_does_not_reset_underflows() {
        let mut book = Book::default();
        book.apply_snapshot(&snapshot(vec![level(410_000, 100)], vec![]));
        book.apply_delta(&delta(Side::Yes, 410_000, -300)); // underflow
        assert_eq!(book.underflows, 1);
        book.apply_snapshot(&snapshot(vec![level(450_000, 50)], vec![]));
        assert_eq!(
            book.underflows, 1,
            "snapshot is a session-level diagnostic, not reset"
        );
    }

    #[test]
    fn default_book_has_zero_underflows() {
        assert_eq!(Book::default().underflows, 0);
    }

    #[test]
    fn empty_book_has_no_top() {
        let top = Book::default().top();
        assert_eq!(top.yes_bid_micro, None);
        assert_eq!(top.yes_ask_micro, None);
        assert_eq!(top.mid_micro(), None);
        assert_eq!(top.spread_micro(), None);
    }
}
