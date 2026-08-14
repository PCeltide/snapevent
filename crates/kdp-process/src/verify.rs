//! Pure diff engine: for each REST verify observation, decide whether the
//! replayed book matched the venue's book within a tolerance window.
//!
//! `kdp-process` replays a captured stream through a [`kdp_core::Book`]
//! (`process::process_ticker`). Capture also persists periodic
//! `RecordKind::Verify(OrderbookSnapshot)` records — REST orderbook
//! observations fetched independently of the WebSocket stream. Because the
//! REST fetch and the WS delta stream are two different network paths, a
//! REST observation can legitimately mismatch the replayed book by a delta or
//! two "in flight" between the fetch and the surrounding WS events even when
//! nothing is actually wrong; this module absorbs that race with a two-sided
//! time window rather than demanding the two states line up at the exact
//! receive instant. This IS the honesty gate the capture's seq-tracking
//! cannot cover: seq tracking proves no WS message was lost in transport, but
//! it cannot catch a venue-side emission bug or a decode/replay bug on our
//! side. So the comparison here is deliberately exact full-state equality
//! (every price level on every side), never a hash or a summary statistic —
//! gambling on a hash collision would undermine the very thing this module
//! exists to guarantee.
//!
//! Conventions:
//! - **Exact equality.** A "match" means the canonical `(yes, no)` level sets
//!   are identical, not merely top-of-book or close-enough.
//! - **Window semantics.** [`VERIFY_WINDOW_US`] is two-sided: a REST
//!   observation may match a replayed state up to that many microseconds
//!   before OR after the observation's receive time. `match_lag_us` is
//!   signed: negative means the match was an earlier book state (the REST
//!   fetch lagged reality), positive means a later one (the WS stream lagged
//!   the fetch).
//! - **Outcome vocabulary** (`VerifyOutcomeRow::outcome`): `"matched"` (proved
//!   equal within the window), `"mismatch"` (proved unequal — no window state
//!   matched), `"skipped_gap"` (book was suspect from an unresolved gap, so no
//!   verdict is rendered — a mismatch here would be a lie), `"truncated"`
//!   (the stream ended before the window closed, verdict withheld).
//! - **No I/O, no wall clock.** This module is pure: it only ever sees
//!   timestamps and book states its caller hands it. Callers own tracing.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use kdp_core::{Book, PriceLevel};

/// Two-sided tolerance window: a REST observation matches if the replayed book
/// equals it at any instant within this many microseconds before or after the
/// observation (absorbs the in-flight-delta race between REST fetch and WS stream).
pub const VERIFY_WINDOW_US: i64 = 5_000_000;

/// Canonical book state: ascending `(price_micro, qty_centi)` per side (BTreeMap
/// iteration order, so already sorted).
type State = (Vec<(u32, i64)>, Vec<(u32, i64)>);

/// One verify observation's verdict, ready to become a row in the manifest /
/// a report.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifyOutcomeRow {
    /// The verify record's arrival time.
    pub recv_ts_us: i64,
    /// `"matched"` | `"mismatch"` | `"skipped_gap"` | `"truncated"`.
    pub outcome: &'static str,
    /// Signed lag to the matching state; `Some` only when matched. Negative
    /// means the match was an earlier book state than the observation.
    pub match_lag_us: Option<i64>,
    /// The REST observation's yes side, JSON `[[price_micro, qty_centi], ...]`
    /// ascending by price.
    pub yes_levels_json: String,
    /// The REST observation's no side, same shape.
    pub no_levels_json: String,
    /// `""` for matched; a one-line human summary otherwise.
    pub detail: String,
}

/// A verify check that has been observed but not yet resolved: either a
/// forward book event will match it, its deadline will pass (mismatch), a gap
/// will arrive (skipped_gap), or the stream will end (truncated).
struct PendingCheck {
    /// The verify record's own receive time (becomes the row's `recv_ts_us`).
    verify_ts: i64,
    /// `verify_ts + VERIFY_WINDOW_US` — the last instant a forward book event
    /// may still rescue this check as matched.
    deadline: i64,
    /// The canonicalized REST observation.
    rest_state: State,
    /// The replayed book's canonical state at the verify point (for the
    /// mismatch detail, if this check never matches).
    book_state_at_verify: State,
    yes_levels_json: String,
    no_levels_json: String,
}

impl PendingCheck {
    fn resolve_matched(self, lag_us: i64) -> VerifyOutcomeRow {
        VerifyOutcomeRow {
            recv_ts_us: self.verify_ts,
            outcome: "matched",
            match_lag_us: Some(lag_us),
            yes_levels_json: self.yes_levels_json,
            no_levels_json: self.no_levels_json,
            detail: String::new(),
        }
    }

    fn resolve_mismatch(self) -> VerifyOutcomeRow {
        let detail = mismatch_detail(&self.book_state_at_verify, &self.rest_state);
        VerifyOutcomeRow {
            recv_ts_us: self.verify_ts,
            outcome: "mismatch",
            match_lag_us: None,
            yes_levels_json: self.yes_levels_json,
            no_levels_json: self.no_levels_json,
            detail,
        }
    }

    fn resolve_skipped_gap(self) -> VerifyOutcomeRow {
        VerifyOutcomeRow {
            recv_ts_us: self.verify_ts,
            outcome: "skipped_gap",
            match_lag_us: None,
            yes_levels_json: self.yes_levels_json,
            no_levels_json: self.no_levels_json,
            detail:
                "book turned suspect (unresolved gap) during the verify window; verdict withheld"
                    .to_string(),
        }
    }

    fn resolve_truncated(self) -> VerifyOutcomeRow {
        VerifyOutcomeRow {
            recv_ts_us: self.verify_ts,
            outcome: "truncated",
            match_lag_us: None,
            yes_levels_json: self.yes_levels_json,
            no_levels_json: self.no_levels_json,
            detail: "stream ended inside the verify window; verdict withheld".to_string(),
        }
    }
}

/// Canonicalize one side of a live `Book` (already sorted by `BTreeMap`
/// iteration order).
fn canonicalize_book_side(side: &BTreeMap<u32, i64>) -> Vec<(u32, i64)> {
    side.iter().map(|(&price, &qty)| (price, qty)).collect()
}

fn canonicalize_book(book: &Book) -> State {
    (
        canonicalize_book_side(&book.yes),
        canonicalize_book_side(&book.no),
    )
}

/// Canonicalize a REST orderbook side. REST returns best-to-worst (not sorted
/// ascending); routing through a `BTreeMap` both sorts it and defensively
/// dedups a duplicate price (last-wins), matching `Book`'s map semantics —
/// `Book` itself can never hold a duplicate price, so this only guards
/// against a malformed REST response.
fn canonicalize_rest_side(levels: &[PriceLevel]) -> Vec<(u32, i64)> {
    let mut map: BTreeMap<u32, i64> = BTreeMap::new();
    for level in levels {
        map.insert(level.price.0, level.quantity.0 as i64);
    }
    map.into_iter().collect()
}

fn canonicalize_rest(yes: &[PriceLevel], no: &[PriceLevel]) -> State {
    (canonicalize_rest_side(yes), canonicalize_rest_side(no))
}

/// `serde_json::to_string` of a `Vec<(u32, i64)>`-shaped side, ascending by
/// price. The element type here is plain primitives, so serialization cannot
/// actually fail; `unwrap_or_else` is a total, panic-free fallback rather than
/// noise-generating error plumbing for a call that cannot err in practice.
fn levels_json(levels: &[(u32, i64)]) -> String {
    serde_json::to_string(levels).unwrap_or_else(|_| "[]".to_string())
}

/// Price in micro-dollars, formatted as dollars to 6dp — exact integer
/// string surgery, no float rounding (ADR-004 spirit extends to display).
fn fmt_price_micro(price_micro: u32) -> String {
    format!("{}.{:06}", price_micro / 1_000_000, price_micro % 1_000_000)
}

/// Quantity in centi-contracts, formatted as contracts to 2dp.
fn fmt_qty_centi(qty_centi: i64) -> String {
    if qty_centi < 0 {
        let abs = qty_centi.unsigned_abs();
        format!("-{}.{:02}", abs / 100, abs % 100)
    } else {
        format!("{}.{:02}", qty_centi / 100, qty_centi % 100)
    }
}

fn fmt_side_value(v: Option<i64>) -> String {
    match v {
        Some(qty) => fmt_qty_centi(qty),
        None => "absent".to_string(),
    }
}

/// One side's first differing price level: `(price_micro, ours_qty, rest_qty)`,
/// where an absent side carries `None`.
type FirstDiff = (u32, Option<i64>, Option<i64>);

/// Diff one side: how many price levels differ (present-only-in-one, or
/// present-in-both-with-different-qty — each counts as one), plus the first
/// (lowest-price) difference for the human-readable detail.
fn side_diff(ours: &[(u32, i64)], rest: &[(u32, i64)]) -> (usize, Option<FirstDiff>) {
    let ours_map: BTreeMap<u32, i64> = ours.iter().copied().collect();
    let rest_map: BTreeMap<u32, i64> = rest.iter().copied().collect();
    let mut prices: BTreeSet<u32> = BTreeSet::new();
    prices.extend(ours_map.keys());
    prices.extend(rest_map.keys());

    let mut count = 0usize;
    let mut first: Option<FirstDiff> = None;
    for price in prices {
        let ours_qty = ours_map.get(&price).copied();
        let rest_qty = rest_map.get(&price).copied();
        if ours_qty != rest_qty {
            count += 1;
            if first.is_none() {
                first = Some((price, ours_qty, rest_qty));
            }
        }
    }
    (count, first)
}

fn side_detail(label: &str, ours: &[(u32, i64)], rest: &[(u32, i64)]) -> String {
    let (count, first) = side_diff(ours, rest);
    match first {
        None => format!("{label}: 0 level(s) differ"),
        Some((price, ours_qty, rest_qty)) => format!(
            "{label}: {count} level(s) differ (first at {}: ours {} vs rest {})",
            fmt_price_micro(price),
            fmt_side_value(ours_qty),
            fmt_side_value(rest_qty)
        ),
    }
}

fn mismatch_detail(ours: &State, rest: &State) -> String {
    format!(
        "{}; {}",
        side_detail("yes", &ours.0, &rest.0),
        side_detail("no", &ours.1, &rest.1)
    )
}

/// The verify diff engine: feeds on book events, gaps, and verify
/// observations (in stream order) and accumulates one [`VerifyOutcomeRow`]
/// per verify observation.
pub struct VerifyEngine {
    /// Recent `(recv_ts_us, canonical book state)` pairs, ascending by
    /// timestamp, pruned to `VERIFY_WINDOW_US` behind the latest book event
    /// PLUS the newest entry at-or-before that cutoff — which is the state
    /// still in force at the cutoff instant, not history (see `on_book_event`).
    window: VecDeque<(i64, State)>,
    /// The verify check awaiting a forward match, a deadline, a gap, or EOF.
    pending: Option<PendingCheck>,
    /// True once a gap has fired with no snapshot since to re-anchor the book.
    gap_pending: bool,
    rows: Vec<VerifyOutcomeRow>,
}

impl Default for VerifyEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl VerifyEngine {
    pub fn new() -> Self {
        Self {
            window: VecDeque::new(),
            pending: None,
            gap_pending: false,
            rows: Vec::new(),
        }
    }

    /// Feed a replayed book event (snapshot or delta application already
    /// applied by the caller into `book`).
    pub fn on_book_event(&mut self, recv_ts_us: i64, book: &Book, is_snapshot: bool) {
        let state = canonicalize_book(book);

        if let Some(pending) = self.pending.take() {
            if recv_ts_us > pending.deadline {
                // States after the deadline can't rescue it.
                self.rows.push(pending.resolve_mismatch());
            } else if state == pending.rest_state {
                let lag_us = recv_ts_us - pending.verify_ts;
                self.rows.push(pending.resolve_matched(lag_us));
            } else {
                // Still inside the window, still unequal: stays pending.
                self.pending = Some(pending);
            }
        }

        self.window.push_back((recv_ts_us, state));
        // Prune to the window but ALWAYS retain the newest entry at-or-before
        // the cutoff. A book state is a step function: it stays in force until
        // the next book event, so the last pre-cutoff entry is the state the
        // book was actually in AT the cutoff instant, not stale history.
        // Dropping it silently narrows the "two-sided" window to "as far back
        // as the previous book event" -- and on a thin strike where nothing has
        // traded for minutes, that is no backward window at all, which is
        // exactly where the REST-vs-WS race this module exists to absorb
        // produces its false mismatches. Costs one extra entry, never more.
        let cutoff = recv_ts_us - VERIFY_WINDOW_US;
        while self.window.len() >= 2 && self.window[1].0 <= cutoff {
            self.window.pop_front();
        }

        if is_snapshot {
            // A real WS snapshot re-anchors the book; it's no longer suspect.
            self.gap_pending = false;
        }
    }

    /// Feed an inline gap marker.
    pub fn on_gap(&mut self) {
        self.gap_pending = true;
        if let Some(pending) = self.pending.take() {
            // The book turned suspect mid-window: a mismatch verdict would be
            // a lie, so withhold it instead.
            self.rows.push(pending.resolve_skipped_gap());
        }
    }

    /// Feed a REST verify observation.
    pub fn on_verify(
        &mut self,
        recv_ts_us: i64,
        rest_yes: &[PriceLevel],
        rest_no: &[PriceLevel],
        book: &Book,
    ) {
        // A previous check's deadline has necessarily passed by now (the
        // sweep interval that produces verify records is far larger than the
        // window), so resolve it first.
        if let Some(pending) = self.pending.take() {
            if self.gap_pending {
                self.rows.push(pending.resolve_skipped_gap());
            } else {
                self.rows.push(pending.resolve_mismatch());
            }
        }

        let rest_state = canonicalize_rest(rest_yes, rest_no);
        let yes_levels_json = levels_json(&rest_state.0);
        let no_levels_json = levels_json(&rest_state.1);

        if self.gap_pending {
            // The book has been known-suspect since the last unresolved gap;
            // no verdict can be honestly rendered yet.
            self.rows.push(VerifyOutcomeRow {
                recv_ts_us,
                outcome: "skipped_gap",
                match_lag_us: None,
                yes_levels_json,
                no_levels_json,
                detail: "book state is suspect since the last unresolved gap".to_string(),
            });
            return;
        }

        let current_state = canonicalize_book(book);
        if current_state == rest_state {
            self.rows.push(VerifyOutcomeRow {
                recv_ts_us,
                outcome: "matched",
                match_lag_us: Some(0),
                yes_levels_json,
                no_levels_json,
                detail: String::new(),
            });
            return;
        }

        // No match at the current instant: scan the window newest-to-oldest
        // (smallest |lag| first) for a backward match.
        let threshold = recv_ts_us - VERIFY_WINDOW_US;
        for (ts, state) in self.window.iter().rev() {
            // The newest entry older than the threshold was still the book's
            // state AT the threshold instant (states hold until the next
            // event), so it is inside the window and must be tested before we
            // stop -- test first, break after. Everything older than IT is
            // genuinely outside.
            let established_before_window = *ts < threshold;
            if *state == rest_state {
                // Report the instant inside the window at which the two were
                // equal, not when that state was established, so |lag| can
                // never exceed VERIFY_WINDOW_US.
                let lag_us = (*ts).max(threshold) - recv_ts_us;
                self.rows.push(VerifyOutcomeRow {
                    recv_ts_us,
                    outcome: "matched",
                    match_lag_us: Some(lag_us),
                    yes_levels_json,
                    no_levels_json,
                    detail: String::new(),
                });
                return;
            }
            if established_before_window {
                break;
            }
        }

        // No backward match either: leave it pending for a forward book event,
        // a gap, the deadline, or EOF to resolve.
        self.pending = Some(PendingCheck {
            verify_ts: recv_ts_us,
            deadline: recv_ts_us + VERIFY_WINDOW_US,
            rest_state,
            book_state_at_verify: current_state,
            yes_levels_json,
            no_levels_json,
        });
    }

    /// Consume the engine, resolving a still-pending check as `truncated`.
    /// Returns all rows in verify-record order.
    pub fn finish(mut self) -> Vec<VerifyOutcomeRow> {
        if let Some(pending) = self.pending.take() {
            self.rows.push(pending.resolve_truncated());
        }
        self.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdp_core::{MicroDollars, RestingQty};

    fn book_of(yes: &[(u32, i64)], no: &[(u32, i64)]) -> Book {
        let mut book = Book::default();
        for &(price, qty) in yes {
            book.yes.insert(price, qty);
        }
        for &(price, qty) in no {
            book.no.insert(price, qty);
        }
        book
    }

    fn levels(pairs: &[(u32, i64)]) -> Vec<PriceLevel> {
        pairs
            .iter()
            .map(|&(price, qty)| PriceLevel {
                price: MicroDollars(price),
                quantity: RestingQty(qty as u64),
            })
            .collect()
    }

    #[test]
    fn matched_at_point() {
        let mut engine = VerifyEngine::new();
        let book = book_of(&[(150_000, 1_200)], &[]);
        engine.on_verify(1_000, &levels(&[(150_000, 1_200)]), &[], &book);
        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "matched");
        assert_eq!(rows[0].match_lag_us, Some(0));
    }

    #[test]
    fn matched_backward() {
        let mut engine = VerifyEngine::new();
        let old_book = book_of(&[(100_000, 500)], &[]);
        engine.on_book_event(0, &old_book, true);

        let new_book = book_of(&[(200_000, 500)], &[]);
        engine.on_verify(2_000_000, &levels(&[(100_000, 500)]), &[], &new_book);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "matched");
        assert_eq!(rows[0].match_lag_us, Some(-2_000_000));
    }

    /// The production false-positive, reproduced from the box's dump of
    /// `KXBTC-26AUG1205-B63750`: a cancel removed `no @ 0.51` (4.00 contracts)
    /// 1.4 ms before the verify record's own `recv_ts`, and the REST read we
    /// compared against was that few ms stale. The pre-cancel state is what
    /// REST saw, and it was in force for the whole preceding 10 s — but the
    /// only book event establishing it landed 10 s back, outside the 5 s
    /// window, so the old prune had evicted it and the two-sided window had
    /// nothing to match against. Thin strikes hit this and liquid ones never
    /// do: on a liquid book some event always lands inside the window and
    /// carries the state forward.
    #[test]
    fn stale_rest_read_matches_the_state_still_in_force_from_before_the_window() {
        let mut engine = VerifyEngine::new();
        let verify_ts = 1_786_522_777_128_110i64;
        let cancel_ts = 1_786_522_777_126_709i64; // 1.4 ms before the verify
        let quiet_ts = cancel_ts - 10_000_000; // last event before it: 10 s back

        // The book as REST still saw it, established 10 s before the cancel.
        let with_level = book_of(&[(490_000, 1_000)], &[(510_000, 400)]);
        engine.on_book_event(quiet_ts, &with_level, true);

        // The cancel we received and applied correctly and on time.
        let without_level = book_of(&[(490_000, 1_000)], &[]);
        engine.on_book_event(cancel_ts, &without_level, false);

        engine.on_verify(
            verify_ts,
            &levels(&[(490_000, 1_000)]),
            &levels(&[(510_000, 400)]),
            &without_level,
        );

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].outcome, "matched",
            "a REST read stale by 1.4 ms is the race the window exists to absorb"
        );
        assert_eq!(
            rows[0].match_lag_us,
            Some(-VERIFY_WINDOW_US),
            "the state was in force at the window edge; lag is clamped there, \
             never reported as the older instant it was established"
        );
    }

    /// The prune keeps exactly ONE entry from before the cutoff, not all of
    /// them — the window must not grow without bound on a busy book.
    #[test]
    fn prune_keeps_one_pre_cutoff_entry_and_no_more() {
        let mut engine = VerifyEngine::new();
        for i in 0..10i64 {
            let book = book_of(&[(100_000 + i as u32, 100)], &[]);
            engine.on_book_event(i * 1_000_000, &book, false);
        }
        // Latest event at t=9s, so the cutoff is 4s. The entry AT 4s is the
        // newest at-or-before it and survives; 0..=3s are all superseded by it
        // and go. 4s..=9s is six entries.
        assert_eq!(engine.window.len(), 6);
        assert_eq!(engine.window[0].0, 4_000_000);
    }

    #[test]
    fn matched_forward() {
        let mut engine = VerifyEngine::new();
        let book_now = book_of(&[(100_000, 500)], &[]);
        engine.on_verify(1_000, &levels(&[(200_000, 700)]), &[], &book_now);

        let book_later = book_of(&[(200_000, 700)], &[]);
        engine.on_book_event(1_000 + 1_000_000, &book_later, false);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "matched");
        assert_eq!(rows[0].match_lag_us, Some(1_000_000));
    }

    #[test]
    fn mismatch_after_window() {
        let mut engine = VerifyEngine::new();
        let t = 1_000;
        let book_now = book_of(&[(100_000, 500)], &[]);
        engine.on_verify(t, &levels(&[(200_000, 700)]), &[], &book_now);

        // Still unequal, still inside the window: stays pending.
        let book_mid = book_of(&[(150_000, 500)], &[]);
        engine.on_book_event(t + 1_000_000, &book_mid, false);

        // Past the deadline (t + VERIFY_WINDOW_US = t + 5_000_000).
        let book_late = book_of(&[(160_000, 500)], &[]);
        engine.on_book_event(t + 6_000_000, &book_late, false);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "mismatch");
        assert_eq!(rows[0].match_lag_us, None);
        assert!(!rows[0].detail.is_empty());
    }

    #[test]
    fn mismatch_resolved_by_next_verify() {
        let mut engine = VerifyEngine::new();
        let t = 1_000;
        let book_now = book_of(&[(100_000, 500)], &[]);
        engine.on_verify(t, &levels(&[(200_000, 700)]), &[], &book_now);

        // No book events after; a second verify 900s later resolves the first
        // as mismatch, then evaluates its own check independently.
        let book_second = book_of(&[(300_000, 900)], &[]);
        engine.on_verify(
            t + 900_000_000,
            &levels(&[(300_000, 900)]),
            &[],
            &book_second,
        );

        let rows = engine.finish();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].outcome, "mismatch");
        assert_eq!(rows[0].recv_ts_us, t);
        assert_eq!(rows[1].outcome, "matched");
        assert_eq!(rows[1].recv_ts_us, t + 900_000_000);
    }

    #[test]
    fn skipped_gap_at_point() {
        let mut engine = VerifyEngine::new();
        engine.on_gap();
        let book = book_of(&[(100_000, 500)], &[]);
        engine.on_verify(1_000, &levels(&[(100_000, 500)]), &[], &book);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "skipped_gap");
    }

    #[test]
    fn snapshot_clears_gap() {
        let mut engine = VerifyEngine::new();
        engine.on_gap();
        let book = book_of(&[(100_000, 500)], &[]);
        engine.on_book_event(500, &book, true); // real snapshot clears gap_pending

        engine.on_verify(1_000, &levels(&[(100_000, 500)]), &[], &book);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "matched");
    }

    #[test]
    fn skipped_gap_mid_pending() {
        let mut engine = VerifyEngine::new();
        let book = book_of(&[(100_000, 500)], &[]);
        engine.on_verify(1_000, &levels(&[(200_000, 700)]), &[], &book); // no match -> pending

        engine.on_gap(); // resolves the pending check as skipped_gap

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "skipped_gap");
    }

    #[test]
    fn truncated_at_eof() {
        let mut engine = VerifyEngine::new();
        let book = book_of(&[(100_000, 500)], &[]);
        engine.on_verify(1_000, &levels(&[(200_000, 700)]), &[], &book); // pending, never resolved

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "truncated");
    }

    #[test]
    fn window_pruning() {
        let mut engine = VerifyEngine::new();
        let old_book = book_of(&[(100_000, 500)], &[]);
        engine.on_book_event(0, &old_book, true); // pushed at ts=0

        // Superseded at 0.5s, which is BEFORE the verify's window edge at 1s,
        // so the ts=0 state was genuinely not in force anywhere inside the
        // window. Feeding this event is what makes the assertion below mean
        // what it says: without it the engine is never told the book changed,
        // so under its own step-function model the ts=0 state is still in
        // force at the edge and matching it is correct, not spurious.
        let different_current = book_of(&[(200_000, 500)], &[]);
        engine.on_book_event(500_000, &different_current, false);

        // Verify 6s after the snapshot (beyond VERIFY_WINDOW_US=5s): the ts=0
        // state must NOT count as a backward match, even though it equals rest.
        engine.on_verify(
            6_000_000,
            &levels(&[(100_000, 500)]),
            &[],
            &different_current,
        );

        // Nothing resolves it going forward: finish() must see it unresolved
        // (truncated), never a spurious "matched".
        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_ne!(rows[0].outcome, "matched");
        assert_eq!(rows[0].outcome, "truncated");
    }

    #[test]
    fn levels_json_shape() {
        let mut engine = VerifyEngine::new();
        let book = book_of(&[(100_000, 30), (200_000, 20), (300_000, 10)], &[]);
        // REST returns best-to-worst (descending here).
        let rest_yes = levels(&[(300_000, 10), (200_000, 20), (100_000, 30)]);
        engine.on_verify(1_000, &rest_yes, &[], &book);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "matched"); // sanity: same set either order
        assert_eq!(
            rows[0].yes_levels_json,
            "[[100000,30],[200000,20],[300000,10]]"
        );
        assert_eq!(rows[0].no_levels_json, "[]");
    }

    #[test]
    fn empty_books_match() {
        let mut engine = VerifyEngine::new();
        let book = Book::default();
        engine.on_verify(1_000, &[], &[], &book);

        let rows = engine.finish();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "matched");
        assert_eq!(rows[0].match_lag_us, Some(0));
    }
}
