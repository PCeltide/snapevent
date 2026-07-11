//! Column builders that turn a stream of [`Envelope`]s into Arrow
//! [`RecordBatch`]es — one builder per output table.
//!
//! Each builder accumulates plain Rust `Vec`s (one per column) as records are
//! pushed, then `finish()`es into a typed [`RecordBatch`] the writer persists as
//! Parquet or Feather. Money stays integer end-to-end (`*_micro` price,
//! `*_centi` size); the only floats are clearly-named **convenience** columns
//! (`yes_bid`, `mid`, `imbalance`, …) derived from the integer source for ease of
//! analysis — the integer columns remain the lossless source of truth.
//!
//! ## Tables
//! - [`BookEventsTable`] — the **lossless** order-book mutation log. One row per
//!   snapshot level (absolute qty) or delta (signed qty). This is what *replaces*
//!   the raw orderbook JSONL: replaying it reconstructs every book state exactly.
//! - [`BookTopTable`] — derived top-of-book series, one row per orderbook message
//!   (driven by [`kdp_core::Book`]).
//! - [`TradesTable`], [`GapsTable`], [`RawTable`] — the trade tape, the inline
//!   gap markers (holes), and any undecodable messages (preserved verbatim).

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use kdp_core::{
    BookSide, Envelope, GapMarker, GapReason, OrderbookDelta, OrderbookSnapshot, RawFallback, Side,
    Timestamp, Trade,
};

use kdp_core::Top;

const MICRO_PER_DOLLAR: f64 = 1_000_000.0;
const CENTI_PER_CONTRACT: f64 = 100.0;

/// Microseconds since the Unix epoch (UTC) — the numeric, tz-free timestamp form
/// every table stores (`pd.to_datetime(col, unit="us")`).
fn ts_us(ts: Timestamp) -> i64 {
    ts.0.timestamp_micros()
}

/// Widen an optional per-subscription sequence to the stored `i64`. `seq` is a
/// small monotonic counter, so the `u64 -> i64` widening is always exact.
fn seq_i64(seq: Option<u64>) -> Option<i64> {
    seq.map(|s| s as i64)
}

/// Convert a resting quantity (centi-contracts, `u64`) to the stored `i64`.
///
/// The `*_centi` column is `i64` because it must also hold signed deltas, so a
/// resting `u64` is narrowed here. Physically impossible for Kalshi sizes
/// (`i64::MAX` is ~9.2e16 contracts), but we use a checked conversion rather than
/// an `as`-cast so a corrupt value errors loudly instead of silently wrapping —
/// "no silent failures" applies to the storage step too.
fn qty_centi_i64(q: u64) -> Result<i64> {
    i64::try_from(q).map_err(|_| anyhow!("resting quantity {q} centi-contracts exceeds i64 range"))
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Yes => "yes",
        Side::No => "no",
    }
}

fn book_side_str(b: BookSide) -> &'static str {
    match b {
        BookSide::Bid => "bid",
        BookSide::Ask => "ask",
    }
}

fn gap_reason_str(r: GapReason) -> &'static str {
    match r {
        GapReason::SeqJump => "seq_jump",
        GapReason::Reconnect => "reconnect",
        GapReason::Resubscribe => "resubscribe",
    }
}

fn micro_to_dollars(micro: Option<u32>) -> Option<f64> {
    micro.map(|m| m as f64 / MICRO_PER_DOLLAR)
}

// --- small ArrayRef constructors (keep `finish` readable) --------------------

fn i64_col(v: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn opt_i64_col(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn bool_col(v: Vec<bool>) -> ArrayRef {
    Arc::new(BooleanArray::from(v))
}
fn f64_col(v: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(v))
}
fn opt_f64_col(v: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(v))
}
// Generic over `AsRef<str>` so columns can hold either owned `String`s (for
// runtime-built values) or `&'static str` (for fixed vocabularies like
// side/event/taker_side), the latter avoiding a per-row heap allocation.
fn str_col<S: AsRef<str>>(v: Vec<S>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(v))
}
fn opt_str_col<S: AsRef<str>>(v: Vec<Option<S>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(v))
}

fn build(table: &str, cols: Vec<(&str, ArrayRef, bool)>) -> Result<RecordBatch> {
    RecordBatch::try_from_iter_with_nullable(cols)
        .with_context(|| format!("assembling the `{table}` record batch"))
}

// --- book_events: the lossless mutation log ----------------------------------

/// Flattened, lossless order-book mutation log — *replaces* the raw orderbook
/// JSONL. One row per snapshot price level (absolute `qty_centi`) or per delta
/// (signed `qty_centi`). Rows of one snapshot share an `event_idx`; an empty
/// snapshot emits a single boundary-marker row (`side`/`price`/`qty` null).
///
/// Replay contract (iterate in row order): on a snapshot row whose `event_idx`
/// differs from the current snapshot group, clear the book and start the new
/// group, then set the level (skip the set for a null-`side` boundary marker); a
/// delta row mutates the level (`+= qty`, drop at `<= 0`).
#[derive(Debug, Default)]
pub struct BookEventsTable {
    event_idx: Vec<i64>,
    recv_ts_us: Vec<i64>,
    event_ts_us: Vec<i64>,
    seq: Vec<Option<i64>>,
    sid: Vec<Option<i64>>,
    is_snapshot: Vec<bool>,
    side: Vec<Option<&'static str>>,
    price_micro: Vec<Option<i64>>,
    qty_centi: Vec<Option<i64>>,
}

impl BookEventsTable {
    #[allow(clippy::too_many_arguments)]
    fn row(
        &mut self,
        event_idx: i64,
        recv: i64,
        evt: i64,
        seq: Option<i64>,
        sid: Option<i64>,
        is_snapshot: bool,
        side: Option<&'static str>,
        price: Option<i64>,
        qty: Option<i64>,
    ) {
        self.event_idx.push(event_idx);
        self.recv_ts_us.push(recv);
        self.event_ts_us.push(evt);
        self.seq.push(seq);
        self.sid.push(sid);
        self.is_snapshot.push(is_snapshot);
        self.side.push(side);
        self.price_micro.push(price);
        self.qty_centi.push(qty);
    }

    // The `push_*` methods are per-record hot-path appends, so they are exempt
    // from the `#[tracing::instrument]` convention (decision D18); `finish()`,
    // called once per ticker, is instrumented.

    /// Append one snapshot as absolute-quantity rows (or a single boundary marker
    /// when the snapshot is empty, so a reset-to-empty book is never lost).
    /// Prices widen losslessly (`u32 -> i64`); quantities use a checked narrow.
    pub fn push_snapshot(
        &mut self,
        event_idx: i64,
        env: &Envelope,
        snap: &OrderbookSnapshot,
    ) -> Result<()> {
        let recv = ts_us(env.recv_ts);
        let evt = ts_us(snap.ts);
        let seq = seq_i64(env.seq);
        let sid = env.sid;
        if snap.yes.is_empty() && snap.no.is_empty() {
            self.row(event_idx, recv, evt, seq, sid, true, None, None, None);
            return Ok(());
        }
        for lvl in &snap.yes {
            let (p, q) = (lvl.price.0 as i64, qty_centi_i64(lvl.quantity.0)?);
            self.row(
                event_idx,
                recv,
                evt,
                seq,
                sid,
                true,
                Some("yes"),
                Some(p),
                Some(q),
            );
        }
        for lvl in &snap.no {
            let (p, q) = (lvl.price.0 as i64, qty_centi_i64(lvl.quantity.0)?);
            self.row(
                event_idx,
                recv,
                evt,
                seq,
                sid,
                true,
                Some("no"),
                Some(p),
                Some(q),
            );
        }
        Ok(())
    }

    /// Append one delta as a single signed-quantity row.
    pub fn push_delta(&mut self, event_idx: i64, env: &Envelope, delta: &OrderbookDelta) {
        self.row(
            event_idx,
            ts_us(env.recv_ts),
            ts_us(delta.ts),
            seq_i64(env.seq),
            env.sid,
            false,
            Some(side_str(delta.side)),
            Some(delta.price.0 as i64),
            Some(delta.delta.0),
        );
    }

    /// Consume the builder into a typed [`RecordBatch`].
    #[tracing::instrument(skip_all)]
    pub fn finish(self) -> Result<RecordBatch> {
        build(
            "book_events",
            vec![
                ("event_idx", i64_col(self.event_idx), false),
                ("recv_ts_us", i64_col(self.recv_ts_us), false),
                ("event_ts_us", i64_col(self.event_ts_us), false),
                ("seq", opt_i64_col(self.seq), true),
                ("sid", opt_i64_col(self.sid), true),
                ("is_snapshot", bool_col(self.is_snapshot), false),
                ("side", opt_str_col(self.side), true),
                ("price_micro", opt_i64_col(self.price_micro), true),
                ("qty_centi", opt_i64_col(self.qty_centi), true),
            ],
        )
    }
}

// --- book_top: derived top-of-book series ------------------------------------

/// Derived top-of-book + book-wide aggregates, one row per orderbook message.
#[derive(Debug, Default)]
pub struct BookTopTable {
    event_idx: Vec<i64>,
    recv_ts_us: Vec<i64>,
    event_ts_us: Vec<i64>,
    seq: Vec<Option<i64>>,
    event: Vec<&'static str>,
    yes_bid_micro: Vec<Option<i64>>,
    yes_bid_sz_centi: Vec<i64>,
    yes_ask_micro: Vec<Option<i64>>,
    yes_ask_sz_centi: Vec<i64>,
    mid_micro: Vec<Option<i64>>,
    spread_micro: Vec<Option<i64>>,
    yes_total_centi: Vec<i64>,
    no_total_centi: Vec<i64>,
    yes_levels: Vec<i64>,
    no_levels: Vec<i64>,
    imbalance: Vec<Option<f64>>,
    // Convenience dollar views (derived from the integer columns above).
    yes_bid: Vec<Option<f64>>,
    yes_ask: Vec<Option<f64>>,
    mid: Vec<Option<f64>>,
}

impl BookTopTable {
    /// Append the top of book after one orderbook message (`event` is
    /// `"snapshot"` or `"delta"`); `evt_us` is that message's own timestamp.
    pub fn push(
        &mut self,
        event_idx: i64,
        env: &Envelope,
        evt_us: i64,
        event: &'static str,
        top: &Top,
    ) {
        let mid = top.mid_micro();
        self.event_idx.push(event_idx);
        self.recv_ts_us.push(ts_us(env.recv_ts));
        self.event_ts_us.push(evt_us);
        self.seq.push(seq_i64(env.seq));
        self.event.push(event);
        self.yes_bid_micro.push(top.yes_bid_micro.map(|p| p as i64));
        self.yes_bid_sz_centi.push(top.yes_bid_sz_centi);
        self.yes_ask_micro.push(top.yes_ask_micro.map(|p| p as i64));
        self.yes_ask_sz_centi.push(top.yes_ask_sz_centi);
        self.mid_micro.push(mid.map(|p| p as i64));
        self.spread_micro.push(top.spread_micro());
        self.yes_total_centi.push(top.yes_total_centi);
        self.no_total_centi.push(top.no_total_centi);
        self.yes_levels.push(top.yes_levels);
        self.no_levels.push(top.no_levels);
        self.imbalance.push(top.imbalance());
        self.yes_bid.push(micro_to_dollars(top.yes_bid_micro));
        self.yes_ask.push(micro_to_dollars(top.yes_ask_micro));
        self.mid.push(micro_to_dollars(mid));
    }

    /// Consume the builder into a typed [`RecordBatch`].
    #[tracing::instrument(skip_all)]
    pub fn finish(self) -> Result<RecordBatch> {
        build(
            "book_top",
            vec![
                ("event_idx", i64_col(self.event_idx), false),
                ("recv_ts_us", i64_col(self.recv_ts_us), false),
                ("event_ts_us", i64_col(self.event_ts_us), false),
                ("seq", opt_i64_col(self.seq), true),
                ("event", str_col(self.event), false),
                ("yes_bid_micro", opt_i64_col(self.yes_bid_micro), true),
                ("yes_bid_sz_centi", i64_col(self.yes_bid_sz_centi), false),
                ("yes_ask_micro", opt_i64_col(self.yes_ask_micro), true),
                ("yes_ask_sz_centi", i64_col(self.yes_ask_sz_centi), false),
                ("mid_micro", opt_i64_col(self.mid_micro), true),
                ("spread_micro", opt_i64_col(self.spread_micro), true),
                ("yes_total_centi", i64_col(self.yes_total_centi), false),
                ("no_total_centi", i64_col(self.no_total_centi), false),
                ("yes_levels", i64_col(self.yes_levels), false),
                ("no_levels", i64_col(self.no_levels), false),
                ("imbalance", opt_f64_col(self.imbalance), true),
                ("yes_bid", opt_f64_col(self.yes_bid), true),
                ("yes_ask", opt_f64_col(self.yes_ask), true),
                ("mid", opt_f64_col(self.mid), true),
            ],
        )
    }
}

// --- trades ------------------------------------------------------------------

/// The executed-trade tape.
#[derive(Debug, Default)]
pub struct TradesTable {
    recv_ts_us: Vec<i64>,
    event_ts_us: Vec<i64>,
    seq: Vec<Option<i64>>,
    price_micro: Vec<i64>,
    count_centi: Vec<i64>,
    yes_price: Vec<f64>,
    count: Vec<f64>,
    taker_side: Vec<&'static str>,
    taker_book_side: Vec<Option<&'static str>>,
    trade_id: Vec<Option<String>>,
}

impl TradesTable {
    /// Append one trade. Price widens losslessly; count uses a checked narrow.
    pub fn push(&mut self, env: &Envelope, trade: &Trade) -> Result<()> {
        let price = trade.price.0 as i64;
        let count = qty_centi_i64(trade.count.0)?;
        self.recv_ts_us.push(ts_us(env.recv_ts));
        self.event_ts_us.push(ts_us(trade.ts));
        self.seq.push(seq_i64(env.seq));
        self.price_micro.push(price);
        self.count_centi.push(count);
        self.yes_price.push(price as f64 / MICRO_PER_DOLLAR);
        self.count.push(count as f64 / CENTI_PER_CONTRACT);
        self.taker_side.push(side_str(trade.taker_side));
        self.taker_book_side
            .push(trade.taker_book_side.map(book_side_str));
        self.trade_id.push(trade.trade_id.clone());
        Ok(())
    }

    /// Consume the builder into a typed [`RecordBatch`].
    #[tracing::instrument(skip_all)]
    pub fn finish(self) -> Result<RecordBatch> {
        build(
            "trades",
            vec![
                ("recv_ts_us", i64_col(self.recv_ts_us), false),
                ("event_ts_us", i64_col(self.event_ts_us), false),
                ("seq", opt_i64_col(self.seq), true),
                ("price_micro", i64_col(self.price_micro), false),
                ("count_centi", i64_col(self.count_centi), false),
                ("yes_price", f64_col(self.yes_price), false),
                ("count", f64_col(self.count), false),
                ("taker_side", str_col(self.taker_side), false),
                ("taker_book_side", opt_str_col(self.taker_book_side), true),
                ("trade_id", opt_str_col(self.trade_id), true),
            ],
        )
    }
}

// --- gaps --------------------------------------------------------------------

/// Inline gap markers — where the stream has a hole. Usually empty.
#[derive(Debug, Default)]
pub struct GapsTable {
    recv_ts_us: Vec<i64>,
    seq: Vec<Option<i64>>,
    // `reason` is a fixed vocabulary (&'static str), like side/event/taker_side.
    reason: Vec<&'static str>,
    channel: Vec<String>,
    last_seq: Vec<Option<i64>>,
    observed_seq: Vec<Option<i64>>,
    detail: Vec<String>,
}

impl GapsTable {
    /// Append one gap marker.
    pub fn push(&mut self, env: &Envelope, gap: &GapMarker) {
        self.recv_ts_us.push(ts_us(env.recv_ts));
        self.seq.push(seq_i64(env.seq));
        self.reason.push(gap_reason_str(gap.reason));
        self.channel.push(gap.channel.clone());
        self.last_seq.push(seq_i64(gap.last_seq));
        self.observed_seq.push(seq_i64(gap.observed_seq));
        self.detail.push(gap.detail.clone());
    }

    /// Consume the builder into a typed [`RecordBatch`].
    #[tracing::instrument(skip_all)]
    pub fn finish(self) -> Result<RecordBatch> {
        build(
            "gaps",
            vec![
                ("recv_ts_us", i64_col(self.recv_ts_us), false),
                ("seq", opt_i64_col(self.seq), true),
                ("reason", str_col(self.reason), false),
                ("channel", str_col(self.channel), false),
                ("last_seq", opt_i64_col(self.last_seq), true),
                ("observed_seq", opt_i64_col(self.observed_seq), true),
                ("detail", str_col(self.detail), false),
            ],
        )
    }
}

// --- raw fallbacks -----------------------------------------------------------

/// Undecodable messages preserved verbatim (`payload_json` is the original
/// payload re-serialized). Non-empty here means the raw files are **not** safe to
/// drop blindly — the operator should review these first.
#[derive(Debug, Default)]
pub struct RawTable {
    recv_ts_us: Vec<i64>,
    seq: Vec<Option<i64>>,
    channel: Vec<String>,
    raw_type: Vec<Option<String>>,
    ticker: Vec<Option<String>>,
    error: Vec<String>,
    payload_json: Vec<String>,
}

impl RawTable {
    /// Append one raw fallback record (preserved from `channel`).
    pub fn push(&mut self, env: &Envelope, channel: &str, raw: &RawFallback) {
        self.recv_ts_us.push(ts_us(env.recv_ts));
        self.seq.push(seq_i64(env.seq));
        self.channel.push(channel.to_string());
        self.raw_type.push(raw.raw_type.clone());
        self.ticker.push(raw.ticker.as_ref().map(|t| t.0.clone()));
        self.error.push(raw.error.clone());
        self.payload_json.push(
            serde_json::to_string(&raw.payload)
                .unwrap_or_else(|_| String::from("\"<unserializable>\"")),
        );
    }

    /// Consume the builder into a typed [`RecordBatch`].
    #[tracing::instrument(skip_all)]
    pub fn finish(self) -> Result<RecordBatch> {
        build(
            "raw",
            vec![
                ("recv_ts_us", i64_col(self.recv_ts_us), false),
                ("seq", opt_i64_col(self.seq), true),
                ("channel", str_col(self.channel), false),
                ("raw_type", opt_str_col(self.raw_type), true),
                ("ticker", opt_str_col(self.ticker), true),
                ("error", str_col(self.error), false),
                ("payload_json", str_col(self.payload_json), false),
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use chrono::{DateTime, Utc};
    use kdp_core::Book;
    use kdp_core::{
        MicroDollars, PriceLevel, QtyDelta, RecordKind, RestingQty, Ticker, ENVELOPE_VERSION,
    };

    fn ts() -> Timestamp {
        "2026-05-30T00:00:00Z"
            .parse::<DateTime<Utc>>()
            .unwrap()
            .into()
    }

    fn env(seq: Option<u64>, sid: Option<i64>, kind: RecordKind) -> Envelope {
        Envelope {
            v: ENVELOPE_VERSION,
            recv_ts: ts(),
            seq,
            sid,
            kind,
        }
    }

    fn level(price: u32, qty: u64) -> PriceLevel {
        PriceLevel {
            price: MicroDollars(price),
            quantity: RestingQty(qty),
        }
    }

    fn snap(yes: Vec<PriceLevel>, no: Vec<PriceLevel>) -> OrderbookSnapshot {
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

    fn i64col<'a>(b: &'a RecordBatch, name: &str) -> &'a Int64Array {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    }
    fn f64col<'a>(b: &'a RecordBatch, name: &str) -> &'a Float64Array {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
    }
    fn strcol<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }
    fn boolcol<'a>(b: &'a RecordBatch, name: &str) -> &'a BooleanArray {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
    }

    /// Reconstruct a `Book` from a finished book_events batch by following the
    /// documented replay contract — this is the test of losslessness.
    fn replay_book_events(b: &RecordBatch) -> Book {
        let event_idx = i64col(b, "event_idx");
        let is_snapshot = boolcol(b, "is_snapshot");
        let side = strcol(b, "side");
        let price = i64col(b, "price_micro");
        let qty = i64col(b, "qty_centi");
        let mut book = Book::default();
        let mut cur_snap: Option<i64> = None;
        for r in 0..b.num_rows() {
            if is_snapshot.value(r) {
                if cur_snap != Some(event_idx.value(r)) {
                    book.yes.clear();
                    book.no.clear();
                    cur_snap = Some(event_idx.value(r));
                }
                if side.is_null(r) {
                    continue; // empty-snapshot boundary marker
                }
                let p = price.value(r) as u32;
                let q = qty.value(r);
                match side.value(r) {
                    "yes" => {
                        book.yes.insert(p, q);
                    }
                    "no" => {
                        book.no.insert(p, q);
                    }
                    other => panic!("bad side {other}"),
                }
            } else {
                cur_snap = None;
                let p = price.value(r) as u32;
                let dq = qty.value(r);
                let target = match side.value(r) {
                    "yes" => &mut book.yes,
                    "no" => &mut book.no,
                    other => panic!("bad side {other}"),
                };
                let e = target.entry(p).or_insert(0);
                *e += dq;
                if *e <= 0 {
                    target.remove(&p);
                }
            }
        }
        // This helper mutates the maps directly, so sync the running totals that
        // `Book::apply_*` would otherwise maintain (so `book.top()` matches).
        book.yes_total = book.yes.values().sum();
        book.no_total = book.no.values().sum();
        book
    }

    #[test]
    fn book_events_round_trips_a_snapshot_plus_deltas_losslessly() {
        // Build the book directly (reference) ...
        let s = snap(
            vec![level(450_000, 8_003_183), level(440_000, 100)],
            vec![level(540_000, 8_427_534)],
        );
        let d1 = delta(Side::Yes, 460_000, 500); // new better bid
        let d2 = delta(Side::Yes, 440_000, -100); // empties the 0.44 level
        let mut reference = Book::default();
        reference.apply_snapshot(&s);
        reference.apply_delta(&d1);
        reference.apply_delta(&d2);

        // ... and via the lossless table.
        let mut t = BookEventsTable::default();
        t.push_snapshot(
            0,
            &env(Some(1), Some(1), RecordKind::Snapshot(s.clone())),
            &s,
        )
        .unwrap();
        t.push_delta(
            1,
            &env(Some(2), Some(1), RecordKind::Delta(d1.clone())),
            &d1,
        );
        t.push_delta(
            2,
            &env(Some(3), Some(1), RecordKind::Delta(d2.clone())),
            &d2,
        );
        let batch = t.finish().unwrap();

        let replayed = replay_book_events(&batch);
        assert_eq!(replayed.top(), reference.top());
        assert_eq!(replayed.yes, reference.yes);
        assert_eq!(replayed.no, reference.no);
    }

    #[test]
    fn book_events_empty_snapshot_is_a_boundary_marker_that_resets() {
        let mut t = BookEventsTable::default();
        let s1 = snap(vec![level(450_000, 100)], vec![]);
        t.push_snapshot(0, &env(None, None, RecordKind::Snapshot(s1.clone())), &s1)
            .unwrap();
        let empty = snap(vec![], vec![]);
        t.push_snapshot(
            1,
            &env(None, None, RecordKind::Snapshot(empty.clone())),
            &empty,
        )
        .unwrap();
        let batch = t.finish().unwrap();
        // 1 level row + 1 boundary marker row.
        assert_eq!(batch.num_rows(), 2);
        assert!(strcol(&batch, "side").is_null(1), "marker has null side");
        // Replaying ends at the empty snapshot -> empty book.
        let book = replay_book_events(&batch);
        assert!(book.yes.is_empty() && book.no.is_empty());
    }

    #[test]
    fn book_top_reports_yes_ask_from_no_side_and_convenience_dollars() {
        let s = snap(
            vec![level(450_000, 8_003_183)],
            vec![level(540_000, 8_427_534)],
        );
        let mut book = Book::default();
        book.apply_snapshot(&s);
        let mut t = BookTopTable::default();
        t.push(
            0,
            &env(Some(1), Some(1), RecordKind::Snapshot(s.clone())),
            ts_us(s.ts),
            "snapshot",
            &book.top(),
        );
        let b = t.finish().unwrap();
        assert_eq!(b.num_rows(), 1);
        assert_eq!(i64col(&b, "yes_bid_micro").value(0), 450_000);
        assert_eq!(i64col(&b, "yes_ask_micro").value(0), 460_000); // $1 - $0.54
        assert_eq!(i64col(&b, "mid_micro").value(0), 455_000);
        assert_eq!(i64col(&b, "spread_micro").value(0), 10_000);
        assert_eq!(strcol(&b, "event").value(0), "snapshot");
        assert!((f64col(&b, "yes_bid").value(0) - 0.45).abs() < 1e-9);
        assert!((f64col(&b, "mid").value(0) - 0.455).abs() < 1e-9);
    }

    #[test]
    fn trades_table_maps_price_size_and_sides() {
        let trade = Trade {
            ticker: Ticker("K".into()),
            ts: ts(),
            price: MicroDollars(460_000),
            count: RestingQty(57_605),
            taker_side: Side::Yes,
            taker_book_side: Some(BookSide::Bid),
            trade_id: Some("abc".into()),
        };
        let mut t = TradesTable::default();
        t.push(
            &env(Some(2), Some(2), RecordKind::Trade(trade.clone())),
            &trade,
        )
        .unwrap();
        let b = t.finish().unwrap();
        assert_eq!(b.num_rows(), 1);
        assert_eq!(i64col(&b, "price_micro").value(0), 460_000);
        assert_eq!(i64col(&b, "count_centi").value(0), 57_605);
        assert!((f64col(&b, "yes_price").value(0) - 0.46).abs() < 1e-9);
        assert!((f64col(&b, "count").value(0) - 576.05).abs() < 1e-9);
        assert_eq!(strcol(&b, "taker_side").value(0), "yes");
        assert_eq!(strcol(&b, "taker_book_side").value(0), "bid");
        assert_eq!(strcol(&b, "trade_id").value(0), "abc");
    }

    #[test]
    fn snapshot_quantity_over_i64_max_errors_instead_of_wrapping() {
        // A resting qty above i64::MAX (impossible for real Kalshi sizes) must be
        // a loud error, not a silent wrap to negative in the lossless column.
        let mut t = BookEventsTable::default();
        let s = snap(vec![level(450_000, u64::MAX)], vec![]);
        let e = env(None, None, RecordKind::Snapshot(s.clone()));
        assert!(
            t.push_snapshot(0, &e, &s).is_err(),
            "u64::MAX quantity must error, not wrap"
        );
    }

    #[test]
    fn gaps_table_maps_reason_and_seqs() {
        let gap = GapMarker {
            reason: GapReason::SeqJump,
            ticker: Ticker("K".into()),
            channel: "orderbook".into(),
            last_seq: Some(10),
            observed_seq: Some(13),
            detail: "seq jumped 10 -> 13".into(),
        };
        let mut t = GapsTable::default();
        t.push(&env(Some(13), Some(1), RecordKind::Gap(gap.clone())), &gap);
        let b = t.finish().unwrap();
        assert_eq!(b.num_rows(), 1);
        assert_eq!(strcol(&b, "reason").value(0), "seq_jump");
        assert_eq!(strcol(&b, "channel").value(0), "orderbook");
        assert_eq!(i64col(&b, "last_seq").value(0), 10);
        assert_eq!(i64col(&b, "observed_seq").value(0), 13);
    }

    #[test]
    fn empty_tables_finish_to_zero_row_batches_with_schema() {
        let b = BookTopTable::default().finish().unwrap();
        assert_eq!(b.num_rows(), 0);
        assert!(b.schema().column_with_name("imbalance").is_some());
        let g = GapsTable::default().finish().unwrap();
        assert_eq!(g.num_rows(), 0);
    }

    #[test]
    fn raw_table_preserves_payload_json() {
        let raw = RawFallback {
            raw_type: Some("orderbook_delta".into()),
            ticker: Some(Ticker("K".into())),
            error: "too many decimals".into(),
            payload: serde_json::json!({"price_dollars":"0.1234567"}),
        };
        let mut t = RawTable::default();
        t.push(
            &env(None, None, RecordKind::Raw(raw.clone())),
            "orderbook",
            &raw,
        );
        let b = t.finish().unwrap();
        assert_eq!(b.num_rows(), 1);
        assert_eq!(strcol(&b, "channel").value(0), "orderbook");
        assert!(strcol(&b, "payload_json").value(0).contains("0.1234567"));
    }
}
