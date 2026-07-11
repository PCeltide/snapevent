//! The deterministic merged stream — kdp-load's core (R1+R2).
//!
//! Three lanes (gaps / book_events / trades) are pulled through a k-way merge
//! ordered by [`MergeKey`], a TOTAL order with no hash-iteration anywhere:
//!
//! 1. `effective_ts` — WS rows use `recv_ts_us` (the authoritative capture
//!    clock); REST-backfilled trades (writer convention: `seq` NULL) use
//!    `event_ts_us` (execution time). NEVER fetch time — ordering by raw
//!    `recv_ts_us` would cluster every backfilled trade at the end of the
//!    timeline, hours after the book it printed against.
//! 2. `seq` class+value — absent `seq` (REST) sorts AFTER present at equal ts.
//! 3. table priority — gaps announce holes before data; book before trade.
//! 4. `event_idx`, then file row order — the final stable disambiguators.
//!
//! Trade dedup (R6) happens at open, before the lane sort: overlapping WS +
//! REST copies of one `trade_id` collapse to a single event (REST fields, WS
//! position) — see [`dedup_trades`].
//!
//! Snapshot grouping happens here too: consecutive `book_events` rows sharing
//! an `event_idx` with `is_snapshot=true` assemble into ONE
//! [`ReplayEvent::Snapshot`] (a null-side row is the empty-side boundary
//! marker), so no consumer ever re-implements the grouping.

use std::path::Path;

use kdp_core::{MicroDollars, QtyDelta, RestingQty};

use crate::error::LoadError;
use crate::event::{EffectiveTs, Level, ReplayEvent, TradeSource};
use crate::reader::{self, BookEventRow, GapRow, TradeRow};

/// The total order over the merged stream. Tuple `Ord` — see module docs.
/// Fields: (effective_ts, seq_class 0=present/1=absent, seq_value,
/// table_priority 0=gap/1=book/2=trade, event_idx, file_row).
pub(crate) type MergeKey = (i64, u8, i64, u8, i64, usize);

const PRI_GAP: u8 = 0;
const PRI_BOOK: u8 = 1;
const PRI_TRADE: u8 = 2;

fn seq_part(seq: Option<i64>) -> (u8, i64) {
    match seq {
        Some(s) => (0, s),
        None => (1, i64::MAX),
    }
}

/// A trade's effective timestamp + provenance (THE R1 rule).
fn trade_effective(row: &TradeRow) -> (i64, TradeSource) {
    match row.seq {
        Some(_) => (row.recv_ts_us, TradeSource::Ws),
        None => (row.event_ts_us, TradeSource::RestBackfill),
    }
}

pub(crate) fn book_key(row: &BookEventRow, file_row: usize) -> MergeKey {
    let (c, s) = seq_part(row.seq);
    (row.recv_ts_us, c, s, PRI_BOOK, row.event_idx, file_row)
}

pub(crate) fn trade_key(row: &TradeRow, file_row: usize) -> MergeKey {
    let (ts, _) = trade_effective(row);
    let (c, s) = seq_part(row.seq);
    (ts, c, s, PRI_TRADE, 0, file_row)
}

pub(crate) fn gap_key(row: &GapRow, file_row: usize) -> MergeKey {
    let (c, s) = seq_part(row.seq);
    (row.recv_ts_us, c, s, PRI_GAP, 0, file_row)
}

/// R6: collapse duplicate prints of one economic trade (key: `trade_id`).
/// The expected pair is one WS copy + one REST copy of the same print: the
/// REST copy wins FIELDS (the richer, authoritative execution record), the
/// WS copy wins POSITION (`recv_ts_us` + `seq` -- a dedup never moves a
/// trade to fetch time). Rows without a `trade_id` are never deduped. A
/// same-source duplicate (two WS or two REST copies) is unexpected:
/// collapsed to the first copy in key order, with a warn -- the consumer
/// must never see one print twice (a double print double-counts flow).
fn dedup_trades(rows: Vec<(usize, TradeRow)>) -> Vec<(usize, TradeRow)> {
    use std::collections::BTreeMap;
    let mut out: Vec<(usize, TradeRow)> = Vec::with_capacity(rows.len());
    // BTreeMap, not HashMap: grouping stays deterministic (no hash iteration
    // anywhere in the ordering path).
    let mut groups: BTreeMap<String, Vec<(usize, TradeRow)>> = BTreeMap::new();
    for (i, row) in rows {
        match row.trade_id.clone() {
            Some(id) => groups.entry(id).or_default().push((i, row)),
            None => out.push((i, row)),
        }
    }
    for (id, mut copies) in groups {
        if copies.len() == 1 {
            if let Some(single) = copies.pop() {
                out.push(single);
            }
            continue;
        }
        copies.sort_by_key(|(i, r)| trade_key(r, *i));
        let n_ws = copies.iter().filter(|(_, r)| r.seq.is_some()).count();
        let n_rest = copies.len() - n_ws;
        if n_ws > 1 || n_rest > 1 {
            tracing::warn!(
                trade_id = %id,
                n_ws,
                n_rest,
                "same-source duplicate trade copies collapsed"
            );
        }
        // Position donor: the first WS copy in key order (else the first copy).
        // Field donor: the first REST copy in key order (else the position
        // donor). find/or instead of indexing: no panic path (the None arms
        // are unreachable -- copies is non-empty -- but structurally total).
        let pos = copies
            .iter()
            .find(|(_, r)| r.seq.is_some())
            .or_else(|| copies.first())
            .cloned();
        let fields = copies
            .iter()
            .find(|(_, r)| r.seq.is_none())
            .cloned()
            .or_else(|| pos.clone());
        let (Some((file_row, pos)), Some((_, fields))) = (pos, fields) else {
            continue;
        };
        out.push((
            file_row,
            TradeRow {
                recv_ts_us: pos.recv_ts_us,
                seq: pos.seq,
                event_ts_us: fields.event_ts_us,
                price_micro: fields.price_micro,
                count_centi: fields.count_centi,
                taker_side: fields.taker_side,
                // REST wins fields; a field the REST record LACKS falls back
                // to the WS copy (dropping known data would be silent loss).
                taker_book_side: fields.taker_book_side.or(pos.taker_book_side),
                trade_id: Some(id),
            },
        ));
    }
    out
}

/// A boxed row source: parquet-backed in production, `Vec`-backed in tests.
type RowSource<T> = Box<dyn Iterator<Item = Result<T, LoadError>>>;

/// One lane of the merge: holds the next ready (key, event), refilling from
/// its row source. A lane error is surfaced once, at its position.
struct Lane {
    head: Option<(MergeKey, ReplayEvent)>,
    err: Option<LoadError>,
}

impl Lane {
    fn empty() -> Lane {
        Lane {
            head: None,
            err: None,
        }
    }
}

/// Streaming iterator over the merged, deterministic, time-ordered stream.
///
/// Memory: `book_events` (the dominant table by an order of magnitude) and
/// `gaps` stream chunked — their file order IS effective-ts order (capture
/// order). The `trades` lane is materialized, deduped (R6), and stably key-sorted at open:
/// REST-backfilled rows sit at the END of the file but their effective
/// timestamps (execution time) belong throughout the timeline, so the lane's
/// file order violates the merge invariant. Bounded by the per-ticker tape
/// size (small); a malformed trade row therefore surfaces at open, with its
/// file+row context (stricter than R4's at-the-row, never weaker).
pub struct EventIter {
    gaps: RowSource<GapRow>,
    book: std::iter::Peekable<RowSource<BookEventRow>>,
    /// (original file row, row) — pre-sorted by [`trade_key`].
    trades: std::vec::IntoIter<(usize, TradeRow)>,
    gap_lane: Lane,
    book_lane: Lane,
    trade_lane: Lane,
    gap_row: usize,
    book_row: usize,
    /// The last emitted key: the runtime tripwire for the one assumption the
    /// merge makes — that book/gap file order is effective-ts order. A
    /// violation (e.g. a wall-clock step mid-capture, or a future writer
    /// change) is a typed `OrderViolation`, never silent corruption of the
    /// stream or of `between`'s pre-roll/early-stop.
    last_key: Option<MergeKey>,
}

impl std::fmt::Debug for EventIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventIter").finish_non_exhaustive()
    }
}

impl EventIter {
    /// Open the three tables under `dir` (parquet-backed lanes).
    pub(crate) fn open(dir: &Path) -> Result<EventIter, LoadError> {
        EventIter::from_sources(
            Box::new(reader::gaps(dir)?),
            Box::new(reader::book_events(dir)?),
            Box::new(reader::trades(dir)?),
        )
    }

    /// Assemble from arbitrary row sources (tests inject `Vec`s here). The
    /// trades source is drained + stably key-sorted here (see the struct doc);
    /// a malformed trade row fails construction with its row context.
    pub(crate) fn from_sources(
        gaps: RowSource<GapRow>,
        book: RowSource<BookEventRow>,
        trades: RowSource<TradeRow>,
    ) -> Result<EventIter, LoadError> {
        let mut rows: Vec<(usize, TradeRow)> = Vec::new();
        for (i, row) in trades.enumerate() {
            rows.push((i, row?));
        }
        let mut rows = dedup_trades(rows); // R6: one event per economic trade
                                           // Stable sort: file order is already the last MergeKey component and
                                           // dedup keeps at most one row per original file_row, so equal keys
                                           // cannot occur; stability is belt-and-suspenders.
        rows.sort_by_key(|(i, r)| trade_key(r, *i));
        Ok(EventIter {
            gaps,
            book: book.peekable(),
            trades: rows.into_iter(),
            gap_lane: Lane::empty(),
            book_lane: Lane::empty(),
            trade_lane: Lane::empty(),
            gap_row: 0,
            book_row: 0,
            last_key: None,
        })
    }

    fn fill_gap_lane(&mut self) {
        if self.gap_lane.head.is_some() || self.gap_lane.err.is_some() {
            return;
        }
        match self.gaps.next() {
            None => {}
            Some(Err(e)) => self.gap_lane.err = Some(e),
            Some(Ok(row)) => {
                let key = gap_key(&row, self.gap_row);
                self.gap_row += 1;
                self.gap_lane.head = Some((
                    key,
                    ReplayEvent::Gap {
                        ts: EffectiveTs(row.recv_ts_us),
                        reason: row.reason,
                        detail: row.detail,
                    },
                ));
            }
        }
    }

    fn fill_trade_lane(&mut self) {
        if self.trade_lane.head.is_some() || self.trade_lane.err.is_some() {
            return;
        }
        match self.trades.next() {
            None => {}
            Some((file_row, row)) => {
                let key = trade_key(&row, file_row);
                let (ts, source) = trade_effective(&row);
                self.trade_lane.head = Some((
                    key,
                    ReplayEvent::Trade {
                        ts: EffectiveTs(ts),
                        price: MicroDollars(row.price_micro),
                        count: RestingQty(row.count_centi),
                        taker_side: row.taker_side,
                        taker_book_side: row.taker_book_side,
                        trade_id: row.trade_id,
                        source,
                    },
                ));
            }
        }
    }

    /// Pull one book EVENT: a single delta row, or a whole grouped snapshot
    /// (all consecutive rows sharing the head row's `event_idx`).
    fn fill_book_lane(&mut self) {
        if self.book_lane.head.is_some() || self.book_lane.err.is_some() {
            return;
        }
        let first = match self.book.next() {
            None => return,
            Some(Err(e)) => {
                self.book_lane.err = Some(e);
                return;
            }
            Some(Ok(row)) => row,
        };
        let key = book_key(&first, self.book_row);
        self.book_row += 1;

        if !first.is_snapshot {
            match delta_event(&first, self.book_row - 1) {
                Ok(ev) => self.book_lane.head = Some((key, ev)),
                Err(e) => self.book_lane.err = Some(e),
            }
            return;
        }

        // Snapshot: assemble every consecutive row of this event_idx (both
        // sides; null-side boundary markers contribute no level).
        let mut yes = Vec::new();
        let mut no = Vec::new();
        let mut push = |row: &BookEventRow, file_row: usize| {
            if let (Some(side), Some(price), Some(qty)) = (row.side, row.price_micro, row.qty_centi)
            {
                let qty = match u64::try_from(qty) {
                    Ok(q) => q,
                    Err(_) => {
                        return Some(LoadError::MalformedRow {
                            table: "book_events",
                            row: file_row,
                            detail: format!("negative snapshot qty {qty}"),
                        })
                    }
                };
                let level = Level {
                    price: MicroDollars(price),
                    qty: RestingQty(qty),
                };
                match side {
                    kdp_core::Side::Yes => yes.push(level),
                    kdp_core::Side::No => no.push(level),
                }
            }
            None
        };
        if let Some(e) = push(&first, self.book_row - 1) {
            self.book_lane.err = Some(e);
            return;
        }
        loop {
            match self.book.peek() {
                Some(Ok(peek)) if peek.is_snapshot && peek.event_idx == first.event_idx => {
                    // Consume the peeked row (Ok by the peek above).
                    let Some(Ok(row)) = self.book.next() else {
                        break;
                    };
                    self.book_row += 1;
                    if let Some(e) = push(&row, self.book_row - 1) {
                        self.book_lane.err = Some(e);
                        return;
                    }
                }
                Some(Err(_)) => {
                    // A malformed row while assembling this group: emitting the
                    // partial snapshot as Ok would hand the consumer a silently
                    // WRONG book state (the R4 failure class) with the error
                    // arriving one event too late. Discard the partial head and
                    // surface the error at this position instead. (The bad row
                    // might have belonged to the NEXT event — undecodable, so
                    // unknowable; discarding is the conservative reading.)
                    if let Some(Err(e)) = self.book.next() {
                        self.book_lane.err = Some(e);
                    }
                    return;
                }
                _ => break,
            }
        }
        self.book_lane.head = Some((
            key,
            ReplayEvent::Snapshot {
                ts: EffectiveTs(first.recv_ts_us),
                event_idx: first.event_idx,
                yes,
                no,
                synthetic: false,
            },
        ));
    }
}

/// A delta row must carry side+price+qty; an absence is a malformed row (a
/// defaulted value would be a silent wrong mutation — the exact failure class
/// this project forbids), so it surfaces as a typed error at its position.
fn delta_event(row: &BookEventRow, file_row: usize) -> Result<ReplayEvent, LoadError> {
    let (Some(side), Some(price), Some(qty)) = (row.side, row.price_micro, row.qty_centi) else {
        return Err(LoadError::MalformedRow {
            table: "book_events",
            row: file_row,
            detail: "delta row missing side/price/qty".to_string(),
        });
    };
    Ok(ReplayEvent::Delta {
        ts: EffectiveTs(row.recv_ts_us),
        event_idx: row.event_idx,
        side,
        price: MicroDollars(price),
        delta: QtyDelta(qty),
    })
}

impl Iterator for EventIter {
    type Item = Result<ReplayEvent, LoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fill_gap_lane();
        self.fill_book_lane();
        self.fill_trade_lane();

        // Surface lane errors deterministically (fixed lane order).
        for lane in [
            &mut self.gap_lane,
            &mut self.book_lane,
            &mut self.trade_lane,
        ] {
            if let Some(e) = lane.err.take() {
                return Some(Err(e));
            }
        }

        // Emit the smallest-key head. Keys are unique (file_row + table
        // priority disambiguate), so `min_by_key` over a fixed lane order is
        // fully deterministic.
        let lanes = [
            &mut self.gap_lane,
            &mut self.book_lane,
            &mut self.trade_lane,
        ];
        let best = lanes
            .into_iter()
            .filter(|l| l.head.is_some())
            .min_by_key(|l| l.head.as_ref().map(|(k, _)| *k))?;
        let (key, ev) = best.head.take()?;
        // The monotonicity tripwire (see the `last_key` field doc).
        if let Some(prev) = self.last_key {
            if key < prev {
                return Some(Err(LoadError::OrderViolation {
                    detail: format!(
                        "effective ts regressed: {} after {} (file order != time order)",
                        key.0, prev.0
                    ),
                }));
            }
        }
        self.last_key = Some(key);
        Some(Ok(ev))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdp_core::Side;

    fn src<T: 'static>(rows: Vec<T>) -> RowSource<T> {
        Box::new(rows.into_iter().map(Ok))
    }

    fn book_snapshot_rows(event_idx: i64, ts: i64, seq: i64) -> Vec<BookEventRow> {
        vec![
            BookEventRow {
                event_idx,
                recv_ts_us: ts,
                seq: Some(seq),
                is_snapshot: true,
                side: Some(Side::Yes),
                price_micro: Some(450_000),
                qty_centi: Some(100),
            },
            BookEventRow {
                event_idx,
                recv_ts_us: ts,
                seq: Some(seq),
                is_snapshot: true,
                side: Some(Side::Yes),
                price_micro: Some(440_000),
                qty_centi: Some(50),
            },
            BookEventRow {
                event_idx,
                recv_ts_us: ts,
                seq: Some(seq),
                is_snapshot: true,
                side: None, // empty NO side boundary marker
                price_micro: None,
                qty_centi: None,
            },
        ]
    }

    fn delta_row(event_idx: i64, ts: i64, seq: i64, qty: i64) -> BookEventRow {
        BookEventRow {
            event_idx,
            recv_ts_us: ts,
            seq: Some(seq),
            is_snapshot: false,
            side: Some(Side::Yes),
            price_micro: Some(450_000),
            qty_centi: Some(qty),
        }
    }

    fn ws_trade(recv: i64, event_ts: i64, seq: i64) -> TradeRow {
        TradeRow {
            recv_ts_us: recv,
            event_ts_us: event_ts,
            seq: Some(seq),
            price_micro: 460_000,
            count_centi: 500,
            taker_side: Side::Yes,
            taker_book_side: None,
            trade_id: Some(format!("ws-{seq}")),
        }
    }

    fn rest_trade(recv: i64, event_ts: i64, id: &str) -> TradeRow {
        TradeRow {
            recv_ts_us: recv,
            event_ts_us: event_ts,
            seq: None,
            price_micro: 470_000,
            count_centi: 200,
            taker_side: Side::No,
            taker_book_side: None,
            trade_id: Some(id.to_string()),
        }
    }

    fn collect(it: EventIter) -> Vec<ReplayEvent> {
        it.map(|e| e.expect("event")).collect()
    }

    #[test]
    fn rest_trade_orders_by_event_ts_never_fetch_time() {
        // Book rows at ts 1000 and 2000; a REST trade fetched at 999_999_999
        // but EXECUTED at 1500 must land between them.
        let book = vec![delta_row(1, 1000, 1, 10), delta_row(2, 2000, 2, 20)];
        let trades = vec![rest_trade(999_999_999, 1500, "r1")];
        let evs =
            collect(EventIter::from_sources(src(vec![]), src(book), src(trades)).expect("build"));
        assert_eq!(evs.len(), 3);
        assert!(matches!(evs[0], ReplayEvent::Delta { .. }));
        match &evs[1] {
            ReplayEvent::Trade { ts, source, .. } => {
                assert_eq!(ts.0, 1500, "effective ts is execution time");
                assert_eq!(*source, TradeSource::RestBackfill);
            }
            other => panic!("expected the REST trade second, got {other:?}"),
        }
        assert!(matches!(evs[2], ReplayEvent::Delta { .. }));
    }

    #[test]
    fn ws_trade_orders_by_recv_ts_with_provenance() {
        let trades = vec![ws_trade(1200, 900, 5)];
        let evs =
            collect(EventIter::from_sources(src(vec![]), src(vec![]), src(trades)).expect("build"));
        match &evs[0] {
            ReplayEvent::Trade { ts, source, .. } => {
                assert_eq!(ts.0, 1200, "WS trade uses recv_ts");
                assert_eq!(*source, TradeSource::Ws);
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[test]
    fn absent_seq_sorts_after_present_at_equal_ts() {
        // Same effective ts 1500: WS trade (seq present) before REST (absent).
        let trades = vec![rest_trade(9_999_999, 1500, "r1"), ws_trade(1500, 1400, 9)];
        let evs =
            collect(EventIter::from_sources(src(vec![]), src(vec![]), src(trades)).expect("build"));
        match (&evs[0], &evs[1]) {
            (ReplayEvent::Trade { source: s0, .. }, ReplayEvent::Trade { source: s1, .. }) => {
                assert_eq!(*s0, TradeSource::Ws);
                assert_eq!(*s1, TradeSource::RestBackfill);
            }
            other => panic!("expected two trades, got {other:?}"),
        }
    }

    #[test]
    fn gap_before_book_before_trade_at_equal_key() {
        let gaps = vec![GapRow {
            recv_ts_us: 1000,
            seq: Some(1),
            reason: "seq_jump".into(),
            detail: "d".into(),
        }];
        let book = vec![delta_row(1, 1000, 1, 10)];
        let trades = vec![ws_trade(1000, 1000, 1)];
        let evs =
            collect(EventIter::from_sources(src(gaps), src(book), src(trades)).expect("build"));
        assert!(matches!(evs[0], ReplayEvent::Gap { .. }), "gap first");
        assert!(matches!(evs[1], ReplayEvent::Delta { .. }), "book second");
        assert!(matches!(evs[2], ReplayEvent::Trade { .. }), "trade last");
    }

    #[test]
    fn snapshot_rows_group_into_one_event_with_boundary_marker_empty_side() {
        let book = book_snapshot_rows(1, 1000, 1);
        let evs =
            collect(EventIter::from_sources(src(vec![]), src(book), src(vec![])).expect("build"));
        assert_eq!(evs.len(), 1, "3 rows -> 1 grouped snapshot");
        match &evs[0] {
            ReplayEvent::Snapshot {
                yes, no, synthetic, ..
            } => {
                assert_eq!(yes.len(), 2);
                assert!(no.is_empty(), "boundary marker => empty NO side");
                assert!(!synthetic);
            }
            other => panic!("expected snapshot, got {other:?}"),
        }
    }

    #[test]
    fn determinism_two_merges_produce_identical_streams() {
        let build = || {
            EventIter::from_sources(
                src(vec![GapRow {
                    recv_ts_us: 1500,
                    seq: None,
                    reason: "reconnect".into(),
                    detail: "d".into(),
                }]),
                src({
                    let mut b = book_snapshot_rows(1, 1000, 1);
                    b.push(delta_row(2, 1200, 2, -10));
                    b.extend(book_snapshot_rows(3, 1600, 4));
                    b
                }),
                src(vec![
                    ws_trade(1100, 1000, 3),
                    rest_trade(9_999_999, 1300, "r1"),
                    rest_trade(9_999_999, 1300, "r2"),
                ]),
            )
            .expect("build")
        };
        let a = format!("{:?}", collect(build()));
        let b = format!("{:?}", collect(build()));
        assert_eq!(
            a, b,
            "same inputs => bit-identical stream (consumer check #1)"
        );
    }

    #[test]
    fn effective_ts_is_non_decreasing_across_stream() {
        let evs = collect(
            EventIter::from_sources(
                src(vec![GapRow {
                    recv_ts_us: 1500,
                    seq: None,
                    reason: "reconnect".into(),
                    detail: "d".into(),
                }]),
                src({
                    let mut b = book_snapshot_rows(1, 1000, 1);
                    b.push(delta_row(2, 1200, 2, -10));
                    b.extend(book_snapshot_rows(3, 1600, 4));
                    b
                }),
                src(vec![
                    ws_trade(1100, 1000, 3),
                    rest_trade(9_999_999, 1300, "r1"),
                ]),
            )
            .expect("build"),
        );
        let ts: Vec<i64> = evs.iter().map(|e| e.ts().0).collect();
        let mut sorted = ts.clone();
        sorted.sort_unstable();
        assert_eq!(ts, sorted, "consumer check #2: non-decreasing effective ts");
    }

    #[test]
    fn lane_error_is_surfaced_at_its_position_not_skipped() {
        let book: RowSource<BookEventRow> = Box::new(
            vec![
                Ok(delta_row(1, 1000, 1, 10)),
                Err(LoadError::MalformedRow {
                    table: "book_events",
                    row: 1,
                    detail: "boom".into(),
                }),
            ]
            .into_iter(),
        );
        let mut it = EventIter::from_sources(src(vec![]), book, src(vec![])).expect("build");
        assert!(it.next().and_then(|r| r.ok()).is_some(), "first row ok");
        assert!(
            matches!(it.next(), Some(Err(LoadError::MalformedRow { .. }))),
            "error surfaced, not skipped (R4)"
        );
    }

    #[test]
    fn ws_and_rest_copies_of_one_trade_dedup_rest_fields_ws_position() {
        // One economic trade observed live (WS recv 1200) AND backfilled
        // (REST fetch 9_999_999, exec 1100): ONE event, WS position, REST fields.
        let mut ws = ws_trade(1200, 1100, 5);
        ws.trade_id = Some("t1".into());
        let rest = rest_trade(9_999_999, 1100, "t1");
        let evs = collect(
            EventIter::from_sources(src(vec![]), src(vec![]), src(vec![ws, rest])).expect("build"),
        );
        assert_eq!(evs.len(), 1, "one economic trade, one event: {evs:?}");
        match &evs[0] {
            ReplayEvent::Trade {
                ts,
                price,
                count,
                taker_side,
                trade_id,
                source,
                ..
            } => {
                assert_eq!(ts.0, 1200, "WS copy wins position, never fetch time");
                assert_eq!(*source, TradeSource::Ws);
                assert_eq!(price.0, 470_000, "REST copy wins fields");
                assert_eq!(count.0, 200);
                assert_eq!(*taker_side, Side::No);
                assert_eq!(trade_id.as_deref(), Some("t1"));
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[test]
    fn trades_without_ids_are_never_deduped() {
        let mut a = ws_trade(1000, 900, 1);
        a.trade_id = None;
        let mut b = ws_trade(1000, 900, 2);
        b.trade_id = None;
        let evs = collect(
            EventIter::from_sources(src(vec![]), src(vec![]), src(vec![a, b])).expect("build"),
        );
        assert_eq!(evs.len(), 2, "no key => no dedup");
    }

    #[test]
    fn same_source_duplicate_collapses_to_first_by_key() {
        // Two REST copies of one id (overlapping backfill runs): unexpected,
        // but the consumer must still never see one print twice.
        let a = rest_trade(9_999_999, 1100, "r1");
        let mut b = rest_trade(9_999_999, 1100, "r1");
        b.price_micro = 480_000;
        let evs = collect(
            EventIter::from_sources(src(vec![]), src(vec![]), src(vec![a, b])).expect("build"),
        );
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            ReplayEvent::Trade { price, source, .. } => {
                assert_eq!(price.0, 470_000, "first copy in key order wins");
                assert_eq!(*source, TradeSource::RestBackfill);
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[test]
    fn deduped_stream_stays_sorted_with_unique_ids() {
        // consumer check #4 at unit level: dup pair + WS-only + REST-only.
        let mut ws_dup = ws_trade(1200, 1100, 5);
        ws_dup.trade_id = Some("t1".into());
        let trades = vec![
            ws_dup,
            ws_trade(1400, 1350, 6), // ws-6, unique
            rest_trade(9_999_999, 1100, "t1"),
            rest_trade(9_999_999, 1300, "t3"),
        ];
        let evs =
            collect(EventIter::from_sources(src(vec![]), src(vec![]), src(trades)).expect("build"));
        let ids: Vec<&str> = evs
            .iter()
            .filter_map(|e| match e {
                ReplayEvent::Trade { trade_id, .. } => trade_id.as_deref(),
                _ => None,
            })
            .collect();
        assert_eq!(
            ids,
            vec!["t1", "t3", "ws-6"],
            "each id exactly once, in ts order"
        );
        let ts: Vec<i64> = evs.iter().map(|e| e.ts().0).collect();
        let mut sorted = ts.clone();
        sorted.sort_unstable();
        assert_eq!(ts, sorted, "dedup preserves the order invariant");
    }
}
