//! Chunked Parquet row readers — the only module that touches arrow/parquet.
//!
//! Each table gets a typed row struct mirroring the kdp-process writer schema
//! (verified against `kdp-process/src/tables.rs`; the fixture round-trip test
//! is the drift tripwire) and a streaming `RowIter` over record batches
//! (bounded memory, R5). Decode failures are `LoadError::MalformedRow` AT the
//! offending row — never a skip (R4). The convenience float columns are never
//! read (ADR-004: integer units only).

use std::fs::File;
use std::path::Path;

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use kdp_core::{BookSide, Side};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::error::LoadError;

/// Record-batch size for streaming reads: small enough for bounded memory,
/// large enough to amortize the batch overhead.
const BATCH_SIZE: usize = 8192;

/// One `book_events` row (a snapshot level, a boundary marker, or a delta).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BookEventRow {
    pub event_idx: i64,
    pub recv_ts_us: i64,
    pub seq: Option<i64>,
    pub is_snapshot: bool,
    /// `None` on a snapshot's empty-side boundary marker row.
    pub side: Option<Side>,
    pub price_micro: Option<u32>,
    /// Snapshot rows: resting qty. Delta rows: SIGNED change.
    pub qty_centi: Option<i64>,
}

/// One `trades` row. `seq: None` means REST-backfilled (writer convention).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TradeRow {
    pub recv_ts_us: i64,
    pub event_ts_us: i64,
    pub seq: Option<i64>,
    pub price_micro: u32,
    pub count_centi: u64,
    pub taker_side: Side,
    pub taker_book_side: Option<BookSide>,
    pub trade_id: Option<String>,
}

/// One `gaps` row (a capture hole marker).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GapRow {
    pub recv_ts_us: i64,
    pub seq: Option<i64>,
    pub reason: String,
    pub detail: String,
}

/// Streaming typed-row iterator over one table's Parquet file.
pub(crate) struct RowIter<T> {
    table: &'static str,
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    idx: usize,
    row: usize,
    /// Fused after a BATCH-level error: a failed batch is not row-recoverable,
    /// and resuming with the next batch would silently lose the failed batch's
    /// rows behind a single earlier error.
    dead: bool,
    decode: fn(&RecordBatch, usize, usize, &'static str) -> Result<T, LoadError>,
}

impl<T> RowIter<T> {
    fn open(
        dir: &Path,
        table: &'static str,
        decode: fn(&RecordBatch, usize, usize, &'static str) -> Result<T, LoadError>,
    ) -> Result<RowIter<T>, LoadError> {
        let path = dir.join(format!("{table}.parquet"));
        if !path.exists() {
            return Err(LoadError::MissingTable { name: table, path });
        }
        let file = File::open(&path).map_err(|source| LoadError::Io {
            path: path.clone(),
            source,
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .and_then(|b| b.with_batch_size(BATCH_SIZE).build())
            .map_err(|e| LoadError::Parquet {
                table,
                detail: e.to_string(),
            })?;
        Ok(RowIter {
            table,
            reader,
            batch: None,
            idx: 0,
            row: 0,
            dead: false,
            decode,
        })
    }
}

impl<T> Iterator for RowIter<T> {
    type Item = Result<T, LoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.dead {
            return None;
        }
        loop {
            if let Some(batch) = &self.batch {
                if self.idx < batch.num_rows() {
                    let out = (self.decode)(batch, self.idx, self.row, self.table);
                    self.idx += 1;
                    self.row += 1;
                    return Some(out);
                }
                self.batch = None;
            }
            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.batch = Some(batch);
                    self.idx = 0;
                }
                Some(Err(e)) => {
                    self.dead = true;
                    return Some(Err(LoadError::Parquet {
                        table: self.table,
                        detail: format!("batch read failed at row {}: {e}", self.row),
                    }));
                }
                None => return None,
            }
        }
    }
}

/// Open the three replay tables (`gaps` is optional-in-practice but always
/// written by kdp-process, so absence is a `MissingTable` like the others —
/// no silent "assume no gaps").
pub(crate) fn book_events(dir: &Path) -> Result<RowIter<BookEventRow>, LoadError> {
    RowIter::open(dir, "book_events", decode_book_row)
}
pub(crate) fn trades(dir: &Path) -> Result<RowIter<TradeRow>, LoadError> {
    RowIter::open(dir, "trades", decode_trade_row)
}
pub(crate) fn gaps(dir: &Path) -> Result<RowIter<GapRow>, LoadError> {
    RowIter::open(dir, "gaps", decode_gap_row)
}

// --- per-column typed access (by NAME, never index) ---------------------------

fn malformed(table: &'static str, row: usize, detail: String) -> LoadError {
    LoadError::MalformedRow { table, row, detail }
}

fn col<'a, A: 'static>(
    b: &'a RecordBatch,
    name: &str,
    table: &'static str,
    row: usize,
) -> Result<&'a A, LoadError> {
    b.column_by_name(name)
        .ok_or_else(|| malformed(table, row, format!("column `{name}` missing")))?
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| malformed(table, row, format!("column `{name}` has unexpected type")))
}

fn req_i64(
    b: &RecordBatch,
    name: &str,
    t: &'static str,
    i: usize,
    row: usize,
) -> Result<i64, LoadError> {
    let a: &Int64Array = col(b, name, t, row)?;
    if a.is_null(i) {
        return Err(malformed(
            t,
            row,
            format!("column `{name}` unexpectedly null"),
        ));
    }
    Ok(a.value(i))
}

fn opt_i64(
    b: &RecordBatch,
    name: &str,
    t: &'static str,
    i: usize,
    row: usize,
) -> Result<Option<i64>, LoadError> {
    let a: &Int64Array = col(b, name, t, row)?;
    Ok((!a.is_null(i)).then(|| a.value(i)))
}

fn req_bool(
    b: &RecordBatch,
    name: &str,
    t: &'static str,
    i: usize,
    row: usize,
) -> Result<bool, LoadError> {
    let a: &BooleanArray = col(b, name, t, row)?;
    if a.is_null(i) {
        return Err(malformed(
            t,
            row,
            format!("column `{name}` unexpectedly null"),
        ));
    }
    Ok(a.value(i))
}

fn opt_str(
    b: &RecordBatch,
    name: &str,
    t: &'static str,
    i: usize,
    row: usize,
) -> Result<Option<String>, LoadError> {
    let a: &StringArray = col(b, name, t, row)?;
    Ok((!a.is_null(i)).then(|| a.value(i).to_string()))
}

fn req_str(
    b: &RecordBatch,
    name: &str,
    t: &'static str,
    i: usize,
    row: usize,
) -> Result<String, LoadError> {
    opt_str(b, name, t, i, row)?
        .ok_or_else(|| malformed(t, row, format!("column `{name}` unexpectedly null")))
}

fn to_price(v: i64, t: &'static str, row: usize) -> Result<u32, LoadError> {
    u32::try_from(v).map_err(|_| malformed(t, row, format!("price_micro {v} out of u32 range")))
}

fn to_side(s: &str, t: &'static str, row: usize) -> Result<Side, LoadError> {
    match s {
        "yes" => Ok(Side::Yes),
        "no" => Ok(Side::No),
        other => Err(malformed(t, row, format!("invalid side {other:?}"))),
    }
}

// --- per-table decoders --------------------------------------------------------

fn decode_book_row(
    b: &RecordBatch,
    i: usize,
    row: usize,
    t: &'static str,
) -> Result<BookEventRow, LoadError> {
    let side = match opt_str(b, "side", t, i, row)? {
        Some(s) => Some(to_side(&s, t, row)?),
        None => None,
    };
    let price_micro = match opt_i64(b, "price_micro", t, i, row)? {
        Some(v) => Some(to_price(v, t, row)?),
        None => None,
    };
    Ok(BookEventRow {
        event_idx: req_i64(b, "event_idx", t, i, row)?,
        recv_ts_us: req_i64(b, "recv_ts_us", t, i, row)?,
        seq: opt_i64(b, "seq", t, i, row)?,
        is_snapshot: req_bool(b, "is_snapshot", t, i, row)?,
        side,
        price_micro,
        qty_centi: opt_i64(b, "qty_centi", t, i, row)?,
    })
}

fn decode_trade_row(
    b: &RecordBatch,
    i: usize,
    row: usize,
    t: &'static str,
) -> Result<TradeRow, LoadError> {
    let count = req_i64(b, "count_centi", t, i, row)?;
    let taker_book_side = match opt_str(b, "taker_book_side", t, i, row)?.as_deref() {
        None => None,
        Some("bid") => Some(BookSide::Bid),
        Some("ask") => Some(BookSide::Ask),
        Some(other) => {
            return Err(malformed(
                t,
                row,
                format!("invalid taker_book_side {other:?}"),
            ))
        }
    };
    Ok(TradeRow {
        recv_ts_us: req_i64(b, "recv_ts_us", t, i, row)?,
        event_ts_us: req_i64(b, "event_ts_us", t, i, row)?,
        seq: opt_i64(b, "seq", t, i, row)?,
        price_micro: to_price(req_i64(b, "price_micro", t, i, row)?, t, row)?,
        count_centi: u64::try_from(count)
            .map_err(|_| malformed(t, row, format!("negative count_centi {count}")))?,
        taker_side: to_side(&req_str(b, "taker_side", t, i, row)?, t, row)?,
        taker_book_side,
        trade_id: opt_str(b, "trade_id", t, i, row)?,
    })
}

fn decode_gap_row(
    b: &RecordBatch,
    i: usize,
    row: usize,
    t: &'static str,
) -> Result<GapRow, LoadError> {
    Ok(GapRow {
        recv_ts_us: req_i64(b, "recv_ts_us", t, i, row)?,
        seq: opt_i64(b, "seq", t, i, row)?,
        reason: req_str(b, "reason", t, i, row)?,
        detail: req_str(b, "detail", t, i, row)?,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use parquet::arrow::ArrowWriter;
    use std::path::PathBuf;
    use std::sync::Arc;

    pub(crate) fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("kdp-load-rd-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("mkdir tmp");
        d
    }

    fn i64s(v: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(v))
    }
    fn opt_i64s(v: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(v))
    }
    fn bools(v: Vec<bool>) -> ArrayRef {
        Arc::new(BooleanArray::from(v))
    }
    fn opt_strs(v: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }
    fn strs(v: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    fn write_table(dir: &Path, table: &str, cols: Vec<(&str, ArrayRef, bool)>) {
        let batch = RecordBatch::try_from_iter_with_nullable(cols).expect("assemble test batch");
        let file = File::create(dir.join(format!("{table}.parquet"))).expect("create");
        let mut w = ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
        w.write(&batch).expect("write");
        w.close().expect("close");
    }

    /// Write a book_events table. Rows: (event_idx, recv, seq, is_snapshot, side, price, qty).
    pub(crate) type BookSpec = (
        i64,
        i64,
        Option<i64>,
        bool,
        Option<&'static str>,
        Option<i64>,
        Option<i64>,
    );
    pub(crate) fn write_book_events(dir: &Path, rows: &[BookSpec]) {
        write_table(
            dir,
            "book_events",
            vec![
                ("event_idx", i64s(rows.iter().map(|r| r.0).collect()), false),
                (
                    "recv_ts_us",
                    i64s(rows.iter().map(|r| r.1).collect()),
                    false,
                ),
                // event_ts_us exists on disk; the loader doesn't read it for book rows.
                (
                    "event_ts_us",
                    i64s(rows.iter().map(|r| r.1).collect()),
                    false,
                ),
                ("seq", opt_i64s(rows.iter().map(|r| r.2).collect()), true),
                ("sid", opt_i64s(rows.iter().map(|_| None).collect()), true),
                (
                    "is_snapshot",
                    bools(rows.iter().map(|r| r.3).collect()),
                    false,
                ),
                ("side", opt_strs(rows.iter().map(|r| r.4).collect()), true),
                (
                    "price_micro",
                    opt_i64s(rows.iter().map(|r| r.5).collect()),
                    true,
                ),
                (
                    "qty_centi",
                    opt_i64s(rows.iter().map(|r| r.6).collect()),
                    true,
                ),
            ],
        );
    }

    /// Rows: (recv, event_ts, seq, price, count, taker_side, taker_book_side, trade_id).
    pub(crate) type TradeSpec = (
        i64,
        i64,
        Option<i64>,
        i64,
        i64,
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
    );
    pub(crate) fn write_trades(dir: &Path, rows: &[TradeSpec]) {
        write_table(
            dir,
            "trades",
            vec![
                (
                    "recv_ts_us",
                    i64s(rows.iter().map(|r| r.0).collect()),
                    false,
                ),
                (
                    "event_ts_us",
                    i64s(rows.iter().map(|r| r.1).collect()),
                    false,
                ),
                ("seq", opt_i64s(rows.iter().map(|r| r.2).collect()), true),
                (
                    "price_micro",
                    i64s(rows.iter().map(|r| r.3).collect()),
                    false,
                ),
                (
                    "count_centi",
                    i64s(rows.iter().map(|r| r.4).collect()),
                    false,
                ),
                (
                    "taker_side",
                    strs(rows.iter().map(|r| r.5).collect()),
                    false,
                ),
                (
                    "taker_book_side",
                    opt_strs(rows.iter().map(|r| r.6).collect()),
                    true,
                ),
                (
                    "trade_id",
                    opt_strs(rows.iter().map(|r| r.7).collect()),
                    true,
                ),
            ],
        );
    }

    /// Rows: (recv, seq, reason, detail).
    pub(crate) type GapSpec = (i64, Option<i64>, &'static str, &'static str);
    pub(crate) fn write_gaps(dir: &Path, rows: &[GapSpec]) {
        write_table(
            dir,
            "gaps",
            vec![
                (
                    "recv_ts_us",
                    i64s(rows.iter().map(|r| r.0).collect()),
                    false,
                ),
                ("seq", opt_i64s(rows.iter().map(|r| r.1).collect()), true),
                ("reason", strs(rows.iter().map(|r| r.2).collect()), false),
                (
                    "channel",
                    strs(rows.iter().map(|_| "orderbook").collect()),
                    false,
                ),
                (
                    "last_seq",
                    opt_i64s(rows.iter().map(|_| None).collect()),
                    true,
                ),
                (
                    "observed_seq",
                    opt_i64s(rows.iter().map(|_| None).collect()),
                    true,
                ),
                ("detail", strs(rows.iter().map(|r| r.3).collect()), false),
            ],
        );
    }

    /// GENERATOR, not a test: writes the committed mixed fixture
    /// `tests/fixture/KDPSYNTH-R6MIX` (overlapping WS+REST trades + a gap) --
    /// synthesized but schema-true (same writer helpers as the unit tests).
    /// Content is fixed, so re-running is an idempotent no-op diff. Run:
    /// `cargo test -p kdp-load --lib -- --ignored generate_mixed_fixture`
    #[test]
    #[ignore = "generator: writes the committed tests/fixture/KDPSYNTH-R6MIX"]
    fn generate_mixed_fixture() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixture/KDPSYNTH-R6MIX");
        std::fs::create_dir_all(&dir).expect("mkdir fixture");
        // Book: anchor@1.0s, deltas, a capture hole@1.3s (seq 3 -> 5),
        // re-anchor@1.5s, deltas to 1.9s. Timestamps are microseconds.
        write_book_events(
            &dir,
            &[
                (
                    1,
                    1_000_000,
                    Some(1),
                    true,
                    Some("yes"),
                    Some(450_000),
                    Some(1_000),
                ),
                (
                    1,
                    1_000_000,
                    Some(1),
                    true,
                    Some("no"),
                    Some(540_000),
                    Some(800),
                ),
                (
                    2,
                    1_100_000,
                    Some(2),
                    false,
                    Some("yes"),
                    Some(450_000),
                    Some(-200),
                ),
                (
                    3,
                    1_200_000,
                    Some(3),
                    false,
                    Some("no"),
                    Some(530_000),
                    Some(400),
                ),
                (
                    4,
                    1_500_000,
                    Some(5),
                    true,
                    Some("yes"),
                    Some(455_000),
                    Some(900),
                ),
                (
                    4,
                    1_500_000,
                    Some(5),
                    true,
                    Some("no"),
                    Some(535_000),
                    Some(700),
                ),
                (
                    5,
                    1_700_000,
                    Some(6),
                    false,
                    Some("yes"),
                    Some(455_000),
                    Some(300),
                ),
                (
                    6,
                    1_900_000,
                    Some(7),
                    false,
                    Some("no"),
                    Some(535_000),
                    Some(-100),
                ),
            ],
        );
        // Trades: t1 observed BOTH ways (WS + a REST copy fetched much later)
        // -> R6 dedup; t2 WS-only; t3 REST-only -- it printed INSIDE the hole
        // (exec 1.35s) and only the backfill saw it. REST rows sit at the file
        // END with fetch-time recv_ts, exactly like the real writer.
        write_trades(
            &dir,
            &[
                (
                    1_150_000,
                    1_140_000,
                    Some(2),
                    450_000,
                    500,
                    "yes",
                    Some("bid"),
                    Some("t1"),
                ),
                (
                    1_800_000,
                    1_790_000,
                    Some(6),
                    455_000,
                    250,
                    "no",
                    Some("ask"),
                    Some("t2"),
                ),
                (
                    9_000_000_000,
                    1_140_000,
                    None,
                    451_000,
                    500,
                    "yes",
                    None,
                    Some("t1"),
                ),
                (
                    9_000_000_000,
                    1_350_000,
                    None,
                    452_000,
                    100,
                    "no",
                    None,
                    Some("t3"),
                ),
            ],
        );
        write_gaps(
            &dir,
            &[(1_300_000, Some(4), "seq_jump", "seq jumped 3 -> 5")],
        );
        let manifest = serde_json::json!({
            "schema_version": 1,
            "ticker": "KDPSYNTH-R6MIX",
            "format": "parquet",
            "complete": true,
            "read_errors": 0,
            "counts": {"book_events": 8, "book_top": 0, "trades": 4, "gaps": 1, "raw": 0}
        });
        std::fs::write(dir.join("manifest.json"), manifest.to_string()).expect("write manifest");
    }

    #[test]
    fn book_events_reader_round_trips_rows_in_order() {
        let dir = tmp("book");
        write_book_events(
            &dir,
            &[
                (
                    1,
                    1000,
                    Some(1),
                    true,
                    Some("yes"),
                    Some(450_000),
                    Some(100),
                ),
                (1, 1000, Some(1), true, None, None, None), // empty-side boundary marker
                (
                    2,
                    2000,
                    Some(2),
                    false,
                    Some("yes"),
                    Some(450_000),
                    Some(-50),
                ),
            ],
        );
        let rows: Vec<_> = book_events(&dir).expect("open").collect();
        assert_eq!(rows.len(), 3);
        let r0 = rows[0].as_ref().expect("row 0");
        assert_eq!(
            (r0.event_idx, r0.side, r0.qty_centi),
            (1, Some(Side::Yes), Some(100))
        );
        let r1 = rows[1].as_ref().expect("row 1 (boundary)");
        assert_eq!((r1.side, r1.price_micro), (None, None));
        let r2 = rows[2].as_ref().expect("row 2");
        assert!(!r2.is_snapshot);
        assert_eq!(r2.qty_centi, Some(-50));
    }

    #[test]
    fn negative_price_is_a_malformed_row_not_a_skip() {
        let dir = tmp("badprice");
        write_book_events(
            &dir,
            &[
                (
                    1,
                    1000,
                    Some(1),
                    true,
                    Some("yes"),
                    Some(450_000),
                    Some(100),
                ),
                (2, 2000, Some(2), false, Some("yes"), Some(-1), Some(5)),
            ],
        );
        let rows: Vec<_> = book_events(&dir).expect("open").collect();
        assert!(rows[0].is_ok());
        match &rows[1] {
            Err(LoadError::MalformedRow { table, row, .. }) => {
                assert_eq!((*table, *row), ("book_events", 1));
            }
            other => panic!("expected MalformedRow, got {other:?}"),
        }
    }

    #[test]
    fn trades_reader_reads_ws_and_rest_rows() {
        let dir = tmp("trades");
        write_trades(
            &dir,
            &[
                (
                    1000,
                    900,
                    Some(7),
                    460_000,
                    500,
                    "yes",
                    Some("bid"),
                    Some("t1"),
                ),
                (99_999, 1500, None, 470_000, 200, "no", None, Some("t2")), // REST: null seq
            ],
        );
        let rows: Vec<_> = trades(&dir).expect("open").collect();
        let r0 = rows[0].as_ref().expect("ws row");
        assert_eq!(
            (r0.seq, r0.price_micro, r0.taker_book_side),
            (Some(7), 460_000, Some(BookSide::Bid))
        );
        let r1 = rows[1].as_ref().expect("rest row");
        assert_eq!((r1.seq, r1.event_ts_us), (None, 1500));
    }

    #[test]
    fn gaps_reader_round_trips() {
        let dir = tmp("gaps");
        write_gaps(&dir, &[(1500, Some(3), "seq_jump", "seq jumped 2 -> 5")]);
        let rows: Vec<_> = gaps(&dir).expect("open").collect();
        let r = rows[0].as_ref().expect("gap row");
        assert_eq!((r.recv_ts_us, r.reason.as_str()), (1500, "seq_jump"));
    }

    #[test]
    fn missing_table_is_typed() {
        let dir = tmp("missing");
        assert!(matches!(
            book_events(&dir),
            Err(LoadError::MissingTable {
                name: "book_events",
                ..
            })
        ));
    }
}
