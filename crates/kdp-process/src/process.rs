//! Per-ticker orchestration: read a capture directory, build the columnar
//! tables, and write them (+ a manifest) to the output directory.
//!
//! Input layout (produced by `kdp-cli capture`):
//! `<in>/<ticker>/<channel>/<YYYY-MM-DD>.jsonl`, channels `orderbook` + `trade`.
//! Output mirrors it: `<out>/<ticker>/{book_events,book_top,trades,gaps[,raw]}.<ext>`
//! plus `manifest.json` (and `read_errors.jsonl` if any line failed to decode).
//!
//! Order matters for book reconstruction: orderbook messages are replayed in
//! capture order (files sorted by date, lines in order), each assigned a
//! monotonic `event_idx`. Trades, gaps, and raw fallbacks are order-independent.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use kdp_core::RecordKind;

use crate::manifest::{us_to_rfc3339, Counts, Manifest, SourceFile, PROCESSED_SCHEMA_VERSION};
use crate::reader::{read_envelopes, ReadError};
use crate::tables::{BookEventsTable, BookTopTable, GapsTable, RawTable, TradesTable};
use crate::writer::{write_batch, Format};
use kdp_core::Book;

/// What [`process_ticker`] produced for one market (for the CLI summary).
#[derive(Debug, Clone)]
pub struct TickerOutcome {
    /// The market ticker.
    pub ticker: String,
    /// Per-table output row counts.
    pub counts: Counts,
    /// Number of unreadable source lines.
    pub read_errors: usize,
    /// True iff the structured tables fully represent the capture (no read errors
    /// and no raw fallbacks) — i.e. the raw JSONL is safe to drop.
    pub complete: bool,
}

/// List `*.jsonl` files in `dir`, sorted (date filenames sort chronologically).
/// A missing directory yields an empty list (that channel simply wasn't captured).
fn jsonl_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry
            .with_context(|| format!("reading an entry in {}", dir.display()))?
            .path();
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Discover ticker subdirectories under `in_dir` (sorted).
#[tracing::instrument(skip_all, fields(in_dir = %in_dir.display()))]
pub fn discover_tickers(in_dir: &Path) -> Result<Vec<String>> {
    let mut tickers = Vec::new();
    for entry in std::fs::read_dir(in_dir)
        .with_context(|| format!("reading input dir {}", in_dir.display()))?
    {
        let entry = entry.with_context(|| format!("reading an entry in {}", in_dir.display()))?;
        if entry
            .file_type()
            .with_context(|| format!("stat {}", entry.path().display()))?
            .is_dir()
        {
            if let Some(name) = entry.file_name().to_str() {
                tickers.push(name.to_string());
            }
        }
    }
    tickers.sort();
    Ok(tickers)
}

/// Process one ticker end-to-end. Returns `None` (with a warning) if the ticker
/// directory holds no capture files.
#[tracing::instrument(skip(in_dir, out_dir), fields(ticker = %ticker, ?format))]
pub fn process_ticker(
    in_dir: &Path,
    out_dir: &Path,
    ticker: &str,
    format: Format,
) -> Result<Option<TickerOutcome>> {
    let ticker_in = in_dir.join(ticker);
    let orderbook_files = jsonl_files(&ticker_in.join("orderbook"))?;
    let trade_files = jsonl_files(&ticker_in.join("trade"))?;
    if orderbook_files.is_empty() && trade_files.is_empty() {
        tracing::warn!(ticker, "no .jsonl capture files found; skipping");
        return Ok(None);
    }

    let mut book_events = BookEventsTable::default();
    let mut book_top = BookTopTable::default();
    let mut trades = TradesTable::default();
    let mut gaps = GapsTable::default();
    let mut raw = RawTable::default();

    let mut book = Book::default();
    let mut event_idx: i64 = 0;
    let mut read_errors: Vec<ReadError> = Vec::new();
    let mut first_recv: Option<i64> = None;
    let mut last_recv: Option<i64> = None;
    let mut source_files: Vec<SourceFile> = Vec::new();
    let mut two_sided_snapshots: usize = 0;

    for (channel, files) in [("orderbook", &orderbook_files), ("trade", &trade_files)] {
        for path in files {
            let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
            let mut records = 0usize;
            // 512 KiB buffer: a JSONL orderbook line is ~100-250 B, so this
            // coalesces thousands of lines per read syscall on large captures.
            let reader = BufReader::with_capacity(512 * 1024, file);
            for item in read_envelopes(reader) {
                records += 1;
                let env = match item {
                    Ok(env) => env,
                    Err(e) => {
                        tracing::warn!(channel, line = e.line_no, error = %e.error, "unreadable line preserved to read_errors.jsonl");
                        read_errors.push(e);
                        continue;
                    }
                };
                let recv = env.recv_ts.0.timestamp_micros();
                first_recv = Some(first_recv.map_or(recv, |m| m.min(recv)));
                last_recv = Some(last_recv.map_or(recv, |m| m.max(recv)));
                match &env.kind {
                    RecordKind::Snapshot(s) => {
                        let idx = event_idx;
                        event_idx += 1;
                        book_events.push_snapshot(idx, &env, s)?;
                        book.apply_snapshot(s);
                        let evt = s.ts.0.timestamp_micros();
                        let top = book.top();
                        if top.yes_bid_micro.is_some() && top.yes_ask_micro.is_some() {
                            two_sided_snapshots += 1;
                        }
                        book_top.push(idx, &env, evt, "snapshot", &top);
                    }
                    RecordKind::Delta(d) => {
                        let idx = event_idx;
                        event_idx += 1;
                        book_events.push_delta(idx, &env, d);
                        book.apply_delta(d);
                        let evt = d.ts.0.timestamp_micros();
                        let top = book.top();
                        if top.yes_bid_micro.is_some() && top.yes_ask_micro.is_some() {
                            two_sided_snapshots += 1;
                        }
                        book_top.push(idx, &env, evt, "delta", &top);
                    }
                    RecordKind::Trade(t) => trades.push(&env, t)?,
                    RecordKind::Gap(g) => gaps.push(&env, g),
                    RecordKind::Raw(r) => raw.push(&env, channel, r),
                }
            }
            source_files.push(SourceFile {
                channel: channel.to_string(),
                path: path.display().to_string(),
                records,
            });
        }
    }

    // Finish all builders first; the batch row counts are the single source of
    // truth for the manifest (no separate length bookkeeping to drift).
    let book_events = book_events.finish()?;
    let book_top = book_top.finish()?;
    let trades = trades.finish()?;
    let gaps = gaps.finish()?;
    let raw = raw.finish()?;
    let counts = Counts {
        book_events: book_events.num_rows(),
        book_top: book_top.num_rows(),
        trades: trades.num_rows(),
        gaps: gaps.num_rows(),
        raw: raw.num_rows(),
    };

    let ticker_out = out_dir.join(ticker);
    std::fs::create_dir_all(&ticker_out)
        .with_context(|| format!("creating output dir {}", ticker_out.display()))?;
    let ext = format.extension();
    write_batch(
        &book_events,
        &ticker_out.join(format!("book_events.{ext}")),
        format,
    )?;
    write_batch(
        &book_top,
        &ticker_out.join(format!("book_top.{ext}")),
        format,
    )?;
    write_batch(&trades, &ticker_out.join(format!("trades.{ext}")), format)?;
    write_batch(&gaps, &ticker_out.join(format!("gaps.{ext}")), format)?;
    // Only emit `raw` when there is something to preserve.
    if counts.raw > 0 {
        write_batch(&raw, &ticker_out.join(format!("raw.{ext}")), format)?;
    }

    if !read_errors.is_empty() {
        write_read_errors(&ticker_out.join("read_errors.jsonl"), &read_errors)?;
    }

    // `complete` means the structured tables fully represent the capture: every
    // line decoded (no read errors) AND nothing was left only as a raw fallback.
    // Both read errors and raw fallbacks are still preserved (in read_errors.jsonl
    // / raw.{ext}), but their presence means the structured output is not the
    // whole story, so it is not a clean "drop the raw" signal on its own.
    let complete = read_errors.is_empty() && counts.raw == 0;
    let mut notes = Vec::new();
    if counts.gaps > 0 {
        notes.push(format!(
            "{} gap marker(s) recorded in gaps.{ext}; holes are expected and preserved",
            counts.gaps
        ));
    }
    if counts.raw > 0 {
        notes.push(format!(
            "{} raw fallback(s) preserved in raw.{ext} (undecoded at capture); review before deleting the source",
            counts.raw
        ));
    }
    if !read_errors.is_empty() {
        notes.push(format!(
            "{} unreadable line(s) saved to read_errors.jsonl; investigate before deleting the source",
            read_errors.len()
        ));
    }
    if complete {
        notes.push(
            "all source records decoded into the structured tables; the raw capture files are safe to drop once these outputs are verified".to_string(),
        );
    } else {
        notes.push(
            "output is INCOMPLETE (raw fallbacks and/or read errors present); do NOT delete the raw capture files until the items above are reviewed".to_string(),
        );
    }

    let manifest = Manifest {
        schema_version: PROCESSED_SCHEMA_VERSION,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        ticker: ticker.to_string(),
        format: ext.to_string(),
        processed_at: chrono::Utc::now().to_rfc3339(),
        source_dir: ticker_in.display().to_string(),
        source_files,
        first_recv_ts_us: first_recv,
        last_recv_ts_us: last_recv,
        first_recv_ts: first_recv.and_then(us_to_rfc3339),
        last_recv_ts: last_recv.and_then(us_to_rfc3339),
        counts: counts.clone(),
        read_errors: read_errors.len(),
        complete,
        notes,
        two_sided: two_sided_snapshots > 0,
        two_sided_snapshots,
    };
    manifest.write(&ticker_out.join("manifest.json"))?;

    Ok(Some(TickerOutcome {
        ticker: ticker.to_string(),
        counts,
        read_errors: read_errors.len(),
        complete,
    }))
}

fn write_read_errors(path: &Path, errors: &[ReadError]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    for e in errors {
        // `ReadError: Serialize`, so each line streams straight to the writer with
        // no intermediate Value/String per error.
        serde_json::to_writer(&mut writer, e)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

/// Process either all discovered tickers (`only = None`) or a specific subset.
#[tracing::instrument(skip_all, fields(in_dir = %in_dir.display(), out_dir = %out_dir.display(), ?format))]
pub fn run(
    in_dir: &Path,
    out_dir: &Path,
    only: Option<&[String]>,
    format: Format,
) -> Result<Vec<TickerOutcome>> {
    let discovered = discover_tickers(in_dir)?;
    let selected: Vec<String> = match only {
        Some(list) => {
            for t in list {
                if !discovered.contains(t) {
                    bail!(
                        "ticker {:?} not found under {} (have: {})",
                        t,
                        in_dir.display(),
                        discovered.join(", ")
                    );
                }
            }
            list.to_vec()
        }
        None => discovered,
    };

    let mut outcomes = Vec::new();
    for ticker in &selected {
        if let Some(outcome) = process_ticker(in_dir, out_dir, ticker, format)? {
            outcomes.push(outcome);
        }
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// A tiny capture tree under a unique temp dir; returns its path.
    fn fixture(name: &str) -> PathBuf {
        let mut root = std::env::temp_dir();
        root.push(format!("kdp_proc_proc_{}_{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&root);
        let ob = root.join("KDPTEST-T1").join("orderbook");
        let tr = root.join("KDPTEST-T1").join("trade");
        std::fs::create_dir_all(&ob).unwrap();
        std::fs::create_dir_all(&tr).unwrap();

        let snap = r#"{"v":1,"recv_ts":"2026-05-30T00:00:00Z","seq":1,"sid":1,"kind":"snapshot","data":{"ticker":"KDPTEST-T1","ts":"2026-05-30T00:00:00Z","yes":[{"price":450000,"quantity":100}],"no":[{"price":540000,"quantity":200}]}}"#;
        let delta = r#"{"v":1,"recv_ts":"2026-05-30T00:00:01Z","seq":2,"sid":1,"kind":"delta","data":{"ticker":"KDPTEST-T1","ts":"2026-05-30T00:00:01Z","side":"yes","price":460000,"delta":500}}"#;
        let mut f = File::create(ob.join("2026-05-30.jsonl")).unwrap();
        writeln!(f, "{snap}").unwrap();
        writeln!(f, "{delta}").unwrap();

        let trade = r#"{"v":1,"recv_ts":"2026-05-30T00:00:02Z","seq":1,"sid":2,"kind":"trade","data":{"ticker":"KDPTEST-T1","ts":"2026-05-30T00:00:02Z","price":460000,"count":5000,"taker_side":"yes","taker_book_side":"bid","trade_id":"t1"}}"#;
        let mut g = File::create(tr.join("2026-05-30.jsonl")).unwrap();
        writeln!(g, "{trade}").unwrap();

        root
    }

    fn parquet_rows(path: &Path) -> usize {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        reader.map(|b| b.unwrap().num_rows()).sum()
    }

    /// Read a small Parquet file as a single concatenated batch (test-only).
    fn read_one_batch(path: &Path) -> arrow::record_batch::RecordBatch {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    fn col_i64<'a>(
        b: &'a arrow::record_batch::RecordBatch,
        name: &str,
    ) -> &'a arrow::array::Int64Array {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
    }

    #[test]
    fn processes_a_ticker_end_to_end_and_is_drop_safe() {
        let root = fixture("e2e");
        let out = root.with_file_name("e2e_out");
        let _ = std::fs::remove_dir_all(&out);

        let outcomes = run(&root, &out, None, Format::Parquet).unwrap();
        assert_eq!(outcomes.len(), 1);
        let o = &outcomes[0];
        assert_eq!(o.ticker, "KDPTEST-T1");
        assert_eq!(o.counts.book_top, 2, "snapshot + delta = 2 orderbook msgs");
        // book_events: 2 snapshot levels (1 yes + 1 no) + 1 delta = 3 rows.
        assert_eq!(o.counts.book_events, 3);
        assert_eq!(o.counts.trades, 1);
        assert_eq!(o.counts.gaps, 0);
        assert!(o.complete, "no read errors -> safe to drop raw");

        let tdir = out.join("KDPTEST-T1");
        assert_eq!(parquet_rows(&tdir.join("book_events.parquet")), 3);
        assert_eq!(parquet_rows(&tdir.join("book_top.parquet")), 2);
        assert_eq!(parquet_rows(&tdir.join("trades.parquet")), 1);
        assert!(tdir.join("manifest.json").is_file());
        assert!(!tdir.join("raw.parquet").exists(), "no raw -> no raw table");
        assert!(!tdir.join("read_errors.jsonl").exists());

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn multi_day_files_continue_event_idx_and_carry_book_across_files() {
        let root = fixture("multiday");
        // fixture wrote orderbook/2026-05-30.jsonl = snapshot(yes 450000:100,
        // no 540000:200) + delta(yes 460000 +500). Add a SECOND date file whose
        // delta only makes sense if the day-1 book carried forward.
        let ob_dir = root.join("KDPTEST-T1").join("orderbook");
        let day2 = r#"{"v":1,"recv_ts":"2026-05-31T00:00:00Z","seq":9,"sid":1,"kind":"delta","data":{"ticker":"KDPTEST-T1","ts":"2026-05-31T00:00:00Z","side":"yes","price":470000,"delta":300}}"#;
        let mut f = File::create(ob_dir.join("2026-05-31.jsonl")).unwrap();
        writeln!(f, "{day2}").unwrap();

        let out = root.with_file_name("multiday_out");
        let _ = std::fs::remove_dir_all(&out);
        run(&root, &out, None, Format::Parquet).unwrap();

        let bt = read_one_batch(&out.join("KDPTEST-T1").join("book_top.parquet"));
        assert_eq!(bt.num_rows(), 3, "day1 snapshot + day1 delta + day2 delta");
        let event_idx = col_i64(&bt, "event_idx");
        assert_eq!(event_idx.value(0), 0);
        assert_eq!(
            event_idx.value(2),
            2,
            "event_idx is continuous across date files"
        );
        // At the day-2 event the book still holds day-1's levels:
        // yes = {450000:100, 460000:500, 470000:300}.
        assert_eq!(
            col_i64(&bt, "yes_levels").value(2),
            3,
            "day-1 levels carried"
        );
        assert_eq!(
            col_i64(&bt, "yes_total_centi").value(2),
            900,
            "100+500+300 — book state carried across files, not reset"
        );
        assert_eq!(col_i64(&bt, "yes_bid_micro").value(2), 470_000);

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn unreadable_line_blocks_drop_safety_and_is_saved() {
        let root = fixture("bad");
        // Append a corrupt line to the orderbook file.
        let ob = root
            .join("KDPTEST-T1")
            .join("orderbook")
            .join("2026-05-30.jsonl");
        let mut f = std::fs::OpenOptions::new().append(true).open(&ob).unwrap();
        writeln!(f, "{{ this is not json").unwrap();

        let out = root.with_file_name("bad_out");
        let _ = std::fs::remove_dir_all(&out);
        let outcomes = run(&root, &out, None, Format::Parquet).unwrap();
        let o = &outcomes[0];
        assert_eq!(o.read_errors, 1);
        assert!(!o.complete, "a read error must block drop-safety");
        assert!(out.join("KDPTEST-T1").join("read_errors.jsonl").is_file());

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn raw_fallback_blocks_drop_safety_and_is_preserved() {
        let root = fixture("raw");
        // Append a raw-fallback record (a message kdp couldn't decode at capture).
        let ob = root
            .join("KDPTEST-T1")
            .join("orderbook")
            .join("2026-05-30.jsonl");
        let mut f = std::fs::OpenOptions::new().append(true).open(&ob).unwrap();
        let raw_line = r#"{"v":1,"recv_ts":"2026-05-30T00:00:03Z","kind":"raw","data":{"raw_type":"weird","error":"unknown shape","payload":{"x":1}}}"#;
        f.write_all(raw_line.as_bytes()).unwrap();
        f.write_all(b"\n").unwrap();

        let out = root.with_file_name("raw_out");
        let _ = std::fs::remove_dir_all(&out);
        let outcomes = run(&root, &out, None, Format::Parquet).unwrap();
        let o = &outcomes[0];
        assert_eq!(o.counts.raw, 1);
        assert_eq!(o.read_errors, 0);
        assert!(
            !o.complete,
            "a raw fallback must block drop-safety even with 0 read errors"
        );
        assert!(out.join("KDPTEST-T1").join("raw.parquet").is_file());

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn requesting_an_unknown_ticker_errors() {
        let root = fixture("unknown");
        let out = root.with_file_name("unknown_out");
        let err = run(&root, &out, Some(&["NOPE".to_string()]), Format::Parquet).unwrap_err();
        assert!(err.to_string().contains("not found"));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Build a temp capture tree for a single ticker with just one orderbook
    /// JSONL file containing the given lines.  Returns the root input dir.
    fn fixture_ob_lines(name: &str, ticker: &str, lines: &[&str]) -> PathBuf {
        let mut root = std::env::temp_dir();
        root.push(format!("kdp_proc_ts_{}_{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&root);
        let ob = root.join(ticker).join("orderbook");
        let tr = root.join(ticker).join("trade");
        std::fs::create_dir_all(&ob).unwrap();
        std::fs::create_dir_all(&tr).unwrap();
        let mut f = File::create(ob.join("2026-06-01.jsonl")).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        root
    }

    /// Read the manifest.json for `ticker` under `out` and return it as a
    /// `serde_json::Value`.
    fn read_manifest(out: &Path, ticker: &str) -> serde_json::Value {
        let path = out.join(ticker).join("manifest.json");
        let json = std::fs::read_to_string(&path).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn one_sided_strike_is_flagged_not_two_sided() {
        // Only a yes side; no_dollars_fp is empty -> yes_ask_micro is None.
        let snap = r#"{"v":1,"recv_ts":"2026-06-01T00:00:00Z","seq":1,"sid":1,"kind":"snapshot","data":{"ticker":"KDPTEST-OS","ts":"2026-06-01T00:00:00Z","yes":[{"price":20000,"quantity":1000}],"no":[]}}"#;
        let root = fixture_ob_lines("one_sided", "KDPTEST-OS", &[snap]);
        let out = root.with_file_name("one_sided_out");
        let _ = std::fs::remove_dir_all(&out);

        run(&root, &out, None, Format::Parquet).unwrap();

        let m = read_manifest(&out, "KDPTEST-OS");
        assert_eq!(
            m["two_sided"], false,
            "one-sided book must not be flagged two_sided"
        );
        assert_eq!(m["two_sided_snapshots"], 0);

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn strike_with_both_sides_is_two_sided() {
        // Both yes and no sides present -> two-way market.
        let snap = r#"{"v":1,"recv_ts":"2026-06-01T00:00:00Z","seq":1,"sid":1,"kind":"snapshot","data":{"ticker":"KDPTEST-TS","ts":"2026-06-01T00:00:00Z","yes":[{"price":450000,"quantity":1000}],"no":[{"price":540000,"quantity":900}]}}"#;
        let root = fixture_ob_lines("two_sided", "KDPTEST-TS", &[snap]);
        let out = root.with_file_name("two_sided_out");
        let _ = std::fs::remove_dir_all(&out);

        run(&root, &out, None, Format::Parquet).unwrap();

        let m = read_manifest(&out, "KDPTEST-TS");
        assert_eq!(
            m["two_sided"], true,
            "book with both sides must be flagged two_sided"
        );
        assert_eq!(m["two_sided_snapshots"], 1);

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&out);
    }
}
