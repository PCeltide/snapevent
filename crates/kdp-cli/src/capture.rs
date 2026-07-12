//! `capture` — drive a live WS session into append-only JSONL on disk.
//!
//! Wires the producer/consumer pipeline (decisions D6/D9): a [`run_session`]
//! producer task forwards decoded [`CaptureEvent`]s through a bounded channel to
//! a writer task that owns the [`StreamSet`] + [`DiskGuard`]. The bounded channel
//! gives never-drop backpressure (a slow disk slows reads, never drops a record).
//! Shutdown is ctrl-c or `--duration`; on shutdown the producer stops, the
//! channel drains, and a run report is printed.
//!
//! Per-record classification ([`classify`]) and the [`CaptureReport`] accounting
//! are pure and unit-tested; the session + disk I/O are verified by the live
//! capture proof.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tokio::sync::mpsc;
use tracing::{error, info, instrument, warn};

use kdp_core::{Envelope, OrderbookSnapshot, RawFallback, RecordKind, Ticker, Timestamp};
use kdp_kalshi::auth::KalshiCredentials;
use kdp_kalshi::rest::{get_orderbooks, RestOrderbookOutcome};
use kdp_kalshi::ws::protocol::FrameOutcome;
use kdp_kalshi::ws::session::{run_session, CaptureEvent};
use kdp_store::{is_safe_segment, DiskGuard, StreamSet};

use crate::args::Args;

/// Channel used for raw records whose channel could not be determined.
const RAW_CHANNEL: &str = "_raw";
/// Ticker used for raw records whose market could not be determined.
const UNROUTED_TICKER: &str = "_unrouted";
/// Quarantine ticker for a (server-controlled) ticker that is not a safe path
/// segment — defence in depth against path traversal.
const UNSAFE_TICKER: &str = "_unsafe";
/// Re-check free disk space every this many writes.
const DISK_CHECK_EVERY: u64 = 500;

/// Return `ticker` if it is a safe path segment, else quarantine it under
/// [`UNSAFE_TICKER`] so a hostile wire ticker can never escape the base dir
/// (the `kdp-store` writer also rejects unsafe keys as a second line of defence).
fn safe_ticker(ticker: String) -> String {
    if is_safe_segment(&ticker) {
        ticker
    } else {
        warn!(unsafe_ticker = %ticker, "ticker is not a safe path segment; quarantining");
        UNSAFE_TICKER.to_string()
    }
}

/// Running tally for the capture run report.
#[derive(Debug, Default)]
pub struct CaptureReport {
    /// Records written per `(ticker, channel)` stream (includes raw fallbacks and
    /// gap markers — both are real persisted records, not control frames).
    pub per_stream: BTreeMap<(String, String), u64>,
    /// How many written records were raw fallbacks. Included in `per_stream` (and
    /// thus `total_written()`); reported separately for observability.
    pub raw: u64,
    /// How many written records were inline gap markers (seq jumps + reconnects).
    /// Included in `per_stream` (and thus `total_written()`); reported separately.
    pub gaps: u64,
    /// How many written records came from the periodic REST verify sweep
    /// (`RecordKind::Verify` snapshots, or their raw fallbacks). Included in
    /// `per_stream` (and thus `total_written()`); reported separately.
    pub verify: u64,
    /// Control frames observed (not persisted).
    pub control: u64,
    /// Server error frames observed.
    pub server_errors: u64,
    /// Free bytes at the last disk check.
    pub free_bytes: Option<u64>,
    /// If the writer halted mid-capture (disk full, append I/O error), the reason
    /// — so the report still prints and the error is surfaced afterward.
    pub halt_error: Option<String>,
}

impl CaptureReport {
    fn count_write(&mut self, ticker: &str, channel: &str) {
        *self
            .per_stream
            .entry((ticker.to_string(), channel.to_string()))
            .or_default() += 1;
    }

    /// Total records written across all streams (includes raw fallbacks and gap
    /// markers, which are persisted records — do not subtract `gaps`/`raw` from
    /// this expecting "data records"; they are already counted here).
    pub fn total_written(&self) -> u64 {
        self.per_stream.values().sum()
    }
}

/// What to persist for one event (or `None` for non-persisted frames).
#[derive(Debug)]
struct WriteAction {
    ticker: String,
    channel: String,
    envelope: Envelope,
    is_raw: bool,
}

/// Classify a capture event: update `report` and return the write to perform, if
/// any. No disk I/O; `control`/`error` frames are counted but not persisted. May
/// emit a `warn!` for a server-error frame or an unsafe (quarantined) ticker.
fn classify(event: CaptureEvent, report: &mut CaptureReport) -> Option<WriteAction> {
    let CaptureEvent { recv_ts, frame } = event;
    let (sid, seq) = (frame.sid, frame.seq);
    match frame.outcome {
        FrameOutcome::Data {
            channel,
            ticker,
            record,
        } => {
            if matches!(&record, RecordKind::Gap(_)) {
                report.gaps += 1;
            }
            let ticker = safe_ticker(ticker.0);
            report.count_write(&ticker, channel);
            Some(WriteAction {
                ticker,
                channel: channel.to_string(),
                envelope: Envelope::new(recv_ts, seq, sid, record),
                is_raw: false,
            })
        }
        FrameOutcome::Raw {
            channel,
            ticker,
            fallback,
        } => {
            report.raw += 1;
            let channel = channel.unwrap_or(RAW_CHANNEL).to_string();
            let ticker = safe_ticker(
                ticker
                    .map(|t| t.0)
                    .unwrap_or_else(|| UNROUTED_TICKER.to_string()),
            );
            report.count_write(&ticker, &channel);
            Some(WriteAction {
                ticker,
                channel,
                envelope: Envelope::new(recv_ts, seq, sid, RecordKind::Raw(fallback)),
                is_raw: true,
            })
        }
        FrameOutcome::Control { .. } => {
            report.control += 1;
            None
        }
        FrameOutcome::ServerError { detail } => {
            report.server_errors += 1;
            warn!(%detail, "server error frame");
            None
        }
    }
}

/// One periodic REST verify-sweep result ready to append: the resolved stream
/// ticker plus the envelope to persist under its `orderbook` channel.
#[derive(Debug)]
struct VerifyWrite {
    ticker: String,
    env: Envelope,
}

/// Convert one REST orderbook-sweep outcome into its on-disk `(ticker,
/// Envelope)`. A decoded book becomes a [`RecordKind::Verify`] snapshot
/// (offline cross-verification only — never a replay re-anchor, see that
/// variant's doc); an undecodable book is preserved as a [`RecordKind::Raw`]
/// fallback, never dropped. No I/O, no logging (the sweep caller warns on the
/// undecodable case) — pure so it's unit-tested without a live server.
///
/// The resolved ticker is quarantined via [`safe_ticker`] exactly like the WS
/// path, since it also comes from server-controlled data.
fn verify_envelope(outcome: RestOrderbookOutcome, now: Timestamp) -> (String, Envelope) {
    match outcome {
        RestOrderbookOutcome::Book { ticker, yes, no } => {
            let ticker = safe_ticker(ticker);
            let snapshot = OrderbookSnapshot {
                ticker: Ticker(ticker.clone()),
                ts: now,
                yes,
                no,
            };
            (
                ticker,
                Envelope::new(now, None, None, RecordKind::Verify(snapshot)),
            )
        }
        RestOrderbookOutcome::Undecodable {
            ticker,
            error,
            payload,
        } => {
            let resolved = safe_ticker(
                ticker
                    .clone()
                    .unwrap_or_else(|| UNROUTED_TICKER.to_string()),
            );
            let fallback = RawFallback {
                raw_type: Some("rest_orderbook".to_string()),
                ticker: ticker.map(Ticker),
                error,
                payload,
            };
            (
                resolved,
                Envelope::new(now, None, None, RecordKind::Raw(fallback)),
            )
        }
    }
}

/// Max tickers per verify-sweep REST call, chunked HERE rather than left to
/// `get_orderbooks`'s own internal chunking. Mirrors
/// `kdp_kalshi::rest::ORDERBOOK_CHUNK_SIZE` (private to that crate; a call
/// with <= this many tickers makes `get_orderbooks`'s internal chunking a
/// no-op pass-through). Chunking here — not just there — is load-bearing: see
/// `verify_sweep`'s per-chunk stamping comment.
const VERIFY_SWEEP_CHUNK_SIZE: usize = 100;

/// Periodic REST orderbook cross-verification sweep (2026-07-11
/// external-methodology-review adoptable, open-items item 1): our per-`sid`
/// seq tracking is exact for transport loss but can't catch a venue-side
/// emission bug or a decode/replay bug on our own side. Polling `GET
/// /markets/orderbooks` every `interval` and persisting each result alongside
/// the WS stream gives an independent, offline-diffable cross-check.
///
/// Runs until `verify_tx`'s receiver is dropped (the writer task's channel
/// closes) or the caller aborts the task (session end, mirroring how
/// `supervisor.rs` aborts its settlement watcher). A failed chunk is a
/// `warn!` and the sweep moves on to the next chunk — never fatal to capture.
#[instrument(skip(creds, verify_tx), fields(tickers = tickers.len()))]
async fn verify_sweep(
    creds: Arc<KalshiCredentials>,
    tickers: Vec<String>,
    interval: Duration,
    verify_tx: mpsc::Sender<VerifyWrite>,
) {
    let client = match reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "verify sweep: could not build http client; sweep disabled");
            return;
        }
    };
    loop {
        tokio::time::sleep(interval).await;
        // Chunk HERE (not inside get_orderbooks) so each chunk is stamped
        // with its OWN `Utc::now()` immediately after ITS response returns.
        // A single sweep-wide timestamp (stamped once after fetching every
        // chunk) would be wrong: with e.g. 188 tickers = 2 chunks, and each
        // chunk retrying transient errors with backoff (up to ~31s), the
        // first chunk's book could be tens of seconds stale by the time it's
        // stamped -- a guaranteed false mismatch against an active book. A
        // failed chunk warns and continues to the next chunk, so one bad
        // chunk can't discard the other chunks' already-fetched results.
        for chunk in tickers.chunks(VERIFY_SWEEP_CHUNK_SIZE) {
            match get_orderbooks(&client, &creds, chunk).await {
                Ok(outcomes) => {
                    let now: Timestamp = Utc::now().into();
                    for outcome in outcomes {
                        if let RestOrderbookOutcome::Undecodable { ticker, error, .. } = &outcome {
                            warn!(
                                ticker = ?ticker,
                                %error,
                                "verify sweep: undecodable orderbook; persisting raw"
                            );
                        }
                        let (ticker, env) = verify_envelope(outcome, now);
                        if verify_tx.send(VerifyWrite { ticker, env }).await.is_err() {
                            return; // writer task gone; session must be ending
                        }
                    }
                }
                Err(e) => warn!(
                    error = %e,
                    chunk_len = chunk.len(),
                    "verify sweep: chunk failed; skipping this chunk"
                ),
            }
        }
    }
}

/// Parse a duration like `"300"`, `"90s"`, `"5m"`, or `"1h"` into a [`Duration`].
fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('h') {
        (n, 3600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        (s, 1)
    };
    let value: u64 = num
        .trim()
        .parse()
        .with_context(|| format!("invalid duration {s:?} (use e.g. 90s, 5m, 1h)"))?;
    let secs = value
        .checked_mul(mult)
        .with_context(|| format!("duration {s:?} is too large"))?;
    Ok(Duration::from_secs(secs))
}

/// The writer half: drain `rx` (the WS stream) and `verify_rx` (the periodic
/// REST verify sweep), persist each record, periodically guard disk.
///
/// Always returns the [`CaptureReport`] (so the run report prints even on a
/// mid-capture halt). A start-of-run disk-space failure is a hard error (nothing
/// has been captured yet); an in-flight append/disk-full error stops the loop and
/// is recorded in `report.halt_error` rather than discarding the report.
///
/// The loop exits ONLY when the WS channel (`rx`) yields `None` (session over).
/// `verify_rx` closing (verify sweep disabled, or its task ending) must NOT end
/// the loop — `verify_open` guards that arm off once it closes so `select!`
/// stops polling a permanently-empty channel.
async fn writer_task(
    mut rx: mpsc::Receiver<CaptureEvent>,
    mut verify_rx: mpsc::Receiver<VerifyWrite>,
    base_dir: String,
    floor_bytes: u64,
) -> anyhow::Result<CaptureReport> {
    let mut streams = StreamSet::new(&base_dir);
    let guard = DiskGuard::new(&base_dir, floor_bytes);
    // Fail fast if we're already below the floor (no report to preserve yet).
    guard
        .check()
        .context("insufficient free disk space to start capture")?;

    let mut report = CaptureReport::default();
    let mut writes_since_check: u64 = 0;
    let mut verify_open = true;

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else { break; };
                let Some(action) = classify(event, &mut report) else {
                    continue;
                };
                if action.is_raw {
                    warn!(
                        ticker = %action.ticker,
                        channel = %action.channel,
                        "persisting raw fallback record"
                    );
                }
                if let Err(e) = streams.append(&action.ticker, &action.channel, &action.envelope) {
                    error!(error = %e, ticker = %action.ticker, channel = %action.channel, "append failed; halting capture");
                    report.halt_error = Some(e.to_string());
                    break;
                }
                writes_since_check += 1;
            }
            vw = verify_rx.recv(), if verify_open => {
                let Some(VerifyWrite { ticker, env }) = vw else {
                    verify_open = false;
                    continue;
                };
                report.verify += 1;
                report.count_write(&ticker, "orderbook");
                if let Err(e) = streams.append(&ticker, "orderbook", &env) {
                    error!(error = %e, ticker = %ticker, channel = "orderbook", "verify append failed; halting capture");
                    report.halt_error = Some(e.to_string());
                    break;
                }
                writes_since_check += 1;
            }
        }

        if writes_since_check >= DISK_CHECK_EVERY {
            match guard.check() {
                Ok(free) => report.free_bytes = Some(free),
                Err(e) => {
                    error!(error = %e, "disk space below floor; halting capture");
                    report.halt_error = Some(e.to_string());
                    break;
                }
            }
            writes_since_check = 0;
        }
    }

    // The main loop exits the instant `rx` yields `None` -- but at that exact
    // moment `verify_rx` (64 slots) may still hold observations the sweep
    // already fetched and paid the REST call for; `select!` gives no ordering
    // guarantee between the two arms, so some can still be sitting unread.
    // Drain them now rather than silently dropping already-fetched data. Skip
    // the drain after a halt: a halt means disk/append is already unhealthy,
    // so touching it further would just risk masking the real error.
    if report.halt_error.is_none() {
        while let Ok(VerifyWrite { ticker, env }) = verify_rx.try_recv() {
            report.verify += 1;
            report.count_write(&ticker, "orderbook");
            if let Err(e) = streams.append(&ticker, "orderbook", &env) {
                error!(error = %e, ticker = %ticker, channel = "orderbook", "verify append failed while draining; halting capture");
                report.halt_error = Some(e.to_string());
                break;
            }
        }
    }

    // Final free-space reading for the report (best effort).
    if let Ok(free) = guard.check() {
        report.free_bytes = Some(free);
    }
    Ok(report)
}

/// Capture `tickers` (L2 + trades) into append-only JSONL under `base_dir` until
/// `shutdown` resolves. The reusable core shared by the `capture` CLI command and
/// every supervisor. Creates `base_dir`, runs the writer + `run_session`
/// pipeline (plus, when `verify_interval_secs > 0`, a periodic REST verify
/// sweep — see [`verify_sweep`]), prints the run report, and returns it (a
/// mid-run halt is surfaced as an error after the report prints). Caller
/// supplies credentials + the shutdown trigger.
///
/// `creds` is an `Arc` (not `&KalshiCredentials`, and `KalshiCredentials` is not
/// `Clone`) so the verify sweep — a separately spawned, independently-timed task
/// — can hold its own owned reference without borrowing across the `run_session`
/// await.
#[allow(clippy::too_many_arguments)]
#[instrument(skip(creds, shutdown), fields(tickers = tickers.len(), out = %base_dir))]
pub async fn capture_session(
    creds: Arc<KalshiCredentials>,
    tickers: &[String],
    base_dir: &str,
    floor_bytes: u64,
    capacity: usize,
    idle: Duration,
    verify_interval_secs: u64,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> anyhow::Result<CaptureReport> {
    std::fs::create_dir_all(base_dir)
        .with_context(|| format!("creating capture dir {base_dir}"))?;
    let (tx, rx) = mpsc::channel::<CaptureEvent>(capacity);
    let (verify_tx, verify_rx) = mpsc::channel::<VerifyWrite>(64);
    let writer = tokio::spawn(writer_task(
        rx,
        verify_rx,
        base_dir.to_string(),
        floor_bytes,
    ));

    let sweep = if verify_interval_secs > 0 {
        Some(tokio::spawn(verify_sweep(
            Arc::clone(&creds),
            tickers.to_vec(),
            Duration::from_secs(verify_interval_secs),
            verify_tx,
        )))
    } else {
        drop(verify_tx); // no sweep: close the channel so verify_rx sees it, not held open
        None
    };

    let session_result = run_session(
        &creds,
        kdp_kalshi::ws::KALSHI_WS_URL,
        tickers,
        tx,
        shutdown,
        idle,
    )
    .await;

    if let Some(sweep) = sweep {
        sweep.abort();
    }

    let report = writer
        .await
        .context("writer task join")?
        .context("capture could not start")?;
    match &session_result {
        Ok(stats) => info!(frames = stats.frames, end = ?stats.end, "session complete"),
        Err(e) => error!(error = %e, "session ended with error"),
    }
    print_report(&report, base_dir);
    if let Some(halt) = &report.halt_error {
        anyhow::bail!("capture halted mid-run: {halt}");
    }
    session_result.map(|_| report).map_err(anyhow::Error::from)
}

/// Parse `--verify-interval` (seconds between periodic REST verify sweeps;
/// `0` disables the sweep entirely). Default 900 (15 min, matching the
/// external-methodology-review reference cadence). Shared by every
/// capture-driving command so the flag behaves identically everywhere.
///
/// Nonzero values below 10s are rejected: kdp-process's verify engine keeps at
/// most one check in flight and force-resolves it when the NEXT verify record
/// arrives, so the sweep interval must comfortably exceed its 5s tolerance
/// window or an in-window check could be resolved as a false mismatch.
pub(crate) fn parse_verify_interval(args: &Args) -> anyhow::Result<u64> {
    let secs: u64 = args
        .get_or("verify-interval", "900")
        .parse()
        .context("--verify-interval must be a non-negative integer (seconds); 0 disables")?;
    if (1..10).contains(&secs) {
        anyhow::bail!(
            "--verify-interval must be 0 (disabled) or >= 10 seconds (it must exceed the \
             5s verify tolerance window; got {secs})"
        );
    }
    Ok(secs)
}

/// `capture --tickers A,B [--out DIR] [--duration 1h] [--disk-floor-gib N] [--buffer N] [--idle S] [--verify-interval 900]`
#[instrument(skip(args))]
pub async fn run_capture(args: &Args) -> anyhow::Result<()> {
    let tickers = parse_tickers(args)?;
    let base_dir = args.get_or("out", "data").to_string();
    let duration = match args.get("duration") {
        Some(s) => Some(parse_duration(s)?),
        None => None,
    };
    let disk_floor_gib: f64 = args
        .get_or("disk-floor-gib", "3")
        .parse()
        .context("--disk-floor-gib must be a number")?;
    if !disk_floor_gib.is_finite() || disk_floor_gib <= 0.0 {
        anyhow::bail!("--disk-floor-gib must be a positive, finite number (got {disk_floor_gib})");
    }
    let floor_bytes = (disk_floor_gib * 1024.0 * 1024.0 * 1024.0) as u64;
    if floor_bytes == 0 {
        anyhow::bail!("--disk-floor-gib is too small; the computed floor is 0 bytes");
    }
    let capacity: usize = args
        .get_or("buffer", "8192")
        .parse()
        .context("--buffer must be a positive integer")?;
    if capacity == 0 {
        anyhow::bail!("--buffer must be >= 1 (a bounded channel needs a non-zero capacity)");
    }
    let idle_secs: u64 = args
        .get_or("idle", "45")
        .parse()
        .context("--idle must be an integer (seconds)")?;
    let verify_interval_secs = parse_verify_interval(args)?;

    let creds = Arc::new(
        KalshiCredentials::from_env()
            .context("loading Kalshi credentials (.env / KDP_KALSHI_PRIVATE_KEY_PATH)")?,
    );

    info!(
        ?tickers,
        out = %base_dir,
        duration = ?duration,
        floor_gib = disk_floor_gib,
        floor_bytes,
        buffer = capacity,
        "starting capture"
    );

    let shutdown = async move {
        match duration {
            Some(d) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => info!("ctrl-c received; stopping capture"),
                    _ = tokio::time::sleep(d) => info!("capture duration elapsed; stopping"),
                }
            }
            None => {
                let _ = tokio::signal::ctrl_c().await;
                info!("ctrl-c received; stopping capture");
            }
        }
    };

    capture_session(
        creds,
        &tickers,
        &base_dir,
        floor_bytes,
        capacity,
        Duration::from_secs(idle_secs),
        verify_interval_secs,
        shutdown,
    )
    .await?;
    Ok(())
}

/// Parse and validate `--tickers A,B,C`.
fn parse_tickers(args: &Args) -> anyhow::Result<Vec<String>> {
    crate::parse_ticker_list(args.get("tickers")).context("capture")
}

/// Print the human-facing run report (user-facing CLI text → stderr).
fn print_report(report: &CaptureReport, base_dir: &str) {
    let bytes = dir_bytes(Path::new(base_dir));
    eprintln!("\n=== capture run report ===");
    eprintln!("output dir:      {base_dir}");
    eprintln!(
        "records written: {} (incl. {} gap markers, {} raw fallbacks, {} verify sweeps)",
        report.total_written(),
        report.gaps,
        report.raw,
        report.verify
    );
    for ((ticker, channel), count) in &report.per_stream {
        eprintln!("  {ticker} / {channel}: {count}");
    }
    eprintln!(
        "control frames:  {} (observed, not written)",
        report.control
    );
    eprintln!("server errors:   {}", report.server_errors);
    eprintln!("bytes on disk:   {bytes}");
    match report.free_bytes {
        Some(free) => eprintln!("free disk:       {} MiB", free / (1024 * 1024)),
        None => eprintln!("free disk:       (unmeasured)"),
    }
    if let Some(halt) = &report.halt_error {
        eprintln!("HALTED:          {halt}");
    }
    eprintln!("===========================\n");
}

/// Recursively sum the sizes of `.jsonl` files under `dir`.
///
/// Uses `file_type()` (which does not follow symlinks) and skips symlinks, so a
/// symlink cycle inside the capture tree cannot cause unbounded recursion.
fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            total += dir_bytes(&path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdp_core::Timestamp;
    use kdp_kalshi::ws::protocol::decode_frame;

    fn recv() -> Timestamp {
        "2026-05-30T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .expect("rfc3339")
            .into()
    }

    fn event(text: &str) -> CaptureEvent {
        CaptureEvent {
            recv_ts: recv(),
            frame: decode_frame(text, recv()),
        }
    }

    #[test]
    fn classify_data_returns_write_and_counts_stream() {
        let mut report = CaptureReport::default();
        let delta = event(
            r#"{"type":"orderbook_delta","sid":1,"seq":5,"msg":{"market_ticker":"KXTEST","price_dollars":"0.70","delta_fp":"-5.00","side":"yes","ts_ms":1}}"#,
        );
        let action = classify(delta, &mut report).expect("data is written");
        assert_eq!(action.ticker, "KXTEST");
        assert_eq!(action.channel, "orderbook");
        assert!(!action.is_raw);
        assert_eq!(action.envelope.seq, Some(5));
        assert_eq!(
            report.per_stream[&("KXTEST".to_string(), "orderbook".to_string())],
            1
        );
        assert_eq!(report.total_written(), 1);
    }

    #[test]
    fn classify_raw_routes_and_counts_raw() {
        let mut report = CaptureReport::default();
        // 7-dp price -> over-precise -> Raw, with ticker + orderbook channel known.
        let bad = event(
            r#"{"type":"orderbook_delta","sid":1,"seq":9,"msg":{"market_ticker":"KXTEST","price_dollars":"0.1234567","delta_fp":"1.00","side":"yes","ts_ms":1}}"#,
        );
        let action = classify(bad, &mut report).expect("raw is still written");
        assert!(action.is_raw);
        assert_eq!(action.ticker, "KXTEST");
        assert_eq!(action.channel, "orderbook");
        assert_eq!(report.raw, 1);
        assert_eq!(report.total_written(), 1);
    }

    #[test]
    fn classify_unrouted_raw_uses_fallback_stream() {
        let mut report = CaptureReport::default();
        let action = classify(event("not json at all"), &mut report).expect("raw written");
        assert!(action.is_raw);
        assert_eq!(action.ticker, UNROUTED_TICKER);
        assert_eq!(action.channel, RAW_CHANNEL);
    }

    #[test]
    fn classify_control_is_not_written() {
        let mut report = CaptureReport::default();
        let ctrl = event(r#"{"type":"subscribed","msg":{"sid":1}}"#);
        assert!(classify(ctrl, &mut report).is_none());
        assert_eq!(report.control, 1);
        assert_eq!(report.total_written(), 0);
    }

    #[test]
    fn classify_server_error_is_counted_not_written() {
        let mut report = CaptureReport::default();
        let err = event(r#"{"type":"error","msg":{"msg":"bad sub"}}"#);
        assert!(classify(err, &mut report).is_none());
        assert_eq!(report.server_errors, 1);
    }

    #[test]
    fn parse_duration_handles_suffixes() {
        assert_eq!(parse_duration("300").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn parse_duration_rejects_overflow() {
        // num itself overflows u64
        assert!(parse_duration("99999999999999999999h").is_err());
        // u64::MAX seconds * 3600 overflows the checked_mul
        assert!(parse_duration("18446744073709551615h").is_err());
    }

    #[test]
    fn safe_ticker_quarantines_path_traversal() {
        assert_eq!(
            safe_ticker("KXBTCD-26MAY3017-T73499.99".to_string()),
            "KXBTCD-26MAY3017-T73499.99"
        );
        assert_eq!(safe_ticker("../../evil".to_string()), UNSAFE_TICKER);
        assert_eq!(safe_ticker("a/b".to_string()), UNSAFE_TICKER);
    }

    #[test]
    fn classify_unsafe_ticker_is_quarantined() {
        let mut report = CaptureReport::default();
        // A (hypothetical) hostile market_ticker with a path separator.
        let frame = event(
            r#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"../x","price_dollars":"0.70","delta_fp":"1.00","side":"yes","ts_ms":1}}"#,
        );
        let action = classify(frame, &mut report).expect("written");
        assert_eq!(action.ticker, UNSAFE_TICKER, "traversal ticker quarantined");
    }

    use kdp_core::{MicroDollars, PriceLevel, RestingQty};

    #[test]
    fn verify_envelope_book_outcome_produces_verify_snapshot() {
        let now = recv();
        let outcome = RestOrderbookOutcome::Book {
            ticker: "KXTEST".to_string(),
            yes: vec![PriceLevel {
                price: MicroDollars(500_000),
                quantity: RestingQty(100),
            }],
            no: vec![],
        };
        let (ticker, env) = verify_envelope(outcome, now);
        assert_eq!(ticker, "KXTEST");
        assert_eq!(env.v, kdp_core::ENVELOPE_VERSION);
        assert_eq!(env.seq, None);
        assert_eq!(env.sid, None);
        match env.kind {
            RecordKind::Verify(snapshot) => {
                assert_eq!(snapshot.ticker, Ticker("KXTEST".to_string()));
                assert_eq!(snapshot.ts, now);
                assert_eq!(snapshot.yes.len(), 1);
                assert!(snapshot.no.is_empty());
            }
            other => panic!("expected RecordKind::Verify, got {other:?}"),
        }
    }

    #[test]
    fn verify_envelope_undecodable_outcome_preserves_raw_fallback() {
        let now = recv();
        let payload = serde_json::json!({"ticker": "KXTEST", "orderbook_fp": "bad"});
        let outcome = RestOrderbookOutcome::Undecodable {
            ticker: Some("KXTEST".to_string()),
            error: "missing orderbook_fp".to_string(),
            payload: payload.clone(),
        };
        let (ticker, env) = verify_envelope(outcome, now);
        assert_eq!(ticker, "KXTEST");
        match env.kind {
            RecordKind::Raw(fallback) => {
                assert_eq!(fallback.raw_type.as_deref(), Some("rest_orderbook"));
                assert_eq!(fallback.ticker, Some(Ticker("KXTEST".to_string())));
                assert_eq!(fallback.error, "missing orderbook_fp");
                assert_eq!(fallback.payload, payload);
            }
            other => panic!("expected RecordKind::Raw, got {other:?}"),
        }
    }

    #[test]
    fn verify_envelope_undecodable_without_ticker_routes_to_unrouted() {
        // Reuses the exact same "no ticker known" convention the WS raw path
        // uses (UNROUTED_TICKER = "_unrouted"), not a separate "unknown" stream.
        let now = recv();
        let outcome = RestOrderbookOutcome::Undecodable {
            ticker: None,
            error: "missing ticker".to_string(),
            payload: serde_json::Value::Null,
        };
        let (ticker, env) = verify_envelope(outcome, now);
        assert_eq!(ticker, UNROUTED_TICKER);
        match env.kind {
            RecordKind::Raw(fallback) => assert_eq!(fallback.ticker, None),
            other => panic!("expected RecordKind::Raw, got {other:?}"),
        }
    }

    fn cli(input: &[&str]) -> Args {
        Args::parse(input.iter().map(|s| s.to_string()))
    }

    #[test]
    fn parse_verify_interval_defaults_to_900() {
        assert_eq!(parse_verify_interval(&cli(&["capture"])).unwrap(), 900);
    }

    #[test]
    fn parse_verify_interval_zero_disables() {
        let a = cli(&["capture", "--verify-interval", "0"]);
        assert_eq!(parse_verify_interval(&a).unwrap(), 0);
    }

    #[test]
    fn parse_verify_interval_parses_explicit_value() {
        let a = cli(&["capture", "--verify-interval", "300"]);
        assert_eq!(parse_verify_interval(&a).unwrap(), 300);
    }

    #[test]
    fn parse_verify_interval_rejects_non_numeric() {
        let a = cli(&["capture", "--verify-interval", "abc"]);
        assert!(parse_verify_interval(&a).is_err());
    }

    #[tokio::test]
    async fn writer_task_drains_queued_verify_writes_after_ws_channel_closes() {
        // Regression test for the review finding: the moment `rx` (the WS
        // channel) yields `None` the loop used to `break` immediately, even
        // though `verify_rx` might still hold observations the sweep already
        // fetched (and paid the REST call for). `tokio::select!` randomly
        // picks among ready branches, so pre-fix this test is FLAKY-FAILS
        // (it very rarely happens to drain everything by luck before `rx`
        // wins the race) -- post-fix the trailing drain makes every queued
        // write survive deterministically, regardless of scheduling.
        let base_dir = std::env::temp_dir()
            .join(format!("kdp_cli_writer_drain_{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        let _ = std::fs::remove_dir_all(&base_dir);
        std::fs::create_dir_all(&base_dir).unwrap();

        let (tx, rx) = mpsc::channel::<CaptureEvent>(8);
        let (verify_tx, verify_rx) = mpsc::channel::<VerifyWrite>(64);

        const N: usize = 20;
        for _ in 0..N {
            verify_tx
                .send(VerifyWrite {
                    ticker: "KXTEST".to_string(),
                    env: Envelope::new(
                        recv(),
                        None,
                        None,
                        RecordKind::Verify(OrderbookSnapshot {
                            ticker: Ticker("KXTEST".to_string()),
                            ts: recv(),
                            yes: vec![],
                            no: vec![],
                        }),
                    ),
                })
                .await
                .expect("channel has room");
        }
        drop(verify_tx);
        drop(tx); // the WS session is already over: rx.recv() resolves to None right away

        let report = writer_task(rx, verify_rx, base_dir.clone(), 1)
            .await
            .expect("writer_task ok");

        assert!(report.halt_error.is_none());
        assert_eq!(
            report.verify, N as u64,
            "every already-fetched verify write must be persisted, never dropped, at session end"
        );

        let _ = std::fs::remove_dir_all(&base_dir);
    }

    #[test]
    fn parse_verify_interval_rejects_nonzero_below_window_floor() {
        // The verify engine holds one check at a time and force-resolves it
        // when the next verify arrives; a sweep interval inside the 5s window
        // could turn an in-window check into a false mismatch.
        let a = cli(&["capture", "--verify-interval", "2"]);
        assert!(parse_verify_interval(&a).is_err());
        let ok = cli(&["capture", "--verify-interval", "10"]);
        assert_eq!(parse_verify_interval(&ok).unwrap(), 10);
    }
}
