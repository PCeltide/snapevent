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
use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc;
use tracing::{error, info, instrument, warn};

use kdp_core::{Envelope, RecordKind};
use kdp_kalshi::auth::KalshiCredentials;
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

/// The writer half: drain `rx`, persist each record, periodically guard disk.
///
/// Always returns the [`CaptureReport`] (so the run report prints even on a
/// mid-capture halt). A start-of-run disk-space failure is a hard error (nothing
/// has been captured yet); an in-flight append/disk-full error stops the loop and
/// is recorded in `report.halt_error` rather than discarding the report.
async fn writer_task(
    mut rx: mpsc::Receiver<CaptureEvent>,
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

    while let Some(event) = rx.recv().await {
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

    // Final free-space reading for the report (best effort).
    if let Ok(free) = guard.check() {
        report.free_bytes = Some(free);
    }
    Ok(report)
}

/// Capture `tickers` (L2 + trades) into append-only JSONL under `base_dir` until
/// `shutdown` resolves. The reusable core shared by the `capture` CLI command and
/// the hourly supervisor. Creates `base_dir`, runs the writer + `run_session`
/// pipeline, prints the run report, and returns it (a mid-run halt is surfaced as
/// an error after the report prints). Caller supplies credentials + the shutdown
/// trigger.
#[instrument(skip(creds, shutdown), fields(tickers = tickers.len(), out = %base_dir))]
pub async fn capture_session(
    creds: &KalshiCredentials,
    tickers: &[String],
    base_dir: &str,
    floor_bytes: u64,
    capacity: usize,
    idle: Duration,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> anyhow::Result<CaptureReport> {
    std::fs::create_dir_all(base_dir)
        .with_context(|| format!("creating capture dir {base_dir}"))?;
    let (tx, rx) = mpsc::channel::<CaptureEvent>(capacity);
    let writer = tokio::spawn(writer_task(rx, base_dir.to_string(), floor_bytes));
    let session_result = run_session(
        creds,
        kdp_kalshi::ws::KALSHI_WS_URL,
        tickers,
        tx,
        shutdown,
        idle,
    )
    .await;
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

/// `capture --tickers A,B [--out DIR] [--duration 1h] [--disk-floor-gib N] [--buffer N] [--idle S]`
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

    let creds = KalshiCredentials::from_env()
        .context("loading Kalshi credentials (.env / KDP_KALSHI_PRIVATE_KEY_PATH)")?;

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
        &creds,
        &tickers,
        &base_dir,
        floor_bytes,
        capacity,
        Duration::from_secs(idle_secs),
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
        "records written: {} (incl. {} gap markers, {} raw fallbacks)",
        report.total_written(),
        report.gaps,
        report.raw
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
}
