//! `backfill` — rate-limited, deduplicated REST trade backfill into JSONL.
//!
//! `/markets/trades` is the only history Kalshi exposes (L2 is live-only,
//! ADR-003). This paginates the trade tape per ticker under a token-bucket rate
//! limit (D5: stay ~10 GET/s, well under the Basic read budget), converts each
//! trade into a lossless [`kdp_core::Trade`] envelope (a malformed trade becomes
//! a raw fallback, never dropped), de-duplicates by `trade_id`, and appends to
//! the same `<base>/<ticker>/trade/<date>.jsonl` layout as live capture.
//!
//! Bounded-concurrent backfill across many tickers and a resumable cross-run
//! checkpoint are deferred (open-items); for a handful of IPL tickers a
//! sequential, rate-limited pass is sufficient and simple.

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use anyhow::Context;
use tracing::{info, instrument, warn};

use kdp_core::{Envelope, RawFallback, RecordKind, Ticker, Timestamp};
use kdp_kalshi::ratelimit::RateLimiter;
use kdp_kalshi::rest::{get_historical_trades, get_trades, Market, RestTrade};
use kdp_kalshi::ws::protocol::CHANNEL_TRADE;
use kdp_store::StreamSet;

use crate::args::Args;

/// Convert a REST trade into its on-disk `(channel, Envelope)`: a typed `Trade`
/// when it parses, else a `RawFallback` preserving the original verbatim.
fn trade_to_envelope(rt: &RestTrade, recv_ts: Timestamp) -> (&'static str, Envelope, bool) {
    match rt.to_core() {
        Ok(trade) => (
            CHANNEL_TRADE,
            Envelope::new(recv_ts, None, None, RecordKind::Trade(trade)),
            false,
        ),
        Err(error) => {
            warn!(ticker = %rt.ticker, %error, "malformed backfill trade; preserving as raw");
            // No silent failures: if the verbatim payload itself can't serialize,
            // warn rather than quietly storing a null (which would be
            // indistinguishable from an intentionally empty payload).
            let payload = match serde_json::to_value(rt) {
                Ok(v) => v,
                Err(e) => {
                    warn!(ticker = %rt.ticker, %e, "could not serialize malformed trade; raw payload will be null");
                    serde_json::Value::Null
                }
            };
            let fallback = RawFallback {
                raw_type: Some("trade".to_string()),
                ticker: Some(Ticker(rt.ticker.clone())),
                error,
                payload,
            };
            (
                CHANNEL_TRADE,
                Envelope::new(recv_ts, None, None, RecordKind::Raw(fallback)),
                true,
            )
        }
    }
}

/// Backfill run tally.
#[derive(Debug, Default)]
struct BackfillReport {
    per_ticker: BTreeMap<String, u64>,
    fetched: u64,
    written: u64,
    raw: u64,
    duplicates: u64,
    pages: u64,
}

/// `backfill (--tickers A,B | --series TICKER [--status S]) [--out DIR] [--since 24h] [--rate N]`
///
/// `--series` backfills **every** market in the series (e.g. all IPL games:
/// `backfill --series KXIPLGAME`); `--status` filters them (omit for all
/// statuses, so concluded/`finalized` markets are included).
#[instrument(skip(args))]
pub async fn run_backfill(args: &Args) -> anyhow::Result<()> {
    let base_dir = args.get_or("out", "data").to_string();
    let rate: f64 = args
        .get_or("rate", "8")
        .parse()
        .context("--rate must be a number (requests/sec)")?;
    let min_ts = match args.get("since") {
        Some(s) => Some(now_unix() - parse_duration_secs(s)? as i64),
        None => None,
    };
    let max_pages: u32 = args
        .get_or("pages", "20")
        .parse()
        .context("--pages must be an integer")?;
    // A generous ceiling on per-ticker trade pagination (100k pages x 1000 = 100M
    // trades) — far beyond any real market, so it never truncates a legitimate
    // backfill, but bounds a stuck/looping server cursor instead of running forever.
    let max_trade_pages: u32 = args
        .get_or("trade-pages", "100000")
        .parse()
        .context("--trade-pages must be an integer")?;

    std::fs::create_dir_all(&base_dir).with_context(|| format!("creating dir {base_dir}"))?;
    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building http client")?;

    // `--historical` routes enumeration + trades to the archive tier
    // (`/historical/*`) for markets that settled / trades that filled before the
    // moving ~3-month cutoff, which the live endpoints stop returning.
    let historical = args.get("historical") == Some("true");

    // Resolve the ticker list. Three sources, in precedence order:
    //   --tickers A,B       an explicit list (no enumeration, no metadata)
    //   --markets-file FILE  a prior `markets.jsonl`, windowed by --min/max-close
    //   --series TICKER      enumerate the series (live windowed, or --historical archive)
    let tickers = if args.get("tickers").is_some() {
        crate::parse_ticker_list(args.get("tickers"))?
    } else {
        let mut markets: Vec<Market> = if let Some(path) = args.get("markets-file") {
            // Fetch from a pre-enumerated list, narrowed to this run's close window.
            // Lets a streaming driver enumerate the series once, then backfill
            // chunk-by-chunk without re-paginating the whole archive each chunk.
            let min_close = match args.get("min-close") {
                Some(s) => Some(parse_when(s)?),
                None => None,
            };
            let max_close = match args.get("max-close") {
                Some(s) => Some(parse_when(s)?),
                None => None,
            };
            load_markets_file(std::path::Path::new(path), min_close, max_close)
                .with_context(|| format!("reading markets file {path}"))?
        } else if let Some(series) = args.get("series") {
            if historical {
                // `/historical/markets` has no close-time filter, so page the whole
                // series and bound `close_time` client-side. Bounds default to
                // [epoch, now] = every archived market in the series.
                let min_close = match args.get("min-close") {
                    Some(s) => parse_when(s)?,
                    None => 0,
                };
                let max_close = match args.get("max-close") {
                    Some(s) => parse_when(s)?,
                    None => now_unix(),
                };
                discover_historical_markets(&client, series, min_close, max_close)
                    .await
                    .with_context(|| format!("historical listing of series {series}"))?
            } else {
                // `--min-close` switches on time-windowed enumeration (reach a full
                // date range, chunk-by-chunk); otherwise the default recent discover.
                match args.get("min-close") {
                    Some(mc) => {
                        let min_close = parse_when(mc)?;
                        let max_close = match args.get("max-close") {
                            Some(s) => parse_when(s)?,
                            None => now_unix(),
                        };
                        let chunk = parse_duration_secs(args.get_or("chunk", "7d"))? as i64;
                        discover_markets_windowed(
                            &client,
                            series,
                            args.get("status"),
                            min_close,
                            max_close,
                            chunk,
                        )
                        .await
                        .with_context(|| format!("windowed listing of series {series}"))?
                    }
                    None => kdp_kalshi::rest::discover_markets(
                        &client,
                        "",
                        Some(series),
                        args.get("status"),
                        max_pages,
                    )
                    .await
                    .with_context(|| format!("listing markets in series {series}"))?,
                }
            }
        } else {
            anyhow::bail!(
                "backfill requires --tickers A,B, --series TICKER, or --markets-file FILE"
            );
        };

        // `--min-volume` skips never-traded markets (e.g. far-OTM strikes in a
        // laddered series) before spending a trade-fetch GET on each. Generic.
        let min_volume: f64 = args
            .get_or("min-volume", "0")
            .parse()
            .context("--min-volume must be a number")?;
        if min_volume > 0.0 {
            let before = markets.len();
            markets.retain(|m| m.volume() >= min_volume);
            info!(
                kept = markets.len(),
                dropped = before - markets.len(),
                min_volume,
                "applied --min-volume pre-filter"
            );
        }
        let resolved: Vec<String> = markets
            .iter()
            .map(|m| m.ticker.as_str().to_string())
            .collect();
        if resolved.is_empty() {
            // A windowed/streaming pull legitimately hits empty chunks (e.g. every
            // strike filtered by --min-volume). Treat as a clean no-op so a driver
            // loop isn't aborted; a warn keeps it from being silent.
            warn!(
                historical,
                out = %base_dir,
                "no markets matched (series / markets-file + filters); nothing to backfill"
            );
            return Ok(());
        }
        write_series_metadata(&base_dir, &markets).context("recording series market metadata")?;
        info!(
            count = resolved.len(),
            historical, "backfilling resolved markets"
        );
        resolved
    };

    // `--discover-only`: stop after enumeration + `markets.jsonl` (no trade GETs).
    // A streaming driver runs this once, then backfills each chunk via --markets-file.
    if args.get("discover-only") == Some("true") {
        info!(
            count = tickers.len(),
            out = %base_dir,
            "discover-only: wrote markets.jsonl, skipping trade fetch"
        );
        return Ok(());
    }

    let mut streams = StreamSet::new(&base_dir);
    let mut limiter = RateLimiter::per_second(rate.max(1.0));
    let start = Instant::now();
    let mut report = BackfillReport::default();

    // Resumable checkpoint: skip tickers completed in a prior run, and record each
    // ticker as it finishes so a long (many-hour) backfill survives an interruption.
    let ckpt_path = std::path::Path::new(&base_dir).join(".backfill-progress.jsonl");
    let done: HashSet<String> = if args.get("resume").is_some() {
        load_checkpoint(&ckpt_path)
    } else {
        HashSet::new()
    };
    if !done.is_empty() {
        info!(
            completed = done.len(),
            "resume: skipping already-completed tickers"
        );
    }
    let mut ckpt = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ckpt_path)
        .with_context(|| format!("opening checkpoint {}", ckpt_path.display()))?;

    info!(?tickers, out = %base_dir, rate, ?min_ts, "starting backfill");

    for ticker in &tickers {
        if done.contains(ticker) {
            continue;
        }
        let mut seen: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut pages_this_ticker: u32 = 0;
        loop {
            if pages_this_ticker >= max_trade_pages {
                warn!(%ticker, max_trade_pages, "hit trade-page cap; backfill may be incomplete (stuck cursor?)");
                break;
            }
            pages_this_ticker += 1;
            let wait = limiter.reserve(start.elapsed().as_secs_f64(), 1.0);
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            let page = if historical {
                get_historical_trades(&client, ticker, min_ts, None, 1000, cursor.as_deref()).await
            } else {
                get_trades(&client, ticker, min_ts, None, 1000, cursor.as_deref()).await
            }
            .with_context(|| format!("fetching trades for {ticker}"))?;
            report.pages += 1;

            for rt in &page.trades {
                report.fetched += 1;
                if !seen.insert(rt.trade_id.clone()) {
                    report.duplicates += 1;
                    continue;
                }
                let recv_ts: Timestamp = chrono::Utc::now().into();
                let (channel, envelope, is_raw) = trade_to_envelope(rt, recv_ts);
                streams
                    .append(ticker, channel, &envelope)
                    .with_context(|| format!("appending trade for {ticker}"))?;
                report.written += 1;
                if is_raw {
                    report.raw += 1;
                }
                *report.per_ticker.entry(ticker.clone()).or_default() += 1;
            }

            match page.cursor.filter(|c| !c.is_empty()) {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        // Record completion to the checkpoint, then release this market's file
        // handle so tens of thousands of strikes don't exhaust the FD limit.
        let count = report.per_ticker.get(ticker).copied().unwrap_or(0);
        let line = serde_json::json!({ "ticker": ticker, "trades": count }).to_string();
        {
            use std::io::Write;
            writeln!(ckpt, "{line}").with_context(|| format!("writing checkpoint for {ticker}"))?;
            let _ = ckpt.flush();
        }
        streams.close(ticker, CHANNEL_TRADE);
        info!(%ticker, "ticker backfill complete");
    }

    print_report(&report, &base_dir);
    Ok(())
}

fn print_report(report: &BackfillReport, base_dir: &str) {
    eprintln!("\n=== backfill run report ===");
    eprintln!("output dir:      {base_dir}");
    eprintln!("pages fetched:   {}", report.pages);
    eprintln!("trades fetched:  {}", report.fetched);
    eprintln!(
        "trades written:  {} (incl. {} raw fallbacks)",
        report.written, report.raw
    );
    eprintln!("duplicates:      {}", report.duplicates);
    for (ticker, count) in &report.per_ticker {
        eprintln!("  {ticker}: {count}");
    }
    eprintln!("===========================\n");
}

/// Record the resolved series markets' metadata to `<base_dir>/markets.jsonl`
/// (one JSON object per line) so a per-event trade window — for sports, the game
/// start from `event_ticker` and the determination from `settlement_ts` — stays
/// reconstructible later. Written once per series backfill, archived to Drive with
/// the trades. It is a top-level file (not a `*/` ticker dir), so `kdp-process`
/// (which walks ticker subdirectories) ignores it.
fn write_series_metadata(base_dir: &str, markets: &[Market]) -> anyhow::Result<()> {
    use std::io::Write;
    let path = format!("{base_dir}/markets.jsonl");
    let mut file = std::fs::File::create(&path).with_context(|| format!("creating {path}"))?;
    for m in markets {
        let line = serde_json::to_string(m).context("serializing market metadata")?;
        writeln!(file, "{line}").with_context(|| format!("writing {path}"))?;
    }
    info!(count = markets.len(), %path, "recorded series market metadata");
    Ok(())
}

/// Unix seconds now (wall clock).
fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Split `[min_ts, max_ts)` into ascending half-open windows at most `step`
/// seconds wide. Enumerating a long-running series (e.g. a full year of hourly
/// KXBTCD) in bounded time chunks keeps the `/markets` pagination tractable and
/// lets a checkpoint resume chunk-by-chunk. Empty when the range is empty or
/// `step <= 0`.
fn time_chunks(min_ts: i64, max_ts: i64, step: i64) -> Vec<(i64, i64)> {
    let mut chunks = Vec::new();
    if step <= 0 || min_ts >= max_ts {
        return chunks;
    }
    let mut lo = min_ts;
    while lo < max_ts {
        let hi = (lo + step).min(max_ts);
        chunks.push((lo, hi));
        lo = hi;
    }
    chunks
}

/// Parse a `--min-close`/`--max-close` value to unix seconds: a bare unix
/// timestamp, a `YYYY-MM-DD` date (UTC midnight), or a full rfc3339 datetime.
fn parse_when(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    if let Ok(secs) = s.parse::<i64>() {
        return Ok(secs);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&format!("{s}T00:00:00Z")) {
        return Ok(dt.timestamp());
    }
    anyhow::bail!("--min-close/--max-close {s:?}: use unix secs, YYYY-MM-DD, or rfc3339")
}

/// Load the set of already-completed tickers from a `--resume` checkpoint log
/// (`<out>/.backfill-progress.jsonl`, one JSON object per completed ticker).
/// A missing file yields an empty set (first run); a torn final line — from a
/// crash mid-write — is skipped rather than fatal, so a resume is always safe.
fn load_checkpoint(path: &std::path::Path) -> HashSet<String> {
    let mut done = HashSet::new();
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return done,
    };
    for line in content.lines() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(t) = v.get("ticker").and_then(|t| t.as_str()) {
                done.insert(t.to_string());
            }
        }
    }
    done
}

/// Enumerate every market in `series` with `close_time` in `[min_close, max_close]`,
/// walking the range in `chunk_secs`-wide windows so a long-running series (a full
/// year of hourly markets is hundreds of thousands of strikes) stays tractable.
/// De-dups by ticker across the fuzzy chunk boundaries. Generic over any series.
#[instrument(skip(client))]
async fn discover_markets_windowed(
    client: &reqwest::Client,
    series: &str,
    status: Option<&str>,
    min_close: i64,
    max_close: i64,
    chunk_secs: i64,
) -> anyhow::Result<Vec<Market>> {
    let mut found: Vec<Market> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (lo, hi) in time_chunks(min_close, max_close, chunk_secs) {
        let mut cursor: Option<String> = None;
        loop {
            let (markets, next) = kdp_kalshi::rest::list_markets_page(
                client,
                1000,
                status,
                Some(series),
                Some(lo),
                Some(hi),
                cursor.as_deref(),
            )
            .await
            .with_context(|| format!("listing {series} markets in [{lo}, {hi})"))?;
            for m in markets {
                if seen.insert(m.ticker.as_str().to_string()) {
                    found.push(m);
                }
            }
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
    }
    info!(matches = found.len(), %series, min_close, max_close, "windowed discover complete");
    Ok(found)
}

/// Parse an rfc3339 timestamp to unix seconds, or `None` if unparseable.
fn rfc3339_to_unix(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp())
}

/// Whether a market's `close_time` (rfc3339) falls in the half-open window
/// `[min, max)` (unix seconds). An absent bound leaves that side open. A market
/// with no/unparseable `close_time` is **excluded** whenever any bound is set —
/// it can't be placed in time, so it must not slip silently into a windowed pull.
fn market_close_in_window(close_time: Option<&str>, min: Option<i64>, max: Option<i64>) -> bool {
    if min.is_none() && max.is_none() {
        return true;
    }
    let ts = match close_time.and_then(rfc3339_to_unix) {
        Some(t) => t,
        None => return false,
    };
    if let Some(lo) = min {
        if ts < lo {
            return false;
        }
    }
    if let Some(hi) = max {
        if ts >= hi {
            return false;
        }
    }
    true
}

/// Load markets from a prior `markets.jsonl` (one [`Market`] per line), keeping
/// only those whose `close_time` is in `[min_close, max_close)`. Lets a streaming
/// driver enumerate a series once and then backfill chunk-by-chunk without
/// re-paginating the whole archive for every chunk.
fn load_markets_file(
    path: &std::path::Path,
    min_close: Option<i64>,
    max_close: Option<i64>,
) -> anyhow::Result<Vec<Market>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading markets file {}", path.display()))?;
    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let m: Market = serde_json::from_str(line)
            .with_context(|| format!("parsing market line in {}", path.display()))?;
        if market_close_in_window(m.close_time.as_deref(), min_close, max_close) {
            out.push(m);
        }
    }
    Ok(out)
}

/// Enumerate every **archived** market in `series` (`GET /historical/markets`)
/// with `close_time` in `[min_close, max_close)`. The endpoint takes no time
/// filter, so this pages the full series and bounds client-side, de-duping by
/// ticker. Logs the earliest archived `close_time` seen — the series' true
/// inception, otherwise hidden behind the live cutoff. Generic over any series.
#[instrument(skip(client))]
async fn discover_historical_markets(
    client: &reqwest::Client,
    series: &str,
    min_close: i64,
    max_close: i64,
) -> anyhow::Result<Vec<Market>> {
    let mut found: Vec<Market> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut cursor: Option<String> = None;
    let mut scanned: u64 = 0;
    let mut earliest_close: Option<i64> = None;
    loop {
        let (markets, next) =
            kdp_kalshi::rest::list_historical_markets_page(client, series, 1000, cursor.as_deref())
                .await
                .with_context(|| format!("historical listing of {series}"))?;
        for m in markets {
            scanned += 1;
            if let Some(t) = m.close_time.as_deref().and_then(rfc3339_to_unix) {
                earliest_close = Some(earliest_close.map_or(t, |e| e.min(t)));
            }
            if market_close_in_window(m.close_time.as_deref(), Some(min_close), Some(max_close))
                && seen.insert(m.ticker.as_str().to_string())
            {
                found.push(m);
            }
        }
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    info!(
        matched = found.len(),
        scanned,
        %series,
        min_close,
        max_close,
        earliest_close,
        "historical discover complete"
    );
    Ok(found)
}

/// Parse a duration like `"86400"`, `"90s"`, `"30m"`, `"24h"`, `"7d"` to seconds.
fn parse_duration_secs(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('d') {
        (n, 86_400u64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600)
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
        .with_context(|| format!("invalid --since {s:?} (use e.g. 24h, 7d)"))?;
    value
        .checked_mul(mult)
        .with_context(|| format!("--since {s:?} too large"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdp_core::{MicroDollars, RestingQty};

    fn recv() -> Timestamp {
        "2026-05-30T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .expect("rfc3339")
            .into()
    }

    fn rest_trade(price: &str, count: &str) -> RestTrade {
        let json = format!(
            r#"{{"trade_id":"t1","ticker":"KXTEST","count_fp":"{count}","yes_price_dollars":"{price}","no_price_dollars":"0.5","taker_outcome_side":"yes","taker_book_side":"bid","created_time":"2026-05-30T00:00:00Z"}}"#
        );
        serde_json::from_str(&json).expect("rest trade")
    }

    #[test]
    fn good_trade_becomes_a_trade_envelope() {
        let (channel, env, is_raw) = trade_to_envelope(&rest_trade("0.4100", "4.68"), recv());
        assert_eq!(channel, CHANNEL_TRADE);
        assert!(!is_raw);
        match env.kind {
            RecordKind::Trade(t) => {
                assert_eq!(t.price, MicroDollars(410_000));
                assert_eq!(t.count, RestingQty(468));
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[test]
    fn write_series_metadata_records_markets_as_jsonl() {
        let markets: Vec<kdp_kalshi::rest::Market> = serde_json::from_str(
            r#"[
              {"ticker":"KXIPLGAME-26MAY291000RRGT-RR","event_ticker":"KXIPLGAME-26MAY291000RRGT","settlement_ts":"2026-05-29T18:01:19Z","status":"finalized","result":"no"},
              {"ticker":"KXIPLGAME-26MAY291000RRGT-GT","event_ticker":"KXIPLGAME-26MAY291000RRGT","settlement_ts":"2026-05-29T18:01:19Z","status":"finalized","result":"yes"}
            ]"#,
        )
        .expect("markets");
        let dir = std::env::temp_dir().join("kdp_backfill_meta_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let base = dir.to_str().expect("utf8 path").to_string();

        write_series_metadata(&base, &markets).expect("write metadata");

        let content = std::fs::read_to_string(dir.join("markets.jsonl")).expect("read back");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON object per market");
        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("json line");
        assert_eq!(first["event_ticker"], "KXIPLGAME-26MAY291000RRGT");
        assert_eq!(first["settlement_ts"], "2026-05-29T18:01:19Z");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_trade_becomes_a_raw_fallback_not_dropped() {
        // 7-dp price exceeds MicroDollars scale.
        let (_, env, is_raw) = trade_to_envelope(&rest_trade("0.1234567", "1.00"), recv());
        assert!(is_raw);
        match env.kind {
            RecordKind::Raw(r) => {
                assert_eq!(r.raw_type.as_deref(), Some("trade"));
                assert_eq!(r.payload["yes_price_dollars"], "0.1234567");
            }
            other => panic!("expected raw, got {other:?}"),
        }
    }

    #[test]
    fn parse_duration_secs_handles_units() {
        assert_eq!(parse_duration_secs("86400").unwrap(), 86_400);
        assert_eq!(parse_duration_secs("24h").unwrap(), 86_400);
        assert_eq!(parse_duration_secs("7d").unwrap(), 604_800);
        assert_eq!(parse_duration_secs("30m").unwrap(), 1_800);
        assert!(parse_duration_secs("abc").is_err());
    }

    #[test]
    fn time_chunks_covers_range_in_half_open_windows() {
        assert_eq!(
            time_chunks(0, 100, 30),
            vec![(0, 30), (30, 60), (60, 90), (90, 100)]
        );
        assert_eq!(time_chunks(0, 30, 30), vec![(0, 30)]);
        assert_eq!(
            time_chunks(0, 10, 100),
            vec![(0, 10)],
            "step wider than range"
        );
        assert!(time_chunks(50, 50, 30).is_empty(), "empty range");
        assert!(time_chunks(50, 10, 30).is_empty(), "inverted range");
        assert!(time_chunks(0, 100, 0).is_empty(), "non-positive step");
    }

    #[test]
    fn parse_when_accepts_unix_date_and_rfc3339() {
        assert_eq!(parse_when("1767225600").unwrap(), 1767225600);
        assert_eq!(parse_when("2026-01-01").unwrap(), 1767225600);
        assert_eq!(parse_when("2026-01-01T00:00:00Z").unwrap(), 1767225600);
        assert!(parse_when("not-a-date").is_err());
    }

    #[test]
    fn load_checkpoint_reads_tickers_and_tolerates_torn_lines() {
        let dir = std::env::temp_dir().join("kdp_backfill_ckpt_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join(".backfill-progress.jsonl");

        // missing file -> empty (first run)
        assert!(load_checkpoint(&path).is_empty());

        // two complete lines + a torn final line (as if killed mid-write)
        std::fs::write(
            &path,
            "{\"ticker\":\"A\",\"trades\":5}\n{\"ticker\":\"B\",\"trades\":0}\n{\"ticker\":\"C\",\"tra",
        )
        .expect("write");
        let done = load_checkpoint(&path);
        assert!(done.contains("A") && done.contains("B"));
        assert_eq!(done.len(), 2, "torn final line skipped, not fatal");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rfc3339_to_unix_parses_or_none() {
        assert_eq!(rfc3339_to_unix("2026-01-01T00:00:00Z"), Some(1767225600));
        assert_eq!(rfc3339_to_unix("nope"), None);
    }

    #[test]
    fn market_close_in_window_bounds_are_half_open() {
        let t = Some("2026-03-15T00:00:00Z"); // unix 1773532800
                                              // both bounds: inside
        assert!(market_close_in_window(
            t,
            Some(1767225600),
            Some(1775000000)
        ));
        // below min -> excluded
        assert!(!market_close_in_window(t, Some(1774000000), None));
        // at/after max -> excluded (half-open upper bound)
        assert!(!market_close_in_window(t, None, Some(1773532800)));
        // no bounds -> always in
        assert!(market_close_in_window(t, None, None));
        // missing close_time with a bound set -> excluded, never silently kept
        assert!(!market_close_in_window(None, Some(0), Some(i64::MAX)));
        assert!(market_close_in_window(None, None, None));
    }

    #[test]
    fn load_markets_file_filters_by_close_window() {
        let dir = std::env::temp_dir().join("kdp_markets_file_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("markets.jsonl");
        std::fs::write(
            &path,
            // close 2026-02-01, 2026-03-01, and one with no close_time
            "{\"ticker\":\"A\",\"close_time\":\"2026-02-01T00:00:00Z\"}\n\
             {\"ticker\":\"B\",\"close_time\":\"2026-03-01T00:00:00Z\"}\n\
             {\"ticker\":\"C\"}\n",
        )
        .expect("write");

        // window [2026-02-15, 2026-03-15) keeps only B
        let got = load_markets_file(
            &path,
            Some(rfc3339_to_unix("2026-02-15T00:00:00Z").unwrap()),
            Some(rfc3339_to_unix("2026-03-15T00:00:00Z").unwrap()),
        )
        .expect("load");
        let tickers: Vec<&str> = got.iter().map(|m| m.ticker.as_str()).collect();
        assert_eq!(tickers, vec!["B"], "only B's close falls in window");

        // no bounds -> all with a parseable line load (incl. C, no close_time)
        let all = load_markets_file(&path, None, None).expect("load all");
        assert_eq!(all.len(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
