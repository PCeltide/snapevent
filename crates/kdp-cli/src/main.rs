//! `kdp-cli` — command driver for the Kalshi data pipeline.
//!
//! Commands:
//! - `hello` — initialise tracing and emit a banner event.
//! - `probe` — call the public Kalshi `/markets` endpoint and report the count.
//! - `ws-probe --tickers A,B,C [--frames N] [--idle S]` — authenticate, open the
//!   live WebSocket, subscribe to the orderbook + trade channels, and log frames.
//!   The Phase 2.4 live de-risk for the RSA-PSS handshake + subscribe + receive.
//!
//! `.env` is loaded at startup (via `dotenvy`) so credentials stay out of
//! settings/git. All diagnostics go through `tracing`; the only direct
//! stdout/stderr writes are user-facing CLI usage text.

mod args;
mod backfill;
mod capture;
mod catalog;
mod discover;
mod event_time;
mod hourly;
mod schedule;
mod scheduled;
mod supervisor;
mod universe;

use std::time::Duration;

use anyhow::Context;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use args::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env if present; absence is fine (env vars may already be set).
    let _ = dotenvy::dotenv();
    init_tracing();

    let parsed = Args::parse(std::env::args().skip(1));
    match parsed.command.as_deref() {
        Some("hello") => cmd_hello(),
        Some("probe") => cmd_probe().await?,
        Some("ws-probe") => cmd_ws_probe(&parsed).await?,
        Some("capture") => capture::run_capture(&parsed).await?,
        Some("capture-hourly") => hourly::run_hourly(&parsed).await?,
        Some("capture-scheduled") => scheduled::run_scheduled(&parsed).await?,
        Some("capture-universe") => universe::run_universe(&parsed).await?,
        Some("catalog") => catalog::run_catalog(&parsed).await?,
        Some("discover") => discover::run_discover(&parsed).await?,
        Some("backfill") => backfill::run_backfill(&parsed).await?,
        Some(other) => {
            warn!(command = other, "unknown command");
            print_usage();
            std::process::exit(2);
        }
        None => {
            print_usage();
            std::process::exit(2);
        }
    }
    Ok(())
}

/// Parse a comma-separated `--tickers` value into a non-empty list of tickers.
/// Shared by `ws-probe`, `capture`, and `backfill`.
pub(crate) fn parse_ticker_list(arg: Option<&str>) -> anyhow::Result<Vec<String>> {
    let tickers: Vec<String> = arg
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if tickers.is_empty() {
        anyhow::bail!("requires --tickers TICKER1,TICKER2,...");
    }
    Ok(tickers)
}

/// Initialise the global tracing subscriber.
///
/// Honours `RUST_LOG` when set, otherwise defaults to `info`. Uses `try_init`
/// so a second call (e.g. from tests) is a no-op rather than a panic.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// CLI usage text — user-facing UX, so it goes to stderr directly.
fn print_usage() {
    eprintln!(
        "usage: kdp-cli <command>\n\
         \n\
         commands:\n\
         \x20 hello                                   emit a banner event\n\
         \x20 probe                                   list public markets (count)\n\
         \x20 ws-probe --tickers A,B [--frames N] [--idle S]\n\
         \x20                                         live WS connect + subscribe + log frames\n\
         \x20 capture --tickers A,B [--out DIR] [--duration 1h] [--disk-floor-gib N]\n\
         \x20         [--verify-interval 900]\n\
         \x20                                         capture live L2 + trades to JSONL; every --verify-interval\n\
         \x20                                         seconds (0 disables), cross-check via a REST orderbook sweep\n\
         \x20 capture-hourly [--series KXBTCD] [--band 25] [--out DIR] [--grace 180] [--poll 30]\n\
         \x20                [--verify-interval 900]\n\
         \x20                                         forever: per-hour near-money L2+trade capture ->\n\
         \x20                                         settle -> background archive (process/curate/Drive/prune)\n\
         \x20 capture-scheduled --schedule FILE [--out DIR] [--arm-lead-min 60] [--max-hours 8]\n\
         \x20                   [--grace 180] [--poll 30] [--resolve-grace 1800] [--archive-cmd PATH]\n\
         \x20                   [--verify-interval 900]\n\
         \x20                                         capture pre-scheduled events from a JSONL schedule: arm each\n\
         \x20                                         at start-lead, resolve its markets (predicted ticker or by\n\
         \x20                                         teams), capture L2+trades -> settle -> background archive\n\
         \x20 capture-universe --series A,B,C --name NAME [--status open,unopened] [--min-volume 0]\n\
         \x20                  [--rediscover-interval 300] [--max-units 8] [--out DIR] [--max-hours 8]\n\
         \x20                  [--grace 180] [--poll 30] [--archive-cmd PATH] [--checkpoint-cmd PATH]\n\
         \x20                  [--verify-interval 900] [--arm-lead-min 30] [--clash-sub on]\n\
         \x20                  [--until DATE|RFC3339] [--for DUR]\n\
         \x20                                         declaratively capture EVERY event in the series matching the\n\
         \x20                                         filter: sweep, arm each new event cohort (cap: --max-units,\n\
         \x20                                         loud warn past it), settle -> background archive; re-sweeps\n\
         \x20                                         every --rediscover-interval for newly-listed markets\n\
         \x20 catalog [--category NAME | --series TICKER] [--limit N]\n\
         \x20                                         browse what's on Kalshi, ranked by volume; --series emits a\n\
         \x20                                         ready-to-run capture command\n\
         \x20 discover [--query SUB] [--series TICKER] [--status S] [--pages N] [--limit N]\n\
         \x20                                         find markets (omit --status for all; e.g. --series KXIPLGAME)\n\
         \x20 backfill (--tickers A,B | --series TICKER | --markets-file F) [--out DIR] [--since 24h] [--rate N]\n\
         \x20          [--status S] [--min-close D --max-close D --chunk 7d] [--min-volume N]\n\
         \x20          [--historical] [--discover-only] [--resume]\n\
         \x20                                         backfill REST trade tape to JSONL (--series = all markets in it;\n\
         \x20                                         --min-close enables date-windowed enumeration, --min-volume skips\n\
         \x20                                         never-traded markets, --resume continues from a prior run;\n\
         \x20                                         --historical uses the /historical/* archive tier (settled markets +\n\
         \x20                                         trades older than the ~3-month cutoff); --discover-only writes\n\
         \x20                                         markets.jsonl then stops; --markets-file backfills a pre-enumerated\n\
         \x20                                         list, windowed by --min/max-close, for chunk-by-chunk streaming)"
    );
}

/// `hello` — emit the banner event.
fn cmd_hello() {
    info!("hello from kdp");
}

/// `probe` — hit the public Kalshi `/markets` endpoint and report the count.
async fn cmd_probe() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building http client")?;

    let markets = kdp_kalshi::rest::list_markets(&client, 100)
        .await
        .context("listing kalshi markets")?;

    info!(market_count = markets.len(), "probe succeeded");
    Ok(())
}

/// `ws-probe` — authenticate, open the live WebSocket, subscribe, and log frames.
async fn cmd_ws_probe(args: &Args) -> anyhow::Result<()> {
    let tickers: Vec<String> = args
        .get("tickers")
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if tickers.is_empty() {
        anyhow::bail!("ws-probe requires --tickers TICKER1,TICKER2,...");
    }
    let frames: usize = args
        .get_or("frames", "30")
        .parse()
        .context("--frames must be a non-negative integer")?;
    let idle_secs: u64 = args
        .get_or("idle", "20")
        .parse()
        .context("--idle must be a non-negative integer (seconds)")?;

    let creds = kdp_kalshi::auth::KalshiCredentials::from_env().context(
        "loading Kalshi credentials from environment (.env / KDP_KALSHI_PRIVATE_KEY_PATH)",
    )?;

    let dump_raw = args.get("raw") == Some("true");
    let report = kdp_kalshi::ws::probe(
        &creds,
        &tickers,
        frames,
        Duration::from_secs(idle_secs),
        dump_raw,
    )
    .await
    .context("websocket probe")?;

    info!(
        status = ?report.status,
        frames = report.frames,
        by_type = ?report.by_type,
        "ws-probe finished"
    );
    Ok(())
}
