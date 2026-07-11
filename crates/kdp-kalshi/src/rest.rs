//! REST client for Kalshi's public HTTP API: market discovery + trade backfill.
//!
//! All endpoints used here are **public / no-auth**: `/markets` + `/markets/trades`
//! (the live tier) and their `/historical/markets` + `/historical/trades`
//! counterparts (the archive tier — settled markets / filled trades older than the
//! moving ~3-month historical cutoff, which the live endpoints stop returning; see
//! `GET /historical/cutoff`). L2 history exists solely on the live WS feed (ADR-003).
//! Responses are cursor-paginated. Trade fixed-point fields are parsed into the
//! lossless [`kdp_core`] units; a malformed trade is surfaced (the caller emits a
//! raw fallback), never silently dropped. Every GET is retried on transient
//! transport/`429`/`5xx` failures (idempotent reads) so a long unattended backfill
//! survives a dropped connection rather than aborting.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, warn};

use kdp_core::{BookSide, MicroDollars, RestingQty, Side, Ticker, Timestamp, Trade};

use crate::{KalshiError, Result, KALSHI_API_BASE};

/// Max total attempts (initial + retries) for a transient GET.
const MAX_GET_ATTEMPTS: u32 = 6;

/// Whether an HTTP status warrants a retry: `429` (rate limited) or any `5xx`
/// (server-side). Pure so the policy is unit-tested without a live server.
fn status_is_retryable(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}

/// Exponential backoff for retry attempt `n` (1-based), capped at 30s:
/// 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
fn retry_backoff(attempt: u32) -> std::time::Duration {
    let secs = 2u64.saturating_pow(attempt.saturating_sub(1)).min(30);
    std::time::Duration::from_secs(secs)
}

/// Issue a GET with bounded retry on transient failures: connection
/// reset/timeout/connect errors (e.g. Windows `os error 10054` mid-request) and
/// retryable HTTP statuses (`429` + `5xx`). For **idempotent reads only**.
/// A permanent status (`4xx` other than `429`) and the final attempt's failure
/// propagate unchanged — no silent success, and each retry is a `warn!`.
async fn get_with_retry(
    client: &reqwest::Client,
    url: &str,
    query: &[(&str, String)],
) -> Result<reqwest::Response> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match client.get(url).query(query).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return Ok(resp);
                }
                if status_is_retryable(status.as_u16()) && attempt < MAX_GET_ATTEMPTS {
                    let backoff = retry_backoff(attempt);
                    warn!(%url, status = status.as_u16(), attempt, backoff_s = backoff.as_secs(),
                          "retryable HTTP status; backing off");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                // Non-retryable status, or out of attempts: surface the HTTP error.
                resp.error_for_status()?;
                unreachable!("error_for_status returns Err for a non-success status");
            }
            Err(e) => {
                // Transport-level failure (reset/timeout/connect): retry the GET.
                if attempt < MAX_GET_ATTEMPTS {
                    let backoff = retry_backoff(attempt);
                    warn!(%url, error = %e, attempt, backoff_s = backoff.as_secs(),
                          "transient transport error; retrying GET");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(e.into());
            }
        }
    }
}

/// Subset of a Kalshi market object we decode (list + discover).
///
/// Kalshi returns many more fields; `serde` ignores the ones we don't name. The
/// timing fields below are what a series backfill records to `markets.jsonl` so a
/// per-event trade window stays reconstructible (e.g. for sports: game start from
/// `event_ticker`, determination from `settlement_ts`). `Serialize` is derived so
/// the resolved markets can be persisted verbatim alongside the trades.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Market {
    /// Unique market ticker.
    pub ticker: Ticker,
    /// Human-readable market title (absent on some responses).
    #[serde(default)]
    pub title: String,
    /// Market status (e.g. `"active"`), when present.
    #[serde(default)]
    pub status: Option<String>,
    /// Total volume as a fixed-point string (`"8.00"`), when present.
    #[serde(default)]
    pub volume_fp: Option<String>,
    /// Market close time (rfc3339), when present — trading stops (≈ event end for sports).
    #[serde(default)]
    pub close_time: Option<String>,
    /// Event ticker. For sports it encodes the date + scheduled start time
    /// (e.g. `KXIPLGAME-26MAY291000RRGT` → 2026-05-29, 10:00 ET start) — the only
    /// place the start appears, so it is how a trade window's lower bound is derived.
    #[serde(default)]
    pub event_ticker: Option<String>,
    /// When the market opened to trading (rfc3339) — often days before the event.
    #[serde(default)]
    pub open_time: Option<String>,
    /// Settlement / determination time (rfc3339) — a trade window's upper bound.
    #[serde(default)]
    pub settlement_ts: Option<String>,
    /// Nominal occurrence / expected-expiration time (rfc3339). For sports this is
    /// the expected END of the event, not the start.
    #[serde(default)]
    pub occurrence_datetime: Option<String>,
    /// Settled outcome (`"yes"`/`"no"`), once finalized.
    #[serde(default)]
    pub result: Option<String>,
    /// Numeric strike (dollars) — the lower bound the market settles above.
    #[serde(default)]
    pub floor_strike: Option<f64>,
    /// Best yes bid as a fixed-point dollar string (e.g. "0.58"), when present.
    #[serde(default)]
    pub yes_bid_dollars: Option<String>,
    /// Best yes ask as a fixed-point dollar string (e.g. "0.59"), when present.
    #[serde(default)]
    pub yes_ask_dollars: Option<String>,
    /// Last trade price as a fixed-point dollar string, when present.
    #[serde(default)]
    pub last_price_dollars: Option<String>,
}

impl Market {
    /// Volume parsed to `f64` for ranking (0.0 if absent/unparseable). Display
    /// convenience only — not used for money math.
    pub fn volume(&self) -> f64 {
        self.volume_fp
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    }

    /// A yes-price proxy in [0,1] for locating the at-the-money strike: the
    /// bid/ask mid when both are present, else the last trade price, else None.
    /// Parsed as `f64` for *ranking only* — never persisted, never money math.
    pub fn yes_price_proxy(&self) -> Option<f64> {
        let parse = |s: &Option<String>| s.as_deref().and_then(|v| v.parse::<f64>().ok());
        match (parse(&self.yes_bid_dollars), parse(&self.yes_ask_dollars)) {
            (Some(b), Some(a)) => Some((b + a) / 2.0),
            _ => parse(&self.last_price_dollars),
        }
    }

    /// |yes_price_proxy - 0.50| - smaller is closer to the money. None if no price.
    pub fn atm_distance(&self) -> Option<f64> {
        self.yes_price_proxy().map(|p| (p - 0.5).abs())
    }
}

/// Envelope returned by `GET /markets`.
#[derive(Debug, Deserialize)]
struct MarketsResponse {
    #[serde(default)]
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

/// Fetch a single page of markets from the public `/markets` endpoint.
///
/// Hits Kalshi's **unauthenticated** market-listing endpoint and returns the
/// decoded markets. `limit` bounds the page size (Kalshi caps it server-side).
/// Used by `kdp-cli probe` to validate the HTTP + JSON + wire path.
#[instrument(skip(client))]
pub async fn list_markets(client: &reqwest::Client, limit: u32) -> Result<Vec<Market>> {
    let url = format!("{KALSHI_API_BASE}/markets");
    debug!(%url, limit, "requesting markets page");

    let resp = get_with_retry(client, &url, &[("limit", limit.to_string())]).await?;

    let body: MarketsResponse = resp.json().await?;
    info!(
        count = body.markets.len(),
        has_cursor = body.cursor.is_some(),
        "received markets page"
    );
    Ok(body.markets)
}

/// Build the `/markets` query params. Pure (no I/O) so the filter wiring is
/// unit-tested without a live server. `min_close_ts`/`max_close_ts` are unix
/// seconds bounding the market `close_time` — the key to enumerating a
/// long-running series chunk-by-chunk (a full year of hourly markets is far too
/// many for a single cursor walk).
fn markets_query(
    limit: u32,
    status: Option<&str>,
    series_ticker: Option<&str>,
    min_close_ts: Option<i64>,
    max_close_ts: Option<i64>,
    cursor: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut query: Vec<(&'static str, String)> = vec![("limit", limit.to_string())];
    if let Some(s) = status {
        query.push(("status", s.to_string()));
    }
    if let Some(s) = series_ticker {
        query.push(("series_ticker", s.to_string()));
    }
    if let Some(t) = min_close_ts {
        query.push(("min_close_ts", t.to_string()));
    }
    if let Some(t) = max_close_ts {
        query.push(("max_close_ts", t.to_string()));
    }
    if let Some(c) = cursor {
        query.push(("cursor", c.to_string()));
    }
    query
}

/// Fetch one page of markets with optional `status`/`series_ticker` filters, a
/// `[min_close_ts, max_close_ts]` close-time window (unix seconds), and a
/// pagination `cursor`. Returns the page plus the next cursor (empty/None when
/// exhausted).
#[instrument(skip(client))]
pub async fn list_markets_page(
    client: &reqwest::Client,
    limit: u32,
    status: Option<&str>,
    series_ticker: Option<&str>,
    min_close_ts: Option<i64>,
    max_close_ts: Option<i64>,
    cursor: Option<&str>,
) -> Result<(Vec<Market>, Option<String>)> {
    let url = format!("{KALSHI_API_BASE}/markets");
    let query = markets_query(
        limit,
        status,
        series_ticker,
        min_close_ts,
        max_close_ts,
        cursor,
    );

    let resp = get_with_retry(client, &url, &query).await?;
    let body: MarketsResponse = resp.json().await?;
    let next = body.cursor.filter(|c| !c.is_empty());
    Ok((body.markets, next))
}

/// Fetch one page of **historical** markets (`GET /historical/markets`) for a
/// series — settled markets older than the cutoff, which `GET /markets` no longer
/// returns. The endpoint's filters are mutually exclusive and it takes **no**
/// close-time filter, so callers page the whole series and bound `close_time`
/// client-side. Returns the page plus the next cursor (`None` when exhausted).
#[instrument(skip(client))]
pub async fn list_historical_markets_page(
    client: &reqwest::Client,
    series_ticker: &str,
    limit: u32,
    cursor: Option<&str>,
) -> Result<(Vec<Market>, Option<String>)> {
    let url = format!("{KALSHI_API_BASE}/historical/markets");
    let mut query: Vec<(&str, String)> = vec![
        ("limit", limit.to_string()),
        ("series_ticker", series_ticker.to_string()),
    ];
    if let Some(c) = cursor {
        query.push(("cursor", c.to_string()));
    }
    let resp = get_with_retry(client, &url, &query).await?;
    let body: MarketsResponse = resp.json().await?;
    let next = body.cursor.filter(|c| !c.is_empty());
    Ok((body.markets, next))
}

/// Discover markets matching `query` (case-insensitive substring of the ticker
/// or title; empty matches all), optionally narrowed server-side to a
/// `series_ticker` and/or `status` (`None` = all statuses, important for finding
/// concluded markets — Kalshi marks them `finalized`/`settled`, not `open`),
/// paginating up to `max_pages` pages and sorting by volume.
///
/// `series` is important: the unfiltered open-markets list is dominated by tens
/// of thousands of multivariate combo markets, so a substring scan rarely
/// reaches a specific single market within a few pages. Narrowing by series
/// (e.g. `KXIPLGAME`) bypasses the combos and finds every market in the series.
#[instrument(skip(client))]
pub async fn discover_markets(
    client: &reqwest::Client,
    query: &str,
    series: Option<&str>,
    status: Option<&str>,
    max_pages: u32,
) -> Result<Vec<Market>> {
    let needle = query.to_lowercase();
    let mut found = Vec::new();
    let mut cursor: Option<String> = None;

    for _ in 0..max_pages {
        let (markets, next) =
            list_markets_page(client, 1000, status, series, None, None, cursor.as_deref()).await?;
        for m in markets {
            if needle.is_empty()
                || m.ticker.as_str().to_lowercase().contains(&needle)
                || m.title.to_lowercase().contains(&needle)
            {
                found.push(m);
            }
        }
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    found.sort_by(|a, b| {
        b.volume()
            .partial_cmp(&a.volume())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    info!(matches = found.len(), %query, ?series, ?status, "discover complete");
    Ok(found)
}

/// A trade as returned by `GET /markets/trades` (raw fixed-point strings).
///
/// `Serialize` is derived so a malformed trade can be preserved verbatim in a
/// `RawFallback` payload.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RestTrade {
    /// Kalshi trade id.
    pub trade_id: String,
    /// Market ticker.
    pub ticker: String,
    /// Contract count, fixed-point string (`"4.68"`).
    pub count_fp: String,
    /// Yes execution price, decimal-dollar string.
    pub yes_price_dollars: String,
    /// No execution price, decimal-dollar string.
    #[serde(default)]
    pub no_price_dollars: String,
    /// Outcome side the taker took (`"yes"`/`"no"`).
    pub taker_outcome_side: String,
    /// Book side the taker hit (`"bid"`/`"ask"`), when present.
    #[serde(default)]
    pub taker_book_side: Option<String>,
    /// Execution time, rfc3339.
    pub created_time: String,
}

impl RestTrade {
    /// Convert to a lossless [`kdp_core::Trade`]. Returns an error string (not a
    /// panic, not a silent zero) on any malformed field, so the caller can emit a
    /// raw fallback instead of dropping the trade.
    pub fn to_core(&self) -> std::result::Result<Trade, String> {
        let price = MicroDollars::parse(&self.yes_price_dollars)
            .map_err(|e| format!("yes_price_dollars {:?}: {e}", self.yes_price_dollars))?;
        let count = RestingQty::parse(&self.count_fp)
            .map_err(|e| format!("count_fp {:?}: {e}", self.count_fp))?;
        let taker_side = match self.taker_outcome_side.as_str() {
            "yes" => Side::Yes,
            "no" => Side::No,
            other => return Err(format!("invalid taker_outcome_side {other:?}")),
        };
        let taker_book_side = match self.taker_book_side.as_deref() {
            None => None,
            Some("bid") => Some(BookSide::Bid),
            Some("ask") => Some(BookSide::Ask),
            Some(other) => return Err(format!("invalid taker_book_side {other:?}")),
        };
        let ts = self
            .created_time
            .parse::<DateTime<Utc>>()
            .map(Timestamp)
            .map_err(|e| format!("created_time {:?}: {e}", self.created_time))?;
        Ok(Trade {
            ticker: Ticker(self.ticker.clone()),
            ts,
            price,
            count,
            taker_side,
            taker_book_side,
            trade_id: Some(self.trade_id.clone()),
        })
    }
}

/// One page of `GET /markets/trades`.
#[derive(Debug, Deserialize)]
pub struct TradesPage {
    /// Trades in this page.
    #[serde(default)]
    pub trades: Vec<RestTrade>,
    /// Next-page cursor (empty/None when exhausted).
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Fetch one page of trades for `ticker` from `url` (`/markets/trades` or
/// `/historical/trades` — identical request + `Trade` shape, different tier).
/// `min_ts`/`max_ts` are unix **seconds**; `limit` ≤ 1000.
async fn fetch_trades_page(
    client: &reqwest::Client,
    url: &str,
    ticker: &str,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    limit: u32,
    cursor: Option<&str>,
) -> Result<TradesPage> {
    let mut query: Vec<(&str, String)> =
        vec![("ticker", ticker.to_string()), ("limit", limit.to_string())];
    if let Some(m) = min_ts {
        query.push(("min_ts", m.to_string()));
    }
    if let Some(m) = max_ts {
        query.push(("max_ts", m.to_string()));
    }
    if let Some(c) = cursor {
        query.push(("cursor", c.to_string()));
    }

    let resp = get_with_retry(client, url, &query).await?;
    let page: TradesPage = resp
        .json()
        .await
        .map_err(|e| KalshiError::Decode(format!("trades page: {e}")))?;
    Ok(page)
}

/// Fetch one page of trades for `ticker` from the public **live** `/markets/trades`
/// endpoint. `min_ts`/`max_ts` are unix **seconds**; `limit` ≤ 1000.
#[instrument(skip(client))]
pub async fn get_trades(
    client: &reqwest::Client,
    ticker: &str,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    limit: u32,
    cursor: Option<&str>,
) -> Result<TradesPage> {
    let url = format!("{KALSHI_API_BASE}/markets/trades");
    fetch_trades_page(client, &url, ticker, min_ts, max_ts, limit, cursor).await
}

/// Fetch one page of trades for `ticker` from the **archive** `/historical/trades`
/// endpoint — trades filled before the historical cutoff, which `/markets/trades`
/// no longer returns. Same request + `Trade` shape as [`get_trades`].
#[instrument(skip(client))]
pub async fn get_historical_trades(
    client: &reqwest::Client,
    ticker: &str,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    limit: u32,
    cursor: Option<&str>,
) -> Result<TradesPage> {
    let url = format!("{KALSHI_API_BASE}/historical/trades");
    fetch_trades_page(client, &url, ticker, min_ts, max_ts, limit, cursor).await
}

/// The live/historical boundary timestamps from `GET /historical/cutoff`. Records
/// older than the relevant cutoff are served only by the `/historical/*` tier.
/// All three are rfc3339; the window advances forward over time (~3-month target).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Cutoff {
    /// Markets that settled before this must use `/historical/markets`.
    pub market_settled_ts: String,
    /// Trades filled before this must use `/historical/trades`.
    pub trades_created_ts: String,
    /// Orders canceled/executed before this must use `/historical/orders`.
    pub orders_updated_ts: String,
}

/// Fetch the current live/historical cutoff timestamps (`GET /historical/cutoff`).
/// Lets a backfill log — or route by — the boundary it is straddling.
#[instrument(skip(client))]
pub async fn get_cutoff(client: &reqwest::Client) -> Result<Cutoff> {
    let url = format!("{KALSHI_API_BASE}/historical/cutoff");
    let resp = get_with_retry(client, &url, &[]).await?;
    let cutoff: Cutoff = resp
        .json()
        .await
        .map_err(|e| KalshiError::Decode(format!("cutoff: {e}")))?;
    Ok(cutoff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_is_retryable_covers_429_and_5xx_only() {
        assert!(status_is_retryable(429), "rate limit");
        assert!(status_is_retryable(500));
        assert!(status_is_retryable(503));
        assert!(status_is_retryable(599));
        assert!(!status_is_retryable(200));
        assert!(!status_is_retryable(400));
        assert!(!status_is_retryable(404), "not found is permanent");
        assert!(!status_is_retryable(401));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        assert_eq!(retry_backoff(1).as_secs(), 1);
        assert_eq!(retry_backoff(2).as_secs(), 2);
        assert_eq!(retry_backoff(3).as_secs(), 4);
        assert_eq!(retry_backoff(4).as_secs(), 8);
        assert_eq!(retry_backoff(5).as_secs(), 16);
        assert_eq!(retry_backoff(6).as_secs(), 30, "capped at 30s");
        assert_eq!(retry_backoff(20).as_secs(), 30, "stays capped, no overflow");
    }

    #[test]
    fn markets_query_includes_present_filters_only() {
        let q = markets_query(
            1000,
            Some("settled"),
            Some("KXBTCD"),
            Some(100),
            Some(200),
            Some("cur"),
        );
        assert!(q.contains(&("limit", "1000".to_string())));
        assert!(q.contains(&("status", "settled".to_string())));
        assert!(q.contains(&("series_ticker", "KXBTCD".to_string())));
        assert!(q.contains(&("min_close_ts", "100".to_string())));
        assert!(q.contains(&("max_close_ts", "200".to_string())));
        assert!(q.contains(&("cursor", "cur".to_string())));
        // omitted filters produce no params
        assert_eq!(
            markets_query(500, None, None, None, None, None),
            vec![("limit", "500".to_string())]
        );
    }

    #[test]
    fn rest_trade_to_core_parses_confirmed_live_shape() {
        // The exact shape returned by GET /markets/trades (verified live).
        let json = r#"{
            "trade_id":"e511db2f-8bec-72d9-6627-7612ab974881",
            "ticker":"KXBTCD-26MAY3017-T73499.99",
            "count_fp":"4.68",
            "yes_price_dollars":"0.4100",
            "no_price_dollars":"0.5900",
            "taker_outcome_side":"yes",
            "taker_side":"yes",
            "taker_book_side":"bid",
            "created_time":"2026-05-30T00:05:19.147391Z"
        }"#;
        let rt: RestTrade = serde_json::from_str(json).expect("deserialize");
        let trade = rt.to_core().expect("to_core");
        assert_eq!(trade.ticker.as_str(), "KXBTCD-26MAY3017-T73499.99");
        assert_eq!(trade.price, MicroDollars(410_000)); // 0.4100
        assert_eq!(trade.count, RestingQty(468)); // 4.68 contracts
        assert_eq!(trade.taker_side, Side::Yes);
        assert_eq!(trade.taker_book_side, Some(BookSide::Bid));
        assert_eq!(
            trade.trade_id.as_deref(),
            Some("e511db2f-8bec-72d9-6627-7612ab974881")
        );
    }

    #[test]
    fn rest_trade_to_core_reports_malformed_price() {
        let json = r#"{
            "trade_id":"x","ticker":"K","count_fp":"1.00",
            "yes_price_dollars":"0.1234567","no_price_dollars":"0.5",
            "taker_outcome_side":"yes","taker_book_side":"bid",
            "created_time":"2026-05-30T00:00:00Z"
        }"#;
        let rt: RestTrade = serde_json::from_str(json).expect("deserialize");
        assert!(
            rt.to_core().is_err(),
            "over-precise price must error, not drop"
        );
    }

    #[test]
    fn market_decodes_strike_selection_fields() {
        let json = r#"{
            "ticker":"KXBTCD-26JUN0814-T63599.99",
            "event_ticker":"KXBTCD-26JUN0814",
            "status":"open",
            "open_time":"2026-06-08T17:00:00Z",
            "close_time":"2026-06-08T18:00:00Z",
            "floor_strike":63599.99,
            "yes_bid_dollars":"0.58",
            "yes_ask_dollars":"0.59",
            "last_price_dollars":"0.60",
            "volume_fp":"69114.48"
        }"#;
        let m: Market = serde_json::from_str(json).expect("decode");
        assert_eq!(m.floor_strike, Some(63599.99));
        assert_eq!(m.yes_bid_dollars.as_deref(), Some("0.58"));
        assert_eq!(m.last_price_dollars.as_deref(), Some("0.60"));
        // ATM proxy: yes mid is derivable from the bid/ask strings.
        let d = m.atm_distance().expect("has price");
        assert!(
            (d - 0.085).abs() < 1e-9,
            "atm_distance ~= |0.585 - 0.5|, got {d}"
        );
    }

    #[test]
    fn market_records_window_relevant_metadata() {
        // The fields needed to reconstruct a per-game trade window later:
        // event_ticker (encodes the scheduled start) + settlement_ts (determination).
        let json = r#"{"ticker":"KXIPLGAME-26MAY291000RRGT-RR","event_ticker":"KXIPLGAME-26MAY291000RRGT","open_time":"2026-05-27T18:24:00Z","close_time":"2026-05-29T17:56:19Z","settlement_ts":"2026-05-29T18:01:19Z","occurrence_datetime":"2026-05-29T18:00:00Z","status":"finalized","result":"no"}"#;
        let m: Market = serde_json::from_str(json).expect("deserialize");
        assert_eq!(m.event_ticker.as_deref(), Some("KXIPLGAME-26MAY291000RRGT"));
        assert_eq!(m.settlement_ts.as_deref(), Some("2026-05-29T18:01:19Z"));
        assert_eq!(m.result.as_deref(), Some("no"));
        // Round-trips (Serialize) so it can be persisted alongside the trades.
        let round = serde_json::to_string(&m).expect("serialize");
        assert!(round.contains(r#""settlement_ts":"2026-05-29T18:01:19Z""#));
    }
}
