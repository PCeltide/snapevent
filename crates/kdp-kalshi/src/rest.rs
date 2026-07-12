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

use kdp_core::{BookSide, MicroDollars, PriceLevel, RestingQty, Side, Ticker, Timestamp, Trade};

use crate::auth::KalshiCredentials;
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

/// Same retry/backoff skeleton as [`get_with_retry`], but for **authenticated**
/// GETs: the three `KALSHI-ACCESS-*` headers are (re)computed from `creds` on
/// every attempt, inside the loop, so a retry after backoff never reuses a
/// stale timestamp (Kalshi ties the signature to `ts_ms + method + path`).
///
/// `sign_path` is the URL path only (no query string) — Kalshi signs
/// `ts_ms + "GET" + path`, matching the convention the WS client uses for its
/// upgrade request (`crate::ws::handshake::authenticated_request`, which signs
/// the full path off the connect URL, e.g. `/trade-api/ws/v2`). A signing
/// failure is a typed [`KalshiError`] that propagates immediately — it is not
/// retryable.
async fn get_with_retry_signed(
    client: &reqwest::Client,
    creds: &KalshiCredentials,
    url: &str,
    sign_path: &str,
    query: &[(&str, String)],
) -> Result<reqwest::Response> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let headers = creds.signed_headers("GET", sign_path)?;
        let mut req = client.get(url).query(query);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        match req.send().await {
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
    /// 24h traded volume as a fixed-point string, when present.
    #[serde(default)]
    pub volume_24h_fp: Option<String>,
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

    /// 24h volume parsed to `f64` for ranking (0.0 if absent/unparseable).
    /// Display convenience only — not used for money math.
    pub fn volume_24h(&self) -> f64 {
        self.volume_24h_fp
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

    // total_cmp: a total order even if the venue ever emitted "NaN"/"inf"
    // (which parse::<f64> accepts) -- partial_cmp-to-Equal would hand sort_by
    // a non-total comparator, which panics on detection in current Rust.
    found.sort_by(|a, b| b.volume().total_cmp(&a.volume()));
    info!(matches = found.len(), %query, ?series, ?status, "discover complete");
    Ok(found)
}

/// A series template row from `GET /series` (tolerant; unknown fields ignored).
/// `volume_fp` is present only when the request sets `include_volume=true`.
#[derive(Debug, Deserialize)]
pub struct Series {
    /// Series ticker (e.g. `"KXBTCD"`).
    pub ticker: String,
    /// Human-readable series title.
    pub title: String,
    /// Series category (e.g. `"Crypto"`).
    pub category: String,
    /// Human-readable cadence (e.g. `"hourly"`, `"weekly"`, `"one-off"`).
    pub frequency: String,
    /// Series tags, when present.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// Total contracts traded across all events in the series, fixed-point
    /// string, present only with `include_volume=true`.
    #[serde(default)]
    pub volume_fp: Option<String>,
}

impl Series {
    /// Volume parsed to `f64` for ranking (0.0 if absent/unparseable). Display
    /// convenience only — not used for money math.
    pub fn volume(&self) -> f64 {
        self.volume_fp
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    }
}

/// Envelope returned by `GET /series`.
#[derive(Debug, Deserialize)]
struct SeriesListResponse {
    series: Vec<Series>,
}

/// Build the `/series` query params. Pure (no I/O) so the filter wiring is
/// unit-tested without a live server. `include_volume` is always requested
/// (the ranking needs it); `category` is added only when given.
fn series_query(category: Option<&str>) -> Vec<(&'static str, String)> {
    let mut query: Vec<(&'static str, String)> = vec![("include_volume", "true".to_string())];
    if let Some(c) = category {
        query.push(("category", c.to_string()));
    }
    query
}

/// Fetch the full series template list from the public, **unauthenticated**
/// `GET /series` endpoint, optionally narrowed server-side to `category`.
/// Unlike `/markets` this endpoint is **unpaginated** — the spec defines no
/// cursor, so one request returns the whole list.
#[instrument(skip(client))]
pub async fn get_series_list(
    client: &reqwest::Client,
    category: Option<&str>,
) -> Result<Vec<Series>> {
    let url = format!("{KALSHI_API_BASE}/series");
    let query = series_query(category);

    let resp = get_with_retry(client, &url, &query).await?;
    let body: SeriesListResponse = resp
        .json()
        .await
        .map_err(|e| KalshiError::Decode(format!("series list: {e}")))?;
    info!(count = body.series.len(), ?category, "received series list");
    Ok(body.series)
}

/// Envelope returned by `GET /series/{series_ticker}`.
#[derive(Debug, Deserialize)]
struct SeriesResponse {
    series: Series,
}

/// Fetch one series template by ticker from the public, **unauthenticated**
/// `GET /series/{series_ticker}` endpoint, with `include_volume=true` so the
/// ranking/summary field is populated. A ticker the venue doesn't know is a
/// plain HTTP error (404) surfaced through the retry path's typed error —
/// authoritative, unlike scanning the (possibly non-exhaustive) full list.
#[instrument(skip(client))]
pub async fn get_series(client: &reqwest::Client, ticker: &str) -> Result<Series> {
    let url = format!("{KALSHI_API_BASE}/series/{ticker}");
    let query = series_query(None);

    let resp = get_with_retry(client, &url, &query).await?;
    let body: SeriesResponse = resp
        .json()
        .await
        .map_err(|e| KalshiError::Decode(format!("series {ticker}: {e}")))?;
    Ok(body.series)
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

/// Max tickers per `GET /markets/orderbooks` request (openapi.yaml: `tickers`
/// query param, `maxItems: 100`).
const ORDERBOOK_CHUNK_SIZE: usize = 100;

/// Outcome of decoding one book from the batch orderbook response.
///
/// Shape pinned from `docs/kalshi/openapi.yaml`: `GetMarketOrderbooksResponse`
/// (`orderbooks: [MarketOrderbookFp]`), `MarketOrderbookFp` (`ticker`,
/// `orderbook_fp`), `OrderbookCountFp` (`yes_dollars`/`no_dollars`: arrays of
/// `PriceLevelDollarsCountFp`, a 2-string array `[price_dollars, count_fp]`,
/// e.g. `["0.1500","100.00"]`). The endpoint returns **bids only** on both
/// sides (no asks) — a yes bid at X is equivalent to a no ask at 100-X.
///
/// A bad level or a missing field never aborts the sweep and never silently
/// drops a market: it becomes [`RestOrderbookOutcome::Undecodable`], which the
/// caller is expected to persist as a `RawFallback` record rather than drop.
#[derive(Debug)]
pub enum RestOrderbookOutcome {
    /// Successfully decoded book for one ticker.
    Book {
        /// Market ticker.
        ticker: String,
        /// Yes-side bid levels as returned (best-to-worst).
        yes: Vec<PriceLevel>,
        /// No-side bid levels as returned (best-to-worst).
        no: Vec<PriceLevel>,
    },
    /// A per-book decode failure: bad level string, missing `orderbook_fp`, etc.
    Undecodable {
        /// Ticker, if it could be read from the payload before the failure.
        ticker: Option<String>,
        /// Human-readable decode error.
        error: String,
        /// The raw per-book JSON, preserved so it can be persisted verbatim.
        payload: serde_json::Value,
    },
}

/// Wire shape of `OrderbookCountFp`: two arrays of `[price_dollars, count_fp]`.
#[derive(Debug, Deserialize)]
struct OrderbookCountFpWire {
    #[serde(default)]
    yes_dollars: Vec<[String; 2]>,
    #[serde(default)]
    no_dollars: Vec<[String; 2]>,
}

/// Decode one `[price_dollars, count_fp]` pair into a lossless [`PriceLevel`]
/// via the same integer string surgery the WS decode path uses
/// (`MicroDollars::parse` for the price, `RestingQty::parse` for the count).
fn decode_level(raw: &[String; 2]) -> std::result::Result<PriceLevel, String> {
    let [price_s, count_s] = raw;
    let price = MicroDollars::parse(price_s).map_err(|e| format!("price {price_s:?}: {e}"))?;
    let quantity = RestingQty::parse(count_s).map_err(|e| format!("count {count_s:?}: {e}"))?;
    Ok(PriceLevel { price, quantity })
}

fn decode_levels(raw: &[[String; 2]]) -> std::result::Result<Vec<PriceLevel>, String> {
    raw.iter().map(decode_level).collect()
}

/// Decode one raw `MarketOrderbookFp` JSON value into a [`RestOrderbookOutcome`].
/// `ticker` is threaded in separately (read before this is called) so it can be
/// attached to an `Undecodable` outcome even when the rest of the book fails.
fn decode_book(raw: &serde_json::Value, ticker: Option<String>) -> RestOrderbookOutcome {
    let result = (|| -> std::result::Result<RestOrderbookOutcome, String> {
        let ticker = ticker.clone().ok_or("missing ticker")?;
        let orderbook_fp = raw.get("orderbook_fp").ok_or("missing orderbook_fp")?;
        let wire: OrderbookCountFpWire = serde_json::from_value(orderbook_fp.clone())
            .map_err(|e| format!("orderbook_fp: {e}"))?;
        let yes = decode_levels(&wire.yes_dollars)?;
        let no = decode_levels(&wire.no_dollars)?;
        Ok(RestOrderbookOutcome::Book { ticker, yes, no })
    })();

    match result {
        Ok(outcome) => outcome,
        Err(error) => {
            warn!(?ticker, %error, "undecodable orderbook entry");
            RestOrderbookOutcome::Undecodable {
                ticker,
                error,
                payload: raw.clone(),
            }
        }
    }
}

/// Decode a full `GET /markets/orderbooks` response body into one outcome per
/// requested ticker. Pure (no I/O) so fixture responses are unit-tested
/// without a live server or a mock HTTP server.
fn decode_orderbooks_response(body: &serde_json::Value) -> Vec<RestOrderbookOutcome> {
    body.get("orderbooks")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .map(|raw| {
            let ticker = raw
                .get("ticker")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            decode_book(raw, ticker)
        })
        .collect()
}

/// Split `tickers` into request-sized chunks (`/markets/orderbooks` caps
/// `tickers` at [`ORDERBOOK_CHUNK_SIZE`] per call). Pure so the chunking policy
/// is unit-tested without a live server.
fn chunk_tickers(tickers: &[String], size: usize) -> Vec<&[String]> {
    tickers.chunks(size.max(1)).collect()
}

/// Fetch order books for up to 100 tickers per request (`GET
/// /markets/orderbooks`, authenticated). Empty `tickers` returns immediately
/// with no network call. Larger inputs are chunked at [`ORDERBOOK_CHUNK_SIZE`]
/// and fetched sequentially; a whole-request failure (after retries) propagates
/// as `Err` — the caller is expected to warn and skip that sweep. A per-book
/// decode problem does not fail the request: it surfaces as
/// [`RestOrderbookOutcome::Undecodable`] alongside the successfully decoded
/// books in the same response.
#[instrument(skip(client, creds))]
pub async fn get_orderbooks(
    client: &reqwest::Client,
    creds: &KalshiCredentials,
    tickers: &[String],
) -> Result<Vec<RestOrderbookOutcome>> {
    if tickers.is_empty() {
        return Ok(Vec::new());
    }

    let url = format!("{KALSHI_API_BASE}/markets/orderbooks");
    let sign_path = reqwest::Url::parse(&url)
        .map(|u| u.path().to_string())
        .map_err(|e| KalshiError::Decode(format!("orderbooks url {url:?}: {e}")))?;

    let mut outcomes = Vec::with_capacity(tickers.len());
    for chunk in chunk_tickers(tickers, ORDERBOOK_CHUNK_SIZE) {
        let query: Vec<(&str, String)> = chunk.iter().map(|t| ("tickers", t.clone())).collect();
        let resp = get_with_retry_signed(client, creds, &url, &sign_path, &query).await?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| KalshiError::Decode(format!("orderbooks body: {e}")))?;
        outcomes.extend(decode_orderbooks_response(&body));
    }
    info!(
        tickers = tickers.len(),
        books = outcomes.len(),
        "orderbooks sweep complete"
    );
    Ok(outcomes)
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
    fn series_query_includes_category_only_when_present() {
        let q = series_query(Some("Crypto"));
        assert!(q.contains(&("include_volume", "true".to_string())));
        assert!(q.contains(&("category", "Crypto".to_string())));

        let q_none = series_query(None);
        assert_eq!(q_none, vec![("include_volume", "true".to_string())]);
    }

    #[test]
    fn series_list_fixture_decodes_tolerantly() {
        let json = r#"{
            "series": [
                {
                    "ticker": "KXBTCD",
                    "title": "Bitcoin price",
                    "category": "Crypto",
                    "frequency": "hourly",
                    "tags": ["btc", "crypto"],
                    "volume_fp": "1234.00"
                },
                {
                    "ticker": "KXETHD",
                    "title": "Ethereum price",
                    "category": "Crypto",
                    "frequency": "hourly",
                    "tags": ["eth"],
                    "volume_fp": "500.00"
                },
                {
                    "ticker": "KXIPLGAME",
                    "title": "IPL game winner",
                    "category": "Sports",
                    "frequency": "one-off",
                    "fee_type": "quadratic"
                }
            ]
        }"#;
        let body: SeriesListResponse = serde_json::from_str(json).expect("decode");
        assert_eq!(body.series.len(), 3);

        assert_eq!(body.series[0].volume(), 1234.0);
        assert_eq!(body.series[1].volume(), 500.0);

        let sports = &body.series[2];
        assert_eq!(sports.category, "Sports");
        assert!(sports.tags.is_none());
        assert!(sports.volume_fp.is_none());
        assert_eq!(sports.volume(), 0.0, "absent volume_fp defaults to 0.0");
    }

    #[test]
    fn single_series_response_decodes() {
        let json = r#"{
            "series": {
                "ticker": "KXBTCD",
                "title": "Bitcoin price",
                "category": "Crypto",
                "frequency": "hourly",
                "tags": ["btc"],
                "volume_fp": "42.00"
            }
        }"#;
        let body: SeriesResponse = serde_json::from_str(json).expect("decode");
        assert_eq!(body.series.ticker, "KXBTCD");
        assert_eq!(body.series.volume(), 42.0);
    }

    #[test]
    fn market_volume_24h_parses_and_defaults() {
        let json = r#"{
            "ticker": "KXBTCD-A",
            "title": "Bitcoin at close",
            "status": "open",
            "volume_fp": "100.00",
            "volume_24h_fp": "42.50"
        }"#;
        let m: Market = serde_json::from_str(json).expect("decode");
        assert_eq!(m.volume_24h(), 42.5);

        // Existing-shape fixture without volume_24h_fp still decodes.
        let json_old = r#"{
            "ticker": "KXIPLGAME-26MAY291000RRGT-RR",
            "event_ticker": "KXIPLGAME-26MAY291000RRGT",
            "open_time": "2026-05-27T18:24:00Z",
            "close_time": "2026-05-29T17:56:19Z",
            "settlement_ts": "2026-05-29T18:01:19Z",
            "occurrence_datetime": "2026-05-29T18:00:00Z",
            "status": "finalized",
            "result": "no"
        }"#;
        let m_old: Market = serde_json::from_str(json_old).expect("decode");
        assert_eq!(m_old.volume_24h(), 0.0);
    }

    #[tokio::test]
    #[ignore = "hits the live Kalshi API"]
    async fn live_get_series_list_probe() {
        let client = reqwest::Client::new();
        let series = get_series_list(&client, None)
            .await
            .expect("fetch series list");
        assert!(!series.is_empty(), "expected at least one series");
        for s in &series {
            assert!(!s.ticker.is_empty(), "ticker must not be empty");
            assert!(!s.category.is_empty(), "category must not be empty");
        }
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

    // A throwaway PKCS#8 test key (not a credential) — same one used in auth.rs
    // tests. Only needed to construct a `KalshiCredentials` value; the empty-
    // tickers test never signs anything (no network call is made).
    const TEST_KEY_PKCS8: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCs9d6u5OrN4b4b
zASLghdDeKjmbwLmLWeLPmuc/hP2OT6uvrWxBJQeuGb5uNYOBmQH1LtvVh84qUct
JpZmUNUSRXiKH3rbyQeJBO5SDx0Qn6pXgv4rJPc20PlItfzPAA9VtEN7oWznbhTG
lL/3SzEBjC6BujtVo4goOi6wu0XaXmjje4hxx4mDblygSktZ0GF32CJw4Hos53Ro
vozyGeM76ClpjxjZO7oPn6Ogqg3KU3HQWzv93/RqU8b79g+6T1P3ehjCL6WAOPvB
hobJx8uh1raRJfGnMMRubsJ/epLPfPoxRdAVntkFq6srhtsZZu62E6ktgTPpZ0w8
9zNP9rtBAgMBAAECggEAEnvniOxY9Zi0REHjkx8w1O/uU6C11XCGb3iZGpAtWkkt
huebAOh62zpHcv+gYfj9NA21hu/+v9juDN1iAbGOG85F1AkKNwvUXNMklFYeJo/R
rgQx8ogYTV48OIY5FswsV13qP6pK5QQRf1QX8dncymFh+lDd6h3jErLTw4+3/DP2
c4TwxmPODmOfnTyNxKXJsqcPbGa7HBN6f7ugNCx+KctuJAlR4PyKy4JkVTBpJP7v
RH1J+LwudLbbL7N/LpSlhK3gRfT4X4pBkam2lGPgtkr9eBfJyo5SEULwBviE3ce9
4FIsYi/aRR+NPiUjAjLXpAXwgif+pcFj0EwV6qgH4wKBgQDsJU2CYPiaL0W6NzQ+
irCT4CDs5UYzGnvC+iYC0PWVhVtQ68W+tTxnlnC6fAjhC1qx/fNOxEbBDnGCXHTc
y2GgICMCu32NhyL/Sq6l6huf0lmuuX126sH1avKgGkLTOMVCKyLINlPWHn2lYsap
ySB7tgdc7e+tgjaB2p/HspOK4wKBgQC7gJg4GspAjqJpUoJuTwcbKCtqXp7YnVMz
bntM/guWrW7rI8+CDkU6h8KrYvJqmCnSd0TgElra7sQ4aB3Tdtt4z5hbPgs8j4Is
pFbnazJqJhbu1pUncOttGgAZSQXTOvONyujYeevVQd7O8yq/HuyOkJsfD6lgOmnq
+y3GP1MGiwKBgQDVPH34LG5wdB1noK/Jhd0bOvkgUYyJWvHEx7OJOX15rfkeYjin
E+res0dJ7fTqmiEktud9CdnGPK+dArX4JqMaP8q9jeY65XthweNhKLwXHpAjKZY0
ypmobhF3Jx+OsiXVsTPwTLZ5lADrVf2ElXyCmYWekbCrIfjsWymK3yNB9wKBgFOa
xUTO/TvH3bckqS/SYRLE2Ib3ZdCkZcLbEnOEG1q2PmzubMpK3qd4fV66IelRq+RC
dh2LUaOpLykPk60EpFu8BO06Pvxj6OFK7c0GSVZ3YWZhm+QYP4FIRJ8Bpm1HLe4d
ebF8u6E9W8HfP0I04bm31NMGwrk7kprKIODyv2x9AoGBAMkBEToLzyqGs7StzyPH
CYC2s34wvDedWFmHmjL0pXaM4THHranHWpn+Hp3pxu2Xzc4C92TAwrrIW8bKZ8w1
G44t8znFg+RVrK6kKvil2bM1ISpG+EtC1j9O8I0W5M0ozDKTckZwbF3VFEUvYiaw
FSCdm2zv/hgDDD5k5SdmeEfR
-----END PRIVATE KEY-----
";

    fn test_creds() -> KalshiCredentials {
        KalshiCredentials::from_pem("kid", TEST_KEY_PKCS8).expect("load test key")
    }

    #[test]
    fn decode_orderbooks_response_parses_two_tickers() {
        let body: serde_json::Value = serde_json::from_str(
            r#"{
                "orderbooks": [
                    {
                        "ticker": "KXBTCD-A",
                        "orderbook_fp": {
                            "yes_dollars": [["0.1500", "100.00"]],
                            "no_dollars": [["0.4000", "50.00"]]
                        }
                    },
                    {
                        "ticker": "KXBTCD-B",
                        "orderbook_fp": {
                            "yes_dollars": [],
                            "no_dollars": []
                        }
                    }
                ]
            }"#,
        )
        .expect("fixture parses");

        let outcomes = decode_orderbooks_response(&body);
        assert_eq!(outcomes.len(), 2);

        match &outcomes[0] {
            RestOrderbookOutcome::Book { ticker, yes, no } => {
                assert_eq!(ticker, "KXBTCD-A");
                assert_eq!(
                    yes,
                    &[PriceLevel {
                        price: MicroDollars(150_000),
                        quantity: RestingQty(10_000),
                    }]
                );
                assert_eq!(
                    no,
                    &[PriceLevel {
                        price: MicroDollars(400_000),
                        quantity: RestingQty(5_000),
                    }]
                );
            }
            other => panic!("expected Book, got {other:?}"),
        }

        match &outcomes[1] {
            RestOrderbookOutcome::Book { ticker, yes, no } => {
                assert_eq!(ticker, "KXBTCD-B");
                assert!(yes.is_empty());
                assert!(no.is_empty());
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn decode_orderbooks_response_reports_malformed_level_string() {
        let body: serde_json::Value = serde_json::from_str(
            r#"{
                "orderbooks": [
                    {
                        "ticker": "KXBTCD-BAD",
                        "orderbook_fp": {
                            "yes_dollars": [["abc", "100.00"]],
                            "no_dollars": []
                        }
                    }
                ]
            }"#,
        )
        .expect("fixture parses");

        let outcomes = decode_orderbooks_response(&body);
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            RestOrderbookOutcome::Undecodable {
                ticker,
                error,
                payload,
            } => {
                assert_eq!(ticker.as_deref(), Some("KXBTCD-BAD"));
                assert!(
                    error.contains("abc"),
                    "error should name the bad string: {error}"
                );
                assert_eq!(payload["ticker"], "KXBTCD-BAD");
            }
            other => panic!("expected Undecodable, got {other:?}"),
        }
    }

    #[test]
    fn decode_orderbooks_response_reports_missing_orderbook_fp() {
        let body: serde_json::Value =
            serde_json::from_str(r#"{"orderbooks": [{"ticker": "KXBTCD-NOFP"}]}"#)
                .expect("fixture parses");

        let outcomes = decode_orderbooks_response(&body);
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            RestOrderbookOutcome::Undecodable { ticker, error, .. } => {
                assert_eq!(ticker.as_deref(), Some("KXBTCD-NOFP"));
                assert!(error.contains("orderbook_fp"), "error: {error}");
            }
            other => panic!("expected Undecodable, got {other:?}"),
        }
    }

    #[test]
    fn chunk_tickers_splits_at_the_request_cap() {
        let tickers: Vec<String> = (0..250).map(|i| format!("T{i}")).collect();
        let chunks = chunk_tickers(&tickers, 100);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[1].len(), 100);
        assert_eq!(chunks[2].len(), 50);

        let empty: Vec<String> = Vec::new();
        assert!(chunk_tickers(&empty, 100).is_empty());
    }

    #[tokio::test]
    async fn get_orderbooks_with_empty_tickers_makes_no_request() {
        let client = reqwest::Client::new();
        let creds = test_creds();
        let outcomes = get_orderbooks(&client, &creds, &[])
            .await
            .expect("empty ticker list never errors");
        assert!(outcomes.is_empty());
    }

    /// One-time live probe (never run in CI). Reads real credentials from the
    /// environment, finds 1-2 open KXBTCD markets via the public
    /// `list_markets_page`, and asserts the authenticated batch orderbook fetch
    /// succeeds.
    ///
    /// Signing-path convention confirmed live: the path signed is the request's
    /// **full URL path including the `/trade-api/v2` prefix**
    /// (`/trade-api/v2/markets/orderbooks`), the same convention the WS client
    /// uses for its upgrade request (full path off the connect URL, e.g.
    /// `/trade-api/ws/v2`) — not the bare endpoint path (`/markets/orderbooks`).
    ///
    /// Run from the repo root with the env loaded from `.env`. Cargo runs test
    /// binaries with cwd = the crate directory, not the repo root, so the key
    /// path must be absolute:
    /// ```text
    /// export PATH="$HOME/.cargo/bin:$PATH" && set -a && source .env && set +a \
    ///   && export KDP_KALSHI_PRIVATE_KEY_PATH="$(pwd)/snapeventing.txt" \
    ///   && cargo test -p kdp-kalshi live_get_orderbooks_probe -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "hits the live Kalshi API; requires real credentials in the environment"]
    async fn live_get_orderbooks_probe() {
        let creds = KalshiCredentials::from_env()
            .expect("set KALSHI_API_KEY_ID + KDP_KALSHI_PRIVATE_KEY_PATH to run this probe");
        let client = reqwest::Client::new();

        let (markets, _) =
            list_markets_page(&client, 50, Some("open"), Some("KXBTCD"), None, None, None)
                .await
                .expect("list open KXBTCD markets");
        let tickers: Vec<String> = markets
            .iter()
            .take(2)
            .map(|m| m.ticker.as_str().to_string())
            .collect();
        assert!(
            !tickers.is_empty(),
            "need at least one open KXBTCD market to probe"
        );

        let outcomes = get_orderbooks(&client, &creds, &tickers)
            .await
            .expect("authenticated batch orderbook fetch");
        assert!(!outcomes.is_empty());
        for outcome in &outcomes {
            match outcome {
                RestOrderbookOutcome::Book { ticker, .. } => {
                    println!("probe ok: {ticker} decoded as Book");
                }
                RestOrderbookOutcome::Undecodable { ticker, error, .. } => {
                    panic!("expected Book, got Undecodable for {ticker:?}: {error}");
                }
            }
        }
    }
}
