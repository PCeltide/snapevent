//! `capture-hourly` — the forward continuous KXBTCD hourly L2 + trade supervisor.
//!
//! One long-lived process. Each hour: pre-arm the next hour from the pre-listed
//! `initialized` ladder, select the near-money strike band, capture L2 + trades to
//! a per-hour session dir (reusing `capture_session`), watch for settlement, then
//! spawn the background archive (process -> curate two-sided -> Drive -> prune).
//! Consecutive hours overlap ~1 min at the seam, so coverage is gap-free; a
//! missing hour (maintenance) is logged and waited out. Capture + store only.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::watch;
use tracing::{info, warn};

use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use kdp_kalshi::auth::KalshiCredentials;
use kdp_kalshi::rest::{list_markets_page, Market};

use crate::supervisor::{
    drain_inflight, install_shutdown, interruptible_sleep, reap_inflight, run_capture_unit,
    CaptureUnit, UnitCfg,
};

/// Default band: `0` = capture the whole ladder (curation trims to the two-sided
/// ATM trail). A positive `N` caps capture to the +/-N strikes around the ATM.
const DEFAULT_BAND: usize = 0;

/// A market is the *hourly* product iff its lifetime is ~1 hour (the series also
/// lists multi-day variants). Tolerates clock/rounding slop with a [50,70] window.
fn is_hourly(m: &Market) -> bool {
    match (
        m.open_time
            .as_deref()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok()),
        m.close_time
            .as_deref()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok()),
    ) {
        (Some(o), Some(c)) => {
            let mins = (c - o).num_minutes();
            (50..=70).contains(&mins)
        }
        _ => false,
    }
}

/// The next top-of-hour strictly after `now` (the next open/close boundary).
fn next_boundary(now: DateTime<Utc>) -> DateTime<Utc> {
    let floor = now
        .with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0));
    match floor {
        Some(f) if f > now => f,
        Some(f) => f + ChronoDuration::hours(1),
        None => now, // unreachable for valid times; never panic
    }
}

/// Select the near-money band for one hour's markets: the up-to-`2*band+1`
/// strikes whose `floor_strike` is closest to the at-the-money strike.
///
/// ATM resolution, in priority order:
/// 1. the strike whose own `yes_price_proxy` is nearest $0.50 (best: real prices);
/// 2. else `anchor` -- the at-the-money strike of an adjacent *open* hour (which
///    trades at spot). At pre-arm the about-to-open hour is `initialized` with NO
///    prices, so without this we'd fall straight to (3);
/// 3. else the **median** `floor_strike` -- last resort only.
///
/// (3) is a TRAP for BTC: Kalshi lists a wide ladder NOT centered on spot, so its
/// median can sit thousands of dollars OTM (this caused the go-live mis-capture --
/// see runbook). The `anchor` avoids (3) whenever any hour of the series is open.
/// Markets without a `floor_strike` are ignored. Returns tickers sorted ascending
/// by strike (stable, dedup'd).
fn select_band(markets: &[Market], band: usize, anchor: Option<f64>) -> Vec<String> {
    use std::cmp::Ordering;
    let mut strikes: Vec<(f64, &str)> = markets
        .iter()
        .filter_map(|m| Some((m.floor_strike?, m.ticker.as_str())))
        .collect();
    strikes.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    strikes.dedup_by(|a, b| a.1 == b.1);
    if strikes.is_empty() {
        return Vec::new();
    }

    // band == 0 -> capture the WHOLE ladder (the default for KXBTCD). The two-sided
    // curation keeps the full ATM trail as spot moves and drops the perma-dead deep
    // strikes, so no band-tracking / resubscribe is needed -- the ~188-strike ladder
    // is far wider (~+/-$9k) than BTC moves in an hour. band N>0 caps to +/-N
    // near-money strikes around the ATM (an optional load limiter for huge ladders).
    if band == 0 {
        return strikes.iter().map(|(_, t)| t.to_string()).collect();
    }

    // ATM floor_strike: own price proxy -> adjacent-open-hour anchor -> median.
    let atm_strike = markets
        .iter()
        .filter_map(|m| Some((m.floor_strike?, m.atm_distance()?)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
        .map(|(s, _)| s)
        .or(anchor)
        .unwrap_or_else(|| strikes[strikes.len() / 2].0);

    // Index of the strike closest to atm_strike, then take +/-band around it.
    let center = strikes
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            (a.0 - atm_strike)
                .abs()
                .partial_cmp(&(b.0 - atm_strike).abs())
                .unwrap_or(Ordering::Equal)
        })
        .map(|(i, _)| i)
        .unwrap_or(strikes.len() / 2);
    let lo = center.saturating_sub(band);
    let hi = (center + band + 1).min(strikes.len());
    strikes[lo..hi].iter().map(|(_, t)| t.to_string()).collect()
}

/// A spot-price anchor for centering the next hour's band: the at-the-money
/// `floor_strike` among currently-*open* hourly markets of the series. The
/// adjacent open hour is actively quoted/traded at spot, so its ATM strike is a
/// tight proxy for BTC spot -- and BTC barely moves in the ~30s pre-arm window.
/// Returns `None` if nothing is open or none carry a usable price (then the
/// caller's own-price/median logic stands). Pure read; never persisted.
#[tracing::instrument(skip(client))]
async fn spot_anchor_strike(client: &reqwest::Client, series: &str) -> Option<f64> {
    use std::cmp::Ordering;
    let (markets, _) =
        list_markets_page(client, 1000, Some("open"), Some(series), None, None, None)
            .await
            .ok()?;
    markets
        .iter()
        .filter(|m| is_hourly(m))
        .filter_map(|m| Some((m.floor_strike?, m.atm_distance()?)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal))
        .map(|(s, _)| s)
}

/// Wait for the hour's ladder to be FULLY listed before selecting the band.
///
/// At/just-before open the `initialized` event carries only a partial high-strike
/// ladder; Kalshi adds the near-money strikes in the first ~minute after open, and
/// only then do markets carry live prices. Selecting too early clamps the band to
/// whatever (deep-OTM) strikes happen to exist -- the go-live mis-capture.
///
/// Strategy: re-discover the event's ladder, returning once it is past `floor`
/// seconds beyond `boundary` AND its strike count is stable across two polls (i.e.
/// it stopped growing -> the near-money strikes have all popped up), or once the
/// `cap` is hit. Returns the freshest full ladder (the last non-empty poll).
#[tracing::instrument(skip(client, shutdown_rx))]
async fn await_full_ladder(
    client: &reqwest::Client,
    series: &str,
    boundary: DateTime<Utc>,
    floor: Duration,
    cap: Duration,
    poll: Duration,
    shutdown_rx: &watch::Receiver<bool>,
) -> Vec<Market> {
    let close_lo = boundary.timestamp();
    let close_hi = close_lo + 2 * 3600;
    let floor_at = boundary + ChronoDuration::from_std(floor).unwrap_or_default();
    let cap_at = boundary + ChronoDuration::from_std(cap).unwrap_or_default();
    let mut last: Vec<Market> = Vec::new();
    let mut prev_count = 0usize;
    loop {
        if *shutdown_rx.borrow() {
            return last;
        }
        match list_markets_page(
            client,
            1000,
            None,
            Some(series),
            Some(close_lo),
            Some(close_hi),
            None,
        )
        .await
        {
            Ok((markets, _)) => {
                if let Some((_, hm)) = market_opening_at(&markets, boundary) {
                    last = hm.into_iter().cloned().collect();
                }
            }
            Err(e) => warn!(error = %e, "ladder poll failed; retrying"),
        }
        let count = last.len();
        let now = Utc::now();
        // Ready: past the settle floor AND the ladder stopped growing (stable,
        // non-empty) -- or we hit the cap (proceed with whatever we have).
        if (now >= floor_at && count > 0 && count == prev_count) || now >= cap_at {
            return last;
        }
        prev_count = count;
        if interruptible_sleep(poll, shutdown_rx).await {
            return last;
        }
    }
}

/// Knobs for one hour's capture (defaults mirror `capture` + settlewatch.sh).
/// The capture/settle/archive subset is forwarded to the spine's [`UnitCfg`];
/// `band`/`listing_grace`/`open_settle` are the hourly adapter's own knobs.
#[derive(Clone)]
pub struct HourCfg {
    pub data_dir: String,          // session dir = data_dir/<event_ticker>
    pub band: usize,               // strikes each side of ATM
    pub floor_bytes: u64,          // disk guard floor
    pub capacity: usize,           // channel buffer
    pub idle_secs: u64,            // WS idle timeout
    pub grace_secs: u64,           // keep capturing this long after settlement
    pub poll_secs: u64,            // settlement poll interval
    pub max_hours: u64,            // hard backstop per hour
    pub listing_grace: u64,        // keep retrying discovery this long past the boundary (s)
    pub open_settle: u64, // wait this long past open for the full ladder before selecting (s)
    pub archive_cmd: String, // path to kdp-archive.sh (empty = skip, for dev)
    pub verify_interval_secs: u64, // periodic REST verify-sweep interval (s); 0 disables
}

impl HourCfg {
    /// The capture/settle/archive subset, as the spine's [`UnitCfg`]. The hourly
    /// backstop is expressed in hours; the spine takes seconds.
    fn unit_cfg(&self) -> UnitCfg {
        UnitCfg {
            data_dir: self.data_dir.clone(),
            floor_bytes: self.floor_bytes,
            capacity: self.capacity,
            idle_secs: self.idle_secs,
            grace_secs: self.grace_secs,
            poll_secs: self.poll_secs,
            max_secs: self.max_hours.saturating_mul(3600),
            archive_cmd: self.archive_cmd.clone(),
            checkpoint_cmd: String::new(),
            verify_interval_secs: self.verify_interval_secs,
        }
    }
}

/// Run one hour end-to-end via the shared spine: build a [`CaptureUnit`] from the
/// selected band + close-time window and hand it to [`run_capture_unit`]. The
/// hourly product shares the KXBTCD Drive default (no per-event-set prefix), so
/// `remote_prefix` is `None`.
async fn run_one_hour(
    creds: Arc<KalshiCredentials>,
    client: &reqwest::Client,
    event_ticker: &str,
    tickers: Vec<String>,
    cfg: &HourCfg,
    shutdown_rx: watch::Receiver<bool>,
) {
    let unit = CaptureUnit {
        session: event_ticker.to_string(),
        tickers,
        event: event_ticker.to_string(),
        remote_prefix: None,
    };
    run_capture_unit(creds, client, &unit, &cfg.unit_cfg(), shutdown_rx).await;
}

/// Pick the hourly market that OPENS at `boundary`. Returns the event ticker +
/// that event's markets (the LARGEST ladder, not a partial stub), or None
/// (maintenance / not yet listed). Filters out multi-day variants via `is_hourly`.
fn market_opening_at(
    markets: &[Market],
    boundary: DateTime<Utc>,
) -> Option<(String, Vec<&Market>)> {
    let mut by_event: HashMap<String, Vec<&Market>> = HashMap::new();
    for m in markets.iter().filter(|m| is_hourly(m)) {
        let opens = m
            .open_time
            .as_deref()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok());
        if opens == Some(boundary) {
            if let Some(ev) = m.event_ticker.clone() {
                by_event.entry(ev).or_default().push(m);
            }
        }
    }
    // Largest ladder (the full hour, not a pre-open stub), tie-broken on the event
    // ticker so selection is deterministic.
    by_event
        .into_iter()
        .max_by(|a, b| a.1.len().cmp(&b.1.len()).then_with(|| a.0.cmp(&b.0)))
}

/// Sleep until just after `boundary` (so the next iteration targets the next hour,
/// bounded >=15s against clock skew), or until shutdown. Returns `true` on shutdown.
async fn wait_past_or_shutdown(boundary: DateTime<Utc>, rx: &watch::Receiver<bool>) -> bool {
    let now = Utc::now();
    let d = (boundary + ChronoDuration::seconds(15) - now)
        .to_std()
        .unwrap_or(Duration::from_secs(15))
        .max(Duration::from_secs(15));
    interruptible_sleep(d, rx).await
}

/// Outcome of trying to discover the hour opening at a boundary.
enum Arm {
    /// Found: the event ticker + that hour's markets.
    Hour(String, Vec<Market>),
    /// No market opened within the listing-grace window (maintenance / gap).
    Maintenance,
    /// A shutdown was requested while discovering.
    Shutdown,
}

/// Discover the hourly market opening at `boundary`, retrying (windowed on the
/// hour's close_time) until it appears or `boundary + grace` passes. Kalshi does
/// NOT reliably pre-list the next hour ahead of time -- it can be created
/// just-in-time near the open -- so a single pre-arm query can miss it and falsely
/// skip the hour; retrying catches a late listing, while a genuine gap
/// (maintenance) still resolves to `Maintenance` after the grace window.
async fn discover_opening_hour(
    client: &reqwest::Client,
    series: &str,
    boundary: DateTime<Utc>,
    grace: Duration,
    poll: Duration,
    shutdown_rx: &watch::Receiver<bool>,
) -> Arm {
    // The hour opening at `boundary` closes ~1h later; window on close_time so the
    // ladder is never truncated out of a 1000-row page (the unfiltered listing is
    // dominated by pre-listed `initialized` future hours).
    let close_lo = boundary.timestamp();
    let close_hi = close_lo + 2 * 3600;
    let deadline = boundary + ChronoDuration::from_std(grace).unwrap_or_default();
    loop {
        if *shutdown_rx.borrow() {
            return Arm::Shutdown;
        }
        match list_markets_page(
            client,
            1000,
            None,
            Some(series),
            Some(close_lo),
            Some(close_hi),
            None,
        )
        .await
        {
            Ok((markets, _)) => {
                if let Some((ev, hm)) = market_opening_at(&markets, boundary) {
                    return Arm::Hour(ev, hm.into_iter().cloned().collect());
                }
            }
            Err(e) => warn!(error = %e, "discover failed; retrying"),
        }
        if Utc::now() >= deadline {
            return Arm::Maintenance;
        }
        if interruptible_sleep(poll, shutdown_rx).await {
            return Arm::Shutdown;
        }
    }
}

/// The forever loop. Each boundary: pre-arm + discover the opening hour, select
/// its band, and SPAWN its capture (concurrently with the still-settling previous
/// hour -> gap-free seam). Maintenance (no market) -> log + wait + retry.
pub async fn run_hourly(args: &crate::args::Args) -> anyhow::Result<()> {
    let series = args.get_or("series", "KXBTCD").to_string();
    let cfg = HourCfg {
        data_dir: args.get_or("out", "/var/lib/kdp/data").to_string(),
        band: args.get_or("band", "0").parse().unwrap_or(DEFAULT_BAND),
        floor_bytes: 3 * 1024 * 1024 * 1024,
        capacity: args.get_or("buffer", "8192").parse().unwrap_or(8192),
        idle_secs: args.get_or("idle", "45").parse().unwrap_or(45),
        grace_secs: args.get_or("grace", "180").parse().unwrap_or(180),
        poll_secs: args.get_or("poll", "30").parse().unwrap_or(30),
        max_hours: args.get_or("max-hours", "2").parse().unwrap_or(2),
        listing_grace: args.get_or("listing-grace", "300").parse().unwrap_or(300),
        open_settle: args.get_or("open-settle", "60").parse().unwrap_or(60),
        archive_cmd: args
            .get_or("archive-cmd", "/opt/kdp/bin/kdp-archive.sh")
            .to_string(),
        verify_interval_secs: crate::capture::parse_verify_interval(args)?,
    };
    let pre_arm = Duration::from_secs(args.get_or("pre-arm", "30").parse().unwrap_or(30));
    let creds = Arc::new(KalshiCredentials::from_env().context("loading Kalshi credentials")?);
    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()?;
    info!(%series, band = cfg.band, out = %cfg.data_dir, "starting hourly supervisor");

    // Process-wide shutdown: ctrl-c / `systemctl stop` (SIGINT) flips the watch to
    // true; the loop stops arming new hours and in-flight hours drain + archive.
    let shutdown_rx = install_shutdown();

    let mut inflight: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    while !*shutdown_rx.borrow() {
        let now = Utc::now();
        let boundary = next_boundary(now);
        let until = (boundary - ChronoDuration::from_std(pre_arm).unwrap_or_default() - now)
            .to_std()
            .unwrap_or(Duration::ZERO);
        if !until.is_zero() && interruptible_sleep(until, &shutdown_rx).await {
            break;
        }

        // Discover the hour opening at `boundary`, retrying past it (Kalshi may
        // list the next hour just-in-time rather than ahead) until it appears or
        // the listing-grace window elapses -- then it's a genuine maintenance gap.
        let event_ticker = match discover_opening_hour(
            &client,
            &series,
            boundary,
            Duration::from_secs(cfg.listing_grace),
            Duration::from_secs(cfg.poll_secs),
            &shutdown_rx,
        )
        .await
        {
            // The discovered ladder here is pre-open/partial -- we only keep the
            // event ticker; the full ladder is re-fetched after open below.
            Arm::Hour(ev, _markets) => ev,
            Arm::Maintenance => {
                warn!(%boundary, "no hourly market opening within grace (maintenance/gap); waiting");
                if wait_past_or_shutdown(boundary, &shutdown_rx).await {
                    break;
                }
                continue;
            }
            Arm::Shutdown => break,
        };
        // The pre-open `initialized` ladder is INCOMPLETE -- only deep-OTM high
        // strikes exist; Kalshi lists the near-money strikes (and live prices) in
        // the first ~minute after open. Selecting now would clamp the band to OTM
        // strikes (the go-live mis-capture). So wait for the ladder to fully list
        // (settle floor + stable count), THEN select from real prices.
        if *shutdown_rx.borrow() {
            break;
        }
        let full = await_full_ladder(
            &client,
            &series,
            boundary,
            Duration::from_secs(cfg.open_settle),
            Duration::from_secs(cfg.open_settle + 60),
            Duration::from_secs(10),
            &shutdown_rx,
        )
        .await;
        // Post-open the markets carry their own live prices, so ATM-by-price works
        // directly; the adjacent-open-hour anchor stays as a belt-and-suspenders
        // fallback for the rare price-less case.
        // band 0 captures the whole ladder (no ATM needed); only fetch the spot
        // anchor when capping to a +/-N near-money band.
        let anchor = if cfg.band == 0 {
            None
        } else {
            spot_anchor_strike(&client, &series).await
        };
        let tickers = select_band(&full, cfg.band, anchor);
        if tickers.is_empty() {
            warn!(%event_ticker, "no strikes with floor_strike; skipping hour");
            if wait_past_or_shutdown(boundary, &shutdown_rx).await {
                break;
            }
            continue;
        }
        info!(%event_ticker, strikes = tickers.len(), ladder = full.len(), anchor = ?anchor, "hour armed; capturing");

        reap_inflight(&mut inflight).await;
        if inflight.len() > 3 {
            warn!(
                inflight = inflight.len(),
                "archives backing up; many concurrent hours"
            );
        }
        let creds2 = Arc::clone(&creds);
        let client2 = client.clone();
        let cfg2 = cfg.clone();
        let ev = event_ticker.clone();
        let srx = shutdown_rx.clone();
        inflight.push(tokio::spawn(async move {
            run_one_hour(creds2, &client2, &ev, tickers, &cfg2, srx).await;
        }));
        if wait_past_or_shutdown(boundary, &shutdown_rx).await {
            break;
        }
    }

    // Graceful drain: in-flight hours have been signaled to stop -> each stops
    // capture, writes .done, and spawns its archive. Bound the wait before exit.
    drain_inflight(inflight).await;
    info!("hourly supervisor stopped");
    Ok(())
}

#[cfg(test)]
mod schedule_tests {
    use super::*;

    /// Build a Market with the given open/close; other fields defaulted. Shared by
    /// the band tests too, hence pub(crate).
    pub(crate) fn mk(open: &str, close: &str) -> Market {
        Market {
            ticker: kdp_core::Ticker("KXBTCD-X".into()),
            title: String::new(),
            status: Some("open".into()),
            volume_fp: None,
            close_time: Some(close.into()),
            event_ticker: Some("KXBTCD-26JUN0814".into()),
            open_time: Some(open.into()),
            settlement_ts: None,
            occurrence_datetime: None,
            result: None,
            floor_strike: None,
            yes_bid_dollars: None,
            yes_ask_dollars: None,
            last_price_dollars: None,
            volume_24h_fp: None,
        }
    }

    #[test]
    fn is_hourly_accepts_one_hour_and_rejects_multiday() {
        assert!(is_hourly(&mk(
            "2026-06-08T17:00:00Z",
            "2026-06-08T18:00:00Z"
        )));
        assert!(!is_hourly(&mk(
            "2026-06-07T20:00:00Z",
            "2026-06-08T21:00:00Z"
        )));
    }

    #[test]
    fn next_boundary_rounds_up_to_the_hour() {
        let now: DateTime<Utc> = "2026-06-08T17:42:10Z".parse().unwrap();
        assert_eq!(next_boundary(now).to_rfc3339(), "2026-06-08T18:00:00+00:00");
        let on: DateTime<Utc> = "2026-06-08T18:00:00Z".parse().unwrap();
        assert_eq!(next_boundary(on).to_rfc3339(), "2026-06-08T19:00:00+00:00");
    }
}

#[cfg(test)]
mod band_tests {
    use super::schedule_tests::mk;
    use super::*;

    /// Ladder of `n` strikes spaced $100 from `base`, with a price gradient that
    /// puts the ATM (price~=0.50) at `atm_idx`.
    fn ladder(n: usize, base: f64, atm_idx: usize) -> Vec<Market> {
        (0..n)
            .map(|i| {
                let strike = base + (i as f64) * 100.0;
                let p = (0.5 + (atm_idx as f64 - i as f64) * 0.05).clamp(0.01, 0.99);
                let mut m = mk("2026-06-08T17:00:00Z", "2026-06-08T18:00:00Z");
                m.floor_strike = Some(strike);
                m.last_price_dollars = Some(format!("{p:.2}"));
                m.ticker = kdp_core::Ticker(format!("KXBTCD-26JUN0814-T{strike}"));
                m
            })
            .collect()
    }

    #[test]
    fn band_centers_on_atm_and_is_symmetric_in_the_interior() {
        let ms = ladder(100, 60000.0, 50);
        let got = select_band(&ms, 25, None);
        assert_eq!(got.len(), 51, "2*25+1 in the interior");
        assert_eq!(got.first().unwrap(), "KXBTCD-26JUN0814-T62500");
        assert_eq!(got.last().unwrap(), "KXBTCD-26JUN0814-T67500");
    }

    #[test]
    fn band_zero_captures_all_strikes() {
        // band 0 = the whole ladder (capture-all; curation trims later). Independent
        // of ATM/anchor -- every strike is returned, sorted ascending.
        let ms = ladder(100, 60000.0, 50);
        let got = select_band(&ms, 0, None);
        assert_eq!(got.len(), 100, "band 0 returns the whole ladder, uncapped");
        assert_eq!(got.first().unwrap(), "KXBTCD-26JUN0814-T60000");
        assert_eq!(got.last().unwrap(), "KXBTCD-26JUN0814-T69900");
    }

    #[test]
    fn own_price_beats_anchor_when_present() {
        // Real prices put ATM at idx 50 ($65000); a stale anchor must NOT override.
        let ms = ladder(100, 60000.0, 50);
        let got = select_band(&ms, 25, Some(99000.0));
        assert_eq!(got.first().unwrap(), "KXBTCD-26JUN0814-T62500");
        assert_eq!(got.last().unwrap(), "KXBTCD-26JUN0814-T67500");
    }

    #[test]
    fn band_clamps_at_the_low_edge() {
        let ms = ladder(100, 60000.0, 3);
        let got = select_band(&ms, 25, None);
        assert_eq!(got.first().unwrap(), "KXBTCD-26JUN0814-T60000");
        assert!(got.len() < 51, "clamped, not wrapped");
    }

    #[test]
    fn cold_open_with_no_prices_falls_back_to_median() {
        let mut ms = ladder(11, 60000.0, 5);
        for m in &mut ms {
            m.last_price_dollars = None;
            m.yes_bid_dollars = None;
            m.yes_ask_dollars = None;
        }
        let got = select_band(&ms, 2, None);
        assert_eq!(
            got,
            vec![
                "KXBTCD-26JUN0814-T60300",
                "KXBTCD-26JUN0814-T60400",
                "KXBTCD-26JUN0814-T60500",
                "KXBTCD-26JUN0814-T60600",
                "KXBTCD-26JUN0814-T60700",
            ]
        );
    }

    #[test]
    fn cold_open_uses_anchor_not_median() {
        // The go-live bug: price-less ladder + no anchor -> median (idx 50, $65000),
        // which is far OTM. With a spot anchor at $61000 (idx 10) we must center
        // there instead -- this is the exact regression that recorded $7k-OTM strikes.
        let mut ms = ladder(101, 60000.0, 50);
        for m in &mut ms {
            m.last_price_dollars = None;
            m.yes_bid_dollars = None;
            m.yes_ask_dollars = None;
        }
        let got = select_band(&ms, 2, Some(61000.0));
        assert_eq!(
            got,
            vec![
                "KXBTCD-26JUN0814-T60800",
                "KXBTCD-26JUN0814-T60900",
                "KXBTCD-26JUN0814-T61000",
                "KXBTCD-26JUN0814-T61100",
                "KXBTCD-26JUN0814-T61200",
            ],
            "anchor must override the median fallback"
        );
    }
}
