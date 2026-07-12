//! `capture-scheduled` — capture pre-scheduled one-off events from a schedule file.
//!
//! The second supervisor over the shared spine ([`crate::supervisor`]). Where
//! `capture-hourly` arms on every top-of-hour and resolves a KXBTCD strike band,
//! this arms on each [`ScheduleEntry`]'s `start_utc - arm_lead` and resolves an
//! event's markets by a **hybrid** rule: try the predicted event ticker, else
//! discover by series + close-time window + team codes (order-agnostic, so the
//! `AAABBB` vs `BBBAAA` ticker-order uncertainty doesn't matter). It then hands a
//! [`CaptureUnit`] to [`run_capture_unit`] — the same capture/settle/archive path
//! the hourly supervisor uses. Overlapping matches run as concurrent tasks.
//!
//! A schedule file is series-agnostic, so the same binary captures the Women's
//! T20 World Cup today and a football World Cup tomorrow — only the file changes.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::sync::watch;
use tracing::{info, warn};

use kdp_kalshi::auth::KalshiCredentials;
use kdp_kalshi::rest::{list_markets_page, Market};

use crate::schedule::{load_schedule, ScheduleEntry};
use crate::supervisor::{
    install_shutdown, interruptible_sleep, park_until_shutdown, reap_inflight, run_capture_unit,
    CaptureUnit, UnitCfg,
};

/// Defaults (overridable per-entry in the schedule, or via CLI flags).
const DEFAULT_ARM_LEAD_MIN: i64 = 60;
const DEFAULT_MAX_HOURS: u64 = 8;

/// Tunables for the scheduler that are not per-unit (those live in [`UnitCfg`]).
struct SchedCfg {
    data_dir: String,
    floor_bytes: u64,
    capacity: usize,
    idle_secs: u64,
    grace_secs: u64,
    poll_secs: u64,
    arm_lead_min: i64,
    default_max_hours: u64,
    /// How long past `arm_at` to keep retrying resolution before giving up (s).
    resolve_grace: u64,
    archive_cmd: String,
    verify_interval_secs: u64,
}

/// Match the markets belonging to a scheduled entry, returning the tickers to
/// capture (both sides of the match). Pure — the hybrid resolution rule:
///
/// 1. If the entry has a predicted `event_ticker` and markets carry it, capture
///    every market under that event.
/// 2. Else match by team codes: markets whose `event_ticker` (or `ticker`)
///    contains **all** of the entry's team codes, case-insensitively. Order-
///    agnostic, so `…AAABBB` and `…BBBAAA` both match `["AAA","BBB"]`.
///
/// Returns tickers sorted + de-duplicated for determinism. Empty => not found yet
/// (the caller retries within the resolve grace, then skips).
fn match_event_markets(entry: &ScheduleEntry, markets: &[Market]) -> Vec<String> {
    // (1) predicted event ticker.
    if let Some(ev) = entry.event_ticker.as_deref() {
        let mut hit: Vec<String> = markets
            .iter()
            .filter(|m| m.event_ticker.as_deref() == Some(ev))
            .map(|m| m.ticker.as_str().to_string())
            .collect();
        if !hit.is_empty() {
            hit.sort();
            hit.dedup();
            return hit;
        }
    }
    // (2) team-code match (order-agnostic, case-insensitive). Needs >=1 code.
    if entry.teams.is_empty() {
        return Vec::new();
    }
    let codes: Vec<String> = entry.teams.iter().map(|t| t.to_uppercase()).collect();
    let mut hit: Vec<String> = markets
        .iter()
        .filter(|m| {
            let hay = format!(
                "{} {}",
                m.event_ticker.as_deref().unwrap_or(""),
                m.ticker.as_str()
            )
            .to_uppercase();
            codes.iter().all(|c| hay.contains(c.as_str()))
        })
        .map(|m| m.ticker.as_str().to_string())
        .collect();
    hit.sort();
    hit.dedup();
    hit
}

/// The `[min_close_ts, max_close_ts]` close-time window (unix seconds) to bound
/// the discovery query for an event starting at `start_utc` with a `max_hours`
/// backstop.
///
/// **Why the upper bound is days, not hours:** Kalshi sets these match markets'
/// `close_time` to a *generic late hard-close* — observed at exactly +2 days past
/// the real `expected_expiration_time` (e.g. a match settling 2026-06-12T21:30Z
/// carries `close_time` 2026-06-14T21:30Z). The discovery query filters on
/// `close_time`, so a window anchored to `start + max_hours` (~hours) would
/// systematically EXCLUDE every such market and the event would never resolve.
/// We widen `win_hi` to cover the hard-close convention plus slack; widening only
/// risks pulling a few extra rows (the team-code / predicted-ticker match then
/// narrows it), never a miss.
fn discovery_window(start_utc: DateTime<Utc>, max_hours: u64) -> (i64, i64) {
    let win_lo = (start_utc - ChronoDuration::hours(2)).timestamp();
    // start + backstop, then +4 days to clear Kalshi's ~+2-day hard-close on
    // close_time with generous margin for late/overrunning settlements.
    let win_hi =
        (start_utc + ChronoDuration::hours(max_hours as i64) + ChronoDuration::days(4)).timestamp();
    (win_lo, win_hi)
}

/// Resolve one entry's markets, retrying within `resolve_grace` (Kalshi may list
/// the event late, or it may already be open). Returns the [`CaptureUnit`] to
/// capture, or `None` if nothing matched in the grace window (skip the entry) or
/// shutdown fired.
async fn resolve_unit(
    client: &reqwest::Client,
    entry: &ScheduleEntry,
    cfg: &SchedCfg,
    shutdown_rx: &watch::Receiver<bool>,
) -> Option<CaptureUnit> {
    let max_hours = entry.max_hours.unwrap_or(cfg.default_max_hours);
    let (win_lo, win_hi) = discovery_window(entry.start_utc, max_hours);
    let deadline = Utc::now() + ChronoDuration::seconds(cfg.resolve_grace as i64);

    loop {
        if *shutdown_rx.borrow() {
            return None;
        }
        match list_markets_page(
            client,
            1000,
            None,
            Some(&entry.series),
            Some(win_lo),
            Some(win_hi),
            None,
        )
        .await
        {
            Ok((markets, _)) => {
                let tickers = match_event_markets(entry, &markets);
                if !tickers.is_empty() {
                    let session = session_name(entry, &tickers);
                    info!(
                        id = %entry.id, label = %entry.label, markets = tickers.len(),
                        %session, "resolved scheduled event; capturing"
                    );
                    return Some(CaptureUnit {
                        session,
                        tickers,
                        series: entry.series.clone(),
                        remote_prefix: entry.remote_prefix.clone(),
                    });
                }
            }
            Err(e) => warn!(id = %entry.id, error = %e, "resolve poll failed; retrying"),
        }
        if Utc::now() >= deadline {
            warn!(id = %entry.id, label = %entry.label, "event not found within resolve grace; skipping");
            return None;
        }
        if interruptible_sleep(Duration::from_secs(cfg.poll_secs), shutdown_rx).await {
            return None;
        }
    }
}

/// The on-disk + Drive session name for an entry: prefer the predicted event
/// ticker, else derive it from the **matched tickers** (authoritative — these are
/// exactly what we subscribe to), else the entry id.
///
/// CRITICAL: derive from the *matched* tickers, NOT the raw discovery page. The
/// discovery window deliberately spans +4 days (to clear Kalshi's +2-day
/// hard-close, see [`discovery_window`]), so the page can contain OTHER events
/// that share a team code — e.g. resolving Scotland-vs-WI (26JUN18..DDDEEE) also
/// returns Scotland-vs-England (26JUN20..DDDBBB). Naming the session from the
/// first market in that page mislabels the folder with the wrong event (and risks
/// clobbering the real one's archive). The matched tickers are already narrowed to
/// this match, so strip the side suffix (`…DDDEEE-DDD` -> `…DDDEEE`) to recover
/// the event ticker the markets actually belong to.
fn session_name(entry: &ScheduleEntry, tickers: &[String]) -> String {
    if let Some(ev) = entry.event_ticker.clone() {
        return ev;
    }
    // A market ticker is `<EVENT_TICKER>-<SIDE>`; the event is everything before
    // the last `-`. All matched tickers share one event, so the first suffices.
    tickers
        .first()
        .and_then(|t| t.rsplit_once('-').map(|(ev, _side)| ev.to_string()))
        .unwrap_or_else(|| entry.id.clone())
}

/// `capture-scheduled --schedule FILE [--out DIR] [--arm-lead-min N]
///  [--max-hours N] [--grace S] [--poll S] [--idle S] [--buffer N]
///  [--resolve-grace S] [--archive-cmd PATH]`
pub async fn run_scheduled(args: &crate::args::Args) -> anyhow::Result<()> {
    let schedule_path = args
        .get("schedule")
        .context("--schedule FILE is required (path to the JSONL schedule)")?;
    let cfg = SchedCfg {
        data_dir: args.get_or("out", "/var/lib/kdp/data").to_string(),
        floor_bytes: 3 * 1024 * 1024 * 1024,
        capacity: args.get_or("buffer", "8192").parse().unwrap_or(8192),
        idle_secs: args.get_or("idle", "45").parse().unwrap_or(45),
        grace_secs: args.get_or("grace", "180").parse().unwrap_or(180),
        poll_secs: args.get_or("poll", "30").parse().unwrap_or(30),
        arm_lead_min: args
            .get_or("arm-lead-min", "")
            .parse()
            .unwrap_or(DEFAULT_ARM_LEAD_MIN),
        default_max_hours: args
            .get_or("max-hours", "")
            .parse()
            .unwrap_or(DEFAULT_MAX_HOURS),
        resolve_grace: args.get_or("resolve-grace", "1800").parse().unwrap_or(1800),
        archive_cmd: args
            .get_or("archive-cmd", "/opt/kdp/bin/kdp-archive.sh")
            .to_string(),
        verify_interval_secs: crate::capture::parse_verify_interval(args)?,
    };

    let entries = load_schedule(Path::new(schedule_path))?;
    let creds = Arc::new(KalshiCredentials::from_env().context("loading Kalshi credentials")?);
    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()?;
    info!(
        schedule = %schedule_path,
        entries = entries.len(),
        out = %cfg.data_dir,
        "starting scheduled supervisor"
    );

    let shutdown_rx = install_shutdown();
    let mut inflight: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Order by arm time; skip entries already over (restart-resilient) or already
    // archived. We arm each in turn, spawning its capture (overlaps run together).
    let mut pending = arm_order(entries, &cfg);
    for entry in pending.drain(..) {
        if *shutdown_rx.borrow() {
            break;
        }
        if already_archived(&cfg.data_dir, &entry) {
            info!(id = %entry.id, "entry already archived; skipping");
            continue;
        }

        // Sleep until this entry's arm time (start - lead), interruptible.
        let arm_at = entry.arm_at(cfg.arm_lead_min);
        let until = (arm_at - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        if !until.is_zero() {
            info!(id = %entry.id, label = %entry.label, arm_at = %arm_at.to_rfc3339(), "waiting to arm");
            if interruptible_sleep(until, &shutdown_rx).await {
                break;
            }
        }

        reap_inflight(&mut inflight).await;
        if inflight.len() > 6 {
            warn!(
                inflight = inflight.len(),
                "many concurrent captures + archives"
            );
        }

        let Some(unit) = resolve_unit(&client, &entry, &cfg, &shutdown_rx).await else {
            continue; // not found within grace, or shutdown
        };
        // Post-resolution archived-guard. The pre-resolution already_archived()
        // only knows the PREDICTED ticker, so a team-discovered entry (event_ticker
        // null) whose window has not yet elapsed gets re-armed on every restart.
        // Now that resolution has given us the real session name, skip if it is
        // already archived -- a restart must not re-capture a finished match (the
        // settled market yields 0 records and the archive then skips on the marker,
        // so it is harmless, but noisy and alarming). See session_name().
        if Path::new(&cfg.data_dir)
            .join(&unit.session)
            .join(".archived")
            .exists()
        {
            info!(id = %entry.id, session = %unit.session, "session already archived; skipping re-capture");
            continue;
        }
        let unit_cfg = UnitCfg {
            data_dir: cfg.data_dir.clone(),
            floor_bytes: cfg.floor_bytes,
            capacity: cfg.capacity,
            idle_secs: cfg.idle_secs,
            grace_secs: cfg.grace_secs,
            poll_secs: cfg.poll_secs,
            max_secs: entry.max_hours.unwrap_or(cfg.default_max_hours) * 3600,
            archive_cmd: cfg.archive_cmd.clone(),
            verify_interval_secs: cfg.verify_interval_secs,
        };
        let creds2 = Arc::clone(&creds);
        let client2 = client.clone();
        let srx = shutdown_rx.clone();
        inflight.push(tokio::spawn(async move {
            run_capture_unit(creds2, &client2, &unit, &unit_cfg, srx).await;
        }));
    }

    // No more entries to arm, but in-flight captures may still be LIVE (their
    // matches not yet settled). Park until an explicit shutdown -- NEVER self-exit
    // here: returning while a match captures (or merely because the schedule is
    // exhausted) lets systemd's Restart=always relaunch us and re-arm in-window
    // matches, reconnecting their feeds every cycle -- a restart loop that
    // fragments the capture. park_until_shutdown waits (reaping finished captures)
    // and only the shutdown path bounds the drain + exits. (2026-06-28 post-mortem.)
    park_until_shutdown(inflight, &shutdown_rx).await;
    info!("scheduled supervisor stopped");
    Ok(())
}

/// Drop entries whose capture window is already fully past (`start + max_hours <
/// now`) — a restart mid-tournament re-reads the file and re-arms only entries
/// still ahead or in-window — then sort by arm time.
fn arm_order(mut entries: Vec<ScheduleEntry>, cfg: &SchedCfg) -> Vec<ScheduleEntry> {
    let now = Utc::now();
    entries.retain(|e| {
        let max_h = e.max_hours.unwrap_or(cfg.default_max_hours) as i64;
        let window_end = e.start_utc + ChronoDuration::hours(max_h);
        let keep = window_end > now;
        if !keep {
            info!(id = %e.id, "entry window already past; skipping");
        }
        keep
    });
    entries.sort_by_key(|e| e.arm_at(cfg.arm_lead_min));
    entries
}

/// True iff this entry's session already has an `.archived` marker (so a restart
/// doesn't re-capture a finished event). Checks the predicted-ticker dir; the
/// team-discovered dir name isn't known until resolution, so this is a
/// best-effort skip for the common (confirmed-ticker) case.
fn already_archived(data_dir: &str, entry: &ScheduleEntry) -> bool {
    let Some(ev) = entry.event_ticker.as_deref() else {
        return false;
    };
    Path::new(data_dir).join(ev).join(".archived").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(ticker: &str, event: &str, close: Option<&str>) -> Market {
        Market {
            ticker: kdp_core::Ticker(ticker.into()),
            title: String::new(),
            status: Some("open".into()),
            volume_fp: None,
            close_time: close.map(|s| s.into()),
            event_ticker: Some(event.into()),
            open_time: None,
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

    fn entry(ev: Option<&str>, teams: &[&str]) -> ScheduleEntry {
        ScheduleEntry {
            id: "t".into(),
            label: String::new(),
            series: "KXCUPMATCH".into(),
            event_ticker: ev.map(|s| s.into()),
            teams: teams.iter().map(|s| s.to_string()).collect(),
            start_utc: "2026-06-12T13:30:00Z".parse().unwrap(),
            arm_lead_min: None,
            max_hours: None,
            remote_prefix: None,
        }
    }

    #[test]
    fn discovery_window_covers_kalshis_two_day_hard_close() {
        // Regression: Kalshi sets these match markets' close_time to a generic
        // late hard-close — observed +2 days past the real expiration. The
        // discovery query filters on close_time, so the window MUST contain that
        // far-future close or the event never resolves. (Found in the first scheduled
        // server dry-run: start 13:30 play, but close_time was +2 days.)
        let start: DateTime<Utc> = "2026-06-12T17:30:00Z".parse().unwrap();
        let (lo, hi) = discovery_window(start, 8);
        // close_time = expected_expiration (~21:30Z same day) + 2 days.
        let close: DateTime<Utc> = "2026-06-14T21:30:00Z".parse().unwrap();
        assert!(
            close.timestamp() >= lo && close.timestamp() <= hi,
            "Kalshi's +2-day hard-close {close} must fall inside the discovery window [{lo}, {hi}]",
        );
        // Sanity: the lower bound still precedes the match start.
        assert!(lo < start.timestamp());
    }

    #[test]
    fn predicted_ticker_captures_all_sides_of_the_event() {
        let ms = vec![
            mk(
                "KXCUPMATCH-26JUN121330AAABBB-AAA",
                "KXCUPMATCH-26JUN121330AAABBB",
                None,
            ),
            mk(
                "KXCUPMATCH-26JUN121330AAABBB-BBB",
                "KXCUPMATCH-26JUN121330AAABBB",
                None,
            ),
            mk(
                "KXCUPMATCH-26JUN130530CCCDDD-CCC",
                "KXCUPMATCH-26JUN130530CCCDDD",
                None,
            ),
        ];
        let got = match_event_markets(
            &entry(Some("KXCUPMATCH-26JUN121330AAABBB"), &["AAA", "BBB"]),
            &ms,
        );
        assert_eq!(
            got,
            vec![
                "KXCUPMATCH-26JUN121330AAABBB-AAA",
                "KXCUPMATCH-26JUN121330AAABBB-BBB",
            ],
            "both sides of the predicted event, sorted; the other match excluded"
        );
    }

    #[test]
    fn team_match_is_order_agnostic_when_predicted_ticker_absent() {
        // The real event ticker uses BBBAAA; our schedule guessed AAABBB order.
        let ms = vec![
            mk(
                "KXCUPMATCH-26JUN121330BBBAAA-AAA",
                "KXCUPMATCH-26JUN121330BBBAAA",
                None,
            ),
            mk(
                "KXCUPMATCH-26JUN121330BBBAAA-BBB",
                "KXCUPMATCH-26JUN121330BBBAAA",
                None,
            ),
        ];
        // event_ticker None => fall to team match; codes present in either order.
        let got = match_event_markets(&entry(None, &["AAA", "BBB"]), &ms);
        assert_eq!(got.len(), 2, "matched despite the team-order mismatch");
    }

    #[test]
    fn predicted_miss_falls_back_to_team_match() {
        // Predicted AAABBB, but the live event is BBBAAA => (1) misses, (2) catches.
        let ms = vec![
            mk(
                "KXCUPMATCH-26JUN121330BBBAAA-AAA",
                "KXCUPMATCH-26JUN121330BBBAAA",
                None,
            ),
            mk(
                "KXCUPMATCH-26JUN121330BBBAAA-BBB",
                "KXCUPMATCH-26JUN121330BBBAAA",
                None,
            ),
        ];
        let got = match_event_markets(
            &entry(Some("KXCUPMATCH-26JUN121330AAABBB"), &["AAA", "BBB"]),
            &ms,
        );
        assert_eq!(
            got.len(),
            2,
            "hybrid fallback recovers the order-swapped event"
        );
    }

    #[test]
    fn no_match_returns_empty() {
        let ms = vec![mk(
            "KXCUPMATCH-26JUN130530CCCDDD-CCC",
            "KXCUPMATCH-26JUN130530CCCDDD",
            None,
        )];
        let got = match_event_markets(&entry(None, &["AAA", "BBB"]), &ms);
        assert!(got.is_empty());
    }

    #[test]
    fn session_name_prefers_predicted_then_resolved_then_id() {
        let tk = vec!["KXCUPMATCH-26JUN121330BBBAAA-BBB".to_string()];
        // predicted present -> use it.
        assert_eq!(
            session_name(&entry(Some("PRED"), &["AAA", "BBB"]), &tk),
            "PRED"
        );
        // predicted absent -> event ticker stripped from the matched ticker.
        assert_eq!(
            session_name(&entry(None, &["AAA", "BBB"]), &tk),
            "KXCUPMATCH-26JUN121330BBBAAA"
        );
        // predicted absent + no matched tickers -> entry id.
        assert_eq!(session_name(&entry(None, &["AAA", "BBB"]), &[]), "t");
    }

    #[test]
    fn session_name_uses_matched_tickers_not_the_raw_page() {
        // Regression for the m12 mislabel (2026-06-18): resolving Scotland-vs-WI
        // (event 26JUN181330DDDEEE) by team code also pulls the later
        // Scotland-vs-England event (26JUN201330DDDBBB) into the +4-day discovery
        // page. Naming from the first PAGE market grabbed DDDBBB and mislabeled the
        // folder. Naming from the MATCHED tickers must yield the DDDEEE event.
        let page = vec![
            mk(
                "KXCUPMATCH-26JUN201330DDDBBB-DDD",
                "KXCUPMATCH-26JUN201330DDDBBB",
                None,
            ),
            mk(
                "KXCUPMATCH-26JUN181330DDDEEE-DDD",
                "KXCUPMATCH-26JUN181330DDDEEE",
                None,
            ),
            mk(
                "KXCUPMATCH-26JUN181330DDDEEE-EEE",
                "KXCUPMATCH-26JUN181330DDDEEE",
                None,
            ),
        ];
        // m12: Scotland vs West Indies, no predicted ticker.
        let m12 = entry(None, &["DDD", "EEE"]);
        let matched = match_event_markets(&m12, &page);
        assert_eq!(
            matched,
            vec![
                "KXCUPMATCH-26JUN181330DDDEEE-DDD",
                "KXCUPMATCH-26JUN181330DDDEEE-EEE",
            ],
            "team match must narrow to the DDDEEE markets only (DDDBBB excluded)"
        );
        assert_eq!(
            session_name(&m12, &matched),
            "KXCUPMATCH-26JUN181330DDDEEE",
            "session must be named for the matched event, never a sibling in the page"
        );
    }
}
