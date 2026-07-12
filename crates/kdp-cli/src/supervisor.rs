//! The shared capture-supervisor spine.
//!
//! Both `capture-hourly` (forward continuous KXBTCD) and `capture-scheduled`
//! (pre-scheduled one-off events) have the same shape — `arm -> resolve targets
//! -> capture -> settle -> archive -> drain` — differing only in (1) where the
//! schedule comes from and (2) how target tickers are resolved. This module is
//! the part they share: the settlement watcher, the per-unit
//! capture->.done->spawn-archive orchestration, process-wide shutdown wiring,
//! the timing helpers, and the in-flight task drain. Each command keeps its own
//! (cheap) arming loop and its own (specific) resolver, then calls
//! [`run_capture_unit`] for the correctness-critical part.
//!
//! Capture + store only — like everything in kdp.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::time;
use tracing::{error, info, warn};

use kdp_kalshi::auth::KalshiCredentials;
use kdp_kalshi::rest::{list_markets_page, Market};

use crate::capture::capture_session;

/// Recognised terminal statuses -- mirrors `KDP_SETTLE_TERMINAL` in settlewatch.sh.
const TERMINAL: [&str; 4] = ["closed", "settled", "finalized", "determined"];

/// True iff `status` is a recognised terminal status. Conservative: an
/// unknown/empty status is NOT terminal (keep capturing -- missing L2 is
/// unrecoverable, ADR-003).
pub fn is_terminal(status: Option<&str>) -> bool {
    status.map(|s| TERMINAL.contains(&s)).unwrap_or(false)
}

/// Given the latest market list for a unit's tickers, are ALL of them terminal?
/// A ticker absent from `latest` counts as not-terminal (conservative).
pub fn all_settled(target: &[String], latest: &[Market]) -> bool {
    target.iter().all(|t| {
        latest
            .iter()
            .find(|m| m.ticker.as_str() == t)
            .map(|m| is_terminal(m.status.as_deref()))
            .unwrap_or(false)
    })
}

/// One unit of capture: a resolved target market set plus the metadata the spine
/// needs to capture it, watch for its settlement, and archive it. Built by each
/// command's resolver (hourly's band selection / the scheduler's hybrid resolve).
#[derive(Clone, Debug)]
pub struct CaptureUnit {
    /// Session name: the on-disk dir (`data_dir/<session>`) AND the Drive folder.
    /// Usually the event ticker.
    pub session: String,
    /// The markets to capture (e.g. a strike band, or both sides of a match).
    pub tickers: Vec<String>,
    /// Series ticker, for the settlement poll's `series` filter.
    pub series: String,
    /// Optional Drive-namespace override passed to the archive script as its 2nd
    /// arg (`None` = the archive script's own `KDP_RCLONE_REMOTE` default).
    pub remote_prefix: Option<String>,
}

/// Knobs for capturing one unit (defaults mirror `capture` + settlewatch.sh).
#[derive(Clone, Debug)]
pub struct UnitCfg {
    pub data_dir: String,
    pub floor_bytes: u64,
    pub capacity: usize,
    pub idle_secs: u64,
    /// Keep capturing this long after all targets go terminal (catch convergence).
    pub grace_secs: u64,
    /// Settlement poll interval.
    pub poll_secs: u64,
    /// Hard backstop per unit (seconds). Hourly sets ~2h; scheduled ~8h.
    pub max_secs: u64,
    /// Path to kdp-archive.sh (empty = skip archive, for dev/capture-only).
    pub archive_cmd: String,
    /// Periodic REST verify-sweep interval (seconds); `0` disables it.
    pub verify_interval_secs: u64,
}

/// Run one unit end-to-end: capture `unit.tickers` into `data_dir/<session>`
/// until the unit settles (all targets terminal -> grace) or the backstop fires,
/// then (best effort) spawn the background archive for that session. Returns when
/// capture has STOPPED; the archive runs detached.
///
/// This is the generalised body of the old `run_one_hour` -- the
/// correctness-critical orchestration shared by every supervisor.
///
/// `creds` is an `Arc` (`KalshiCredentials` is not `Clone`) so `capture_session`
/// can hand a clone to its own periodic verify-sweep task without borrowing
/// across an await; every caller already builds one `Arc<KalshiCredentials>`
/// once at startup and clones it per spawned unit.
pub async fn run_capture_unit(
    creds: Arc<KalshiCredentials>,
    client: &reqwest::Client,
    unit: &CaptureUnit,
    cfg: &UnitCfg,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let base_dir = format!("{}/{}", cfg.data_dir, unit.session);
    let stop = Arc::new(Notify::new());

    // Settlement watcher: poll /markets filtered by SERIES ONLY, match targets by
    // ticker, stop when all are terminal (+grace) or the backstop fires. This
    // mirrors the proven `kdp-settlewatch.sh` exactly. Do NOT window the poll on
    // `close_time`: Kalshi REWRITES close_time on settlement (a pre-settle +2-day
    // hard-close jumps to the real close, e.g. 06-14 -> 06-12 21:12Z), so any
    // close_time window built at resolve time would filter the settled markets
    // OUT of the response -> all_settled never sees them terminal -> capture runs
    // to the backstop and never archives. (This bit the first scheduled go-live; the hourly
    // path shared the same window but escaped only because KXBTCD close_time is
    // stable.) The series filter scopes the result; absent targets count as
    // not-terminal (conservative), so a truncated page can never stop us early.
    let watcher = {
        let stop = stop.clone();
        let client = client.clone();
        let series = unit.series.clone();
        let target = unit.tickers.clone();
        let (poll, grace, max_secs) = (cfg.poll_secs, cfg.grace_secs, cfg.max_secs);
        tokio::spawn(async move {
            let deadline = time::Instant::now() + Duration::from_secs(max_secs);
            loop {
                if time::Instant::now() >= deadline {
                    warn!("unit backstop reached; stopping capture");
                    break;
                }
                match list_markets_page(&client, 1000, None, Some(&series), None, None, None).await
                {
                    Ok((markets, _)) if all_settled(&target, &markets) => {
                        info!(
                            grace_s = grace,
                            "unit settled; capturing grace then stopping"
                        );
                        time::sleep(Duration::from_secs(grace)).await;
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "settlement poll failed; will retry"),
                }
                time::sleep(Duration::from_secs(poll)).await;
            }
            stop.notify_one();
        })
    };

    // Stop capture on settlement (the watcher) OR a process-wide shutdown signal
    // (ctrl-c / `systemctl stop`) -- the latter lets an in-flight unit drain cleanly
    // and still archive, rather than being killed mid-write.
    let shutdown = {
        let stop = stop.clone();
        async move {
            tokio::select! {
                _ = stop.notified() => {}
                _ = wait_shutdown(&mut shutdown_rx) => {}
            }
        }
    };
    match capture_session(
        creds,
        &unit.tickers,
        &base_dir,
        cfg.floor_bytes,
        cfg.capacity,
        Duration::from_secs(cfg.idle_secs),
        cfg.verify_interval_secs,
        shutdown,
    )
    .await
    {
        Ok(report) => {
            info!(session = %unit.session, records = report.total_written(), "unit capture stopped")
        }
        Err(e) => error!(session = %unit.session, error = %e, "unit capture error"),
    }
    watcher.abort();

    // Mark the session done so the nightly archive sweep picks it up even if the
    // inline archive below is interrupted (e.g. a shutdown killing the spawned child).
    if let Err(e) = std::fs::File::create(format!("{base_dir}/.done")) {
        warn!(session = %unit.session, error = %e, "could not write .done marker");
    }

    // Background archive: process -> curate two_sided -> verified Drive -> prune.
    // When a remote prefix override is set, pass it as the 2nd arg so this unit's
    // event-set lands in its own storage namespace (`<KDP_RCLONE_REMOTE>/<set>/<session>`).
    if !cfg.archive_cmd.is_empty() {
        let mut command = std::process::Command::new(&cfg.archive_cmd);
        command.arg(&unit.session);
        if let Some(prefix) = &unit.remote_prefix {
            command.arg(prefix);
        }
        match command.spawn() {
            Ok(mut child) => {
                info!(session = %unit.session, "background archive spawned");
                // Reap the finished archive so it does not linger as a zombie.
                std::thread::spawn(move || {
                    let _ = child.wait();
                });
            }
            Err(e) => {
                error!(session = %unit.session, error = %e, "could not spawn archive; raw kept locally")
            }
        }
    }
}

/// Install the process-wide shutdown channel: ctrl-c / `systemctl stop` (SIGINT)
/// flips the returned watch to `true`. A supervisor's arming loop watches this to
/// stop arming new units; in-flight units drain + archive. Returns the receiver
/// (the spawned signal task owns the sender).
pub fn install_shutdown() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("shutdown signal received; stopping new units, draining in-flight");
            let _ = shutdown_tx.send(true);
        }
    });
    shutdown_rx
}

/// Resolve once `rx` carries `true` (shutdown requested), now or in the future.
/// Returns immediately if already shutting down; returns if the sender is dropped.
pub async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}

/// Sleep `d`, returning `true` if a shutdown fired first.
pub async fn interruptible_sleep(d: Duration, rx: &watch::Receiver<bool>) -> bool {
    let mut rx = rx.clone();
    tokio::select! {
        _ = time::sleep(d) => false,
        _ = wait_shutdown(&mut rx) => true,
    }
}

/// Drop finished unit-tasks, surfacing any that ended abnormally (panic/cancel).
pub async fn reap_inflight(inflight: &mut Vec<tokio::task::JoinHandle<()>>) {
    let mut alive = Vec::new();
    for h in inflight.drain(..) {
        if h.is_finished() {
            if let Err(e) = h.await {
                warn!(error = %e, "a unit task ended abnormally (panic/cancel)");
            }
        } else {
            alive.push(h);
        }
    }
    *inflight = alive;
}

/// Graceful drain: in-flight units have been signaled to stop -> each stops
/// capture, writes .done, and spawns its archive. Bound the wait before exit.
///
/// NB: the 30s bound is correct ONLY when the units have been told to stop (a
/// shutdown). It must NOT be used to "wait for a live match to finish" -- a match
/// runs for hours, so the bound would fire while it is still capturing. Use
/// [`park_until_shutdown`] for the end-of-schedule wait. (2026-06-28 INDAUS
/// post-mortem.)
pub async fn drain_inflight(inflight: Vec<tokio::task::JoinHandle<()>>) {
    info!(inflight = inflight.len(), "draining in-flight units");
    for h in inflight {
        match time::timeout(Duration::from_secs(30), h).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(error = %e, "a unit task ended abnormally"),
            Err(_) => warn!("a unit task did not drain within 30s"),
        }
    }
}

/// Park until shutdown, reaping in-flight unit tasks as they finish on their own
/// (their matches settle + archive). Unlike [`drain_inflight`] (a *bounded*
/// shutdown drain), this NEVER returns on its own initiative.
///
/// Why it must never self-return: after arming every scheduled entry the
/// supervisor has no more work, but its in-flight captures may still be LIVE
/// (hours from settling). If it returned here, systemd's `Restart=always` would
/// relaunch it and re-arm every still-in-window match, reconnecting their live
/// feeds on each cycle -- a ~30s restart loop that fragments the capture (and,
/// once the schedule is fully past, a pointless hot idle-loop). So the scheduled
/// supervisor exits ONLY on an explicit shutdown, which then bounds the drain.
/// (2026-06-28 INDAUS post-mortem: `drain_inflight`'s 30s bound, reached at
/// end-of-pending while the match was still live, drove exactly that loop.)
pub async fn park_until_shutdown(
    mut inflight: Vec<tokio::task::JoinHandle<()>>,
    shutdown_rx: &watch::Receiver<bool>,
) {
    while !*shutdown_rx.borrow() {
        reap_inflight(&mut inflight).await;
        // Wake immediately on shutdown; otherwise re-check every 5s to reap any
        // captures that have since settled + archived.
        if interruptible_sleep(Duration::from_secs(5), shutdown_rx).await {
            break;
        }
    }
    // Shutdown: bound the drain of whatever is still running, then return to exit.
    drain_inflight(inflight).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn park_until_shutdown_stays_parked_until_signal() {
        // Regression for the 2026-06-28 INDAUS restart loop: after arming all
        // entries the supervisor must NOT return on its own (which Restart=always
        // would turn into a re-arming loop); it returns only once shutdown fires.
        let (tx, rx) = watch::channel(false);
        let h = tokio::spawn(async move { park_until_shutdown(Vec::new(), &rx).await });
        // No shutdown yet -> must still be parked a beat later.
        time::sleep(Duration::from_millis(50)).await;
        assert!(
            !h.is_finished(),
            "must stay parked until shutdown is signalled"
        );
        // Signal shutdown -> must return promptly.
        let _ = tx.send(true);
        time::timeout(Duration::from_secs(2), h)
            .await
            .expect("park must return after shutdown")
            .expect("park task must not panic");
    }

    fn mk(ticker: &str, status: &str) -> Market {
        Market {
            ticker: kdp_core::Ticker(ticker.into()),
            title: String::new(),
            status: Some(status.into()),
            volume_fp: None,
            close_time: None,
            event_ticker: None,
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

    #[test]
    fn is_terminal_only_for_recognised_statuses() {
        assert!(is_terminal(Some("settled")));
        assert!(is_terminal(Some("finalized")));
        assert!(is_terminal(Some("closed")));
        assert!(is_terminal(Some("determined")));
        assert!(!is_terminal(Some("open")));
        assert!(!is_terminal(None));
    }

    #[test]
    fn all_settled_requires_every_target_terminal_and_present() {
        let a = mk("A", "settled");
        let b_open = mk("B", "open");
        let target = vec!["A".to_string(), "B".to_string()];
        assert!(
            !all_settled(&target, &[a.clone(), b_open]),
            "B still open -> not settled"
        );
        let b_settled = mk("B", "settled");
        assert!(all_settled(&target, &[a.clone(), b_settled]));
        assert!(!all_settled(&target, &[a]), "B absent -> not settled");
    }

    #[test]
    fn settlement_detection_ignores_close_time() {
        // Regression (first scheduled go-live): settlement is decided by STATUS alone, never
        // by close_time. Kalshi rewrites close_time on settlement, so the watcher
        // must not depend on it -- it polls by series only and matches by ticker.
        // A finalized market with a wildly stale/far-future close_time still counts
        // as settled.
        let mut m = mk("KXCUPMATCH-26JUN121330AAABBB-BBB", "finalized");
        m.close_time = Some("2026-06-14T21:30:00Z".into()); // the +2-day hard-close
        let target = vec!["KXCUPMATCH-26JUN121330AAABBB-BBB".to_string()];
        assert!(
            all_settled(&target, &[m]),
            "terminal status must settle the unit regardless of close_time",
        );
    }
}
