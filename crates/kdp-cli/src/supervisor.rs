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
use kdp_kalshi::rest::{list_markets_by_event, Market};

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

/// Seconds from `now` to the next UTC midnight (exactly 86400 at midnight).
pub(crate) fn secs_to_next_utc_midnight(now: chrono::DateTime<chrono::Utc>) -> u64 {
    let next = (now.date_naive() + chrono::Days::new(1))
        .and_hms_opt(0, 0, 0)
        .map(|n| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(n, chrono::Utc));
    match next {
        Some(n) => (n - now).num_seconds().max(1) as u64,
        None => 86_400, // unreachable in practice; a sane fallback beats a panic
    }
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
    /// The Kalshi **event ticker** this unit captures — the settlement poll's
    /// scope (`GET /markets?event_ticker=…`). Must be a real API event ticker,
    /// not a display name: the poll matches the response against `tickers`, and
    /// a wrong value returns an empty page, whose targets are all "absent" and
    /// therefore conservatively not-terminal, so the unit would run to its
    /// backstop. Usually equal to `session`, but they are different contracts —
    /// `session` names a directory, this names an API resource.
    pub event: String,
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
    /// Daily raw checkpoint command (empty = disabled). Spawned in the
    /// background just after each UTC day rotation for units still alive,
    /// with the same args as archive_cmd (session, then remote_prefix when
    /// set). A failed checkpoint alerts but never touches capture.
    pub checkpoint_cmd: String,
    /// Periodic REST verify-sweep interval (seconds); `0` disables it.
    pub verify_interval_secs: u64,
}

/// One settlement poll's verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollOutcome {
    /// Every target is terminal and the whole target set was visible.
    Settled,
    /// At least one target is live, absent, or the page was truncated.
    NotSettled,
    /// The fetch itself failed; nothing was learned. Retry.
    Failed,
}

/// Decide, from one settlement poll, whether the unit is done.
///
/// Split out of the watcher loop and generic over the fetch so a test can drive
/// the real decision path against a fixture that models Kalshi's paging. The
/// bug this replaces (2026-08-08) was never in `all_settled` — it was in WHICH
/// markets the poll asked for, and nothing tested that. `fetch` takes the event
/// ticker by value so the closure can be `move`-free at the call site.
///
/// A truncated page (cursor present) is NOT settled: with part of the target
/// set invisible, "all terminal" cannot be honestly concluded. It also warns,
/// because silently running to the backstop is how the original bug hid for
/// months.
pub(crate) async fn poll_once<F, Fut>(fetch: F, event: &str, target: &[String]) -> PollOutcome
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<(Vec<Market>, Option<String>)>>,
{
    match fetch(event.to_string()).await {
        Err(e) => {
            warn!(event = %event, error = %e, "settlement poll failed; will retry");
            PollOutcome::Failed
        }
        Ok((_, Some(_))) => {
            warn!(
                event = %event, targets = target.len(),
                "settlement poll page was truncated; cannot see the whole cohort, so \
                 settlement can never be detected -- this unit will run to its backstop"
            );
            PollOutcome::NotSettled
        }
        Ok((m, None)) if m.is_empty() => {
            warn!(
                event = %event, targets = target.len(),
                "settlement poll returned zero markets for this event ticker; the poll scope is \
                 probably wrong -- this unit can never settle and will run to its backstop"
            );
            PollOutcome::NotSettled
        }
        Ok((markets, None)) if all_settled(target, &markets) => PollOutcome::Settled,
        Ok(_) => PollOutcome::NotSettled,
    }
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

    // Persist the Drive-namespace prefix where the OUT-OF-PROCESS recovery
    // paths can find it: the nightly kdp-archive.sh sweep (and a manual
    // no-prefix invocation) read `.remote-prefix` before falling back to the
    // bare env remote. Without it, a sweep-recovered prefixed session uploads
    // outside its namespace and its raw-inflight checkpoints are never purged
    // (review I-2). Written at unit START so it survives every death mode.
    if let Some(prefix) = &unit.remote_prefix {
        let write = std::fs::create_dir_all(&base_dir)
            .and_then(|()| std::fs::write(format!("{base_dir}/.remote-prefix"), prefix));
        if let Err(e) = write {
            warn!(session = %unit.session, error = %e,
                "could not write .remote-prefix marker; a sweep-recovered archive would fall back to the bare remote");
        }
    }

    // Settlement watcher: poll `/markets?event_ticker=<event>` and stop when
    // every target is terminal (+grace) or the backstop fires.
    //
    // Scoped to the EVENT, never the series (2026-08-08 post-mortem). A cohort
    // IS an event, so the response is exactly the target set and the
    // conservative "absent => not terminal" rule in `all_settled` is safe. The
    // previous series-scoped poll read one 1000-row page and discarded the
    // cursor; a cohort's targets were essentially never in that page, so
    // `all_settled` returned false forever and EVERY unit ran to its backstop
    // -- 0 settlements across two supervisors, 164 cohorts permanently lost
    // (ADR-003) in one 7-day soak.
    //
    // Also, still: never window this poll on `close_time`. Kalshi rewrites
    // close_time on settlement (a pre-settle +2-day hard-close jumps to the
    // real close), so a resolve-time window filters the settled markets out
    // (2026-06-12 post-mortem). The event filter is stable; close_time is not.
    let watcher = {
        let stop = stop.clone();
        let client = client.clone();
        let event = unit.event.clone();
        let target = unit.tickers.clone();
        let (poll, grace, max_secs) = (cfg.poll_secs, cfg.grace_secs, cfg.max_secs);
        tokio::spawn(async move {
            let deadline = time::Instant::now() + Duration::from_secs(max_secs);
            loop {
                if time::Instant::now() >= deadline {
                    warn!("unit backstop reached; stopping capture");
                    break;
                }
                let outcome = poll_once(
                    |ev| {
                        let client = client.clone();
                        async move {
                            list_markets_by_event(&client, &ev, 1000)
                                .await
                                .map_err(anyhow::Error::from)
                        }
                    },
                    &event,
                    &target,
                )
                .await;
                if outcome == PollOutcome::Settled {
                    info!(
                        grace_s = grace,
                        "unit settled; capturing grace then stopping"
                    );
                    time::sleep(Duration::from_secs(grace)).await;
                    break;
                }
                time::sleep(Duration::from_secs(poll)).await;
            }
            stop.notify_one();
        })
    };

    // Daily raw checkpoint: fires ~60s after each UTC day rotation for units
    // still alive at that point (hourly units never live past one midnight, so
    // this is a no-op for them). Same spawn idiom as the archive below.
    let checkpoint = if cfg.checkpoint_cmd.is_empty() {
        None
    } else {
        let cmd = cfg.checkpoint_cmd.clone();
        let session = unit.session.clone();
        let prefix = unit.remote_prefix.clone();
        Some(tokio::spawn(async move {
            loop {
                let wait = secs_to_next_utc_midnight(chrono::Utc::now()) + 60;
                time::sleep(Duration::from_secs(wait)).await;
                let mut command = std::process::Command::new(&cmd);
                command.arg(&session);
                if let Some(p) = &prefix {
                    command.arg(p);
                }
                match command.spawn() {
                    Ok(mut child) => {
                        info!(session = %session, "background raw checkpoint spawned");
                        std::thread::spawn(move || {
                            let _ = child.wait();
                        });
                    }
                    Err(e) => error!(session = %session, error = %e,
                        "could not spawn checkpoint; raw remains local-only until archive"),
                }
            }
        }))
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
    if let Some(h) = checkpoint {
        h.abort();
    }

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

    #[test]
    fn secs_to_next_utc_midnight_counts_down_and_wraps() {
        let t = |s: &str| s.parse::<chrono::DateTime<chrono::Utc>>().unwrap();
        assert_eq!(
            secs_to_next_utc_midnight(t("2026-08-01T23:59:00+00:00")),
            60
        );
        assert_eq!(
            secs_to_next_utc_midnight(t("2026-08-01T00:00:00+00:00")),
            86_400
        );
        assert_eq!(
            secs_to_next_utc_midnight(t("2026-08-01T12:00:00+00:00")),
            43_200
        );
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

    fn mk_ev(ticker: &str, event: &str, status: &str) -> Market {
        let mut m = mk(ticker, status);
        m.event_ticker = Some(event.to_string());
        m
    }

    /// A fake venue that models Kalshi's real paging: many events, far more
    /// than one page of markets in the series, and a cursor when a query
    /// overflows `limit`. `seen` records every event actually asked for, so a
    /// regression to series-wide polling fails loudly instead of silently.
    struct Venue {
        markets: Vec<Market>,
        limit: usize,
        seen: std::sync::Mutex<Vec<String>>,
    }

    impl Venue {
        /// 60 events x 200 markets = 12,000 markets in one series. `TARGET` is
        /// the LAST event by ticker order, so it is nowhere near the first
        /// 1000-row page a series-scoped listing would return -- the exact
        /// production shape (measured: KXBTC/KXETHD carry 6,848 / 10,840
        /// unopened plus ~1,000 settled markets each).
        fn new(target_status: &str) -> Self {
            let mut markets = Vec::new();
            for ev in 0..60 {
                let event = format!("KXTEST-26AUG{ev:04}");
                let status = if event == Self::TARGET {
                    target_status
                } else {
                    "active"
                };
                for k in 0..200 {
                    markets.push(mk_ev(&format!("{event}-T{k}"), &event, status));
                }
            }
            Venue {
                markets,
                limit: 1000,
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }

        const TARGET: &'static str = "KXTEST-26AUG0059";

        fn targets() -> Vec<String> {
            (0..200).map(|k| format!("{}-T{k}", Self::TARGET)).collect()
        }

        /// What a series-scoped poll saw: the first `limit` rows of the series.
        fn series_first_page(&self) -> Vec<Market> {
            self.markets.iter().take(self.limit).cloned().collect()
        }

        async fn event_page(&self, event: String) -> anyhow::Result<(Vec<Market>, Option<String>)> {
            if let Ok(mut s) = self.seen.lock() {
                s.push(event.clone());
            }
            let hits: Vec<Market> = self
                .markets
                .iter()
                .filter(|m| m.event_ticker.as_deref() == Some(event.as_str()))
                .cloned()
                .collect();
            let next = (hits.len() > self.limit).then(|| "more".to_string());
            Ok((hits.into_iter().take(self.limit).collect(), next))
        }
    }

    #[test]
    fn the_series_scoped_page_never_contained_the_targets() {
        // The 2026-08-08 post-mortem, encoded. This is not a test of new code;
        // it is the fixture's proof that the OLD poll could not work, so the
        // tests below are testing something real. 172 cohorts armed, 0 ever
        // settled, 164 permanently lost (ADR-003).
        let venue = Venue::new("finalized");
        let page = venue.series_first_page();
        let target = Venue::targets();
        assert_eq!(page.len(), 1000, "the series page truncates");
        assert!(
            !all_settled(&target, &page),
            "the targets are absent from the series page, so the unit can NEVER settle"
        );
    }

    #[tokio::test]
    async fn poll_detects_settlement_of_an_event_buried_past_the_first_page() {
        let venue = Venue::new("finalized");
        let out = poll_once(|ev| venue.event_page(ev), Venue::TARGET, &Venue::targets()).await;
        assert_eq!(out, PollOutcome::Settled);
        assert_eq!(
            *venue.seen.lock().expect("seen"),
            vec![Venue::TARGET.to_string()],
            "the poll must scope to the unit's EVENT, never to its series"
        );
    }

    #[tokio::test]
    async fn poll_keeps_capturing_while_any_target_is_active() {
        let venue = Venue::new("active");
        let out = poll_once(|ev| venue.event_page(ev), Venue::TARGET, &Venue::targets()).await;
        assert_eq!(out, PollOutcome::NotSettled);
    }

    #[tokio::test]
    async fn poll_keeps_capturing_a_cohort_that_has_not_opened_yet() {
        // The D1 x D2 interaction guard. Pre-open arming (Task 3) arms a unit
        // up to --arm-lead-min before its markets open, when they report
        // `initialized`. That is not terminal, so the unit must keep capturing
        // -- if this ever returned Settled the whole cohort would be dropped
        // at t-30min, every hour, silently.
        let venue = Venue::new("initialized");
        let out = poll_once(|ev| venue.event_page(ev), Venue::TARGET, &Venue::targets()).await;
        assert_eq!(out, PollOutcome::NotSettled);
    }

    #[tokio::test]
    async fn poll_refuses_to_conclude_settled_from_a_truncated_page() {
        // An event bigger than one page means we cannot see every target, which
        // is the failure class this whole change exists to close. Never settle
        // on partial data; the backstop remains the honest bound.
        let mut venue = Venue::new("finalized");
        venue.limit = 50;
        let out = poll_once(|ev| venue.event_page(ev), Venue::TARGET, &Venue::targets()).await;
        assert_eq!(out, PollOutcome::NotSettled);
    }

    #[tokio::test]
    async fn poll_does_not_settle_when_the_event_ticker_matches_nothing() {
        // Real trigger: a CaptureUnit.event that is not a real API event ticker.
        // scheduled.rs's session_name() falls back to entry.id when a matched
        // ticker doesn't split on '-' and no event_ticker was read -- that id
        // (e.g. "wt20-sco-eng") is never a real event ticker, so the poll
        // returns zero markets every time. Without this arm that degrades
        // silently into NotSettled and the unit just rides the backstop.
        let venue = Venue::new("finalized");
        let out = poll_once(|ev| venue.event_page(ev), "wt20-sco-eng", &Venue::targets()).await;
        assert_eq!(out, PollOutcome::NotSettled);
    }

    #[tokio::test]
    async fn poll_reports_failure_without_settling_when_the_fetch_errors() {
        let out = poll_once(
            |_ev| async { Err(anyhow::anyhow!("503 from the venue")) },
            "KXTEST-26AUG0059",
            &Venue::targets(),
        )
        .await;
        assert_eq!(out, PollOutcome::Failed);
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
