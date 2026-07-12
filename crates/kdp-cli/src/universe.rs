//! `capture-universe` — declarative breadth capture over the supervisor spine.
//!
//! The third adapter (after `capture-hourly` and `capture-scheduled`, D22): its
//! arming loop is "sweep `/markets` for every series in a filter, group the
//! non-terminal markets into settlement cohorts (one per event), arm each new
//! cohort as a `CaptureUnit`" — re-discovering on an interval so newly-listed
//! markets are picked up. The capture/settle/archive path is the spine,
//! unchanged. Breadth is bounded by `--max-units`; past the cap an event is
//! skipped with a LOUD warn (never silently) — that warn firing in real use is
//! the measured trigger for WS connection sharding (see the design doc).
//!
//! Capture + store only — like everything in kdp.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use kdp_kalshi::auth::KalshiCredentials;
use kdp_kalshi::rest::{list_markets_page, Market};
use tracing::{info, warn};

use crate::supervisor::{
    drain_inflight, install_shutdown, interruptible_sleep, is_terminal, run_capture_unit,
    CaptureUnit, UnitCfg,
};

/// One settlement cohort: the markets of a single event, plus the series the
/// settlement watcher must poll (`all_settled` matches by ticker within a
/// series-only poll — the 2026-06-12 post-mortem).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Cohort {
    pub(crate) series: String,
    pub(crate) tickers: Vec<String>,
}

/// Group one series-sweep's markets into settlement cohorts, keyed by event
/// ticker. Prefers the API's `event_ticker`; falls back to stripping the side
/// suffix (`<EVENT>-<SIDE>`, the `scheduled::session_name` derivation).
/// Terminal markets are dropped — never arm a settled market.
pub(crate) fn group_by_event(series: &str, markets: &[Market]) -> BTreeMap<String, Cohort> {
    let mut groups: BTreeMap<String, Cohort> = BTreeMap::new();
    for m in markets {
        if is_terminal(m.status.as_deref()) {
            continue;
        }
        let event = m.event_ticker.clone().or_else(|| {
            m.ticker
                .as_str()
                .rsplit_once('-')
                .map(|(ev, _side)| ev.to_string())
        });
        let Some(event) = event else {
            warn!(ticker = %m.ticker.as_str(), "market has no derivable event; skipping");
            continue;
        };
        groups
            .entry(event)
            .or_insert_with(|| Cohort {
                series: series.to_string(),
                tickers: Vec::new(),
            })
            .tickers
            .push(m.ticker.as_str().to_string());
    }
    groups
}

/// What one sweep decided.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ArmPlan {
    /// Cohorts to arm this sweep: (session = event ticker, cohort).
    pub(crate) arm: Vec<(String, Cohort)>,
    /// Events skipped because the unit cap was hit — the caller MUST warn per
    /// entry (loud, never a silent drop; this firing is the sharding trigger).
    pub(crate) capped: Vec<String>,
}

/// Decide which discovered cohorts to arm: skip in-flight sessions and events
/// already captured on disk, then admit up to the remaining unit budget
/// (`max_units - in_flight`). Everything past the budget lands in `capped`.
///
/// An event is "already captured" if its session dir carries **`.done`** OR
/// **`.archived`**. `.archived` alone is not enough: it is written only after
/// the whole process->upload->verify archive completes (minutes), which is
/// routinely longer than `--rediscover-interval`. A market that stays open past
/// the `--max-hours` backstop (the long-lived lifecycle universe is meant to
/// sweep) is stopped by the spine, drops out of `in_flight`, is STILL
/// non-terminal in the next sweep, and — if we only checked `.archived` — would
/// be re-armed WHILE its background archive is still pruning/uploading the same
/// session dir: concurrent prune of live files + a second archive that skips on
/// the `.archived` guard => silent L2 loss (ADR-003). `.done` is written
/// synchronously at unit stop (before the archive spawns, `supervisor.rs`) for
/// ALL stop reasons and survives prune, so it closes that race. (Bounded
/// lifecycle by design: stop at the backstop, do not auto-resume; raise
/// `--max-hours` to capture longer. `.done`/`.archived` are reaped after
/// `KDP_HOT_DAYS`, so an event still open past that horizon could re-arm — an
/// accepted edge; use a larger `--max-hours` for genuinely long-lived markets.)
pub(crate) fn plan_arms(
    groups: BTreeMap<String, Cohort>,
    in_flight: &HashSet<String>,
    data_dir: &str,
    max_units: usize,
) -> ArmPlan {
    let mut plan = ArmPlan::default();
    let mut budget = max_units.saturating_sub(in_flight.len());
    for (event, cohort) in groups {
        if in_flight.contains(&event) {
            continue;
        }
        let sdir = Path::new(data_dir).join(&event);
        if sdir.join(".done").exists() || sdir.join(".archived").exists() {
            continue;
        }
        if budget == 0 {
            // Deterministic (BTreeMap-ordered) skip, not fair rotation: under a
            // persistent over-cap condition the same alphabetically-later events
            // are starved every interval. That is by design -- the warn naming
            // each starved event IS the "raise --max-units or shard" trigger.
            plan.capped.push(event);
            continue;
        }
        budget -= 1;
        plan.arm.push((event, cohort));
    }
    plan
}

/// `capture-universe --series A,B,C --name NAME [--status open] [--min-volume 0]
///  [--rediscover-interval 300] [--max-units 8] [--out DIR] [--max-hours 8]
///  [--grace 180] [--poll 30] [--idle 45] [--buffer 8192] [--archive-cmd PATH]`
///
/// Forever (until shutdown): sweep each series, arm every new settlement cohort
/// up to --max-units, sleep --rediscover-interval, repeat. The re-sweep loop IS
/// the park — like the scheduled adapter it exits ONLY on explicit shutdown
/// (2026-06-28 post-mortem: any other return under Restart=always is a restart
/// loop), which then bounds the drain.
pub(crate) async fn run_universe(args: &crate::args::Args) -> anyhow::Result<()> {
    let series: Vec<String> = args
        .get("series")
        .context("--series A,B,C is required (series tickers to sweep)")?
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if series.is_empty() {
        anyhow::bail!("--series must name at least one series");
    }
    let name = args
        .get("name")
        .context("--name NAME is required (Drive namespace: universe-<name>)")?;
    let statuses: Vec<String> = args
        .get_or("status", "open")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if statuses.is_empty() {
        // An explicit empty/whitespace --status would leave the sweep loop with
        // nothing to iterate -> the supervisor idles forever arming nothing.
        anyhow::bail!("--status must name at least one status (e.g. open)");
    }
    let min_volume: f64 = args.get_or("min-volume", "0").parse().unwrap_or(0.0);
    let rediscover: u64 = args
        .get_or("rediscover-interval", "300")
        .parse()
        .unwrap_or(300);
    let max_units: usize = args.get_or("max-units", "8").parse().unwrap_or(8);
    let cfg = UnitCfg {
        data_dir: args.get_or("out", "/var/lib/kdp/data").to_string(),
        floor_bytes: 3 * 1024 * 1024 * 1024,
        capacity: args.get_or("buffer", "8192").parse().unwrap_or(8192),
        idle_secs: args.get_or("idle", "45").parse().unwrap_or(45),
        grace_secs: args.get_or("grace", "180").parse().unwrap_or(180),
        poll_secs: args.get_or("poll", "30").parse().unwrap_or(30),
        max_secs: args.get_or("max-hours", "8").parse::<u64>().unwrap_or(8) * 3600,
        archive_cmd: args
            .get_or("archive-cmd", "/opt/kdp/bin/kdp-archive.sh")
            .to_string(),
        verify_interval_secs: crate::capture::parse_verify_interval(args)?,
    };
    let remote_prefix = format!("universe-{name}");

    let creds = Arc::new(KalshiCredentials::from_env().context("loading Kalshi credentials")?);
    let client = reqwest::Client::builder()
        .user_agent(concat!("kdp-cli/", env!("CARGO_PKG_VERSION")))
        .build()?;
    info!(
        series = ?series, name = %name, statuses = ?statuses, min_volume,
        rediscover_s = rediscover, max_units, out = %cfg.data_dir,
        "starting universe supervisor"
    );

    let shutdown_rx = install_shutdown();
    let mut inflight: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        reap_named(&mut inflight).await;

        // Sweep: one paginated listing per (series, status). A failed sweep is a
        // warn + retry next interval, never an abort (capture must keep running).
        let mut groups: BTreeMap<String, Cohort> = BTreeMap::new();
        for s in &series {
            let mut markets: Vec<Market> = Vec::new();
            for st in &statuses {
                match sweep_series(&client, s, st).await {
                    Ok(mut ms) => markets.append(&mut ms),
                    Err(e) => {
                        warn!(series = %s, status = %st, error = %e, "sweep failed; retrying next interval")
                    }
                }
            }
            markets.retain(|m| m.volume() >= min_volume);
            // Merge this series' cohorts into the sweep. Safe against cross-series
            // key collision because Kalshi event tickers are always series-prefixed
            // (KXWT20MATCH-..., KXIPLGAME-..., KXBTCD-...), so two distinct --series
            // can't produce the same event key. If Kalshi ever breaks that, switch
            // to a (series, event) composite key.
            groups.extend(group_by_event(s, &markets));
        }

        let in_flight_names: HashSet<String> = inflight.iter().map(|(n, _)| n.clone()).collect();
        let plan = plan_arms(groups, &in_flight_names, &cfg.data_dir, max_units);
        for event in &plan.capped {
            warn!(
                event = %event, max_units,
                "universe cap reached; NOT capturing this event -- raise --max-units or shard"
            );
        }
        for (session, cohort) in plan.arm {
            info!(session = %session, tickers = cohort.tickers.len(), series = %cohort.series, "arming universe event");
            let unit = CaptureUnit {
                session: session.clone(),
                tickers: cohort.tickers,
                series: cohort.series,
                remote_prefix: Some(remote_prefix.clone()),
            };
            let creds2 = Arc::clone(&creds);
            let client2 = client.clone();
            let cfg2 = cfg.clone();
            let srx = shutdown_rx.clone();
            inflight.push((
                session,
                tokio::spawn(async move {
                    run_capture_unit(creds2, &client2, &unit, &cfg2, srx).await;
                }),
            ));
        }

        if interruptible_sleep(Duration::from_secs(rediscover), &shutdown_rx).await {
            break;
        }
    }

    // Shutdown: bound the drain of whatever is still capturing, then exit.
    drain_inflight(inflight.into_iter().map(|(_, h)| h).collect()).await;
    info!("universe supervisor stopped");
    Ok(())
}

/// One paginated `/markets` sweep for a series + status. Bounded at 10 pages
/// (10k markets) per sweep — far beyond any live series today. The cursor is NOT
/// persisted across sweeps (each sweep re-lists from the top), so if a series
/// ever genuinely exceeds 10k open markets, everything past page 10 would be
/// invisible every sweep — a silent truncation. We refuse to be silent: hitting
/// the page cap with more pages pending emits a `warn!` (raise `MAX_PAGES` or
/// narrow the filter). Not reachable with any live series today.
async fn sweep_series(
    client: &reqwest::Client,
    series: &str,
    status: &str,
) -> anyhow::Result<Vec<Market>> {
    const MAX_PAGES: u32 = 10;
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let (markets, next) = list_markets_page(
            client,
            1000,
            Some(status),
            Some(series),
            None,
            None,
            cursor.as_deref(),
        )
        .await?;
        all.extend(markets);
        match next {
            Some(c) => cursor = Some(c),
            None => return Ok(all),
        }
    }
    warn!(
        series = %series, status = %status, max_pages = MAX_PAGES,
        "sweep hit the page cap with more markets pending; markets beyond the cap are not captured this sweep -- raise MAX_PAGES or narrow the filter"
    );
    Ok(all)
}

/// Reap finished unit tasks, keeping (session, handle) pairs so the sweep's
/// in-flight dedup set stays accurate. `supervisor::reap_inflight` with names.
async fn reap_named(inflight: &mut Vec<(String, tokio::task::JoinHandle<()>)>) {
    let mut kept = Vec::with_capacity(inflight.len());
    for (name, handle) in inflight.drain(..) {
        if handle.is_finished() {
            if let Err(e) = handle.await {
                warn!(session = %name, error = %e, "a unit task ended abnormally");
            }
        } else {
            kept.push((name, handle));
        }
    }
    *inflight = kept;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unique temp dir for a test (repo idiom: `temp_dir` + pid, no extra
    /// dep). Created fresh; distinct `tag` per test avoids in-process clashes.
    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("kdp-universe-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("mkdir tmp");
        d
    }

    fn cohort(series: &str, tickers: &[&str]) -> Cohort {
        Cohort {
            series: series.to_string(),
            tickers: tickers.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Build a Market via serde (all fields are #[serde(default)]), so tests
    /// don't break when the struct gains fields.
    fn mk(ticker: &str, event: Option<&str>, status: &str) -> Market {
        serde_json::from_value(serde_json::json!({
            "ticker": ticker,
            "event_ticker": event,
            "status": status,
        }))
        .expect("test market")
    }

    #[test]
    fn groups_cohorts_by_event_ticker() {
        let ms = vec![
            mk("EV1-A", Some("EV1"), "active"),
            mk("EV1-B", Some("EV1"), "active"),
            mk("EV2-X", Some("EV2"), "active"),
        ];
        let g = group_by_event("KXFOO", &ms);
        assert_eq!(g["EV1"].tickers, vec!["EV1-A", "EV1-B"]);
        assert_eq!(g["EV1"].series, "KXFOO");
        assert_eq!(g["EV2"].tickers, vec!["EV2-X"]);
    }

    #[test]
    fn falls_back_to_ticker_suffix_strip_when_event_ticker_absent() {
        let g = group_by_event("KXFOO", &[mk("KXFOO-26JUL031500ABC-YES", None, "active")]);
        assert_eq!(
            g["KXFOO-26JUL031500ABC"].tickers,
            vec!["KXFOO-26JUL031500ABC-YES"]
        );
    }

    #[test]
    fn drops_terminal_markets() {
        let ms = vec![
            mk("EV1-A", Some("EV1"), "finalized"),
            mk("EV1-B", Some("EV1"), "active"),
        ];
        let g = group_by_event("KXFOO", &ms);
        assert_eq!(g["EV1"].tickers, vec!["EV1-B"]);
    }

    #[test]
    fn unknown_status_is_not_terminal_so_still_grouped() {
        // Conservative like is_terminal: unknown/absent status keeps capturing.
        let g = group_by_event("KXFOO", &[mk("EV1-A", Some("EV1"), "weird_new_status")]);
        assert_eq!(g["EV1"].tickers, vec!["EV1-A"]);
    }

    #[test]
    fn plan_skips_done_marked_event_not_yet_archived() {
        // Finding 1 regression: a market open past the --max-hours backstop is
        // stopped (writes `.done`), drops out of in_flight, and is STILL
        // non-terminal on the next sweep. Its background archive has not yet
        // written `.archived` (that lags by minutes). Skipping on `.done` alone
        // must prevent re-arming it mid-archive (which would prune/clobber the
        // live session -> silent L2 loss).
        let dir = tmp("done");
        std::fs::create_dir_all(dir.join("EV-DONE")).expect("mkdir");
        std::fs::File::create(dir.join("EV-DONE").join(".done")).expect("done marker");
        // Deliberately NO .archived marker.
        assert!(!dir.join("EV-DONE").join(".archived").exists());

        let mut groups = BTreeMap::new();
        groups.insert("EV-DONE".to_string(), cohort("KXFOO", &["EV-DONE-A"]));
        groups.insert("EV-NEW".to_string(), cohort("KXFOO", &["EV-NEW-A"]));

        let none: HashSet<String> = HashSet::new();
        let plan = plan_arms(groups, &none, dir.to_str().expect("utf8 tmp"), 8);
        assert_eq!(
            plan.arm,
            vec![("EV-NEW".to_string(), cohort("KXFOO", &["EV-NEW-A"]))],
            "the .done event must be skipped, only the fresh event armed"
        );
        assert!(plan.capped.is_empty());
    }

    #[test]
    fn plan_skips_in_flight_and_archived_and_caps_loudly() {
        let dir = tmp("cap");
        std::fs::create_dir_all(dir.join("EV-ARCH")).expect("mkdir");
        std::fs::File::create(dir.join("EV-ARCH").join(".archived")).expect("marker");

        let mut groups = BTreeMap::new();
        groups.insert("EV-ARCH".to_string(), cohort("KXFOO", &["EV-ARCH-A"]));
        groups.insert("EV-LIVE".to_string(), cohort("KXFOO", &["EV-LIVE-A"]));
        groups.insert("EV-NEW1".to_string(), cohort("KXFOO", &["EV-NEW1-A"]));
        groups.insert("EV-NEW2".to_string(), cohort("KXFOO", &["EV-NEW2-A"]));

        let in_flight: HashSet<String> = HashSet::from(["EV-LIVE".to_string()]);
        // max_units=2 with 1 in flight -> budget 1: arm NEW1 (BTreeMap order), cap NEW2.
        let plan = plan_arms(groups, &in_flight, dir.to_str().expect("utf8 tmp"), 2);
        assert_eq!(
            plan.arm,
            vec![("EV-NEW1".to_string(), cohort("KXFOO", &["EV-NEW1-A"]))]
        );
        assert_eq!(plan.capped, vec!["EV-NEW2".to_string()]);
    }

    #[test]
    fn new_market_on_a_later_sweep_gets_armed() {
        let dir = tmp("pickup");
        let mut sweep1 = BTreeMap::new();
        sweep1.insert("EV1".to_string(), cohort("KXFOO", &["EV1-A"]));
        let none: HashSet<String> = HashSet::new();
        let p1 = plan_arms(sweep1, &none, dir.to_str().expect("utf8 tmp"), 8);
        assert_eq!(p1.arm.len(), 1);

        // Sweep 2 re-discovers EV1 (now in flight) + newly-listed EV2.
        let mut sweep2 = BTreeMap::new();
        sweep2.insert("EV1".to_string(), cohort("KXFOO", &["EV1-A"]));
        sweep2.insert("EV2".to_string(), cohort("KXFOO", &["EV2-A"]));
        let in_flight: HashSet<String> = HashSet::from(["EV1".to_string()]);
        let p2 = plan_arms(sweep2, &in_flight, dir.to_str().expect("utf8 tmp"), 8);
        assert_eq!(
            p2.arm,
            vec![("EV2".to_string(), cohort("KXFOO", &["EV2-A"]))]
        );
        assert!(p2.capped.is_empty());
    }

    fn cli(input: &[&str]) -> crate::args::Args {
        crate::args::Args::parse(input.iter().map(|s| s.to_string()))
    }

    #[tokio::test]
    async fn run_universe_requires_series() {
        let err = run_universe(&cli(&["capture-universe", "--name", "x"]))
            .await
            .expect_err("must require --series");
        assert!(err.to_string().contains("--series"), "got: {err}");
    }

    #[tokio::test]
    async fn run_universe_requires_name() {
        let err = run_universe(&cli(&["capture-universe", "--series", "KXFOO"]))
            .await
            .expect_err("must require --name");
        assert!(err.to_string().contains("--name"), "got: {err}");
    }
}
