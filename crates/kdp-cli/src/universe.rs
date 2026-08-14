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
use chrono::{DateTime, Utc};
use kdp_kalshi::auth::KalshiCredentials;
use kdp_kalshi::rest::{list_markets_page, Market};
use tracing::{info, warn};

use crate::event_time::parse_event_start;
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
    pub(crate) open: Option<DateTime<Utc>>, // earliest market open_time
    pub(crate) close: Option<DateTime<Utc>>, // latest market close_time
    pub(crate) inferred_start: Option<DateTime<Utc>>, // from the event ticker (ET->UTC)
}

/// A contract's expiry cadence (hourly/daily/weekly/monthly-or-longer/unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cadence {
    Hourly,
    Daily,
    Weekly,
    Long,
    Other,
}

/// Classify cadence by LIFETIME ONLY (open->close duration), per the design's
/// P2.2 (no strike-count signal, no ticker parsing). Tolerant windows;
/// anything unclassifiable is `Other` (armed normally).
pub(crate) fn cadence(open: Option<DateTime<Utc>>, close: Option<DateTime<Utc>>) -> Cadence {
    let (Some(o), Some(c)) = (open, close) else {
        return Cadence::Other;
    };
    let mins = (c - o).num_minutes();
    // Measured (2026-08-01, live KXBTCD): the normal daily is exactly 1500 min
    // (25h, ET-anchored at both ends year-round) and the DST fall-back-day
    // daily is 1560 min (26h, real 2025 contract) -- so the Daily top edge
    // carries headroom past both, or the annual fall-back contract silently
    // classifies `Other` and arms at listing, defeating substitution with no
    // warn. Weekly gets the symmetric margin (measured 10140; fall-back week
    // computes 10200).
    match mins {
        50..=70 => Cadence::Hourly,
        1380..=1620 => Cadence::Daily, // ~23h .. 27h (25h normal, 26h DST fall-back)
        9360..=10860 => Cadence::Weekly, // ~6.5d .. ~7.5d incl. fall-back week
        m if m > 10860 => Cadence::Long, // monthly/annual scale
        _ => Cadence::Other,
    }
}

/// Parse an RFC3339 timestamp string, tolerating absence/malformed input as
/// `None` — never a panic (hard rule: no unwrap/expect outside tests).
fn parse_rfc3339(s: Option<&str>) -> Option<DateTime<Utc>> {
    s.and_then(|s| s.parse::<DateTime<Utc>>().ok())
}

/// Drop markets whose ticker was already seen, keeping the FIRST occurrence.
///
/// The per-status sweeps are concatenated, and Kalshi's status indexes are not
/// disjoint at a transition: a cohort swept seconds after it opens can come
/// back from BOTH the `open` and the (still stale) `unopened` listing, and the
/// merged vector then subscribes every ticker twice against one WS connection.
/// Observed live 2026-08-08T21:00:14Z — a restart landing on the open boundary
/// armed KXETHD-26AUG0818 with `tickers=600` where every later hour was 300.
///
/// First-occurrence wins deliberately: `--status` is swept in the order given
/// (`open,unopened`), so the freshest view of a transitioning market — the one
/// that already reports `active` — is the one kept.
pub(crate) fn dedup_by_ticker(markets: &mut Vec<Market>) {
    let mut seen: HashSet<String> = HashSet::new();
    markets.retain(|m| seen.insert(m.ticker.as_str().to_string()));
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
        let entry = groups.entry(event.clone()).or_insert_with(|| Cohort {
            series: series.to_string(),
            tickers: Vec::new(),
            open: None,
            close: None,
            inferred_start: parse_event_start(&event),
        });
        let o = parse_rfc3339(m.open_time.as_deref());
        let c = parse_rfc3339(m.close_time.as_deref());
        entry.open = match (entry.open, o) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        entry.close = match (entry.close, c) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        entry.tickers.push(m.ticker.as_str().to_string());
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
    /// Waiting on the arm gate / clash window this sweep; consumes no budget.
    pub(crate) deferred: Vec<String>,
    /// Never-armed this run (Long cadence, or Daily/Weekly with clash-sub off).
    pub(crate) skipped: Vec<String>,
    /// Windowed run + unparseable start (Other cadence). Also present in
    /// `arm` when the budget admits it, else in `capped` -- the fallback warn
    /// fires either way (pushed before the budget check, deliberately).
    pub(crate) no_start: Vec<String>,
}

/// Admission-gate parameters for one sweep's `plan_arms` call.
pub(crate) struct GateCfg {
    pub(crate) now: DateTime<Utc>,
    pub(crate) arm_lead: chrono::Duration,
    pub(crate) clash_sub: bool,
    /// The run's bound (`--until`/`--for`), if any. Affects the `no_start`
    /// loud-fallback warn here; the arming loop separately uses it to cap
    /// each spawned unit's `max_secs` at time-to-bound (design 1.3).
    pub(crate) until: Option<DateTime<Utc>>,
}

/// Resolve `--until <RFC3339 | YYYY-MM-DD>` / `--for <duration>` into one
/// absolute UTC bound. A bare date is INCLUSIVE (bound = next UTC midnight).
/// `--for` resolves at launch so a restart recomputes time-remaining and can
/// never extend the window (design 1.3, pinned). Mutually exclusive with
/// `--until`; both absent -> `Ok(None)` (unwindowed, today's behavior).
pub(crate) fn parse_window(
    args: &crate::args::Args,
    now: DateTime<Utc>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let until = match (args.get("until"), args.get("for")) {
        (Some(_), Some(_)) => anyhow::bail!("--until and --for are mutually exclusive"),
        (None, None) => return Ok(None),
        (Some(s), None) => match s.parse::<DateTime<Utc>>() {
            Ok(dt) => dt,
            Err(_) => {
                let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.succ_opt())
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .context(format!("--until must be RFC3339 or YYYY-MM-DD (got {s})"))?;
                DateTime::<Utc>::from_naive_utc_and_offset(d, Utc)
            }
        },
        (None, Some(s)) => {
            let d = crate::capture::parse_duration(s)
                .context("--for must be a duration like 14d / 36h")?;
            if d.is_zero() {
                anyhow::bail!("--for must be a positive duration");
            }
            let delta = chrono::Duration::from_std(d).context("--for is out of range")?;
            now.checked_add_signed(delta)
                .context("--for is out of range")?
        }
    };
    // A bound already in the past is NOT an error here: semantically it is
    // "bound reached", the pinned legitimate exit-0 category. run_universe
    // handles it as a loud clean no-op start -- under Restart=on-failure an
    // Err here would crash-loop an enabled windowed instance forever after a
    // post-bound reboot (StartLimitIntervalSec=0 disables the rate limiter).
    // A MALFORMED --until/--for still fails hard above.
    Ok(Some(until))
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
    gate: &GateCfg,
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

        // --- admission gate (arm timing / clash-slot substitution) ---
        let mut is_no_start = false;
        if let Some(start) = cohort.inferred_start {
            if gate.now < start - gate.arm_lead {
                plan.deferred.push(event);
                continue;
            }
        } else {
            let cad = cadence(cohort.open, cohort.close);
            match cad {
                Cadence::Daily | Cadence::Weekly if gate.clash_sub => {
                    // Substitution: capture the owning contract's EXPIRY hour only.
                    match cohort.close {
                        Some(c) if gate.now >= c - chrono::Duration::minutes(70) => {}
                        Some(_) => {
                            plan.deferred.push(event);
                            continue;
                        }
                        None => {} // no close known: arm (tolerant, never a silent hole)
                    }
                }
                Cadence::Daily | Cadence::Weekly => {
                    plan.skipped.push(event); // clash-sub off: the slot hole stays
                    continue;
                }
                Cadence::Long => {
                    plan.skipped.push(event); // monthly/annual: loud skip per design
                    continue;
                }
                Cadence::Hourly | Cadence::Other => {
                    // Arm `--arm-lead-min` BEFORE the market opens, off the
                    // API's open_time. The sweep now includes `unopened`, so a
                    // cohort is visible ~1.6 days early; without this gate it
                    // would arm at listing and hold a unit slot for its whole
                    // pre-open life. Gating here (rather than filtering the
                    // sweep) is forced: Kalshi has no "opening soon" filter --
                    // min/max_close_ts pair only with `closed`, and
                    // `unopened`/`open` pair with CREATION time, which does not
                    // track open time.
                    //
                    // Deliberately NOT applied to the inferred_start branch
                    // above: for a match cohort `open` is the listing date, not
                    // the start (KXWT20MATCH-26JUN121330SRIENG opens 06-01 and
                    // plays 06-12), so gating it on `open` would arm it eleven
                    // days early. The two branches stay separate.
                    //
                    // A cohort with no known open time has no gate: arm it
                    // (tolerant -- a missed cohort is unrecoverable, ADR-003).
                    if let Some(o) = cohort.open {
                        if gate.now < o - gate.arm_lead {
                            plan.deferred.push(event);
                            continue;
                        }
                    }
                    if cad == Cadence::Other && gate.until.is_some() {
                        is_no_start = true; // windowed loud fallback
                    }
                }
            }
        }

        if is_no_start {
            // Pushed unconditionally: even if budget caps this event below, the
            // loud fallback still fired and still deserves its warn-once.
            plan.no_start.push(event.clone());
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

/// The universe Drive namespace: `$KDP_RCLONE_REMOTE/universe-<name>`.
///
/// kdp-archive.sh / kdp-checkpoint.sh take the prefix WHOLESALE as the rclone
/// destination, and rclone treats a colon-less path as LOCAL -- so a bare
/// `universe-<name>` would silently archive to the server filesystem. Join
/// under the env remote, and refuse to start when a command that uploads is
/// configured but the remote is unset.
pub(crate) fn resolve_remote_prefix(
    env_remote: Option<&str>,
    name: &str,
    archive_cmd: &str,
    checkpoint_cmd: &str,
) -> anyhow::Result<Option<String>> {
    match env_remote.map(str::trim).filter(|s| !s.is_empty()) {
        Some(r) => Ok(Some(format!("{}/universe-{name}", r.trim_end_matches('/')))),
        None if archive_cmd.is_empty() && checkpoint_cmd.is_empty() => Ok(None),
        None => anyhow::bail!(
            "KDP_RCLONE_REMOTE must be set when --archive-cmd/--checkpoint-cmd are \
             enabled (the universe namespace is $KDP_RCLONE_REMOTE/universe-<name>); \
             set it, or pass --archive-cmd \"\" for a local-only run"
        ),
    }
}

/// `capture-universe --series A,B,C --name NAME [--status open,unopened] [--min-volume 0]
///  [--rediscover-interval 300] [--max-units 8] [--out DIR] [--max-hours 8]
///  [--grace 180] [--poll 30] [--idle 45] [--buffer 8192] [--archive-cmd PATH]
///  [--until DATE|RFC3339] [--for DURATION]`
///
/// Forever (until shutdown, or until an optional `--until`/`--for` window
/// bound is reached): sweep each series, arm every new settlement cohort up
/// to --max-units, sleep min(--rediscover-interval, time-to-bound), repeat.
/// The re-sweep loop IS the park. **Exit-code discipline (pinned, design
/// 1.4): this function returns `Ok(())` (exit 0) ONLY on (a) an explicit
/// shutdown signal or (b) the window bound being reached** — three `break`
/// sites (the top-of-loop shutdown check, the window-bound check, and the
/// interruptible-sleep wakeup, which is itself shutdown-only) collapsing
/// into those same two exit categories, all falling through to the single
/// tail `Ok(())` below, plus one early return for a bound already past AT
/// LAUNCH (still category (b): a rebooted, still-enabled windowed instance
/// must no-op cleanly, not crash-loop under Restart=on-failure). Any OTHER termination path must be an `Err` (nonzero exit): under `Restart=always`
/// a stray `Ok(())` is a restart loop (2026-06-28 post-mortem), and a
/// windowed run additionally needs a clean nonzero signal to distinguish "the
/// process crashed" from "the window finished" for anything watching the
/// exit code.
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
    // Default includes `unopened` so a cohort is discovered BEFORE it opens and
    // the --arm-lead-min gate can arm it ahead of the first tick. With `open`
    // alone a cohort was invisible until the instant it opened, so the earliest
    // possible arm was the next sweep boundary and the first orderbook write
    // landed ~4-5 min into every hourly market's life (measured 2026-08-08).
    // Kalshi permits only ONE status per request; the sweep loop already issues
    // one request per status, so a comma list is the right shape. Pass
    // `--status open` to restore the old behaviour.
    let statuses: Vec<String> = args
        .get_or("status", "open,unopened")
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
        checkpoint_cmd: args.get_or("checkpoint-cmd", "").to_string(),
        verify_interval_secs: crate::capture::parse_verify_interval(args)?,
    };
    // Clamped to [0, one week]: chrono::Duration::minutes PANICS on i64
    // millisecond overflow (hard rule: no implicit panic paths), and an arm
    // lead beyond a week is operator error anyway.
    let arm_lead_min: i64 = args
        .get_or("arm-lead-min", "30")
        .parse()
        .unwrap_or(30)
        .clamp(0, 10_080);
    let clash_sub = match args.get_or("clash-sub", "on") {
        "on" => true,
        "off" => false,
        other => anyhow::bail!("--clash-sub must be \"on\" or \"off\", got {other:?}"),
    };
    let until = parse_window(args, Utc::now())?;
    if let Some(u) = until {
        if u <= Utc::now() {
            warn!(
                until = %u.to_rfc3339(),
                "window bound already passed at launch; bound reached -- exiting cleanly (no capture)"
            );
            return Ok(());
        }
    }
    // Placed after flag validation (--clash-sub, --until/--for) so a bad flag
    // is reported as itself, not masked by an unrelated env/config bail here.
    let remote_prefix = resolve_remote_prefix(
        std::env::var("KDP_RCLONE_REMOTE").ok().as_deref(),
        name,
        &cfg.archive_cmd,
        &cfg.checkpoint_cmd,
    )?;

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
    // Warn-once sets: a 300s-ish sweep loop would otherwise repeat the same
    // warn every pass for a still-skipped/still-fallback-armed event.
    let mut warned_skip: HashSet<String> = HashSet::new();
    let mut warned_no_start: HashSet<String> = HashSet::new();

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        if let Some(u) = until {
            if Utc::now() >= u {
                info!(until = %u.to_rfc3339(), "capture window bound reached; draining");
                break;
            }
        }
        reap_named(&mut inflight).await;

        // Sweep: one paginated listing per (series, status). A failed sweep is a
        // warn + retry next interval, never an abort (capture must keep running).
        let mut groups: BTreeMap<String, Cohort> = BTreeMap::new();
        for s in &series {
            let mut markets: Vec<Market> = Vec::new();
            for st in &statuses {
                match sweep_series(&client, s, st).await {
                    Ok(mut ms) => {
                        // An unopened market has no volume by construction, so
                        // applying --min-volume to that sweep would silently
                        // discard every pre-open cohort and quietly undo
                        // pre-open arming. Liquidity is judged once a market
                        // trades; the `open` sweep still applies the filter.
                        if st != "unopened" {
                            ms.retain(|m| m.volume() >= min_volume);
                        }
                        markets.append(&mut ms);
                    }
                    Err(e) => {
                        warn!(series = %s, status = %st, error = %e, "sweep failed; retrying next interval")
                    }
                }
            }
            dedup_by_ticker(&mut markets);
            // Merge this series' cohorts into the sweep. Safe against cross-series
            // key collision because Kalshi event tickers are always series-prefixed
            // (KXWT20MATCH-..., KXIPLGAME-..., KXBTCD-...), so two distinct --series
            // can't produce the same event key. If Kalshi ever breaks that, switch
            // to a (series, event) composite key.
            groups.extend(group_by_event(s, &markets));
        }

        let in_flight_names: HashSet<String> = inflight.iter().map(|(n, _)| n.clone()).collect();
        let gate = GateCfg {
            now: Utc::now(),
            arm_lead: chrono::Duration::minutes(arm_lead_min),
            clash_sub,
            until,
        };
        let plan = plan_arms(groups, &in_flight_names, &cfg.data_dir, max_units, &gate);
        for event in &plan.capped {
            warn!(
                event = %event, max_units,
                "universe cap reached; NOT capturing this event -- raise --max-units or shard"
            );
        }
        for event in &plan.skipped {
            if warned_skip.insert(event.clone()) {
                warn!(event = %event, "cohort cadence not captured (monthly/annual, or clash-sub off); skipping");
            }
        }
        for event in &plan.no_start {
            if warned_no_start.insert(event.clone()) {
                warn!(event = %event, "windowed run but no start time inferred from the event ticker; arming at listing (budget permitting)");
            }
        }
        if !plan.deferred.is_empty() {
            info!(
                deferred = plan.deferred.len(),
                "cohorts waiting on arm gate / clash window"
            );
        }
        for (session, cohort) in plan.arm {
            info!(session = %session, tickers = cohort.tickers.len(), series = %cohort.series, "arming universe event");
            let unit = CaptureUnit {
                session: session.clone(),
                tickers: cohort.tickers,
                event: session.clone(),
                remote_prefix: remote_prefix.clone(),
            };
            let creds2 = Arc::clone(&creds);
            let client2 = client.clone();
            let mut cfg2 = cfg.clone();
            if let Some(u) = until {
                // Deliberate (design 1.3, pinned): the window IS the unit bound,
                // so it overrides --max-hours outright rather than being min()'d
                // against cfg.max_secs -- a min() would let a shorter --max-hours
                // silently reintroduce the same backstop the window is meant to
                // replace. Settlement can still end a unit earlier than either.
                // Non-windowed runs keep today's --max-hours backstop unchanged
                // (this branch never runs when `until` is None).
                cfg2.max_secs = (u - Utc::now()).num_seconds().max(60) as u64;
            }
            let srx = shutdown_rx.clone();
            inflight.push((
                session,
                tokio::spawn(async move {
                    run_capture_unit(creds2, &client2, &unit, &cfg2, srx).await;
                }),
            ));
        }

        let sleep_secs = match until {
            Some(u) => {
                // Ceil, not floor: flooring the remaining time to the bound
                // would land the sleep short and wake one junk sweep still
                // before the bound instead of at/past it.
                let to_bound = ((u - Utc::now()).num_milliseconds() + 999) / 1000;
                let to_bound = to_bound.max(1) as u64;
                rediscover.min(to_bound)
            }
            None => rediscover,
        };
        if interruptible_sleep(Duration::from_secs(sleep_secs), &shutdown_rx).await {
            break;
        }
    }

    // Shutdown: bound the drain of whatever is still capturing, then exit.
    drain_inflight(inflight.into_iter().map(|(_, h)| h).collect()).await;
    info!("universe supervisor stopped");
    Ok(())
}

/// One paginated `/markets` sweep for a series + status. The cursor is NOT
/// persisted across sweeps (each sweep re-lists from the top), so if a series
/// ever genuinely exceeds the page cap, everything past it would be invisible
/// every sweep — a silent truncation. We refuse to be silent: hitting the page
/// cap with more pages pending emits a `warn!` (raise `MAX_PAGES` or narrow the
/// filter).
async fn sweep_series(
    client: &reqwest::Client,
    series: &str,
    status: &str,
) -> anyhow::Result<Vec<Market>> {
    // Measured 2026-08-08 on the live box: with `unopened` in the sweep,
    // KXBTC lists 6,848 unopened markets (7 pages) and KXETHD 10,840 (11) --
    // past the old bound of 10, which would have truncated KXETHD from day one.
    // 30 pages = 30k markets per (series, status) leaves real headroom as the
    // listed lookahead grows. Open sets are one page each (318 / 390).
    const MAX_PAGES: u32 = 30;
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    for page in 1..=MAX_PAGES {
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
            None => {
                info!(series = %series, status = %status, pages = page, markets = all.len(), "sweep complete");
                return Ok(all);
            }
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
            open: None,
            close: None,
            inferred_start: None,
        }
    }

    /// A deterministic gate for a fixed `now` (the established idiom: no
    /// wall-clock reads in tests). Defaults: 30m arm lead, clash-sub on, no
    /// window bound.
    fn gate(now: &str) -> GateCfg {
        GateCfg {
            now: now.parse().unwrap(),
            arm_lead: chrono::Duration::minutes(30),
            clash_sub: true,
            until: None,
        }
    }

    fn groups_of(entries: Vec<(&str, Cohort)>) -> BTreeMap<String, Cohort> {
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    fn cohort_t(
        series: &str,
        tickers: &[&str],
        open: Option<&str>,
        close: Option<&str>,
        start: Option<&str>,
    ) -> Cohort {
        Cohort {
            series: series.to_string(),
            tickers: tickers.iter().map(|s| s.to_string()).collect(),
            open: open.map(|s| s.parse().unwrap()),
            close: close.map(|s| s.parse().unwrap()),
            inferred_start: start.map(|s| s.parse().unwrap()),
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

    fn mk_t(ticker: &str, event: Option<&str>, status: &str, open: &str, close: &str) -> Market {
        let mut v = serde_json::json!({
            "ticker": ticker, "status": status, "open_time": open, "close_time": close
        });
        if let Some(ev) = event {
            v["event_ticker"] = serde_json::Value::String(ev.to_string());
        }
        serde_json::from_value(v).expect("test market")
    }

    #[test]
    fn dedup_keeps_the_first_sighting_of_a_ticker() {
        // The live shape: an `open` sweep and a stale `unopened` sweep return
        // the same cohort, concatenated in --status order.
        let mut ms = vec![
            mk("EV1-A", Some("EV1"), "active"),
            mk("EV1-B", Some("EV1"), "active"),
            mk("EV1-A", Some("EV1"), "initialized"),
            mk("EV1-B", Some("EV1"), "initialized"),
        ];
        dedup_by_ticker(&mut ms);
        assert_eq!(ms.len(), 2);
        // First-occurrence wins: the `active` view survives, not the stale one.
        assert!(ms.iter().all(|m| m.status.as_deref() == Some("active")));
        let cohorts = group_by_event("KXFOO", &ms);
        assert_eq!(cohorts["EV1"].tickers.len(), 2);
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
        let plan = plan_arms(
            groups,
            &none,
            dir.to_str().expect("utf8 tmp"),
            8,
            &gate("2026-08-01T00:00:00+00:00"),
        );
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
        let plan = plan_arms(
            groups,
            &in_flight,
            dir.to_str().expect("utf8 tmp"),
            2,
            &gate("2026-08-01T00:00:00+00:00"),
        );
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
        let p1 = plan_arms(
            sweep1,
            &none,
            dir.to_str().expect("utf8 tmp"),
            8,
            &gate("2026-08-01T00:00:00+00:00"),
        );
        assert_eq!(p1.arm.len(), 1);

        // Sweep 2 re-discovers EV1 (now in flight) + newly-listed EV2.
        let mut sweep2 = BTreeMap::new();
        sweep2.insert("EV1".to_string(), cohort("KXFOO", &["EV1-A"]));
        sweep2.insert("EV2".to_string(), cohort("KXFOO", &["EV2-A"]));
        let in_flight: HashSet<String> = HashSet::from(["EV1".to_string()]);
        let p2 = plan_arms(
            sweep2,
            &in_flight,
            dir.to_str().expect("utf8 tmp"),
            8,
            &gate("2026-08-01T00:00:00+00:00"),
        );
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

    #[test]
    fn cadence_classifies_by_lifetime_only() {
        let t = |s: &str| {
            DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .ok()
        };
        let c = |o: &str, cl: &str| cadence(t(o), t(cl));
        assert_eq!(
            c("2026-08-01T17:00:00Z", "2026-08-01T18:00:00Z"),
            Cadence::Hourly
        );
        assert_eq!(
            c("2026-08-01T21:00:00Z", "2026-08-02T21:00:00Z"),
            Cadence::Daily
        );
        assert_eq!(
            c("2026-07-31T21:00:00Z", "2026-08-07T21:00:00Z"),
            Cadence::Weekly
        );
        assert_eq!(
            c("2026-08-01T00:00:00Z", "2026-08-31T00:00:00Z"),
            Cadence::Long
        ); // monthly
        assert_eq!(
            c("2026-08-01T12:00:00Z", "2026-08-01T15:00:00Z"),
            Cadence::Other
        ); // 3h match
        assert_eq!(cadence(None, None), Cadence::Other); // unknown -> tolerant default

        // Review I-1 regression: the DST fall-back-day daily spans 26h (1560
        // min, measured on the real KXBTCD-25NOV0217: 2025-11-01T20:00Z ->
        // 2025-11-02T22:00Z) and MUST classify Daily -- `Other` would arm it
        // at listing and silently defeat clash-slot substitution once a year.
        assert_eq!(
            c("2026-10-31T20:00:00Z", "2026-11-01T22:00:00Z"),
            Cadence::Daily
        );
        // The normal daily rides 1500 min exactly (25h, measured live) -- the
        // top edge must keep admitting it with room to spare.
        assert_eq!(
            c("2026-08-01T20:00:00Z", "2026-08-02T21:00:00Z"),
            Cadence::Daily
        );
        // Fall-back week: 10200 min (7d + 2h) stays Weekly.
        assert_eq!(
            c("2026-10-27T21:00:00Z", "2026-11-03T23:00:00Z"),
            Cadence::Weekly
        );
    }

    #[test]
    fn group_by_event_carries_lifecycle_metadata() {
        let ms = vec![
            mk_t(
                "KXBTCD-26JUL3118-T60000",
                Some("KXBTCD-26JUL3118"),
                "active",
                "2026-07-31T17:00:00Z",
                "2026-07-31T18:00:00Z",
            ),
            mk_t(
                "KXBTCD-26JUL3118-T61000",
                Some("KXBTCD-26JUL3118"),
                "active",
                "2026-07-31T16:55:00Z",
                "2026-07-31T18:00:00Z",
            ),
        ];
        let g = group_by_event("KXBTCD", &ms);
        let c = &g["KXBTCD-26JUL3118"];
        // earliest open, latest close, no inferred start (2-digit expiry hour)
        assert_eq!(
            c.open.map(|d| d.to_rfc3339()),
            Some("2026-07-31T16:55:00+00:00".into())
        );
        assert_eq!(
            c.close.map(|d| d.to_rfc3339()),
            Some("2026-07-31T18:00:00+00:00".into())
        );
        assert_eq!(c.inferred_start, None);
    }

    #[test]
    fn group_by_event_infers_match_start_from_the_event_key() {
        use chrono::TimeZone;
        let ms = vec![mk_t(
            "KXWT20MATCH-26JUN121330SRIENG-SRI",
            Some("KXWT20MATCH-26JUN121330SRIENG"),
            "active",
            "2026-06-01T00:00:00Z",
            "2026-06-14T21:30:00Z",
        )];
        let g = group_by_event("KXWT20MATCH", &ms);
        assert_eq!(
            g["KXWT20MATCH-26JUN121330SRIENG"].inferred_start,
            Some(Utc.with_ymd_and_hms(2026, 6, 12, 17, 30, 0).unwrap())
        );
    }

    #[test]
    fn match_cohort_defers_until_start_minus_lead_then_arms() {
        let dir = tmp("armgate");
        let c = cohort_t(
            "KXWT20MATCH",
            &["KXWT20MATCH-26JUN121330SRIENG-SRI"],
            Some("2026-06-01T00:00:00Z"),
            Some("2026-06-14T21:30:00Z"),
            Some("2026-06-12T17:30:00Z"),
        );
        let g = |now| {
            plan_arms(
                groups_of(vec![("KXWT20MATCH-26JUN121330SRIENG", c.clone())]),
                &HashSet::new(),
                dir.to_str().unwrap(),
                8,
                &gate(now),
            )
        };
        let before = g("2026-06-12T16:59:00+00:00"); // lead is 30m; 17:00 is the edge
        assert!(before.arm.is_empty());
        assert_eq!(
            before.deferred,
            vec!["KXWT20MATCH-26JUN121330SRIENG".to_string()]
        );
        let due = g("2026-06-12T17:00:00+00:00");
        assert_eq!(due.arm.len(), 1);
        assert!(due.deferred.is_empty());
    }

    #[test]
    fn hourly_cohort_defers_until_open_minus_lead_then_arms() {
        // The 4-5 minute hole, closed. --status now includes `unopened`, so an
        // hourly cohort is visible ~1.6 days before it opens; without this gate
        // it would arm at listing and hold a slot for its entire pre-open life.
        // Measured before the fix (KXBTC-26AUG0809): open 12:00:00Z, armed
        // 12:01:45Z, first orderbook write 12:04:11Z -- every cohort, every
        // hour, since the universe went live.
        let dir = tmp("openlead");
        let c = cohort_t(
            "KXBTC",
            &["KXBTC-26AUG0812-T60000"],
            Some("2026-08-08T12:00:00Z"),
            Some("2026-08-08T13:00:00Z"),
            None,
        );
        let g = |now| {
            plan_arms(
                groups_of(vec![("KXBTC-26AUG0812", c.clone())]),
                &HashSet::new(),
                dir.to_str().expect("utf8 tmp"),
                8,
                &gate(now),
            )
        };
        // Listed a day early: deferred, and it must not consume budget.
        let early = g("2026-08-07T12:00:00+00:00");
        assert!(early.arm.is_empty());
        assert_eq!(early.deferred, vec!["KXBTC-26AUG0812".to_string()]);
        // One second before the edge (lead is 30m; the edge is 11:30:00Z).
        let before = g("2026-08-08T11:29:59+00:00");
        assert!(
            before.arm.is_empty(),
            "one second before the edge must DEFER"
        );
        // Exactly at the edge: armed, 30 minutes AHEAD of open.
        let edge = g("2026-08-08T11:30:00+00:00");
        assert_eq!(edge.arm.len(), 1, "exactly open-30min must ARM");
        // And still armed once open (the restart / late-discovery case).
        assert_eq!(g("2026-08-08T12:10:00+00:00").arm.len(), 1);
    }

    #[test]
    fn open_gate_never_applies_to_a_cohort_with_an_inferred_start() {
        // Correction to D2: `open` and `inferred_start` are DIFFERENT times for
        // match cohorts. This one opens 2026-06-01 and plays 2026-06-12T17:30Z.
        // Gating on `open` would arm it eleven days early and hold a slot
        // through a dead book. The inferred-start branch must win outright.
        let dir = tmp("inferredwins");
        let c = cohort_t(
            "KXWT20MATCH",
            &["KXWT20MATCH-26JUN121330SRIENG-SRI"],
            Some("2026-06-01T00:00:00Z"),
            Some("2026-06-14T21:30:00Z"),
            Some("2026-06-12T17:30:00Z"),
        );
        let plan = plan_arms(
            groups_of(vec![("KXWT20MATCH-26JUN121330SRIENG", c)]),
            &HashSet::new(),
            dir.to_str().expect("utf8 tmp"),
            8,
            &gate("2026-06-01T00:00:00+00:00"),
        );
        assert!(
            plan.arm.is_empty(),
            "open_time must not arm a match cohort; its inferred start governs"
        );
        assert_eq!(
            plan.deferred,
            vec!["KXWT20MATCH-26JUN121330SRIENG".to_string()]
        );
    }

    #[test]
    fn a_cohort_with_no_known_open_still_arms() {
        // Tolerant by design (ADR-003: never a silent hole). No open time means
        // no gate -- arm and capture.
        let dir = tmp("noopen");
        let c = cohort("KXFOO", &["KXFOO-EV1-A"]); // open/close/start all None
        let plan = plan_arms(
            groups_of(vec![("KXFOO-EV1", c)]),
            &HashSet::new(),
            dir.to_str().expect("utf8 tmp"),
            8,
            &gate("2026-08-08T12:00:00+00:00"),
        );
        assert_eq!(plan.arm.len(), 1);
    }

    #[test]
    fn daily_cohort_is_deferred_until_its_final_hour_then_substituted() {
        let dir = tmp("clash");
        // Daily: 21:00Z Jul 31 -> 21:00Z Aug 1 (5PM ET clash slot).
        let c = cohort_t(
            "KXBTCD",
            &["KXBTCD-26AUG01-T60000"],
            Some("2026-07-31T21:00:00Z"),
            Some("2026-08-01T21:00:00Z"),
            None,
        );
        let g = |now, sub| {
            let mut ga = gate(now);
            ga.clash_sub = sub;
            plan_arms(
                groups_of(vec![("KXBTCD-26AUG01", c.clone())]),
                &HashSet::new(),
                dir.to_str().unwrap(),
                8,
                &ga,
            )
        };
        // Mid-life: deferred, not armed, not capped.
        let mid = g("2026-08-01T12:00:00+00:00", true);
        assert!(mid.arm.is_empty() && mid.capped.is_empty());
        assert_eq!(mid.deferred, vec!["KXBTCD-26AUG01".to_string()]);
        // Final hour (>= close - 70min): armed.
        let fin = g("2026-08-01T19:55:00+00:00", true);
        assert_eq!(fin.arm.len(), 1);
        // Boundary-exact: close - 70min is the arm edge itself.
        let edge_arm = g("2026-08-01T19:50:00+00:00", true);
        assert_eq!(edge_arm.arm.len(), 1, "exactly close-70min must ARM");
        // One second earlier: still deferred.
        let edge_defer = g("2026-08-01T19:49:59+00:00", true);
        assert!(
            edge_defer.arm.is_empty(),
            "one second before the edge must DEFER"
        );
        assert_eq!(edge_defer.deferred, vec!["KXBTCD-26AUG01".to_string()]);
        // Opt-out: never armed, listed as skipped.
        let off = g("2026-08-01T19:55:00+00:00", false);
        assert!(off.arm.is_empty());
        assert_eq!(off.skipped, vec!["KXBTCD-26AUG01".to_string()]);
    }

    #[test]
    fn long_cadence_is_skipped_and_hourly_arms_at_listing() {
        let dir = tmp("longskip");
        let monthly = cohort_t(
            "KXBTC",
            &["KXBTC-26AUG-T60000"],
            Some("2026-08-01T00:00:00Z"),
            Some("2026-08-31T21:00:00Z"),
            None,
        );
        let hourly = cohort_t(
            "KXBTCD",
            &["KXBTCD-26AUG0118-T60000"],
            Some("2026-08-01T17:00:00Z"),
            Some("2026-08-01T18:00:00Z"),
            None,
        );
        let plan = plan_arms(
            groups_of(vec![("KXBTC-26AUG", monthly), ("KXBTCD-26AUG0118", hourly)]),
            &HashSet::new(),
            dir.to_str().unwrap(),
            8,
            &gate("2026-08-01T17:05:00+00:00"),
        );
        assert_eq!(plan.arm.len(), 1);
        assert_eq!(plan.arm[0].0, "KXBTCD-26AUG0118");
        assert_eq!(plan.skipped, vec!["KXBTC-26AUG".to_string()]);
    }

    #[test]
    fn windowed_run_warns_once_for_unparseable_start_but_still_arms() {
        let dir = tmp("nostart");
        // A 3h "Other"-cadence cohort with no inferred start, under a window.
        let c = cohort_t(
            "KXODD",
            &["KXODD-26AUGXX-A"],
            Some("2026-08-01T12:00:00Z"),
            Some("2026-08-01T15:00:00Z"),
            None,
        );
        let mut ga = gate("2026-08-01T12:30:00+00:00");
        ga.until = Some("2026-08-10T00:00:00+00:00".parse().unwrap());
        let plan = plan_arms(
            groups_of(vec![("KXODD-26AUGXX", c)]),
            &HashSet::new(),
            dir.to_str().unwrap(),
            8,
            &ga,
        );
        assert_eq!(plan.arm.len(), 1);
        assert_eq!(plan.no_start, vec!["KXODD-26AUGXX".to_string()]);
    }

    #[test]
    fn deferred_cohorts_do_not_consume_budget() {
        let dir = tmp("budget");
        // Event key deliberately sorts BEFORE the due one ("KXBTCD-26AUG01" is
        // a strict string prefix of "KXBTCD-26AUG0118"), so BTreeMap iteration
        // processes the deferred cohort FIRST. That makes this test actually
        // prove deferral skips budget consumption -- with the due cohort
        // processed first (as the un-renamed keys sorted), it would grab the
        // single budget slot regardless of what the deferred cohort does,
        // making the assertion below vacuous.
        let deferred = cohort_t(
            "KXBTCD",
            &["KXBTCD-26AUG01-T1"],
            Some("2026-08-01T21:00:00Z"),
            Some("2026-08-02T21:00:00Z"),
            None,
        );
        let due = cohort_t(
            "KXBTCD",
            &["KXBTCD-26AUG0118-T1"],
            Some("2026-08-01T17:00:00Z"),
            Some("2026-08-01T18:00:00Z"),
            None,
        );
        // max_units = 1: the deferred daily must not eat the one slot.
        let plan = plan_arms(
            groups_of(vec![
                ("KXBTCD-26AUG01", deferred),
                ("KXBTCD-26AUG0118", due),
            ]),
            &HashSet::new(),
            dir.to_str().unwrap(),
            1,
            &gate("2026-08-01T17:05:00+00:00"),
        );
        assert_eq!(plan.arm.len(), 1);
        assert_eq!(plan.arm[0].0, "KXBTCD-26AUG0118");
        assert!(plan.capped.is_empty());
    }

    #[tokio::test]
    async fn run_universe_rejects_bad_clash_sub_value() {
        let err = run_universe(&cli(&[
            "capture-universe",
            "--series",
            "KXBTCD",
            "--name",
            "t",
            "--clash-sub",
            "sideways",
        ]))
        .await
        .expect_err("must reject");
        assert!(err.to_string().contains("--clash-sub"));
    }

    #[test]
    fn parse_window_accepts_bare_date_as_end_of_utc_day() {
        let now: chrono::DateTime<Utc> = "2026-08-01T12:00:00+00:00".parse().unwrap();
        // A past date still PARSES (run_universe turns it into a clean
        // bound-reached no-op exit, review M-2); only malformed input errors.
        let w = parse_window(&cli(&["capture-universe", "--until", "2026-07-14"]), now)
            .unwrap()
            .unwrap();
        assert_eq!(w.to_rfc3339(), "2026-07-15T00:00:00+00:00");
        assert!(parse_window(&cli(&["capture-universe", "--until", "not-a-date"]), now).is_err());
        let w = parse_window(&cli(&["capture-universe", "--until", "2026-08-14"]), now)
            .unwrap()
            .unwrap();
        // inclusive-day: the bound is the NEXT midnight
        assert_eq!(w.to_rfc3339(), "2026-08-15T00:00:00+00:00");
    }

    #[test]
    fn parse_window_for_resolves_to_absolute_until() {
        let now: chrono::DateTime<Utc> = "2026-08-01T12:00:00+00:00".parse().unwrap();
        let w = parse_window(&cli(&["capture-universe", "--for", "14d"]), now)
            .unwrap()
            .unwrap();
        assert_eq!(w.to_rfc3339(), "2026-08-15T12:00:00+00:00");
    }

    #[test]
    fn parse_window_rejects_both_flags_and_none_is_ok() {
        let now: chrono::DateTime<Utc> = "2026-08-01T12:00:00+00:00".parse().unwrap();
        assert!(parse_window(
            &cli(&["capture-universe", "--until", "2026-08-14", "--for", "3d"]),
            now
        )
        .is_err());
        assert_eq!(
            parse_window(&cli(&["capture-universe"]), now).unwrap(),
            None
        );
    }

    #[test]
    fn remote_prefix_joins_under_the_env_remote() {
        assert_eq!(
            resolve_remote_prefix(
                Some("remote:kdp"),
                "crypto",
                "/opt/kdp/bin/kdp-archive.sh",
                ""
            )
            .unwrap(),
            Some("remote:kdp/universe-crypto".to_string())
        );
        // trailing slash + whitespace tolerated
        assert_eq!(
            resolve_remote_prefix(Some(" remote:kdp/ "), "crypto", "x", "").unwrap(),
            Some("remote:kdp/universe-crypto".to_string())
        );
    }

    #[test]
    fn remote_prefix_absent_env_is_fatal_only_when_a_cmd_needs_it() {
        // No archive, no checkpoint: dev-box smoke -- fine, no prefix.
        assert_eq!(resolve_remote_prefix(None, "crypto", "", "").unwrap(), None);
        // Archive enabled without the remote: refuse loudly (the silent-local-copy trap).
        let err = resolve_remote_prefix(None, "crypto", "/opt/kdp/bin/kdp-archive.sh", "")
            .expect_err("must refuse");
        assert!(err.to_string().contains("KDP_RCLONE_REMOTE"));
        // Same for checkpoint-only.
        assert!(
            resolve_remote_prefix(Some(""), "crypto", "", "/opt/kdp/bin/kdp-checkpoint.sh")
                .is_err()
        );
    }

    #[tokio::test]
    async fn run_universe_exits_cleanly_when_bound_already_passed() {
        // Review M-2: an enabled windowed instance rebooted after its bound
        // must be a single loud no-op start (bound reached => exit 0), not an
        // Err that Restart=on-failure + StartLimitIntervalSec=0 turns into an
        // infinite 5s crash loop. (Returns before creds are ever needed.)
        run_universe(&cli(&[
            "capture-universe",
            "--series",
            "KXBTCD",
            "--name",
            "t",
            "--until",
            "2020-01-01",
        ]))
        .await
        .expect("past bound must exit cleanly");
    }
}
