//! The declarative capture schedule format + loader.
//!
//! A schedule is a JSONL file, one [`ScheduleEntry`] per line — series-agnostic,
//! so the same format drives a cricket World Cup, a football World Cup, or
//! any future set of pre-scheduled events. Each entry is one capture job: when to
//! arm, which series, the predicted event ticker (if known) and/or the team codes
//! to resolve it by, and where to archive it. The `capture-scheduled` supervisor
//! reads this, sleeps until each entry's arm time, resolves its markets, and
//! captures through settlement.
//!
//! Parsing is line-oriented and torn-line tolerant (mirrors the backfill progress
//! reader): a single malformed line is logged + skipped, never aborting the load —
//! one bad entry must not strand a whole tournament.

use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Deserialize;
use tracing::{info, warn};

/// One scheduled capture job. `series` + `start_utc` are required; an entry must
/// also carry at least one of `event_ticker` or a non-empty `teams` so the
/// resolver has something to match on.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleEntry {
    /// Stable id for logs / dedup (e.g. `"cup-m1"`).
    pub id: String,
    /// Human label for logs (e.g. `"Alpha vs Beta"`).
    #[serde(default)]
    pub label: String,
    /// Series ticker the event lives under (e.g. `"KXCUPMATCH"`).
    pub series: String,
    /// Predicted event ticker, if confirmed. `None`/absent => discover by teams.
    #[serde(default)]
    pub event_ticker: Option<String>,
    /// Team codes for the discovery fallback + side identification (e.g.
    /// `["AAA","BBB"]`). Order-agnostic when matching.
    #[serde(default)]
    pub teams: Vec<String>,
    /// Scheduled start of play (UTC). Arm time = this minus the arm lead.
    pub start_utc: DateTime<Utc>,
    /// Minutes before `start_utc` to begin capturing. `None` => CLI/env default.
    #[serde(default)]
    pub arm_lead_min: Option<i64>,
    /// Hard backstop in hours (rain delays / overruns). `None` => CLI/env default.
    #[serde(default)]
    pub max_hours: Option<u64>,
    /// Storage-namespace prefix for this entry (e.g. `"remote:kdp/cup-2026"`).
    /// `None` => the archive script's own `KDP_RCLONE_REMOTE` setting.
    #[serde(default)]
    pub remote_prefix: Option<String>,
}

impl ScheduleEntry {
    /// The wall-clock moment to begin arming: `start_utc - arm_lead`. `default_lead`
    /// (minutes) is used when the entry does not override `arm_lead_min`.
    pub fn arm_at(&self, default_lead_min: i64) -> DateTime<Utc> {
        let lead = self.arm_lead_min.unwrap_or(default_lead_min).max(0);
        self.start_utc - ChronoDuration::minutes(lead)
    }

    /// True iff this entry is resolvable: it has a predicted ticker or team codes.
    pub fn is_resolvable(&self) -> bool {
        self.event_ticker.is_some() || !self.teams.is_empty()
    }
}

/// Load + validate a schedule file. Blank lines are skipped; a malformed or
/// unresolvable line is logged and dropped (never fatal). Returns the valid
/// entries in file order. A missing/unreadable file is a hard error (the operator
/// pointed `--schedule` at the wrong path).
pub fn load_schedule(path: &Path) -> anyhow::Result<Vec<ScheduleEntry>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading schedule file {}", path.display()))?;
    Ok(parse_schedule(&text))
}

/// Parse schedule JSONL text (pure; unit-tested). See [`load_schedule`].
///
/// Tolerates a leading UTF-8 BOM (`\u{feff}`) — editors and some PowerShell
/// encoders prepend one, which would otherwise corrupt the FIRST line's JSON and
/// silently drop event #1 (a confirmed match). We strip it rather than lose data.
pub fn parse_schedule(text: &str) -> Vec<ScheduleEntry> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<ScheduleEntry>(line) {
            Ok(entry) if entry.is_resolvable() => out.push(entry),
            Ok(entry) => warn!(
                line = i + 1,
                id = %entry.id,
                "schedule entry has neither event_ticker nor teams; skipping (unresolvable)"
            ),
            Err(e) => warn!(line = i + 1, error = %e, "malformed schedule line; skipping"),
        }
    }
    info!(entries = out.len(), "loaded schedule");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(json: &str) -> Vec<ScheduleEntry> {
        parse_schedule(json)
    }

    #[test]
    fn the_committed_example_schedule_loads() {
        // Regression guard for deploy/schedules/example.jsonl: the shipped example
        // must parse with the real loader -- one predicted-ticker entry plus one
        // team-placeholder entry, both resolvable.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/schedules/example.jsonl");
        let entries = load_schedule(&path).expect("schedule loads");
        assert_eq!(entries.len(), 2, "both example entries are resolvable");
        let m1 = entries.iter().find(|e| e.id == "cup-m1").expect("m1");
        assert_eq!(
            m1.event_ticker.as_deref(),
            Some("KXCUPMATCH-26JUN121330AAABBB")
        );
        let m2 = entries.iter().find(|e| e.id == "cup-m2").expect("m2");
        assert!(m2.event_ticker.is_none(), "placeholder -> resolve by teams");
        assert_eq!(m2.teams, vec!["CCC", "DDD"]);
        // Every entry carries the event-set storage namespace.
        assert!(entries
            .iter()
            .all(|e| e.remote_prefix.as_deref() == Some("remote:kdp/cup-2026")));
    }

    #[test]
    fn parses_a_full_entry() {
        let got = entry(
            r#"{"id":"cup-m1","label":"Alpha vs Beta","series":"KXCUPMATCH","event_ticker":"KXCUPMATCH-26JUN121330AAABBB","teams":["AAA","BBB"],"start_utc":"2026-06-12T13:30:00Z","arm_lead_min":60,"max_hours":8,"remote_prefix":"remote:kdp/cup-2026"}"#,
        );
        assert_eq!(got.len(), 1);
        let e = &got[0];
        assert_eq!(e.id, "cup-m1");
        assert_eq!(e.series, "KXCUPMATCH");
        assert_eq!(
            e.event_ticker.as_deref(),
            Some("KXCUPMATCH-26JUN121330AAABBB")
        );
        assert_eq!(e.teams, vec!["AAA", "BBB"]);
        assert_eq!(e.max_hours, Some(8));
        assert_eq!(e.remote_prefix.as_deref(), Some("remote:kdp/cup-2026"));
    }

    #[test]
    fn optional_fields_default() {
        // No event_ticker (placeholder match) but teams present => resolvable.
        let got = entry(
            r#"{"id":"cup-m2","series":"KXCUPMATCH","teams":["CCC","DDD"],"start_utc":"2026-06-18T13:30:00Z"}"#,
        );
        assert_eq!(got.len(), 1);
        let e = &got[0];
        assert!(e.event_ticker.is_none());
        assert_eq!(e.arm_lead_min, None);
        assert_eq!(e.max_hours, None);
        assert!(e.label.is_empty());
    }

    #[test]
    fn arm_at_uses_entry_override_then_default() {
        let got = entry(
            r#"{"id":"a","series":"S","teams":["X","Y"],"start_utc":"2026-06-12T13:30:00Z","arm_lead_min":90}"#,
        );
        let e = &got[0];
        // entry override (90m) wins over the default (60m).
        assert_eq!(e.arm_at(60).to_rfc3339(), "2026-06-12T12:00:00+00:00");
        // a no-override entry falls back to the default.
        let g2 = entry(
            r#"{"id":"b","series":"S","teams":["X","Y"],"start_utc":"2026-06-12T13:30:00Z"}"#,
        );
        assert_eq!(g2[0].arm_at(60).to_rfc3339(), "2026-06-12T12:30:00+00:00");
    }

    #[test]
    fn blank_and_malformed_lines_are_skipped_not_fatal() {
        let text = concat!(
            "\n",
            "{ not json\n",
            r#"{"id":"ok","series":"S","teams":["X","Y"],"start_utc":"2026-06-12T13:30:00Z"}"#,
            "\n",
            "   \n",
        );
        let got = parse_schedule(text);
        assert_eq!(got.len(), 1, "the one valid line survives the bad ones");
        assert_eq!(got[0].id, "ok");
    }

    #[test]
    fn unresolvable_entry_is_dropped() {
        // Neither event_ticker nor teams => nothing to resolve by => dropped.
        let got = entry(r#"{"id":"empty","series":"S","start_utc":"2026-06-12T13:30:00Z"}"#);
        assert!(got.is_empty());
    }

    #[test]
    fn unknown_field_is_rejected_as_malformed() {
        // deny_unknown_fields => a typo'd field makes the line malformed (skipped),
        // surfacing schema drift instead of silently ignoring it.
        let got = entry(
            r#"{"id":"x","series":"S","teams":["X","Y"],"start_utc":"2026-06-12T13:30:00Z","arm_lead_minutes":60}"#,
        );
        assert!(got.is_empty(), "unknown field -> malformed -> skipped");
    }
}
