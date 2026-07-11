//! Per-ticker `manifest.json` — the provenance + safety record for one processed
//! market.
//!
//! Written alongside the Parquet/Feather tables, it records what was read, what
//! was produced, the capture's time span, and — crucially — whether the
//! structured tables fully represent the capture (`complete`). The operator
//! consults `complete` + `notes` before deleting the raw JSONL: only when
//! `complete` is true (no unreadable lines AND no raw fallbacks) is dropping the
//! raw safe.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

/// Schema version of the processed output (the Parquet table set + this
/// manifest). Distinct from the on-disk envelope version; bump when a table's
/// columns change.
pub const PROCESSED_SCHEMA_VERSION: u16 = 1;

/// One source capture file that fed this ticker's output.
#[derive(Debug, Clone, Serialize)]
pub struct SourceFile {
    /// Logical channel the file came from (`"orderbook"` / `"trade"`).
    pub channel: String,
    /// Path to the source `.jsonl` file.
    pub path: String,
    /// Number of records read from it (decoded + unreadable; excludes blanks).
    pub records: usize,
}

/// Per-table output row counts.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Counts {
    /// Rows in `book_events` (snapshot levels + deltas) — the lossless log.
    pub book_events: usize,
    /// Rows in `book_top` (one per orderbook message).
    pub book_top: usize,
    /// Rows in `trades`.
    pub trades: usize,
    /// Rows in `gaps`.
    pub gaps: usize,
    /// Rows in `raw` (undecodable messages preserved verbatim).
    pub raw: usize,
}

/// The provenance + safety record written next to a ticker's output tables.
#[derive(Debug, Clone, Serialize)]
pub struct Manifest {
    /// Processed-output schema version ([`PROCESSED_SCHEMA_VERSION`]).
    pub schema_version: u16,
    /// `kdp-process` version that produced this output.
    pub tool_version: String,
    /// The market ticker.
    pub ticker: String,
    /// Output format (`"parquet"` / `"feather"`).
    pub format: String,
    /// When this output was produced (RFC 3339, UTC).
    pub processed_at: String,
    /// Source capture directory for this ticker.
    pub source_dir: String,
    /// The source files consumed.
    pub source_files: Vec<SourceFile>,
    /// Earliest receive timestamp seen (microseconds since epoch), if any.
    pub first_recv_ts_us: Option<i64>,
    /// Latest receive timestamp seen (microseconds since epoch), if any.
    pub last_recv_ts_us: Option<i64>,
    /// Earliest receive timestamp as RFC 3339 (convenience), if any.
    pub first_recv_ts: Option<String>,
    /// Latest receive timestamp as RFC 3339 (convenience), if any.
    pub last_recv_ts: Option<String>,
    /// Per-table output row counts.
    pub counts: Counts,
    /// Number of source lines that could not be decoded (saved to
    /// `read_errors.jsonl`).
    pub read_errors: usize,
    /// True iff the structured tables fully represent the capture: `read_errors
    /// == 0` AND no raw fallbacks (`counts.raw == 0`). When true, the raw JSONL is
    /// safe to drop once the output is verified. (Raw fallbacks / read errors are
    /// still preserved in `raw.*` / `read_errors.jsonl`, but their presence means
    /// the structured output isn't the whole story — hence not "complete".)
    pub complete: bool,
    /// Human-readable advisories (gaps recorded, raw preserved, drop-safety).
    pub notes: Vec<String>,
    /// True iff at some orderbook message this strike had BOTH a yes-ask
    /// (`yes_ask_micro.is_some()`, you could buy YES) AND a no-ask
    /// (`yes_bid_micro.is_some()`, you could buy NO) -- a genuine two-way market.
    /// A strike one-sided the entire session is dropped from the curated upload
    /// (its raw still backs up to Drive). See the Phase C plan.
    #[serde(default)]
    pub two_sided: bool,
    /// How many orderbook messages showed a two-sided top-of-book (diagnostic).
    #[serde(default)]
    pub two_sided_snapshots: usize,
}

impl Manifest {
    /// Serialize as pretty JSON and write to `path`.
    #[tracing::instrument(skip(self), fields(path = %path.display()))]
    pub fn write(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self).context("serializing manifest")?;
        std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}

/// Render a microsecond-epoch timestamp as RFC 3339 (UTC), if representable.
pub fn us_to_rfc3339(us: i64) -> Option<String> {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us).map(|dt| dt.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_flag_round_trips_in_json() {
        let m = Manifest {
            schema_version: PROCESSED_SCHEMA_VERSION,
            tool_version: "0.1.0".into(),
            ticker: "K".into(),
            format: "parquet".into(),
            processed_at: "2026-05-30T00:00:00+00:00".into(),
            source_dir: "data/K".into(),
            source_files: vec![SourceFile {
                channel: "orderbook".into(),
                path: "data/K/orderbook/2026-05-30.jsonl".into(),
                records: 100,
            }],
            first_recv_ts_us: Some(0),
            last_recv_ts_us: Some(1_000_000),
            first_recv_ts: us_to_rfc3339(0),
            last_recv_ts: us_to_rfc3339(1_000_000),
            counts: Counts {
                book_events: 50,
                book_top: 20,
                trades: 5,
                gaps: 0,
                raw: 0,
            },
            read_errors: 0,
            complete: true,
            notes: vec!["safe".into()],
            two_sided: false,
            two_sided_snapshots: 0,
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back["complete"], true);
        assert_eq!(back["counts"]["book_events"], 50);
        assert_eq!(back["ticker"], "K");
    }

    #[test]
    fn us_to_rfc3339_is_utc() {
        assert_eq!(
            us_to_rfc3339(0).as_deref(),
            Some("1970-01-01T00:00:00+00:00")
        );
    }
}
