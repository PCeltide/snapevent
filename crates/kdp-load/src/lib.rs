//! `kdp-load` — deterministic, time-ordered, typed replay over a processed
//! ticker directory (kdp-process output).
//!
//! The consumer contract (agreed with the first downstream consumer, a
//! backtest framework): one iterator per ticker
//! directory interleaving `book_events`, `trades`, and `gaps` rows on an
//! **effective market timestamp** (WS rows → `recv_ts_us`; REST-backfilled
//! trades → `event_ts_us`, never fetch time), under a total, fully-specified
//! tie-break — the same inputs ALWAYS produce the identical stream. Integer
//! units only (ADR-004); malformed rows are typed errors, never skips (R4);
//! an incomplete manifest must be explicitly acknowledged (R3); unknown newer
//! schema versions are refused (R8).
//!
//! ```no_run
//! # fn main() -> Result<(), kdp_load::LoadError> {
//! let loader = kdp_load::Loader::open("data/KXTEST")?;
//! for event in loader.events()? {
//!     let event = event?;
//!     // feed a backtest engine…
//! }
//! # Ok(()) }
//! ```

mod error;
mod event;
mod manifest;
mod merge;
mod reader;
mod replay;

use std::path::{Path, PathBuf};

pub use error::LoadError;
pub use event::{EffectiveTs, Level, ReplayEvent, TradeSource};
pub use manifest::{Completeness, SUPPORTED_SCHEMA_VERSION};
pub use merge::EventIter;
pub use replay::{BookReplayer, RangeIter};

use manifest::Manifest;

/// A processed ticker directory, opened and validated, ready to stream.
/// (The parsed manifest itself isn't retained — the completeness verdict is
/// derived at open; store the manifest again when a consumer needs its counts.)
#[derive(Debug)]
pub struct Loader {
    dir: PathBuf,
    completeness: Completeness,
    acknowledged: bool,
}

impl Loader {
    /// Open `<dir>` (a kdp-process per-ticker output directory): read +
    /// validate `manifest.json`, refuse unknown newer schema versions (R8).
    /// Opening an incomplete directory SUCCEEDS — the verdict is surfaced via
    /// [`Loader::completeness`] and gates iteration (R3), so callers see the
    /// warning at open time, not mid-stream.
    #[tracing::instrument]
    pub fn open(dir: impl AsRef<Path> + std::fmt::Debug) -> Result<Loader, LoadError> {
        let dir = dir.as_ref().to_path_buf();
        let manifest = Manifest::open(&dir)?;
        let completeness = manifest.completeness();
        if let Completeness::Incomplete { reasons } = &completeness {
            tracing::warn!(dir = %dir.display(), ?reasons, "directory is incomplete");
        }
        Ok(Loader {
            dir,
            completeness,
            acknowledged: false,
        })
    }

    /// The manifest's completeness verdict (R3), decided at open time.
    pub fn completeness(&self) -> &Completeness {
        &self.completeness
    }

    /// Explicitly acknowledge an incomplete directory so it can be iterated.
    /// Without this, `events()`/`between()` on an incomplete directory return
    /// [`LoadError::Incomplete`] — never a silent load.
    pub fn allow_incomplete(mut self) -> Loader {
        self.acknowledged = true;
        self
    }

    /// The full merged, deterministic, time-ordered event stream (R1/R2).
    #[tracing::instrument(skip(self), fields(dir = %self.dir.display()))]
    pub fn events(&self) -> Result<EventIter, LoadError> {
        self.gate()?;
        EventIter::open(&self.dir)
    }

    /// Range replay over `[t0, t1)` (effective-ts microseconds): pre-rolls
    /// `book_events` to `t0` and emits (1) any gap after the last re-anchoring
    /// snapshot — a window opening on a corrupt book is poisoned from event
    /// zero (consumer-confirmed rule) — (2) a synthetic opening snapshot of the book
    /// state at `t0` (R7), then (3) the merged stream within the range.
    ///
    /// Edge semantics: a `t0` before all data yields an opener with an EMPTY
    /// book — meaning "no book state observed yet", not "the book was empty";
    /// check the first real event's timestamp if the distinction matters. A
    /// `t0` past all data yields the final book state and an empty tail.
    #[tracing::instrument(skip(self), fields(dir = %self.dir.display()))]
    pub fn between(&self, t0: i64, t1: i64) -> Result<RangeIter, LoadError> {
        if t0 >= t1 {
            return Err(LoadError::InvalidRange { t0, t1 });
        }
        self.gate()?;
        RangeIter::build(EventIter::open(&self.dir)?, t0, t1)
    }

    /// R3 gate: incomplete + unacknowledged ⇒ typed refusal.
    fn gate(&self) -> Result<(), LoadError> {
        match (&self.completeness, self.acknowledged) {
            (Completeness::Incomplete { reasons }, false) => Err(LoadError::Incomplete {
                reasons: reasons.clone(),
            }),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Repo test idiom: pid-keyed temp dir, fresh per tag.
    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("kdp-load-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("mkdir tmp");
        d
    }

    fn write_manifest(dir: &Path, v: serde_json::Value) {
        std::fs::write(dir.join("manifest.json"), v.to_string()).expect("write manifest");
    }

    fn complete_manifest(schema_version: u16, format: &str) -> serde_json::Value {
        serde_json::json!({
            "schema_version": schema_version, "ticker": "KXTEST", "format": format,
            "complete": true, "read_errors": 0,
            "counts": {"book_events": 2, "book_top": 1, "trades": 1, "gaps": 0, "raw": 0}
        })
    }

    #[test]
    fn open_complete_manifest_yields_complete() {
        let dir = tmp("complete");
        write_manifest(&dir, complete_manifest(1, "parquet"));
        let l = Loader::open(&dir).expect("open");
        assert_eq!(*l.completeness(), Completeness::Complete);
    }

    #[test]
    fn incomplete_manifest_blocks_events_until_acknowledged() {
        let dir = tmp("incomplete");
        write_manifest(
            &dir,
            serde_json::json!({
                "schema_version": 1, "ticker": "KXTEST", "format": "parquet",
                "complete": false, "read_errors": 3,
                "counts": {"book_events": 2, "book_top": 1, "trades": 1, "gaps": 0, "raw": 1}
            }),
        );
        let l = Loader::open(&dir).expect("open still succeeds");
        match l.completeness() {
            Completeness::Incomplete { reasons } => {
                assert_eq!(
                    reasons.len(),
                    2,
                    "read_errors AND raw fallbacks: {reasons:?}"
                )
            }
            c => panic!("expected incomplete, got {c:?}"),
        }
        assert!(
            matches!(l.events(), Err(LoadError::Incomplete { .. })),
            "must not iterate silently"
        );
        let l = l.allow_incomplete();
        assert!(
            !matches!(l.events(), Err(LoadError::Incomplete { .. })),
            "acknowledged directory must pass the gate"
        );
    }

    #[test]
    fn newer_schema_major_is_refused_typed() {
        let dir = tmp("future");
        write_manifest(&dir, complete_manifest(99, "parquet"));
        assert!(matches!(
            Loader::open(&dir),
            Err(LoadError::UnsupportedSchema {
                found: 99,
                supported: SUPPORTED_SCHEMA_VERSION
            })
        ));
    }

    #[test]
    fn non_parquet_format_is_a_typed_manifest_error() {
        let dir = tmp("feather");
        write_manifest(&dir, complete_manifest(1, "feather"));
        assert!(matches!(
            Loader::open(&dir),
            Err(LoadError::Manifest { .. })
        ));
    }

    #[test]
    fn missing_manifest_is_a_typed_io_error() {
        let dir = tmp("nomanifest");
        assert!(matches!(Loader::open(&dir), Err(LoadError::Io { .. })));
    }
}
