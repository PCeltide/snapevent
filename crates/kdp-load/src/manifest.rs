//! Reading + gating on `manifest.json` (R3 verdict, R8 version refusal).
//!
//! Mirrors ONLY the fields kdp-load acts on; deliberately tolerant of extra
//! fields (no `deny_unknown_fields`) so a minor-version writer that ADDS
//! manifest fields doesn't break readers — the `schema_version` gate is what
//! protects against semantic changes.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::LoadError;

/// The newest processed-schema version this crate knows how to read
/// (kdp-process `PROCESSED_SCHEMA_VERSION`).
pub const SUPPORTED_SCHEMA_VERSION: u16 = 1;

/// The subset of `manifest.json` kdp-load acts on.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Manifest {
    pub schema_version: u16,
    #[allow(dead_code)] // read for diagnostics/log context
    pub ticker: String,
    pub format: String,
    pub complete: bool,
    #[serde(default)]
    pub read_errors: usize,
    #[serde(default)]
    pub counts: BTreeMap<String, u64>,
}

/// The manifest's completeness verdict, surfaced at open time (R3). An
/// `Incomplete` directory can still be loaded — but only after the caller
/// explicitly acknowledges via [`crate::Loader::allow_incomplete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completeness {
    /// The structured tables fully represent the capture.
    Complete,
    /// They don't — the reasons say why (read errors, raw fallbacks).
    Incomplete { reasons: Vec<String> },
}

impl Manifest {
    /// Read + validate `<dir>/manifest.json`. Typed errors for a missing/
    /// malformed file, an unsupported schema version, or a non-parquet format.
    pub(crate) fn open(dir: &Path) -> Result<Manifest, LoadError> {
        let path = dir.join("manifest.json");
        let text = std::fs::read_to_string(&path).map_err(|source| LoadError::Io {
            path: path.clone(),
            source,
        })?;
        let m: Manifest = serde_json::from_str(&text).map_err(|e| LoadError::Manifest {
            path: path.clone(),
            detail: e.to_string(),
        })?;
        if m.schema_version > SUPPORTED_SCHEMA_VERSION {
            return Err(LoadError::UnsupportedSchema {
                found: m.schema_version,
                supported: SUPPORTED_SCHEMA_VERSION,
            });
        }
        if m.format != "parquet" {
            return Err(LoadError::Manifest {
                path,
                detail: format!(
                    "format {:?} not supported (v1 reads parquet only)",
                    m.format
                ),
            });
        }
        Ok(m)
    }

    /// Derive the completeness verdict, with human-actionable reasons.
    pub(crate) fn completeness(&self) -> Completeness {
        if self.complete {
            return Completeness::Complete;
        }
        let mut reasons = Vec::new();
        if self.read_errors > 0 {
            reasons.push(format!(
                "{} undecodable source lines (read_errors.jsonl)",
                self.read_errors
            ));
        }
        let raw = self.counts.get("raw").copied().unwrap_or(0);
        if raw > 0 {
            reasons.push(format!("{raw} raw fallback records (raw table)"));
        }
        if reasons.is_empty() {
            reasons.push("manifest says complete=false".to_string());
        }
        Completeness::Incomplete { reasons }
    }
}
