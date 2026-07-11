//! Typed errors for every way a load can fail.
//!
//! Library crate ⇒ `thiserror`, never `anyhow` (workspace convention). Each
//! variant carries the context a consumer needs to act: the path, the table,
//! the row. Two rules shape this enum: **no silent skips** (a malformed row is
//! `MalformedRow`, never a dropped record — R4) and **no silent loads** (an
//! incomplete manifest is `Incomplete` unless explicitly acknowledged — R3).

use std::path::PathBuf;

/// Any failure opening or streaming a processed ticker directory.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// Filesystem-level failure (missing dir, unreadable file).
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// `manifest.json` is missing, unparseable, or self-inconsistent.
    #[error("manifest error at {path}: {detail}")]
    Manifest { path: PathBuf, detail: String },

    /// The manifest declares a schema newer than this crate reads (R8):
    /// refuse loudly instead of guessing at unknown semantics.
    #[error("unsupported processed-schema version {found} (this kdp-load reads <= {supported})")]
    UnsupportedSchema { found: u16, supported: u16 },

    /// The manifest says the structured tables are NOT the whole story
    /// (`complete: false`). Iterating requires `Loader::allow_incomplete()`.
    #[error("directory is incomplete ({}); call allow_incomplete() to load anyway", reasons.join("; "))]
    Incomplete { reasons: Vec<String> },

    /// An expected table file is absent from the directory.
    #[error("missing table {name} (expected at {path})")]
    MissingTable { name: &'static str, path: PathBuf },

    /// A row failed to decode into its typed form. Never skipped (R4): the
    /// stream surfaces this error AT the row, with enough context to find it.
    #[error("malformed row {row} in `{table}`: {detail}")]
    MalformedRow {
        table: &'static str,
        row: usize,
        detail: String,
    },

    /// Parquet-level read failure on a table. Carries the rendered error text,
    /// NOT the parquet error type: arrow/parquet stay out of the public API
    /// (leaf-crate contract), so a parquet major bump is never a semver break
    /// for consumers.
    #[error("parquet error in `{table}`: {detail}")]
    Parquet { table: &'static str, detail: String },

    /// `between(t0, t1)` called with an empty or inverted range.
    #[error("invalid range: t0 ({t0}) must be < t1 ({t1})")]
    InvalidRange { t0: i64, t1: i64 },

    /// The merged stream regressed in time — the writer's file-order-equals-
    /// time-order assumption was violated (e.g. a wall-clock step mid-capture).
    /// Loud, never silent: a mis-ordered tape corrupts range replay.
    #[error("ordering violation in the merged stream: {detail}")]
    OrderViolation { detail: String },
}
