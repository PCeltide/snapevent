//! Append-only JSONL storage with daily file rotation.
//!
//! Phase 1 persistence layer — see `docs/adr/ADR-002-storage-strategy.md`. Each
//! `(ticker, channel)` stream gets its own directory; within it, records are
//! appended to a per-UTC-day file named `YYYY-MM-DD.jsonl`, one JSON document
//! per line. Rotation is lazy: the active file is reopened the first time an
//! append happens on a new UTC day.
//!
//! This keeps writes simple and durable and makes replay trivial. Columnar
//! Parquet is derived offline in a later phase rather than written on the hot
//! path.

pub mod disk_guard;
pub mod stream_set;

pub use disk_guard::{DiskGuard, DEFAULT_MIN_FREE_BYTES};
pub use stream_set::StreamSet;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use serde::Serialize;
use thiserror::Error;
use tracing::{info, instrument};

/// Errors raised by the JSONL writer.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A filesystem operation failed.
    #[error("store io error: {0}")]
    Io(#[from] std::io::Error),

    /// A record could not be serialized to JSON.
    #[error("store serialize error: {0}")]
    Serialize(#[from] serde_json::Error),

    /// Free disk space dropped below the configured floor; capture must halt
    /// loudly rather than risk silently filling the disk.
    #[error("disk full: {available} bytes free at {path} is below the {floor}-byte floor")]
    DiskFull {
        /// The guarded path.
        path: PathBuf,
        /// Observed available bytes.
        available: u64,
        /// Configured minimum-free floor.
        floor: u64,
    },

    /// A stream key (ticker or channel) is not a safe single path segment, so
    /// using it as a directory name could escape the base dir (path traversal).
    #[error(
        "unsafe stream key {key:?}: must be a single path segment (no separators, '.', or '..')"
    )]
    UnsafeKey {
        /// The offending key.
        key: String,
    },
}

/// Whether `s` is safe to use as a single filesystem path segment.
///
/// Rejects empty, `.`/`..`, and anything containing a path separator, colon, or
/// NUL — so a server-controlled key (e.g. a Kalshi ticker) cannot traverse out of
/// the base directory. Legitimate tickers (`KXBTCD-26MAY3017-T73499.99`) — which
/// contain `.` and `-` but no separators — pass.
pub fn is_safe_segment(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && !s.contains('/')
        && !s.contains('\\')
        && !s.contains(':')
        && !s.contains('\0')
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, StoreError>;

/// Append-only writer that rotates to a fresh file each UTC day.
///
/// Construct one per `(ticker, channel)` stream and call [`JsonlWriter::append`]
/// to write a record; rotation happens automatically on UTC-day boundaries.
#[derive(Debug)]
pub struct JsonlWriter {
    dir: PathBuf,
    current_day: Option<NaiveDate>,
    writer: Option<BufWriter<File>>,
    current_path: Option<PathBuf>,
}

impl JsonlWriter {
    /// Create a writer rooted at `<base_dir>/<ticker>/<channel>/`.
    ///
    /// The directory tree is created eagerly; the first day-file is opened
    /// lazily on the first [`append`](JsonlWriter::append).
    #[instrument(skip(base_dir))]
    pub fn new(base_dir: impl AsRef<Path>, ticker: &str, channel: &str) -> Result<Self> {
        // Defence in depth: keys become directory names, so a key with a path
        // separator or `..` could escape the base dir. Reject unsafe keys.
        if !is_safe_segment(ticker) {
            return Err(StoreError::UnsafeKey {
                key: ticker.to_string(),
            });
        }
        if !is_safe_segment(channel) {
            return Err(StoreError::UnsafeKey {
                key: channel.to_string(),
            });
        }
        let dir = base_dir.as_ref().join(ticker).join(channel);
        std::fs::create_dir_all(&dir)?;
        info!(dir = %dir.display(), "opened jsonl stream");
        Ok(Self {
            dir,
            current_day: None,
            writer: None,
            current_path: None,
        })
    }

    /// Append one record as a single JSON line, rotating the file first if the
    /// UTC day has changed since the previous write.
    pub fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
        let today = Utc::now().date_naive();
        self.rotate_if_needed(today)?;

        // `rotate_if_needed` guarantees an open writer.
        let writer = self
            .writer
            .as_mut()
            .expect("writer is opened by rotate_if_needed");
        // Serialize straight into the BufWriter (no intermediate String alloc per
        // record). The per-append `flush()` is deliberate (ADR-002): it pushes the
        // record into the kernel page cache so a process-level crash never loses
        // captured L2 (which is unrecoverable). Batched flushing is an explicit
        // future tuning knob, not a silent default.
        serde_json::to_writer(&mut *writer, record)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    /// Path of the file currently being written, or `None` before the first
    /// append.
    pub fn current_path(&self) -> Option<&Path> {
        self.current_path.as_deref()
    }

    fn rotate_if_needed(&mut self, day: NaiveDate) -> Result<()> {
        if self.current_day == Some(day) && self.writer.is_some() {
            return Ok(());
        }
        let path = self.dir.join(format!("{day}.jsonl"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        info!(path = %path.display(), "rotated to jsonl file");
        self.writer = Some(BufWriter::new(file));
        self.current_day = Some(day);
        self.current_path = Some(path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Rec {
        a: u32,
        b: String,
    }

    #[test]
    fn append_writes_one_json_line_per_record() {
        let base = std::env::temp_dir().join(format!("kdp-store-test-{}", std::process::id()));
        // Start from a clean slate in case a previous run left files behind.
        let _ = std::fs::remove_dir_all(&base);

        let mut w = JsonlWriter::new(&base, "KXTEST", "trades").expect("create writer");
        w.append(&Rec {
            a: 1,
            b: "x".into(),
        })
        .expect("append 1");
        w.append(&Rec {
            a: 2,
            b: "y".into(),
        })
        .expect("append 2");

        let path = w.current_path().expect("a file was written").to_path_buf();
        let contents = std::fs::read_to_string(&path).expect("read file back");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "exactly one line per record");
        assert!(lines[0].contains("\"a\":1"), "first line: {}", lines[0]);
        assert!(lines[1].contains("\"a\":2"), "second line: {}", lines[1]);

        // Best-effort cleanup.
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn is_safe_segment_allows_real_tickers_rejects_traversal() {
        // Real Kalshi tickers contain '.' and '-' but no separators.
        assert!(is_safe_segment("KXBTCD-26MAY3017-T73499.99"));
        assert!(is_safe_segment("orderbook"));
        assert!(!is_safe_segment(""));
        assert!(!is_safe_segment("."));
        assert!(!is_safe_segment(".."));
        assert!(!is_safe_segment("../../etc/passwd"));
        assert!(!is_safe_segment("a/b"));
        assert!(!is_safe_segment("a\\b"));
        assert!(!is_safe_segment("C:evil"));
    }

    #[test]
    fn new_rejects_path_traversal_keys() {
        let base = std::env::temp_dir().join(format!("kdp-store-unsafe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        assert!(matches!(
            JsonlWriter::new(&base, "../../../tmp/evil", "orderbook"),
            Err(StoreError::UnsafeKey { .. })
        ));
        assert!(matches!(
            JsonlWriter::new(&base, "KXTEST", "a/b"),
            Err(StoreError::UnsafeKey { .. })
        ));
        // A legitimate dotted ticker is accepted.
        assert!(JsonlWriter::new(&base, "KXBTCD-26MAY3017-T73499.99", "orderbook").is_ok());
        let _ = std::fs::remove_dir_all(&base);
    }
}
