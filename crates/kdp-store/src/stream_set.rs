//! Lazy per-`(ticker, channel)` fan-out over [`JsonlWriter`](crate::JsonlWriter).
//!
//! Capture multiplexes many markets and channels over one connection but each
//! stream needs its own append-only log. A [`StreamSet`] owns a map from
//! `(ticker, channel)` to a writer, created lazily on first append, so the
//! caller just routes each decoded record by its `(ticker, channel)` and the
//! set handles directory layout (`<base>/<ticker>/<channel>/<date>.jsonl`) and
//! daily rotation. It stays generic (`T: Serialize`, `&str` keys) — no domain
//! types — so the stored [`kdp_core::Envelope`] is just a `T`.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{JsonlWriter, Result};

/// A lazily-populated set of append-only writers keyed by `(ticker, channel)`.
///
/// Each distinct `(ticker, channel)` gets its own [`JsonlWriter`] (and thus its
/// own directory + daily-rotated file), created on first append. Stays generic
/// over `T: Serialize`, so the domain `Envelope` is just a `T`.
#[derive(Debug)]
pub struct StreamSet {
    base_dir: PathBuf,
    writers: HashMap<(String, String), JsonlWriter>,
}

impl StreamSet {
    /// Create a stream set rooted at `base_dir`. No directories are created until
    /// the first [`append`](Self::append) for a given stream.
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            writers: HashMap::new(),
        }
    }

    /// Append `record` to the `(ticker, channel)` stream, creating that stream's
    /// writer on first use.
    ///
    /// Not `#[instrument]`d: this is the per-record hot path, matching
    /// [`JsonlWriter::append`](crate::JsonlWriter::append). The lazily-invoked
    /// `JsonlWriter::new` is itself instrumented, so stream creation is still
    /// traced.
    pub fn append<T: Serialize>(&mut self, ticker: &str, channel: &str, record: &T) -> Result<()> {
        let writer = match self
            .writers
            .entry((ticker.to_string(), channel.to_string()))
        {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let writer = JsonlWriter::new(&self.base_dir, ticker, channel)?;
                e.insert(writer)
            }
        };
        writer.append(record)
    }

    /// Number of distinct `(ticker, channel)` streams currently open. Useful for
    /// the capture run report.
    pub fn active_stream_count(&self) -> usize {
        self.writers.len()
    }

    /// Evict the writer for `(ticker, channel)`, releasing its file handle.
    /// Records are flushed per-append, so nothing is lost; a later append for the
    /// same stream re-opens the day-file in append mode. Returns whether a writer
    /// was present.
    ///
    /// Lets a long backfill over tens of thousands of markets release each
    /// market's handle on completion instead of holding them all open at once — a
    /// guard against exhausting the process file-descriptor limit.
    pub fn close(&mut self, ticker: &str, channel: &str) -> bool {
        self.writers
            .remove(&(ticker.to_string(), channel.to_string()))
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Rec {
        n: u32,
    }

    fn unique_base(tag: &str) -> PathBuf {
        let base =
            std::env::temp_dir().join(format!("kdp-streamset-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        base
    }

    fn read_jsonl_lines(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).expect("read_dir") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                let contents = std::fs::read_to_string(&path).expect("read file");
                out.extend(contents.lines().map(|s| s.to_string()));
            }
        }
        out
    }

    #[test]
    fn append_fans_out_to_ticker_channel_files() {
        let base = unique_base("fanout");
        let mut set = StreamSet::new(&base);
        set.append("KXTEST", "orderbook", &Rec { n: 1 })
            .expect("ob1");
        set.append("KXTEST", "orderbook", &Rec { n: 2 })
            .expect("ob2");
        set.append("KXTEST", "trade", &Rec { n: 3 }).expect("tr1");

        let ob = read_jsonl_lines(&base.join("KXTEST").join("orderbook"));
        let tr = read_jsonl_lines(&base.join("KXTEST").join("trade"));
        assert_eq!(ob.len(), 2, "two orderbook records");
        assert_eq!(tr.len(), 1, "one trade record");
        assert!(ob[0].contains("\"n\":1"), "first ob line: {}", ob[0]);

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn reuses_one_writer_per_stream() {
        let base = unique_base("reuse");
        let mut set = StreamSet::new(&base);
        for i in 0..5 {
            set.append("KXAAA", "orderbook", &Rec { n: i })
                .expect("append");
        }
        assert_eq!(set.active_stream_count(), 1, "one writer for one stream");
        set.append("KXAAA", "trade", &Rec { n: 9 }).expect("append");
        assert_eq!(set.active_stream_count(), 2, "second stream adds a writer");

        let lines = read_jsonl_lines(&base.join("KXAAA").join("orderbook"));
        assert_eq!(lines.len(), 5, "all five appends in one file");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn close_evicts_writer_and_reopen_appends() {
        let base = unique_base("close");
        let mut set = StreamSet::new(&base);
        set.append("KXAAA", "trade", &Rec { n: 1 }).expect("a1");
        assert_eq!(set.active_stream_count(), 1);
        assert!(set.close("KXAAA", "trade"), "writer was present");
        assert_eq!(set.active_stream_count(), 0, "FD released on close");
        assert!(!set.close("KXAAA", "trade"), "second close is a no-op");
        // re-opening the same stream appends (does not truncate the day-file)
        set.append("KXAAA", "trade", &Rec { n: 2 }).expect("a2");
        let lines = read_jsonl_lines(&base.join("KXAAA").join("trade"));
        assert_eq!(lines.len(), 2, "both records survive close + reopen");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn separate_tickers_get_separate_directories() {
        let base = unique_base("tickers");
        let mut set = StreamSet::new(&base);
        set.append("KXAAA", "trade", &Rec { n: 1 }).expect("a");
        set.append("KXBBB", "trade", &Rec { n: 2 }).expect("b");
        assert_eq!(set.active_stream_count(), 2);
        assert_eq!(read_jsonl_lines(&base.join("KXAAA").join("trade")).len(), 1);
        assert_eq!(read_jsonl_lines(&base.join("KXBBB").join("trade")).len(), 1);

        let _ = std::fs::remove_dir_all(&base);
    }
}
