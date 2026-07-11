//! Stream a capture JSONL file into typed [`Envelope`]s.
//!
//! One stored line == one [`Envelope`] (see `kdp_core::envelope`). The reader is
//! a lazy iterator so a multi-hour capture is processed with O(1) line memory
//! (the growing output is the columnar tables, not the input). Upholding "no
//! silent failures": a line that fails to parse or validate is **not** dropped —
//! it is surfaced as a [`ReadError`] (line number + message + the raw text) that
//! the caller counts, warns on, and (when non-empty) persists for review before
//! the raw files may be deleted. A truncated trailing line from a killed capture
//! is therefore reported, not hidden.

use std::io::BufRead;

use kdp_core::Envelope;
use serde::Serialize;

/// A line that could not be decoded into a valid [`Envelope`].
///
/// Carries enough to investigate (and re-derive) the lost line: its 1-based
/// position in the file, the failure, and the original text. `Serialize` so it
/// writes straight to `read_errors.jsonl` (its field order is the on-disk shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadError {
    /// 1-based line number within the source file.
    pub line_no: usize,
    /// Why the line could not be turned into a valid envelope.
    pub error: String,
    /// The original line text (empty only when the failure was the read itself).
    pub raw: String,
}

/// Lazily decode each non-blank line of `reader` into an [`Envelope`].
///
/// Yields `Ok(env)` for a decoded + version-validated line, `Err(ReadError)` for
/// a line that failed I/O, JSON parsing, or schema validation. Blank lines (only
/// whitespace) are skipped — they carry no record and are not a failure.
#[tracing::instrument(skip_all)]
pub fn read_envelopes<R: BufRead>(reader: R) -> impl Iterator<Item = Result<Envelope, ReadError>> {
    reader.lines().enumerate().filter_map(|(i, line)| {
        let line_no = i + 1;
        match line {
            Err(e) => Some(Err(ReadError {
                line_no,
                error: format!("io error: {e}"),
                raw: String::new(),
            })),
            Ok(text) => {
                if text.trim().is_empty() {
                    return None;
                }
                match serde_json::from_str::<Envelope>(&text) {
                    Err(e) => Some(Err(ReadError {
                        line_no,
                        error: format!("json parse: {e}"),
                        raw: text,
                    })),
                    Ok(env) => match env.validate() {
                        Ok(()) => Some(Ok(env)),
                        Err(ve) => Some(Err(ReadError {
                            line_no,
                            error: format!("schema: {ve}"),
                            raw: text,
                        })),
                    },
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdp_core::RecordKind;

    fn parse_all(input: &str) -> Vec<Result<Envelope, ReadError>> {
        read_envelopes(input.as_bytes()).collect()
    }

    const SNAP: &str = r#"{"v":1,"recv_ts":"2026-05-30T00:56:57.9Z","seq":1,"sid":1,"kind":"snapshot","data":{"ticker":"K","ts":"2026-05-30T00:56:57.9Z","yes":[{"price":450000,"quantity":100}],"no":[]}}"#;
    const DELTA: &str = r#"{"v":1,"recv_ts":"2026-05-30T00:56:58.0Z","seq":3,"sid":1,"kind":"delta","data":{"ticker":"K","ts":"2026-05-30T00:56:59.2Z","side":"yes","price":420000,"delta":200000}}"#;
    const TRADE: &str = r#"{"v":1,"recv_ts":"2026-05-30T00:56:58.2Z","seq":2,"sid":2,"kind":"trade","data":{"ticker":"K","ts":"2026-05-30T00:56:59.5Z","price":460000,"count":57605,"taker_side":"yes","taker_book_side":"bid","trade_id":"abc"}}"#;

    #[test]
    fn decodes_each_record_kind_in_order() {
        let out = parse_all(&format!("{SNAP}\n{DELTA}\n{TRADE}\n"));
        assert_eq!(out.len(), 3);
        assert!(matches!(
            out[0].as_ref().unwrap().kind,
            RecordKind::Snapshot(_)
        ));
        assert!(matches!(
            out[1].as_ref().unwrap().kind,
            RecordKind::Delta(_)
        ));
        assert!(matches!(
            out[2].as_ref().unwrap().kind,
            RecordKind::Trade(_)
        ));
    }

    #[test]
    fn blank_lines_are_skipped_not_errored() {
        let out = parse_all(&format!("{SNAP}\n\n   \n{DELTA}\n"));
        assert_eq!(out.len(), 2, "two records, blank lines dropped silently");
        assert!(out.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn malformed_line_is_reported_with_position_and_raw_not_dropped() {
        let out = parse_all(&format!("{SNAP}\n{{not json\n{DELTA}\n"));
        assert_eq!(out.len(), 3);
        assert!(out[0].is_ok());
        let err = out[1].as_ref().unwrap_err();
        assert_eq!(err.line_no, 2);
        assert_eq!(err.raw, "{not json");
        assert!(err.error.contains("json parse"));
        assert!(out[2].is_ok(), "reading continues past the bad line");
    }

    #[test]
    fn future_schema_version_is_a_loud_read_error() {
        let future = SNAP.replacen("\"v\":1", "\"v\":2", 1);
        let out = parse_all(&future);
        assert_eq!(out.len(), 1);
        let err = out[0].as_ref().unwrap_err();
        assert!(err.error.contains("schema"), "got {}", err.error);
    }
}
