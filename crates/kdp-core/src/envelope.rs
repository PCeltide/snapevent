//! The stored record envelope — one tagged JSON object per JSONL line.
//!
//! Every line kdp persists is an [`Envelope`]: a versioned, receive-stamped
//! wrapper around exactly one [`RecordKind`] (a snapshot, delta, trade, gap
//! marker, or raw fallback). This is the on-disk contract (ADR-004): the
//! envelope adds the receive timestamp the wire snapshot lacks, the
//! per-subscription `seq`/`sid` needed to reason about gaps, and a schema
//! version `v` for forward evolution.
//!
//! Two of the kinds exist specifically to uphold "no silent failures":
//! [`GapMarker`] records a hole (sequence jump, reconnect, or resubscribe)
//! *inline* and positionally exact in the append-only log, and [`RawFallback`]
//! preserves a message we could not decode verbatim (original payload + the
//! decode error) instead of dropping it.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::ids::{Ticker, Timestamp};
use crate::records::{OrderbookDelta, OrderbookSnapshot, Trade};

/// Current envelope schema version. Bump when the on-disk shape changes; the
/// `v` field lets a reader dispatch on the version it finds (see
/// [`Envelope::validate`]).
///
/// v2 added [`RecordKind::Verify`] and [`GapReason::VerifyMismatch`]; v1 lines
/// remain readable (older-or-equal versions are accepted by
/// [`Envelope::validate`]).
pub const ENVELOPE_VERSION: u16 = 2;

/// Errors from validating a (typically just-deserialized) [`Envelope`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeError {
    /// The line's schema version is newer than this build understands, so this
    /// build cannot interpret it correctly — a loud failure rather than a silent
    /// misread of newer data.
    #[error("unsupported envelope schema version {found} (this build supports up to {supported})")]
    UnsupportedVersion {
        /// The version found on the line.
        found: u16,
        /// The highest version this build supports ([`ENVELOPE_VERSION`]).
        supported: u16,
    },
}

/// One stored JSONL line: a versioned, receive-stamped wrapper around exactly
/// one record.
///
/// The [`kind`](Envelope::kind) is flattened into the top-level object so a line
/// reads as a single tagged object — e.g.
/// `{"v":1,"recv_ts":"…","seq":42,"sid":7,"kind":"delta","data":{…}}`.
/// `seq`/`sid` are omitted entirely when absent (snapshots/trades that carry no
/// subscription sequence).
///
/// **Unknown top-level fields are silently ignored, not rejected.**
/// `#[serde(deny_unknown_fields)]` cannot be combined with the `#[serde(flatten)]`
/// on [`kind`](Envelope::kind) (serde-rs/serde#1600), so an extra top-level key
/// is dropped on read. The forward-compatibility guard is instead the `v` field
/// plus [`Envelope::validate`]: always bump [`ENVELOPE_VERSION`] when adding a
/// top-level field, so an older build rejects the newer line loudly rather than
/// silently discarding the field. (The inner `data` record itself *does* use
/// `deny_unknown_fields`, so unknown fields there are a loud error.)
///
/// Not `Eq`: [`RawFallback`] carries a [`serde_json::Value`], which is only
/// `PartialEq`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    /// Schema version (see [`ENVELOPE_VERSION`]).
    pub v: u16,
    /// Receive wall-clock time (UTC), always present — this is the authoritative
    /// timestamp for snapshots, which carry none on the wire.
    pub recv_ts: Timestamp,
    /// Per-subscription sequence number, when the source provides one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// Server subscription id, when the source provides one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sid: Option<i64>,
    /// The wrapped record, flattened so its `kind`/`data` keys sit at top level.
    #[serde(flatten)]
    pub kind: RecordKind,
}

impl Envelope {
    /// Wrap a record at the current [`ENVELOPE_VERSION`] with the given receive
    /// time and optional `seq`/`sid`.
    pub fn new(recv_ts: Timestamp, seq: Option<u64>, sid: Option<i64>, kind: RecordKind) -> Self {
        Self {
            v: ENVELOPE_VERSION,
            recv_ts,
            seq,
            sid,
            kind,
        }
    }

    /// Validate a deserialized envelope against this build's schema.
    ///
    /// Returns [`EnvelopeError::UnsupportedVersion`] when `v` is newer than
    /// [`ENVELOPE_VERSION`]. Call this at the read boundary on replay so a
    /// future schema is rejected loudly instead of being silently misread (the
    /// permissive serde default would otherwise drop fields it doesn't know).
    /// Older or equal versions are accepted.
    pub fn validate(&self) -> Result<(), EnvelopeError> {
        if self.v > ENVELOPE_VERSION {
            return Err(EnvelopeError::UnsupportedVersion {
                found: self.v,
                supported: ENVELOPE_VERSION,
            });
        }
        Ok(())
    }
}

/// The payload of an [`Envelope`] — exactly one of the record kinds kdp stores.
///
/// Adjacently tagged: serializes as `{"kind":"…","data":{…}}`, so the
/// discriminant is explicit and the record body is namespaced under `data`.
/// Not `Eq` (see [`RawFallback`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum RecordKind {
    /// A full order-book snapshot.
    Snapshot(OrderbookSnapshot),
    /// An incremental order-book change.
    Delta(OrderbookDelta),
    /// An executed trade.
    Trade(Trade),
    /// An inline gap marker (a hole in the stream).
    Gap(GapMarker),
    /// A message we could not decode, preserved verbatim.
    Raw(RawFallback),
    /// An external REST orderbook observation fetched during capture for
    /// offline cross-verification against the replayed book — NOT a stream
    /// snapshot. Replay must never re-anchor the book on it.
    Verify(OrderbookSnapshot),
}

/// Why a [`GapMarker`] was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    /// A delta `seq` skipped ahead of `last_seq + 1` — at least one delta was
    /// missed.
    SeqJump,
    /// The WebSocket connection dropped and was re-established; the book state
    /// before the gap is no longer trustworthy until the next snapshot.
    Reconnect,
    /// We deliberately resubscribed (e.g. after a seq jump) to obtain a fresh
    /// snapshot.
    Resubscribe,
    /// The replayed book failed to match a REST verify observation within
    /// kdp-process's tolerance window; synthesized by kdp-process at
    /// verification time — never written by capture.
    VerifyMismatch,
}

/// An inline marker recording a hole in a stream.
///
/// Written into the same append-only log as the data so replay sees the hole at
/// its exact position. A gap is always *also* a `tracing::warn!` at the call
/// site — never silent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GapMarker {
    /// Why the gap occurred.
    pub reason: GapReason,
    /// Market the gap belongs to.
    pub ticker: Ticker,
    /// Logical channel the gap is in (e.g. `"orderbook"`, `"trade"`).
    pub channel: String,
    /// Last sequence number seen before the gap, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    /// Sequence number observed that revealed the gap, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_seq: Option<u64>,
    /// Human-readable detail for diagnostics.
    pub detail: String,
}

/// A message that could not be decoded, preserved verbatim instead of dropped.
///
/// Upholds "no silent failures": when a wire value is unparseable (e.g. a price
/// with more precision than [`crate::units::MicroDollars`] can hold) or a
/// message shape is unrecognized, we persist the original JSON plus the decode
/// error so nothing is lost and the anomaly can be investigated later.
///
/// Not `Eq`: `payload` is a [`serde_json::Value`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawFallback {
    /// The wire message's declared type, if we could read it. (`payload` is
    /// always written, even as JSON `null`; do not add `skip_serializing_if` to
    /// it — a stored line missing `payload` is intentionally a loud read error.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_type: Option<String>,
    /// The market the message concerned, if we could read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticker: Option<Ticker>,
    /// Why decoding failed.
    pub error: String,
    /// The original message payload, preserved verbatim.
    pub payload: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{Side, Ticker};
    use crate::records::{OrderbookDelta, OrderbookSnapshot, PriceLevel, Trade};
    use crate::units::{MicroDollars, QtyDelta, RestingQty};
    use chrono::{DateTime, Utc};

    fn ts() -> Timestamp {
        "2026-05-30T12:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("valid RFC3339")
            .into()
    }

    fn ts_value() -> serde_json::Value {
        serde_json::to_value(ts()).expect("serialize ts")
    }

    fn sample_snapshot() -> OrderbookSnapshot {
        OrderbookSnapshot {
            ticker: Ticker("KXTEST".to_string()),
            ts: ts(),
            yes: vec![PriceLevel {
                price: MicroDollars(80_000),
                quantity: RestingQty(30_000),
            }],
            no: vec![],
        }
    }

    #[test]
    fn snapshot_envelope_has_exact_flat_json_shape() {
        let env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        let value = serde_json::to_value(&env).expect("to_value");
        assert_eq!(
            value,
            serde_json::json!({
                "v": 2,
                "recv_ts": ts_value(),
                "kind": "snapshot",
                "data": {
                    "ticker": "KXTEST",
                    "ts": ts_value(),
                    "yes": [{ "price": 80_000, "quantity": 30_000 }],
                    "no": []
                }
            })
        );
    }

    #[test]
    fn delta_envelope_includes_seq_sid_and_signed_delta() {
        let delta = OrderbookDelta {
            ticker: Ticker("KXTEST".to_string()),
            ts: ts(),
            side: Side::Yes,
            price: MicroDollars(80_000),
            delta: QtyDelta(-500),
        };
        let env = Envelope::new(ts(), Some(42), Some(7), RecordKind::Delta(delta));
        let value = serde_json::to_value(&env).expect("to_value");
        assert_eq!(value["kind"], "delta");
        assert_eq!(value["seq"], 42);
        assert_eq!(value["sid"], 7);
        assert_eq!(value["data"]["delta"], -500);
        assert_eq!(value["data"]["side"], "yes");
    }

    #[test]
    fn absent_seq_and_sid_are_omitted() {
        let env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        let value = serde_json::to_value(&env).expect("to_value");
        let obj = value.as_object().expect("object");
        assert!(!obj.contains_key("seq"), "seq omitted when None");
        assert!(!obj.contains_key("sid"), "sid omitted when None");
    }

    #[test]
    fn constructor_stamps_current_schema_version() {
        let env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        assert_eq!(env.v, ENVELOPE_VERSION);
    }

    #[test]
    fn gap_marker_envelope_shape_and_round_trip() {
        let gap = GapMarker {
            reason: GapReason::SeqJump,
            ticker: Ticker("KXTEST".to_string()),
            channel: "orderbook".to_string(),
            last_seq: Some(10),
            observed_seq: Some(13),
            detail: "seq jumped 10 -> 13".to_string(),
        };
        let env = Envelope::new(ts(), Some(13), Some(7), RecordKind::Gap(gap));
        let value = serde_json::to_value(&env).expect("to_value");
        assert_eq!(value["kind"], "gap");
        assert_eq!(value["data"]["reason"], "seq_jump");
        assert_eq!(value["data"]["channel"], "orderbook");
        assert_eq!(value["data"]["last_seq"], 10);
        assert_eq!(value["data"]["observed_seq"], 13);

        let json = serde_json::to_string(&env).expect("serialize");
        let back: Envelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, back);
    }

    #[test]
    fn raw_fallback_preserves_payload_and_round_trips() {
        let raw = RawFallback {
            raw_type: Some("orderbook_delta".to_string()),
            ticker: Some(Ticker("KXTEST".to_string())),
            error: "too many decimal places".to_string(),
            payload: serde_json::json!({ "price_dollars": "0.1234567", "delta_fp": "5.00" }),
        };
        let env = Envelope::new(ts(), None, None, RecordKind::Raw(raw));
        let value = serde_json::to_value(&env).expect("to_value");
        assert_eq!(value["kind"], "raw");
        assert_eq!(value["data"]["payload"]["price_dollars"], "0.1234567");
        assert_eq!(value["data"]["error"], "too many decimal places");

        let json = serde_json::to_string(&env).expect("serialize");
        let back: Envelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, back);
    }

    #[test]
    fn trade_envelope_round_trips() {
        let trade = Trade {
            ticker: Ticker("KXTEST".to_string()),
            ts: ts(),
            price: MicroDollars(500_000),
            count: RestingQty(1_200),
            taker_side: Side::No,
            taker_book_side: None,
            trade_id: Some("t-1".to_string()),
        };
        let env = Envelope::new(ts(), None, None, RecordKind::Trade(trade));
        let json = serde_json::to_string(&env).expect("serialize");
        let back: Envelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, back);
        assert_eq!(
            serde_json::to_value(&env).expect("to_value")["kind"],
            "trade"
        );
    }

    #[test]
    fn validate_accepts_current_version() {
        let env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        assert_eq!(env.validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_future_version() {
        let mut env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        env.v = ENVELOPE_VERSION + 1;
        assert_eq!(
            env.validate(),
            Err(EnvelopeError::UnsupportedVersion {
                found: ENVELOPE_VERSION + 1,
                supported: ENVELOPE_VERSION,
            })
        );
    }

    #[test]
    fn unknown_field_in_a_record_is_rejected_on_read() {
        // deny_unknown_fields on the inner records turns an unexpected field into
        // a loud deserialize error instead of silently dropping it on replay.
        let line = serde_json::json!({
            "v": 1,
            "recv_ts": ts_value(),
            "kind": "delta",
            "data": {
                "ticker": "KXTEST",
                "ts": ts_value(),
                "side": "yes",
                "price": 80_000,
                "delta": -500,
                "surprise": 1
            }
        })
        .to_string();
        let result: Result<Envelope, _> = serde_json::from_str(&line);
        assert!(
            result.is_err(),
            "unknown inner field must be rejected, got {result:?}"
        );
    }

    #[test]
    fn unknown_top_level_envelope_field_is_silently_ignored() {
        // Envelope has #[serde(flatten)] on `kind`, so deny_unknown_fields cannot
        // apply: an extra top-level key is dropped, not rejected. This pins that
        // known behavior so the `v` + validate() version guard stays the
        // documented forward-compat mechanism (and a regression is caught).
        let line = serde_json::json!({
            "v": 2,
            "recv_ts": ts_value(),
            "extra_top_level": 99,
            "kind": "snapshot",
            "data": {
                "ticker": "KXTEST",
                "ts": ts_value(),
                "yes": [{ "price": 80_000, "quantity": 30_000 }],
                "no": []
            }
        })
        .to_string();
        let env: Envelope =
            serde_json::from_str(&line).expect("extra top-level key is ignored, not rejected");
        assert_eq!(
            env,
            Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()))
        );
    }

    #[test]
    fn verify_envelope_has_verify_kind_and_current_version_and_round_trips() {
        let env = Envelope::new(ts(), None, None, RecordKind::Verify(sample_snapshot()));
        let value = serde_json::to_value(&env).expect("to_value");
        assert_eq!(value["v"], 2);
        assert_eq!(value["kind"], "verify");
        assert_eq!(value["data"]["ticker"], "KXTEST");

        let json = serde_json::to_string(&env).expect("serialize");
        let back: Envelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, back);
    }

    #[test]
    fn verify_mismatch_gap_reason_serializes_to_snake_case_string() {
        let gap = GapMarker {
            reason: GapReason::VerifyMismatch,
            ticker: Ticker("KXTEST".to_string()),
            channel: "orderbook".to_string(),
            last_seq: None,
            observed_seq: None,
            detail: "book diverged from REST verify observation".to_string(),
        };
        let env = Envelope::new(ts(), None, None, RecordKind::Gap(gap));
        let value = serde_json::to_value(&env).expect("to_value");
        assert_eq!(value["data"]["reason"], "verify_mismatch");

        let json = serde_json::to_string(&env).expect("serialize");
        let back: Envelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, back);
    }

    #[test]
    fn validate_accepts_exactly_current_version_two() {
        let mut env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        env.v = 2;
        assert_eq!(env.validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_version_three() {
        let mut env = Envelope::new(ts(), None, None, RecordKind::Snapshot(sample_snapshot()));
        env.v = 3;
        assert_eq!(
            env.validate(),
            Err(EnvelopeError::UnsupportedVersion {
                found: 3,
                supported: 2,
            })
        );
    }

    #[test]
    fn old_v1_line_still_deserializes_and_validates() {
        // A line written by a build before ENVELOPE_VERSION was bumped to 2 must
        // still be readable: v is a forward-compat guard against *newer* schemas,
        // not a requirement that every line matches the current build exactly.
        let line = serde_json::json!({
            "v": 1,
            "recv_ts": ts_value(),
            "kind": "snapshot",
            "data": {
                "ticker": "KXTEST",
                "ts": ts_value(),
                "yes": [{ "price": 80_000, "quantity": 30_000 }],
                "no": []
            }
        })
        .to_string();
        let env: Envelope = serde_json::from_str(&line).expect("v1 line still deserializes");
        assert_eq!(env.validate(), Ok(()), "v1 line still validates");
    }
}
