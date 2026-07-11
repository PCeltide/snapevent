//! Tolerant decode of WebSocket text frames into [`kdp_core`] records.
//!
//! Every Kalshi market-data frame is a JSON object `{type, sid?, seq?, msg}`.
//! [`decode_frame`] classifies a frame into a [`FrameOutcome`]: a persistable
//! data record (snapshot/delta/trade), a control frame (subscribed/ok), a server
//! error, or — for anything it cannot decode into a known record — a
//! [`RawFallback`] that preserves the original payload verbatim plus the reason.
//!
//! "Tolerant" is the whole point: an unparseable price, a missing field, an
//! unknown `type`, or non-JSON text never drops a message or substitutes a zero
//! (the hard "no silent failures" rule). It becomes a raw record the operator
//! can inspect, and we tighten the typed path once the shape is understood.
//!
//! Wire shapes are confirmed against the live feed (see open-items.md):
//! - snapshot `msg`: `{market_ticker, market_id, yes_dollars_fp:[[price,size]…], no_dollars_fp:[…]}` (no timestamp — the record's `ts` is the receive time).
//! - delta `msg`: `{market_ticker, price_dollars, delta_fp, side, ts, ts_ms}`.
//! - trade `msg`: `{trade_id, market_ticker, yes_price_dollars, no_price_dollars, count_fp, taker_outcome_side, taker_book_side, ts, ts_ms}`.
//!
//! delta/trade carry an exchange `ts_ms`; the record's `ts` uses it when present,
//! else the receive time. The [`Envelope`](kdp_core::Envelope)'s `recv_ts` is
//! always the receive time regardless.

use chrono::DateTime;
use serde_json::Value;

use kdp_core::{
    BookSide, MicroDollars, OrderbookDelta, OrderbookSnapshot, PriceLevel, QtyDelta, RawFallback,
    RecordKind, RestingQty, Side, Ticker, Timestamp, Trade,
};

/// On-disk channel name for order-book snapshots + deltas.
pub const CHANNEL_ORDERBOOK: &str = "orderbook";

/// On-disk channel name for the trade tape.
pub const CHANNEL_TRADE: &str = "trade";

/// A decoded WS frame: its sequence/subscription ids plus the classified outcome.
#[derive(Debug)]
pub struct DecodedFrame {
    /// Server subscription id (top-level `sid`), if present.
    pub sid: Option<i64>,
    /// Per-subscription sequence number (top-level `seq`), if present.
    pub seq: Option<u64>,
    /// What the frame turned out to be.
    pub outcome: FrameOutcome,
}

/// The classification of a decoded frame.
#[derive(Debug)]
pub enum FrameOutcome {
    /// A persistable data record routed to `channel`/`ticker`.
    Data {
        /// On-disk channel (`CHANNEL_ORDERBOOK` / `CHANNEL_TRADE`).
        channel: &'static str,
        /// Market the record belongs to.
        ticker: Ticker,
        /// The decoded record (Snapshot/Delta/Trade).
        record: RecordKind,
    },
    /// A non-data control frame (e.g. `subscribed`, `ok`) — observed, not persisted.
    Control {
        /// The wire `type`.
        msg_type: String,
    },
    /// A server-sent `error` frame.
    ServerError {
        /// Human-readable detail.
        detail: String,
    },
    /// Could not decode into a known data record; capture verbatim. Routing hints
    /// (`channel`/`ticker`) are best-effort so the session can still place the
    /// raw record near the data it concerns.
    Raw {
        /// Best-effort channel, if recognizable.
        channel: Option<&'static str>,
        /// Best-effort ticker, if extractable.
        ticker: Option<Ticker>,
        /// The verbatim fallback record.
        fallback: RawFallback,
    },
}

/// Decode one WS text frame into a [`DecodedFrame`], never panicking and never
/// dropping data: anything unrecognized or unparseable becomes a [`RawFallback`].
pub fn decode_frame(text: &str, recv_ts: Timestamp) -> DecodedFrame {
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(e) => {
            return DecodedFrame {
                sid: None,
                seq: None,
                outcome: raw_outcome(
                    None,
                    None,
                    None,
                    format!("invalid JSON frame: {e}"),
                    Value::String(text.to_string()),
                ),
            };
        }
    };

    let msg_type = value
        .get("type")
        .and_then(Value::as_str)
        .map(str::to_string);
    let sid = value.get("sid").and_then(Value::as_i64);
    let seq = value.get("seq").and_then(Value::as_u64);
    let msg = value.get("msg");

    let outcome = match msg_type.as_deref() {
        Some("orderbook_snapshot") => {
            decode_data(CHANNEL_ORDERBOOK, "orderbook_snapshot", msg, |m| {
                decode_snapshot(m, recv_ts)
            })
        }
        Some("orderbook_delta") => decode_data(CHANNEL_ORDERBOOK, "orderbook_delta", msg, |m| {
            decode_delta(m, recv_ts)
        }),
        Some("trade") => decode_data(CHANNEL_TRADE, "trade", msg, |m| decode_trade(m, recv_ts)),
        Some(ctrl @ ("subscribed" | "ok")) => FrameOutcome::Control {
            msg_type: ctrl.to_string(),
        },
        Some("error") => {
            let detail = value
                .get("msg")
                .map(|m| {
                    m.get("msg")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| m.to_string())
                })
                .unwrap_or_else(|| value.to_string());
            FrameOutcome::ServerError { detail }
        }
        Some(other) => raw_outcome(
            Some(other),
            None,
            None,
            format!("unknown frame type {other:?}"),
            msg.cloned().unwrap_or_else(|| value.clone()),
        ),
        None => raw_outcome(
            None,
            None,
            None,
            "frame missing string 'type'".to_string(),
            value.clone(),
        ),
    };

    DecodedFrame { sid, seq, outcome }
}

/// Run `decode` over the frame's `msg`; on success emit `Data`, on any error emit
/// `Raw` (preserving the original `msg` and a best-effort ticker).
fn decode_data(
    channel: &'static str,
    raw_type: &str,
    msg: Option<&Value>,
    decode: impl FnOnce(&Value) -> Result<(Ticker, RecordKind), String>,
) -> FrameOutcome {
    let Some(msg) = msg else {
        return raw_outcome(
            Some(raw_type),
            Some(channel),
            None,
            "frame missing 'msg' object".to_string(),
            Value::Null,
        );
    };
    match decode(msg) {
        Ok((ticker, record)) => FrameOutcome::Data {
            channel,
            ticker,
            record,
        },
        Err(error) => {
            let ticker = msg
                .get("market_ticker")
                .and_then(Value::as_str)
                .map(|s| Ticker(s.to_string()));
            raw_outcome(Some(raw_type), Some(channel), ticker, error, msg.clone())
        }
    }
}

fn raw_outcome(
    raw_type: Option<&str>,
    channel: Option<&'static str>,
    ticker: Option<Ticker>,
    error: String,
    payload: Value,
) -> FrameOutcome {
    FrameOutcome::Raw {
        channel,
        ticker: ticker.clone(),
        fallback: RawFallback {
            raw_type: raw_type.map(str::to_string),
            ticker,
            error,
            payload,
        },
    }
}

fn decode_snapshot(msg: &Value, recv_ts: Timestamp) -> Result<(Ticker, RecordKind), String> {
    let ticker = ticker_of(msg)?;
    let yes = decode_levels(msg.get("yes_dollars_fp"))?;
    let no = decode_levels(msg.get("no_dollars_fp"))?;
    let record = RecordKind::Snapshot(OrderbookSnapshot {
        ticker: ticker.clone(),
        ts: recv_ts,
        yes,
        no,
    });
    Ok((ticker, record))
}

fn decode_delta(msg: &Value, recv_ts: Timestamp) -> Result<(Ticker, RecordKind), String> {
    let ticker = ticker_of(msg)?;
    let side = side_of(msg.get("side"))?;
    let price_s = str_field(msg, "price_dollars")?;
    let price =
        MicroDollars::parse(price_s).map_err(|e| format!("price_dollars {price_s:?}: {e}"))?;
    let delta_s = str_field(msg, "delta_fp")?;
    let delta = QtyDelta::parse(delta_s).map_err(|e| format!("delta_fp {delta_s:?}: {e}"))?;
    let record = RecordKind::Delta(OrderbookDelta {
        ticker: ticker.clone(),
        ts: wire_ts(msg, recv_ts),
        side,
        price,
        delta,
    });
    Ok((ticker, record))
}

fn decode_trade(msg: &Value, recv_ts: Timestamp) -> Result<(Ticker, RecordKind), String> {
    let ticker = ticker_of(msg)?;
    let price_s = str_field(msg, "yes_price_dollars")?;
    let price =
        MicroDollars::parse(price_s).map_err(|e| format!("yes_price_dollars {price_s:?}: {e}"))?;
    let count_s = str_field(msg, "count_fp")?;
    let count = RestingQty::parse(count_s).map_err(|e| format!("count_fp {count_s:?}: {e}"))?;
    let taker_side = side_of(msg.get("taker_outcome_side"))?;
    let taker_book_side = match msg.get("taker_book_side") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "bid" => Some(BookSide::Bid),
        Some(Value::String(s)) if s == "ask" => Some(BookSide::Ask),
        Some(other) => return Err(format!("unknown taker_book_side {other}")),
    };
    let trade_id = msg
        .get("trade_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let record = RecordKind::Trade(Trade {
        ticker: ticker.clone(),
        ts: wire_ts(msg, recv_ts),
        price,
        count,
        taker_side,
        taker_book_side,
        trade_id,
    });
    Ok((ticker, record))
}

/// Extract `market_ticker` as a [`Ticker`], or an error.
fn ticker_of(msg: &Value) -> Result<Ticker, String> {
    Ok(Ticker(str_field(msg, "market_ticker")?.to_string()))
}

/// Extract a required string field.
fn str_field<'a>(msg: &'a Value, key: &str) -> Result<&'a str, String> {
    msg.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string field {key:?}"))
}

/// Decode a yes/no side from a `"yes"`/`"no"` JSON value.
fn side_of(value: Option<&Value>) -> Result<Side, String> {
    match value.and_then(Value::as_str) {
        Some("yes") => Ok(Side::Yes),
        Some("no") => Ok(Side::No),
        other => Err(format!("invalid side {other:?}")),
    }
}

/// Decode order-book levels (`[[price_str, size_str], …]`) into [`PriceLevel`]s.
///
/// An **absent or null** levels field is a legitimately empty book side — a
/// settled/closed market sends a snapshot of just `{market_id, market_ticker}`
/// with no `*_dollars_fp` (confirmed live at settlement). Treat that as empty.
/// A **present but non-array** value is genuinely malformed and still errors
/// (preserving "no silent failures" — it becomes a `RawFallback`, not an empty book).
fn decode_levels(value: Option<&Value>) -> Result<Vec<PriceLevel>, String> {
    let array = match value {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(present) => present.as_array().ok_or("non-array levels")?,
    };
    let mut levels = Vec::with_capacity(array.len());
    for level in array {
        let pair = level.as_array().ok_or("level is not an array")?;
        let price_s = pair
            .first()
            .and_then(Value::as_str)
            .ok_or("level price not a string")?;
        let size_s = pair
            .get(1)
            .and_then(Value::as_str)
            .ok_or("level size not a string")?;
        let price =
            MicroDollars::parse(price_s).map_err(|e| format!("level price {price_s:?}: {e}"))?;
        let quantity =
            RestingQty::parse(size_s).map_err(|e| format!("level size {size_s:?}: {e}"))?;
        levels.push(PriceLevel { price, quantity });
    }
    Ok(levels)
}

/// The wire event time from `ts_ms`, falling back to the receive time.
fn wire_ts(msg: &Value, recv_ts: Timestamp) -> Timestamp {
    msg.get("ts_ms")
        .and_then(Value::as_i64)
        .and_then(DateTime::from_timestamp_millis)
        .map(Timestamp)
        .unwrap_or(recv_ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use kdp_core::{BookSide, MicroDollars, QtyDelta, RecordKind, RestingQty, Side, Timestamp};

    fn recv() -> Timestamp {
        "2026-05-30T00:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("rfc3339")
            .into()
    }

    const SNAP: &str = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"KXBTCD-26MAY3017-T72999.99","market_id":"5823f7f2","yes_dollars_fp":[["0.0100","109767.00"],["0.0200","282.00"]],"no_dollars_fp":[["0.9900","5.00"]]}}"#;
    const DELTA: &str = r#"{"type":"orderbook_delta","sid":1,"seq":4,"msg":{"market_ticker":"KXBTCD-26MAY3017-T72999.99","market_id":"5823f7f2","price_dollars":"0.7000","delta_fp":"-5000.00","side":"yes","ts":"2026-05-29T23:18:53Z","ts_ms":1780096733014}}"#;
    const TRADE: &str = r#"{"type":"trade","sid":2,"seq":1,"msg":{"trade_id":"cd6c95cc-46e1-4654-faa5-1636dec2e5fe","market_ticker":"KXBTCD-26MAY3017-T73499.99","yes_price_dollars":"0.4200","no_price_dollars":"0.5800","count_fp":"3.00","taker_side":"yes","taker_outcome_side":"yes","taker_book_side":"bid","ts":1780096775,"ts_ms":1780096775004}}"#;
    const SUBSCRIBED: &str =
        r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1}}"#;

    #[test]
    fn decodes_snapshot_with_levels_and_recv_ts() {
        let frame = decode_frame(SNAP, recv());
        assert_eq!(frame.sid, Some(1));
        assert_eq!(frame.seq, Some(1));
        match frame.outcome {
            FrameOutcome::Data {
                channel,
                ticker,
                record,
            } => {
                assert_eq!(channel, CHANNEL_ORDERBOOK);
                assert_eq!(ticker.as_str(), "KXBTCD-26MAY3017-T72999.99");
                match record {
                    RecordKind::Snapshot(s) => {
                        assert_eq!(s.ts, recv(), "snapshot ts is the receive time");
                        assert_eq!(s.yes[0].price, MicroDollars(10_000)); // 0.0100
                        assert_eq!(s.yes[0].quantity, RestingQty(10_976_700)); // 109767.00
                        assert_eq!(s.yes[1].price, MicroDollars(20_000));
                        assert_eq!(s.no[0].price, MicroDollars(990_000)); // 0.9900
                        assert_eq!(s.no[0].quantity, RestingQty(500)); // 5.00
                    }
                    other => panic!("expected snapshot, got {other:?}"),
                }
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[test]
    fn decodes_delta_with_signed_qty_and_wire_ts() {
        let frame = decode_frame(DELTA, recv());
        assert_eq!(frame.seq, Some(4));
        match frame.outcome {
            FrameOutcome::Data {
                channel,
                ticker,
                record,
            } => {
                assert_eq!(channel, CHANNEL_ORDERBOOK);
                assert_eq!(ticker.as_str(), "KXBTCD-26MAY3017-T72999.99");
                match record {
                    RecordKind::Delta(d) => {
                        assert_eq!(d.side, Side::Yes);
                        assert_eq!(d.price, MicroDollars(700_000)); // 0.7000
                        assert_eq!(d.delta, QtyDelta(-500_000)); // -5000.00
                        let expected = Timestamp(
                            DateTime::from_timestamp_millis(1_780_096_733_014).expect("ts"),
                        );
                        assert_eq!(d.ts, expected, "delta ts from wire ts_ms");
                    }
                    other => panic!("expected delta, got {other:?}"),
                }
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[test]
    fn decodes_trade_to_trade_channel() {
        let frame = decode_frame(TRADE, recv());
        assert_eq!(frame.sid, Some(2));
        match frame.outcome {
            FrameOutcome::Data {
                channel,
                ticker,
                record,
            } => {
                assert_eq!(channel, CHANNEL_TRADE);
                assert_eq!(ticker.as_str(), "KXBTCD-26MAY3017-T73499.99");
                match record {
                    RecordKind::Trade(t) => {
                        assert_eq!(t.price, MicroDollars(420_000)); // yes 0.4200
                        assert_eq!(t.count, RestingQty(300)); // 3.00
                        assert_eq!(t.taker_side, Side::Yes);
                        assert_eq!(t.taker_book_side, Some(BookSide::Bid));
                        assert_eq!(
                            t.trade_id.as_deref(),
                            Some("cd6c95cc-46e1-4654-faa5-1636dec2e5fe")
                        );
                    }
                    other => panic!("expected trade, got {other:?}"),
                }
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[test]
    fn subscribed_is_a_control_frame() {
        match decode_frame(SUBSCRIBED, recv()).outcome {
            FrameOutcome::Control { msg_type } => assert_eq!(msg_type, "subscribed"),
            other => panic!("expected Control, got {other:?}"),
        }
    }

    #[test]
    fn error_frame_is_server_error() {
        let text = r#"{"type":"error","msg":{"code":6,"msg":"already subscribed"}}"#;
        match decode_frame(text, recv()).outcome {
            FrameOutcome::ServerError { detail } => {
                assert!(detail.contains("already subscribed"), "detail: {detail}")
            }
            other => panic!("expected ServerError, got {other:?}"),
        }
    }

    #[test]
    fn invalid_json_becomes_raw_not_dropped() {
        match decode_frame("this is not json", recv()).outcome {
            FrameOutcome::Raw { fallback, .. } => {
                assert!(!fallback.error.is_empty());
                assert_eq!(fallback.payload, serde_json::json!("this is not json"));
            }
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    #[test]
    fn unknown_type_becomes_raw() {
        let text = r#"{"type":"some_future_channel","sid":9,"seq":3,"msg":{"x":1}}"#;
        let frame = decode_frame(text, recv());
        assert_eq!(frame.sid, Some(9));
        assert_eq!(frame.seq, Some(3));
        match frame.outcome {
            FrameOutcome::Raw { fallback, .. } => {
                assert_eq!(fallback.raw_type.as_deref(), Some("some_future_channel"));
            }
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    #[test]
    fn over_precise_price_is_raw_with_payload_preserved_not_dropped() {
        // 7-dp price exceeds MicroDollars' 6-dp scale -> must be captured, not zeroed.
        let bad = r#"{"type":"orderbook_delta","sid":1,"seq":9,"msg":{"market_ticker":"KXTEST","price_dollars":"0.1234567","delta_fp":"1.00","side":"yes","ts_ms":1}}"#;
        match decode_frame(bad, recv()).outcome {
            FrameOutcome::Raw {
                channel,
                ticker,
                fallback,
            } => {
                assert_eq!(channel, Some(CHANNEL_ORDERBOOK));
                assert_eq!(ticker.as_ref().map(|t| t.as_str()), Some("KXTEST"));
                assert!(!fallback.error.is_empty());
                assert_eq!(fallback.payload["price_dollars"], "0.1234567");
            }
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_with_a_bad_level_is_raw() {
        let bad = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"KXTEST","yes_dollars_fp":[["0.01","bogus"]],"no_dollars_fp":[]}}"#;
        match decode_frame(bad, recv()).outcome {
            FrameOutcome::Raw { ticker, .. } => {
                assert_eq!(ticker.as_ref().map(|t| t.as_str()), Some("KXTEST"));
            }
            other => panic!("expected Raw, got {other:?}"),
        }
    }

    // A settled/closed market sends a snapshot whose `*_dollars_fp` fields are
    // absent entirely (real shape observed at the IPL final settlement, 2026-05-31):
    // `{market_id, market_ticker}` with no levels. That is a legitimately EMPTY
    // book, not a malformed frame, and must decode to an empty Snapshot — not a
    // RawFallback (which would flip the session manifest to INCOMPLETE).
    const SETTLED_SNAP: &str = r#"{"type":"orderbook_snapshot","sid":1,"seq":221173,"msg":{"market_id":"d45f2a3f-483e-4087-a25d-ca8c04b33fd0","market_ticker":"KXIPL-26-RCB"}}"#;

    #[test]
    fn settled_snapshot_with_absent_levels_decodes_as_empty_book() {
        match decode_frame(SETTLED_SNAP, recv()).outcome {
            FrameOutcome::Data {
                channel,
                ticker,
                record,
            } => {
                assert_eq!(channel, CHANNEL_ORDERBOOK);
                assert_eq!(ticker.as_str(), "KXIPL-26-RCB");
                match record {
                    RecordKind::Snapshot(s) => {
                        assert_eq!(s.ts, recv());
                        assert!(s.yes.is_empty(), "absent yes levels -> empty book side");
                        assert!(s.no.is_empty(), "absent no levels -> empty book side");
                    }
                    other => panic!("expected snapshot, got {other:?}"),
                }
            }
            other => panic!("expected empty-book Data, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_with_null_levels_decodes_as_empty_book() {
        let text = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"KXTEST","yes_dollars_fp":null,"no_dollars_fp":null}}"#;
        match decode_frame(text, recv()).outcome {
            FrameOutcome::Data { record, .. } => match record {
                RecordKind::Snapshot(s) => {
                    assert!(s.yes.is_empty() && s.no.is_empty());
                }
                other => panic!("expected snapshot, got {other:?}"),
            },
            other => panic!("expected empty-book Data, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_with_non_array_levels_is_still_raw() {
        // Strictness preserved: a present-but-wrong-type levels value is NOT an
        // empty book — it is malformed and must still become a RawFallback.
        let text = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"KXTEST","yes_dollars_fp":42,"no_dollars_fp":[]}}"#;
        match decode_frame(text, recv()).outcome {
            FrameOutcome::Raw { ticker, .. } => {
                assert_eq!(ticker.as_ref().map(|t| t.as_str()), Some("KXTEST"));
            }
            other => panic!("expected Raw, got {other:?}"),
        }
    }
}
