//! WebSocket client for Kalshi's live L2 market-data feed.
//!
//! The live WebSocket is the **only** source of L2 order-book history (see
//! `docs/adr/ADR-003-capture-vs-backfill.md`). This module owns the wire-level
//! interaction: the authenticated upgrade ([`handshake`]), the subscribe
//! command, and reading frames. Phase 2.4 provides a minimal [`probe`] that
//! connects, subscribes, and logs frames — proving auth + subscribe + receive
//! against a live market before the full decode/capture path is built.
//!
//! Decoding ([`protocol`]), the sequence/gap state machine ([`gap`]), reconnect
//! backoff ([`backoff`]), and the resilient capture loop ([`session`]) build on
//! top; `probe` deliberately does none of those — it only counts frames by wire
//! `type` and is kept as a lightweight live diagnostic.

pub mod backoff;
pub mod gap;
pub mod handshake;
pub mod protocol;
pub mod session;

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, instrument, warn};

use crate::auth::KalshiCredentials;
use crate::KalshiError;

/// The connected (TLS) Kalshi WebSocket stream type.
pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect to the WS feed at `url` (authenticated upgrade) and send one
/// `subscribe` for `channels` × `tickers`. Returns the live stream and the
/// upgrade status (101 on success). Shared by [`probe`] and [`session`]; `url`
/// is a parameter so tests can target a mock server.
#[instrument(skip(creds), fields(tickers = tickers.len()))]
pub async fn connect_and_subscribe(
    creds: &KalshiCredentials,
    url: &str,
    tickers: &[String],
    channels: &[&str],
) -> Result<(WsStream, u16), KalshiError> {
    let request = handshake::authenticated_request(creds, url)?;
    let (mut ws, response) = connect_async(request)
        .await
        .map_err(|e| KalshiError::WebSocket(e.to_string()))?;
    let status = response.status().as_u16();
    info!(status, "websocket upgrade complete");

    let subscribe = subscribe_message(1, channels, tickers);
    ws.send(Message::Text(subscribe))
        .await
        .map_err(|e| KalshiError::WebSocket(e.to_string()))?;
    info!(?tickers, ?channels, "subscribe sent");

    Ok((ws, status))
}

/// Kalshi production WebSocket endpoint for the market-data feed.
pub const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Request path signed for the WebSocket upgrade (the URL's path component).
pub const WS_PATH: &str = "/trade-api/ws/v2";

/// Channel carrying the order-book snapshot + deltas.
pub const ORDERBOOK_CHANNEL: &str = "orderbook_delta";

/// Channel carrying the public trade tape.
pub const TRADE_CHANNEL: &str = "trade";

/// Build a Kalshi `subscribe` command for the given channels and tickers.
///
/// Shape: `{"id":N,"cmd":"subscribe","params":{"channels":[…],"market_tickers":[…]}}`.
pub fn subscribe_message(id: u64, channels: &[&str], tickers: &[String]) -> String {
    serde_json::json!({
        "id": id,
        "cmd": "subscribe",
        "params": { "channels": channels, "market_tickers": tickers },
    })
    .to_string()
}

/// Frame metadata pulled out for logging/diagnostics: the wire `type`, the
/// `sid`/`seq` (top level), and the `msg.market_ticker` when present.
#[derive(Debug, Default)]
struct FrameMeta {
    kind: String,
    sid: Option<i64>,
    seq: Option<u64>,
    ticker: Option<String>,
}

/// Parse the diagnostic metadata from a frame's JSON text.
fn frame_meta(text: &str) -> FrameMeta {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => {
            return FrameMeta {
                kind: "<unparsed>".to_string(),
                ..Default::default()
            }
        }
    };
    FrameMeta {
        kind: value
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("<unknown>")
            .to_string(),
        sid: value.get("sid").and_then(serde_json::Value::as_i64),
        seq: value.get("seq").and_then(serde_json::Value::as_u64),
        ticker: value
            .get("msg")
            .and_then(|m| m.get("market_ticker"))
            .and_then(|t| t.as_str())
            .map(str::to_string),
    }
}

/// Summary of a [`probe`] run: the upgrade status and a count of frames by type.
#[derive(Debug, Default, Clone)]
pub struct ProbeReport {
    /// HTTP status of the upgrade response (101 on success).
    pub status: Option<u16>,
    /// Total frames received.
    pub frames: usize,
    /// Count of frames per wire `type`.
    pub by_type: BTreeMap<String, usize>,
}

/// Connect (authenticated), subscribe to orderbook + trade for `tickers`, and
/// log up to `max_frames` frames (or until `idle` elapses with no frame).
///
/// This is the Phase 2.4 live de-risk: it proves the RSA-PSS handshake yields a
/// 101 upgrade and that we receive an `orderbook_snapshot` followed by deltas.
/// It does not decode or persist anything — that is Phases 2.5/2.7.
#[instrument(skip(creds), fields(tickers = tickers.len(), max_frames))]
pub async fn probe(
    creds: &KalshiCredentials,
    tickers: &[String],
    max_frames: usize,
    idle: Duration,
    dump_raw: bool,
) -> Result<ProbeReport, KalshiError> {
    let (mut ws, status) = connect_and_subscribe(
        creds,
        KALSHI_WS_URL,
        tickers,
        &[ORDERBOOK_CHANNEL, TRADE_CHANNEL],
    )
    .await?;
    let mut report = ProbeReport {
        status: Some(status),
        ..Default::default()
    };

    while report.frames < max_frames {
        match tokio::time::timeout(idle, ws.next()).await {
            Err(_) => {
                warn!(idle_secs = idle.as_secs(), "idle timeout; ending probe");
                break;
            }
            Ok(None) => {
                warn!("stream closed by server");
                break;
            }
            Ok(Some(Err(e))) => return Err(KalshiError::WebSocket(e.to_string())),
            Ok(Some(Ok(Message::Text(text)))) => {
                report.frames += 1;
                let meta = frame_meta(text.as_str());
                *report.by_type.entry(meta.kind.clone()).or_default() += 1;
                if dump_raw {
                    info!(raw = %text, "raw frame");
                }
                // Log sid/seq/ticker for the first frames so the seq/sid model
                // (per-channel vs per-ticker) is visible in the de-risk run.
                if report.frames <= 15 {
                    info!(
                        kind = %meta.kind,
                        sid = ?meta.sid,
                        seq = ?meta.seq,
                        ticker = ?meta.ticker,
                        "frame"
                    );
                } else {
                    debug!(kind = %meta.kind, sid = ?meta.sid, seq = ?meta.seq, "frame");
                }
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                // Keep the connection alive.
                ws.send(Message::Pong(payload))
                    .await
                    .map_err(|e| KalshiError::WebSocket(e.to_string()))?;
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                warn!(?frame, "server sent close");
                break;
            }
            Ok(Some(Ok(_))) => { /* binary/pong/frame — ignore for the probe */ }
        }
    }

    info!(
        frames = report.frames,
        by_type = ?report.by_type,
        "probe complete"
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_message_has_expected_shape() {
        let msg = subscribe_message(
            1,
            &[ORDERBOOK_CHANNEL, TRADE_CHANNEL],
            &["KXTEST".to_string()],
        );
        let value: serde_json::Value = serde_json::from_str(&msg).expect("valid json");
        assert_eq!(value["id"], 1);
        assert_eq!(value["cmd"], "subscribe");
        assert_eq!(value["params"]["channels"][0], "orderbook_delta");
        assert_eq!(value["params"]["channels"][1], "trade");
        assert_eq!(value["params"]["market_tickers"][0], "KXTEST");
    }

    #[test]
    fn frame_meta_reads_type_sid_seq_and_ticker() {
        let meta = frame_meta(
            r#"{"type":"orderbook_delta","sid":1,"seq":42,"msg":{"market_ticker":"KXBTCD-X"}}"#,
        );
        assert_eq!(meta.kind, "orderbook_delta");
        assert_eq!(meta.sid, Some(1));
        assert_eq!(meta.seq, Some(42));
        assert_eq!(meta.ticker.as_deref(), Some("KXBTCD-X"));
    }

    #[test]
    fn frame_meta_handles_non_object_and_missing_fields() {
        let bad = frame_meta("not json");
        assert_eq!(bad.kind, "<unparsed>");
        assert!(bad.sid.is_none() && bad.seq.is_none() && bad.ticker.is_none());

        let no_type = frame_meta(r#"{"sid":1}"#);
        assert_eq!(no_type.kind, "<unknown>");
        assert_eq!(no_type.sid, Some(1));
    }
}
