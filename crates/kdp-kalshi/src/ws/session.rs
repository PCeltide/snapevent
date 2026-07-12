//! The capture read loop with reconnect + inline gap detection.
//!
//! [`run_session`] is the producer half of the capture pipeline (D6): it owns
//! the live WebSocket, stamps a receive time on every frame, decodes it
//! ([`crate::ws::protocol`]), and forwards a [`CaptureEvent`] to a bounded
//! channel — `send().await` backpressure means a slow consumer slows reads
//! rather than dropping data (D9). It never touches disk.
//!
//! Resilience (Phase 2.8):
//! - **Reconnect with backoff.** A dropped/idle/errored connection is NOT
//!   terminal: the session waits ([`Backoff`], exponential + jitter) and
//!   reconnects, emitting an inline `Gap{Reconnect}` into every `(ticker,
//!   channel)` stream and resetting the per-sid sequence trackers, so the holes
//!   are positionally exact and the fresh snapshots re-baseline. Only an external
//!   shutdown (ctrl-c / `--duration`) or a closed consumer ends the session.
//! - **Inline seq-gap detection.** A per-`sid` [`SeqTracker`] (D19) classifies
//!   each sequenced message; a jump emits an inline `Gap{SeqJump}` into every
//!   ticker on that channel (the seq is shared across them). Targeted
//!   resubscribe-for-fresh-snapshot on a seq jump is deferred (open-items:
//!   Kalshi resubscribe semantics unconfirmed) — the gap machine adopts the new
//!   seq and continues, and a reconnect is the hard re-baseline.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, instrument, warn};

use kdp_core::{GapMarker, GapReason, RecordKind, Ticker, Timestamp};

use crate::auth::KalshiCredentials;
use crate::ws::backoff::Backoff;
use crate::ws::gap::{SeqTracker, SeqVerdict};
use crate::ws::protocol::{
    decode_frame, DecodedFrame, FrameOutcome, CHANNEL_ORDERBOOK, CHANNEL_TRADE,
};
use crate::ws::{connect_and_subscribe, ORDERBOOK_CHANNEL, TRADE_CHANNEL};
use crate::KalshiError;

/// On-disk channels a capture produces (for reconnect gap fan-out).
const ONDISK_CHANNELS: [&str; 2] = [CHANNEL_ORDERBOOK, CHANNEL_TRADE];

/// One decoded frame plus the receive time stamped when it arrived.
#[derive(Debug)]
pub struct CaptureEvent {
    /// Receive wall-clock time (UTC), stamped at read (or at gap synthesis).
    pub recv_ts: Timestamp,
    /// The decoded (or synthesized, for gap markers) frame.
    pub frame: DecodedFrame,
}

/// Why a session ended (terminal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEnd {
    /// External shutdown (ctrl-c / `--duration`).
    Shutdown,
    /// The consumer dropped the event receiver.
    SinkClosed,
}

/// Stats from a session run (across reconnects).
#[derive(Debug, Default, Clone)]
pub struct SessionStats {
    /// Last upgrade status (101 on success).
    pub status: Option<u16>,
    /// Total frames read from the wire (excludes synthesized gap markers).
    pub frames: u64,
    /// Frame count by label. Keyed by `Cow<'static, str>` so the common static
    /// labels (snapshot/delta/trade/gap/raw) need no per-frame allocation.
    pub by_type: BTreeMap<Cow<'static, str>, u64>,
    /// Number of (re)connections made.
    pub connects: u64,
    /// Inline gap markers emitted (seq jumps + reconnects).
    pub gaps: u64,
    /// Why the loop ended.
    pub end: Option<SessionEnd>,
}

/// Run the resilient capture session against `url` (production passes
/// [`crate::ws::KALSHI_WS_URL`]; tests pass a mock). Reconnects with backoff
/// until `shutdown` resolves or the consumer drops the receiver.
#[instrument(skip(creds, sink, shutdown), fields(tickers = tickers.len()))]
pub async fn run_session(
    creds: &KalshiCredentials,
    url: &str,
    tickers: &[String],
    sink: mpsc::Sender<CaptureEvent>,
    shutdown: impl Future<Output = ()>,
    idle: Duration,
) -> Result<SessionStats, KalshiError> {
    let mut stats = SessionStats::default();
    let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    tokio::pin!(shutdown);

    let mut first = true;
    loop {
        // --- connect (with shutdown + backoff-on-failure) ---
        let connect =
            connect_and_subscribe(creds, url, tickers, &[ORDERBOOK_CHANNEL, TRADE_CHANNEL]);
        let (mut ws, status) = tokio::select! {
            biased;
            _ = &mut shutdown => { stats.end = Some(SessionEnd::Shutdown); break; }
            result = connect => match result {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "connect failed; backing off");
                    if sleep_or_shutdown(shutdown.as_mut(), backoff.next(jitter())).await {
                        stats.end = Some(SessionEnd::Shutdown);
                        break;
                    }
                    continue;
                }
            }
        };
        stats.status = Some(status);
        stats.connects += 1;
        backoff.reset();

        // On a reconnect, mark the hole in every stream before fresh data flows.
        if !first {
            let recv_ts: Timestamp = Utc::now().into();
            if !emit_reconnect_gaps(&sink, recv_ts, tickers, &mut stats).await {
                stats.end = Some(SessionEnd::SinkClosed);
                break;
            }
        }
        first = false;

        // --- inner read loop ---
        let mut trackers: HashMap<i64, SeqTracker> = HashMap::new();
        let reconnect = loop {
            let next = tokio::select! {
                biased;
                _ = &mut shutdown => { stats.end = Some(SessionEnd::Shutdown); break false; }
                next = tokio::time::timeout(idle, ws.next()) => next,
            };
            match next {
                Err(_) => {
                    warn!(idle_secs = idle.as_secs(), "idle timeout; reconnecting");
                    break true;
                }
                Ok(None) => {
                    warn!("server closed stream; reconnecting");
                    break true;
                }
                Ok(Some(Err(e))) => {
                    warn!(error = %e, "websocket error; reconnecting");
                    break true;
                }
                Ok(Some(Ok(Message::Text(text)))) => {
                    let recv_ts: Timestamp = Utc::now().into();
                    let frame = decode_frame(&text, recv_ts);
                    stats.frames += 1;
                    *stats.by_type.entry(frame_label(&frame)).or_default() += 1;

                    // No silent anomalies: a frame we could not fully decode is
                    // persisted as a RawFallback AND warned here; a server error
                    // frame is surfaced too (it is otherwise not persisted).
                    match &frame.outcome {
                        FrameOutcome::Raw { fallback, .. } => warn!(
                            error = %fallback.error,
                            raw_type = ?fallback.raw_type,
                            "raw fallback frame: could not decode wire message"
                        ),
                        FrameOutcome::ServerError { detail } => {
                            warn!(detail = %detail, "server sent an error frame")
                        }
                        _ => {}
                    }

                    // Per-sid gap detection for sequenced data records.
                    match classify_seq(&frame, &mut trackers) {
                        SeqOutcome::Duplicate => continue, // do not persist a duplicate
                        SeqOutcome::Gap {
                            channel,
                            sid,
                            last_seq,
                            observed_seq,
                        } => {
                            if !emit_seq_gaps(
                                &sink,
                                recv_ts,
                                tickers,
                                channel,
                                sid,
                                last_seq,
                                observed_seq,
                                &mut stats,
                            )
                            .await
                            {
                                stats.end = Some(SessionEnd::SinkClosed);
                                break false;
                            }
                            // fall through: persist the gap-triggering record too
                        }
                        SeqOutcome::Persist => {}
                    }

                    if sink.send(CaptureEvent { recv_ts, frame }).await.is_err() {
                        stats.end = Some(SessionEnd::SinkClosed);
                        break false;
                    }
                }
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    if let Err(e) = ws.send(Message::Pong(payload)).await {
                        warn!(error = %e, "pong failed; reconnecting");
                        break true;
                    }
                }
                Ok(Some(Ok(Message::Close(frame)))) => {
                    warn!(?frame, "server sent close; reconnecting");
                    break true;
                }
                Ok(Some(Ok(_))) => {}
            }
        };

        let _ = ws.close(None).await;

        if !reconnect {
            // Terminal (shutdown / sink closed).
            break;
        }
        if sleep_or_shutdown(shutdown.as_mut(), backoff.next(jitter())).await {
            stats.end = Some(SessionEnd::Shutdown);
            break;
        }
    }

    info!(
        frames = stats.frames,
        connects = stats.connects,
        gaps = stats.gaps,
        end = ?stats.end,
        "session ended"
    );
    Ok(stats)
}

/// Sleep for `delay`, returning `true` if `shutdown` fired first.
async fn sleep_or_shutdown(
    shutdown: std::pin::Pin<&mut impl Future<Output = ()>>,
    delay: Duration,
) -> bool {
    tokio::select! {
        biased;
        _ = shutdown => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

/// Random jitter in `[0.0, 1.0)` for the backoff (runtime only).
fn jitter() -> f64 {
    rand::random::<f64>()
}

/// What to do with a frame after per-sid sequence checking.
enum SeqOutcome {
    /// Persist the frame normally (in order, snapshot, or not seq-tracked).
    Persist,
    /// A duplicate seq — skip persisting.
    Duplicate,
    /// A gap: emit `Gap{SeqJump}` for `channel`/`sid` then persist the frame.
    Gap {
        channel: &'static str,
        sid: i64,
        last_seq: Option<u64>,
        observed_seq: u64,
    },
}

/// Feed a sequenced data frame to its sid's tracker and classify the result.
/// Non-data / no-sid frames are not seq-tracked and just [`SeqOutcome::Persist`].
fn classify_seq(frame: &DecodedFrame, trackers: &mut HashMap<i64, SeqTracker>) -> SeqOutcome {
    let (sid, channel, is_snapshot) = match (&frame.outcome, frame.sid) {
        (
            FrameOutcome::Data {
                channel, record, ..
            },
            Some(sid),
        ) => (sid, *channel, matches!(record, RecordKind::Snapshot(_))),
        _ => return SeqOutcome::Persist,
    };
    let tracker = trackers.entry(sid).or_default();
    let verdict = if is_snapshot {
        tracker.on_snapshot(frame.seq)
    } else {
        match frame.seq {
            Some(seq) => tracker.on_sequenced(seq),
            None => SeqVerdict::Accept,
        }
    };
    match verdict {
        SeqVerdict::Accept => SeqOutcome::Persist,
        SeqVerdict::Duplicate => SeqOutcome::Duplicate,
        SeqVerdict::Gap {
            last_seq,
            observed_seq,
            ..
        } => SeqOutcome::Gap {
            channel,
            sid,
            last_seq,
            observed_seq,
        },
    }
}

/// Emit an inline `Gap{Reconnect}` into every `(ticker, channel)` stream.
async fn emit_reconnect_gaps(
    sink: &mpsc::Sender<CaptureEvent>,
    recv_ts: Timestamp,
    tickers: &[String],
    stats: &mut SessionStats,
) -> bool {
    for ticker in tickers {
        for channel in ONDISK_CHANNELS {
            let marker = GapMarker {
                reason: GapReason::Reconnect,
                ticker: Ticker(ticker.clone()),
                channel: channel.to_string(),
                last_seq: None,
                observed_seq: None,
                detail: "reconnected after disconnect/idle".to_string(),
            };
            if !send_gap(sink, recv_ts, None, channel, marker, stats).await {
                return false;
            }
        }
    }
    true
}

/// Emit an inline `Gap{SeqJump}` into every ticker on `channel`.
#[allow(clippy::too_many_arguments)]
async fn emit_seq_gaps(
    sink: &mpsc::Sender<CaptureEvent>,
    recv_ts: Timestamp,
    tickers: &[String],
    channel: &'static str,
    sid: i64,
    last_seq: Option<u64>,
    observed_seq: u64,
    stats: &mut SessionStats,
) -> bool {
    warn!(
        sid,
        ?last_seq,
        observed_seq,
        channel,
        "sequence gap detected on channel"
    );
    for ticker in tickers {
        let marker = GapMarker {
            reason: GapReason::SeqJump,
            ticker: Ticker(ticker.clone()),
            channel: channel.to_string(),
            last_seq,
            observed_seq: Some(observed_seq),
            detail: format!("seq jump on sid {sid}"),
        };
        if !send_gap(sink, recv_ts, Some(sid), channel, marker, stats).await {
            return false;
        }
    }
    true
}

/// Send one synthesized gap marker as a `CaptureEvent` (flows through the same
/// persistence path as data via `RecordKind::Gap`). Returns false if sink closed.
async fn send_gap(
    sink: &mpsc::Sender<CaptureEvent>,
    recv_ts: Timestamp,
    sid: Option<i64>,
    channel: &'static str,
    marker: GapMarker,
    stats: &mut SessionStats,
) -> bool {
    let ticker = marker.ticker.clone();
    let frame = DecodedFrame {
        sid,
        seq: None,
        outcome: FrameOutcome::Data {
            channel,
            ticker,
            record: RecordKind::Gap(marker),
        },
    };
    stats.gaps += 1;
    sink.send(CaptureEvent { recv_ts, frame }).await.is_ok()
}

/// A short label for a decoded frame, for the by-type stats histogram. Returns a
/// borrowed `&'static str` for the common fixed labels (no per-frame allocation);
/// only the rare `control:<type>` label allocates.
fn frame_label(frame: &DecodedFrame) -> Cow<'static, str> {
    match &frame.outcome {
        FrameOutcome::Data { record, .. } => Cow::Borrowed(match record {
            RecordKind::Snapshot(_) => "snapshot",
            RecordKind::Delta(_) => "delta",
            RecordKind::Trade(_) => "trade",
            RecordKind::Gap(_) => "gap",
            RecordKind::Raw(_) => "raw",
            // ponytail: capture never emits Verify (kdp-process synthesizes it
            // offline); arm exists only so this match stays exhaustive.
            RecordKind::Verify(_) => "verify",
        }),
        FrameOutcome::Control { msg_type } => Cow::Owned(format!("control:{msg_type}")),
        FrameOutcome::ServerError { .. } => Cow::Borrowed("error"),
        FrameOutcome::Raw { .. } => Cow::Borrowed("raw"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws::protocol::decode_frame;

    fn recv() -> Timestamp {
        "2026-05-30T00:00:00Z"
            .parse::<chrono::DateTime<Utc>>()
            .expect("rfc3339")
            .into()
    }

    #[test]
    fn frame_label_distinguishes_kinds() {
        let delta = decode_frame(
            r#"{"type":"orderbook_delta","sid":1,"seq":4,"msg":{"market_ticker":"K","price_dollars":"0.70","delta_fp":"-5.00","side":"yes","ts_ms":1}}"#,
            recv(),
        );
        assert_eq!(frame_label(&delta), "delta");

        let sub = decode_frame(r#"{"type":"subscribed","msg":{"sid":1}}"#, recv());
        assert_eq!(frame_label(&sub), "control:subscribed");

        let raw = decode_frame("not json", recv());
        assert_eq!(frame_label(&raw), "raw");
    }

    #[test]
    fn classify_seq_flags_a_jump() {
        let mut trackers: HashMap<i64, SeqTracker> = HashMap::new();
        let snap = decode_frame(
            r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"K","yes_dollars_fp":[],"no_dollars_fp":[]}}"#,
            recv(),
        );
        assert!(matches!(
            classify_seq(&snap, &mut trackers),
            SeqOutcome::Persist
        ));
        let d2 = decode_frame(
            r#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"K","price_dollars":"0.70","delta_fp":"1.00","side":"yes","ts_ms":1}}"#,
            recv(),
        );
        assert!(matches!(
            classify_seq(&d2, &mut trackers),
            SeqOutcome::Persist
        ));
        let d5 = decode_frame(
            r#"{"type":"orderbook_delta","sid":1,"seq":5,"msg":{"market_ticker":"K","price_dollars":"0.70","delta_fp":"1.00","side":"yes","ts_ms":1}}"#,
            recv(),
        );
        match classify_seq(&d5, &mut trackers) {
            SeqOutcome::Gap {
                channel,
                sid,
                last_seq,
                observed_seq,
            } => {
                assert_eq!(channel, CHANNEL_ORDERBOOK);
                assert_eq!(sid, 1);
                assert_eq!(last_seq, Some(2));
                assert_eq!(observed_seq, 5);
            }
            _ => panic!("expected a gap"),
        }
    }

    // A throwaway PKCS#8 test key (not a credential) for the mock-server test.
    const TEST_KEY_PKCS8: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCs9d6u5OrN4b4b
zASLghdDeKjmbwLmLWeLPmuc/hP2OT6uvrWxBJQeuGb5uNYOBmQH1LtvVh84qUct
JpZmUNUSRXiKH3rbyQeJBO5SDx0Qn6pXgv4rJPc20PlItfzPAA9VtEN7oWznbhTG
lL/3SzEBjC6BujtVo4goOi6wu0XaXmjje4hxx4mDblygSktZ0GF32CJw4Hos53Ro
vozyGeM76ClpjxjZO7oPn6Ogqg3KU3HQWzv93/RqU8b79g+6T1P3ehjCL6WAOPvB
hobJx8uh1raRJfGnMMRubsJ/epLPfPoxRdAVntkFq6srhtsZZu62E6ktgTPpZ0w8
9zNP9rtBAgMBAAECggEAEnvniOxY9Zi0REHjkx8w1O/uU6C11XCGb3iZGpAtWkkt
huebAOh62zpHcv+gYfj9NA21hu/+v9juDN1iAbGOG85F1AkKNwvUXNMklFYeJo/R
rgQx8ogYTV48OIY5FswsV13qP6pK5QQRf1QX8dncymFh+lDd6h3jErLTw4+3/DP2
c4TwxmPODmOfnTyNxKXJsqcPbGa7HBN6f7ugNCx+KctuJAlR4PyKy4JkVTBpJP7v
RH1J+LwudLbbL7N/LpSlhK3gRfT4X4pBkam2lGPgtkr9eBfJyo5SEULwBviE3ce9
4FIsYi/aRR+NPiUjAjLXpAXwgif+pcFj0EwV6qgH4wKBgQDsJU2CYPiaL0W6NzQ+
irCT4CDs5UYzGnvC+iYC0PWVhVtQ68W+tTxnlnC6fAjhC1qx/fNOxEbBDnGCXHTc
y2GgICMCu32NhyL/Sq6l6huf0lmuuX126sH1avKgGkLTOMVCKyLINlPWHn2lYsap
ySB7tgdc7e+tgjaB2p/HspOK4wKBgQC7gJg4GspAjqJpUoJuTwcbKCtqXp7YnVMz
bntM/guWrW7rI8+CDkU6h8KrYvJqmCnSd0TgElra7sQ4aB3Tdtt4z5hbPgs8j4Is
pFbnazJqJhbu1pUncOttGgAZSQXTOvONyujYeevVQd7O8yq/HuyOkJsfD6lgOmnq
+y3GP1MGiwKBgQDVPH34LG5wdB1noK/Jhd0bOvkgUYyJWvHEx7OJOX15rfkeYjin
E+res0dJ7fTqmiEktud9CdnGPK+dArX4JqMaP8q9jeY65XthweNhKLwXHpAjKZY0
ypmobhF3Jx+OsiXVsTPwTLZ5lADrVf2ElXyCmYWekbCrIfjsWymK3yNB9wKBgFOa
xUTO/TvH3bckqS/SYRLE2Ib3ZdCkZcLbEnOEG1q2PmzubMpK3qd4fV66IelRq+RC
dh2LUaOpLykPk60EpFu8BO06Pvxj6OFK7c0GSVZ3YWZhm+QYP4FIRJ8Bpm1HLe4d
ebF8u6E9W8HfP0I04bm31NMGwrk7kprKIODyv2x9AoGBAMkBEToLzyqGs7StzyPH
CYC2s34wvDedWFmHmjL0pXaM4THHranHWpn+Hp3pxu2Xzc4C92TAwrrIW8bKZ8w1
G44t8znFg+RVrK6kKvil2bM1ISpG+EtC1j9O8I0W5M0ozDKTckZwbF3VFEUvYiaw
FSCdm2zv/hgDDD5k5SdmeEfR
-----END PRIVATE KEY-----
";

    /// Mock-server integration test (★ Phase 2.8 verification): script a seq jump
    /// and assert the session emits an inline gap marker and loses no delta.
    #[tokio::test]
    async fn mock_server_seq_jump_emits_gap_and_loses_nothing() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;
        use tokio_tungstenite::tungstenite::Message;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        // Server: accept, read the subscribe, send scripted frames (with a seq
        // jump 3 -> 5), then close.
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_async(stream).await.expect("ws accept");
            let _subscribe = ws.next().await; // consume the subscribe command
            let frames = [
                r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1}}"#.to_string(),
                r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"KXTEST","yes_dollars_fp":[["0.50","10.00"]],"no_dollars_fp":[]}}"#.to_string(),
                delta_frame(2),
                delta_frame(3),
                delta_frame(5), // JUMP — seq 4 is missing
                delta_frame(6),
            ];
            for f in frames {
                ws.send(Message::Text(f)).await.expect("send");
            }
            let _ = ws.close(None).await;
        });

        let creds = KalshiCredentials::from_pem("test-kid", TEST_KEY_PKCS8).expect("creds");
        let (tx, mut rx) = mpsc::channel::<CaptureEvent>(100);
        let url = format!("ws://{addr}/");
        let tickers = vec!["KXTEST".to_string()];
        // Shutdown after a short window (after the server closes, run_session would
        // otherwise keep retrying the now-dead port).
        let shutdown = async {
            tokio::time::sleep(Duration::from_millis(700)).await;
        };
        let session = tokio::spawn(async move {
            run_session(&creds, &url, &tickers, tx, shutdown, Duration::from_secs(5)).await
        });

        let mut events = Vec::new();
        while let Some(ev) = rx.recv().await {
            events.push(ev);
        }
        let _ = session.await;
        let _ = server.await;

        // Collect delta seqs and gap markers in arrival order.
        let mut delta_seqs = Vec::new();
        let mut gap_markers = Vec::new();
        for ev in &events {
            if let FrameOutcome::Data { record, .. } = &ev.frame.outcome {
                match record {
                    RecordKind::Delta(_) => delta_seqs.push(ev.frame.seq),
                    RecordKind::Gap(g) => gap_markers.push(g.clone()),
                    _ => {}
                }
            }
        }

        // No delta lost: 2, 3, 5, 6 all delivered (4 is the genuine hole).
        assert_eq!(
            delta_seqs,
            vec![Some(2), Some(3), Some(5), Some(6)],
            "all received deltas forwarded, none dropped"
        );
        // Exactly one seq-jump gap marker, positioned at the hole.
        assert_eq!(gap_markers.len(), 1, "one gap marker for the 3->5 jump");
        let gap = &gap_markers[0];
        assert_eq!(gap.reason, GapReason::SeqJump);
        assert_eq!(gap.last_seq, Some(3));
        assert_eq!(gap.observed_seq, Some(5));
        assert_eq!(gap.ticker.as_str(), "KXTEST");
        assert_eq!(gap.channel, CHANNEL_ORDERBOOK);
    }

    fn delta_frame(seq: u64) -> String {
        format!(
            r#"{{"type":"orderbook_delta","sid":1,"seq":{seq},"msg":{{"market_ticker":"KXTEST","price_dollars":"0.50","delta_fp":"1.00","side":"yes","ts_ms":1}}}}"#
        )
    }

    #[test]
    fn classify_seq_skips_duplicates_and_passes_control() {
        let mut trackers: HashMap<i64, SeqTracker> = HashMap::new();
        let d2 = decode_frame(
            r#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"K","price_dollars":"0.70","delta_fp":"1.00","side":"yes","ts_ms":1}}"#,
            recv(),
        );
        assert!(matches!(
            classify_seq(&d2, &mut trackers),
            SeqOutcome::Persist
        ));
        // same seq again -> duplicate
        let dup = decode_frame(
            r#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"K","price_dollars":"0.70","delta_fp":"1.00","side":"yes","ts_ms":1}}"#,
            recv(),
        );
        assert!(matches!(
            classify_seq(&dup, &mut trackers),
            SeqOutcome::Duplicate
        ));
        // control frame is not seq-tracked
        let sub = decode_frame(r#"{"type":"subscribed","msg":{"sid":1}}"#, recv());
        assert!(matches!(
            classify_seq(&sub, &mut trackers),
            SeqOutcome::Persist
        ));
    }
}
