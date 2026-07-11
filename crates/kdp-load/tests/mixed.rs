//! consumer acceptance checks #4 (dedup) and #5 (gap honesty) against the committed
//! MIXED fixture `tests/fixture/KDPSYNTH-R6MIX`. Unlike the pure-WS captured
//! fixture (roundtrip.rs), this one is SYNTHESIZED -- schema-true Parquet via
//! the same writer helpers the unit tests trust; regenerate with
//! `cargo test -p kdp-load --lib -- --ignored generate_mixed_fixture` -- so
//! REST placement, R6 dedup, and in-stream gaps are exercised through the real
//! Parquet read path. Only the public API is used.

use kdp_load::{Loader, ReplayEvent, TradeSource};

fn fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixture/KDPSYNTH-R6MIX")
}

fn events() -> Vec<ReplayEvent> {
    Loader::open(fixture())
        .expect("open fixture")
        .events()
        .expect("stream")
        .map(|e| e.expect("every fixture row decodes"))
        .collect()
}

#[test]
fn each_trade_id_appears_exactly_once() {
    // consumer check #4: 4 trade rows (t1 twice: WS + REST) -> 3 economic trades.
    let evs = events();
    let mut ids: Vec<&str> = evs
        .iter()
        .filter_map(|e| match e {
            ReplayEvent::Trade { trade_id, .. } => trade_id.as_deref(),
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 3, "one event per economic trade: {ids:?}");
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids, vec!["t1", "t2", "t3"], "no id twice, none lost");
}

#[test]
fn deduped_trade_keeps_ws_position_and_rest_fields() {
    let evs = events();
    let t1 = evs
        .iter()
        .find_map(|e| match e {
            ReplayEvent::Trade {
                trade_id: Some(id),
                ts,
                price,
                source,
                taker_book_side,
                ..
            } if id == "t1" => Some((ts.0, price.0, *source, *taker_book_side)),
            _ => None,
        })
        .expect("t1 present");
    assert_eq!(
        t1,
        (
            1_150_000,
            451_000,
            TradeSource::Ws,
            // The REST copy lacks taker_book_side; the WS copy's value must
            // survive the "REST wins fields" merge (fallback, no silent loss).
            Some(kdp_core::BookSide::Bid)
        ),
        "WS position (recv_ts), REST fields (price), WS fallback for absent REST field"
    );
}

#[test]
fn rest_only_trade_lands_at_execution_time_inside_the_hole() {
    // t3 printed during the capture hole; only the backfill saw it. It must
    // sit at execution time (1.35s: after the gap, before the re-anchor) --
    // never at its fetch time (9000s).
    let evs = events();
    let pos =
        |pred: &dyn Fn(&ReplayEvent) -> bool| evs.iter().position(pred).expect("event present");
    let i_gap = pos(&|e| matches!(e, ReplayEvent::Gap { .. }));
    let i_t3 = pos(&|e| matches!(e, ReplayEvent::Trade { trade_id: Some(id), .. } if id == "t3"));
    let i_reanchor = pos(&|e| matches!(e, ReplayEvent::Snapshot { event_idx: 4, .. }));
    assert!(i_gap < i_t3 && i_t3 < i_reanchor, "gap < t3 < re-anchor");
    match &evs[i_t3] {
        ReplayEvent::Trade { ts, source, .. } => {
            assert_eq!(ts.0, 1_350_000, "execution time, not fetch time");
            assert_eq!(*source, TradeSource::RestBackfill);
        }
        other => panic!("expected trade, got {other:?}"),
    }
}

#[test]
fn gap_events_appear_in_stream_exactly_as_written() {
    // consumer check #5 (in-stream half; the incomplete-manifest half is unit-tested
    // in lib.rs): a non-empty gaps table yields exactly its gap events.
    let evs = events();
    let gaps: Vec<_> = evs
        .iter()
        .filter_map(|e| match e {
            ReplayEvent::Gap { ts, reason, .. } => Some((ts.0, reason.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(gaps, vec![(1_300_000, "seq_jump")]);
    let ts: Vec<i64> = evs.iter().map(|e| e.ts().0).collect();
    let mut sorted = ts.clone();
    sorted.sort_unstable();
    assert_eq!(ts, sorted, "mixed stream stays non-decreasing (check #2)");
}

#[test]
fn between_inside_the_hole_leads_with_the_gap() {
    // t0 = 1.4s: after the gap, before the re-anchor -> the window opens on a
    // suspect book and must be visibly poisoned from event zero (consumer-confirmed gap rule),
    // now covered through real Parquet.
    let evs: Vec<ReplayEvent> = Loader::open(fixture())
        .expect("open")
        .between(1_400_000, i64::MAX)
        .expect("range")
        .map(|e| e.expect("decodes"))
        .collect();
    assert!(
        matches!(&evs[0], ReplayEvent::Gap { ts, .. } if ts.0 == 1_300_000),
        "leads with the unresolved gap: {evs:?}"
    );
    assert!(matches!(
        &evs[1],
        ReplayEvent::Snapshot {
            synthetic: true,
            ..
        }
    ));
}
