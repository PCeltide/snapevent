//! Integration round-trip over a REAL captured ticker directory.
//!
//! The fixture (`tests/fixture/KXBTCD-26JUL0306-T61699.99`, 2909 book events +
//! 18 WS trades, manifest-complete, ~130KB) was captured live by
//! `capture-universe` (2026-07-03 smoke) and processed by `kdp-process` —
//! so this test is the SCHEMA-DRIFT TRIPWIRE between the writer and this
//! reader: a kdp-process schema change breaks here, not at a consumer. It
//! doubles as the published test fixture the downstream consumer asked for (S4).
//!
//! Only the public API is used (this is exactly what a consumer sees).
//!
//! Honest scope: the opener test verifies `between()` ≡ a replay of
//! `events()`' own output through the shared `kdp_core::Book` — a strong
//! consistency check of the pre-roll/opener machinery, not independent ground
//! truth (decode/order/replay-rule correctness is carried by the hand-built
//! unit tests in each module + kdp-core's hand-computed Book tests). This
//! fixture also has 0 gap rows and 0 REST trades; the REST-placement and
//! gap-at-t0 contracts are unit-tested against Vec sources — a second fixture
//! carrying both is tracked in open-items.

use kdp_load::{Completeness, Loader, ReplayEvent, TradeSource};

fn fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixture/KXBTCD-26JUL0306-T61699.99")
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
fn fixture_is_complete_and_fully_decodes() {
    let l = Loader::open(fixture()).expect("open");
    assert_eq!(*l.completeness(), Completeness::Complete);
    let evs = events();
    // 2825 orderbook messages (grouped snapshots + deltas) + 18 trades.
    let trades = evs
        .iter()
        .filter(|e| matches!(e, ReplayEvent::Trade { .. }))
        .count();
    assert_eq!(trades, 18, "all captured trades present exactly once");
    assert!(evs.len() > 2500, "grouped events: got {}", evs.len());
    // This capture was pure WS: every trade carries Ws provenance.
    assert!(evs.iter().all(|e| !matches!(
        e,
        ReplayEvent::Trade {
            source: TradeSource::RestBackfill,
            ..
        }
    )));
}

#[test]
fn stream_is_deterministic_and_time_ordered() {
    let a = events();
    let b = events();
    assert_eq!(a, b, "consumer check #1: identical stream across runs");
    let ts: Vec<i64> = a.iter().map(|e| e.ts().0).collect();
    let mut sorted = ts.clone();
    sorted.sort_unstable();
    assert_eq!(ts, sorted, "consumer check #2: non-decreasing effective ts");
}

#[test]
fn between_opener_matches_public_replay_of_the_prefix() {
    let evs = events();
    let (first, last) = (evs[0].ts().0, evs[evs.len() - 1].ts().0);
    let t0 = (first + last) / 2;

    // Reference book state at t0 built from the PUBLIC pieces only —
    // BookReplayer (S2) is the same shared rule the loader uses internally.
    let mut replayer = kdp_load::BookReplayer::default();
    for ev in evs.iter().take_while(|e| e.ts().0 < t0) {
        replayer.apply(ev);
    }

    let opener = Loader::open(fixture())
        .expect("open")
        .between(t0, i64::MAX)
        .expect("range")
        .next()
        .expect("an opener")
        .expect("decodes");
    match opener {
        ReplayEvent::Snapshot {
            yes,
            no,
            synthetic: true,
            ..
        } => {
            let to_pairs = |ls: &[kdp_load::Level]| {
                ls.iter()
                    .map(|l| (l.price.0, l.qty.0 as i64))
                    .collect::<Vec<_>>()
            };
            let book_yes: Vec<(u32, i64)> =
                replayer.book().yes.iter().map(|(&p, &q)| (p, q)).collect();
            let book_no: Vec<(u32, i64)> =
                replayer.book().no.iter().map(|(&p, &q)| (p, q)).collect();
            assert_eq!(to_pairs(&yes), book_yes, "consumer check #3 at t0={t0}");
            assert_eq!(to_pairs(&no), book_no);
        }
        other => panic!("expected the synthetic opener, got {other:?}"),
    }
}
