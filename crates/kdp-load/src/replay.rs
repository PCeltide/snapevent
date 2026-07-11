//! Range replay: `between(t0, t1)` — the R7 synthetic opening snapshot and the
//! consumer-confirmed gap-at-t0 rule (R3).
//!
//! The pre-roll replays every event before `t0` through a [`BookReplayer`]
//! (the shared `kdp_core::Book` rule): snapshots RE-ANCHOR the book and clear
//! the pending-gap list; deltas mutate it; gaps accumulate as pending. At
//! `t0` the stream emits, in order:
//!
//! 1. every pending gap (a gap after the last re-anchoring snapshot — the
//!    window opens on a corrupt book and must be visibly poisoned from event
//!    zero; a gap RESOLVED by a later snapshot does not poison),
//! 2. one synthetic [`ReplayEvent::Snapshot`] of the book state at `t0`
//!    (`synthetic: true`, `event_idx: -1`),
//! 3. the merged events in `[t0, t1)`, unchanged.

use kdp_core::{Book, MicroDollars, RestingQty};

use crate::error::LoadError;
use crate::event::{EffectiveTs, Level, ReplayEvent};
use crate::merge::EventIter;

/// Book state + pending-gap tracking over a replayed event stream — the ONE
/// shared replay rule (`kdp_core::Book`), public (S2) so a consumer can fold
/// `ReplayEvent`s into book state without re-implementing replay. The R7
/// opener uses this same type, so there is exactly one implementation.
#[derive(Debug, Default)]
pub struct BookReplayer {
    book: Book,
    /// Gaps seen since the last re-anchoring full snapshot (consumer-confirmed rule).
    pending_gaps: Vec<ReplayEvent>,
}

impl BookReplayer {
    /// Feed one pre-roll event. Trades don't touch the book.
    pub fn apply(&mut self, ev: &ReplayEvent) {
        match ev {
            ReplayEvent::Snapshot { yes, no, .. } => {
                let to_levels = |ls: &[Level]| {
                    ls.iter()
                        .map(|l| kdp_core::PriceLevel {
                            price: l.price,
                            quantity: l.qty,
                        })
                        .collect::<Vec<_>>()
                };
                self.book
                    .apply_snapshot_levels(&to_levels(yes), &to_levels(no));
                // A full snapshot re-anchors: earlier holes no longer poison.
                self.pending_gaps.clear();
            }
            ReplayEvent::Delta {
                side, price, delta, ..
            } => self.book.apply_change(*side, price.0, delta.0),
            ReplayEvent::Gap { .. } => self.pending_gaps.push(ev.clone()),
            ReplayEvent::Trade { .. } => {}
        }
    }

    /// The replayed book state (levels strictly positive by `Book`'s invariant).
    pub fn book(&self) -> &Book {
        &self.book
    }

    /// Gaps since the last re-anchoring snapshot: non-empty means the current
    /// book state is suspect (the consumer-confirmed gap rule). Cleared by every full snapshot.
    pub fn pending_gaps(&self) -> &[ReplayEvent] {
        &self.pending_gaps
    }

    /// The book's levels as a synthetic snapshot at `t0`. Levels in a `Book`
    /// are strictly positive by its invariant (zero/negative levels are
    /// pruned on apply), so the u64 cast cannot truncate a real level.
    fn synthetic_snapshot(&self, t0: i64) -> ReplayEvent {
        let side = |m: &std::collections::BTreeMap<u32, i64>| {
            m.iter()
                .filter(|(_, q)| **q > 0)
                .map(|(&p, &q)| Level {
                    price: MicroDollars(p),
                    qty: RestingQty(q as u64),
                })
                .collect::<Vec<_>>()
        };
        ReplayEvent::Snapshot {
            ts: EffectiveTs(t0),
            event_idx: -1,
            yes: side(&self.book.yes),
            no: side(&self.book.no),
            synthetic: true,
        }
    }
}

/// Iterator returned by [`crate::Loader::between`]: leading pending gaps, the
/// synthetic opener, then the merged stream filtered to `[t0, t1)`.
#[derive(Debug)]
pub struct RangeIter {
    lead: std::vec::IntoIter<ReplayEvent>,
    /// The first >= t0 event, held back by the pre-roll.
    held: Option<ReplayEvent>,
    rest: EventIter,
    t1: i64,
}

impl RangeIter {
    /// Pre-roll `events` to `t0`, building the opener; stream `[t0, t1)` after.
    pub(crate) fn build(mut events: EventIter, t0: i64, t1: i64) -> Result<RangeIter, LoadError> {
        let mut replayer = BookReplayer::default();
        let mut held = None;
        for ev in events.by_ref() {
            let ev = ev?; // a malformed pre-roll row fails the open, typed
            if ev.ts().0 >= t0 {
                held = Some(ev);
                break;
            }
            replayer.apply(&ev);
        }
        let mut lead = std::mem::take(&mut replayer.pending_gaps);
        lead.push(replayer.synthetic_snapshot(t0));
        Ok(RangeIter {
            lead: lead.into_iter(),
            held,
            rest: events,
            t1,
        })
    }
}

impl Iterator for RangeIter {
    type Item = Result<ReplayEvent, LoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ev) = self.lead.next() {
            return Some(Ok(ev));
        }
        let next = match self.held.take() {
            Some(ev) => Some(Ok(ev)),
            None => self.rest.next(),
        }?;
        match next {
            Err(e) => Some(Err(e)),
            Ok(ev) if ev.ts().0 < self.t1 => Some(Ok(ev)),
            Ok(_) => None, // reached t1: half-open range done
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::EventIter;
    use crate::reader::{BookEventRow, GapRow, TradeRow};
    use kdp_core::Side;

    fn src<T: 'static>(rows: Vec<T>) -> Box<dyn Iterator<Item = Result<T, LoadError>>> {
        Box::new(rows.into_iter().map(Ok))
    }

    fn snap_row(event_idx: i64, ts: i64, price: i64, qty: i64) -> BookEventRow {
        BookEventRow {
            event_idx,
            recv_ts_us: ts,
            seq: Some(event_idx),
            is_snapshot: true,
            side: Some(Side::Yes),
            price_micro: Some(price as u32),
            qty_centi: Some(qty),
        }
    }

    fn delta_row(event_idx: i64, ts: i64, price: i64, qty: i64) -> BookEventRow {
        BookEventRow {
            event_idx,
            recv_ts_us: ts,
            seq: Some(event_idx),
            is_snapshot: false,
            side: Some(Side::Yes),
            price_micro: Some(price as u32),
            qty_centi: Some(qty),
        }
    }

    fn gap_row(ts: i64) -> GapRow {
        GapRow {
            recv_ts_us: ts,
            seq: None,
            reason: "reconnect".into(),
            detail: "d".into(),
        }
    }

    /// A small deterministic book history: snapshot at 100, deltas every 100us
    /// after, re-snapshot at 1000, more deltas to 2000.
    fn history() -> Vec<BookEventRow> {
        let mut rows = vec![snap_row(1, 100, 450_000, 100)];
        for i in 2..10 {
            rows.push(delta_row(i, i * 100, 440_000 + (i % 3) * 10_000, 25));
        }
        rows.push(snap_row(10, 1000, 460_000, 300));
        for i in 11..20 {
            rows.push(delta_row(i, i * 100, 450_000 + (i % 2) * 10_000, -5));
        }
        rows
    }

    fn range(book: Vec<BookEventRow>, gaps: Vec<GapRow>, t0: i64, t1: i64) -> Vec<ReplayEvent> {
        let it = EventIter::from_sources(src(gaps), src(book), src(Vec::<TradeRow>::new()))
            .expect("merge");
        RangeIter::build(it, t0, t1)
            .expect("range")
            .map(|e| e.expect("event"))
            .collect()
    }

    #[test]
    fn public_replayer_tracks_book_and_pending_gaps() {
        let mut r = BookReplayer::default();
        r.apply(&ReplayEvent::Snapshot {
            ts: EffectiveTs(100),
            event_idx: 1,
            yes: vec![Level {
                price: MicroDollars(450_000),
                qty: RestingQty(100),
            }],
            no: vec![],
            synthetic: false,
        });
        r.apply(&ReplayEvent::Gap {
            ts: EffectiveTs(200),
            reason: "reconnect".into(),
            detail: "d".into(),
        });
        assert_eq!(r.pending_gaps().len(), 1, "gap pending until re-anchor");
        assert_eq!(r.book().yes.get(&450_000), Some(&100));
        r.apply(&ReplayEvent::Snapshot {
            ts: EffectiveTs(300),
            event_idx: 2,
            yes: vec![],
            no: vec![],
            synthetic: false,
        });
        assert!(r.pending_gaps().is_empty(), "snapshot re-anchors");
        assert!(r.book().yes.is_empty(), "snapshot replaces the book");
    }

    #[test]
    fn between_opener_equals_full_replay_at_random_t0() {
        // Deterministic LCG over 32 pseudo-random t0s (no rand dep): consumer check #3.
        let mut seed: u64 = 0x2545F4914F6CDD1D;
        for _ in 0..32 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let t0 = 100 + (seed % 2000) as i64;

            // Reference: replay everything strictly before t0 by hand.
            let mut reference = BookReplayer::default();
            {
                let it = EventIter::from_sources(
                    src(vec![]),
                    src(history()),
                    src(Vec::<TradeRow>::new()),
                )
                .expect("merge");
                for ev in it {
                    let ev = ev.expect("event");
                    if ev.ts().0 >= t0 {
                        break;
                    }
                    reference.apply(&ev);
                }
            }
            let expected = reference.synthetic_snapshot(t0);

            let evs = range(history(), vec![], t0, i64::MAX);
            let opener = evs
                .iter()
                .find(|e| {
                    matches!(
                        e,
                        ReplayEvent::Snapshot {
                            synthetic: true,
                            ..
                        }
                    )
                })
                .expect("synthetic opener present");
            assert_eq!(*opener, expected, "opener == full replay at t0={t0}");
        }
    }

    #[test]
    fn gap_after_last_anchor_poisons_window_from_event_zero() {
        // snapshot@100, gap@200, t0=300 with no re-anchor in between:
        // stream must lead with the Gap, THEN the synthetic opener (consumer-confirmed rule).
        let evs = range(
            vec![snap_row(1, 100, 450_000, 100)],
            vec![gap_row(200)],
            300,
            i64::MAX,
        );
        assert!(
            matches!(&evs[0], ReplayEvent::Gap { ts, .. } if ts.0 == 200),
            "unresolved gap poisons from event zero: {evs:?}"
        );
        assert!(
            matches!(
                &evs[1],
                ReplayEvent::Snapshot {
                    synthetic: true,
                    ..
                }
            ),
            "synthetic opener follows the gap"
        );
    }

    #[test]
    fn gap_resolved_by_fresh_snapshot_does_not_poison() {
        // gap@200 then a re-anchoring snapshot@250: t0=300 opens clean.
        let evs = range(
            vec![
                snap_row(1, 100, 450_000, 100),
                snap_row(2, 250, 455_000, 80),
            ],
            vec![gap_row(200)],
            300,
            i64::MAX,
        );
        assert!(
            matches!(
                &evs[0],
                ReplayEvent::Snapshot {
                    synthetic: true,
                    ..
                }
            ),
            "re-anchored window must NOT lead with a gap: {evs:?}"
        );
        assert!(!evs.iter().any(|e| matches!(e, ReplayEvent::Gap { .. })));
    }

    #[test]
    fn between_filters_to_half_open_range() {
        // deltas at 500..1900; window [600, 1000) keeps ts 600..=900 only.
        let evs = range(history(), vec![], 600, 1000);
        let post_opener: Vec<i64> = evs
            .iter()
            .filter(|e| {
                !matches!(
                    e,
                    ReplayEvent::Snapshot {
                        synthetic: true,
                        ..
                    }
                )
            })
            .map(|e| e.ts().0)
            .collect();
        assert!(!post_opener.is_empty());
        assert!(
            post_opener.iter().all(|&ts| (600..1000).contains(&ts)),
            "all in [t0, t1): {post_opener:?}"
        );
    }

    #[test]
    fn synthetic_opener_book_state_reflects_deltas() {
        // snapshot@100 (450000 -> 100), delta@200 (+25 at 440000): opener at
        // t0=300 must contain both levels.
        let evs = range(
            vec![
                snap_row(1, 100, 450_000, 100),
                delta_row(2, 200, 440_000, 25),
            ],
            vec![],
            300,
            i64::MAX,
        );
        match &evs[0] {
            ReplayEvent::Snapshot {
                yes,
                synthetic: true,
                ..
            } => {
                assert_eq!(yes.len(), 2, "both levels present: {yes:?}");
            }
            other => panic!("expected synthetic opener, got {other:?}"),
        }
    }
}
