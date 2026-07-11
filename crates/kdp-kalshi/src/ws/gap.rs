//! The sequence/gap state machine — pure, per-`sid` seq tracking.
//!
//! Live-confirmed (decision D19): Kalshi assigns one `sid` per channel
//! subscription and `seq` is a single monotonic counter over that sid's merged
//! per-ticker stream. So gap detection is **per-sid**: one [`SeqTracker`] per
//! `sid` watches the running `seq` and classifies each message. The async
//! session interprets the [`SeqVerdict`]: persist or skip the record, and on a
//! gap emit inline gap markers (into every ticker on the sid) and resubscribe.
//!
//! This module is pure (no I/O, no async) so the correctness core is exhaustively
//! unit-testable. A fresh `orderbook_snapshot` re-baselines `last_seq` from its
//! own seq — correct whether a resubscribe resets seq to 1 or continues it.

use kdp_core::GapReason;

/// The classification of one sequenced message against the running `seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqVerdict {
    /// In order (or a fresh baseline): persist the record and advance.
    Accept,
    /// A repeat of the last seq: skip persisting (it would double-apply).
    Duplicate,
    /// A hole was detected. Persist the triggering record (it is real data) AND
    /// emit a gap marker; the caller then resubscribes. The tracker adopts
    /// `observed_seq` so it keeps detecting further gaps until a fresh snapshot
    /// re-baselines.
    Gap {
        /// Why (always [`GapReason::SeqJump`] from this method — reconnect/
        /// resubscribe gaps are emitted by the session via [`SeqTracker::reset`]).
        reason: GapReason,
        /// Last in-order seq seen before the hole.
        last_seq: Option<u64>,
        /// The seq that revealed the hole.
        observed_seq: u64,
    },
}

/// Per-`sid` sequence tracker (see module docs). Cheap to hold one per active sid.
#[derive(Debug, Default, Clone)]
pub struct SeqTracker {
    last_seq: Option<u64>,
}

impl SeqTracker {
    /// A fresh tracker with no baseline (awaiting the first message/snapshot).
    pub fn new() -> Self {
        Self { last_seq: None }
    }

    /// The last in-order seq accepted, if any.
    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// Process an `orderbook_snapshot`: it carries full state, so it always
    /// re-baselines `last_seq` from its own seq and is accepted — a seq jump on a
    /// snapshot is not a gap (the snapshot supersedes any missed deltas).
    pub fn on_snapshot(&mut self, seq: Option<u64>) -> SeqVerdict {
        self.last_seq = seq;
        SeqVerdict::Accept
    }

    /// Process a sequenced delta/trade with sequence `seq`.
    pub fn on_sequenced(&mut self, seq: u64) -> SeqVerdict {
        match self.last_seq {
            // No baseline yet: adopt this seq (no false gap at start/after reset).
            None => {
                self.last_seq = Some(seq);
                SeqVerdict::Accept
            }
            Some(last) if seq == last + 1 => {
                self.last_seq = Some(seq);
                SeqVerdict::Accept
            }
            Some(last) if seq == last => SeqVerdict::Duplicate,
            Some(last) => {
                // Forward jump or backwards anomaly: a hole. Adopt the new seq so
                // continuity continues from here if resubscribe is slow.
                self.last_seq = Some(seq);
                SeqVerdict::Gap {
                    reason: GapReason::SeqJump,
                    last_seq: Some(last),
                    observed_seq: seq,
                }
            }
        }
    }

    /// Reset to "no baseline" — for a reconnect/resubscribe. The session emits the
    /// `Reconnect`/`Resubscribe` gap marker(s) itself; the next message (a fresh
    /// snapshot) re-baselines.
    pub fn reset(&mut self) {
        self.last_seq = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_then_in_order_deltas_accept() {
        let mut t = SeqTracker::new();
        assert_eq!(t.on_snapshot(Some(1)), SeqVerdict::Accept);
        assert_eq!(t.on_sequenced(2), SeqVerdict::Accept);
        assert_eq!(t.on_sequenced(3), SeqVerdict::Accept);
        assert_eq!(t.last_seq(), Some(3));
    }

    #[test]
    fn duplicate_seq_is_ignored() {
        let mut t = SeqTracker::new();
        t.on_snapshot(Some(1));
        t.on_sequenced(2);
        assert_eq!(t.on_sequenced(2), SeqVerdict::Duplicate);
        assert_eq!(t.last_seq(), Some(2), "duplicate does not advance");
    }

    #[test]
    fn forward_jump_is_a_gap_and_then_continues() {
        let mut t = SeqTracker::new();
        t.on_snapshot(Some(1));
        t.on_sequenced(2);
        assert_eq!(
            t.on_sequenced(5),
            SeqVerdict::Gap {
                reason: GapReason::SeqJump,
                last_seq: Some(2),
                observed_seq: 5,
            }
        );
        // Adopts the jumped-to seq so further gaps are still detected before a
        // resubscribe snapshot arrives.
        assert_eq!(t.on_sequenced(6), SeqVerdict::Accept);
    }

    #[test]
    fn backwards_seq_is_a_gap() {
        let mut t = SeqTracker::new();
        t.on_snapshot(Some(10));
        match t.on_sequenced(3) {
            SeqVerdict::Gap {
                reason: GapReason::SeqJump,
                last_seq: Some(10),
                observed_seq: 3,
            } => {}
            other => panic!("expected backwards gap, got {other:?}"),
        }
        assert_eq!(t.last_seq(), Some(3), "adopts the new seq");
    }

    #[test]
    fn fresh_snapshot_rebaselines_after_a_gap() {
        let mut t = SeqTracker::new();
        t.on_snapshot(Some(5));
        // gap
        assert!(matches!(t.on_sequenced(20), SeqVerdict::Gap { .. }));
        // resubscribe -> fresh snapshot re-baselines (seq may continue or reset)
        assert_eq!(t.on_snapshot(Some(1)), SeqVerdict::Accept);
        assert_eq!(t.last_seq(), Some(1));
        assert_eq!(t.on_sequenced(2), SeqVerdict::Accept);
    }

    #[test]
    fn snapshot_jump_does_not_emit_a_gap() {
        // A snapshot is full state, so a seq jump on a snapshot just re-baselines.
        let mut t = SeqTracker::new();
        t.on_snapshot(Some(5));
        t.on_sequenced(6);
        assert_eq!(t.on_snapshot(Some(99)), SeqVerdict::Accept);
        assert_eq!(t.last_seq(), Some(99));
    }

    #[test]
    fn first_message_without_snapshot_baselines_from_it() {
        let mut t = SeqTracker::new();
        assert_eq!(t.on_sequenced(7), SeqVerdict::Accept);
        assert_eq!(t.last_seq(), Some(7));
        assert_eq!(t.on_sequenced(8), SeqVerdict::Accept);
    }

    #[test]
    fn reset_awaits_a_new_baseline() {
        let mut t = SeqTracker::new();
        t.on_snapshot(Some(10));
        t.reset();
        assert_eq!(t.last_seq(), None);
        // After a reconnect, the next seen seq becomes the baseline (no false gap).
        assert_eq!(t.on_sequenced(1), SeqVerdict::Accept);
    }
}
