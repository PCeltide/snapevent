//! Exponential reconnect backoff with jitter — pure and deterministic in tests.
//!
//! On a dropped connection the session waits before reconnecting, growing the
//! delay exponentially (capped) so a flapping server isn't hammered, with jitter
//! so many clients don't reconnect in lockstep. The jitter fraction is
//! **injected** into [`Backoff::next`] (the session supplies a random one at
//! runtime; tests supply a fixed one), keeping the policy a pure, testable
//! function. Uses "equal jitter": each delay is half fixed + half jittered, so
//! it is bounded in `[cap/2, cap]` and never collapses to zero.

use std::time::Duration;

/// Exponential-backoff schedule with a base, a cap, and an attempt counter.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    /// A schedule starting at `base`, doubling each attempt, capped at `max`.
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// Reset to the first attempt (call after a successfully-established,
    /// stable connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// The current capped exponential value, `min(max, base * 2^attempt)`,
    /// before jitter. Saturating — never overflows.
    fn capped(&self) -> Duration {
        let base_ms = self.base.as_millis() as u64;
        let max_ms = self.max.as_millis() as u64;
        let scaled = base_ms.checked_shl(self.attempt).unwrap_or(u64::MAX);
        Duration::from_millis(scaled.min(max_ms))
    }

    /// The next delay for `jitter` in `[0.0, 1.0]`, then advance the attempt.
    ///
    /// Equal jitter: `cap/2 + (cap/2) * jitter`, so the result lies in
    /// `[cap/2, cap]`.
    pub fn next(&mut self, jitter: f64) -> Duration {
        let cap = self.capped();
        self.attempt = self.attempt.saturating_add(1);
        let half = cap / 2;
        half + half.mul_f64(jitter.clamp(0.0, 1.0))
    }

    /// The number of delays handed out since the last reset.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially_with_zero_jitter() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(10_000));
        // jitter 0 -> exactly cap/2 each time; cap doubles per attempt.
        assert_eq!(b.next(0.0), Duration::from_millis(50)); // cap 100
        assert_eq!(b.next(0.0), Duration::from_millis(100)); // cap 200
        assert_eq!(b.next(0.0), Duration::from_millis(200)); // cap 400
        assert_eq!(b.next(0.0), Duration::from_millis(400)); // cap 800
    }

    #[test]
    fn jitter_one_yields_full_cap() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(10_000));
        assert_eq!(b.next(1.0), Duration::from_millis(100)); // cap 100 -> 50 + 50
        assert_eq!(b.next(1.0), Duration::from_millis(200)); // cap 200
    }

    #[test]
    fn is_capped_at_max() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(500));
        // attempts: caps 100,200,400,500(capped),500,...
        let delays: Vec<u64> = (0..6).map(|_| b.next(0.0).as_millis() as u64).collect();
        assert_eq!(delays, vec![50, 100, 200, 250, 250, 250]);
    }

    #[test]
    fn reset_returns_to_first_attempt() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(10_000));
        b.next(0.0);
        b.next(0.0);
        assert_eq!(b.attempt(), 2);
        b.reset();
        assert_eq!(b.attempt(), 0);
        assert_eq!(b.next(0.0), Duration::from_millis(50));
    }

    #[test]
    fn jitter_is_clamped() {
        let mut b = Backoff::new(Duration::from_millis(100), Duration::from_millis(10_000));
        // out-of-range jitter clamps to [0,1]; cap 100 -> [50,100].
        assert_eq!(b.next(5.0), Duration::from_millis(100));
        let mut b2 = Backoff::new(Duration::from_millis(100), Duration::from_millis(10_000));
        assert_eq!(b2.next(-3.0), Duration::from_millis(50));
    }
}
