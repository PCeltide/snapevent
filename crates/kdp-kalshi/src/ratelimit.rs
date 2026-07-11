//! A token-bucket rate limiter with an injected clock (pure, deterministic).
//!
//! Backfill must stay well under Kalshi's read limit (assume Basic: ~200 read
//! tokens/s, a normal GET ≈ 10 tokens ⇒ ~20 GET/s; decision D5 caps us ~10
//! GET/s). [`RateLimiter::reserve`] uses virtual-scheduling: it refills based on
//! elapsed time, deducts the request's cost (allowing the balance to go
//! negative), and returns how long the caller must sleep before issuing the
//! request. The clock is passed in (`now` as monotonic seconds), so the policy
//! is a pure function and fully unit-testable without real time.

use std::time::Duration;

/// A token bucket: `capacity` tokens, refilled at `refill_per_sec`.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_now: f64,
}

impl RateLimiter {
    /// A full bucket holding `capacity` tokens, refilling at `refill_per_sec`.
    pub fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec,
            last_now: 0.0,
        }
    }

    /// A limiter expressed as a requests-per-second rate with a small burst.
    pub fn per_second(rate: f64) -> Self {
        Self::new(rate.max(1.0), rate.max(1.0))
    }

    /// Reserve `cost` tokens at time `now` (monotonic seconds). Refills for the
    /// elapsed time, deducts the cost (the balance may go negative), and returns
    /// the [`Duration`] the caller should sleep before issuing the request — so
    /// the average rate is honored without a retry loop.
    pub fn reserve(&mut self, now: f64, cost: f64) -> Duration {
        let elapsed = (now - self.last_now).max(0.0);
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_now = now;

        self.tokens -= cost;
        if self.tokens >= 0.0 {
            Duration::ZERO
        } else {
            // Deficit must be replenished before the request may go out.
            Duration::from_secs_f64(-self.tokens / self.refill_per_sec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_capacity_immediately() {
        let mut rl = RateLimiter::new(10.0, 10.0);
        assert_eq!(rl.reserve(0.0, 1.0), Duration::ZERO);
        assert_eq!(rl.reserve(0.0, 9.0), Duration::ZERO);
    }

    #[test]
    fn delays_once_the_bucket_is_empty() {
        let mut rl = RateLimiter::new(10.0, 10.0);
        rl.reserve(0.0, 10.0); // drains the bucket
                               // next token needs 1/10 s to refill
        assert_eq!(rl.reserve(0.0, 1.0), Duration::from_secs_f64(0.1));
    }

    #[test]
    fn refills_over_time() {
        let mut rl = RateLimiter::new(10.0, 10.0);
        rl.reserve(0.0, 10.0); // empty
                               // after 1s, 10 tokens refilled (capped at capacity) -> immediate again
        assert_eq!(rl.reserve(1.0, 5.0), Duration::ZERO);
    }

    #[test]
    fn refill_is_capped_at_capacity() {
        let mut rl = RateLimiter::new(10.0, 10.0);
        rl.reserve(0.0, 10.0);
        // 100s would refill 1000 tokens, but capacity caps at 10.
        assert_eq!(rl.reserve(100.0, 10.0), Duration::ZERO);
        assert_eq!(rl.reserve(100.0, 1.0), Duration::from_secs_f64(0.1));
    }

    #[test]
    fn steady_state_holds_the_rate() {
        // 10 req/s, cost 1 each: after draining, each subsequent reserve waits
        // ~0.1s more (virtual scheduling accumulates the deficit).
        let mut rl = RateLimiter::new(1.0, 10.0);
        assert_eq!(rl.reserve(0.0, 1.0), Duration::ZERO); // 1 -> 0
        assert_eq!(rl.reserve(0.0, 1.0), Duration::from_secs_f64(0.1)); // 0 -> -1
        assert_eq!(rl.reserve(0.0, 1.0), Duration::from_secs_f64(0.2)); // -1 -> -2
    }

    #[test]
    fn per_second_constructor() {
        let mut rl = RateLimiter::per_second(10.0);
        assert_eq!(rl.reserve(0.0, 1.0), Duration::ZERO);
    }
}
