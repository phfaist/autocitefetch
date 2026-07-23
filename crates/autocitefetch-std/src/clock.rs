//! A [`Clock`] backed by the system **wall clock**.
//!
//! Wall clock, not monotonic, and that is the correct choice here: the only
//! consumer of [`Clock::now`] is the cache's expiry policy (`stale_after` /
//! `expires` / the grace window), and those timestamps are persisted — a
//! committed `citations.jsonl` has to stay meaningful across process restarts
//! and across machines, which a monotonic clock's arbitrary zero point cannot
//! express.
//!
//! Nothing time-sensitive *within* a run depends on this: retry backoff and
//! per-source rate limiting both go through
//! [`Timer::sleep(Duration)`](autocitefetch::Timer::sleep) and never consult
//! the clock, so a wall clock that jumps (NTP correction, DST, a user setting
//! the date) cannot shorten or lengthen a backoff. The worst it can do is age
//! cache entries early or late.

use std::time::{SystemTime, UNIX_EPOCH};

use autocitefetch::{Clock, Timestamp};

/// [`Clock`] backed by the system wall clock. See the [module docs](self).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl SystemClock {
    pub fn new() -> Self {
        SystemClock
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
            // `Timestamp` is `i64` ms; `as i64` would silently wrap a
            // nonsensical far-future clock into a *negative* timestamp, which
            // reads as "long expired". Saturate instead.
            Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
            // Before the epoch (clock badly set): clamp to 0.
            Err(_) => 0,
        };
        Timestamp::from_millis(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_a_plausible_epoch_millisecond_value() {
        let ms = SystemClock.now().as_millis();
        // 2020-01-01 .. 2100-01-01, in ms since the epoch. Wide enough not to
        // be flaky, narrow enough to catch a seconds/nanos unit mix-up.
        assert!(
            (1_577_836_800_000..4_102_444_800_000).contains(&ms),
            "implausible timestamp: {ms}"
        );
    }

    #[test]
    fn now_is_non_decreasing() {
        let a = SystemClock.now().as_millis();
        let b = SystemClock.now().as_millis();
        assert!(b >= a, "clock went backwards: {a} then {b}");
    }
}
