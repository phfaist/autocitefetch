use std::time::{SystemTime, UNIX_EPOCH};

use autocitefetch::{Clock, Timestamp};

/// [`Clock`] backed by the system wall clock.
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
            Ok(d) => d.as_millis() as i64,
            // Before the epoch (clock badly set): clamp to 0.
            Err(_) => 0,
        };
        Timestamp::from_millis(ms)
    }
}
