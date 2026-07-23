//! Host-provided environment: the wall clock and the async delay primitive.
//!
//! `no_std` has no notion of "now" or "sleep", so both are injected.

use core::time::Duration;

use serde::{Deserialize, Serialize};

use crate::BoxFuture;

/// A point in time, as milliseconds since the Unix epoch.
///
/// Stored as `i64` so that adding/subtracting [`Duration`]s and comparing
/// expiries is trivial and allocation-free. Serialized transparently as a
/// plain integer so cache files stay compact.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Construct from milliseconds since the Unix epoch.
    pub const fn from_millis(ms: i64) -> Self {
        Timestamp(ms)
    }

    /// Milliseconds since the Unix epoch.
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// `self + dur`, saturating instead of overflowing.
    pub fn saturating_add(self, dur: Duration) -> Timestamp {
        Timestamp(self.0.saturating_add(dur.as_millis() as i64))
    }

    /// `self - dur`, saturating instead of overflowing.
    pub fn saturating_sub(self, dur: Duration) -> Timestamp {
        Timestamp(self.0.saturating_sub(dur.as_millis() as i64))
    }
}

/// Source of wall-clock time. The host provides one (`std`: `SystemTime`;
/// WASM: `Date.now()`).
pub trait Clock {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// Async delay primitive, used to space out requests to an API (rate
/// limiting). The host provides one (`std`: a timer/thread; WASM:
/// `setTimeout`).
pub trait Timer {
    /// Resolve after (at least) `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()>;
}
