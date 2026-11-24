//! Host-provided environment: the wall clock and the async delay primitive.
//!
//! `no_std` has no notion of "now" or "sleep", so both are injected. The clock
//! and its [`Timestamp`] are cachette's ([`cachette::Clock`],
//! [`cachette::Timestamp`]), re-exported here under their old names, so one
//! clock serves both this crate and a host's cachette stores. [`Timer`] is this
//! crate's own.

use core::time::Duration;

pub use cachette::{Clock, Timestamp};

use crate::BoxFuture;

/// A [`Duration`] as whole milliseconds, saturating at [`i64::MAX`].
///
/// `Duration::as_millis()` is a `u128`, and a plain `as i64` cast *truncates*
/// (`Duration::MAX` → `-1`) — which silently turns "saturating" arithmetic into
/// wrapping arithmetic and, worse, turns a huge TTL/grace into a negative one.
/// Every conversion in the crate must go through this.
pub(crate) fn millis_i64(dur: Duration) -> i64 {
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}

/// Async delay primitive, used to space out requests to an API (rate
/// limiting). The host provides one (`std`: a timer/thread; WASM:
/// `setTimeout`).
pub trait Timer {
    /// Resolve after (at least) `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()>;
}
