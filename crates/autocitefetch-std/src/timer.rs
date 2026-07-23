//! A [`Timer`] that sleeps the current thread.
//!
//! # This backend serialises everything
//!
//! The manager drives its per-source buckets with
//! `buffer_unordered(MAX_CONCURRENT_SOURCES)`, but with the bundled blocking
//! backends that concurrency buys **nothing**: `UreqFetcher::fetch` and
//! [`BlockingTimer::sleep`] both do their work *inside the first poll* and
//! return `Poll::Ready`, so `buffer_unordered` never gets the chance to
//! interleave — it polls one source's future, that future blocks the thread
//! until it is finished, and only then does the next one start. Sources run
//! strictly sequentially, and one source's 3100 ms inter-chunk sleep or 30 s
//! retry backoff stalls *every* other in-flight source with it.
//!
//! That is a fine trade for a CLI resolving a few dozen citations. If it is
//! not, the fix is not here: supply a genuinely async
//! [`Fetcher`](autocitefetch::Fetcher) **and** `Timer` (e.g. `reqwest` +
//! `tokio::time::sleep`) so their futures actually pend and yield.

use std::time::Duration;

use autocitefetch::{BoxFuture, Timer};

/// A [`Timer`] that sleeps the current thread.
///
/// Simple and dependency-free, suitable for CLI/batch tools driven by a
/// blocking executor (e.g. `pollster::block_on`). It **blocks the thread**
/// while sleeping, so it is *not* appropriate on an async runtime — there,
/// implement [`Timer`] with `tokio::time::sleep` / `async_io::Timer` instead.
/// See the [module docs](self) for what that costs in concurrency.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlockingTimer;

impl BlockingTimer {
    pub fn new() -> Self {
        BlockingTimer
    }
}

impl Timer for BlockingTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !dur.is_zero() {
                std::thread::sleep(dur);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    use std::time::Instant;

    use super::*;

    /// Blocking driver: `BlockingTimer` resolves on the first poll.
    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut fut = std::pin::pin!(fut);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        for _ in 0..1_000_000 {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
        panic!("future did not complete (a backend unexpectedly pended)");
    }

    #[test]
    fn a_zero_sleep_returns_promptly() {
        let t = Instant::now();
        block_on(BlockingTimer.sleep(Duration::ZERO));
        assert!(
            t.elapsed() < Duration::from_millis(50),
            "a zero sleep should not actually sleep, took {:?}",
            t.elapsed()
        );
    }

    #[test]
    fn a_nonzero_sleep_actually_elapses() {
        let t = Instant::now();
        block_on(BlockingTimer.sleep(Duration::from_millis(10)));
        assert!(
            t.elapsed() >= Duration::from_millis(10),
            "slept only {:?}",
            t.elapsed()
        );
    }
}
