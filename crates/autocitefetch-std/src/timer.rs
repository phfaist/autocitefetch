use std::pin::Pin;
use std::future::Future;
use std::time::Duration;

use autocitefetch::Timer;

/// A [`Timer`] that sleeps the current thread.
///
/// Simple and dependency-free, suitable for CLI/batch tools driven by a
/// blocking executor (e.g. `pollster::block_on`). It **blocks the thread**
/// while sleeping, so it is *not* appropriate on an async runtime — there,
/// implement [`Timer`] with `tokio::time::sleep` / `async_io::Timer` instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlockingTimer;

impl BlockingTimer {
    pub fn new() -> Self {
        BlockingTimer
    }
}

impl Timer for BlockingTimer {
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(async move {
            if !dur.is_zero() {
                std::thread::sleep(dur);
            }
        })
    }
}
