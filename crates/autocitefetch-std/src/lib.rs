//! `std`-backed backends for [`autocitefetch`], for consumers that are *not*
//! targeting a browser/WASM: a system [`Clock`], a blocking [`Timer`], and a
//! filesystem [`CacheStore`].
//!
//! A network [`Fetcher`](autocitefetch::Fetcher) is intentionally left out of
//! the default build so this crate compiles with no HTTP dependency; wire one
//! up with `reqwest`/`ureq` in your binary (see `examples/`).

mod clock;
mod store;
mod timer;

pub use clock::SystemClock;
pub use store::DirCacheStore;
pub use timer::BlockingTimer;
