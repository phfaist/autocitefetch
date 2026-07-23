//! `std`-backed backends for [`autocitefetch`], for consumers that are *not*
//! targeting a browser/WASM: a system [`Clock`](autocitefetch::Clock)
//! ([`SystemClock`]), a blocking [`Timer`](autocitefetch::Timer)
//! ([`BlockingTimer`]), and a filesystem
//! [`CacheStore`](autocitefetch::CacheStore) ([`SingleFileCacheStore`], a
//! single committable JSONL file plus per-writer append logs).
//!
//! A blocking network [`Fetcher`](autocitefetch::Fetcher), [`UreqFetcher`], is
//! provided behind the default-on `http` feature (backed by `ureq`). Disable
//! default features to build with no HTTP/TLS dependency (the fetcher is then
//! simply absent) and supply your own — e.g. `reqwest` on an async runtime, or
//! a browser `fetch()` on WASM.
//!
//! Every backend here is **blocking-in-a-future**: it does its work during the
//! first poll and returns `Poll::Ready`, so a `Waker::noop()` poll loop drives
//! it and no async runtime is pulled in. The catch is that nothing ever pends,
//! so the manager's per-source concurrency degenerates to sequential execution
//! — see the [`timer`] module docs.
//!
//! The backend modules are public so their design rationale (the
//! blocking-in-a-future contract, the `file:` URL handling, why the clock is a
//! wall clock) is reachable from the docs; the types are also re-exported at
//! the crate root, which is the preferred path.

#![forbid(unsafe_code)]

pub mod clock;
#[cfg(feature = "http")]
pub mod fetcher;
pub mod store;
pub mod timer;

pub use clock::SystemClock;
pub use store::{SingleFileCacheStore, StdCacheFs};
pub use timer::BlockingTimer;

#[cfg(feature = "http")]
pub use fetcher::{DEFAULT_USER_AGENT, UreqFetcher};
