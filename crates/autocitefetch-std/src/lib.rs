//! `std`-backed backends for [`autocitefetch`], for consumers that are *not*
//! targeting a browser/WASM: a system [`Clock`](autocitefetch::Clock)
//! ([`SystemClock`], cachette's, re-exported), a blocking
//! [`Timer`](autocitefetch::Timer) ([`BlockingTimer`]), and a filesystem
//! [`CacheStore`](autocitefetch::CacheStore) ([`SingleFileCacheStore`], a
//! single committable JSONL file plus per-writer append logs, from cachette's
//! `JsonlStore`).
//!
//! A host that keeps its caches with a cachette
//! [`Manager`](autocitefetch::cachette::Manager) uses [`SessionCache`]
//! instead: the items then live in an instance of a values cache that the
//! host's manager keeps, sizes and prunes.
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
//! blocking-in-a-future contract, the `file:` URL handling) is reachable from
//! the docs; the types are also re-exported at
//! the crate root, which is the preferred path.

#![forbid(unsafe_code)]

#[cfg(feature = "http")]
pub mod fetcher;
pub mod session;
pub mod store;
pub mod timer;

/// The system wall clock: cachette's, made with `SystemClock::new()`. Wall
/// clock, not monotonic, because the expiry times it feeds are persisted and
/// must keep their meaning across restarts and machines.
pub use autocitefetch::cachette::SystemClock;
pub use session::SessionCache;
pub use store::{DEFAULT_BASE, SingleFileCacheStore};
pub use timer::BlockingTimer;

#[cfg(feature = "http")]
pub use fetcher::{DEFAULT_USER_AGENT, UreqFetcher};
