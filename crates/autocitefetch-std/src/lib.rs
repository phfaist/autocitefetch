//! `std`-backed backends for [`autocitefetch`], for consumers that are *not*
//! targeting a browser/WASM: a system [`Clock`], a blocking [`Timer`], and a
//! filesystem [`CacheStore`].
//!
//! A blocking network [`Fetcher`](autocitefetch::Fetcher), [`UreqFetcher`], is
//! provided behind the default-on `http` feature (backed by `ureq`). Disable
//! default features to build with no HTTP/TLS dependency (the fetcher is then
//! simply absent) and supply your own — e.g. `reqwest` on an async runtime, or
//! a browser `fetch()` on WASM.

#[cfg(feature = "http")]
mod fetcher;
mod clock;
mod store;
mod timer;

pub use clock::SystemClock;
pub use store::DirCacheStore;
pub use timer::BlockingTimer;

#[cfg(feature = "http")]
pub use fetcher::{UreqFetcher, DEFAULT_USER_AGENT};
