//! `autocitefetch` — automatic retrieval of bibliographic citations from
//! multiple sources, into a canonical CSL-JSON representation.
//!
//! This crate is `#![no_std]` (with `alloc`) and executor-agnostic: all I/O
//! — URL retrieval, cache persistence, the wall clock, and delays — is
//! injected through traits so the same core runs on a native `std` host or in
//! a browser (WASM `fetch()` + IndexedDB + `setTimeout`).
//!
//! # Model
//!
//! A citation is a `(prefix, key)` pair, e.g. `("arxiv", "1211.1037")`. The
//! prefix selects a [`Source`](source::Source) (arXiv API, doi.org, a manual
//! entry, a local bibliography file, …). Retrieval is two-phase:
//!
//! 1. [`CitationManager::retrieve`] — async; routes each key to its source,
//!    fetches (chunked + rate-limited), and writes results to the cache.
//! 2. [`CitationManager::get`] — reads a resolved CSL-JSON item back out,
//!    following any *chaining* pointers (e.g. arXiv → DOI).
//!
//! CSL-JSON items are represented as dynamic [`CslValue`]s (serde_json).

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod cache;
pub mod csl;
pub mod driver;
pub mod env;
pub mod error;
pub mod fetch;
pub mod filecache;
pub mod manager;
pub mod retry;
pub mod source;
pub mod store;

pub use crate::cache::{Freshness, TtlPolicy};
pub use crate::csl::CslValue;
pub use crate::env::{Clock, Timer, Timestamp};
pub use crate::error::{Error, Result};
pub use crate::fetch::{FetchError, Fetcher, Method, Request, Response};
pub use crate::filecache::{CacheFs, CacheGuard, FileCacheStore, FsError};
pub use crate::manager::{CitationManager, CiteFailure, RetrieveReport};
pub use crate::retry::{RetryPolicy, RetryingFetcher};
pub use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
pub use crate::store::{CacheRecord, CacheStore, Payload, StoreError};

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

/// A heap-allocated, single-threaded future.
///
/// No `Send` bound: futures on WASM are `!Send`, and the whole core is
/// designed to run on a single cooperative task. This is the return type of
/// every async trait method so the traits stay object-safe (usable as
/// `&dyn Fetcher`, `Box<dyn Source>`, …).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
