//! `autocitefetch` — automatic retrieval of bibliographic citations from
//! multiple sources, into a canonical CSL-JSON representation.
//!
//! This crate is `#![no_std]` (with `alloc`) and executor-agnostic: all I/O
//! — URL retrieval, cache persistence, the wall clock, and delays — is
//! injected through traits so the same core runs on a native `std` host or in
//! a browser (WASM `fetch()` + IndexedDB + `setTimeout`). Progress is reported
//! the same way, through an optional [`report::Reporter`].
//!
//! # Model
//!
//! A citation is a `(prefix, key)` pair, e.g. `("arxiv", "1211.1037")`. The
//! prefix selects a [`source::Source`] (arXiv API, doi.org, a manual
//! entry, a local bibliography file, …). Retrieval is two-phase:
//!
//! 1. [`CitationManager::retrieve`] — async; routes each key to its source,
//!    fetches (chunked + rate-limited), and writes results to the cache.
//! 2. [`CitationManager::get`] — synchronous; reads a resolved CSL-JSON item
//!    back out, following any *chaining* (e.g. arXiv → DOI), which is derived
//!    from the cached items at read time ([`Source::interpret`]).
//!
//! CSL-JSON items are represented as dynamic [`CslValue`]s (serde_json).
//!
//! # Storage
//!
//! The cache is a [`CacheStore`]. [`StoreCache`] implements it over any
//! [`cachette::Store`] — a [`cachette::MemoryStore`], a
//! [`cachette::JsonlStore`] (the single committable JSONL file), or a store of
//! the host's own. The [`cachette`] crate is re-exported so that a host uses
//! the version this crate was built against. `autocitefetch-std` adds
//! `SingleFileCacheStore` (the JSONL file on the real filesystem, the
//! default) and `SessionCache` (a session on a host's `cachette::Manager`).

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod batching;
pub mod cache;
pub mod csl;
pub mod driver;
pub mod env;
pub mod error;
pub mod fetch;
pub mod manager;
pub mod report;
pub mod retry;
pub mod source;
pub mod store;

pub use crate::batching::{Fill, RefreshBatching, TopUp};
pub use crate::cache::{Freshness, TtlPolicy};
pub use crate::csl::CslValue;
pub use crate::env::{Clock, Timer, Timestamp};
pub use crate::error::{Error, Result};
pub use crate::fetch::{FetchError, Fetcher, Method, Request, Response};
pub use crate::manager::{CitationManager, CiteFailure, RetrieveReport};
pub use crate::report::{Event, NopReporter, Reporter, Resolved, ThrottledReporter, Wait};
pub use crate::retry::{RetryPolicy, RetryingFetcher};
pub use crate::source::{Interpreted, Outcome, Resolution, RetrieveCtx, Source};
pub use crate::store::{CacheStore, CachedItem, StoragePolicy, StoreCache, StoreError};

/// The storage crate this crate is built on. Re-exported so that a host names
/// the same version: its stores, `cachette::Manager` (feature `std`),
/// [`cachette::export`] and [`cachette::import`].
pub use cachette;

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
