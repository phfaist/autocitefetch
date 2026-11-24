//! [`SingleFileCacheStore`]: the cache in one committable JSONL file.
//!
//! A thin wrapper around [`StoreCache`]`<`[`JsonlStore`]`<`[`StdFs`]`>>`: the
//! storage engine — the per-writer logs, the locks, the merge — is cachette's
//! [`JsonlStore`], and its module documentation holds the rationale for all of
//! it. This module only chooses the defaults.
//!
//! The cache is a single committable `citations.jsonl` file plus files that
//! exist only while a run is in flight: per-writer append logs
//! (`._citations.<writer>.log`) with their liveness locks
//! (`._citations.<writer>.log.lock`), the merge lock (`._citations.lock`) and
//! the staging file of the atomic replacement (`._citations.jsonl.tmp`), all in
//! a directory you choose. Only `citations.jsonl` is worth committing to
//! version control — and since every other file hides under the `._citations`
//! prefix, ignoring the lot takes **one rule**:
//!
//! ```gitignore
//! ._citations*
//! ```
//!
//! These are the same files, in the same place, as autocitefetch 0.1.1 wrote,
//! and a cache written by 0.1.1 is read as it is.

use std::path::Path;
use std::time::Duration;

use autocitefetch::cachette::{JsonlStore, StdFs};
use autocitefetch::{CacheStore, CachedItem, CslValue, StoragePolicy, StoreCache, StoreError, Timestamp};

/// The base file name used by [`SingleFileCacheStore::new`] (`citations.jsonl`
/// etc.).
pub const DEFAULT_BASE: &str = "citations";

/// A ready-to-use single-file cache over the local filesystem.
///
/// Keeps one `citations.jsonl` file (plus the transient `._citations*` files of
/// cachette's [`JsonlStore`]) in a directory of your choosing. Construct with
/// [`SingleFileCacheStore::new`], or with [`SingleFileCacheStore::with_base`]
/// to rename the whole family; [`SingleFileCacheStore::with_policy`] sets the
/// storage policy.
pub struct SingleFileCacheStore(StoreCache<JsonlStore<StdFs>>);

impl SingleFileCacheStore {
    /// Open (creating the directory if needed) a single-file cache in `dir`,
    /// named after [`DEFAULT_BASE`].
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::with_base(dir, DEFAULT_BASE)
    }

    /// Like [`new`](Self::new), but with a caller-chosen `base` for every file
    /// in the family: the committable `{base}.jsonl`, and — all under the
    /// `._{base}` throwaway prefix — `._{base}.lock`,
    /// `._{base}.<writer>.log` with its companion `.log.lock`, and
    /// `._{base}.jsonl.tmp`. Use it to keep the cache out of the way in a
    /// directory that is not its own — a CLI dropping the cache in the user's
    /// working directory wants `".citations"`, so the committable file is the
    /// hidden `.citations.jsonl` and the throwaway files are `._.citations*`.
    ///
    /// The writer name is made by [`JsonlStore::open_path`], unique per store
    /// object, so concurrent processes never share a log.
    ///
    /// # Errors
    ///
    /// The errors of [`JsonlStore::open_path`]: the directory cannot be
    /// created or is not valid UTF-8, `base` contains `/`, the file system
    /// fails, or the main file cannot be read at all.
    pub fn with_base(dir: impl AsRef<Path>, base: &str) -> Result<Self, StoreError> {
        let store = JsonlStore::open_path(dir, base)?;
        Ok(SingleFileCacheStore(StoreCache::new(store)))
    }

    /// Set the storage policy. Builder-style.
    pub fn with_policy(self, policy: StoragePolicy) -> Self {
        SingleFileCacheStore(self.0.with_policy(policy))
    }

    /// Access the underlying generic cache.
    pub fn inner(&self) -> &StoreCache<JsonlStore<StdFs>> {
        &self.0
    }

    /// The underlying generic cache, by value.
    pub fn into_inner(self) -> StoreCache<JsonlStore<StdFs>> {
        self.0
    }
}

// Delegate the cache trait straight through to the inner `StoreCache`.
impl CacheStore for SingleFileCacheStore {
    fn policy(&self) -> StoragePolicy {
        self.0.policy()
    }
    fn get(&self, id: &str) -> Result<Option<CachedItem>, StoreError> {
        self.0.get(id)
    }
    fn put(&self, id: &str, item: &CslValue, ttl: Duration, now: Timestamp) -> Result<(), StoreError> {
        self.0.put(id, item, ttl, now)
    }
    fn remove(&self, id: &str) -> Result<(), StoreError> {
        self.0.remove(id)
    }
    fn flush(&self) -> Result<(), StoreError> {
        self.0.flush()
    }
    fn prune(&self, now: Timestamp) -> Result<usize, StoreError> {
        self.0.prune(now)
    }
}
