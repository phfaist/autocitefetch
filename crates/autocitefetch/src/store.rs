//! The [`CacheStore`] trait: host-provided persistence, one entry per key.
//!
//! A key-value shape (rather than a single serialized blob) lets writes be
//! incremental and maps naturally onto IndexedDB, `localStorage`, an embedded
//! KV store, or one-file-per-entry on disk.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::BoxFuture;
use crate::csl::CslValue;
use crate::env::Timestamp;

/// The payload of a cache entry: either concrete metadata, or a pointer to
/// another `(prefix, key)` whose metadata should be used instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    /// Concrete CSL-JSON (already carrying its `id`).
    Concrete(CslValue),
    /// A chained pointer, e.g. `arxiv:… -> doi:…`. On read, the target is
    /// resolved and `set_properties` is merged in, **overriding** any colliding
    /// field of the target (the request's `id` is still forced last).
    Chained {
        prefix: String,
        key: String,
        #[serde(default)]
        set_properties: CslValue,
    },
}

/// A persisted cache entry with a two-tier expiry.
///
/// * `stale_after` — soft expiry. Past this we *prefer* to revalidate, but the
///   entry is still usable.
/// * `expires` — hard expiry. Past this the entry is refetched on the next
///   `retrieve`, and [`prune`](crate::manager::CitationManager::prune) may drop
///   it — but only once it is also past the policy's grace window, which is
///   what keeps a stale copy around while its source is unreachable. Reads
///   ([`get`](crate::manager::CitationManager::get)) are not gated on either
///   timestamp: whatever is in the store is served.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheRecord {
    pub payload: Payload,
    pub stale_after: Timestamp,
    pub expires: Timestamp,
}

/// The cache backend failed.
#[derive(Clone, Debug)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl core::error::Error for StoreError {}

/// Host-provided cache persistence, keyed by the canonical `"prefix:key"` id.
/// Object-safe. `&self` methods (with interior mutability on the impl side) so
/// the store can be shared freely.
pub trait CacheStore {
    /// Fetch a record by id.
    fn get(&self, id: &str)
    -> BoxFuture<'_, core::result::Result<Option<CacheRecord>, StoreError>>;

    /// Insert or replace a record.
    fn put(
        &self,
        id: &str,
        record: CacheRecord,
    ) -> BoxFuture<'_, core::result::Result<(), StoreError>>;

    /// Remove a record (no-op if absent).
    fn remove(&self, id: &str) -> BoxFuture<'_, core::result::Result<(), StoreError>>;

    /// Enumerate all `(id, record)` pairs (used for pruning / inspection).
    fn entries(
        &self,
    ) -> BoxFuture<'_, core::result::Result<Vec<(String, CacheRecord)>, StoreError>>;

    /// Durably persist any buffered writes (e.g. compact an append log into the
    /// committed file). The default is a no-op: stores that persist on every
    /// write, and in-memory mocks, need not override it. The manager calls this
    /// at the end of `retrieve`/`prune`.
    fn flush(&self) -> BoxFuture<'_, core::result::Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}
