//! [`SessionCache`]: the cache as a session on a host's [`Manager`].
//!
//! A host program that keeps its caches with cachette's cache manager hands
//! autocitefetch a session of that manager instead of a file of its own. The
//! manager then decides where the items live (a folder under its root), keeps
//! them within its size limit, and prunes them in its maintenance runs
//! ([`Manager::maintain`]).
//!
//! The cache is declared by [`SessionCache::declaration`]: a values cache named
//! `autocitefetch.items`, version 1, kept by the JSONL engine, whose values are
//! volatile with the TTL policy of the [`StoragePolicy`]. The host chooses the
//! *instance* (one per project, say) with any `Serialize` value; the two
//! `keep_*` flags of the storage policy are part of the instance key, so that
//! caches that keep different fields are never mixed.
//!
//! Records are encoded as [`StoreCache`](autocitefetch::StoreCache) encodes
//! them ([`encode_item`], [`decode_item`]), so the lines of the instance's
//! JSONL file have the shape of a [`SingleFileCacheStore`](crate::SingleFileCacheStore)
//! file, with the fields a session adds (time of write, last use, cost).
//!
//! # Write-through memory
//!
//! A session never fails: when the file system fails, a read answers "not
//! found" and a write stores nothing, and the failure goes to the manager's
//! [`Reporter`](autocitefetch::cachette::Reporter). A session can also be
//! without a cache from the start ([`Manager::none`], an instance that could
//! not be opened). The retrieval of autocitefetch is two-phase — `retrieve`
//! stores, `get` reads — so a `SessionCache` keeps every `put` and `remove` of
//! its life in memory, and answers `get` from there first. `retrieve` followed
//! by `get` therefore works whatever the session does with the writes.
//!
//! An item stored with a zero TTL (the `manual` source) is *ephemeral*: the
//! session keeps it in memory and never writes it, and the write-through
//! memory answers it for the life of the `SessionCache`.
//!
//! # Flushing and pruning
//!
//! [`flush`](CacheStore::flush) does nothing: the session makes its writes
//! durable when it is dropped, so drop the `SessionCache` (or the manager that
//! holds it) at the end of the run. [`prune`](CacheStore::prune) answers
//! `Ok(0)`: the host's [`Manager::maintain`] prunes.

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use autocitefetch::cachette::{
    Cost, Engine, InstanceKey, Manager, ManagerError, PutOptions, ValueSort,
    ValuesCacheDeclaration, ValuesSession,
};
use autocitefetch::store::{decode_item, encode_item};
use autocitefetch::{CacheStore, CachedItem, CslValue, StoragePolicy, StoreError, Timestamp};
use serde::Serialize;

/// The name of the values cache that [`SessionCache::declaration`] declares.
pub const CACHE_NAME: &str = "autocitefetch.items";

/// The version of that values cache.
pub const CACHE_VERSION: u32 = 1;

/// The lifetime of a value whose owner states none. autocitefetch always
/// states one (the source's), so this is only the declared default.
const DEFAULT_LIFETIME: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// The cost of a value: about the time to fetch a citation again, with the
/// rate limits of the sources (one doi.org request per second or so).
const DEFAULT_COST: Cost = Cost::new(1000).unwrap();

/// What the instance key of a [`SessionCache`] is made from: the host's value
/// and the two flags that shape the contents.
#[derive(Serialize)]
struct Instance<'a, T: ?Sized> {
    host: &'a T,
    keep_reference: bool,
    keep_abstract: bool,
}

/// A [`CacheStore`] on a session of a host's cachette [`Manager`].
///
/// ```
/// use autocitefetch::cachette::{DiscardReports, Manager, ManagerSettings, SystemClock};
/// use autocitefetch::StoragePolicy;
/// use autocitefetch_std::SessionCache;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let root = std::env::temp_dir().join(format!("acf-session-doc-{}", std::process::id()));
/// let manager = Manager::open_path(&root, ManagerSettings::default(), SystemClock::new(), DiscardReports)?;
/// // One instance per project of the host.
/// let cache = SessionCache::open(&manager, "my-project", StoragePolicy::default())?;
/// // ... CitationManager::new(fetcher, cache, clock, timer) ...
/// # drop(cache);
/// # std::fs::remove_dir_all(&root).ok();
/// # Ok(())
/// # }
/// ```
///
/// See the [module docs](self) for what it keeps, and why `get` always
/// answers what this object stored.
pub struct SessionCache {
    session: ValuesSession,
    policy: StoragePolicy,
    /// Every `put` (`Some`) and `remove` (`None`) of this object's life.
    memory: RefCell<HashMap<String, Option<CachedItem>>>,
}

impl SessionCache {
    /// The declaration of autocitefetch's values cache for `policy`: name
    /// [`CACHE_NAME`], version [`CACHE_VERSION`], the JSONL engine, no blobs,
    /// volatile values with a lifetime of 30 days and the TTL policy
    /// `policy.ttl`, a default cost of 1000 ms, and no migration.
    ///
    /// [`open`](Self::open) uses it. A host calls it to check the declaration
    /// up front ([`Manager::check`]).
    pub fn declaration(policy: &StoragePolicy) -> ValuesCacheDeclaration {
        ValuesCacheDeclaration {
            name: CACHE_NAME.into(),
            version: CACHE_VERSION,
            engine: Engine::Jsonl,
            blobs: false,
            sort: ValueSort::Volatile {
                lifetime: DEFAULT_LIFETIME,
                policy: policy.ttl,
            },
            default_cost: DEFAULT_COST,
            migration: None,
        }
    }

    /// The instance key that [`open`](Self::open) uses for `instance` and
    /// `policy`: made from `instance` and the two `keep_*` flags of `policy`.
    ///
    /// # Errors
    ///
    /// The errors of [`InstanceKey::new`]: `instance` cannot be written as
    /// JSON, or the key is too long or too deep.
    pub fn instance_key<T: Serialize + ?Sized>(
        instance: &T,
        policy: &StoragePolicy,
    ) -> Result<InstanceKey, ManagerError> {
        InstanceKey::new(&Instance {
            host: instance,
            keep_reference: policy.keep_reference,
            keep_abstract: policy.keep_abstract,
        })
    }

    /// Open a session on `manager` for the instance chosen by `instance`, any
    /// value the host likes (`&()` for a single instance), with the storage
    /// policy `policy`.
    ///
    /// Caches opened with different `keep_*` flags are different instances,
    /// so an item stored without a field is never read by a cache that keeps
    /// it. The TTL policy is not part of the instance: a new one replaces the
    /// recorded one.
    ///
    /// A failure to open the instance is reported to the manager's reporter,
    /// and gives a session without a cache: this object then works for the
    /// run, from its write-through memory, and keeps nothing.
    ///
    /// # Errors
    ///
    /// An incorrect configuration only: the errors of [`instance_key`](Self::instance_key)
    /// and of [`Manager::open`].
    pub fn open<T: Serialize + ?Sized>(
        manager: &Manager,
        instance: &T,
        policy: StoragePolicy,
    ) -> Result<Self, ManagerError> {
        let key = Self::instance_key(instance, &policy)?;
        let session = manager.open(&Self::declaration(&policy), &key)?;
        Ok(SessionCache {
            session,
            policy,
            memory: RefCell::new(HashMap::new()),
        })
    }

    /// Whether the session keeps the items: see
    /// [`ValuesSession::has_cache`]. `false` for a session on
    /// [`Manager::none`], or one whose instance could not be opened.
    pub fn has_cache(&self) -> bool {
        self.session.has_cache()
    }

    /// The session, for a host that wants to inspect it.
    pub fn session(&self) -> &ValuesSession {
        &self.session
    }
}

impl CacheStore for SessionCache {
    fn policy(&self) -> StoragePolicy {
        self.policy
    }

    fn get(&self, id: &str) -> Result<Option<CachedItem>, StoreError> {
        if let Some(entry) = self.memory.borrow().get(id) {
            return Ok(entry.clone());
        }
        // A session answers `None` for a damaged value and for a failure,
        // which it reports; either way the citation is fetched again.
        let Some(found) = self.session.get(id) else {
            return Ok(None);
        };
        let Some(expiry) = found.fields.expiry else {
            return Ok(None);
        };
        Ok(decode_item(&found.record).map(|item| CachedItem { item, expiry }))
    }

    fn put(&self, id: &str, item: &CslValue, ttl: Duration, now: Timestamp) -> Result<(), StoreError> {
        let record = encode_item(item)?;
        // The session computes the expiry times it stores with the manager's
        // clock and the declaration's TTL policy; a zero `ttl` gives an
        // ephemeral value, which it never writes.
        self.session.put(
            id,
            record,
            Vec::new(),
            PutOptions {
                lifetime: Some(ttl),
                cost: None,
            },
        );
        let cached = CachedItem {
            item: item.clone(),
            expiry: self.policy.ttl.expiry(now, ttl, id),
        };
        self.memory.borrow_mut().insert(id.to_string(), Some(cached));
        Ok(())
    }

    fn remove(&self, id: &str) -> Result<(), StoreError> {
        self.session.remove(id);
        self.memory.borrow_mut().insert(id.to_string(), None);
        Ok(())
    }

    /// Does nothing: the session makes its writes durable when it is dropped.
    fn flush(&self) -> Result<(), StoreError> {
        Ok(())
    }

    /// Answers `Ok(0)`: the host's [`Manager::maintain`] prunes.
    fn prune(&self, _now: Timestamp) -> Result<usize, StoreError> {
        Ok(0)
    }
}
