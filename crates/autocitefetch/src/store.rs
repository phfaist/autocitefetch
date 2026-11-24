//! The cache seam: the [`CacheStore`] trait, the [`StoragePolicy`] of a cache,
//! and [`StoreCache`], the implementation over any [`cachette::Store`].
//!
//! # What a cache holds
//!
//! A cache holds **one CSL-JSON item per citation id**: what the source
//! returned for that key ([`Outcome::Concrete`]), under the canonical
//! `"prefix:key"` id, with its two expiry times ([`Expiry`]). Nothing that
//! depends on the configuration of a run is stored: DOI overrides, the chain
//! target of arXiv and the fields dropped with
//! [`with_dropped_csl_fields`](crate::manager::CitationManager::with_dropped_csl_fields)
//! are applied when an item is read ([`Source::interpret`] and
//! [`CitationManager::get`](crate::manager::CitationManager::get)). Changing
//! them therefore never changes the cache, and no chain pointer is stored.
//!
//! What *does* shape the contents is the [`StoragePolicy`], which belongs to
//! the cache object: the TTL policy that turns a lifetime into expiry times,
//! and whether the bulky fields `reference` and `abstract` are kept. The
//! manager removes the omitted fields before it stores an item, and again
//! when it reads one, so an item stored under another policy is read the same.
//!
//! # Synchronous by design
//!
//! Every method is synchronous and takes `&self` (implementations use interior
//! mutability). cachette's stores are synchronous, so nothing is gained by an
//! async seam, and a synchronous call can never hold a borrow across an
//! `.await` of the retrieval loop. A host whose storage answers asynchronously
//! (a browser's IndexedDB) loads a [`cachette::MemoryStore`] before the run
//! (`cachette::import`) and saves it afterwards (`cachette::export`).
//!
//! # Record encoding
//!
//! [`StoreCache`] writes the JSON text `{"concrete": <item>}` as the record of
//! a value ([`encode_item`]): exactly what autocitefetch 0.1.1 wrote for a
//! concrete entry, so files written by 0.1.1 are read as they are, and 0.1.1
//! can read files written now. A 0.1.1 chain pointer (`{"chained": …}`) reads
//! as absent ([`decode_item`]), and the citation is fetched again.
//!
//! [`Outcome::Concrete`]: crate::source::Outcome::Concrete
//! [`Source::interpret`]: crate::source::Source::interpret

use core::cell::RefCell;
use core::time::Duration;

use cachette::{
    Expiry, Fields, NewValue, PruneRule, Record, Store, StoreErrorKind, WithEphemeral,
};
use serde::Serialize;

use crate::csl::CslValue;
use crate::env::Timestamp;

pub use cachette::StoreError;

/// How a cache stores items. Everything that shapes cache contents is here.
///
/// The [`Default`] is cachette's default TTL policy, with `reference` and
/// `abstract` omitted.
///
/// The two flags are applied by the manager on both sides of the cache: an
/// omitted field is removed from an item before it is stored, and from an item
/// read back, so turning a flag off hides the field at once, also for items
/// stored before. Turning a flag on takes effect as entries are fetched again.
#[derive(Clone, Copy, Debug, Default)]
pub struct StoragePolicy {
    /// The TTL policy that turns a source's lifetime into expiry times, and
    /// decides when an entry is refreshed.
    pub ttl: cachette::TtlPolicy,
    /// Keep the CSL field `reference` (a work's own bibliography).
    pub keep_reference: bool,
    /// Keep the CSL field `abstract`.
    pub keep_abstract: bool,
}

impl StoragePolicy {
    /// The top-level CSL fields this policy omits, in a fixed order.
    pub fn omitted_fields(&self) -> impl Iterator<Item = &'static str> {
        [
            (!self.keep_reference).then_some("reference"),
            (!self.keep_abstract).then_some("abstract"),
        ]
        .into_iter()
        .flatten()
    }

    /// Remove the fields this policy omits from the top level of `item`. A
    /// value that is not a JSON object is left alone.
    pub fn strip(&self, item: &mut CslValue) {
        if let Some(obj) = item.as_object_mut() {
            for field in self.omitted_fields() {
                obj.remove(field);
            }
        }
    }
}

/// An item read from a cache, with its expiry times.
#[derive(Clone, Debug)]
pub struct CachedItem {
    /// The CSL-JSON item, as the source returned it (minus the fields the
    /// storage policy omitted when it was stored). It carries no meaningful
    /// `id`: the manager sets the requested one on read.
    pub item: CslValue,
    /// When the item goes stale, and when it expires.
    pub expiry: Expiry,
}

/// Host-provided cache persistence, keyed by the canonical `"prefix:key"` id.
///
/// Object-safe and synchronous; methods take `&self`, so implementations use
/// interior mutability (the manager only ever holds shared borrows of it while
/// it drives sources concurrently).
///
/// **Division of work.** The manager removes the fields that
/// [`policy`](Self::policy) omits before [`put`](Self::put) and after
/// [`get`](Self::get). An implementation only stores what it is given.
pub trait CacheStore {
    /// The storage policy of this cache.
    fn policy(&self) -> StoragePolicy;

    /// Read the item stored under `id`. A value this cache cannot read as an
    /// item (a 0.1.1 chain pointer, an unreadable record, a value without
    /// expiry times) is reported as absent: the manager then fetches the
    /// citation again, and the [`put`](Self::put) replaces it.
    fn get(&self, id: &str) -> Result<Option<CachedItem>, StoreError>;

    /// Store `item` under `id`, written at `now` with the lifetime `ttl`. The
    /// expiry times are computed with the policy's TTL policy.
    ///
    /// A `ttl` of zero stores an **ephemeral** item: readable for the life of
    /// the cache object, never written to disk. This is what the `manual`
    /// source needs.
    fn put(&self, id: &str, item: &CslValue, ttl: Duration, now: Timestamp)
    -> Result<(), StoreError>;

    /// Remove the item under `id` (no-op if absent).
    fn remove(&self, id: &str) -> Result<(), StoreError>;

    /// Make the writes so far durable (for the JSONL store, merge the logs into
    /// the main file). The manager calls it at the end of `retrieve` and
    /// `prune`.
    fn flush(&self) -> Result<(), StoreError>;

    /// Removes the items past their grace period; answers how many.
    fn prune(&self, now: Timestamp) -> Result<usize, StoreError>;
}

/// The JSON shape of a stored record: `{"concrete": <item>}`, borrowed.
#[derive(Serialize)]
struct ConcreteRecord<'a> {
    concrete: &'a CslValue,
}

/// Encode `item` as the record of a cache value: the JSON text
/// `{"concrete": <item>}`, which is what autocitefetch 0.1.1 writes for a
/// concrete entry.
///
/// # Errors
///
/// The error of [`Record::from_serialize`] when the item cannot be written as
/// JSON (which a `serde_json::Value` always can).
pub fn encode_item(item: &CslValue) -> Result<Record, StoreError> {
    Record::from_serialize(&ConcreteRecord { concrete: item })
}

/// Decode the record of a cache value into an item.
///
/// * `{"concrete": X}` where `X` is a JSON object ⇒ `Some(X)`;
/// * `{"chained": …}`, a pointer written by 0.1.1 ⇒ `None`;
/// * anything else ⇒ `None`.
pub fn decode_item(record: &Record) -> Option<CslValue> {
    let CslValue::Object(mut obj) = serde_json::from_str::<CslValue>(record.as_json()).ok()? else {
        return None;
    };
    if obj.len() != 1 {
        return None;
    }
    match obj.remove("concrete")? {
        item @ CslValue::Object(_) => Some(item),
        _ => None,
    }
}

/// The [`CacheStore`] over any [`cachette::Store`].
///
/// The store is wrapped in a [`WithEphemeral`], so items stored with a zero TTL
/// stay in memory and never reach the store. Construct with
/// [`StoreCache::new`]; [`StoreCache::with_policy`] sets the storage policy.
///
/// ```
/// use autocitefetch::{StoragePolicy, StoreCache};
/// use autocitefetch::cachette::MemoryStore;
///
/// let cache = StoreCache::new(MemoryStore::new()).with_policy(StoragePolicy {
///     keep_abstract: true,
///     ..StoragePolicy::default()
/// });
/// # let _ = cache;
/// ```
///
/// Values are written with expiry times only: no time of write, use or cost,
/// so the lines of a JSONL file keep the 0.1.1 shape, and a file under version
/// control changes only when an entry is fetched again. For the same reason
/// uses are never recorded.
pub struct StoreCache<S: Store> {
    store: RefCell<WithEphemeral<S>>,
    policy: StoragePolicy,
}

impl<S: Store> StoreCache<S> {
    /// A cache over `store`, with the default [`StoragePolicy`].
    pub fn new(store: S) -> Self {
        StoreCache {
            store: RefCell::new(WithEphemeral::new(store)),
            policy: StoragePolicy::default(),
        }
    }

    /// Set the storage policy. Builder-style.
    pub fn with_policy(mut self, policy: StoragePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The wrapped store. The ephemeral items are dropped.
    pub fn into_inner(self) -> S {
        self.store.into_inner().into_inner()
    }

    /// Run `f` on the wrapped store, for tests and for a host that calls
    /// [`cachette::export`]. The ephemeral items are not in it.
    ///
    /// # Panics
    ///
    /// If called from inside another `with_store` on the same cache.
    pub fn with_store<R>(&self, f: impl FnOnce(&mut S) -> R) -> R {
        f(self.store.borrow_mut().get_mut())
    }
}

impl<S: Store> CacheStore for StoreCache<S> {
    fn policy(&self) -> StoragePolicy {
        self.policy
    }

    fn get(&self, id: &str) -> Result<Option<CachedItem>, StoreError> {
        let value = match self.store.borrow_mut().get(id) {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            // One unreadable value: fetch the citation again, and the `put`
            // replaces it.
            Err(e) if e.kind() == StoreErrorKind::DamagedValue => return Ok(None),
            Err(e) => return Err(e),
        };
        // A value without expiry times was not written by this cache.
        let Some(expiry) = value.fields.expiry else {
            return Ok(None);
        };
        Ok(decode_item(&value.record).map(|item| CachedItem { item, expiry }))
    }

    fn put(
        &self,
        id: &str,
        item: &CslValue,
        ttl: Duration,
        now: Timestamp,
    ) -> Result<(), StoreError> {
        let fields = Fields {
            expiry: Some(self.policy.ttl.expiry(now, ttl, id)),
            ..Default::default()
        };
        let value = NewValue::new(encode_item(item)?, fields);
        self.store.borrow_mut().put(id, value)
    }

    fn remove(&self, id: &str) -> Result<(), StoreError> {
        self.store.borrow_mut().remove(id)
    }

    fn flush(&self) -> Result<(), StoreError> {
        self.store.borrow_mut().flush()
    }

    fn prune(&self, now: Timestamp) -> Result<usize, StoreError> {
        let rule = PruneRule::Volatile {
            grace: self.policy.ttl.grace,
        };
        Ok(self.store.borrow_mut().prune(rule, now)?.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cachette::MemoryStore;
    use serde_json::json;

    #[test]
    fn encode_writes_the_0_1_1_concrete_shape() {
        let item = json!({"title": "T", "DOI": "10.1/X"});
        let rec = encode_item(&item).unwrap();
        assert_eq!(rec.as_json(), r#"{"concrete":{"DOI":"10.1/X","title":"T"}}"#);
        assert_eq!(decode_item(&rec), Some(item));
    }

    #[test]
    fn decode_reads_concrete_objects_only() {
        let rec = |s: &str| Record::from_json(s).unwrap();
        assert_eq!(decode_item(&rec(r#"{"concrete":{"a":1}}"#)), Some(json!({"a": 1})));
        // A 0.1.1 chain pointer.
        assert_eq!(
            decode_item(&rec(
                r#"{"chained":{"prefix":"doi","key":"10.1/x","set_properties":{"arxivid":"1"}}}"#
            )),
            None
        );
        // Anything else.
        assert_eq!(decode_item(&rec(r#"{"concrete":[1]}"#)), None);
        assert_eq!(decode_item(&rec(r#"{"concrete":{"a":1},"x":2}"#)), None);
        assert_eq!(decode_item(&rec(r#"{"a":1}"#)), None);
        assert_eq!(decode_item(&rec("null")), None);
        assert_eq!(decode_item(&rec("[1,2]")), None);
    }

    #[test]
    fn store_cache_roundtrips_and_keeps_ephemeral_items_out_of_the_store() {
        let cache = StoreCache::new(MemoryStore::new());
        let now = Timestamp::from_millis(1_000);
        cache
            .put("doi:x", &json!({"title": "T"}), Duration::from_secs(60), now)
            .unwrap();
        cache
            .put("manual:y", &json!({"title": "M"}), Duration::ZERO, now)
            .unwrap();
        let got = cache.get("doi:x").unwrap().unwrap();
        assert_eq!(got.item, json!({"title": "T"}));
        assert!(got.expiry.stale_after < got.expiry.expires);
        // The ephemeral item is readable, but not in the wrapped store.
        assert_eq!(cache.get("manual:y").unwrap().unwrap().item, json!({"title": "M"}));
        assert!(cache.with_store(|s| s.get("manual:y").unwrap().is_none()));
        assert!(cache.with_store(|s| s.get("doi:x").unwrap().is_some()));
        cache.remove("doi:x").unwrap();
        assert!(cache.get("doi:x").unwrap().is_none());
    }

    #[test]
    fn store_cache_reads_unknown_records_and_stable_values_as_absent() {
        let cache = StoreCache::new(MemoryStore::new());
        cache.with_store(|s| {
            let expiry = Expiry {
                stale_after: Timestamp::from_millis(1),
                expires: Timestamp::from_millis(2),
            };
            let pointer = Record::from_json(r#"{"chained":{"prefix":"doi","key":"k"}}"#).unwrap();
            s.put("arxiv:1", NewValue::new(pointer, Fields { expiry: Some(expiry), ..Default::default() }))
                .unwrap();
            let stable = encode_item(&json!({"a": 1})).unwrap();
            s.put("doi:2", NewValue::new(stable, Fields::default())).unwrap();
        });
        assert!(cache.get("arxiv:1").unwrap().is_none());
        assert!(cache.get("doi:2").unwrap().is_none());
    }

    #[test]
    fn prune_counts_values_past_their_grace() {
        let policy = StoragePolicy {
            ttl: cachette::TtlPolicy {
                grace: Duration::from_millis(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let cache = StoreCache::new(MemoryStore::new()).with_policy(policy);
        let t0 = Timestamp::from_millis(0);
        cache.put("a:1", &json!({}), Duration::from_millis(100), t0).unwrap();
        // Hard expiry is at most 115 ms (15% jitter); grace 10 ms.
        assert_eq!(cache.prune(Timestamp::from_millis(50)).unwrap(), 0);
        assert_eq!(cache.prune(Timestamp::from_millis(200)).unwrap(), 1);
        assert!(cache.get("a:1").unwrap().is_none());
    }

    #[test]
    fn policy_strips_omitted_fields() {
        let mut item = json!({"reference": [1], "abstract": "A", "title": "T"});
        StoragePolicy::default().strip(&mut item);
        assert_eq!(item, json!({"title": "T"}));
        let mut item = json!({"reference": [1], "abstract": "A"});
        StoragePolicy {
            keep_abstract: true,
            ..Default::default()
        }
        .strip(&mut item);
        assert_eq!(item, json!({"abstract": "A"}));
    }
}
