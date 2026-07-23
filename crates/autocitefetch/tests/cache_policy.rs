//! Cache policy: freshness classification, TTL jitter, the stale-while-
//! revalidate grace window, `prune`, and the store-error boundary.
//!
//! These are the two headline "improvements over the reference
//! implementations" (jitter, stale-while-revalidate) plus the arithmetic that
//! makes them safe, so they are exercised directly rather than through a
//! source. The clock here is *movable* — every other test file's clock is a
//! constant, which is why none of them can reach time-dependent behavior.

use std::cell::{Cell, RefCell};
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, FetchError, Fetcher,
    Freshness, Payload, Request, Resolution, Response, RetrieveCtx, Source, StoreError, Timer,
    Timestamp, TtlPolicy,
};

// --- a minimal, always-ready block_on (mocks never truly pend) -------------

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future did not complete (a mock unexpectedly pended)");
}

// --- mocks -----------------------------------------------------------------

struct NoopFetcher;
impl Fetcher for NoopFetcher {
    fn fetch(&self, _req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        Box::pin(async { Err(FetchError::Status(404)) })
    }
}

#[derive(Default)]
struct MemStore {
    map: RefCell<StdMap<String, CacheRecord>>,
}

impl CacheStore for MemStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let v = self.map.borrow().get(id).cloned();
        Box::pin(async move { Ok(v) })
    }
    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        self.map.borrow_mut().insert(id.into(), record);
        Box::pin(async move { Ok(()) })
    }
    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        self.map.borrow_mut().remove(id);
        Box::pin(async move { Ok(()) })
    }
    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        let all: Vec<_> = self
            .map
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Box::pin(async move { Ok(all) })
    }
}

/// A store whose `put` always fails, so the "only store errors abort" boundary
/// (and the flush-on-error-path guarantee) can be checked. Counts `flush`
/// calls.
#[derive(Clone, Default)]
struct FailingStore {
    flushes: Rc<Cell<u32>>,
}

impl CacheStore for FailingStore {
    fn get(&self, _id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        Box::pin(async { Ok(None) })
    }
    fn put(&self, _id: &str, _record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError("disk on fire".into())) })
    }
    fn remove(&self, _id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.flushes.set(self.flushes.get() + 1);
        Box::pin(async { Ok(()) })
    }
}

/// A clock the test can move forward. Shared by `Rc` so the handle stays usable
/// after the manager takes ownership of its clone.
#[derive(Clone, Default)]
struct MovableClock {
    ms: Rc<Cell<i64>>,
}
impl MovableClock {
    fn new(ms: i64) -> Self {
        MovableClock {
            ms: Rc::new(Cell::new(ms)),
        }
    }
    fn set(&self, ms: i64) {
        self.ms.set(ms);
    }
}
impl Clock for MovableClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.ms.get())
    }
}

struct InstantTimer;
impl Timer for InstantTimer {
    fn sleep(&self, _dur: Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// A source that records every key it is asked for, and can be switched
/// "down" so every key fails (to exercise the grace window).
struct ProbeSource {
    ttl: Duration,
    calls: Rc<RefCell<Vec<String>>>,
    down: Rc<Cell<bool>>,
}

impl ProbeSource {
    fn new(ttl: Duration) -> Self {
        ProbeSource {
            ttl,
            calls: Rc::new(RefCell::new(Vec::new())),
            down: Rc::new(Cell::new(false)),
        }
    }
}

impl Source for ProbeSource {
    fn prefix(&self) -> &str {
        "probe"
    }
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn default_ttl(&self) -> Duration {
        self.ttl
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            self.calls.borrow_mut().extend(keys.iter().cloned());
            let down = self.down.get();
            keys.into_iter()
                .map(|k| {
                    if down {
                        Resolution::failed(k, autocitefetch::Error::Source("source down".into()))
                    } else {
                        let mut m = serde_json::Map::new();
                        m.insert("title".into(), CslValue::String(format!("item {k}")));
                        Resolution::concrete(k, CslValue::Object(m))
                    }
                })
                .collect()
        })
    }
}

fn no_jitter(grace: Duration) -> TtlPolicy {
    TtlPolicy {
        stale_percent: 80,
        jitter_percent: 0,
        grace,
    }
}

fn cites(key: &str) -> Vec<(String, String)> {
    vec![("probe".to_string(), key.to_string())]
}

// --- freshness / refetch ---------------------------------------------------

/// A fresh entry is served from the cache without touching the source; once it
/// goes stale the source is asked again.
#[test]
fn fresh_is_not_refetched_stale_is() {
    let clock = MovableClock::new(0);
    let src = ProbeSource::new(Duration::from_millis(1000));
    let calls = src.calls.clone();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(60)))
        .register(src);

    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(calls.borrow().len(), 1, "first retrieve must hit the source");

    // stale_after = 800, expires = 1000.
    clock.set(500);
    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(calls.borrow().len(), 1, "a fresh entry must not be refetched");

    clock.set(900);
    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(calls.borrow().len(), 2, "a stale entry must be refetched");
}

/// `classify` boundaries are half-open: `now == stale_after` is already Stale,
/// `now == expires` is already Expired.
#[test]
fn freshness_boundaries_are_half_open() {
    let policy = no_jitter(Duration::from_secs(60));
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(0),
        Duration::from_millis(1000),
        "probe:k",
    );
    assert_eq!(rec.stale_after, Timestamp::from_millis(800));
    assert_eq!(rec.expires, Timestamp::from_millis(1000));

    let at = |ms| policy.classify(&rec, Timestamp::from_millis(ms));
    assert_eq!(at(799), Freshness::Fresh);
    assert_eq!(at(800), Freshness::Stale);
    assert_eq!(at(999), Freshness::Stale);
    assert_eq!(at(1000), Freshness::Expired);
}

/// A zero TTL takes the early return: the entry is born expired (the `manual`
/// source relies on this to stay ephemeral).
#[test]
fn zero_ttl_entries_are_born_expired() {
    let policy = TtlPolicy::default();
    let now = Timestamp::from_millis(12_345);
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        now,
        Duration::ZERO,
        "manual:whatever",
    );
    assert_eq!(rec.stale_after, now);
    assert_eq!(rec.expires, now);
    assert_eq!(policy.classify(&rec, now), Freshness::Expired);
}

// --- jitter ----------------------------------------------------------------

/// Jitter stays inside ±`jitter_percent`, is deterministic per id, and actually
/// spreads a batch fetched at the same instant.
#[test]
fn jitter_is_bounded_deterministic_and_spreading() {
    let policy = TtlPolicy {
        stale_percent: 80,
        jitter_percent: 15,
        grace: Duration::from_secs(60),
    };
    let now = Timestamp::from_millis(0);
    let ttl = Duration::from_secs(1000); // 1_000_000 ms
    let mut seen = Vec::new();

    for n in 0..24 {
        let id = format!("probe:{n}");
        let rec = policy.make_record(Payload::Concrete(CslValue::Null), now, ttl, &id);
        let hard = rec.expires.as_millis();
        assert!(
            (850_000..=1_150_000).contains(&hard),
            "hard expiry {hard} for `{id}` is outside ±15%"
        );
        // Soft expiry is 80% *of the jittered hard TTL*, never past it.
        assert_eq!(rec.stale_after.as_millis(), hard * 80 / 100);

        let again = policy.make_record(Payload::Concrete(CslValue::Null), now, ttl, &id);
        assert_eq!(again.expires, rec.expires, "jitter must be deterministic");

        seen.push(hard);
    }

    seen.sort_unstable();
    seen.dedup();
    assert!(
        seen.len() > 12,
        "jitter should spread a batch out, got {} distinct expiries",
        seen.len()
    );
}

/// A `jitter_percent` above 100 used to push ~25% of entries' hard expiry
/// before `now` (born expired). It is clamped like `stale_percent`.
#[test]
fn absurd_jitter_percent_cannot_create_born_expired_entries() {
    let policy = TtlPolicy {
        stale_percent: 80,
        jitter_percent: 200,
        grace: Duration::from_secs(60),
    };
    let now = Timestamp::from_millis(0);
    let ttl = Duration::from_secs(10);
    for n in 0..64 {
        let id = format!("probe:{n}");
        let rec = policy.make_record(Payload::Concrete(CslValue::Null), now, ttl, &id);
        assert!(
            rec.expires > now,
            "`{id}` was born expired ({:?})",
            rec.expires
        );
    }
}

// --- overflow / absurd durations -------------------------------------------

/// `Timestamp` arithmetic saturates, as documented — it used to *wrap*, because
/// `Duration::as_millis()` is a u128 and `as i64` truncates.
#[test]
fn timestamp_arithmetic_saturates_instead_of_wrapping() {
    let zero = Timestamp::from_millis(0);
    assert_eq!(zero.saturating_add(Duration::MAX), Timestamp::from_millis(i64::MAX));
    // `Duration::MAX` saturates to `i64::MAX` ms, so subtracting it lands one
    // above `i64::MIN` — the point is that it no longer *wraps* to `1`.
    assert_eq!(
        zero.saturating_sub(Duration::MAX),
        Timestamp::from_millis(i64::MIN + 1)
    );
    assert_eq!(
        zero.saturating_add(Duration::from_secs(u64::MAX)),
        Timestamp::from_millis(i64::MAX)
    );
    // Ordinary values are unaffected.
    assert_eq!(
        zero.saturating_add(Duration::from_millis(1500)),
        Timestamp::from_millis(1500)
    );
}

/// "Cache forever" TTLs must produce far-future expiries, not immediately
/// expired entries (`Duration::MAX as i64 == -1`) and not a debug panic in the
/// jitter arithmetic (`2 * span + 1` with a negative span).
#[test]
fn absurd_ttls_do_not_panic_or_expire_immediately() {
    let now = Timestamp::from_millis(1_000_000);
    for policy in [TtlPolicy::default(), no_jitter(Duration::from_secs(60))] {
        for ttl in [Duration::MAX, Duration::from_secs(u64::MAX)] {
            let rec = policy.make_record(
                Payload::Concrete(CslValue::Null),
                now,
                ttl,
                "probe:forever",
            );
            assert_eq!(
                policy.classify(&rec, now),
                Freshness::Fresh,
                "a `cache forever` TTL ({ttl:?}) produced {rec:?}"
            );
            assert!(rec.stale_after >= now && rec.expires >= rec.stale_after);
        }
    }
}

/// `grace: Duration::MAX` — "never drop cached entries while the source is
/// down" — used to compute a *negative* window, so an entry expired 1 ms ago
/// was already past grace.
#[test]
fn absurd_grace_keeps_expired_entries_usable() {
    let policy = TtlPolicy {
        stale_percent: 80,
        jitter_percent: 0,
        grace: Duration::MAX,
    };
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(0),
        Duration::from_millis(1000),
        "probe:k",
    );
    assert!(policy.usable_within_grace(&rec, Timestamp::from_millis(1001)));
    assert!(policy.usable_within_grace(&rec, Timestamp::from_millis(i64::MAX - 1)));
}

// --- stale-while-revalidate ------------------------------------------------

/// A hard-expired entry whose source is down is kept and *not* reported while
/// inside the grace window; once past it, the failure surfaces.
#[test]
fn expired_entry_within_grace_is_kept_and_not_reported() {
    let clock = MovableClock::new(0);
    let src = ProbeSource::new(Duration::from_millis(1000));
    let down = src.down.clone();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(60)))
        .register(src);

    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(block_on(mgr.get("probe", "k")).unwrap()["title"], "item k");

    // Hard-expired (expires = 1000) but well inside the 60 s grace window.
    down.set(true);
    clock.set(30_000);
    let report = block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert!(
        report.is_complete(),
        "within grace a dead source must not be reported: {:?}",
        report.failures
    );
    assert_eq!(
        block_on(mgr.get("probe", "k")).unwrap()["title"],
        "item k",
        "the stale copy must still be served"
    );

    // Past the grace window (expires 1000 + 60_000).
    clock.set(61_001);
    let report = block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(report.failures.len(), 1, "past grace the failure must surface");
    assert_eq!(report.failures[0].key, "k");
}

// --- prune -----------------------------------------------------------------

/// `prune` drops exactly the entries past `expires + grace`.
#[test]
fn prune_drops_only_entries_past_the_grace_window() {
    let clock = MovableClock::new(0);
    let src = ProbeSource::new(Duration::from_millis(1000));
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(60)))
        .register(src);

    block_on(mgr.retrieve(&cites("old"))).unwrap();
    clock.set(30_000);
    block_on(mgr.retrieve(&cites("new"))).unwrap();

    // `old` expires at 1000, `new` at 31_000; grace is 60 s.
    clock.set(61_500);
    assert_eq!(block_on(mgr.prune()).unwrap(), 1);
    assert!(block_on(mgr.get("probe", "old")).is_err());
    assert!(block_on(mgr.get("probe", "new")).is_ok());

    // Nothing left to drop while `new` is still within grace.
    assert_eq!(block_on(mgr.prune()).unwrap(), 0);

    clock.set(91_001);
    assert_eq!(block_on(mgr.prune()).unwrap(), 1);
    assert_eq!(block_on(mgr.store().entries()).unwrap().len(), 0);
}

// --- the store-error boundary ----------------------------------------------

/// Only *store* errors abort `retrieve` — and the buffered writes are still
/// flushed on the way out, so a file-backed cache is not left with un-folded
/// sidecars.
#[test]
fn store_errors_abort_retrieve_but_still_flush() {
    let store = FailingStore::default();
    let flushes = store.flushes.clone();
    let src = ProbeSource::new(Duration::from_millis(1000));
    let mgr = CitationManager::new(NoopFetcher, store, MovableClock::new(0), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(60)))
        .register(src);

    let err = block_on(mgr.retrieve(&cites("k"))).unwrap_err();
    assert!(
        matches!(err, autocitefetch::Error::Store(_)),
        "expected a store error, got {err}"
    );
    assert_eq!(flushes.get(), 1, "buffered writes must be flushed even on the error path");
}
