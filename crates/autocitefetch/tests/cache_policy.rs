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

/// A source with a configurable prefix that records every key it is asked for
/// and returns one concrete item per key. Used to observe whether the manager
/// actually drives a source (a not-refetched entry leaves the log empty) and to
/// stand in as a chain target.
struct RecordingSource {
    prefix: &'static str,
    calls: Rc<RefCell<Vec<String>>>,
}

impl RecordingSource {
    fn new(prefix: &'static str) -> Self {
        RecordingSource {
            prefix,
            calls: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl Source for RecordingSource {
    fn prefix(&self) -> &str {
        self.prefix
    }
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn default_ttl(&self) -> Duration {
        Duration::from_secs(3600)
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            self.calls.borrow_mut().extend(keys.iter().cloned());
            keys.into_iter()
                .map(|k| {
                    let mut m = serde_json::Map::new();
                    m.insert(
                        "title".into(),
                        CslValue::String(format!("{}:{k}", self.prefix)),
                    );
                    Resolution::concrete(k, CslValue::Object(m))
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
/// is hard-expired the source is always asked again. (Stale-window refetch is
/// probabilistic — covered by the `should_refetch` tests below — so it is not
/// asserted here where a single id/instant would be a coin flip.)
#[test]
fn fresh_is_not_refetched_expired_is() {
    let clock = MovableClock::new(0);
    let src = ProbeSource::new(Duration::from_millis(1000));
    let calls = src.calls.clone();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(60)))
        .register(src).unwrap();

    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(calls.borrow().len(), 1, "first retrieve must hit the source");

    // stale_after = 800, expires = 1000.
    clock.set(500);
    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(calls.borrow().len(), 1, "a fresh entry must not be refetched");

    clock.set(1000);
    block_on(mgr.retrieve(&cites("k"))).unwrap();
    assert_eq!(calls.borrow().len(), 2, "a hard-expired entry must be refetched");
}

// --- probabilistic stale-window refetch (`should_refetch`) -----------------

/// The stale-window refetch probability ramps with the stale fraction `f`: near
/// `stale_after` (f≈0) almost nothing is refetched; near `expires` (f≈1) almost
/// everything is. Demonstrated by the refetch *rate* over many ids at a fixed
/// `now`, which validates the ramp without pinning any single id's draw.
#[test]
fn stale_refetch_rate_ramps_with_the_stale_fraction() {
    let policy = no_jitter(Duration::from_secs(60));
    // stale_after = 800, expires = 1000 (a 200 ms-wide stale window).
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(0),
        Duration::from_millis(1000),
        "seed",
    );
    let rate = |now_ms: i64| {
        let now = Timestamp::from_millis(now_ms);
        let n = 2000;
        let hits = (0..n)
            .filter(|i| policy.should_refetch(&rec, now, &format!("id{i}")))
            .count();
        hits as f64 / n as f64
    };
    let low = rate(820); // f = 0.10
    let mid = rate(900); // f = 0.50
    let high = rate(980); // f = 0.90
    assert!(low < 0.25, "at f≈0.1 few ids should refetch, got {low}");
    assert!(high > 0.75, "at f≈0.9 most ids should refetch, got {high}");
    assert!(
        low < mid && mid < high,
        "refetch rate must rise with f: {low} < {mid} < {high}"
    );
}

/// The deterministic window edges: `Fresh` never refetches; `Expired` always
/// does, for every id.
#[test]
fn fresh_never_and_expired_always_refetch() {
    let policy = no_jitter(Duration::from_secs(60));
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(0),
        Duration::from_millis(1000),
        "seed",
    );
    // Fresh: now < stale_after (800).
    assert!(!policy.should_refetch(&rec, Timestamp::from_millis(0), "any"));
    assert!(!policy.should_refetch(&rec, Timestamp::from_millis(799), "any"));
    // Expired: now >= expires (1000), regardless of the id's draw.
    for i in 0..100 {
        let id = format!("id{i}");
        assert!(policy.should_refetch(&rec, Timestamp::from_millis(1000), &id), "{id}");
        assert!(policy.should_refetch(&rec, Timestamp::from_millis(5000), &id), "{id}");
    }
}

/// At `now == stale_after` the fraction is exactly 0, so *no* id refetches — a
/// deterministic, hash-independent floor. This is what lets a manager test force
/// "stale but not refetched" without knowing any hash.
#[test]
fn at_the_soft_edge_nothing_refetches() {
    let policy = no_jitter(Duration::from_secs(60));
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(0),
        Duration::from_millis(1000),
        "seed",
    );
    assert_eq!(policy.classify(&rec, Timestamp::from_millis(800)), Freshness::Stale);
    for i in 0..100 {
        assert!(
            !policy.should_refetch(&rec, Timestamp::from_millis(800), &format!("id{i}")),
            "f == 0 must never refetch"
        );
    }
}

/// An ephemeral zero-width window (`stale_after == expires`, i.e. a zero-TTL
/// record) never reaches the probabilistic branch: it is `Fresh` before the
/// instant and `Expired` at/after it, keeping the plain always/never behavior.
#[test]
fn ephemeral_zero_window_is_never_probabilistic() {
    let policy = TtlPolicy::default();
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(500),
        Duration::ZERO,
        "manual:x",
    );
    assert_eq!(rec.stale_after, rec.expires);
    assert!(!policy.should_refetch(&rec, Timestamp::from_millis(499), "manual:x"));
    // At/after the instant it is Expired → always refetch, for any id.
    for i in 0..50 {
        assert!(policy.should_refetch(&rec, Timestamp::from_millis(500), &format!("m{i}")));
        assert!(policy.should_refetch(&rec, Timestamp::from_millis(9999), &format!("m{i}")));
    }
}

/// The draw is a pure function of `(id, now)`: the same pair decides the same
/// way every call, and distinct ids at one instant disagree (a real per-entry
/// coin, not a global flag).
#[test]
fn should_refetch_is_deterministic_per_id_and_now() {
    let policy = no_jitter(Duration::from_secs(60));
    let rec = policy.make_record(
        Payload::Concrete(CslValue::Null),
        Timestamp::from_millis(0),
        Duration::from_millis(1000),
        "seed",
    );
    let now = Timestamp::from_millis(900); // f = 0.5, deep in the stale window.
    let first = policy.should_refetch(&rec, now, "determinism:probe");
    for _ in 0..8 {
        assert_eq!(
            policy.should_refetch(&rec, now, "determinism:probe"),
            first,
            "same (id, now) must decide the same way"
        );
    }
    // Different ids at the same instant do not all agree.
    let decisions: Vec<bool> = (0..64)
        .map(|i| policy.should_refetch(&rec, now, &format!("id{i}")))
        .collect();
    assert!(
        decisions.iter().any(|&d| d) && decisions.iter().any(|&d| !d),
        "the per-id draw must actually vary across ids"
    );
}

/// Manager-level: a batch of stale entries is only *partly* refetched — with the
/// draw at f≈0.5 a fetch-counting source sees strictly fewer refetches than
/// there are entries (and more than zero), so the soft tier does real work.
#[test]
fn a_stale_batch_is_only_partly_refetched() {
    let clock = MovableClock::new(0);
    let src = ProbeSource::new(Duration::from_millis(1000));
    let calls = src.calls.clone();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(600)))
        .register(src).unwrap();

    let n = 200;
    let batch: Vec<(String, String)> = (0..n)
        .map(|i| ("probe".to_string(), format!("k{i}")))
        .collect();

    // First pass at t=0: everything fetched fresh (stale_after=800, expires=1000).
    block_on(mgr.retrieve(&batch)).unwrap();
    assert_eq!(calls.borrow().len(), n, "first pass fetches all {n}");

    // Second pass at t=900 (f=0.5): each stale entry is refetched independently
    // with probability ~f, so some are and some are not.
    clock.set(900);
    block_on(mgr.retrieve(&batch)).unwrap();
    let refetched = calls.borrow().len() - n;
    assert!(
        refetched > 0 && refetched < n,
        "a stale batch must be only partly refetched, got {refetched}/{n}"
    );
}

/// Manager-level: a stale chained pointer that is *not* refetched this pass must
/// still have its target pulled in, or a later `get()` breaks on the missing
/// link. Forced deterministically by aging the pointer to exactly `stale_after`
/// (f = 0 ⇒ never refetched, whatever the id hashes to).
#[test]
fn a_not_refetched_stale_pointer_keeps_its_target() {
    let clock = MovableClock::new(1000);
    let a = RecordingSource::new("a");
    let a_calls = a.calls.clone();
    let b = RecordingSource::new("b");
    let b_calls = b.calls.clone();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_policy(no_jitter(Duration::from_secs(600)))
        .register(a).unwrap()
        .register(b).unwrap();

    // Seed `a:x` as a stale pointer to a `b:t` that is not yet cached.
    // stale_after == now (1000) < expires (2000) ⇒ Stale with f = 0.
    block_on(mgr.store().put(
        "a:x",
        CacheRecord {
            payload: Payload::Chained {
                prefix: "b".into(),
                key: "t".into(),
                set_properties: CslValue::Object(serde_json::Map::new()),
            },
            stale_after: Timestamp::from_millis(1000),
            expires: Timestamp::from_millis(2000),
        },
    ))
    .unwrap();

    let report = block_on(mgr.retrieve(&[("a".to_string(), "x".to_string())])).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert!(
        a_calls.borrow().is_empty(),
        "the stale pointer must not be refetched at f = 0: {:?}",
        a_calls.borrow()
    );
    assert_eq!(b_calls.borrow().len(), 1, "the kept pointer's target must be fetched");
    assert_eq!(b_calls.borrow()[0], "t");
    assert_eq!(
        block_on(mgr.get("a", "x")).unwrap()["title"],
        "b:t",
        "get() must still resolve the chain"
    );
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
        .register(src).unwrap();

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
        .register(src).unwrap();

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
        .register(src).unwrap();

    let err = block_on(mgr.retrieve(&cites("k"))).unwrap_err();
    assert!(
        matches!(err, autocitefetch::Error::Store(_)),
        "expected a store error, got {err}"
    );
    assert_eq!(flushes.get(), 1, "buffered writes must be flushed even on the error path");
}
