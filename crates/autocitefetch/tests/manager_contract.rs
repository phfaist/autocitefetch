//! The manager's contract with a source, and the driver's rate limiting.
//!
//! Covers the things a badly-behaved (or merely unlucky) source can do — omit
//! a key, answer twice, chain to itself, chain forever, hand back a non-object
//! payload — plus chunking/pacing, which no other test reaches because no test
//! elsewhere supplies more keys than `chunk_size` or has a clock that moves.

use std::cell::{Cell, RefCell};
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, Error, FetchError,
    Fetcher, Outcome, Payload, Request, Resolution, Response, RetrieveCtx, Source, StoreError,
    Timer, Timestamp,
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

/// A `MemStore` that also counts `put`s, so a source answering twice for one
/// key can be caught double-writing.
#[derive(Clone, Default)]
struct MemStore {
    map: Rc<RefCell<StdMap<String, CacheRecord>>>,
    puts: Rc<Cell<u32>>,
}

impl MemStore {
    fn seed(&self, id: &str, payload: Payload, stale_after: i64, expires: i64) {
        self.map.borrow_mut().insert(
            id.into(),
            CacheRecord {
                payload,
                stale_after: Timestamp::from_millis(stale_after),
                expires: Timestamp::from_millis(expires),
            },
        );
    }
}

impl CacheStore for MemStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let v = self.map.borrow().get(id).cloned();
        Box::pin(async move { Ok(v) })
    }
    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        self.puts.set(self.puts.get() + 1);
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

/// A clock the mocks move forward (request latency, timer sleeps).
#[derive(Clone, Default)]
struct MovableClock {
    ms: Rc<Cell<i64>>,
}
impl MovableClock {
    fn advance(&self, ms: i64) {
        self.ms.set(self.ms.get() + ms);
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

/// A timer that makes time actually pass: sleeping moves the shared clock. With
/// it, "when was each request issued" is observable.
struct AdvancingTimer {
    clock: MovableClock,
}
impl Timer for AdvancingTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        self.clock.advance(dur.as_millis() as i64);
        Box::pin(async {})
    }
}

/// What a [`ScriptSource`] does with the keys it is handed.
#[derive(Clone, Copy)]
enum Answer {
    /// Resolve every key to a concrete item.
    Concrete,
    /// Chain key `k` to `(target, k)`.
    ChainTo(&'static str),
    /// Chain numeric key `n` to `(own prefix, n + 1)` — an unbounded chain.
    ChainToSuccessor,
    /// Chain every key to itself.
    SelfChain,
    /// Fail every key.
    Fail,
    /// Resolve concretely, but with a JSON array instead of an object.
    NonObject,
    /// Answer only the key `"ok"`, silently dropping every other key.
    OmitOthers,
    /// Return *two* resolutions for every key.
    Duplicate,
}

/// One configurable source, so each test can express exactly the (mis)behavior
/// it is about.
struct ScriptSource {
    prefix: &'static str,
    answer: Answer,
    chunk: usize,
    interval: Duration,
    /// Simulated per-chunk request latency, in ms.
    latency: i64,
    clock: MovableClock,
    /// `(key, clock reading when the chunk carrying it was issued)`.
    log: Rc<RefCell<Vec<(String, i64)>>>,
}

impl ScriptSource {
    fn new(prefix: &'static str, answer: Answer, clock: &MovableClock) -> Self {
        ScriptSource {
            prefix,
            answer,
            chunk: 512,
            interval: Duration::ZERO,
            latency: 0,
            clock: clock.clone(),
            log: Rc::new(RefCell::new(Vec::new())),
        }
    }
    fn paced(mut self, chunk: usize, interval_ms: u64, latency: i64) -> Self {
        self.chunk = chunk;
        self.interval = Duration::from_millis(interval_ms);
        self.latency = latency;
        self
    }
    fn log(&self) -> Rc<RefCell<Vec<(String, i64)>>> {
        self.log.clone()
    }
}

fn item(title: &str) -> CslValue {
    let mut m = serde_json::Map::new();
    m.insert("title".into(), CslValue::String(title.into()));
    CslValue::Object(m)
}

impl Source for ScriptSource {
    fn prefix(&self) -> &str {
        self.prefix
    }
    fn chunk_size(&self) -> usize {
        self.chunk
    }
    fn min_interval(&self) -> Duration {
        self.interval
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
            let issued = self.clock.now().as_millis();
            for k in &keys {
                self.log.borrow_mut().push((k.clone(), issued));
            }
            self.clock.advance(self.latency);

            let mut out = Vec::new();
            for k in keys {
                match self.answer {
                    Answer::Concrete => {
                        out.push(Resolution::concrete(k.clone(), item(&format!("{}:{k}", self.prefix))))
                    }
                    Answer::ChainTo(target) => out.push(Resolution {
                        key: k.clone(),
                        outcome: Outcome::Chained {
                            prefix: target.into(),
                            key: k,
                            set_properties: CslValue::Object(serde_json::Map::new()),
                        },
                    }),
                    Answer::ChainToSuccessor => {
                        let next = k.parse::<u32>().unwrap() + 1;
                        out.push(Resolution {
                            key: k,
                            outcome: Outcome::Chained {
                                prefix: self.prefix.into(),
                                key: next.to_string(),
                                set_properties: CslValue::Object(serde_json::Map::new()),
                            },
                        });
                    }
                    Answer::SelfChain => out.push(Resolution {
                        key: k.clone(),
                        outcome: Outcome::Chained {
                            prefix: self.prefix.into(),
                            key: k,
                            set_properties: CslValue::Object(serde_json::Map::new()),
                        },
                    }),
                    Answer::Fail => {
                        out.push(Resolution::failed(k, Error::Source("source down".into())))
                    }
                    Answer::NonObject => out.push(Resolution::concrete(
                        k,
                        CslValue::Array(vec![CslValue::String("not a CSL item".into())]),
                    )),
                    Answer::OmitOthers => {
                        if k == "ok" {
                            out.push(Resolution::concrete(k, item("ok")));
                        }
                    }
                    Answer::Duplicate => {
                        out.push(Resolution::concrete(k.clone(), item("first")));
                        out.push(Resolution::concrete(k, item("second")));
                    }
                }
            }
            out
        })
    }
}

fn cites(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(p, k)| (p.to_string(), k.to_string()))
        .collect()
}

// --- the one-resolution-per-key contract -----------------------------------

/// A source that drops a key must not leave the citation *both* unstored and
/// unreported: `retrieve` used to claim success for something `get` can never
/// return.
#[test]
fn a_key_the_source_omits_is_reported() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(ScriptSource::new("s", Answer::OmitOthers, &clock));

    let report = block_on(mgr.retrieve(&cites(&[("s", "ok"), ("s", "ghost")]))).unwrap();
    assert!(!report.is_complete(), "the dropped key must be reported");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].key, "ghost");
    assert!(
        report.failures[0].message.contains("no resolution"),
        "unhelpful message: {}",
        report.failures[0].message
    );
    // The key that *was* answered still resolved.
    assert!(block_on(mgr.get("s", "ok")).is_ok());
    assert!(block_on(mgr.get("s", "ghost")).is_err());
}

/// A source answering twice for one key writes the store once (and, for
/// failures, reports once).
#[test]
fn duplicate_resolutions_for_one_key_are_collapsed() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let puts = store.puts.clone();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(ScriptSource::new("s", Answer::Duplicate, &clock));

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(puts.get(), 1, "the duplicate must not double-write the store");
    assert_eq!(block_on(mgr.get("s", "k")).unwrap()["title"], "first");
}

/// A concrete payload that is not a JSON object is a source bug: it used to be
/// silently replaced by a bare `{"id": …}` stub and cached as a success.
#[test]
fn a_non_object_concrete_payload_is_reported_not_stubbed() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let entries = store.clone();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(ScriptSource::new("s", Answer::NonObject, &clock));

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].message.contains("non-object"),
        "unhelpful message: {}",
        report.failures[0].message
    );
    assert_eq!(
        block_on(entries.entries()).unwrap().len(),
        0,
        "nothing should have been cached"
    );
}

// --- chains ----------------------------------------------------------------

/// `retrieve` refuses to walk further than `get` could ever follow. Without the
/// bound, a source chaining `k -> k+1` fetched thousands of links for a single
/// requested citation.
#[test]
fn chain_discovery_is_bounded_by_max_chain_depth() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let entries = store.clone();
    let src = ScriptSource::new("c", Answer::ChainToSuccessor, &clock);
    let log = src.log();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .with_max_chain_depth(4)
        .register(src);

    let report = block_on(mgr.retrieve(&cites(&[("c", "0")]))).unwrap();

    // Links at depth 0..3 were fetched; depth 4 was refused and reported.
    assert_eq!(log.borrow().len(), 4, "fetched: {:?}", log.borrow());
    assert_eq!(block_on(entries.entries()).unwrap().len(), 4);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].key, "4");
    assert!(
        report.failures[0].message.contains("links"),
        "unhelpful message: {}",
        report.failures[0].message
    );

    // And reading it fails cleanly rather than looping.
    let err = block_on(mgr.get("c", "0")).unwrap_err();
    assert!(matches!(err, Error::Chain(_)), "got {err}");
}

/// A pointer at itself is caught at store time instead of surfacing as
/// "chain too deep" `max_chain_depth` store reads later.
#[test]
fn a_self_chain_is_rejected_at_store_time() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let entries = store.clone();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(ScriptSource::new("s", Answer::SelfChain, &clock));

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].message.contains("itself"),
        "unhelpful message: {}",
        report.failures[0].message
    );
    assert_eq!(block_on(entries.entries()).unwrap().len(), 0);
}

/// A chain target that is not in the cache names *both* ids: the target alone
/// is a citation the caller never asked for.
#[test]
fn a_missing_chain_target_names_both_ids() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "gone".into(),
            set_properties: CslValue::Object(serde_json::Map::new()),
        },
        10_000,
        10_000,
    );
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer);

    let err = block_on(mgr.get("a", "x")).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("a:x") && msg.contains("b:gone"), "got: {msg}");

    // A miss on the *requested* id is still a plain NotFound.
    let err = block_on(mgr.get("a", "absent")).unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "got {err}");
}

/// A stale chained record must not drag its old target along: the refetch may
/// well replace the pointer (retracted DOI, suppressed by an override), and the
/// dead target's 404 would be reported as a failure for a citation nobody
/// requested — plus one wasted rate-limited request.
#[test]
fn a_stale_pointer_does_not_fetch_its_dead_target() {
    let clock = MovableClock::default();
    clock.advance(1_000_000);
    let store = MemStore::default();
    // `a:x` used to chain to `b:OLD`, and is now hard-expired.
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "OLD".into(),
            set_properties: CslValue::Object(serde_json::Map::new()),
        },
        0,
        0,
    );

    let a = ScriptSource::new("a", Answer::Concrete, &clock);
    let b = ScriptSource::new("b", Answer::Fail, &clock);
    let b_log = b.log();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(a)
        .register(b);

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert!(
        b_log.borrow().is_empty(),
        "the superseded target was fetched anyway: {:?}",
        b_log.borrow()
    );
    assert!(
        report.is_complete(),
        "a citation nobody asked for was reported: {:?}",
        report.failures
    );
    assert_eq!(block_on(mgr.get("a", "x")).unwrap()["title"], "a:x");
}

/// …but a chained record kept alive by the grace window *does* still need its
/// target, or `get()` breaks on the next link.
#[test]
fn a_grace_served_pointer_keeps_its_target() {
    let clock = MovableClock::default();
    clock.advance(1000);
    let store = MemStore::default();
    // Hard-expired (but well within the default 14-day grace) pointer whose
    // target is not in the cache yet.
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "t".into(),
            set_properties: CslValue::Object(serde_json::Map::new()),
        },
        0,
        0,
    );

    let a = ScriptSource::new("a", Answer::Fail, &clock);
    let b = ScriptSource::new("b", Answer::Concrete, &clock);
    let b_log = b.log();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(a)
        .register(b);

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(
        b_log.borrow().len(),
        1,
        "the kept pointer's target must be fetched: {:?}",
        b_log.borrow()
    );
    assert_eq!(block_on(mgr.get("a", "x")).unwrap()["title"], "b:t");
}

/// A chain's `set_properties` **override** the concrete target's colliding
/// fields (both reference impls do `{ ...target, ...set_properties }`). Here the
/// chain carries `title:"FROM CHAIN"` and the target has `title:"FROM TARGET"`;
/// the chain must win.
#[test]
fn chained_set_properties_override_the_target() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "t".into(),
            set_properties: serde_json::json!({"title": "FROM CHAIN", "extra": "kept"}),
        },
        10_000,
        10_000,
    );
    store.seed(
        "b:t",
        Payload::Concrete(serde_json::json!({"id": "b:t", "title": "FROM TARGET", "year": 1935})),
        10_000,
        10_000,
    );
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer);

    let item = block_on(mgr.get("a", "x")).unwrap();
    assert_eq!(item["title"], "FROM CHAIN", "the chain must override the target");
    assert_eq!(item["extra"], "kept", "chain-only field is attached");
    assert_eq!(item["year"], 1935, "target-only field is preserved");
    assert_eq!(item["id"], "a:x", "id is rewritten to the requested one");
}

/// The requested `id` is forced last, so a `set_properties` carrying its own
/// `id` can never override it — even though `set_properties` otherwise win.
#[test]
fn a_set_properties_id_cannot_override_the_requested_id() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    store.seed(
        "a:y",
        Payload::Chained {
            prefix: "b".into(),
            key: "u".into(),
            set_properties: serde_json::json!({"id": "HACKED", "note": "n"}),
        },
        10_000,
        10_000,
    );
    store.seed(
        "b:u",
        Payload::Concrete(serde_json::json!({"id": "b:u", "title": "T"})),
        10_000,
        10_000,
    );
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer);

    let item = block_on(mgr.get("a", "y")).unwrap();
    assert_eq!(item["id"], "a:y", "the requested id must win over set_properties.id");
    assert_eq!(item["note"], "n", "other set_properties still apply");
    assert_eq!(item["title"], "T", "target field with no override is kept");
}

// --- chunking and rate limiting --------------------------------------------

/// Requests are spaced by `min_interval` *start to start*, across chunks and
/// across retrieval passes.
///
/// Both used to be broken: `drive_source` was called once per pass and reset
/// its "first chunk, no sleep" flag every time, so the second pass (which the
/// arXiv→DOI chain guarantees) fired immediately after the first; and the wait
/// was a full interval *after* the previous chunk returned, i.e.
/// `interval + latency`.
#[test]
fn pacing_is_start_to_start_and_survives_passes() {
    let clock = MovableClock::default();
    // `a` chains to `b` (so a second pass reaches `b`) but costs no time.
    let a = ScriptSource::new("a", Answer::ChainTo("b"), &clock);
    // One key per request, 1100 ms apart, each request taking 200 ms.
    let b = ScriptSource::new("b", Answer::Concrete, &clock).paced(1, 1100, 200);
    let b_log = b.log();
    let timer = AdvancingTimer {
        clock: clock.clone(),
    };
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), timer)
        .register(a)
        .register(b);

    let report =
        block_on(mgr.retrieve(&cites(&[("b", "d1"), ("b", "d2"), ("a", "x")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let times: Vec<i64> = b_log.borrow().iter().map(|(_, t)| *t).collect();
    assert_eq!(
        times.len(),
        3,
        "expected three `b` requests (two direct + one chained): {:?}",
        b_log.borrow()
    );
    // 0, then +1100 (not +1300 = interval + latency), then the *next pass*
    // waits out the remainder of the interval instead of firing at once.
    assert_eq!(times, vec![0, 1100, 2200], "log: {:?}", b_log.borrow());
}

// --- reading ---------------------------------------------------------------

/// `get_by_id` round-trips a `"prefix:key"` id, and an id without a `':'` is a
/// malformed id — not "no source registered for prefix `foo`".
#[test]
fn get_by_id_round_trips_and_rejects_a_missing_colon() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(ScriptSource::new("s", Answer::Concrete, &clock));

    // A key containing a colon still round-trips: ids split on the *first* one.
    block_on(mgr.retrieve(&cites(&[("s", "k"), ("s", "10.1/x:y")]))).unwrap();
    assert_eq!(block_on(mgr.get_by_id("s:k")).unwrap()["id"], "s:k");
    assert_eq!(
        block_on(mgr.get_by_id("s:10.1/x:y")).unwrap()["id"],
        "s:10.1/x:y"
    );

    let err = block_on(mgr.get_by_id("no-colon-here")).unwrap_err();
    assert!(matches!(err, Error::InvalidId(_)), "got {err}");
    assert!(err.to_string().contains("malformed"), "got: {err}");
}

/// A prefix containing `':'` would make `prefix:key` ids ambiguous, so it is
/// refused at registration time rather than silently corrupting the cache.
#[test]
#[should_panic(expected = "must not contain ':'")]
fn registering_a_prefix_with_a_colon_panics() {
    let clock = MovableClock::default();
    let _ = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(ScriptSource::new("bad:prefix", Answer::Concrete, &clock));
}
